//go:build e2e

package e2e

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// ---- ports (the project's range is 21200-21299; AGENTS.md lists who uses what) ----

const (
	portAPI0      = 21270 // manager 0: API, then gRPC; manager 1 is two higher
	portHealth0   = 21274 // agent health endpoints: 21274, 21275, 21276
	portProxy0    = 21277 // the TCP proxy in front of the first manager
	portEtcd      = 21278 // client; the peer port is the next
	portPostgres  = 21236
	apiToken      = "e2e-api-token-0123456789-abcdef"
	dbCanary      = "e2e-db-password-canary-9f3a"
	managerTLSDNS = "localhost"
)

// ---- processes ----

type procRegistry struct {
	mu    sync.Mutex
	procs []*proc
}

var registry procRegistry

func (r *procRegistry) add(p *proc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.procs = append(r.procs, p)
}

func (r *procRegistry) killAll() {
	r.mu.Lock()
	procs := append([]*proc(nil), r.procs...)
	r.mu.Unlock()
	for _, p := range procs {
		p.kill()
	}
}

// proc is a child process in a process group of its own, logging to a file.
type proc struct {
	name    string
	cmd     *exec.Cmd
	logPath string
	done    chan struct{}
}

func start(name, bin string, args, env []string, logPath string) (*proc, error) {
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(bin, args...)
	cmd.Env = env
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("start %s: %w", name, err)
	}
	p := &proc{name: name, cmd: cmd, logPath: logPath, done: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		_ = logFile.Close()
		close(p.done)
	}()
	registry.add(p)
	return p, nil
}

func (p *proc) running() bool {
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

func (p *proc) wait(timeout time.Duration) bool {
	select {
	case <-p.done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// signal sends a signal to the process itself, not to its group.
func (p *proc) signal(sig syscall.Signal) {
	if p.running() {
		_ = syscall.Kill(p.cmd.Process.Pid, sig)
	}
}

// kill is a crash: SIGKILL to the whole group, and wait for the exit.
func (p *proc) kill() {
	if p.running() {
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
	}
	p.wait(10 * time.Second)
}

// terminate is what a supervisor does on stop: SIGTERM, then SIGKILL after
// timeout. It returns the exit code (-1 when the process was killed).
func (p *proc) terminate(timeout time.Duration) (code int, graceful bool) {
	p.signal(syscall.SIGTERM)
	if p.wait(timeout) {
		return p.exitCode(), true
	}
	p.kill()
	return -1, false
}

func (p *proc) exitCode() int {
	<-p.done
	return p.cmd.ProcessState.ExitCode()
}

func (p *proc) logs() string {
	raw, _ := os.ReadFile(p.logPath)
	return string(raw)
}

func tailLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// ---- the tools every scenario needs: binaries, PostgreSQL, etcd ----

var (
	repoRoot  string
	workDir   string
	binDir    string
	pgSrv     *pgServer   // nil when an external server is used
	etcdSrv   *etcdServer // nil when an external etcd is used
	adminURL  string      // a role that may CREATE DATABASE
	etcdURL   string
	havePG    bool
	haveEtcd  bool
	buildOnce sync.Once
)

func findRepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("go.mod not found above the test directory")
		}
		dir = parent
	}
}

func goBinary() string {
	if p, err := exec.LookPath("go"); err == nil {
		return p
	}
	return filepath.Join(runtime.GOROOT(), "bin", "go")
}

func buildBinaries() error {
	env := append(os.Environ(), "CGO_ENABLED=0", "GOFLAGS=-p=2")
	for _, b := range []struct{ out, pkg, tags string }{
		{"dockfleet-manager", "./cmd/manager", ""},
		{"dockfleet-agent", "./cmd/agent", "e2efake"},
		{"dockfleet-pki", "./cmd/pki", ""},
	} {
		args := []string{"build", "-trimpath", "-o", filepath.Join(binDir, b.out)}
		if b.tags != "" {
			args = append(args, "-tags", b.tags)
		}
		args = append(args, b.pkg)
		cmd := exec.Command(goBinary(), args...)
		cmd.Dir, cmd.Env = repoRoot, env
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("go %s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
	return nil
}

func TestMain(m *testing.M) { os.Exit(run(m)) }

func run(m *testing.M) int {
	var err error
	if repoRoot, err = findRepoRoot(); err != nil {
		fmt.Fprintln(os.Stderr, "e2e:", err)
		return 1
	}
	if workDir, err = os.MkdirTemp("", "dockfleet-e2e-"); err != nil {
		fmt.Fprintln(os.Stderr, "e2e:", err)
		return 1
	}
	defer os.RemoveAll(workDir)
	defer registry.killAll()
	binDir = filepath.Join(workDir, "bin")
	if err := os.Mkdir(binDir, 0o700); err != nil {
		fmt.Fprintln(os.Stderr, "e2e:", err)
		return 1
	}
	if err := buildBinaries(); err != nil {
		fmt.Fprintln(os.Stderr, "e2e: build:", err)
		return 1
	}

	if adminURL = os.Getenv("DOCKFLEET_TEST_DATABASE_URL"); adminURL != "" {
		havePG = true
	} else if bin := firstExisting(os.Getenv("DOCKFLEET_E2E_POSTGRES_BIN"), filepath.Join(repoRoot, ".tools", "postgres", "bin", "postgres")); bin != "" {
		pgSrv = &pgServer{bin: bin, initdb: filepath.Join(filepath.Dir(bin), "initdb"), data: filepath.Join(workDir, "pgdata"), port: portPostgres}
		if err := pgSrv.init(); err != nil {
			fmt.Fprintln(os.Stderr, "e2e: postgres:", err)
			return 1
		}
		if err := pgSrv.start(); err != nil {
			fmt.Fprintln(os.Stderr, "e2e: postgres:", err)
			return 1
		}
		adminURL, havePG = pgSrv.url("postgres"), true
	}
	if etcdURL = os.Getenv("DOCKFLEET_TEST_ETCD_ENDPOINTS"); etcdURL != "" {
		haveEtcd = true
	} else if bin := firstExisting(os.Getenv("DOCKFLEET_E2E_ETCD_BIN"), filepath.Join(repoRoot, ".tools", "etcd", "etcd")); bin != "" {
		etcdSrv = &etcdServer{bin: bin, data: filepath.Join(workDir, "etcd"), port: portEtcd}
		if err := etcdSrv.start(); err != nil {
			fmt.Fprintln(os.Stderr, "e2e: etcd:", err)
			return 1
		}
		etcdURL, haveEtcd = etcdSrv.url(), true
	}
	return m.Run()
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// firstExisting returns the first of the paths that names a file, or "".
// DOCKFLEET_E2E_POSTGRES_BIN and DOCKFLEET_E2E_ETCD_BIN name server binaries
// the tests start (and stop) themselves, which is what lets them crash the
// database and take etcd away; they default to the ones under .tools/.
func firstExisting(paths ...string) string {
	for _, p := range paths {
		if p != "" && fileExists(p) {
			return p
		}
	}
	return ""
}

// ---- PostgreSQL ----

type pgServer struct {
	bin, initdb, data string
	port              int
	p                 *proc
}

func (s *pgServer) url(db string) string {
	return fmt.Sprintf("postgres://dockfleet@127.0.0.1:%d/%s?sslmode=disable", s.port, db)
}

func (s *pgServer) init() error {
	cmd := exec.Command(s.initdb, "-D", s.data, "-U", "dockfleet", "-A", "trust", "-E", "UTF8", "--no-locale")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("initdb: %v\n%s", err, out)
	}
	return nil
}

func (s *pgServer) start() error {
	p, err := start("postgres", s.bin, []string{
		"-D", s.data, "-p", fmt.Sprint(s.port), "-c", "listen_addresses=127.0.0.1", "-c", "unix_socket_directories=",
		// fsync is off for speed, which only matters if the machine loses power; a crash of the
		// postmaster, which is what the tests cause, loses nothing that was committed, as long as a
		// commit waits for its WAL (the default). synchronous_commit=off would lose the last moments.
		"-c", "fsync=off", "-c", "max_connections=100",
	}, os.Environ(), filepath.Join(workDir, "postgres.log"))
	if err != nil {
		return err
	}
	s.p = p
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := pgx.Connect(ctx, s.url("postgres"))
		cancel()
		if err == nil {
			_ = conn.Close(context.Background())
			return nil
		}
		if !p.running() || time.Now().After(deadline) {
			return fmt.Errorf("postgres did not become ready: %v\n%s", err, tailLines(p.logs(), 20))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// crash is an immediate shutdown (SIGQUIT to the postmaster): no checkpoint, no
// goodbye, as when the machine loses power; the next start recovers.
func (s *pgServer) crash() {
	s.p.signal(syscall.SIGQUIT)
	if !s.p.wait(20 * time.Second) {
		s.p.kill()
	}
}

// newDatabase makes an empty database for one test and drops it afterwards.
func newDatabase(t *testing.T) string {
	t.Helper()
	if !havePG {
		t.Skip("no PostgreSQL: set DOCKFLEET_TEST_DATABASE_URL or install one under .tools/postgres")
	}
	name := fmt.Sprintf("dockfleet_e2e_%d", time.Now().UnixNano())
	connect := func() (*pgx.Conn, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return pgx.Connect(ctx, adminURL)
	}
	conn, err := connect()
	if err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}
	if _, err := conn.Exec(context.Background(), `CREATE DATABASE `+name); err != nil {
		t.Fatalf("create the test database: %v", err)
	}
	_ = conn.Close(context.Background())
	t.Cleanup(func() {
		deadline := time.Now().Add(15 * time.Second)
		for {
			c, err := connect()
			if err == nil {
				_, err = c.Exec(context.Background(), `DROP DATABASE IF EXISTS `+name+` WITH (FORCE)`)
				_ = c.Close(context.Background())
				if err == nil {
					return
				}
			}
			if time.Now().After(deadline) {
				t.Logf("could not drop database %s: %v", name, err)
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
	})
	u := strings.Replace(adminURL, "/postgres?", "/"+name+"?", 1)
	if u == adminURL { // an admin URL that names another database
		parsed := strings.SplitN(adminURL, "?", 2)
		base := parsed[0][:strings.LastIndex(parsed[0], "/")]
		u = base + "/" + name
		if len(parsed) == 2 {
			u += "?" + parsed[1]
		}
	}
	return u
}

// ---- etcd ----

type etcdServer struct {
	bin, data string
	port      int
	p         *proc
}

func (e *etcdServer) url() string { return fmt.Sprintf("http://127.0.0.1:%d", e.port) }

func (e *etcdServer) start() error {
	client, peer := e.url(), fmt.Sprintf("http://127.0.0.1:%d", e.port+1)
	p, err := start("etcd", e.bin, []string{
		"--name", "e2e", "--data-dir", e.data,
		"--listen-client-urls", client, "--advertise-client-urls", client,
		"--listen-peer-urls", peer, "--initial-advertise-peer-urls", peer, "--initial-cluster", "e2e=" + peer,
		"--heartbeat-interval", "20", "--election-timeout", "200", "--log-level", "warn",
	}, os.Environ(), filepath.Join(workDir, "etcd.log"))
	if err != nil {
		return err
	}
	e.p = p
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := http.Get(client + "/health")
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == 200 && strings.Contains(string(body), `"true"`) {
				return nil
			}
		}
		if !p.running() || time.Now().After(deadline) {
			return fmt.Errorf("etcd did not become healthy: %v\n%s", err, tailLines(p.logs(), 20))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (e *etcdServer) stop() {
	e.p.signal(syscall.SIGTERM)
	if !e.p.wait(10 * time.Second) {
		e.p.kill()
	}
}

// ---- waiting ----

func eventually(t *testing.T, timeout time.Duration, what string, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := ""
	for {
		ok, why := cond()
		if ok {
			return
		}
		last = why
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s: %s", timeout, what, last)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// consistently fails the test if cond is false at any moment during d.
func consistently(t *testing.T, d time.Duration, what string, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if ok, why := cond(); !ok {
			t.Fatalf("%s did not hold: %s", what, why)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ---- a TCP proxy that can be cut, to partition one agent from the managers ----

type tcpProxy struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	cut    bool
	conns  map[net.Conn]struct{}
}

func newProxy(t *testing.T, listen, target string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		t.Fatalf("proxy: %v", err)
	}
	p := &tcpProxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	go p.accept()
	t.Cleanup(func() {
		_ = ln.Close()
		p.closeAll()
	})
	return p
}

func (p *tcpProxy) accept() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		cut := p.cut
		if !cut {
			p.conns[c] = struct{}{}
		}
		p.mu.Unlock()
		if cut {
			_ = c.Close()
			continue
		}
		go p.pipe(c)
	}
}

func (p *tcpProxy) pipe(c net.Conn) {
	up, err := net.DialTimeout("tcp", p.target, 2*time.Second)
	if err != nil {
		_ = c.Close()
		return
	}
	p.mu.Lock()
	if p.cut {
		p.mu.Unlock()
		_ = c.Close()
		_ = up.Close()
		return
	}
	p.conns[up] = struct{}{}
	p.mu.Unlock()
	done := make(chan struct{}, 2)
	cp := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		_ = dst.Close()
		_ = src.Close()
		done <- struct{}{}
	}
	go cp(up, c)
	go cp(c, up)
	<-done
	<-done
	p.mu.Lock()
	delete(p.conns, c)
	delete(p.conns, up)
	p.mu.Unlock()
}

func (p *tcpProxy) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		_ = c.Close()
	}
	p.conns = map[net.Conn]struct{}{}
}

// cutOff drops every connection and refuses new ones, with neither side told why.
func (p *tcpProxy) cutOff() {
	p.mu.Lock()
	p.cut = true
	p.mu.Unlock()
	p.closeAll()
}

func (p *tcpProxy) heal() {
	p.mu.Lock()
	p.cut = false
	p.mu.Unlock()
}

// ---- the API as an administrator sees it ----

type apiClient struct {
	t     *testing.T
	base  string
	token string
	http  *http.Client
}

func newAPIClient(t *testing.T, addr string, roots *x509.CertPool, token string) *apiClient {
	return &apiClient{
		t: t, base: "https://" + addr, token: token,
		http: &http.Client{
			Timeout:       10 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
			Transport:     &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13}, DisableKeepAlives: true},
		},
	}
}

// try performs a request and reports a transport failure as an error.
func (a *apiClient) try(method, path, body string, headers ...string) (int, []byte, error) {
	req, err := http.NewRequest(method, a.base+path, strings.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	if a.token != "" {
		req.Header.Set("Authorization", "Bearer "+a.token)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := a.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	return resp.StatusCode, raw, err
}

func (a *apiClient) do(method, path, body string) (int, []byte) {
	a.t.Helper()
	code, raw, err := a.try(method, path, body)
	if err != nil {
		a.t.Fatalf("%s %s: %v", method, path, err)
	}
	return code, raw
}

// must makes a request that has to answer want, and decodes the answer into out (if not nil).
func (a *apiClient) must(method, path, body string, want int, out any) {
	a.t.Helper()
	code, raw := a.do(method, path, body)
	if code != want {
		a.t.Fatalf("%s %s = %d, want %d: %s", method, path, code, want, raw)
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			a.t.Fatalf("%s %s: %v: %s", method, path, err, raw)
		}
	}
}

type replicaView struct {
	Revision     int    `json:"revision"`
	Index        int    `json:"index"`
	WorkloadID   string `json:"workload_id"`
	NodeID       string `json:"node_id"`
	DesiredState string `json:"desired_state"`
	Generation   int64  `json:"generation"`
	Acknowledged bool   `json:"acknowledged"`
	Phase        string `json:"phase"`
	RestartCount int    `json:"restart_count"`
	NodeHealth   string `json:"node_health"`
}

type serviceView struct {
	Name     string `json:"name"`
	Replicas int    `json:"replicas"`
	Revision int    `json:"revision"`
	Version  int64  `json:"version"`
	Spec     struct {
		Image string `json:"image"`
	} `json:"spec"`
	Status struct {
		Scaling struct {
			State    string `json:"state"`
			Desired  int    `json:"desired"`
			Ready    int    `json:"ready"`
			Starting int    `json:"starting"`
			Waiting  int    `json:"waiting"`
			Surplus  int    `json:"surplus"`
			Stopping int    `json:"stopping"`
			Outdated int    `json:"outdated"`
			Message  string `json:"message"`
		} `json:"scaling"`
		Rollout struct {
			Revision int    `json:"revision"`
			State    string `json:"state"`
			Reason   string `json:"reason"`
			Message  string `json:"message"`
		} `json:"rollout"`
		Replicas      []replicaView `json:"replicas"`
		Unschedulable []struct {
			Code    string `json:"code"`
			Message string `json:"message"`
			Nodes   []struct {
				NodeID string `json:"node_id"`
				Code   string `json:"code"`
			} `json:"nodes"`
		} `json:"unschedulable"`
	} `json:"status"`
}

// meant are the replicas that are meant to run; running are those that also
// report running on a node that is not given up on.
func (s serviceView) meant() []replicaView {
	var out []replicaView
	for _, r := range s.Status.Replicas {
		if r.DesiredState == "running" {
			out = append(out, r)
		}
	}
	return out
}

func (s serviceView) running() []replicaView {
	var out []replicaView
	for _, r := range s.meant() {
		if r.Phase == "running" && r.Acknowledged && r.NodeHealth != "unavailable" {
			out = append(out, r)
		}
	}
	return out
}

func ids(rs []replicaView) map[string]bool {
	out := map[string]bool{}
	for _, r := range rs {
		out[r.WorkloadID] = true
	}
	return out
}

func (a *apiClient) service(name string) serviceView {
	a.t.Helper()
	var v serviceView
	a.must(http.MethodGet, "/v1/services/"+name, "", http.StatusOK, &v)
	return v
}

// stable is true once the service has exactly n replicas, all running, with
// nothing on its way in or out.
func stable(v serviceView, n int) (bool, string) {
	s := v.Status.Scaling
	if s.State != "stable" || s.Desired != n || s.Ready != n || len(v.meant()) != n || len(v.Status.Replicas) != n {
		return false, fmt.Sprintf("scaling %s desired %d ready %d, %d meant to run of %d listed: %s", s.State, s.Desired, s.Ready, len(v.meant()), len(v.Status.Replicas), s.Message)
	}
	return true, ""
}

func (a *apiClient) waitStable(timeout time.Duration, name string, n int) serviceView {
	a.t.Helper()
	var v serviceView
	eventually(a.t, timeout, fmt.Sprintf("%s to be stable at %d replicas", name, n), func() (bool, string) {
		code, raw, err := a.try(http.MethodGet, "/v1/services/"+name, "")
		if err != nil || code != http.StatusOK {
			return false, fmt.Sprintf("GET service = %d %v %s", code, err, raw)
		}
		v = serviceView{}
		if json.Unmarshal(raw, &v) != nil {
			return false, "undecodable answer"
		}
		return stable(v, n)
	})
	return v
}

type nodeView struct {
	ID           string `json:"id"`
	Status       string `json:"status"`
	Health       string `json:"health"`
	HealthReason string `json:"health_reason"`
}

func (a *apiClient) nodeHealth(id string) (string, bool) {
	code, raw, err := a.try(http.MethodGet, "/v1/nodes/"+id, "")
	if err != nil || code != http.StatusOK {
		return "", false
	}
	var n nodeView
	if json.Unmarshal(raw, &n) != nil {
		return "", false
	}
	return n.Health, true
}

func (a *apiClient) waitNode(timeout time.Duration, id, health string) {
	a.t.Helper()
	eventually(a.t, timeout, fmt.Sprintf("node %s to be %s", id, health), func() (bool, string) {
		got, ok := a.nodeHealth(id)
		return ok && got == health, fmt.Sprintf("health %q (answer ok: %v)", got, ok)
	})
}

type eventView struct {
	ID         int64             `json:"id"`
	At         time.Time         `json:"at"`
	InstanceID string            `json:"instance_id"`
	Action     string            `json:"action"`
	Message    string            `json:"message"`
	Details    map[string]string `json:"details"`
}

// events returns the audit trail oldest first.
func (a *apiClient) events() []eventView {
	a.t.Helper()
	var all []eventView
	before := int64(0)
	for {
		path := "/v1/events?limit=1000"
		if before > 0 {
			path += fmt.Sprintf("&before=%d", before)
		}
		var page struct {
			Events []eventView `json:"events"`
		}
		a.must(http.MethodGet, path, "", http.StatusOK, &page)
		all = append(all, page.Events...)
		if len(page.Events) < 1000 {
			break
		}
		before = page.Events[len(page.Events)-1].ID
	}
	for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
		all[i], all[j] = all[j], all[i]
	}
	return all
}

func countAction(evs []eventView, action string) int {
	n := 0
	for _, e := range evs {
		if e.Action == action {
			n++
		}
	}
	return n
}

func serviceBody(replicas int, image string) string {
	return fmt.Sprintf(`{"replicas":%d,"spec":{"image":%q,"cpu_millis":500,"memory_bytes":134217728}}`, replicas, image)
}

func createBody(name string, replicas int, image string) string {
	return fmt.Sprintf(`{"name":%q,`, name) + strings.TrimPrefix(serviceBody(replicas, image), "{")
}

// getURL is a plain request without TLS, for the agent's loopback health endpoint.
func getURL(url string) (int, string) {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	return resp.StatusCode, string(raw)
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func poolFrom(t *testing.T, caPath string) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(readFile(t, caPath)) {
		t.Fatalf("%s holds no certificate", caPath)
	}
	return pool
}

func jsonUnmarshal(raw []byte, v any) error { return json.Unmarshal(raw, v) }
