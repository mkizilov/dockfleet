//go:build e2e

package e2e

import (
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// timing is the node-failure settings of a manager, scaled down so the tests
// finish in seconds. The proportions are the ones the manager insists on.
type timing struct {
	heartbeat, nodeTTL, lease, unavailable time.Duration
	// pass is the interval of the scheduling and reconciliation loops; zero is 200 ms.
	pass time.Duration
}

var (
	// A node is suspect after 0.8 s of silence, its agent fences itself after 0.6 s
	// without an accepted heartbeat, and it is given up on after 2 s.
	fastTiming = timing{heartbeat: 200 * time.Millisecond, nodeTTL: 800 * time.Millisecond, lease: 600 * time.Millisecond, unavailable: 2 * time.Second}
	// Heartbeats a good deal slower than the loops, as in a real deployment (10 s against 5 s):
	// after an outage of the database the loops run again well before the agents have been heard.
	slowHeartbeats = timing{heartbeat: time.Second, nodeTTL: 2 * time.Second, lease: 2 * time.Second, unavailable: 4 * time.Second, pass: 100 * time.Millisecond}
	// A node is given up on after 12 s: the agents' registration wait doubles from 1 s up to
	// 30 s, so after a manager restart an agent may take that long to come back; the real default
	// is 2 minutes for this reason.
	restartTiming = timing{heartbeat: 200 * time.Millisecond, nodeTTL: time.Second, lease: time.Second, unavailable: 12 * time.Second}
	// With several managers a failover has to fit in the lease: 2 x the election TTL (5 s, the
	// shortest allowed) plus a heartbeat.
	haTiming = timing{heartbeat: 250 * time.Millisecond, nodeTTL: 2 * time.Second, lease: 11 * time.Second, unavailable: 13 * time.Second}
)

type nodeSpec struct {
	id         string
	cpuMillis  int
	viaProxy   bool // the agent reaches the managers through the cuttable proxy
	csr        bool // its certificate is made with the signing-request flow
	unenrolled bool // the node gets a certificate but is not enrolled
}

type clusterOpts struct {
	managers   int
	ha         bool
	timing     timing
	nodes      []nodeSpec
	manualOnly bool // start no agent; the test starts them
}

type mgr struct {
	c      *cluster
	index  int
	id     string
	api    string
	grpc   string
	p      *proc
	client *apiClient
}

type agentProc struct {
	c      *cluster
	spec   nodeSpec
	health string
	p      *proc
	extra  []string
}

type cluster struct {
	t        *testing.T
	dir      string
	opts     clusterOpts
	db       string
	prefix   string
	pkiDir   string
	caPath   string
	roots    *x509.CertPool
	managers []*mgr
	agents   map[string]*agentProc
	proxy    *tcpProxy
	procs    []*proc
}

func (c *cluster) pki(args ...string) string {
	c.t.Helper()
	cmd := exec.Command(filepath.Join(binDir, "dockfleet-pki"), args...)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	if err != nil {
		c.t.Fatalf("dockfleet-pki %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func (c *cluster) agentCert(id string) (cert, key string) {
	return filepath.Join(c.pkiDir, "agents", id, "agent.pem"), filepath.Join(c.pkiDir, "agents", id, "agent.key")
}

func (c *cluster) issueAgent(n nodeSpec) {
	c.t.Helper()
	if !n.csr {
		c.pki("issue-agent", "-dir", c.pkiDir, "-node-id", n.id)
		return
	}
	// The worker makes its own key; only the request reaches the CA.
	workerDir := filepath.Join(c.dir, "worker-"+n.id)
	if err := os.MkdirAll(workerDir, 0o700); err != nil {
		c.t.Fatal(err)
	}
	c.pki("new-csr", "-node-id", n.id, "-out", workerDir)
	c.pki("sign-agent", "-dir", c.pkiDir, "-node-id", n.id, "-csr", filepath.Join(workerDir, "agent.csr"))
	cert, key := c.agentCert(n.id)
	if err := os.MkdirAll(filepath.Dir(cert), 0o700); err != nil {
		c.t.Fatal(err)
	}
	// The key is moved next to the certificate the way an administrator would place it.
	if err := os.Rename(filepath.Join(workerDir, "agent.key"), key); err != nil {
		c.t.Fatal(err)
	}
}

func (c *cluster) writeSecret(name, value string) string {
	c.t.Helper()
	p := filepath.Join(c.dir, "secrets", name)
	if fileExists(p) {
		return p
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		c.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(value+"\n"), 0o400); err != nil {
		c.t.Fatal(err)
	}
	return p
}

func baseEnv() []string {
	return []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "TMPDIR=" + os.TempDir()}
}

func newCluster(t *testing.T, opts clusterOpts) *cluster {
	t.Helper()
	if opts.managers == 0 {
		opts.managers = 1
	}
	if opts.timing == (timing{}) {
		opts.timing = fastTiming
	}
	if opts.ha && !haveEtcd {
		t.Skip("no etcd: set DOCKFLEET_TEST_ETCD_ENDPOINTS or install one under .tools/etcd")
	}
	dbURL := newDatabase(t)
	dir, err := os.MkdirTemp(workDir, "cluster-")
	if err != nil {
		t.Fatal(err)
	}
	c := &cluster{t: t, dir: dir, opts: opts, pkiDir: filepath.Join(dir, "pki"), agents: map[string]*agentProc{}, prefix: "/e2e/" + filepath.Base(dir)}
	c.db = strings.Replace(dbURL, "dockfleet@", "dockfleet:"+dbCanary+"@", 1)
	c.caPath = filepath.Join(c.pkiDir, "ca.pem")

	c.pki("init-ca", "-dir", c.pkiDir)
	c.pki("issue-manager", "-dir", c.pkiDir)
	c.roots = poolFrom(t, c.caPath)
	for _, n := range opts.nodes {
		c.issueAgent(n)
	}

	for i := 0; i < opts.managers; i++ {
		m := &mgr{c: c, index: i, id: fmt.Sprintf("manager-%d", i+1)}
		m.api = fmt.Sprintf("127.0.0.1:%d", portAPI0+2*i)
		m.grpc = fmt.Sprintf("127.0.0.1:%d", portAPI0+2*i+1)
		m.client = newAPIClient(t, m.api, c.roots, apiToken)
		c.managers = append(c.managers, m)
	}
	if hasProxied(opts.nodes) {
		c.proxy = newProxy(t, fmt.Sprintf("127.0.0.1:%d", portProxy0), c.managers[0].grpc)
	}
	t.Cleanup(c.shutdown)
	for _, m := range c.managers {
		m.start()
	}
	for _, m := range c.managers {
		m.waitReady(30 * time.Second)
	}
	for _, n := range opts.nodes {
		if !n.unenrolled {
			c.enroll(n.id)
		}
	}
	if !opts.manualOnly {
		for j, n := range opts.nodes {
			c.agents[n.id] = c.newAgent(n, j)
			c.agents[n.id].start()
		}
		for _, n := range opts.nodes {
			c.agents[n.id].waitReady(30 * time.Second)
		}
	}
	return c
}

func hasProxied(nodes []nodeSpec) bool {
	for _, n := range nodes {
		if n.viaProxy {
			return true
		}
	}
	return false
}

// shutdown stops everything the cluster started and, if the test failed, shows
// the end of every log.
func (c *cluster) shutdown() {
	if c.t.Failed() {
		for _, m := range c.managers {
			if m.p != nil {
				c.t.Logf("---- %s (last lines)\n%s", m.id, tailLines(m.p.logs(), 25))
			}
		}
		for id, a := range c.agents {
			if a.p != nil {
				c.t.Logf("---- agent %s (last lines)\n%s", id, tailLines(a.p.logs(), 20))
			}
		}
	}
	for _, a := range c.agents {
		if a.p != nil {
			a.p.terminate(5 * time.Second)
		}
	}
	for _, m := range c.managers {
		if m.p != nil {
			m.p.terminate(15 * time.Second)
		}
	}
}

func (c *cluster) enroll(id string) {
	c.t.Helper()
	body := fmt.Sprintf(`{"node_id":%q,"description":"e2e"}`, id)
	c.managers[0].client.must(http.MethodPost, "/v1/enrollments", body, http.StatusCreated, nil)
}

// ---- managers ----

func (m *mgr) env() []string {
	c, tm := m.c, m.c.opts.timing
	pass := tm.pass
	if pass == 0 {
		pass = 200 * time.Millisecond
	}
	env := append(baseEnv(),
		"DOCKFLEET_INSTANCE_ID="+m.id,
		"DOCKFLEET_LOG_FORMAT=json", "DOCKFLEET_LOG_LEVEL=info",
		"DOCKFLEET_API_ADDR="+m.api, "DOCKFLEET_GRPC_ADDR="+m.grpc,
		"DOCKFLEET_API_TOKEN_FILE="+c.writeSecret("api-token", apiToken),
		"DOCKFLEET_DATABASE_URL_FILE="+c.writeSecret("database-url", c.db),
		"DOCKFLEET_TLS_CERT_FILE="+filepath.Join(c.pkiDir, "manager.pem"),
		"DOCKFLEET_TLS_KEY_FILE="+filepath.Join(c.pkiDir, "manager.key"),
		"DOCKFLEET_TLS_CLIENT_CA_FILE="+c.caPath,
		"DOCKFLEET_HEARTBEAT_INTERVAL="+tm.heartbeat.String(),
		"DOCKFLEET_NODE_TTL="+tm.nodeTTL.String(),
		"DOCKFLEET_NODE_LEASE="+tm.lease.String(),
		"DOCKFLEET_NODE_UNAVAILABLE_AFTER="+tm.unavailable.String(),
		"DOCKFLEET_SCHEDULER_INTERVAL="+pass.String(), "DOCKFLEET_RECONCILE_INTERVAL="+pass.String(),
		"DOCKFLEET_SHUTDOWN_TIMEOUT=5s",
	)
	if c.opts.ha {
		env = append(env,
			"DOCKFLEET_ETCD_ENDPOINTS="+etcdURL, "DOCKFLEET_ETCD_PREFIX="+c.prefix, "DOCKFLEET_ELECTION_TTL=5s")
	}
	return env
}

func (m *mgr) start() {
	m.c.t.Helper()
	p, err := start(m.id, filepath.Join(binDir, "dockfleet-manager"), nil, m.env(), filepath.Join(m.c.dir, m.id+".log"))
	if err != nil {
		m.c.t.Fatal(err)
	}
	m.p = p
}

func (m *mgr) waitReady(timeout time.Duration) {
	m.c.t.Helper()
	eventually(m.c.t, timeout, m.id+" to be ready", func() (bool, string) {
		if !m.p.running() {
			return false, "the process exited:\n" + tailLines(m.p.logs(), 15)
		}
		code, raw, err := m.client.try(http.MethodGet, "/readyz", "")
		return err == nil && code == http.StatusOK, fmt.Sprintf("GET /readyz = %d %v %s", code, err, raw)
	})
}

// healthcheck runs the binary's own `healthcheck` subcommand, as the image's
// HEALTHCHECK does, with the environment of the process, and returns its exit code.
func (m *mgr) healthcheck() (int, string) {
	cmd := exec.Command(filepath.Join(binDir, "dockfleet-manager"), "healthcheck")
	cmd.Env = m.env()
	out, err := cmd.CombinedOutput()
	if err == nil {
		return 0, string(out)
	}
	if ee, ok := err.(*exec.ExitError); ok {
		return ee.ExitCode(), string(out)
	}
	return -1, err.Error()
}

type clusterView struct {
	Mode         string `json:"mode"`
	InstanceID   string `json:"instance_id"`
	Role         string `json:"role"`
	Leader       string `json:"leader"`
	Epoch        int64  `json:"epoch"`
	ControlPlane struct {
		Epoch  int64  `json:"epoch"`
		Leader string `json:"leader"`
	} `json:"control_plane"`
}

func (m *mgr) cluster() (clusterView, bool) {
	code, raw, err := m.client.try(http.MethodGet, "/v1/cluster", "")
	var v clusterView
	if err != nil || code != http.StatusOK || json.Unmarshal(raw, &v) != nil {
		return v, false
	}
	return v, true
}

// leader returns the running manager that leads under the epoch, waiting for it.
func (c *cluster) waitLeader(timeout time.Duration, epoch int64) *mgr {
	c.t.Helper()
	var leader *mgr
	eventually(c.t, timeout, fmt.Sprintf("a manager to lead under epoch %d", epoch), func() (bool, string) {
		leader = nil
		var seen []string
		for _, m := range c.managers {
			if m.p == nil || !m.p.running() {
				continue
			}
			v, ok := m.cluster()
			seen = append(seen, fmt.Sprintf("%s:%s/%d/ok=%v", m.id, v.Role, v.Epoch, ok))
			if ok && v.Role == "leader" && v.Epoch == epoch {
				if leader != nil {
					return false, "two leaders: " + strings.Join(seen, " ")
				}
				leader = m
			}
		}
		return leader != nil, strings.Join(seen, " ")
	})
	return leader
}

// ---- agents ----

func (c *cluster) newAgent(n nodeSpec, index int) *agentProc {
	return &agentProc{c: c, spec: n, health: fmt.Sprintf("127.0.0.1:%d", portHealth0+index)}
}

func (a *agentProc) env() []string {
	c := a.c
	addrs := make([]string, 0, len(c.managers))
	for _, m := range c.managers {
		addrs = append(addrs, m.grpc)
	}
	if a.spec.viaProxy {
		addrs = []string{fmt.Sprintf("127.0.0.1:%d", portProxy0)}
	}
	cert, key := c.agentCert(a.spec.id)
	cpu := a.spec.cpuMillis
	if cpu == 0 {
		cpu = 4000
	}
	env := append(baseEnv(),
		"DOCKFLEET_NODE_ID="+a.spec.id,
		"DOCKFLEET_LOG_FORMAT=json", "DOCKFLEET_LOG_LEVEL=info",
		"DOCKFLEET_MANAGER_ADDRS="+strings.Join(addrs, ","),
		"DOCKFLEET_TLS_CA_FILE="+c.caPath, "DOCKFLEET_TLS_CERT_FILE="+cert, "DOCKFLEET_TLS_KEY_FILE="+key,
		fmt.Sprintf("DOCKFLEET_NODE_CPU_MILLIS=%d", cpu), "DOCKFLEET_NODE_MEMORY_MB=1024",
		"DOCKFLEET_HEALTH_ADDR="+a.health, "DOCKFLEET_RPC_TIMEOUT=1s",
	)
	return append(env, a.extra...)
}

func (a *agentProc) start() {
	a.c.t.Helper()
	p, err := start("agent-"+a.spec.id, filepath.Join(binDir, "dockfleet-agent"), nil, a.env(), filepath.Join(a.c.dir, "agent-"+a.spec.id+".log"))
	if err != nil {
		a.c.t.Fatal(err)
	}
	a.p = p
}

func (a *agentProc) readyz() (int, string) { return getURL("http://" + a.health + "/readyz") }

func (a *agentProc) waitReady(timeout time.Duration) {
	a.c.t.Helper()
	eventually(a.c.t, timeout, "agent "+a.spec.id+" to be ready", func() (bool, string) {
		if !a.p.running() {
			return false, "the process exited:\n" + tailLines(a.p.logs(), 15)
		}
		code, body := a.readyz()
		return code == http.StatusOK, fmt.Sprintf("GET /readyz = %d %s", code, body)
	})
}

func (a *agentProc) waitNotReady(timeout time.Duration) {
	a.c.t.Helper()
	eventually(a.c.t, timeout, "agent "+a.spec.id+" to be unready", func() (bool, string) {
		code, body := a.readyz()
		return code == http.StatusServiceUnavailable, fmt.Sprintf("GET /readyz = %d %s", code, body)
	})
}

// healthcheck runs `dockfleet-agent healthcheck` with the agent's environment.
func (a *agentProc) healthcheck() (int, string) {
	cmd := exec.Command(filepath.Join(binDir, "dockfleet-agent"), "healthcheck")
	cmd.Env = a.env()
	out, err := cmd.CombinedOutput()
	if err == nil {
		return 0, string(out)
	}
	if ee, ok := err.(*exec.ExitError); ok {
		return ee.ExitCode(), string(out)
	}
	return -1, err.Error()
}

// crash is a SIGKILL: the process, and the containers it kept, are gone.
func (a *agentProc) crash() { a.p.kill() }

// freeze is SIGSTOP: the process exists and does nothing, as when its machine hangs.
func (a *agentProc) freeze()   { a.p.signal(syscall.SIGSTOP) }
func (a *agentProc) unfreeze() { a.p.signal(syscall.SIGCONT) }

// logTime returns the time of the first log line of the process containing msg.
func logTime(logs, msg string) (time.Time, bool) {
	for _, line := range strings.Split(logs, "\n") {
		if !strings.Contains(line, msg) {
			continue
		}
		var rec struct {
			Time time.Time `json:"time"`
		}
		if json.Unmarshal([]byte(line), &rec) == nil && !rec.Time.IsZero() {
			return rec.Time, true
		}
	}
	return time.Time{}, false
}
