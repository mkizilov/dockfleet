package manager

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dockfleet/dockfleet/internal/config"
	"github.com/dockfleet/dockfleet/internal/election"
	"github.com/dockfleet/dockfleet/internal/logstream"
	"github.com/dockfleet/dockfleet/internal/pki"
	"github.com/dockfleet/dockfleet/internal/protocol"
	"github.com/dockfleet/dockfleet/internal/store"
)

// Loopback ports come from the project's reserved range 21200-21299.
const (
	testAPIAddr  = "127.0.0.1:21290"
	testGRPCAddr = "127.0.0.1:21291"
	// Used only by the non-loopback refusal test, which never serves on them.
	testRefusedAPIAddr  = "127.0.0.1:21292"
	testRefusedGRPCAddr = "127.0.0.1:21293"
	// Used by the mutual TLS test.
	testTLSAPIAddr  = "127.0.0.1:21294"
	testTLSGRPCAddr = "127.0.0.1:21295"
	// Used by the container log test.
	testLogsAPIAddr  = "127.0.0.1:21298"
	testLogsGRPCAddr = "127.0.0.1:21299"
	// Used by the metrics and dashboard test.
	testObserveAPIAddr  = "127.0.0.1:21288"
	testObserveGRPCAddr = "127.0.0.1:21289"
)

func testConfig() config.Manager {
	return config.Manager{
		APIToken:          "api-token-0123456789",
		AgentToken:        "agent-token-0123456789",
		HeartbeatInterval: time.Second,
		NodeTTL:           10 * time.Second,
		RequestTimeout:    5 * time.Second,
		ShutdownTimeout:   5 * time.Second,
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestServeRegistersNodeAndShutsDownGracefully(t *testing.T) {
	cfg := testConfig()
	st := store.NewMemory()
	if _, err := st.CreateEnrollment(context.Background(), store.Enrollment{NodeID: "node-1", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	m, err := New(cfg, st, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	apiLn, err := net.Listen("tcp", testAPIAddr)
	if err != nil {
		t.Fatal(err)
	}
	grpcLn, err := net.Listen("tcp", testGRPCAddr)
	if err != nil {
		_ = apiLn.Close()
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- m.Serve(ctx, apiLn, grpcLn) }()

	client, err := protocol.Dial(protocol.ClientConfig{Addr: testGRPCAddr, Token: cfg.AgentToken})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
	defer callCancel()
	session, err := client.Register(callCtx, protocol.NodeInfo{ID: "node-1", Name: "Node 1", CPUMillis: 1000, MemoryBytes: 1 << 30})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if session.HeartbeatInterval != time.Second {
		t.Errorf("heartbeat interval = %s, want 1s", session.HeartbeatInterval)
	}
	if sessions := m.Sessions(); len(sessions) != 1 || sessions[0].ID != session.ID {
		t.Fatalf("Sessions = %+v, want the registered session", sessions)
	}

	req, err := http.NewRequestWithContext(callCtx, http.MethodGet, "http://"+testAPIAddr+"/v1/nodes", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+cfg.APIToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /v1/nodes: %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		Nodes []struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"nodes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || len(body.Nodes) != 1 || body.Nodes[0].ID != "node-1" || body.Nodes[0].Status != "ready" {
		t.Fatalf("unexpected API response: %d %+v", resp.StatusCode, body)
	}

	cancel()
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("Serve returned %v after graceful shutdown", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after context cancellation")
	}
}

// An agent's log stream reaches the administrator's HTTP response, both need
// credentials, and a stream that is still open does not make the shutdown wait
// for its timeout.
func TestServeStreamsContainerLogsAndShutsDownWithAnOpenStream(t *testing.T) {
	cfg := testConfig()
	cfg.ShutdownTimeout = time.Minute
	st := store.NewMemory()
	bg := context.Background()
	if _, err := st.CreateEnrollment(bg, store.Enrollment{NodeID: "node-1", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	m, err := New(cfg, st, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	apiLn, err := net.Listen("tcp", testLogsAPIAddr)
	if err != nil {
		t.Fatal(err)
	}
	grpcLn, err := net.Listen("tcp", testLogsGRPCAddr)
	if err != nil {
		_ = apiLn.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- m.Serve(ctx, apiLn, grpcLn) }()

	client, err := protocol.Dial(protocol.ClientConfig{Addr: testLogsGRPCAddr, Token: cfg.AgentToken})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	callCtx, callCancel := context.WithTimeout(ctx, 20*time.Second)
	defer callCancel()
	// The administrator's requests outlive the manager's context: the test ends
	// the manager while a response is open and must still read the end of it.
	httpCtx, httpCancel := context.WithTimeout(bg, 30*time.Second)
	defer httpCancel()
	session, err := client.Register(callCtx, protocol.NodeInfo{ID: "node-1", Name: "Node 1", CPUMillis: 1000, MemoryBytes: 1 << 30})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	// A workload can only be placed on a node that has registered.
	if _, err := st.CreateWorkload(bg, store.Workload{ID: "w1", Name: "web", Image: "nginx", CPUMillis: 1, MemoryBytes: 1, NodeID: "node-1", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	conn, err := client.Logs(callCtx, "node-1", session.ID)
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	// The agent: every request is answered with one line.
	go func() {
		for {
			d, err := conn.Recv()
			if err != nil {
				return
			}
			if d.Open != nil {
				_ = conn.Send(logstream.Up{Batch: &logstream.Batch{RequestID: d.Open.RequestID, HistoryComplete: true, Lines: []logstream.Line{
					{Time: time.Now().UTC(), Stream: logstream.StreamStdout, Text: "hello from w1"},
				}}})
			}
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for !m.logs.Connected("node-1") {
		if time.Now().After(deadline) {
			t.Fatal("the agent's log stream never attached")
		}
		time.Sleep(time.Millisecond)
	}

	get := func(token string) *http.Response {
		req, err := http.NewRequestWithContext(httpCtx, http.MethodGet, "http://"+testLogsAPIAddr+"/v1/workloads/w1/logs?follow=true&tail=5", nil)
		if err != nil {
			t.Fatal(err)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	for name, token := range map[string]string{"no credentials": "", "the agents' token": cfg.AgentToken} {
		resp := get(token)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s: status = %d, want 401", name, resp.StatusCode)
		}
	}

	resp := get(cfg.APIToken)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/x-ndjson") {
		t.Fatalf("status %d, content type %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	body := bufio.NewReader(resp.Body)
	line, err := body.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	var rec struct {
		Type, Text, Workload, Node, Time string
	}
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("%q: %v", line, err)
	}
	if !strings.EqualFold(rec.Type, "line") || rec.Text != "hello from w1" || rec.Workload != "w1" || rec.Node != "node-1" || rec.Time == "" {
		t.Fatalf("first record = %q", line)
	}

	// Shutting down ends the open response with an end record, promptly.
	cancel()
	last := line
	for {
		next, err := body.ReadString('\n')
		if next != "" {
			last = next
		}
		if err != nil {
			break
		}
	}
	if !strings.Contains(last, `"type":"end"`) {
		t.Fatalf("last record = %q, want an end record", last)
	}
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("Serve returned %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Serve waited for its shutdown timeout because a log stream was open")
	}
}

// With metrics and the dashboard on, a running manager exports what it does
// (agent calls, scheduling and reconciliation passes, the fleet) and shows the
// fleet in the dashboard, and both ask for the administrator's token.
func TestServeExportsMetricsAndServesTheDashboard(t *testing.T) {
	cfg := testConfig()
	cfg.MetricsEnabled, cfg.DashboardEnabled = true, true
	cfg.SchedulerInterval, cfg.ReconcileInterval = 20*time.Millisecond, 20*time.Millisecond
	cfg.InstanceID = "observe-test"
	bg := context.Background()
	st := store.NewMemory()
	if _, err := st.CreateEnrollment(bg, store.Enrollment{NodeID: "node-1", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateService(bg, store.NewService{
		ID: "svc-web", Name: "web", Replicas: 1, CreatedBy: "test", At: time.Now(),
		Spec: store.ServiceSpec{Image: "nginx:1.27", CPUMillis: 500, MemoryBytes: 64 << 20},
	}); err != nil {
		t.Fatal(err)
	}
	m, err := New(cfg, st, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	apiLn, err := net.Listen("tcp", testObserveAPIAddr)
	if err != nil {
		t.Fatal(err)
	}
	grpcLn, err := net.Listen("tcp", testObserveGRPCAddr)
	if err != nil {
		_ = apiLn.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- m.Serve(ctx, apiLn, grpcLn) }()

	client, err := protocol.Dial(protocol.ClientConfig{Addr: testObserveGRPCAddr, Token: cfg.AgentToken})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	callCtx, callCancel := context.WithTimeout(ctx, 20*time.Second)
	defer callCancel()
	if _, err := client.Register(callCtx, protocol.NodeInfo{ID: "node-1", Name: "Node 1", CPUMillis: 1000, MemoryBytes: 1 << 30}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// A browser would see the redirects; so does this client.
	httpClient := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Timeout: 10 * time.Second}
	do := func(method, path, token string, form url.Values) (*http.Response, string) {
		t.Helper()
		var body io.Reader
		if form != nil {
			body = strings.NewReader(form.Encode())
		}
		req, err := http.NewRequestWithContext(callCtx, method, "http://"+testObserveAPIAddr+path, body)
		if err != nil {
			t.Fatal(err)
		}
		if form != nil {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp, string(raw)
	}

	// Access control: the metrics and the pages are for the administrator.
	for name, token := range map[string]string{"no credentials": "", "the agents' token": cfg.AgentToken, "a wrong token": "wrong-token-0123456789"} {
		resp, body := do(http.MethodGet, "/metrics", token, nil)
		if resp.StatusCode != http.StatusUnauthorized || strings.Contains(body, "dockfleet_") {
			t.Errorf("/metrics with %s = %d: %s", name, resp.StatusCode, body)
		}
		resp, body = do(http.MethodGet, "/dashboard", token, nil)
		switch {
		case token == "" && (resp.StatusCode != http.StatusSeeOther || !strings.HasPrefix(resp.Header.Get("Location"), "/dashboard/login")):
			t.Errorf("/dashboard with %s = %d, Location %q; want the redirect to the login page", name, resp.StatusCode, resp.Header.Get("Location"))
		case token != "" && resp.StatusCode != http.StatusUnauthorized:
			t.Errorf("/dashboard with %s = %d, want 401", name, resp.StatusCode)
		}
		if strings.Contains(body, "node-1") || strings.Contains(body, "nginx") {
			t.Errorf("/dashboard with %s leaked cluster data: %s", name, body)
		}
	}

	// What the manager does shows up in the metrics.
	scrape := func() map[string]string {
		t.Helper()
		resp, body := do(http.MethodGet, "/metrics", cfg.APIToken, nil)
		if resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/plain; version=0.0.4") {
			t.Fatalf("/metrics = %d %q: %s", resp.StatusCode, resp.Header.Get("Content-Type"), body)
		}
		series := map[string]string{}
		for _, line := range strings.Split(body, "\n") {
			if i := strings.LastIndex(line, " "); i > 0 && !strings.HasPrefix(line, "#") {
				series[line[:i]] = line[i+1:]
			}
		}
		return series
	}
	deadline := time.Now().Add(15 * time.Second)
	var series map[string]string
	for {
		series = scrape()
		if series[`dockfleet_scheduling_decisions_total{result="placed"}`] == "1" && series[`dockfleet_reconcile_passes_total{result="ok"}`] > "0" &&
			series[`dockfleet_service_replicas{service="web",state="starting"}`] == "1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the replica was not placed, or the loops did not report their passes: %v", series)
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, tt := range []struct{ series, want string }{
		{`dockfleet_build_info{version="dev"}`, "1"},
		{`dockfleet_nodes{health="healthy"}`, "1"},
		{`dockfleet_node_healthy{node="node-1"}`, "1"},
		{`dockfleet_node_cpu_millicores{node="node-1",kind="capacity"}`, "1000"},
		{`dockfleet_node_cpu_millicores{node="node-1",kind="allocated"}`, "500"},
		{`dockfleet_node_allocation_ratio{node="node-1",resource="cpu"}`, "0.5"},
		{`dockfleet_service_replicas_desired{service="web"}`, "1"},
		{`dockfleet_services`, "1"},
		{`dockfleet_scheduling_errors_total`, "0"},
		{`dockfleet_reconcile_errors_total`, "0"},
		{`dockfleet_grpc_request_duration_seconds_count{method="/dockfleet.v1.NodeService/RegisterNode",code="OK"}`, "1"},
		{`dockfleet_http_request_duration_seconds_count{method="GET",route="/metrics",code="401"}`, "3"},
	} {
		if got := series[tt.series]; got != tt.want {
			t.Errorf("%s = %q, want %q", tt.series, got, tt.want)
		}
	}

	// The dashboard shows the same fleet, to the administrator.
	resp, page := do(http.MethodGet, "/dashboard", cfg.APIToken, nil)
	if resp.StatusCode != http.StatusOK || !strings.Contains(page, "node-1") || !strings.Contains(page, "/dashboard/services/web") || !strings.Contains(page, "replica.scheduled") {
		t.Fatalf("/dashboard = %d: %s", resp.StatusCode, page)
	}
	if resp, page = do(http.MethodGet, "/dashboard/services/web", cfg.APIToken, nil); resp.StatusCode != http.StatusOK || !strings.Contains(page, "nginx:1.27") {
		t.Fatalf("/dashboard/services/web = %d: %s", resp.StatusCode, page)
	}
	if resp, _ = do(http.MethodGet, "/dashboard/login", "", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("the login page = %d", resp.StatusCode)
	}

	// A login is an administrative act: it is in the audit trail.
	if resp, _ = do(http.MethodPost, "/dashboard/login", "", url.Values{"token": {cfg.APIToken}}); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("login = %d", resp.StatusCode)
	}
	login, err := st.ListEvents(bg, store.EventFilter{ResourceType: "dashboard"})
	if err != nil {
		t.Fatal(err)
	}
	if len(login) != 1 || login[0].Action != "dashboard.login" || login[0].Actor != "admin" || login[0].InstanceID != "observe-test" || login[0].Details["remote"] != "127.0.0.1" {
		t.Fatalf("dashboard audit events = %+v", login)
	}

	// The rest of the API is where it was, and the probes stay open.
	if resp, body := do(http.MethodGet, "/v1/nodes", cfg.APIToken, nil); resp.StatusCode != http.StatusOK || !strings.Contains(body, "node-1") {
		t.Errorf("/v1/nodes = %d: %s", resp.StatusCode, body)
	}
	if resp, _ := do(http.MethodGet, "/v1/nodes", "", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("/v1/nodes without credentials = %d", resp.StatusCode)
	}
	// Without etcd this manager is the only one: it says so, and leads by itself.
	if resp, body := do(http.MethodGet, "/v1/cluster", cfg.APIToken, nil); resp.StatusCode != http.StatusOK || !strings.Contains(body, `"mode":"single"`) || !strings.Contains(body, `"role":"single"`) {
		t.Errorf("/v1/cluster = %d: %s", resp.StatusCode, body)
	}
	if st := m.Leadership(); st.Role != election.Leader || st.ID != "observe-test" {
		t.Errorf("a manager without an election has leadership %+v", st)
	}
	if resp, _ := do(http.MethodGet, "/healthz", "", nil); resp.StatusCode != http.StatusOK {
		t.Errorf("/healthz = %d", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("Serve returned %v after graceful shutdown", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after context cancellation")
	}
}

// A cluster that is configured but has no way to elect is a mistake, not a manager
// that quietly runs as the only one: two of them would both lead.
func TestHighAvailabilityNeedsAnElectionBackend(t *testing.T) {
	cfg := testConfig()
	cfg.EtcdEndpoints = []string{"http://127.0.0.1:2379"}
	if _, err := New(cfg, store.NewMemory(), discardLogger()); err == nil || !strings.Contains(err.Error(), "no election backend") {
		t.Fatalf("New with endpoints and no backend = %v, want it refused", err)
	}
	if _, err := New(testConfig(), store.NewMemory(), discardLogger()); err != nil {
		t.Fatalf("a single manager is built without one: %v", err)
	}
}

// Both are switches: off, the endpoints do not exist, and the rest of the API is unchanged.
func TestMetricsAndTheDashboardCanBeSwitchedOff(t *testing.T) {
	call := func(cfg config.Manager, method, path string) *httptest.ResponseRecorder {
		t.Helper()
		m, err := New(cfg, store.NewMemory(), discardLogger())
		if err != nil {
			t.Fatal(err)
		}
		h, err := m.httpHandler()
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(method, path, nil)
		req.Header.Set("Authorization", "Bearer "+cfg.APIToken)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	off := testConfig()
	for path, want := range map[string]int{"/metrics": http.StatusNotFound, "/dashboard": http.StatusNotFound, "/dashboard/login": http.StatusNotFound, "/v1/nodes": http.StatusOK} {
		if rec := call(off, http.MethodGet, path); rec.Code != want {
			t.Errorf("%s with both off = %d, want %d", path, rec.Code, want)
		}
	}

	metricsOnly := testConfig()
	metricsOnly.MetricsEnabled = true
	if rec := call(metricsOnly, http.MethodGet, "/metrics"); rec.Code != http.StatusOK {
		t.Errorf("/metrics with metrics on = %d", rec.Code)
	}
	if rec := call(metricsOnly, http.MethodGet, "/dashboard"); rec.Code != http.StatusNotFound {
		t.Errorf("/dashboard with the dashboard off = %d, want 404", rec.Code)
	}

	dashboardOnly := testConfig()
	dashboardOnly.DashboardEnabled = true
	if rec := call(dashboardOnly, http.MethodGet, "/dashboard"); rec.Code != http.StatusOK {
		t.Errorf("/dashboard with the dashboard on = %d", rec.Code)
	}
	if rec := call(dashboardOnly, http.MethodGet, "/metrics"); rec.Code != http.StatusNotFound {
		t.Errorf("/metrics with metrics off = %d, want 404", rec.Code)
	}

	// A token too short to be a credential cannot be turned into a dashboard login.
	short := testConfig()
	short.DashboardEnabled, short.APIToken = true, "short"
	m, err := New(short, store.NewMemory(), discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.httpHandler(); err == nil {
		t.Error("a dashboard was built on a token too short to be a credential")
	}
}

type remoteListener struct {
	net.Listener
}

func (remoteListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("203.0.113.7"), Port: 21281}
}

func TestServeRefusesPlaintextOnNonLoopbackListeners(t *testing.T) {
	m, err := New(testConfig(), store.NewMemory(), discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	apiLn, err := net.Listen("tcp", testRefusedAPIAddr)
	if err != nil {
		t.Fatal(err)
	}
	grpcLn, err := net.Listen("tcp", testRefusedGRPCAddr)
	if err != nil {
		_ = apiLn.Close()
		t.Fatal(err)
	}

	err = m.Serve(context.Background(), apiLn, remoteListener{grpcLn})
	if err == nil {
		t.Fatal("Serve must refuse a non-loopback gRPC listener without TLS")
	}
	// Serve owns the listeners and must have closed both.
	if c, err := net.DialTimeout("tcp", testRefusedAPIAddr, 200*time.Millisecond); err == nil {
		_ = c.Close()
		t.Error("API listener was left open")
	}
	if c, err := net.DialTimeout("tcp", testRefusedGRPCAddr, 200*time.Millisecond); err == nil {
		_ = c.Close()
		t.Error("gRPC listener was left open")
	}
}

func writeIssued(t *testing.T, issued *pki.Issued, dir, name string) (certFile, keyFile string) {
	t.Helper()
	certFile, keyFile = filepath.Join(dir, name+".pem"), filepath.Join(dir, name+".key")
	if err := issued.Save(certFile, keyFile, pki.WriteOptions{}); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

func TestServeMutualTLSEndToEnd(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	ca, err := pki.NewCA("Test CA", now.Add(-time.Hour), now.Add(48*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	caFile := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caFile, ca.CertPEM(), 0o644); err != nil {
		t.Fatal(err)
	}
	server, err := ca.IssueServer(pki.ServerRequest{Name: "manager", DNSNames: []string{"localhost"}, IPs: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(24 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	agentIssued, err := ca.IssueNode(pki.NodeRequest{NodeID: "node-tls", NotBefore: now.Add(-time.Minute), NotAfter: now.Add(24 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	serverCert, serverKey := writeIssued(t, server, dir, "manager")
	agentCert, agentKey := writeIssued(t, agentIssued, dir, "agent")

	cfg := testConfig()
	cfg.AgentToken = ""
	cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSClientCAFile = serverCert, serverKey, caFile
	m, err := New(cfg, store.NewMemory(), discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	apiLn, err := net.Listen("tcp", testTLSAPIAddr)
	if err != nil {
		t.Fatal(err)
	}
	grpcLn, err := net.Listen("tcp", testTLSGRPCAddr)
	if err != nil {
		_ = apiLn.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- m.Serve(ctx, apiLn, grpcLn) }()

	clientTLS, err := pki.ClientTLS(pki.ClientConfig{CAFile: caFile, CertFile: agentCert, KeyFile: agentKey, ServerName: "localhost"})
	if err != nil {
		t.Fatal(err)
	}
	client, err := protocol.Dial(protocol.ClientConfig{Addr: testTLSGRPCAddr, TLS: clientTLS})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	callCtx, callCancel := context.WithTimeout(ctx, 10*time.Second)
	defer callCancel()
	node := protocol.NodeInfo{ID: "node-tls", Name: "TLS node", CPUMillis: 1000, MemoryBytes: 1 << 30}

	// A valid certificate alone is not enough: the node has to be enrolled.
	if _, err := client.Register(callCtx, node); !protocol.IsNotEnrolled(err) {
		t.Fatalf("Register before enrollment: error = %v, want NOT_ENROLLED", err)
	}

	// The admin API serves HTTPS with the manager certificate and needs no client certificate.
	roots, err := pki.LoadPool(caFile)
	if err != nil {
		t.Fatal(err)
	}
	httpClient := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13}}}
	defer httpClient.CloseIdleConnections()
	req, err := http.NewRequestWithContext(callCtx, http.MethodPost, "https://"+testTLSAPIAddr+"/v1/enrollments", strings.NewReader(`{"node_id":"node-tls"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+cfg.APIToken)
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("enroll over HTTPS: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("enrollment status = %d, want 201", resp.StatusCode)
	}

	session, err := client.Register(callCtx, node)
	if err != nil {
		t.Fatalf("Register after enrollment: %v", err)
	}
	if sessions := m.Sessions(); len(sessions) != 1 || sessions[0].ID != session.ID {
		t.Fatalf("Sessions = %+v", sessions)
	}

	// A plaintext client gets nowhere on the TLS port, even on loopback.
	plain, err := protocol.Dial(protocol.ClientConfig{Addr: testTLSGRPCAddr, Token: "anything-at-all-0123456789"})
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	if _, err := plain.Heartbeat(callCtx, "node-tls", session.ID, nil); status.Code(err) != codes.Unavailable {
		t.Fatalf("plaintext heartbeat: error = %v, want Unavailable", err)
	}

	cancel()
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("Serve returned %v after graceful shutdown", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after context cancellation")
	}
}

func TestNewFailsOnMissingTLSFiles(t *testing.T) {
	cfg := config.Manager{TLSCertFile: "/nonexistent/cert.pem", TLSKeyFile: "/nonexistent/key.pem"}
	if _, err := New(cfg, store.NewMemory(), discardLogger()); err == nil {
		t.Fatal("expected error for unreadable TLS files")
	}
}
