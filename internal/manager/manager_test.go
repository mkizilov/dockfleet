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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dockfleet/dockfleet/internal/config"
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
	if _, err := st.CreateWorkload(bg, store.Workload{ID: "w1", Name: "web", Image: "nginx", CPUMillis: 1, MemoryBytes: 1, NodeID: "node-1", CreatedAt: time.Now()}); err != nil {
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
	session, err := client.Register(callCtx, protocol.NodeInfo{ID: "node-1", Name: "Node 1", CPUMillis: 1000, MemoryBytes: 1 << 30})
	if err != nil {
		t.Fatalf("Register: %v", err)
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
		req, err := http.NewRequestWithContext(callCtx, http.MethodGet, "http://"+testLogsAPIAddr+"/v1/workloads/w1/logs?follow=true&tail=5", nil)
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
