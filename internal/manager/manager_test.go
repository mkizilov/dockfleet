package manager

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/config"
	"github.com/dockfleet/dockfleet/internal/protocol"
	"github.com/dockfleet/dockfleet/internal/store"
)

// Loopback ports come from the project's reserved range 21200-21299.
const (
	testAPIAddr  = "127.0.0.1:21290"
	testGRPCAddr = "127.0.0.1:21291"
)

func TestServeRegistersNodeAndShutsDownGracefully(t *testing.T) {
	cfg := config.Manager{
		APIToken:          "api-token-0123456789",
		AgentToken:        "agent-token-0123456789",
		HeartbeatInterval: time.Second,
		NodeTTL:           10 * time.Second,
		ShutdownTimeout:   5 * time.Second,
	}
	m, err := New(cfg, store.NewMemory(), slog.New(slog.NewTextHandler(io.Discard, nil)))
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

	client, err := protocol.Dial(testGRPCAddr, cfg.AgentToken, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
	defer callCancel()
	interval, err := client.RegisterNode(callCtx, protocol.NodeInfo{ID: "node-1", Name: "Node 1", CPUMillis: 1000, MemoryBytes: 1 << 30})
	if err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	if interval != time.Second {
		t.Errorf("heartbeat interval = %s, want 1s", interval)
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

func TestNewFailsOnMissingTLSFiles(t *testing.T) {
	cfg := config.Manager{TLSCertFile: "/nonexistent/cert.pem", TLSKeyFile: "/nonexistent/key.pem"}
	if _, err := New(cfg, store.NewMemory(), slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("expected error for unreadable TLS files")
	}
}
