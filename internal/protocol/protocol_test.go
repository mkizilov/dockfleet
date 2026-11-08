package protocol

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dockfleet/dockfleet/internal/store"
)

const serverToken = "server-token-0123456789"

func newTestClient(t *testing.T, clientToken string) (*Client, *store.Memory) {
	t.Helper()
	st := store.NewMemory()
	lis := bufconn.Listen(1 << 20)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := NewGRPCServer(NewNodeServer(st, 5*time.Second, logger), serverToken, nil)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	dialer := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	})
	c, err := Dial("passthrough:///bufnet", clientToken, nil, dialer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, st
}

func testNode() NodeInfo {
	return NodeInfo{ID: "node-1", Name: "Node 1", CPUMillis: 2000, MemoryBytes: 1 << 30, Labels: map[string]string{"zone": "a"}, AgentVersion: "test"}
}

func ctxWithTimeout(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestRegisterAndHeartbeat(t *testing.T) {
	c, st := newTestClient(t, serverToken)
	ctx := ctxWithTimeout(t)

	interval, err := c.RegisterNode(ctx, testNode())
	if err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	if interval != 5*time.Second {
		t.Errorf("interval = %s, want 5s", interval)
	}
	n, err := st.GetNode(ctx, "node-1")
	if err != nil || n.Name != "Node 1" || n.CPUMillis != 2000 || n.Labels["zone"] != "a" || n.AgentVersion != "test" {
		t.Fatalf("stored node = %+v, %v", n, err)
	}

	before := n.LastHeartbeat
	time.Sleep(5 * time.Millisecond)
	if err := c.Heartbeat(ctx, "node-1"); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	n, _ = st.GetNode(ctx, "node-1")
	if !n.LastHeartbeat.After(before) {
		t.Errorf("heartbeat did not advance LastHeartbeat: %s -> %s", before, n.LastHeartbeat)
	}
}

func TestHeartbeatUnknownNode(t *testing.T) {
	c, _ := newTestClient(t, serverToken)
	err := c.Heartbeat(ctxWithTimeout(t), "ghost")
	if !IsNotRegistered(err) {
		t.Fatalf("error = %v, want NotFound", err)
	}
}

func TestRejectsBadToken(t *testing.T) {
	for _, token := range []string{"wrong-token-0123456789", ""} {
		c, _ := newTestClient(t, token)
		_, err := c.RegisterNode(ctxWithTimeout(t), testNode())
		if status.Code(err) != codes.Unauthenticated {
			t.Errorf("token %q: code = %v, want Unauthenticated (err %v)", token, status.Code(err), err)
		}
	}
}

func TestRegisterValidation(t *testing.T) {
	c, _ := newTestClient(t, serverToken)
	tests := map[string]func(*NodeInfo){
		"bad id":       func(n *NodeInfo) { n.ID = "../etc" },
		"empty name":   func(n *NodeInfo) { n.Name = "" },
		"zero cpu":     func(n *NodeInfo) { n.CPUMillis = 0 },
		"negative mem": func(n *NodeInfo) { n.MemoryBytes = -1 },
		"long name":    func(n *NodeInfo) { n.Name = strings.Repeat("x", 256) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			n := testNode()
			mutate(&n)
			_, err := c.RegisterNode(ctxWithTimeout(t), n)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("code = %v, want InvalidArgument (err %v)", status.Code(err), err)
			}
		})
	}
}

func TestReRegisterKeepsRegisteredAt(t *testing.T) {
	c, st := newTestClient(t, serverToken)
	ctx := ctxWithTimeout(t)
	if _, err := c.RegisterNode(ctx, testNode()); err != nil {
		t.Fatal(err)
	}
	first, _ := st.GetNode(ctx, "node-1")
	time.Sleep(5 * time.Millisecond)
	if _, err := c.RegisterNode(ctx, testNode()); err != nil {
		t.Fatal(err)
	}
	second, _ := st.GetNode(ctx, "node-1")
	if !second.RegisteredAt.Equal(first.RegisteredAt) {
		t.Errorf("RegisteredAt changed on re-register: %s -> %s", first.RegisteredAt, second.RegisteredAt)
	}
	if !second.LastHeartbeat.After(first.LastHeartbeat) {
		t.Errorf("LastHeartbeat not refreshed on re-register")
	}
}
