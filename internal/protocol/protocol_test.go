package protocol

import (
	"context"
	"errors"
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

	dockfleetv1 "github.com/dockfleet/dockfleet/internal/protocol/dockfleet/v1"
	"github.com/dockfleet/dockfleet/internal/registry"
	"github.com/dockfleet/dockfleet/internal/store"
)

const serverToken = "server-token-0123456789"

type testEnv struct {
	store *store.Memory
	lis   *bufconn.Listener
}

func newEnv(t *testing.T, mutate func(*NodeServer)) *testEnv {
	t.Helper()
	st := store.NewMemory()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	node := NewNodeServer(registry.New(st, logger), 5*time.Second, logger)
	if mutate != nil {
		mutate(node)
	}
	lis := bufconn.Listen(1 << 20)
	srv := NewGRPCServer(node, ServerOptions{Token: serverToken, HandlerTimeout: 2 * time.Second})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return &testEnv{store: st, lis: lis}
}

func (e *testEnv) client(t *testing.T, token string, mutate func(*ClientConfig)) *Client {
	t.Helper()
	cfg := ClientConfig{
		Addr:  "passthrough:///bufnet",
		Token: token,
		Dialer: func(ctx context.Context, _ string) (net.Conn, error) {
			return e.lis.DialContext(ctx)
		},
	}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := Dial(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
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

func TestRegisterHeartbeatDeregister(t *testing.T) {
	env := newEnv(t, nil)
	c := env.client(t, serverToken, nil)
	ctx := ctxWithTimeout(t)

	session, err := c.Register(ctx, testNode())
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if session.ID == "" || session.HeartbeatInterval != 5*time.Second || len(session.Assignments) != 0 {
		t.Fatalf("unexpected session: %+v", session)
	}
	n, err := env.store.GetNode(ctx, "node-1")
	if err != nil || n.Name != "Node 1" || n.CPUMillis != 2000 || n.MemoryBytes != 1<<30 || n.Labels["zone"] != "a" || n.AgentVersion != "test" {
		t.Fatalf("stored node = %+v, %v", n, err)
	}

	_, err = env.store.CreateWorkload(ctx, store.Workload{ID: "w1", Name: "web", Image: "nginx", CPUMillis: 100, MemoryBytes: 1 << 20, NodeID: "node-1", State: store.WorkloadScheduled, CreatedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	statuses := []*dockfleetv1.WorkloadStatus{{WorkloadId: "w1", Phase: dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_RUNNING, Message: "up"}}
	assigned, err := c.Heartbeat(ctx, "node-1", session.ID, statuses)
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if len(assigned) != 1 || assigned[0].GetWorkloadId() != "w1" || assigned[0].GetImage() != "nginx" || assigned[0].GetCpuMillis() != 100 {
		t.Fatalf("assignments = %v", assigned)
	}
	w, _ := env.store.GetWorkload(ctx, "w1")
	if w.State != store.WorkloadRunning || w.StatusMessage != "up" {
		t.Fatalf("status report was not recorded: %+v", w)
	}

	if err := c.Deregister(ctx, "node-1", session.ID); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	_, err = c.Heartbeat(ctx, "node-1", session.ID, nil)
	if !NeedsRegistration(err) || ErrorCode(err) != dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_REGISTERED {
		t.Fatalf("heartbeat after deregister error = %v, want NOT_REGISTERED", err)
	}
}

func TestSupersededSession(t *testing.T) {
	env := newEnv(t, nil)
	first := env.client(t, serverToken, nil)
	second := env.client(t, serverToken, nil)
	ctx := ctxWithTimeout(t)

	s1, err := first.Register(ctx, testNode())
	if err != nil {
		t.Fatal(err)
	}
	s2, err := second.Register(ctx, testNode())
	if err != nil {
		t.Fatal(err)
	}
	_, err = first.Heartbeat(ctx, "node-1", s1.ID, nil)
	if !NeedsRegistration(err) || ErrorCode(err) != dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_SESSION_SUPERSEDED {
		t.Fatalf("old session error = %v, want SESSION_SUPERSEDED", err)
	}
	if _, err := second.Heartbeat(ctx, "node-1", s2.ID, nil); err != nil {
		t.Fatalf("current session heartbeat: %v", err)
	}
}

func TestRejectsBadToken(t *testing.T) {
	env := newEnv(t, nil)
	for _, token := range []string{"wrong-token-0123456789", ""} {
		c := env.client(t, token, nil)
		_, err := c.Register(ctxWithTimeout(t), testNode())
		if status.Code(err) != codes.Unauthenticated || !IsPermanent(err) {
			t.Errorf("token %q: code = %v, permanent = %v (err %v)", token, status.Code(err), IsPermanent(err), err)
		}
	}
}

func TestRegisterValidation(t *testing.T) {
	env := newEnv(t, nil)
	c := env.client(t, serverToken, nil)
	tests := map[string]func(*NodeInfo){
		"bad id":       func(n *NodeInfo) { n.ID = "../etc" },
		"empty name":   func(n *NodeInfo) { n.Name = "" },
		"zero cpu":     func(n *NodeInfo) { n.CPUMillis = 0 },
		"negative mem": func(n *NodeInfo) { n.MemoryBytes = -1 },
		"long name":    func(n *NodeInfo) { n.Name = strings.Repeat("x", 256) },
		"empty label":  func(n *NodeInfo) { n.Labels = map[string]string{"": "v"} },
		"long label":   func(n *NodeInfo) { n.Labels = map[string]string{"k": strings.Repeat("v", 129)} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			n := testNode()
			mutate(&n)
			_, err := c.Register(ctxWithTimeout(t), n)
			if status.Code(err) != codes.InvalidArgument || ErrorCode(err) != dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_INVALID_REQUEST || !IsPermanent(err) {
				t.Fatalf("code = %v, protocol code = %v (err %v)", status.Code(err), ErrorCode(err), err)
			}
		})
	}
	if _, err := env.store.GetNode(context.Background(), "../etc"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("invalid registration must not be stored, GetNode error = %v", err)
	}
}

func TestHeartbeatValidation(t *testing.T) {
	env := newEnv(t, nil)
	c := env.client(t, serverToken, nil)
	ctx := ctxWithTimeout(t)
	session, err := c.Register(ctx, testNode())
	if err != nil {
		t.Fatal(err)
	}
	pending := dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_PENDING
	tooMany := make([]*dockfleetv1.WorkloadStatus, maxStatuses+1)
	for i := range tooMany {
		tooMany[i] = &dockfleetv1.WorkloadStatus{WorkloadId: "w", Phase: pending}
	}
	tests := map[string][]*dockfleetv1.WorkloadStatus{
		"unspecified phase": {{WorkloadId: "w1"}},
		"unknown phase":     {{WorkloadId: "w1", Phase: 99}},
		"empty id":          {{Phase: pending}},
		"long id":           {{WorkloadId: strings.Repeat("x", 65), Phase: pending}},
		"long message":      {{WorkloadId: "w1", Phase: pending, Message: strings.Repeat("m", 513)}},
		"too many":          tooMany,
	}
	for name, statuses := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := c.Heartbeat(ctx, "node-1", session.ID, statuses)
			if ErrorCode(err) != dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_INVALID_REQUEST {
				t.Fatalf("error = %v, want INVALID_REQUEST", err)
			}
		})
	}
}

func TestVersionNegotiation(t *testing.T) {
	env := newEnv(t, func(s *NodeServer) { s.WithVersion(&dockfleetv1.ProtocolVersion{Major: 1, Minor: 2}) })
	tests := []struct {
		name       string
		major      uint32
		minor      uint32
		compatible bool
	}{
		{"same version", 1, 2, true},
		{"older minor", 1, 0, true},
		{"newer minor", 1, 3, false},
		{"newer major", 2, 0, false},
		{"older major", 0, 9, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := env.client(t, serverToken, func(cfg *ClientConfig) {
				cfg.Version = &dockfleetv1.ProtocolVersion{Major: tt.major, Minor: tt.minor}
			})
			_, err := c.Register(ctxWithTimeout(t), testNode())
			if tt.compatible {
				if err != nil {
					t.Fatalf("Register: %v", err)
				}
				return
			}
			var pe *Error
			if !errors.As(err, &pe) || !IsIncompatibleVersion(err) || !IsPermanent(err) {
				t.Fatalf("error = %v, want an INCOMPATIBLE_VERSION protocol error", err)
			}
			if pe.Supported.GetMajor() != 1 || pe.Supported.GetMinor() != 2 {
				t.Errorf("error should report the manager's version 1.2, got %v", pe.Supported)
			}
			if status.Code(err) != codes.FailedPrecondition {
				t.Errorf("gRPC code = %v, want FailedPrecondition", status.Code(err))
			}
		})
	}
}

func TestMissingVersionIsIncompatible(t *testing.T) {
	env := newEnv(t, nil)
	c := env.client(t, serverToken, nil)
	_, err := c.nodes.RegisterNode(ctxWithTimeout(t), &dockfleetv1.RegisterNodeRequest{
		Identity: &dockfleetv1.NodeIdentity{NodeId: "node-1", Name: "n"},
		Capacity: &dockfleetv1.NodeCapacity{CpuMillis: 1, MemoryBytes: 1},
	})
	if !IsIncompatibleVersion(decodeError(err)) {
		t.Fatalf("error = %v, want INCOMPATIBLE_VERSION", err)
	}
}

type fixedRegisterClient struct {
	dockfleetv1.NodeServiceClient
	resp *dockfleetv1.RegisterNodeResponse
}

func (f fixedRegisterClient) RegisterNode(context.Context, *dockfleetv1.RegisterNodeRequest, ...grpc.CallOption) (*dockfleetv1.RegisterNodeResponse, error) {
	return f.resp, nil
}

func TestClientRejectsIncompatibleManagerResponse(t *testing.T) {
	env := newEnv(t, nil)
	c := env.client(t, serverToken, nil)
	c.nodes = fixedRegisterClient{resp: &dockfleetv1.RegisterNodeResponse{
		ProtocolVersion: &dockfleetv1.ProtocolVersion{Major: 2},
		SessionId:       "s",
	}}
	_, err := c.Register(ctxWithTimeout(t), testNode())
	if !IsIncompatibleVersion(err) {
		t.Fatalf("error = %v, want INCOMPATIBLE_VERSION", err)
	}

	c.nodes = fixedRegisterClient{resp: &dockfleetv1.RegisterNodeResponse{
		ProtocolVersion: &dockfleetv1.ProtocolVersion{Major: VersionMajor, Minor: VersionMinor + 5},
		SessionId:       "s",
	}}
	if _, err := c.Register(ctxWithTimeout(t), testNode()); err != nil {
		t.Fatalf("a newer manager minor must be accepted: %v", err)
	}
}

func TestDialRefusesPlaintextToRemoteAddresses(t *testing.T) {
	for _, addr := range []string{"10.1.2.3:21281", "manager.example.com:21281", ":21281"} {
		if _, err := Dial(ClientConfig{Addr: addr, Token: serverToken}); !errors.Is(err, ErrInsecureRemote) {
			t.Errorf("Dial(%q) error = %v, want ErrInsecureRemote", addr, err)
		}
	}
	c, err := Dial(ClientConfig{Addr: "127.0.0.1:21281", Token: serverToken})
	if err != nil {
		t.Fatalf("loopback plaintext must be allowed: %v", err)
	}
	_ = c.Close()
}

func TestErrorRoundTrip(t *testing.T) {
	own := &dockfleetv1.ProtocolVersion{Major: 1, Minor: 4}
	err := newStatusError(codes.FailedPrecondition, dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_REGISTERED, "register again", own)
	decoded := decodeError(err)
	var pe *Error
	if !errors.As(decoded, &pe) {
		t.Fatalf("decodeError(%v) is not *Error", err)
	}
	if pe.Code != dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_REGISTERED || pe.Message != "register again" || pe.Supported.GetMinor() != 4 {
		t.Fatalf("unexpected decoded error: %+v", pe)
	}
	if status.Code(decoded) != codes.FailedPrecondition {
		t.Errorf("gRPC code was lost: %v", status.Code(decoded))
	}
	plain := status.Error(codes.Unavailable, "down")
	if decodeError(plain) != plain {
		t.Error("errors without a ProtocolError detail must pass through unchanged")
	}
	if decodeError(nil) != nil {
		t.Error("decodeError(nil) must be nil")
	}
}

func TestIsPermanent(t *testing.T) {
	if IsPermanent(status.Error(codes.Unavailable, "down")) || IsPermanent(context.DeadlineExceeded) || IsPermanent(errors.New("boom")) {
		t.Error("transient errors must not be permanent")
	}
	if !IsPermanent(status.Error(codes.PermissionDenied, "no")) {
		t.Error("PermissionDenied must be permanent")
	}
}

func TestCheckVersions(t *testing.T) {
	v := func(major, minor uint32) *dockfleetv1.ProtocolVersion {
		return &dockfleetv1.ProtocolVersion{Major: major, Minor: minor}
	}
	if err := checkAgentVersion(v(1, 1), nil); err == nil {
		t.Error("a missing agent version must be rejected")
	}
	if err := checkManagerVersion(v(1, 0), nil); err == nil {
		t.Error("a missing manager version must be rejected")
	}
	if err := checkManagerVersion(v(1, 0), v(1, 7)); err != nil {
		t.Errorf("newer manager minor rejected: %v", err)
	}
	if err := checkManagerVersion(v(1, 0), v(2, 0)); err == nil {
		t.Error("different manager major must be rejected")
	}
}
