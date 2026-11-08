package protocol

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dockfleet/dockfleet/internal/pki"
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
	// node-1 is the only enrolled node.
	if _, err := st.CreateEnrollment(context.Background(), store.Enrollment{NodeID: "node-1", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	lis := bufconn.Listen(1 << 20)
	srv, err := NewGRPCServer(node, ServerOptions{Token: serverToken, HandlerTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
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

	_, err = env.store.CreateWorkload(ctx, store.Workload{ID: "w1", Name: "web", Image: "nginx", CPUMillis: 100, MemoryBytes: 1 << 20, NodeID: "node-1", CreatedAt: time.Now()})
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
	if w.Observed == nil || w.Observed.State != store.WorkloadRunning || w.Observed.Message != "up" || w.Observed.NodeID != "node-1" {
		t.Fatalf("status report was not recorded as an observation: %+v", w.Observed)
	}
	if w.DesiredState != store.DesiredRunning {
		t.Fatalf("a status report must not change the desired state: %+v", w)
	}

	if err := c.Deregister(ctx, "node-1", session.ID); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	_, err = c.Heartbeat(ctx, "node-1", session.ID, nil)
	if !NeedsRegistration(err) || ErrorCode(err) != dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_REGISTERED {
		t.Fatalf("heartbeat after deregister error = %v, want NOT_REGISTERED", err)
	}
}

// An assignment carries everything the agent needs to act on it, and the
// agent's acknowledgment, container and exit code come back as the observation.
func TestAssignmentsCarryTheInstructionAndStatusesCarryTheAcknowledgment(t *testing.T) {
	env := newEnv(t, nil)
	c := env.client(t, serverToken, nil)
	ctx := ctxWithTimeout(t)
	session, err := c.Register(ctx, testNode())
	if err != nil {
		t.Fatal(err)
	}

	web1 := store.Workload{ID: "w1", Name: "web-r1-1-abc123", Image: "nginx:1.27", CPUMillis: 500, MemoryBytes: 128 << 20, NodeID: "node-1", CreatedAt: time.Now()}
	web1.Command = []string{"nginx", "-g", "daemon off;"}
	web1.Env = map[string]string{"MODE": "prod"}
	web1.Ports = []store.ServicePort{{ContainerPort: 80, Protocol: "tcp", HostPort: 18080}, {ContainerPort: 53, Protocol: "udp"}}
	web1.RestartPolicy = &store.RestartPolicy{Condition: store.RestartOnFailure, MaxRetries: 3}
	_, err = env.store.CreateWorkload(ctx, web1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = env.store.CreateWorkload(ctx, store.Workload{ID: "w2", Name: "z-old", Image: "nginx:1.26", CPUMillis: 100, MemoryBytes: 1 << 20, NodeID: "node-1", CreatedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.store.SetWorkloadDesiredState(ctx, "w2", store.DesiredStopped, time.Now()); err != nil {
		t.Fatal(err)
	}

	exit := int32(137)
	killed := &dockfleetv1.WorkloadStatus{WorkloadId: "w1", Phase: dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_FAILED, Message: "killed"}
	killed.AcknowledgedGeneration, killed.ContainerId, killed.ExitCode, killed.OomKilled, killed.RestartCount = 1, "c0ffee", &exit, true, 2
	assigned, err := c.Heartbeat(ctx, "node-1", session.ID, []*dockfleetv1.WorkloadStatus{killed})
	if err != nil {
		t.Fatal(err)
	}
	if len(assigned) != 2 {
		t.Fatalf("assignments = %v", assigned)
	}
	web, old := assigned[0], assigned[1]
	if web.GetWorkloadId() != "w1" || web.GetGeneration() != 1 || web.GetDesiredState() != dockfleetv1.WorkloadDesiredState_WORKLOAD_DESIRED_STATE_RUNNING ||
		web.GetName() != "web-r1-1-abc123" || len(web.GetCommand()) != 3 || web.GetEnv()["MODE"] != "prod" ||
		len(web.GetPorts()) != 2 || web.GetPorts()[0].GetContainerPort() != 80 || web.GetPorts()[0].GetHostPort() != 18080 || web.GetPorts()[0].GetProtocol() != "tcp" ||
		web.GetPorts()[1].GetProtocol() != "udp" ||
		web.GetRestartPolicy().GetCondition() != "on-failure" || web.GetRestartPolicy().GetMaxRetries() != 3 {
		t.Fatalf("running assignment = %v", web)
	}
	if old.GetWorkloadId() != "w2" || old.GetGeneration() != 2 || old.GetDesiredState() != dockfleetv1.WorkloadDesiredState_WORKLOAD_DESIRED_STATE_STOPPED || old.GetRestartPolicy() != nil {
		t.Fatalf("stopped assignment = %v; it stays in the set as an instruction to stop", old)
	}

	w, _ := env.store.GetWorkload(ctx, "w1")
	o := w.Observed
	if o == nil || o.State != store.WorkloadFailed || o.AckedGeneration != 1 || o.ContainerID != "c0ffee" || o.ExitCode == nil || *o.ExitCode != 137 || !o.OOMKilled || o.RestartCount != 2 {
		t.Fatalf("observation = %+v", o)
	}
}

// The lease is granted with the session; a manager that grants none says so with zero.
func TestRegistrationGrantsALease(t *testing.T) {
	ctx := ctxWithTimeout(t)
	if session, err := newEnv(t, nil).client(t, serverToken, nil).Register(ctx, testNode()); err != nil || session.Lease != 0 {
		t.Fatalf("without a lease: %+v, %v", session, err)
	}
	env := newEnv(t, func(s *NodeServer) { s.WithLease(45 * time.Second) })
	session, err := env.client(t, serverToken, nil).Register(ctx, testNode())
	if err != nil || session.Lease != 45*time.Second || session.HeartbeatInterval != 5*time.Second {
		t.Fatalf("session = %+v, %v", session, err)
	}
}

// How often a container is restarted, how long to wait, and its health check
// travel with the assignment; the health the agent observes comes back with the report.
func TestAssignmentsCarryTheRestartPolicyAndHealthCheckAndStatusesTheHealth(t *testing.T) {
	env := newEnv(t, nil)
	c := env.client(t, serverToken, nil)
	ctx := ctxWithTimeout(t)
	session, err := c.Register(ctx, testNode())
	if err != nil {
		t.Fatal(err)
	}
	w := store.Workload{ID: "w1", Name: "web", Image: "nginx", CPUMillis: 100, MemoryBytes: 1 << 20, NodeID: "node-1", CreatedAt: time.Now()}
	w.RestartPolicy = &store.RestartPolicy{Condition: store.RestartAlways, MaxRetries: 9, BackoffSeconds: 2, MaxBackoffSeconds: 90}
	w.HealthCheck = &store.HealthCheck{Command: []string{"curl", "-f", "localhost"}, IntervalSeconds: 10, TimeoutSeconds: 2, StartPeriodSeconds: 5, Retries: 4}
	if _, err := env.store.CreateWorkload(ctx, w); err != nil {
		t.Fatal(err)
	}

	report := &dockfleetv1.WorkloadStatus{WorkloadId: "w1", Phase: dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_RUNNING, Health: "unhealthy"}
	assigned, err := c.Heartbeat(ctx, "node-1", session.ID, []*dockfleetv1.WorkloadStatus{report})
	if err != nil || len(assigned) != 1 {
		t.Fatalf("assignments = %v, %v", assigned, err)
	}
	rp, hc := assigned[0].GetRestartPolicy(), assigned[0].GetHealthCheck()
	if rp.GetCondition() != "always" || rp.GetMaxRetries() != 9 || rp.GetBackoffSeconds() != 2 || rp.GetMaxBackoffSeconds() != 90 {
		t.Fatalf("restart policy = %v", rp)
	}
	if len(hc.GetCommand()) != 3 || hc.GetIntervalSeconds() != 10 || hc.GetTimeoutSeconds() != 2 || hc.GetStartPeriodSeconds() != 5 || hc.GetRetries() != 4 {
		t.Fatalf("health check = %v", hc)
	}
	if got, _ := env.store.GetWorkload(ctx, "w1"); got.Observed == nil || got.Observed.Health != store.HealthUnhealthy {
		t.Fatalf("observation = %+v", got.Observed)
	}

	// A workload without a health check sends none.
	if _, err := env.store.CreateWorkload(ctx, store.Workload{ID: "w2", Name: "z-plain", Image: "nginx", CPUMillis: 100, MemoryBytes: 1 << 20, NodeID: "node-1", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	assigned, err = c.Heartbeat(ctx, "node-1", session.ID, nil)
	if err != nil || len(assigned) != 2 || assigned[1].GetHealthCheck() != nil {
		t.Fatalf("assignments = %v, %v", assigned, err)
	}

	// The agent may only report a health result there is.
	bad := &dockfleetv1.WorkloadStatus{WorkloadId: "w1", Phase: dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_RUNNING, Health: "sleepy"}
	_, err = c.Heartbeat(ctx, "node-1", session.ID, []*dockfleetv1.WorkloadStatus{bad})
	if ErrorCode(err) != dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_INVALID_REQUEST {
		t.Fatalf("error = %v, want INVALID_REQUEST", err)
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
		"long container id": {{WorkloadId: "w1", Phase: pending, ContainerId: strings.Repeat("c", 129)}},
		"negative ack":      {{WorkloadId: "w1", Phase: pending, AcknowledgedGeneration: -1}},
		"negative restarts": {{WorkloadId: "w1", Phase: pending, RestartCount: -1}},
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
	_, err := c.node().RegisterNode(ctxWithTimeout(t), &dockfleetv1.RegisterNodeRequest{
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
	c.eps[0].nodes = fixedRegisterClient{resp: &dockfleetv1.RegisterNodeResponse{
		ProtocolVersion: &dockfleetv1.ProtocolVersion{Major: 2},
		SessionId:       "s",
	}}
	_, err := c.Register(ctxWithTimeout(t), testNode())
	if !IsIncompatibleVersion(err) {
		t.Fatalf("error = %v, want INCOMPATIBLE_VERSION", err)
	}

	c.eps[0].nodes = fixedRegisterClient{resp: &dockfleetv1.RegisterNodeResponse{
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
	if !IsPermanent(status.Error(codes.PermissionDenied, "no")) || !IsPermanent(status.Error(codes.Unauthenticated, "no")) {
		t.Error("plain authentication failures must be permanent")
	}

	code := func(c dockfleetv1.ProtocolErrorCode) error { return &Error{Code: c, Message: "x"} }
	permanent := []dockfleetv1.ProtocolErrorCode{
		dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_INCOMPATIBLE_VERSION,
		dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_INVALID_REQUEST,
		dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_IDENTITY_MISMATCH,
	}
	for _, c := range permanent {
		if !IsPermanent(code(c)) {
			t.Errorf("%v must be permanent", c)
		}
	}
	transient := []dockfleetv1.ProtocolErrorCode{
		dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_REGISTERED,
		dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_SESSION_SUPERSEDED,
		dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_ENROLLED,
		dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_CERTIFICATE_INVALID,
	}
	for _, c := range transient {
		if IsPermanent(code(c)) {
			t.Errorf("%v must not be permanent", c)
		}
	}
	// A bad certificate arrives as Unauthenticated but is fixed by renewing it.
	badCert := &Error{Code: dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_CERTIFICATE_INVALID, cause: status.Error(codes.Unauthenticated, "expired")}
	if IsPermanent(badCert) {
		t.Error("an invalid client certificate must not be permanent")
	}
	if !NeedsRegistration(code(dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_ENROLLED)) {
		t.Error("NOT_ENROLLED must send the agent back to registration")
	}
}

func TestUnenrolledNodeIsRejected(t *testing.T) {
	env := newEnv(t, nil)
	c := env.client(t, serverToken, nil)
	n := testNode()
	n.ID = "stranger"

	_, err := c.Register(ctxWithTimeout(t), n)
	if !IsNotEnrolled(err) || status.Code(err) != codes.PermissionDenied || IsPermanent(err) {
		t.Fatalf("error = %v (code %v), want a retryable NOT_ENROLLED", err, status.Code(err))
	}
	if _, err := env.store.GetNode(context.Background(), "stranger"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("an unenrolled node must not be stored, GetNode error = %v", err)
	}

	if _, err := env.store.CreateEnrollment(context.Background(), store.Enrollment{NodeID: "stranger", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	session, err := c.Register(ctxWithTimeout(t), n)
	if err != nil {
		t.Fatalf("register after enrollment: %v", err)
	}

	if _, err := env.store.RevokeEnrollment(context.Background(), "stranger", time.Now()); err != nil {
		t.Fatal(err)
	}
	_, err = c.Heartbeat(ctxWithTimeout(t), "stranger", session.ID, nil)
	if !IsNotEnrolled(err) {
		t.Fatalf("heartbeat after revocation error = %v, want NOT_ENROLLED", err)
	}
}

// peerContext fakes what grpc puts in the context of a call made over TLS.
func peerContext(leaf *x509.Certificate, verified bool) context.Context {
	state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}
	if verified {
		state.VerifiedChains = [][]*x509.Certificate{{leaf}}
	}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: state}})
}

func TestIdentityInterceptor(t *testing.T) {
	now := time.Now()
	ca, err := pki.NewCA("Test CA", now.Add(-time.Hour), now.Add(100*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	issue := func(id string, notBefore, notAfter time.Time) *x509.Certificate {
		issued, err := ca.IssueNode(pki.NodeRequest{NodeID: id, NotBefore: notBefore, NotAfter: notAfter})
		if err != nil {
			t.Fatal(err)
		}
		return issued.Cert
	}
	valid := issue("worker-1", now.Add(-time.Minute), now.Add(time.Hour))
	expired := issue("worker-1", now.Add(-3*time.Hour), now.Add(-time.Hour))
	future := issue("worker-1", now.Add(time.Hour), now.Add(2*time.Hour))
	serverOnly, err := ca.IssueServer(pki.ServerRequest{Name: "m", DNSNames: []string{"m"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}

	var seen string
	handler := func(ctx context.Context, _ any) (any, error) {
		seen, _ = identityFromContext(ctx)
		return "ok", nil
	}
	intercept := UnaryIdentityInterceptor(func() time.Time { return now })
	call := func(ctx context.Context) error {
		seen = ""
		_, err := intercept(ctx, nil, &grpc.UnaryServerInfo{}, handler)
		return err
	}

	if err := call(peerContext(valid, true)); err != nil || seen != "worker-1" {
		t.Fatalf("valid certificate: err=%v identity=%q", err, seen)
	}

	failures := map[string]struct {
		ctx  context.Context
		cert bool
	}{
		"expired certificate":     {peerContext(expired, true), true},
		"not yet valid":           {peerContext(future, true), true},
		"no node identity":        {peerContext(serverOnly.Cert, true), true},
		"unverified chain":        {peerContext(valid, false), false},
		"plaintext connection":    {peer.NewContext(context.Background(), &peer.Peer{AuthInfo: nil}), false},
		"no peer information":     {context.Background(), false},
		"TLS without certificate": {peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{}}), false},
	}
	for name, tt := range failures {
		t.Run(name, func(t *testing.T) {
			err := call(tt.ctx)
			if status.Code(err) != codes.Unauthenticated || seen != "" {
				t.Fatalf("err = %v, identity = %q; want Unauthenticated and no handler call", err, seen)
			}
			if tt.cert && !IsCertificateInvalid(decodeError(err)) {
				t.Errorf("a bad certificate must be reported as CERTIFICATE_INVALID, got %v", err)
			}
		})
	}

	// The same connection becomes invalid once the clock passes the certificate's expiry.
	later := UnaryIdentityInterceptor(func() time.Time { return now.Add(2 * time.Hour) })
	if _, err := later(peerContext(valid, true), nil, &grpc.UnaryServerInfo{}, handler); !IsCertificateInvalid(decodeError(err)) {
		t.Errorf("a certificate that expired mid-connection must be rejected, got %v", err)
	}
}

func TestIdentityBindingRejectsOtherNodeIDs(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	node := NewNodeServer(registry.New(store.NewMemory(), logger), time.Second, logger)
	ctx := context.WithValue(context.Background(), identityKey{}, "worker-1")

	req := &dockfleetv1.RegisterNodeRequest{
		ProtocolVersion: CurrentVersion(),
		Identity:        &dockfleetv1.NodeIdentity{NodeId: "worker-2", Name: "n"},
		Capacity:        &dockfleetv1.NodeCapacity{CpuMillis: 1, MemoryBytes: 1},
	}
	_, err := node.RegisterNode(ctx, req)
	if status.Code(err) != codes.PermissionDenied || !IsIdentityMismatch(decodeError(err)) {
		t.Errorf("RegisterNode error = %v, want PERMISSION_DENIED / IDENTITY_MISMATCH", err)
	}
	_, err = node.Heartbeat(ctx, &dockfleetv1.HeartbeatRequest{NodeId: "worker-2", SessionId: "s"})
	if !IsIdentityMismatch(decodeError(err)) {
		t.Errorf("Heartbeat error = %v, want IDENTITY_MISMATCH", err)
	}
	_, err = node.DeregisterNode(ctx, &dockfleetv1.DeregisterNodeRequest{NodeId: "worker-2", SessionId: "s"})
	if !IsIdentityMismatch(decodeError(err)) {
		t.Errorf("DeregisterNode error = %v, want IDENTITY_MISMATCH", err)
	}
}

func TestNewGRPCServerRefusesWeakConfigurations(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	node := NewNodeServer(registry.New(store.NewMemory(), logger), time.Second, logger)

	if _, err := NewGRPCServer(node, ServerOptions{TLS: &tls.Config{MinVersion: tls.VersionTLS13}}); !errors.Is(err, ErrClientAuthRequired) {
		t.Errorf("TLS without client certificate verification: error = %v, want ErrClientAuthRequired", err)
	}
	optional := &tls.Config{ClientAuth: tls.VerifyClientCertIfGiven}
	if _, err := NewGRPCServer(node, ServerOptions{TLS: optional}); !errors.Is(err, ErrClientAuthRequired) {
		t.Errorf("optional client certificates: error = %v, want ErrClientAuthRequired", err)
	}
	if _, err := NewGRPCServer(node, ServerOptions{}); err == nil {
		t.Error("a plaintext server without a token must be refused")
	}
	mutual := &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert}
	if srv, err := NewGRPCServer(node, ServerOptions{TLS: mutual, MaxConnectionAge: time.Minute}); err != nil {
		t.Errorf("mutual TLS must be accepted: %v", err)
	} else {
		srv.Stop()
	}
}

func TestDialRequiresAClientCertificateWithTLS(t *testing.T) {
	if _, err := Dial(ClientConfig{Addr: "manager.example.com:21281", TLS: &tls.Config{MinVersion: tls.VersionTLS13}}); !errors.Is(err, ErrNoClientCertificate) {
		t.Errorf("error = %v, want ErrNoClientCertificate", err)
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
