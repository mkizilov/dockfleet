package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dockfleet/dockfleet/internal/docker"
	"github.com/dockfleet/dockfleet/internal/dockerfake"
	"github.com/dockfleet/dockfleet/internal/protocol"
	dockfleetv1 "github.com/dockfleet/dockfleet/internal/protocol/dockfleet/v1"
)

type heartbeatCall struct {
	sessionID string
	statuses  []*dockfleetv1.WorkloadStatus
}

type fakeManager struct {
	mu            sync.Mutex
	sessionSeq    int
	registerErrs  []error // consumed one per Register call; nil entries succeed
	registerCalls int
	heartbeatErrs []error // consumed one per Heartbeat call; nil entries succeed
	heartbeats    []heartbeatCall
	assignments   []*dockfleetv1.WorkloadAssignment
	deregistered  []string
	deregisterErr error
	interval      time.Duration
}

func (f *fakeManager) Register(context.Context, protocol.NodeInfo) (protocol.Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registerCalls++
	if len(f.registerErrs) > 0 {
		err := f.registerErrs[0]
		f.registerErrs = f.registerErrs[1:]
		if err != nil {
			return protocol.Session{}, err
		}
	}
	f.sessionSeq++
	return protocol.Session{ID: "session-" + string(rune('0'+f.sessionSeq)), HeartbeatInterval: f.interval, Assignments: f.assignments}, nil
}

func (f *fakeManager) Heartbeat(_ context.Context, _, sessionID string, statuses []*dockfleetv1.WorkloadStatus) ([]*dockfleetv1.WorkloadAssignment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heartbeats = append(f.heartbeats, heartbeatCall{sessionID: sessionID, statuses: statuses})
	if len(f.heartbeatErrs) > 0 {
		err := f.heartbeatErrs[0]
		f.heartbeatErrs = f.heartbeatErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return f.assignments, nil
}

func (f *fakeManager) Deregister(_ context.Context, _, sessionID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deregistered = append(f.deregistered, sessionID)
	return f.deregisterErr
}

func (f *fakeManager) registers() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.registerCalls
}

func (f *fakeManager) calls() []heartbeatCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]heartbeatCall(nil), f.heartbeats...)
}

func (f *fakeManager) deregistrations() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deregistered...)
}

func newTestAgent(m Manager, d *dockerfake.Fake) *Agent {
	return New(Config{
		Node:       protocol.NodeInfo{ID: "node-1"},
		Manager:    m,
		Docker:     d,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		RPCTimeout: time.Second,
		RetryMin:   time.Millisecond,
		RetryMax:   4 * time.Millisecond,
		Resync:     5 * time.Millisecond,
	})
}

// unreachableDocker fails its first ping, as an engine that is not up yet would.
func unreachableDocker() *dockerfake.Fake {
	d := dockerfake.New("node-1")
	d.FailNext("ping", errors.New("no socket"), 1)
	return d
}

type running struct {
	t      *testing.T
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
	result error
}

func start(t *testing.T, a *Agent) *running {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{t: t, cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- a.Run(ctx) }()
	t.Cleanup(func() { _ = r.stop() })
	return r
}

// stop cancels the agent and returns what Run returned.
func (r *running) stop() error {
	r.once.Do(func() {
		r.cancel()
		select {
		case r.result = <-r.done:
		case <-time.After(5 * time.Second):
			r.t.Error("agent did not stop after context cancellation")
		}
	})
	return r.result
}

// wait returns Run's result without canceling, for agents expected to exit on their own.
func (r *running) wait() error {
	select {
	case err := <-r.done:
		return err
	case <-time.After(5 * time.Second):
		r.t.Fatal("agent did not exit on its own")
		return nil
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func assignment(id string) *dockfleetv1.WorkloadAssignment {
	return &dockfleetv1.WorkloadAssignment{WorkloadId: id, Name: id, Image: "img", Generation: 1, CpuMillis: 100, MemoryBytes: 64 << 20}
}

func TestRunRegistersHeartbeatsAndDeregistersOnShutdown(t *testing.T) {
	m := &fakeManager{interval: 2 * time.Millisecond}
	r := start(t, newTestAgent(m, dockerfake.New("node-1")))

	waitFor(t, "three heartbeats", func() bool { return len(m.calls()) >= 3 })
	if err := r.stop(); err != nil {
		t.Fatalf("Run returned %v, want nil on cancellation", err)
	}
	if m.registers() != 1 {
		t.Errorf("registered %d times, want 1", m.registers())
	}
	for _, c := range m.calls() {
		if c.sessionID != "session-1" {
			t.Fatalf("heartbeat used session %q, want session-1", c.sessionID)
		}
	}
	if got := m.deregistrations(); len(got) != 1 || got[0] != "session-1" {
		t.Errorf("deregistrations = %v, want [session-1]", got)
	}
}

func TestRunRetriesRegistrationUntilManagerIsAvailable(t *testing.T) {
	down := status.Error(codes.Unavailable, "down")
	m := &fakeManager{interval: 2 * time.Millisecond, registerErrs: []error{down, down, down}}
	start(t, newTestAgent(m, dockerfake.New("node-1")))

	waitFor(t, "a heartbeat after retries", func() bool { return len(m.calls()) >= 1 })
	if m.registers() != 4 {
		t.Errorf("registered %d times, want 4 (3 failures + 1 success)", m.registers())
	}
}

func TestRunStopsOnPermanentRegistrationErrors(t *testing.T) {
	tests := map[string]error{
		"incompatible version": &protocol.Error{Code: dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_INCOMPATIBLE_VERSION, Message: "major mismatch"},
		"invalid request":      &protocol.Error{Code: dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_INVALID_REQUEST, Message: "bad node id"},
		"bad credentials":      status.Error(codes.Unauthenticated, "no"),
	}
	for name, cause := range tests {
		t.Run(name, func(t *testing.T) {
			m := &fakeManager{registerErrs: []error{cause}}
			r := start(t, newTestAgent(m, dockerfake.New("node-1")))
			err := r.wait()
			if err == nil || !errors.Is(err, cause) {
				t.Fatalf("Run returned %v, want an error wrapping %v", err, cause)
			}
			if m.registers() != 1 {
				t.Errorf("a permanent rejection must not be retried, got %d attempts", m.registers())
			}
			if len(m.deregistrations()) != 0 {
				t.Error("an agent that never registered must not deregister")
			}
		})
	}
}

func TestRunRetriesUntilTheNodeIsEnrolled(t *testing.T) {
	notEnrolled := &protocol.Error{Code: dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_ENROLLED, Message: "ask an administrator"}
	m := &fakeManager{interval: 2 * time.Millisecond, registerErrs: []error{notEnrolled, notEnrolled, notEnrolled}}
	r := start(t, newTestAgent(m, dockerfake.New("node-1")))

	waitFor(t, "a heartbeat once enrolled", func() bool { return len(m.calls()) >= 1 })
	if m.registers() != 4 {
		t.Errorf("registered %d times, want 4 (3 rejections + 1 success)", m.registers())
	}
	if err := r.stop(); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
}

func TestRunStopsWhenTheManagerRejectsTheIdentity(t *testing.T) {
	mismatch := &protocol.Error{Code: dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_IDENTITY_MISMATCH, Message: "certificate is for another node"}
	m := &fakeManager{registerErrs: []error{mismatch}}
	r := start(t, newTestAgent(m, dockerfake.New("node-1")))
	if err := r.wait(); !errors.Is(err, mismatch) {
		t.Fatalf("Run returned %v, want the identity mismatch", err)
	}
	if m.registers() != 1 {
		t.Errorf("an identity mismatch must not be retried, got %d attempts", m.registers())
	}
}

func TestRunStopsOnPermanentHeartbeatRejection(t *testing.T) {
	bad := status.Error(codes.Unauthenticated, "token rotated")
	m := &fakeManager{interval: 2 * time.Millisecond, heartbeatErrs: []error{bad}}
	r := start(t, newTestAgent(m, dockerfake.New("node-1")))
	if err := r.wait(); !errors.Is(err, bad) {
		t.Fatalf("Run returned %v, want the rejection", err)
	}
	if len(m.deregistrations()) != 0 {
		t.Error("a rejected agent must not try to deregister")
	}
}

func TestRunKeepsGoingWhenTheCertificateIsRejectedTemporarily(t *testing.T) {
	expired := &protocol.Error{Code: dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_CERTIFICATE_INVALID, Message: "client certificate has expired"}
	m := &fakeManager{interval: 2 * time.Millisecond, heartbeatErrs: []error{expired, expired}}
	start(t, newTestAgent(m, dockerfake.New("node-1")))

	waitFor(t, "heartbeats after the certificate errors", func() bool { return len(m.calls()) >= 4 })
	if m.registers() != 1 {
		t.Errorf("a certificate error must not trigger re-registration, got %d registrations", m.registers())
	}
}

func TestRunRegistersAgainWhenSessionIsLost(t *testing.T) {
	lost := &protocol.Error{Code: dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_REGISTERED, Message: "restarted"}
	m := &fakeManager{interval: 2 * time.Millisecond, heartbeatErrs: []error{lost}}
	start(t, newTestAgent(m, dockerfake.New("node-1")))

	waitFor(t, "a heartbeat on the second session", func() bool {
		for _, c := range m.calls() {
			if c.sessionID == "session-2" {
				return true
			}
		}
		return false
	})
	if m.registers() != 2 {
		t.Errorf("registered %d times, want 2", m.registers())
	}
}

func TestRunSurvivesTransientHeartbeatFailures(t *testing.T) {
	blip := status.Error(codes.Unavailable, "blip")
	m := &fakeManager{interval: 2 * time.Millisecond, heartbeatErrs: []error{blip, blip, context.DeadlineExceeded}}
	start(t, newTestAgent(m, dockerfake.New("node-1")))

	waitFor(t, "heartbeats after transient errors", func() bool { return len(m.calls()) >= 5 })
	if m.registers() != 1 {
		t.Errorf("transient errors must not trigger re-registration, got %d registrations", m.registers())
	}
}

func TestRunStopsWhileManagerIsDown(t *testing.T) {
	m := &fakeManager{registerErrs: []error{status.Error(codes.Unavailable, "down")}}
	a := newTestAgent(m, dockerfake.New("node-1"))
	a.cfg.RetryMin = time.Hour
	a.cfg.RetryMax = time.Hour
	r := start(t, a)

	waitFor(t, "the first registration attempt", func() bool { return m.registers() >= 1 })
	if err := r.stop(); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
	if len(m.deregistrations()) != 0 {
		t.Error("an agent that never registered must not deregister")
	}
}

// An assignment becomes a running container; the report says so and
// acknowledges the instruction; and stopping the agent leaves the container
// running for the next agent to find.
func TestAgentRunsAssignedWorkloadsAndLeavesThemWhenItStops(t *testing.T) {
	engine := dockerfake.New("node-1")
	engine.AddImage("img")
	web := assignment("w1")
	web.Generation = 3
	m := &fakeManager{interval: 2 * time.Millisecond, assignments: []*dockfleetv1.WorkloadAssignment{web}}
	r := start(t, newTestAgent(m, engine))

	waitFor(t, "w1 to be reported running and acknowledged", func() bool {
		calls := m.calls()
		if len(calls) == 0 {
			return false
		}
		for _, st := range calls[len(calls)-1].statuses {
			if st.GetWorkloadId() == "w1" && st.GetPhase() == dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_RUNNING &&
				st.GetAcknowledgedGeneration() == 3 && st.GetContainerId() != "" {
				return true
			}
		}
		return false
	})
	if err := r.stop(); err != nil {
		t.Fatalf("Run returned %v", err)
	}
	if managed := engine.Managed(); len(managed) != 1 || managed[0].WorkloadID != "w1" || managed[0].State != docker.StateRunning {
		t.Fatalf("containers after the agent stopped = %+v; they must keep running", managed)
	}

	// A new agent on the same node finds the container and does not create another.
	m2 := &fakeManager{interval: 2 * time.Millisecond, assignments: []*dockfleetv1.WorkloadAssignment{web}}
	start(t, newTestAgent(m2, engine))
	waitFor(t, "the new agent to report w1", func() bool {
		calls := m2.calls()
		return len(calls) > 0 && len(calls[len(calls)-1].statuses) == 1 &&
			calls[len(calls)-1].statuses[0].GetPhase() == dockfleetv1.WorkloadPhase_WORKLOAD_PHASE_RUNNING
	})
	if engine.Count("create") != 1 || engine.Count("start") != 1 {
		t.Fatalf("calls = %v; a restarted agent must adopt the container it finds", engine.Calls())
	}
}

func TestRunToleratesUnreachableDocker(t *testing.T) {
	m := &fakeManager{interval: 2 * time.Millisecond}
	start(t, newTestAgent(m, unreachableDocker()))

	waitFor(t, "a heartbeat", func() bool { return len(m.calls()) >= 1 })
}

func TestDeregisterFailureDoesNotFailShutdown(t *testing.T) {
	m := &fakeManager{interval: 2 * time.Millisecond, deregisterErr: errors.New("manager gone")}
	r := start(t, newTestAgent(m, dockerfake.New("node-1")))

	waitFor(t, "a heartbeat", func() bool { return len(m.calls()) >= 1 })
	if err := r.stop(); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
	if len(m.deregistrations()) != 1 {
		t.Errorf("deregistrations = %v, want one attempt", m.deregistrations())
	}
}
