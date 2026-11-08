package registry

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/scheduler"
	"github.com/dockfleet/dockfleet/internal/store"
)

var t0 = time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

// countingStore counts status writes so tests can assert unchanged reports are skipped.
type countingStore struct {
	*store.Memory
	statusWrites int
}

func (c *countingStore) UpdateWorkloadStatus(ctx context.Context, id, nodeID, state, message string, at time.Time) error {
	c.statusWrites++
	return c.Memory.UpdateWorkloadStatus(ctx, id, nodeID, state, message, at)
}

type fixture struct {
	reg   *Registry
	store *countingStore
	clock time.Time
}

// newFixture returns a registry whose clock the test controls. Nodes n1 and n2 are enrolled.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{store: &countingStore{Memory: store.NewMemory()}, clock: t0}
	f.reg = New(f.store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	f.reg.now = func() time.Time { return f.clock }
	f.enroll(t, "n1", "n2")
	return f
}

func (f *fixture) enroll(t *testing.T, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if _, err := f.store.CreateEnrollment(context.Background(), store.Enrollment{NodeID: id, CreatedAt: t0}); err != nil {
			t.Fatal(err)
		}
	}
}

func node(id string) store.Node {
	return store.Node{ID: id, Name: id, CPUMillis: 2000, MemoryBytes: 1 << 30, Labels: map[string]string{"zone": "a"}}
}

func (f *fixture) addWorkload(t *testing.T, id, nodeID string) {
	t.Helper()
	_, err := f.store.CreateWorkload(context.Background(), store.Workload{
		ID: id, Name: id, Image: "img", CPUMillis: 1, MemoryBytes: 1, NodeID: nodeID, State: store.WorkloadScheduled, CreatedAt: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRegisterPersistsNodeAndStartsSession(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	s, assigned, err := f.reg.Register(ctx, node("n1"))
	if err != nil {
		t.Fatal(err)
	}
	if s.ID == "" || s.NodeID != "n1" || !s.StartedAt.Equal(t0) || len(assigned) != 0 {
		t.Fatalf("unexpected session %+v, assignments %v", s, assigned)
	}
	got, err := f.store.GetNode(ctx, "n1")
	if err != nil || !got.RegisteredAt.Equal(t0) || !got.LastHeartbeat.Equal(t0) || got.Labels["zone"] != "a" {
		t.Fatalf("stored node = %+v, %v", got, err)
	}
	if sessions := f.reg.Sessions(); len(sessions) != 1 || sessions[0].ID != s.ID {
		t.Fatalf("Sessions = %+v", sessions)
	}
}

func TestRegisterRequiresAnActiveEnrollment(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	if _, _, err := f.reg.Register(ctx, node("stranger")); !errors.Is(err, ErrNotEnrolled) {
		t.Fatalf("unenrolled register error = %v, want ErrNotEnrolled", err)
	}
	if _, err := f.store.GetNode(ctx, "stranger"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("an unenrolled node must not be stored, GetNode error = %v", err)
	}
	if len(f.reg.Sessions()) != 0 {
		t.Fatalf("an unenrolled node must not get a session: %+v", f.reg.Sessions())
	}

	f.enroll(t, "stranger")
	if _, _, err := f.reg.Register(ctx, node("stranger")); err != nil {
		t.Fatalf("register after enrollment: %v", err)
	}

	if _, err := f.store.RevokeEnrollment(ctx, "stranger", t0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.reg.Register(ctx, node("stranger")); !errors.Is(err, ErrNotEnrolled) {
		t.Fatalf("register after revocation error = %v, want ErrNotEnrolled", err)
	}
}

func TestRevocationEndsTheSessionOnTheNextHeartbeat(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	s, _, err := f.reg.Register(ctx, node("n1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.reg.Heartbeat(ctx, "n1", s.ID, nil); err != nil {
		t.Fatal(err)
	}

	f.clock = f.clock.Add(time.Minute)
	if _, err := f.store.RevokeEnrollment(ctx, "n1", f.clock); err != nil {
		t.Fatal(err)
	}
	if _, err := f.reg.Heartbeat(ctx, "n1", s.ID, nil); !errors.Is(err, ErrNotEnrolled) {
		t.Fatalf("heartbeat after revocation error = %v, want ErrNotEnrolled", err)
	}
	if len(f.reg.Sessions()) != 0 {
		t.Fatalf("the session must be dropped: %+v", f.reg.Sessions())
	}
	n, _ := f.store.GetNode(ctx, "n1")
	if !n.LastHeartbeat.Equal(t0) {
		t.Errorf("a revoked node must not refresh its liveness, LastHeartbeat = %s", n.LastHeartbeat)
	}

	if _, err := f.store.CreateEnrollment(ctx, store.Enrollment{NodeID: "n1", CreatedAt: f.clock}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.reg.Heartbeat(ctx, "n1", s.ID, nil); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("the old session must stay dead after re-enrollment, got %v", err)
	}
	if _, _, err := f.reg.Register(ctx, node("n1")); err != nil {
		t.Fatalf("register after re-enrollment: %v", err)
	}
}

func TestRegisterReturnsExistingAssignments(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if _, _, err := f.reg.Register(ctx, node("n1")); err != nil {
		t.Fatal(err)
	}
	f.addWorkload(t, "w1", "n1")

	_, assigned, err := f.reg.Register(ctx, node("n1"))
	if err != nil || len(assigned) != 1 || assigned[0].ID != "w1" {
		t.Fatalf("assignments = %+v, %v", assigned, err)
	}
}

func TestReRegisterSupersedesOldSession(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	first, _, _ := f.reg.Register(ctx, node("n1"))
	f.clock = f.clock.Add(time.Minute)
	second, _, err := f.reg.Register(ctx, node("n1"))
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID {
		t.Fatal("re-registration must create a new session")
	}
	if _, err := f.reg.Heartbeat(ctx, "n1", first.ID, nil); !errors.Is(err, ErrSessionSuperseded) {
		t.Fatalf("old session heartbeat error = %v, want ErrSessionSuperseded", err)
	}
	if _, err := f.reg.Heartbeat(ctx, "n1", second.ID, nil); err != nil {
		t.Fatalf("new session heartbeat: %v", err)
	}
	if len(f.reg.Sessions()) != 1 {
		t.Fatalf("want exactly one live session, got %+v", f.reg.Sessions())
	}
	n, _ := f.store.GetNode(ctx, "n1")
	if !n.RegisteredAt.Equal(t0) {
		t.Errorf("RegisteredAt must survive re-registration, got %s", n.RegisteredAt)
	}
}

func TestHeartbeatRefreshesLiveness(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	s, _, _ := f.reg.Register(ctx, node("n1"))
	f.clock = f.clock.Add(5 * time.Second)

	if _, err := f.reg.Heartbeat(ctx, "n1", s.ID, nil); err != nil {
		t.Fatal(err)
	}
	n, _ := f.store.GetNode(ctx, "n1")
	if !n.LastHeartbeat.Equal(t0.Add(5 * time.Second)) {
		t.Errorf("LastHeartbeat = %s", n.LastHeartbeat)
	}
	if got := f.reg.Sessions()[0].LastSeen; !got.Equal(t0.Add(5 * time.Second)) {
		t.Errorf("session LastSeen = %s", got)
	}
}

func TestHeartbeatUnknownNodeOrSession(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if _, err := f.reg.Heartbeat(ctx, "ghost", "x", nil); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("unknown node error = %v, want ErrNotRegistered", err)
	}
	if _, _, err := f.reg.Register(ctx, node("n1")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.reg.Heartbeat(ctx, "n1", "forged", nil); !errors.Is(err, ErrSessionSuperseded) {
		t.Fatalf("forged session error = %v, want ErrSessionSuperseded", err)
	}
}

func TestHeartbeatAppliesStatusReportsOnce(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	s, _, _ := f.reg.Register(ctx, node("n1"))
	if _, _, err := f.reg.Register(ctx, node("n2")); err != nil {
		t.Fatal(err)
	}
	f.addWorkload(t, "w1", "n1")
	f.addWorkload(t, "w2", "n2")
	f.clock = f.clock.Add(time.Second)

	reports := []StatusReport{
		{WorkloadID: "w1", State: store.WorkloadPending, Message: "accepted"},
		{WorkloadID: "w2", State: store.WorkloadRunning, Message: "belongs to another node"},
		{WorkloadID: "ghost", State: store.WorkloadRunning},
	}
	assigned, err := f.reg.Heartbeat(ctx, "n1", s.ID, reports)
	if err != nil {
		t.Fatal(err)
	}
	if len(assigned) != 1 || assigned[0].ID != "w1" || assigned[0].State != store.WorkloadPending || assigned[0].StatusMessage != "accepted" {
		t.Fatalf("returned assignments = %+v", assigned)
	}
	w1, _ := f.store.GetWorkload(ctx, "w1")
	if w1.State != store.WorkloadPending || w1.StatusUpdatedAt == nil || !w1.StatusUpdatedAt.Equal(t0.Add(time.Second)) {
		t.Fatalf("stored w1 = %+v", w1)
	}
	w2, _ := f.store.GetWorkload(ctx, "w2")
	if w2.State != store.WorkloadScheduled {
		t.Fatalf("a node must not update another node's workload: %+v", w2)
	}
	if f.store.statusWrites != 1 {
		t.Fatalf("status writes = %d, want 1", f.store.statusWrites)
	}

	if _, err := f.reg.Heartbeat(ctx, "n1", s.ID, reports); err != nil {
		t.Fatal(err)
	}
	if f.store.statusWrites != 1 {
		t.Fatalf("unchanged report must not be written again, writes = %d", f.store.statusWrites)
	}
}

func TestDeregisterMarksNodeNotReady(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	s, _, _ := f.reg.Register(ctx, node("n1"))

	if err := f.reg.Deregister(ctx, "n1", "forged"); !errors.Is(err, ErrSessionSuperseded) {
		t.Fatalf("forged deregister error = %v, want ErrSessionSuperseded", err)
	}
	if err := f.reg.Deregister(ctx, "n1", s.ID); err != nil {
		t.Fatal(err)
	}
	if len(f.reg.Sessions()) != 0 {
		t.Fatalf("session must be gone: %+v", f.reg.Sessions())
	}
	n, _ := f.store.GetNode(ctx, "n1")
	if scheduler.Ready(n, t0, time.Hour) {
		t.Fatalf("deregistered node must not be ready: %+v", n)
	}
	if _, err := f.reg.Heartbeat(ctx, "n1", s.ID, nil); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("heartbeat after deregister error = %v, want ErrNotRegistered", err)
	}
	if err := f.reg.Deregister(ctx, "n1", s.ID); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("second deregister error = %v, want ErrNotRegistered", err)
	}
}
