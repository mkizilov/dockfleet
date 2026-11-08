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

// countingStore counts the observations written so tests can assert that
// unchanged reports are skipped.
type countingStore struct {
	*store.Memory
	observationWrites int
}

func (c *countingStore) RecordObservations(ctx context.Context, nodeID string, obs []store.Observation) error {
	c.observationWrites += len(obs)
	return c.Memory.RecordObservations(ctx, nodeID, obs)
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
		ID: id, Name: id, Image: "img", CPUMillis: 1, MemoryBytes: 1, NodeID: nodeID, CreatedAt: t0,
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

func (f *fixture) events(t *testing.T) []string {
	t.Helper()
	list, err := f.store.ListEvents(context.Background(), store.EventFilter{})
	if err != nil {
		t.Fatal(err)
	}
	var actions []string
	for i := len(list) - 1; i >= 0; i-- { // oldest first
		actions = append(actions, list[i].Action)
	}
	return actions
}

func TestSessionLifecycleIsRecordedAsEvents(t *testing.T) {
	f := newFixture(t)
	f.reg.WithInstanceID("manager-7")
	ctx := context.Background()

	s, _, _ := f.reg.Register(ctx, node("n1"))
	if err := f.reg.Deregister(ctx, "n1", s.ID); err != nil {
		t.Fatal(err)
	}
	s, _, _ = f.reg.Register(ctx, node("n1"))
	if _, err := f.store.RevokeEnrollment(ctx, "n1", f.clock); err != nil {
		t.Fatal(err)
	}
	if _, err := f.reg.Heartbeat(ctx, "n1", s.ID, nil); !errors.Is(err, ErrNotEnrolled) {
		t.Fatalf("error = %v", err)
	}

	want := []string{"node.registered", "node.deregistered", "node.registered", "node.session_ended"}
	if got := f.events(t); !equal(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	list, _ := f.store.ListEvents(ctx, store.EventFilter{ResourceType: "node", ResourceID: "n1"})
	if len(list) != 4 || list[0].InstanceID != "manager-7" || list[0].Actor != "node:n1" || list[0].Details["reason"] != "enrollment revoked" {
		t.Fatalf("newest event = %+v", list)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// failingEventStore makes every event write fail, to prove registration and
// its audit event succeed or fail together.
type failingEventStore struct{ *store.Memory }

func (f failingEventStore) InTx(ctx context.Context, fn func(store.Tx) error) error {
	return f.Memory.InTx(ctx, func(tx store.Tx) error { return fn(failingEventTx{tx}) })
}

type failingEventTx struct{ store.Tx }

func (failingEventTx) AppendEvent(context.Context, store.Event) (store.Event, error) {
	return store.Event{}, errors.New("event log unavailable")
}

func TestRegistrationAndItsEventSucceedOrFailTogether(t *testing.T) {
	mem := store.NewMemory()
	ctx := context.Background()
	if _, err := mem.CreateEnrollment(ctx, store.Enrollment{NodeID: "n1", CreatedAt: t0}); err != nil {
		t.Fatal(err)
	}
	reg := New(failingEventStore{mem}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if _, _, err := reg.Register(ctx, node("n1")); err == nil {
		t.Fatal("registration must fail when its event cannot be recorded")
	}
	if _, err := mem.GetNode(ctx, "n1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the node was saved although the transaction failed: %v", err)
	}
	if len(reg.Sessions()) != 0 {
		t.Fatalf("no session may exist for a failed registration: %+v", reg.Sessions())
	}
}

func TestStoppedWorkloadsAreNotAssigned(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	s, _, _ := f.reg.Register(ctx, node("n1"))
	f.addWorkload(t, "keep", "n1")
	f.addWorkload(t, "stop", "n1")
	if _, err := f.store.SetWorkloadDesiredState(ctx, "stop", store.DesiredStopped, t0); err != nil {
		t.Fatal(err)
	}

	assigned, err := f.reg.Heartbeat(ctx, "n1", s.ID, nil)
	if err != nil || len(assigned) != 1 || assigned[0].ID != "keep" {
		t.Fatalf("heartbeat assignments = %+v, %v", assigned, err)
	}
	_, onRegister, err := f.reg.Register(ctx, node("n1"))
	if err != nil || len(onRegister) != 1 || onRegister[0].ID != "keep" {
		t.Fatalf("registration assignments = %+v, %v", onRegister, err)
	}
}

func TestHeartbeatNeverChangesDesiredState(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	s, _, _ := f.reg.Register(ctx, node("n1"))
	f.addWorkload(t, "w1", "n1")

	// The agent says the container is stopped; the administrator still wants it running.
	if _, err := f.reg.Heartbeat(ctx, "n1", s.ID, []StatusReport{{WorkloadID: "w1", State: store.WorkloadStopped}}); err != nil {
		t.Fatal(err)
	}
	w, _ := f.store.GetWorkload(ctx, "w1")
	if w.DesiredState != store.DesiredRunning || w.Phase() != store.WorkloadStopped || !w.UpdatedAt.Equal(t0) {
		t.Fatalf("observed state leaked into desired state: %+v", w)
	}
}

func TestHeartbeatRejectsUnknownPhasesWithoutStoringAnything(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	s, _, _ := f.reg.Register(ctx, node("n1"))
	f.addWorkload(t, "w1", "n1")
	f.addWorkload(t, "w2", "n1")

	_, err := f.reg.Heartbeat(ctx, "n1", s.ID, []StatusReport{
		{WorkloadID: "w1", State: store.WorkloadRunning},
		{WorkloadID: "w2", State: "exploded"},
	})
	if !errors.Is(err, store.ErrInvalid) {
		t.Fatalf("error = %v, want store.ErrInvalid", err)
	}
	if w1, _ := f.store.GetWorkload(ctx, "w1"); w1.Observed != nil {
		t.Fatalf("half a rejected batch was stored: %+v", w1.Observed)
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
	if len(assigned) != 1 || assigned[0].ID != "w1" || assigned[0].Phase() != store.WorkloadPending || assigned[0].Observed == nil || assigned[0].Observed.Message != "accepted" {
		t.Fatalf("returned assignments = %+v", assigned)
	}
	w1, _ := f.store.GetWorkload(ctx, "w1")
	if w1.Observed == nil || w1.Observed.State != store.WorkloadPending || w1.Observed.NodeID != "n1" || !w1.Observed.ObservedAt.Equal(t0.Add(time.Second)) {
		t.Fatalf("stored w1 = %+v", w1.Observed)
	}
	w2, _ := f.store.GetWorkload(ctx, "w2")
	if w2.Observed != nil || w2.Phase() != store.WorkloadScheduled {
		t.Fatalf("a node must not update another node's workload: %+v", w2)
	}
	if f.store.observationWrites != 1 {
		t.Fatalf("observation writes = %d, want 1", f.store.observationWrites)
	}

	f.clock = f.clock.Add(time.Second)
	if _, err := f.reg.Heartbeat(ctx, "n1", s.ID, reports); err != nil {
		t.Fatal(err)
	}
	if f.store.observationWrites != 1 {
		t.Fatalf("unchanged report must not be written again, writes = %d", f.store.observationWrites)
	}
	if w1, _ := f.store.GetWorkload(ctx, "w1"); !w1.Observed.ObservedAt.Equal(t0.Add(time.Second)) {
		t.Fatalf("an unchanged report must leave the observation as it was: %+v", w1.Observed)
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
