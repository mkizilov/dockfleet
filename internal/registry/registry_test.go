package registry

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
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

// InTx counts the observations written inside a transaction, which is where
// the registry writes them together with their events.
func (c *countingStore) InTx(ctx context.Context, fn func(store.Tx) error) error {
	return c.Memory.InTx(ctx, func(tx store.Tx) error { return fn(countingTx{Tx: tx, c: c}) })
}

type countingTx struct {
	store.Tx
	c *countingStore
}

func (t countingTx) RecordObservations(ctx context.Context, nodeID string, obs []store.Observation) error {
	t.c.observationWrites += len(obs)
	return t.Tx.RecordObservations(ctx, nodeID, obs)
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

// CheckSession is what a long-lived stream bound to a session asks. It changes
// nothing but the end of a session whose enrollment was revoked.
func TestCheckSessionFollowsTheSessionAndTheEnrollment(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if err := f.reg.CheckSession(ctx, "n1", "nope"); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("before registering: %v, want ErrNotRegistered", err)
	}
	s, _, err := f.reg.Register(ctx, node("n1"))
	if err != nil {
		t.Fatal(err)
	}
	before := f.store.observationWrites
	if err := f.reg.CheckSession(ctx, "n1", s.ID); err != nil {
		t.Fatalf("the current session: %v", err)
	}
	if err := f.reg.CheckSession(ctx, "n1", "another"); !errors.Is(err, ErrSessionSuperseded) {
		t.Fatalf("another session: %v, want ErrSessionSuperseded", err)
	}
	if err := f.reg.CheckSession(ctx, "n2", s.ID); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("another node: %v, want ErrNotRegistered", err)
	}
	n, _ := f.store.GetNode(ctx, "n1")
	if f.store.observationWrites != before || !n.LastHeartbeat.Equal(t0) {
		t.Fatal("checking a session must not write observations or refresh liveness")
	}

	newer, _, err := f.reg.Register(ctx, node("n1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.reg.CheckSession(ctx, "n1", s.ID); !errors.Is(err, ErrSessionSuperseded) {
		t.Fatalf("the replaced session: %v, want ErrSessionSuperseded", err)
	}

	if _, err := f.store.RevokeEnrollment(ctx, "n1", t0); err != nil {
		t.Fatal(err)
	}
	if err := f.reg.CheckSession(ctx, "n1", newer.ID); !errors.Is(err, ErrNotEnrolled) {
		t.Fatalf("after revocation: %v, want ErrNotEnrolled", err)
	}
	if len(f.reg.Sessions()) != 0 {
		t.Fatalf("a revoked node keeps its session: %+v", f.reg.Sessions())
	}
	if err := f.reg.CheckSession(ctx, "n1", newer.ID); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("after the session ended: %v, want ErrNotRegistered", err)
	}
	if !strings.Contains(strings.Join(f.events(t), " "), "node.session_ended") {
		t.Fatalf("events = %v; ending the session must be recorded", f.events(t))
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

// A workload that is meant to stop stays in the assignments, now with the
// desired state "stopped": that is the instruction to stop and remove its
// container. It leaves the list only when the manager deletes it.
func TestStoppedWorkloadsAreAssignedAsStopped(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	s, _, _ := f.reg.Register(ctx, node("n1"))
	f.addWorkload(t, "keep", "n1")
	f.addWorkload(t, "stop", "n1")
	if _, err := f.store.SetWorkloadDesiredState(ctx, "stop", store.DesiredStopped, t0); err != nil {
		t.Fatal(err)
	}

	check := func(what string, got []store.Workload, err error) {
		t.Helper()
		if err != nil || len(got) != 2 ||
			got[0].ID != "keep" || got[0].DesiredState != store.DesiredRunning || got[0].Generation != 1 ||
			got[1].ID != "stop" || got[1].DesiredState != store.DesiredStopped || got[1].Generation != 2 {
			t.Fatalf("%s assignments = %+v, %v", what, got, err)
		}
	}
	assigned, err := f.reg.Heartbeat(ctx, "n1", s.ID, nil)
	check("heartbeat", assigned, err)
	_, onRegister, err := f.reg.Register(ctx, node("n1"))
	check("registration", onRegister, err)

	if err := f.store.DeleteWorkload(ctx, "stop"); err != nil {
		t.Fatal(err)
	}
	s2, _, _ := f.reg.Register(ctx, node("n1"))
	if assigned, err := f.reg.Heartbeat(ctx, "n1", s2.ID, nil); err != nil || len(assigned) != 1 || assigned[0].ID != "keep" {
		t.Fatalf("after the record was deleted: %+v, %v", assigned, err)
	}
}

// What the agent acknowledges is stored beside the observation. It cannot
// name a generation that does not exist yet, and it never goes back.
func TestAcknowledgmentsAreClampedAndNeverGoBack(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	s, _, _ := f.reg.Register(ctx, node("n1"))
	f.addWorkload(t, "w1", "n1")
	exit := 0
	report := func(acked int64) {
		t.Helper()
		f.clock = f.clock.Add(time.Second)
		reports := []StatusReport{{WorkloadID: "w1", State: store.WorkloadRunning, ContainerID: "c1", ExitCode: &exit, RestartCount: 2, AckedGeneration: acked}}
		if _, err := f.reg.Heartbeat(ctx, "n1", s.ID, reports); err != nil {
			t.Fatal(err)
		}
	}
	stored := func() store.Observation {
		t.Helper()
		w, err := f.store.GetWorkload(ctx, "w1")
		if err != nil || w.Observed == nil {
			t.Fatalf("workload = %+v, %v", w, err)
		}
		return *w.Observed
	}

	report(99)
	if o := stored(); o.AckedGeneration != 1 || o.ContainerID != "c1" || o.ExitCode == nil || *o.ExitCode != 0 || o.RestartCount != 2 {
		t.Fatalf("observation = %+v; generation 99 does not exist, the workload is at generation 1", o)
	}

	if _, err := f.store.SetWorkloadDesiredState(ctx, "w1", store.DesiredStopped, f.clock); err != nil {
		t.Fatal(err)
	}
	report(1)
	if got := stored().AckedGeneration; got != 1 {
		t.Fatalf("acknowledged generation = %d, want 1 until the agent sees generation 2", got)
	}
	report(2)
	if got := stored().AckedGeneration; got != 2 {
		t.Fatalf("acknowledged generation = %d, want 2", got)
	}
	report(1)
	report(0)
	if got := stored().AckedGeneration; got != 2 {
		t.Fatalf("acknowledged generation went back to %d", got)
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

// A report that says a workload failed, turned unhealthy, was restarted or
// came back is an audit event, once per change: the same report repeated says
// nothing new.
func TestFailuresAreRecordedAsEventsOnce(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	s, _, _ := f.reg.Register(ctx, node("n1"))
	f.addWorkload(t, "w1", "n1")
	exit := 1
	report := func(r StatusReport) {
		t.Helper()
		f.clock = f.clock.Add(time.Second)
		r.WorkloadID = "w1"
		if _, err := f.reg.Heartbeat(ctx, "n1", s.ID, []StatusReport{r}); err != nil {
			t.Fatal(err)
		}
	}

	report(StatusReport{State: store.WorkloadRunning, ContainerID: "c1"})
	report(StatusReport{State: store.WorkloadFailed, ContainerID: "c1", ExitCode: &exit, Message: "exited with code 1"})
	report(StatusReport{State: store.WorkloadFailed, ContainerID: "c1", ExitCode: &exit, Message: "exited with code 1"})
	report(StatusReport{State: store.WorkloadRunning, ContainerID: "c1", RestartCount: 1})
	report(StatusReport{State: store.WorkloadRunning, ContainerID: "c1", RestartCount: 1, Health: store.HealthUnhealthy})
	report(StatusReport{State: store.WorkloadRunning, ContainerID: "c1", RestartCount: 1, Health: store.HealthUnhealthy, Message: "still"})
	report(StatusReport{State: store.WorkloadRunning, ContainerID: "c1", RestartCount: 1, Health: store.HealthHealthy})

	want := []string{
		"node.registered",
		"workload.failed",
		"workload.recovered", "workload.restarted",
		"workload.unhealthy",
		"workload.recovered",
	}
	if got := f.events(t); !equal(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	list, err := f.store.ListEvents(ctx, store.EventFilter{ResourceType: "workload", ResourceID: "w1"})
	if err != nil || len(list) != 5 {
		t.Fatalf("workload events = %+v, %v", list, err)
	}
	failed := list[4]
	if failed.Action != "workload.failed" || failed.Actor != "node:n1" || failed.Details["node_id"] != "n1" || failed.Message == "" {
		t.Fatalf("failure event = %+v", failed)
	}
	if w, _ := f.store.GetWorkload(ctx, "w1"); w.Observed == nil || w.Observed.Health != store.HealthHealthy {
		t.Fatalf("the health result must be stored with the observation: %+v", w.Observed)
	}
}

// The first report of a workload that is fine says nothing, and neither does a
// restart count that went back to zero because the agent restarted.
func TestHealthyReportsAndAgentRestartsAreNotFailures(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	s, _, _ := f.reg.Register(ctx, node("n1"))
	f.addWorkload(t, "w1", "n1")
	for i, r := range []StatusReport{
		{State: store.WorkloadPending, Message: "starting"},
		{State: store.WorkloadRunning, Health: store.HealthStarting},
		{State: store.WorkloadRunning, Health: store.HealthHealthy, RestartCount: 3},
		{State: store.WorkloadRunning, Health: store.HealthHealthy, RestartCount: 0},
	} {
		f.clock = f.clock.Add(time.Second)
		r.WorkloadID = "w1"
		if _, err := f.reg.Heartbeat(ctx, "n1", s.ID, []StatusReport{r}); err != nil {
			t.Fatalf("report %d: %v", i, err)
		}
	}
	// The third report raised the restart count from 0 to 3.
	want := []string{"node.registered", "workload.restarted"}
	if got := f.events(t); !equal(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

// A failed report and its event are saved together.
func TestObservationsAndTheirEventsSucceedOrFailTogether(t *testing.T) {
	mem := store.NewMemory()
	ctx := context.Background()
	if _, err := mem.CreateEnrollment(ctx, store.Enrollment{NodeID: "n1", CreatedAt: t0}); err != nil {
		t.Fatal(err)
	}
	reg := New(mem, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s, _, err := reg.Register(ctx, node("n1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mem.CreateWorkload(ctx, store.Workload{ID: "w1", Name: "w1", Image: "img", CPUMillis: 1, MemoryBytes: 1, NodeID: "n1", CreatedAt: t0}); err != nil {
		t.Fatal(err)
	}
	broken := New(failingEventStore{mem}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	broken.sessions = reg.sessions
	_, err = broken.Heartbeat(ctx, "n1", s.ID, []StatusReport{{WorkloadID: "w1", State: store.WorkloadFailed, Message: "boom"}})
	if err == nil {
		t.Fatal("a report whose event cannot be recorded must fail")
	}
	if w, _ := mem.GetWorkload(ctx, "w1"); w.Observed != nil {
		t.Fatalf("the observation was saved although its event was not: %+v", w.Observed)
	}
}

// A clean departure is remembered with its time: that is where the clock for
// "unavailable" starts, because the node's containers keep running.
func TestDeregisterRecordsWhenTheAgentLeft(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	s, _, _ := f.reg.Register(ctx, node("n1"))
	f.clock = f.clock.Add(time.Minute)
	if err := f.reg.Deregister(ctx, "n1", s.ID); err != nil {
		t.Fatal(err)
	}
	n, _ := f.store.GetNode(ctx, "n1")
	if !n.DeregisteredAt.Equal(f.clock) || !n.LastHeartbeat.IsZero() {
		t.Fatalf("node after leaving = %+v", n)
	}
	f.clock = f.clock.Add(time.Minute)
	if _, _, err := f.reg.Register(ctx, node("n1")); err != nil {
		t.Fatal(err)
	}
	n, _ = f.store.GetNode(ctx, "n1")
	if !n.DeregisteredAt.IsZero() || !n.LastHeartbeat.Equal(f.clock) {
		t.Fatalf("registering again must clear the departure: %+v", n)
	}
}
