package reconcile

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/placement"
	"github.com/dockfleet/dockfleet/internal/store"
)

// The suite takes a store factory: it runs against the in-memory store always
// and against PostgreSQL under the `integration` build tag.
//
// The control loop under test is the manager's: the scheduler places the
// replicas that have no workload, the reconciler retires the ones that should
// go, and a stand-in for the agents reports what a real agent reports. The
// stand-in never decides anything; it only does what each assignment says.

var t0 = time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)

type env struct {
	t        *testing.T
	st       store.Store
	clock    time.Time
	obsAt    time.Time
	rec      *Reconciler
	place    *placement.Scheduler
	holdBack func(store.Workload) bool
}

func newEnv(t *testing.T, st store.Store) *env {
	t.Helper()
	e := &env{t: t, st: st, clock: t0, obsAt: t0}
	now := func() time.Time { return e.clock }
	e.rec = New(Config{Store: st, InstanceID: "mgr-1", Now: now})
	e.place = placement.New(placement.Config{Store: st, NodeTTL: time.Minute, InstanceID: "mgr-1", Now: now})
	return e
}

func (e *env) ctx() context.Context { return context.Background() }

func (e *env) must(err error) {
	e.t.Helper()
	if err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) node(id string) {
	e.t.Helper()
	_, err := e.st.UpsertNode(e.ctx(), store.Node{ID: id, Name: id, CPUMillis: 4000, MemoryBytes: 8 << 30, RegisteredAt: e.clock, LastHeartbeat: e.clock})
	e.must(err)
}

func spec(image string) store.ServiceSpec {
	return store.ServiceSpec{Image: image, CPUMillis: 500, MemoryBytes: 128 << 20}
}

func (e *env) service(name string, replicas int, image string) store.Service {
	e.t.Helper()
	svc, err := e.st.CreateService(e.ctx(), store.NewService{ID: "svc-" + name, Name: name, Replicas: replicas, Spec: spec(image), CreatedBy: "test", At: e.clock})
	e.must(err)
	return svc
}

func (e *env) scale(name string, replicas int) {
	e.t.Helper()
	svc, err := e.st.GetServiceByName(e.ctx(), name)
	e.must(err)
	_, err = e.st.ScaleService(e.ctx(), svc.ID, replicas, 0, e.clock)
	e.must(err)
}

func (e *env) deploy(name, image string) {
	e.t.Helper()
	svc, err := e.st.GetServiceByName(e.ctx(), name)
	e.must(err)
	_, _, err = e.st.DeployService(e.ctx(), svc.ID, store.Deployment{Spec: spec(image), CreatedBy: "test", At: e.clock})
	e.must(err)
}

func (e *env) remove(name string) {
	e.t.Helper()
	svc, err := e.st.GetServiceByName(e.ctx(), name)
	e.must(err)
	_, err = e.st.DeleteService(e.ctx(), svc.ID, 0, e.clock)
	e.must(err)
}

// agents plays every node's agent for one heartbeat: it reports each assigned
// workload the way a real agent does. A workload meant to run is running
// (unless the test holds it back), a workload meant to be stopped is stopped
// with no container, and the instruction's generation is acknowledged.
func (e *env) agents() {
	e.t.Helper()
	nodes, err := e.st.ListNodes(e.ctx())
	e.must(err)
	for _, n := range nodes {
		workloads, err := e.st.ListWorkloadsForNode(e.ctx(), n.ID)
		e.must(err)
		var reports []store.Observation
		for _, w := range workloads {
			e.obsAt = e.obsAt.Add(time.Second)
			o := store.Observation{WorkloadID: w.ID, AckedGeneration: w.Generation, ObservedAt: e.obsAt}
			switch {
			case w.DesiredState == store.DesiredStopped:
				o.State = store.WorkloadStopped
			case e.holdBack != nil && e.holdBack(w):
				o.State = store.WorkloadPending
			default:
				o.State, o.ContainerID = store.WorkloadRunning, "container-"+w.ID
			}
			reports = append(reports, o)
		}
		e.must(e.st.RecordObservations(e.ctx(), n.ID, reports))
	}
}

// cycle is one round of the control loop, and whether it changed anything.
func (e *env) cycle() (changed bool) {
	e.t.Helper()
	placed, err := e.place.RunOnce(e.ctx())
	e.must(err)
	reconciled, err := e.rec.RunOnce(e.ctx())
	e.must(err)
	before := e.fingerprint()
	e.agents()
	return placed.Placed > 0 || reconciled != (Report{}) || before != e.fingerprint()
}

// fingerprint summarizes every workload's observation, to notice when an agent's report changed something.
func (e *env) fingerprint() string {
	all, err := e.st.ListWorkloads(e.ctx())
	e.must(err)
	s := ""
	for _, w := range all {
		o := ""
		if w.Observed != nil {
			o = fmt.Sprintf("%s/%d/%q", w.Observed.State, w.Observed.AckedGeneration, w.Observed.ContainerID)
		}
		s += fmt.Sprintf("%s:%s:%d:%s;", w.ID, w.DesiredState, w.Generation, o)
	}
	return s
}

// settle runs the loop until two rounds in a row change nothing.
func (e *env) settle() {
	e.t.Helper()
	quiet := 0
	for i := 0; i < 40; i++ {
		if e.cycle() {
			quiet = 0
		} else if quiet++; quiet == 2 {
			return
		}
	}
	e.t.Fatalf("the control loop did not settle in 40 rounds:\n%s", e.describe())
}

func (e *env) workloads(name string) []store.Workload {
	e.t.Helper()
	svc, err := e.st.GetServiceByName(e.ctx(), name)
	if errors.Is(err, store.ErrNotFound) {
		svc, err = e.st.GetService(e.ctx(), "svc-"+name)
	}
	e.must(err)
	list, err := e.st.ListWorkloadsForService(e.ctx(), svc.ID)
	e.must(err)
	return list
}

func (e *env) describe() string {
	all, err := e.st.ListWorkloads(e.ctx())
	e.must(err)
	s := ""
	for _, w := range all {
		state := "-"
		if w.Observed != nil {
			state = w.Observed.State
		}
		s += fmt.Sprintf("  %s revision %d replica %d desired %s generation %d observed %s\n", w.Name, w.ServiceRevision, w.ReplicaIndex, w.DesiredState, w.Generation, state)
	}
	return s
}

func (e *env) events(action string) int {
	e.t.Helper()
	all, err := e.st.ListEvents(e.ctx(), store.EventFilter{Limit: store.MaxEventLimit})
	e.must(err)
	n := 0
	for _, ev := range all {
		if ev.Action == action {
			n++
		}
	}
	return n
}

// wantReplicas checks that a service has exactly one running workload for each replica 1..n of a revision and nothing else.
func (e *env) wantReplicas(name string, revision, n int, image string) []store.Workload {
	e.t.Helper()
	list := e.workloads(name)
	if len(list) != n {
		e.t.Fatalf("%s has %d workloads, want %d:\n%s", name, len(list), n, e.describe())
	}
	for i, w := range list {
		if w.ServiceRevision != revision || w.ReplicaIndex != i+1 || w.Image != image || w.DesiredState != store.DesiredRunning ||
			w.Observed == nil || w.Observed.State != store.WorkloadRunning || w.Observed.AckedGeneration != w.Generation {
			e.t.Fatalf("replica %d of %s = %+v (observed %+v), want revision %d running on %s:\n%s", i+1, name, w, w.Observed, revision, image, e.describe())
		}
	}
	return list
}

// everyReplicaRuns is the rollout invariant: at no point is a replica without a running workload.
func (e *env) everyReplicaRuns(name string, n int) {
	e.t.Helper()
	runs := map[int]bool{}
	for _, w := range e.workloads(name) {
		if w.DesiredState == store.DesiredRunning && w.Observed != nil && w.Observed.State == store.WorkloadRunning {
			runs[w.ReplicaIndex] = true
		}
	}
	for i := 1; i <= n; i++ {
		if !runs[i] {
			e.t.Fatalf("replica %d of %s is not running:\n%s", i, name, e.describe())
		}
	}
}

type reconcileTest struct {
	name string
	run  func(t *testing.T, newStore func(t *testing.T) store.Store)
}

var reconcileTests = []reconcileTest{
	{"the control loop deploys, scales, rolls out and removes a service", testControlLoopLifecycle},
	{"a replica that never comes up keeps the old one", testFailedRolloutKeepsTheOldReplica},
	{"a fixed host port is handed over to the new revision", testFixedHostPortIsHandedOver},
	{"repeating a pass changes nothing", testPassesAreIdempotent},
	{"a failed pass saves nothing", testFailedPassIsRolledBack},
	{"the loop retires replicas and stops", testRunLoop},
}

func runReconcileTests(t *testing.T, newStore func(t *testing.T) store.Store) {
	t.Helper()
	for _, tt := range reconcileTests {
		t.Run(tt.name, func(t *testing.T) { tt.run(t, newStore) })
	}
}

func TestReconciliationWithMemoryStore(t *testing.T) {
	runReconcileTests(t, func(*testing.T) store.Store { return store.NewMemory() })
}

func testControlLoopLifecycle(t *testing.T, newStore func(t *testing.T) store.Store) {
	e := newEnv(t, newStore(t))
	e.node("n1")
	e.node("n2")

	// Deploy: three replicas, spread over the nodes.
	e.service("web", 3, "nginx:1.0")
	e.settle()
	first := e.wantReplicas("web", 1, 3, "nginx:1.0")
	onNode := map[string]int{}
	for _, w := range first {
		onNode[w.NodeID]++
	}
	if len(onNode) != 2 || e.events("replica.scheduled") != 3 {
		t.Fatalf("replicas per node %v, %d scheduling events", onNode, e.events("replica.scheduled"))
	}

	// Repeating the loop changes nothing: same workloads, same IDs, no new events.
	for i := 0; i < 5; i++ {
		e.cycle()
	}
	again := e.wantReplicas("web", 1, 3, "nginx:1.0")
	for i := range first {
		if again[i].ID != first[i].ID || again[i].Generation != 1 {
			t.Fatalf("workload %d changed under repeated passes: %+v -> %+v", i+1, first[i], again[i])
		}
	}
	if e.events("replica.scheduled") != 3 || e.events("replica.stopped") != 0 {
		t.Fatalf("repeated passes recorded events: scheduled %d, stopped %d", e.events("replica.scheduled"), e.events("replica.stopped"))
	}

	// Scale down: the highest replicas are stopped, confirmed gone, and deleted.
	e.scale("web", 1)
	e.settle()
	survivor := e.wantReplicas("web", 1, 1, "nginx:1.0")
	if survivor[0].ID != first[0].ID {
		t.Fatalf("scaling down replaced replica 1: %s -> %s", first[0].ID, survivor[0].ID)
	}
	if e.events("replica.stopped") != 2 || e.events("replica.removed") != 2 {
		t.Fatalf("stopped %d, removed %d, want 2 and 2", e.events("replica.stopped"), e.events("replica.removed"))
	}

	// Scale up again: the freed slots are filled by new workloads, never by the old IDs.
	e.scale("web", 3)
	e.settle()
	third := e.wantReplicas("web", 1, 3, "nginx:1.0")
	if third[0].ID != first[0].ID {
		t.Fatalf("replica 1 was replaced: %s -> %s", first[0].ID, third[0].ID)
	}
	seen := map[string]bool{first[0].ID: true, first[1].ID: true, first[2].ID: true}
	for _, w := range third[1:] {
		if seen[w.ID] {
			t.Fatalf("workload ID %s was used twice", w.ID)
		}
		seen[w.ID] = true
	}

	// Roll out a new revision: each replica is replaced only after its successor runs.
	e.deploy("web", "nginx:2.0")
	for i := 0; i < 40; i++ {
		e.cycle()
		e.everyReplicaRuns("web", 3)
	}
	rolled := e.wantReplicas("web", 2, 3, "nginx:2.0")
	for _, w := range rolled {
		if seen[w.ID] {
			t.Fatalf("workload ID %s was used twice", w.ID)
		}
	}

	// Delete the service: every replica is stopped, confirmed gone and removed.
	e.remove("web")
	e.settle()
	if left := e.workloads("web"); len(left) != 0 {
		t.Fatalf("workloads left after deleting the service:\n%s", e.describe())
	}
	if all, err := e.st.ListWorkloads(e.ctx()); err != nil || len(all) != 0 {
		t.Fatalf("workloads = %+v, %v", all, err)
	}
}

func testFailedRolloutKeepsTheOldReplica(t *testing.T, newStore func(t *testing.T) store.Store) {
	e := newEnv(t, newStore(t))
	e.node("n1")
	e.service("web", 2, "nginx:1.0")
	e.settle()
	old := e.wantReplicas("web", 1, 2, "nginx:1.0")

	// The new revision's containers never start.
	e.holdBack = func(w store.Workload) bool { return w.ServiceRevision == 2 }
	e.deploy("web", "broken:1")
	e.settle()
	list := e.workloads("web")
	if len(list) != 4 {
		t.Fatalf("workloads:\n%s", e.describe())
	}
	for _, w := range list[:2] {
		if w.ServiceRevision != 1 || w.DesiredState != store.DesiredRunning || w.Generation != 1 || w.Observed.State != store.WorkloadRunning {
			t.Fatalf("an old replica was touched although nothing replaced it: %+v", w)
		}
	}
	if e.events("replica.stopped") != 0 {
		t.Fatalf("a replica was stopped without a successor:\n%s", e.describe())
	}

	// Once the new containers run, the old ones go.
	e.holdBack = nil
	e.settle()
	rolled := e.wantReplicas("web", 2, 2, "broken:1")
	if rolled[0].ID == old[0].ID {
		t.Fatal("the new revision reused a workload")
	}
}

// Two replicas cannot publish the same host port on one node, so the new
// revision can only start once the old one has let go of it.
func testFixedHostPortIsHandedOver(t *testing.T, newStore func(t *testing.T) store.Store) {
	e := newEnv(t, newStore(t))
	e.node("n1")
	withPort := func(image string) store.ServiceSpec {
		s := spec(image)
		s.Ports = []store.ServicePort{{ContainerPort: 80, Protocol: "tcp", HostPort: 18080}}
		return s
	}
	svc, err := e.st.CreateService(e.ctx(), store.NewService{ID: "svc-web", Name: "web", Replicas: 1, Spec: withPort("nginx:1.0"), CreatedBy: "test", At: e.clock})
	e.must(err)
	e.settle()
	old := e.wantReplicas("web", 1, 1, "nginx:1.0")

	_, _, err = e.st.DeployService(e.ctx(), svc.ID, store.Deployment{Spec: withPort("nginx:2.0"), CreatedBy: "test", At: e.clock})
	e.must(err)
	e.settle()
	rolled := e.wantReplicas("web", 2, 1, "nginx:2.0")
	if rolled[0].ID == old[0].ID {
		t.Fatal("the new revision reused the old workload")
	}
	events, err := e.st.ListEvents(e.ctx(), store.EventFilter{ResourceType: "workload", Limit: store.MaxEventLimit})
	e.must(err)
	stopped := 0
	for _, ev := range events {
		if ev.Action == "replica.stopped" {
			stopped++
			if ev.ResourceID != old[0].ID || ev.Details["reason"] != ReasonPortHandover {
				t.Fatalf("event = %+v", ev)
			}
		}
	}
	if stopped != 1 {
		t.Fatalf("%d replicas were stopped, want the old one only", stopped)
	}
}

func testPassesAreIdempotent(t *testing.T, newStore func(t *testing.T) store.Store) {
	e := newEnv(t, newStore(t))
	e.node("n1")
	e.service("web", 3, "nginx:1.0")
	e.settle()
	e.scale("web", 1)

	report, err := e.rec.RunOnce(e.ctx())
	e.must(err)
	if report.Stopped != 2 || report.Deleted != 0 {
		t.Fatalf("first pass = %+v", report)
	}
	before := e.fingerprint()
	for i := 0; i < 3; i++ {
		e.clock = e.clock.Add(time.Second)
		report, err := e.rec.RunOnce(e.ctx())
		e.must(err)
		if report != (Report{}) {
			t.Fatalf("pass %d = %+v; the instruction was already given", i+2, report)
		}
	}
	if e.fingerprint() != before || e.events("replica.stopped") != 2 {
		t.Fatalf("repeated passes changed the workloads or recorded events again (%d stopped events)", e.events("replica.stopped"))
	}
	// A workload's generation counts instructions: stopping it once is generation 2, however often the pass runs.
	for _, w := range e.workloads("web")[1:] {
		if w.DesiredState != store.DesiredStopped || w.Generation != 2 {
			t.Fatalf("workload %+v", w)
		}
	}
}

// failingEventStore makes every audit event fail, to show that a pass either
// saves everything or nothing.
type failingEventStore struct{ store.Store }

func (f failingEventStore) InTx(ctx context.Context, fn func(store.Tx) error) error {
	return f.Store.InTx(ctx, func(tx store.Tx) error { return fn(failingEventTx{tx}) })
}

type failingEventTx struct{ store.Tx }

func (failingEventTx) AppendEvent(context.Context, store.Event) (store.Event, error) {
	return store.Event{}, errors.New("event log unavailable")
}

func testFailedPassIsRolledBack(t *testing.T, newStore func(t *testing.T) store.Store) {
	st := newStore(t)
	e := newEnv(t, st)
	e.node("n1")
	e.service("web", 3, "nginx:1.0")
	e.settle()
	e.scale("web", 1)
	before := e.fingerprint()

	broken := New(Config{Store: failingEventStore{st}, Now: func() time.Time { return e.clock }})
	if _, err := broken.RunOnce(e.ctx()); err == nil {
		t.Fatal("a pass that cannot record its events must fail")
	}
	if e.fingerprint() != before {
		t.Fatal("a failed pass left changes behind")
	}
	if report, err := e.rec.RunOnce(e.ctx()); err != nil || report.Stopped != 2 {
		t.Fatalf("the next pass starts over: %+v, %v", report, err)
	}
}

func testRunLoop(t *testing.T, newStore func(t *testing.T) store.Store) {
	st := newStore(t)
	e := newEnv(t, st)
	e.node("n1")
	e.service("web", 2, "nginx:1.0")
	e.settle()
	e.scale("web", 1)

	loop := New(Config{Store: st, Interval: 5 * time.Millisecond, Now: time.Now})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("Run did not stop after its context was canceled")
		}
	}()

	deadline := time.Now().Add(10 * time.Second)
	for {
		list := e.workloads("web")
		if len(list) == 2 && list[1].DesiredState == store.DesiredStopped {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the loop did not stop the extra replica:\n%s", e.describe())
		}
		time.Sleep(5 * time.Millisecond)
	}

	// With no interval the loop does nothing and returns at once.
	idle := make(chan struct{})
	go func() {
		New(Config{Store: st}).Run(context.Background())
		close(idle)
	}()
	select {
	case <-idle:
	case <-time.After(5 * time.Second):
		t.Fatal("Run with a zero interval must return immediately")
	}
}
