package reconcile

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/placement"
	"github.com/dockfleet/dockfleet/internal/scheduler"
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
	// timing is what the manager under test trusts: a node is suspect after 30
	// seconds of silence and unavailable after two minutes.
	timing scheduler.Timing
	// down are the nodes whose agents say nothing: no heartbeat, no reports.
	down map[string]bool
	// failed and unhealthy are the workloads whose containers their agents
	// report failed, or running but failing their health check.
	failed, unhealthy map[string]bool
	// breaks, when set, makes the agents report the workloads it selects failed,
	// and healthOf the health check result they report ("" leaves the default,
	// healthy for a workload that has a check).
	breaks   func(store.Workload) bool
	healthOf func(store.Workload) string
}

func newEnv(t *testing.T, st store.Store) *env {
	t.Helper()
	e := &env{t: t, st: st, clock: t0, obsAt: t0, down: map[string]bool{}, failed: map[string]bool{}, unhealthy: map[string]bool{}}
	e.timing = scheduler.Timing{SuspectAfter: 30 * time.Second, UnavailableAfter: 2 * time.Minute}
	e.startManager()
	return e
}

// startManager builds the scheduler and the reconciler, as a manager does when
// it starts: from now on it counts silence, not from before.
func (e *env) startManager() {
	now := func() time.Time { return e.clock }
	e.timing.Since = e.clock
	e.rec = New(Config{Store: e.st, InstanceID: "mgr-1", Timing: e.timing, Now: now})
	e.place = placement.New(placement.Config{Store: e.st, NodeTTL: 30 * time.Second, Timing: e.timing, InstanceID: "mgr-1", Now: now})
}

// advance moves the clock on while nobody is heartbeating (the manager is down).
func (e *env) advance(d time.Duration) { e.clock = e.clock.Add(d) }

// elapse lets time pass the way it does in a running fleet: the agents that are
// up heartbeat every ten seconds, the ones that are down say nothing.
func (e *env) elapse(d time.Duration) {
	e.t.Helper()
	for d > 0 {
		step := min(d, 10*time.Second)
		e.advance(step)
		e.agents()
		d -= step
	}
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
	if !errors.Is(err, store.ErrUnchanged) { // asking for the count it already has is not an error
		e.must(err)
	}
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
		if e.down[n.ID] {
			continue
		}
		e.must(e.st.Heartbeat(e.ctx(), n.ID, e.clock))
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
			case e.failed[w.ID] || (e.breaks != nil && e.breaks(w)):
				o.State, o.ContainerID, o.Message = store.WorkloadFailed, "container-"+w.ID, "exited with code 1"
			case e.unhealthy[w.ID]:
				o.State, o.ContainerID, o.Health = store.WorkloadRunning, "container-"+w.ID, store.HealthUnhealthy
			default:
				o.State, o.ContainerID = store.WorkloadRunning, "container-"+w.ID
				if w.HealthCheck != nil {
					o.Health = store.HealthHealthy
				}
				if e.healthOf != nil {
					if h := e.healthOf(w); h != "" {
						o.Health = h
					}
				}
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

// everyReplicaRuns is the rollout invariant: at no point do fewer than n
// workloads of the service run, whichever revision they belong to.
func (e *env) everyReplicaRuns(name string, n int) {
	e.t.Helper()
	running := 0
	for _, w := range e.workloads(name) {
		if w.DesiredState == store.DesiredRunning && w.Observed != nil && w.Observed.State == store.WorkloadRunning {
			running++
		}
	}
	if running < n {
		e.t.Fatalf("%d replicas of %s run, want at least %d:\n%s", running, name, n, e.describe())
	}
}

type reconcileTest struct {
	name string
	run  func(t *testing.T, newStore func(t *testing.T) store.Store)
}

var reconcileTests = []reconcileTest{
	{"the control loop deploys, scales, rolls out and removes a service", testControlLoopLifecycle},
	{"a replica that never comes up keeps the old one", testFailedRolloutKeepsTheOldReplica},
	{"a fixed host port needs max_unavailable before the rollout can go on", testFixedHostPortNeedsMaxUnavailable},
	{"a rolling update stays within max_surge and max_unavailable", testRollingUpdateStaysWithinThePolicy},
	{"replacements that fail leave the old replicas running and are reported", testFailingReplacementsAreReported},
	{"a rollout without room waits, says why, and goes on when the policy allows", testRolloutWithoutRoom},
	{"a paused rollout stands still and its deadline stops", testPausedRolloutStandsStill},
	{"health checks decide when a replica is ready", testHealthGatesReadiness},
	{"a rollback keeps the replicas that already run the old spec", testRollbackKeepsMatchingReplicas},
	{"a rollback after a finished rollout rolls back gradually", testRollbackAfterACompletedRollout},
	{"a new revision overtakes a rollout that was not finished", testRolloutIsSuperseded},
	{"replicas of a lost node move, and the node is cleaned up when it returns", testLostNodeReplicasMove},
	{"a manager that was down does not mistake its own absence for dead nodes", testManagerRestartDoesNotMoveReplicas},
	{"a node that goes quiet and comes back keeps its replicas", testSuspectNodeKeepsItsReplicas},
	{"an agent that left cleanly is given up on after the same time", testDepartedNodeIsGivenUpOnLater},
	{"the only node is lost: the replica waits, then starts when it returns", testLostOnlyNode},
	{"workloads that belong to no service stay where they were put", testStandaloneWorkloadsAreNotMoved},
	{"scaling down stops the worst replicas and keeps the healthy ones as they were", testScaleDownKeepsHealthyReplicas},
	{"scaling up and down again and again restarts nothing that stays", testScalingBackAndForth},
	{"scaling up while a node is unavailable uses the others", testScaleUpWithANodeUnavailable},
	{"scaling down while a node is quiet removes its replica first", testScaleDownWithANodeQuiet},
	{"scaling up beyond the cluster places what fits and waits for the rest", testScaleUpBeyondCapacity},
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
	if len(list) != 3 { // the two old replicas, and the one extra replica the default surge allows
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

// meantToRun are the workloads of a service whose desired state is running.
func (e *env) meantToRun(name string) []store.Workload {
	e.t.Helper()
	var out []store.Workload
	for _, w := range e.workloads(name) {
		if w.DesiredState == store.DesiredRunning {
			out = append(out, w)
		}
	}
	return out
}

func (e *env) health(nodeID string) string {
	e.t.Helper()
	n, err := e.st.GetNode(e.ctx(), nodeID)
	e.must(err)
	return n.Health
}

// wantOneEach checks that the workloads that are meant to run cover replicas
// 1..n once each, and returns them by node.
func (e *env) wantOneEach(name string, n int) map[string][]store.Workload {
	e.t.Helper()
	seen := map[int]bool{}
	byNode := map[string][]store.Workload{}
	list := e.meantToRun(name)
	for _, w := range list {
		if w.ReplicaIndex < 1 || w.ReplicaIndex > n || seen[w.ReplicaIndex] {
			e.t.Fatalf("replica %d of %s is missing, duplicated or out of range:\n%s", w.ReplicaIndex, name, e.describe())
		}
		seen[w.ReplicaIndex] = true
		byNode[w.NodeID] = append(byNode[w.NodeID], w)
	}
	if len(list) != n {
		e.t.Fatalf("%s has %d workloads meant to run, want %d:\n%s", name, len(list), n, e.describe())
	}
	return byNode
}

func testLostNodeReplicasMove(t *testing.T, newStore func(t *testing.T) store.Store) {
	e := newEnv(t, newStore(t))
	for _, id := range []string{"n1", "n2", "n3"} {
		e.node(id)
	}
	e.service("web", 3, "nginx:1.0")
	e.settle()
	first := e.wantReplicas("web", 1, 3, "nginx:1.0")
	var victim store.Workload
	for _, w := range first {
		if w.NodeID == "n3" {
			victim = w
		}
	}
	if victim.ID == "" {
		t.Fatalf("three replicas over three nodes must put one on n3:\n%s", e.describe())
	}

	// n3 stops answering. Twenty seconds is within the limit: nothing happens.
	e.down["n3"] = true
	e.elapse(20 * time.Second)
	e.settle()
	if e.health("n3") != store.NodeHealthy || e.events("node.suspect") != 0 {
		t.Fatalf("n3 is %s after 20s of silence", e.health("n3"))
	}

	// Forty seconds: suspect. It gets no new replicas, but keeps the one it has.
	e.elapse(20 * time.Second)
	e.settle()
	if e.health("n3") != store.NodeSuspect || e.events("node.suspect") != 1 || e.events("replica.stopped") != 0 {
		t.Fatalf("n3 is %s, %d suspect events, %d stops", e.health("n3"), e.events("node.suspect"), e.events("replica.stopped"))
	}
	e.wantOneEach("web", 3)

	// More than the lease and its grace: unavailable. Its replica is stopped
	// (generation up) and started on a node that answers.
	e.elapse(90 * time.Second)
	e.settle()
	if e.health("n3") != store.NodeUnavailable || e.events("node.unavailable") != 1 {
		t.Fatalf("n3 is %s, %d unavailable events", e.health("n3"), e.events("node.unavailable"))
	}
	byNode := e.wantOneEach("web", 3)
	if len(byNode["n3"]) != 0 || len(byNode["n1"])+len(byNode["n2"]) != 3 {
		t.Fatalf("replicas per node = %v; none may stay on the lost node:\n%s", byNode, e.describe())
	}
	for _, w := range e.meantToRun("web") {
		if w.ID == victim.ID {
			t.Fatal("the lost node's workload is still meant to run")
		}
	}
	old, err := e.st.GetWorkload(e.ctx(), victim.ID)
	e.must(err)
	if old.DesiredState != store.DesiredStopped || old.Generation != 2 || old.NodeID != "n3" || old.Observed == nil || old.Observed.AckedGeneration != 1 {
		t.Fatalf("the lost node's workload = %+v; it must be stopped at generation 2 and kept until the node confirms", old)
	}
	if len(e.workloads("web")) != 4 || e.events("replica.stopped") != 1 || e.events("replica.scheduled") != 4 {
		t.Fatalf("%d workloads, %d stops, %d placements:\n%s", len(e.workloads("web")), e.events("replica.stopped"), e.events("replica.scheduled"), e.describe())
	}

	// Settling again changes nothing: the move happens once.
	for i := 0; i < 5; i++ {
		e.advance(time.Second)
		e.cycle()
	}
	if len(e.workloads("web")) != 4 || e.events("replica.scheduled") != 4 {
		t.Fatalf("repeated passes moved more replicas:\n%s", e.describe())
	}

	// n3 returns. It learns that its workload was stopped, confirms that the
	// container is gone, the record is deleted, and nothing is moved back.
	delete(e.down, "n3")
	e.settle()
	if e.health("n3") != store.NodeHealthy || e.events("node.recovered") != 1 || e.events("replica.removed") != 1 {
		t.Fatalf("n3 is %s, %d recoveries, %d removals", e.health("n3"), e.events("node.recovered"), e.events("replica.removed"))
	}
	after := e.wantOneEach("web", 3)
	if len(e.workloads("web")) != 3 || len(after["n3"]) != 0 {
		t.Fatalf("after the node returned:\n%s", e.describe())
	}
}

func testManagerRestartDoesNotMoveReplicas(t *testing.T, newStore func(t *testing.T) store.Store) {
	e := newEnv(t, newStore(t))
	e.node("n1")
	e.node("n2")
	e.service("web", 2, "nginx:1.0")
	e.settle()
	first := e.wantReplicas("web", 1, 2, "nginx:1.0")

	// The manager is down for an hour, so no heartbeat was recorded. When it
	// starts again every node looks silent, but it cannot know that for longer
	// than it has been watching.
	e.advance(time.Hour)
	e.startManager()
	for i := 0; i < 3; i++ {
		e.cycle()
	}
	after := e.wantReplicas("web", 1, 2, "nginx:1.0")
	for i := range first {
		if after[i].ID != first[i].ID {
			t.Fatalf("replica %d was replaced after a manager restart", i+1)
		}
	}
	if e.events("node.unavailable") != 0 || e.events("node.suspect") != 0 || e.events("replica.stopped") != 0 {
		t.Fatal("a manager restart was taken for a failure of its nodes")
	}

	// A node that does not come back is given up on, counted from the restart.
	e.down["n2"] = true
	e.elapse(3 * time.Minute)
	e.settle()
	if e.health("n2") != store.NodeUnavailable || e.health("n1") != store.NodeHealthy {
		t.Fatalf("n1 is %s, n2 is %s", e.health("n1"), e.health("n2"))
	}
	if byNode := e.wantOneEach("web", 2); len(byNode["n1"]) != 2 {
		t.Fatalf("both replicas belong on n1 now:\n%s", e.describe())
	}
}

func testSuspectNodeKeepsItsReplicas(t *testing.T, newStore func(t *testing.T) store.Store) {
	e := newEnv(t, newStore(t))
	e.node("n1")
	e.node("n2")
	e.service("web", 2, "nginx:1.0")
	e.settle()
	first := e.wantReplicas("web", 1, 2, "nginx:1.0")

	e.down["n2"] = true
	e.elapse(45 * time.Second)
	e.settle()
	if e.health("n2") != store.NodeSuspect {
		t.Fatalf("n2 is %s", e.health("n2"))
	}
	delete(e.down, "n2")
	e.settle()
	if e.health("n2") != store.NodeHealthy || e.events("node.suspect") != 1 || e.events("node.recovered") != 1 {
		t.Fatalf("n2 is %s, %d suspect events, %d recoveries", e.health("n2"), e.events("node.suspect"), e.events("node.recovered"))
	}
	after := e.wantReplicas("web", 1, 2, "nginx:1.0")
	for i := range first {
		if after[i].ID != first[i].ID || after[i].Generation != 1 {
			t.Fatalf("replica %d changed although its node only went quiet: %+v", i+1, after[i])
		}
	}
	if e.events("replica.stopped") != 0 {
		t.Fatal("a suspect node's replicas must not be stopped")
	}
}

func testDepartedNodeIsGivenUpOnLater(t *testing.T, newStore func(t *testing.T) store.Store) {
	e := newEnv(t, newStore(t))
	e.node("n1")
	e.node("n2")
	e.service("web", 2, "nginx:1.0")
	e.settle()
	e.wantReplicas("web", 1, 2, "nginx:1.0")

	// The agent on n2 shuts down cleanly; its containers keep running.
	e.must(e.st.DeregisterNode(e.ctx(), "n2", e.clock))
	e.down["n2"] = true
	e.settle()
	if e.health("n2") != store.NodeSuspect || e.events("replica.stopped") != 0 {
		t.Fatalf("n2 is %s right after leaving, %d stops", e.health("n2"), e.events("replica.stopped"))
	}
	e.elapse(time.Minute)
	e.settle()
	if e.health("n2") != store.NodeSuspect || e.events("replica.stopped") != 0 {
		t.Fatalf("n2 is %s after a minute, %d stops", e.health("n2"), e.events("replica.stopped"))
	}
	e.elapse(61 * time.Second)
	e.settle()
	if e.health("n2") != store.NodeUnavailable {
		t.Fatalf("n2 is %s", e.health("n2"))
	}
	if byNode := e.wantOneEach("web", 2); len(byNode["n1"]) != 2 {
		t.Fatalf("both replicas belong on n1 now:\n%s", e.describe())
	}
}

func testLostOnlyNode(t *testing.T, newStore func(t *testing.T) store.Store) {
	e := newEnv(t, newStore(t))
	e.node("n1")
	e.service("web", 1, "nginx:1.0")
	e.settle()
	old := e.wantReplicas("web", 1, 1, "nginx:1.0")[0]

	e.down["n1"] = true
	e.advance(3 * time.Minute)
	e.settle()
	if got := e.meantToRun("web"); len(got) != 0 {
		t.Fatalf("nothing can run while the only node is gone:\n%s", e.describe())
	}
	waiting, err := e.st.ListUnschedulable(e.ctx(), "svc-web")
	e.must(err)
	if len(waiting) != 1 || waiting[0].Code != "no_available_nodes" {
		t.Fatalf("the replica must be explained as waiting for a node: %+v", waiting)
	}

	delete(e.down, "n1")
	e.settle()
	now := e.wantReplicas("web", 1, 1, "nginx:1.0")[0]
	if now.ID == old.ID || now.NodeID != "n1" || e.events("replica.removed") != 1 {
		t.Fatalf("after the node returned: %+v, %d removals", now, e.events("replica.removed"))
	}
}

func testStandaloneWorkloadsAreNotMoved(t *testing.T, newStore func(t *testing.T) store.Store) {
	e := newEnv(t, newStore(t))
	e.node("n1")
	e.node("n2")
	_, err := e.st.CreateWorkload(e.ctx(), store.Workload{
		ID: "solo", Name: "solo", Image: "nginx:1.0", CPUMillis: 100, MemoryBytes: 64 << 20, NodeID: "n2", CreatedAt: e.clock,
	})
	e.must(err)
	e.down["n2"] = true
	e.elapse(5 * time.Minute)
	e.settle()
	w, err := e.st.GetWorkload(e.ctx(), "solo")
	e.must(err)
	if w.DesiredState != store.DesiredRunning || w.Generation != 1 || w.NodeID != "n2" {
		t.Fatalf("a workload an administrator pinned to a node was changed: %+v", w)
	}
	if e.health("n2") != store.NodeUnavailable {
		t.Fatalf("n2 is %s, but the node's state is shown anyway", e.health("n2"))
	}
}

// stoppedEvents are the replica.stopped events of the workload, newest first.
func (e *env) stoppedEvents(workloadID string) []store.Event {
	e.t.Helper()
	all, err := e.st.ListEvents(e.ctx(), store.EventFilter{ResourceType: "workload", ResourceID: workloadID, Limit: store.MaxEventLimit})
	e.must(err)
	var out []store.Event
	for _, ev := range all {
		if ev.Action == "replica.stopped" {
			out = append(out, ev)
		}
	}
	return out
}

func testScaleDownKeepsHealthyReplicas(t *testing.T, newStore func(t *testing.T) store.Store) {
	e := newEnv(t, newStore(t))
	e.node("n1")
	e.node("n2")
	e.service("web", 4, "nginx:1.0")
	e.settle()
	first := e.wantReplicas("web", 1, 4, "nginx:1.0")

	// Replica 2 fails and replica 3 runs but fails its health check. Neither is a reason to act.
	e.failed[first[1].ID], e.unhealthy[first[2].ID] = true, true
	e.settle()
	if e.events("replica.stopped") != 0 {
		t.Fatalf("a failing replica was stopped although the count is right:\n%s", e.describe())
	}

	// Two fewer are wanted: the failing ones go, the healthy ones, replicas 1 and 4, stay untouched.
	e.scale("web", 2)
	e.settle()
	left := e.meantToRun("web")
	if len(left) != 2 || left[0].ID != first[0].ID || left[1].ID != first[3].ID {
		t.Fatalf("the replicas left are not the healthy ones:\n%s", e.describe())
	}
	for _, w := range left {
		if w.Generation != 1 || w.Observed == nil || w.Observed.State != store.WorkloadRunning || w.Observed.ContainerID != "container-"+w.ID {
			t.Fatalf("a replica that stays was touched: %+v (observed %+v)", w, w.Observed)
		}
	}
	if got := e.stoppedEvents(first[1].ID); len(got) != 1 || got[0].Details["why"] != WhyFailed || got[0].Details["reason"] != ReasonScaledDown {
		t.Fatalf("the failed replica's stop = %+v", got)
	}
	if got := e.stoppedEvents(first[2].ID); len(got) != 1 || got[0].Details["why"] != WhyUnhealthy {
		t.Fatalf("the unhealthy replica's stop = %+v", got)
	}
	if e.events("replica.stopped") != 2 || e.events("replica.removed") != 2 || len(e.workloads("web")) != 2 {
		t.Fatalf("%d stopped, %d removed, %d records left:\n%s", e.events("replica.stopped"), e.events("replica.removed"), len(e.workloads("web")), e.describe())
	}

	// Scaling up again fills the freed slots with new workloads; the survivors are not touched either.
	e.scale("web", 4)
	e.settle()
	grown := e.meantToRun("web")
	if len(grown) != 4 {
		t.Fatalf("after scaling up:\n%s", e.describe())
	}
	for i, w := range grown {
		if w.ReplicaIndex != i+1 {
			t.Fatalf("replica %d has index %d; the lowest free slots are used:\n%s", i+1, w.ReplicaIndex, e.describe())
		}
	}
	if grown[0].ID != first[0].ID || grown[3].ID != first[3].ID || e.events("replica.scheduled") != 6 {
		t.Fatalf("after scaling up:\n%s", e.describe())
	}
}

// Many scalings in a row: whatever stays is never restarted, nothing is created
// that was not needed, and asking again for the same count changes nothing.
func testScalingBackAndForth(t *testing.T, newStore func(t *testing.T) store.Store) {
	e := newEnv(t, newStore(t))
	for _, id := range []string{"n1", "n2", "n3"} {
		e.node(id)
	}
	e.service("web", 2, "nginx:1.0")
	e.settle()

	seen := map[string]bool{} // every workload ID that ever ran
	current := map[string]bool{}
	for _, w := range e.meantToRun("web") {
		seen[w.ID], current[w.ID] = true, true
	}
	for _, n := range []int{5, 5, 3, 3, 6, 0, 0, 2, 2, 1} {
		scheduled, stopped := e.events("replica.scheduled"), e.events("replica.stopped")
		was := len(current)
		e.scale("web", n)
		e.settle()

		now := e.meantToRun("web")
		if len(now) != n || len(e.workloads("web")) != n {
			t.Fatalf("scaled to %d:\n%s", n, e.describe())
		}
		after := map[string]bool{}
		for _, w := range now {
			after[w.ID] = true
			switch {
			case current[w.ID]:
				// It was there before: not restarted, not replaced.
				if w.Generation != 1 || w.Observed == nil || w.Observed.ContainerID != "container-"+w.ID {
					t.Fatalf("scaled to %d: replica %s that stays was touched: %+v", n, w.Name, w)
				}
			case seen[w.ID]:
				t.Fatalf("scaled to %d: workload %s ran before and came back", n, w.Name)
			}
			seen[w.ID] = true
		}
		switch {
		case n > was:
			for id := range current {
				if !after[id] {
					t.Fatalf("scaled up to %d: replica %s was removed", n, id)
				}
			}
			if got := e.events("replica.scheduled") - scheduled; got != n-was || e.events("replica.stopped") != stopped {
				t.Fatalf("scaled up from %d to %d: %d placements, %d stops", was, n, got, e.events("replica.stopped")-stopped)
			}
		case n < was:
			for id := range after {
				if !current[id] {
					t.Fatalf("scaled down to %d: replica %s is new", n, id)
				}
			}
			if got := e.events("replica.stopped") - stopped; got != was-n || e.events("replica.scheduled") != scheduled {
				t.Fatalf("scaled down from %d to %d: %d stops, %d placements", was, n, got, e.events("replica.scheduled")-scheduled)
			}
		default:
			// The same count again: nothing at all happens.
			if e.events("replica.scheduled") != scheduled || e.events("replica.stopped") != stopped {
				t.Fatalf("asking again for %d changed something", n)
			}
		}
		current = after
	}
}

func testScaleUpWithANodeUnavailable(t *testing.T, newStore func(t *testing.T) store.Store) {
	e := newEnv(t, newStore(t))
	e.node("n1")
	e.node("n2")
	e.service("web", 2, "nginx:1.0")
	e.settle()

	// n2 stops answering and is given up on; its replica starts on n1.
	e.down["n2"] = true
	e.elapse(130 * time.Second)
	e.settle()
	if e.health("n2") != store.NodeUnavailable {
		t.Fatalf("n2 is %s", e.health("n2"))
	}
	before := e.wantOneEach("web", 2)
	if len(before["n1"]) != 2 {
		t.Fatalf("both replicas belong on n1 now:\n%s", e.describe())
	}

	// More replicas are wanted while n2 is away: they go where there is room and an answer.
	e.scale("web", 5)
	e.settle()
	after := e.wantOneEach("web", 5)
	if len(after["n1"]) != 5 || len(after["n2"]) != 0 {
		t.Fatalf("replicas per node = n1 %d, n2 %d:\n%s", len(after["n1"]), len(after["n2"]), e.describe())
	}
	for _, w := range before["n1"] {
		if got, err := e.st.GetWorkload(e.ctx(), w.ID); err != nil || got.Generation != 1 || got.DesiredState != store.DesiredRunning {
			t.Fatalf("a replica that was running changed: %+v, %v", got, err)
		}
	}

	// The node returns and the next replicas spread to it.
	delete(e.down, "n2")
	e.settle()
	e.scale("web", 7)
	e.settle()
	final := e.wantOneEach("web", 7)
	if len(final["n2"]) != 2 || len(final["n1"]) != 5 {
		t.Fatalf("replicas per node = n1 %d, n2 %d; the new ones belong on the node that has none:\n%s", len(final["n1"]), len(final["n2"]), e.describe())
	}
}

func testScaleDownWithANodeQuiet(t *testing.T, newStore func(t *testing.T) store.Store) {
	e := newEnv(t, newStore(t))
	for _, id := range []string{"n1", "n2", "n3"} {
		e.node(id)
	}
	e.service("web", 3, "nginx:1.0")
	e.settle()
	before := e.wantOneEach("web", 3)
	if len(before["n1"]) != 1 || len(before["n2"]) != 1 || len(before["n3"]) != 1 {
		t.Fatalf("three replicas over three nodes:\n%s", e.describe())
	}
	victim := before["n3"][0]

	// n3 goes quiet (not yet given up on) and one replica fewer is wanted: that one goes.
	e.down["n3"] = true
	e.elapse(45 * time.Second)
	e.settle()
	if e.health("n3") != store.NodeSuspect {
		t.Fatalf("n3 is %s", e.health("n3"))
	}
	e.scale("web", 2)
	e.settle()
	left := e.wantOneEach("web", 2)
	if len(left["n1"]) != 1 || len(left["n2"]) != 1 || left["n1"][0].ID != before["n1"][0].ID || left["n2"][0].ID != before["n2"][0].ID {
		t.Fatalf("the replicas that answer were not the ones kept:\n%s", e.describe())
	}
	if got := e.stoppedEvents(victim.ID); len(got) != 1 || got[0].Details["why"] != WhySuspectNode {
		t.Fatalf("the stop of the quiet node's replica = %+v", got)
	}
	// n3 cannot confirm, so the record stays; nothing else is created or stopped meanwhile.
	if got, err := e.st.GetWorkload(e.ctx(), victim.ID); err != nil || got.DesiredState != store.DesiredStopped || got.Generation != 2 {
		t.Fatalf("the quiet node's workload = %+v, %v", got, err)
	}
	if e.events("replica.stopped") != 1 || e.events("replica.scheduled") != 3 {
		t.Fatalf("%d stops, %d placements", e.events("replica.stopped"), e.events("replica.scheduled"))
	}

	// When n3 answers again it removes the container and the record goes.
	delete(e.down, "n3")
	e.settle()
	if _, err := e.st.GetWorkload(e.ctx(), victim.ID); !errors.Is(err, store.ErrNotFound) || e.events("replica.removed") != 1 {
		t.Fatalf("after n3 returned: %v, %d removals", err, e.events("replica.removed"))
	}
}

// More replicas are wanted than the cluster can hold: the ones that fit are
// placed, the others wait with a reason, and nothing is promised twice. A node
// that joins takes the rest, and scaling down afterwards keeps the nodes even.
func testScaleUpBeyondCapacity(t *testing.T, newStore func(t *testing.T) store.Store) {
	e := newEnv(t, newStore(t))
	e.node("n1") // 4000m: eight replicas of 500m
	e.service("web", 2, "nginx:1.0")
	e.settle()

	e.scale("web", 12)
	e.settle()
	placed := e.meantToRun("web")
	if len(placed) != 8 {
		t.Fatalf("%d replicas placed on a node with room for 8:\n%s", len(placed), e.describe())
	}
	waiting, err := e.st.ListUnschedulable(e.ctx(), "svc-web")
	e.must(err)
	if len(waiting) != 4 {
		t.Fatalf("%d replicas waiting, want 4: %+v", len(waiting), waiting)
	}
	for i, u := range waiting {
		if u.Code != "insufficient_resources" || u.ReplicaIndex != 9+i {
			t.Fatalf("explanation %d = %+v; the replicas that wait take the next free slots", i, u)
		}
	}

	// Another node: the rest are placed and the explanations go.
	e.node("n2")
	e.settle()
	byNode := e.wantOneEach("web", 12)
	if len(byNode["n1"]) != 8 || len(byNode["n2"]) != 4 {
		t.Fatalf("replicas per node = n1 %d, n2 %d:\n%s", len(byNode["n1"]), len(byNode["n2"]), e.describe())
	}
	if waiting, err = e.st.ListUnschedulable(e.ctx(), "svc-web"); err != nil || len(waiting) != 0 {
		t.Fatalf("explanations left: %+v, %v", waiting, err)
	}

	// Scaling down takes from the fuller node first, so the nodes end up level.
	// The replicas left are not numbered 1 to 4: the ones that went were chosen by node, not by number.
	e.scale("web", 4)
	e.settle()
	left := map[string]int{}
	for _, w := range e.meantToRun("web") {
		left[w.NodeID]++
	}
	if left["n1"] != 2 || left["n2"] != 2 || len(e.workloads("web")) != 4 {
		t.Fatalf("replicas per node after scaling down = %v:\n%s", left, e.describe())
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
