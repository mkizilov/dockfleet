package placement

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/scheduler"
	"github.com/dockfleet/dockfleet/internal/store"
)

// The placement suite takes a store factory: it runs against the in-memory
// store always and against PostgreSQL under the `integration` build tag, so
// both must make the same decisions and keep the same records.

var t0 = time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)

const (
	mib = int64(1) << 20
	gib = int64(1) << 30
	ttl = 30 * time.Second
)

type env struct {
	t     *testing.T
	st    store.Store
	clock time.Time
	sched *Scheduler
}

func newEnv(t *testing.T, st store.Store, tweak ...func(*Config)) *env {
	t.Helper()
	e := &env{t: t, st: st, clock: t0}
	cfg := Config{
		Store: st, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), NodeTTL: ttl,
		InstanceID: "mgr-1", Now: func() time.Time { return e.clock },
	}
	for _, f := range tweak {
		f(&cfg)
	}
	e.sched = New(cfg)
	return e
}

func (e *env) ctx() context.Context { return context.Background() }

func (e *env) must(err error) {
	e.t.Helper()
	if err != nil {
		e.t.Fatal(err)
	}
}

// node registers a node that has just sent a heartbeat.
func (e *env) node(id string, cpu, mem int64, labels ...string) {
	e.t.Helper()
	e.nodeBeat(id, e.clock, cpu, mem, labels...)
}

func (e *env) nodeBeat(id string, heartbeat time.Time, cpu, mem int64, labels ...string) {
	e.t.Helper()
	n := store.Node{ID: id, Name: id, CPUMillis: cpu, MemoryBytes: mem, RegisteredAt: heartbeat, LastHeartbeat: heartbeat}
	if len(labels) > 0 {
		n.Labels = map[string]string{}
		for _, l := range labels {
			k, v, _ := strings.Cut(l, "=")
			n.Labels[k] = v
		}
	}
	_, err := e.st.UpsertNode(e.ctx(), n)
	e.must(err)
}

func spec(cpu int64, ports ...store.ServicePort) store.ServiceSpec {
	return store.ServiceSpec{Image: "nginx:1.27", CPUMillis: cpu, MemoryBytes: 128 * mib, Ports: ports}
}

func (e *env) service(name string, replicas int, s store.ServiceSpec) store.Service {
	e.t.Helper()
	svc, err := e.st.CreateService(e.ctx(), store.NewService{ID: "svc-" + name, Name: name, Replicas: replicas, Spec: s, CreatedBy: "test", At: e.clock})
	e.must(err)
	return svc
}

func (e *env) run() Report {
	e.t.Helper()
	report, err := e.sched.RunOnce(e.ctx())
	e.must(err)
	return report
}

// placed returns, for the current revision of a service, which node each replica is on.
func (e *env) placed(name string) map[int]string {
	e.t.Helper()
	svc, err := e.st.GetServiceByName(e.ctx(), name)
	e.must(err)
	workloads, err := e.st.ListWorkloadsForService(e.ctx(), svc.ID)
	e.must(err)
	out := map[int]string{}
	for _, w := range workloads {
		if w.ServiceRevision == svc.CurrentRevision {
			out[w.ReplicaIndex] = w.NodeID
		}
	}
	return out
}

func (e *env) waiting(name string) []store.Unschedulable {
	e.t.Helper()
	list, err := e.st.ListUnschedulable(e.ctx(), "svc-"+name)
	e.must(err)
	return list
}

func (e *env) events(action string) []store.Event {
	e.t.Helper()
	all, err := e.st.ListEvents(e.ctx(), store.EventFilter{Limit: store.MaxEventLimit})
	e.must(err)
	var out []store.Event
	for i := len(all) - 1; i >= 0; i-- { // oldest first
		if all[i].Action == action {
			out = append(out, all[i])
		}
	}
	return out
}

func (e *env) workloadCount() int {
	e.t.Helper()
	all, err := e.st.ListWorkloads(e.ctx())
	e.must(err)
	return len(all)
}

type placementTest struct {
	name string
	run  func(t *testing.T, newStore func(t *testing.T) store.Store)
}

func eachStore(fn func(t *testing.T, e *env)) func(t *testing.T, newStore func(t *testing.T) store.Store) {
	return func(t *testing.T, newStore func(t *testing.T) store.Store) { fn(t, newEnv(t, newStore(t))) }
}

var placementTests = []placementTest{
	{"assigns every waiting replica", eachStore(testAssignsWaitingReplicas)},
	{"a second pass changes nothing", eachStore(testPassesAreIdempotent)},
	{"scaling up places only the new replicas", eachStore(testScalingUp)},
	{"scaling down removes nothing", eachStore(testScalingDownRemovesNothing)},
	{"unschedulable replicas are explained once", eachStore(testUnschedulableAreExplained)},
	{"offline and revoked nodes get nothing", eachStore(testUnavailableNodes)},
	{"host port conflicts", eachStore(testHostPorts)},
	{"existing assignments reserve capacity", eachStore(testExistingAssignments)},
	{"node selectors", eachStore(testSelectors)},
	{"a new revision is placed beside the old", eachStore(testNewRevision)},
	{"deleted services are not scheduled", eachStore(testDeletedServices)},
	{"spreading many replicas", eachStore(testSpreadsEvenly)},
	{"workload names", eachStore(testWorkloadNames)},
	{"overcommit is off unless configured", testOvercommit},
	{"the same input gives the same placement", testDeterminism},
	{"a failed pass saves nothing", testFailedPassIsRolledBack},
	{"the loop places replicas and stops", testRunLoop},
}

func runPlacementTests(t *testing.T, newStore func(t *testing.T) store.Store) {
	t.Helper()
	for _, tt := range placementTests {
		t.Run(tt.name, func(t *testing.T) { tt.run(t, newStore) })
	}
}

func TestPlacementWithMemoryStore(t *testing.T) {
	runPlacementTests(t, func(*testing.T) store.Store { return store.NewMemory() })
}

func testAssignsWaitingReplicas(t *testing.T, e *env) {
	for _, id := range []string{"n1", "n2", "n3"} {
		e.node(id, 4000, 8*gib)
	}
	ports := []store.ServicePort{{ContainerPort: 80, Protocol: "tcp"}}
	svc := e.service("web", 3, spec(500, ports...))

	report := e.run()
	if report.Placed != 3 || report.Unschedulable != 0 || report.NewReasons != 0 {
		t.Fatalf("report = %+v", report)
	}
	workloads, err := e.st.ListWorkloadsForService(e.ctx(), svc.ID)
	e.must(err)
	if len(workloads) != 3 {
		t.Fatalf("workloads = %+v", workloads)
	}
	nodes := map[string]bool{}
	for i, w := range workloads {
		nodes[w.NodeID] = true
		if w.ServiceID != svc.ID || w.ServiceRevision != 1 || w.ReplicaIndex != i+1 || w.Image != "nginx:1.27" || w.CPUMillis != 500 || w.MemoryBytes != 128*mib ||
			w.DesiredState != store.DesiredRunning || w.Observed != nil || !w.CreatedAt.Equal(t0) || !reflect.DeepEqual(w.Ports, ports) {
			t.Errorf("workload %d = %+v", i+1, w)
		}
		if !strings.HasPrefix(w.Name, "web-r1-"+string(rune('1'+i))+"-") || len(w.Name) > 63 {
			t.Errorf("workload name %q", w.Name)
		}
	}
	if len(nodes) != 3 {
		t.Errorf("three replicas of one service on %d nodes; they should be spread out", len(nodes))
	}

	events := e.events("replica.scheduled")
	if len(events) != 3 {
		t.Fatalf("events = %+v", events)
	}
	for i, ev := range events {
		w := workloads[i]
		if ev.Actor != Actor || ev.InstanceID != "mgr-1" || ev.ResourceType != "workload" || ev.ResourceID != w.ID || !ev.At.Equal(t0) ||
			ev.Details["node_id"] != w.NodeID || ev.Details["service"] != "web" || ev.Details["service_id"] != svc.ID || ev.Details["replica"] != string(rune('1'+i)) {
			t.Errorf("event %d = %+v", i, ev)
		}
	}
}

func testPassesAreIdempotent(t *testing.T, e *env) {
	e.node("n1", 4000, 8*gib)
	e.node("n2", 4000, 8*gib)
	e.service("web", 4, spec(500))
	e.service("api", 1, spec(500))
	e.run()
	before, err := e.st.ListWorkloads(e.ctx())
	e.must(err)
	events := len(e.events("replica.scheduled"))

	for i := 0; i < 3; i++ {
		e.clock = e.clock.Add(time.Second)
		e.node("n1", 4000, 8*gib)
		e.node("n2", 4000, 8*gib)
		if report := e.run(); report != (Report{}) {
			t.Fatalf("pass %d = %+v, want nothing to do", i+2, report)
		}
	}
	after, err := e.st.ListWorkloads(e.ctx())
	e.must(err)
	if !reflect.DeepEqual(before, after) || len(e.events("replica.scheduled")) != events || len(e.events("replica.scheduled")) != 5 {
		t.Fatalf("later passes changed the cluster: %d workloads before, %d after, %d events", len(before), len(after), len(e.events("replica.scheduled")))
	}
}

func testScalingUp(t *testing.T, e *env) {
	e.node("n1", 8000, 8*gib)
	e.node("n2", 8000, 8*gib)
	svc := e.service("web", 3, spec(500))
	e.run()
	first, err := e.st.ListWorkloadsForService(e.ctx(), svc.ID)
	e.must(err)

	_, err = e.st.ScaleService(e.ctx(), svc.ID, 5, 0, t0)
	e.must(err)
	if report := e.run(); report.Placed != 2 {
		t.Fatalf("report = %+v, want only the two new replicas placed", report)
	}
	all, err := e.st.ListWorkloadsForService(e.ctx(), svc.ID)
	e.must(err)
	if len(all) != 5 || !reflect.DeepEqual(all[:3], first) {
		t.Fatalf("the first three replicas changed or were duplicated: %+v", all)
	}
	if all[3].ReplicaIndex != 4 || all[4].ReplicaIndex != 5 {
		t.Fatalf("new replicas = %+v", all[3:])
	}
}

// Scheduling places; it does not remove. Retiring replicas is a reconciler's job.
func testScalingDownRemovesNothing(t *testing.T, e *env) {
	e.node("n1", 8000, 8*gib)
	svc := e.service("web", 5, spec(500))
	e.run()
	_, err := e.st.ScaleService(e.ctx(), svc.ID, 2, 0, t0)
	e.must(err)
	if report := e.run(); report != (Report{}) {
		t.Fatalf("report = %+v", report)
	}
	all, err := e.st.ListWorkloadsForService(e.ctx(), svc.ID)
	e.must(err)
	if len(all) != 5 {
		t.Fatalf("workloads = %d, want all 5 left as they were", len(all))
	}
	for _, w := range all {
		if w.DesiredState != store.DesiredRunning {
			t.Errorf("%s was changed to %s", w.Name, w.DesiredState)
		}
	}
}

func testUnschedulableAreExplained(t *testing.T, e *env) {
	e.node("n1", 1000, 4*gib)
	svc := e.service("web", 3, spec(600))

	report := e.run()
	if report.Placed != 1 || report.Unschedulable != 2 || report.NewReasons != 2 {
		t.Fatalf("report = %+v", report)
	}
	waiting := e.waiting("web")
	if len(waiting) != 2 {
		t.Fatalf("waiting = %+v", waiting)
	}
	for i, u := range waiting {
		index := i + 2
		wantMessage := "0/1 nodes can run web#" + string(rune('0'+index)) + ": 1 node(s) lack CPU (needs 600m)"
		if u.ServiceID != svc.ID || u.Revision != 1 || u.ReplicaIndex != index || u.Code != scheduler.CodeInsufficientResources || u.Message != wantMessage || !u.Since.Equal(t0) {
			t.Errorf("record %d = %+v", index, u)
		}
		if len(u.Nodes) != 1 || u.Nodes[0] != (store.NodeReason{NodeID: "n1", Code: scheduler.NodeInsufficientResources, Message: "insufficient resources (CPU: needs 600m, 400m free)"}) {
			t.Errorf("node reasons of %d = %+v", index, u.Nodes)
		}
	}
	events := e.events("replica.unschedulable")
	if len(events) != 2 || events[0].Actor != Actor || events[0].ResourceType != "service" || events[0].ResourceID != svc.ID ||
		events[0].Details["code"] != scheduler.CodeInsufficientResources || events[0].Details["replica"] != "2" || events[0].Message != waiting[0].Message {
		t.Fatalf("events = %+v", events)
	}

	// Later passes with nothing new: no new events, the start time stays.
	for i := 0; i < 3; i++ {
		e.clock = e.clock.Add(5 * time.Second)
		e.node("n1", 1000, 4*gib)
		if report := e.run(); report.Placed != 0 || report.Unschedulable != 2 || report.NewReasons != 0 {
			t.Fatalf("pass %d = %+v", i+2, report)
		}
	}
	if len(e.events("replica.unschedulable")) != 2 || !e.waiting("web")[0].Since.Equal(t0) {
		t.Fatalf("an unchanged explanation was reported again or lost its start: %+v", e.waiting("web"))
	}

	// Room appears; the waiting replicas are placed and their explanations go.
	e.node("n2", 2000, 4*gib)
	if report := e.run(); report.Placed != 2 || report.Unschedulable != 0 {
		t.Fatalf("pass after a node joined = %+v", report)
	}
	if got := e.waiting("web"); len(got) != 0 {
		t.Fatalf("explanations left after placement: %+v", got)
	}
	if got := e.placed("web"); !reflect.DeepEqual(got, map[int]string{1: "n1", 2: "n2", 3: "n2"}) {
		t.Fatalf("placement = %v", got)
	}
}

func testUnavailableNodes(t *testing.T, e *env) {
	e.nodeBeat("silent", e.clock.Add(-time.Minute), 64000, 64*gib) // the emptiest, but not heard from within the limit
	e.node("revoked", 64000, 64*gib)
	_, err := e.st.CreateEnrollment(e.ctx(), store.Enrollment{NodeID: "revoked", CreatedAt: t0})
	e.must(err)
	_, err = e.st.RevokeEnrollment(e.ctx(), "revoked", t0)
	e.must(err)
	e.node("enrolled", 2000, 2*gib)
	_, err = e.st.CreateEnrollment(e.ctx(), store.Enrollment{NodeID: "enrolled", CreatedAt: t0})
	e.must(err)

	e.service("web", 2, spec(500))
	if report := e.run(); report.Placed != 2 {
		t.Fatalf("report = %+v", report)
	}
	if got := e.placed("web"); !reflect.DeepEqual(got, map[int]string{1: "enrolled", 2: "enrolled"}) {
		t.Fatalf("placement = %v, want only the enrolled, live node", got)
	}

	// With nothing usable left, the reasons say which node is out and why.
	e.clock = e.clock.Add(time.Hour)
	e.service("api", 1, spec(500))
	if report := e.run(); report.Placed != 0 || report.Unschedulable != 1 {
		t.Fatalf("report = %+v", report)
	}
	u := e.waiting("api")[0]
	reasons := map[string]string{}
	for _, nr := range u.Nodes {
		reasons[nr.NodeID] = nr.Message
	}
	if u.Code != scheduler.CodeNoAvailableNodes || reasons["silent"] != "no heartbeat within the last 30s" || reasons["enrolled"] != "no heartbeat within the last 30s" {
		t.Fatalf("explanation = %+v", u)
	}

	// A revoked node is named as such while it is otherwise alive.
	e.node("revoked", 64000, 64*gib)
	e.run()
	for _, nr := range e.waiting("api")[0].Nodes {
		if nr.NodeID == "revoked" && (nr.Code != scheduler.NodeUnavailable || nr.Message != RevokedEnrollment) {
			t.Errorf("revoked node reason = %+v", nr)
		}
	}
	if e.placed("api")[1] != "" {
		t.Fatalf("a revoked node was used: %v", e.placed("api"))
	}

	// A node that is enrolled again and heard from is usable again.
	_, err = e.st.CreateEnrollment(e.ctx(), store.Enrollment{NodeID: "revoked", CreatedAt: e.clock})
	e.must(err)
	e.run()
	if got := e.placed("api"); got[1] != "revoked" {
		t.Fatalf("placement after re-enrollment = %v", got)
	}
}

func testHostPorts(t *testing.T, e *env) {
	e.node("n1", 8000, 8*gib)
	e.node("n2", 8000, 8*gib)
	fixed := store.ServicePort{ContainerPort: 80, Protocol: "tcp", HostPort: 8080}

	e.service("web", 3, spec(500, fixed))
	report := e.run()
	if report.Placed != 2 || report.Unschedulable != 1 {
		t.Fatalf("report = %+v; one fixed host port allows one replica per node", report)
	}
	if got := e.placed("web"); !reflect.DeepEqual(got, map[int]string{1: "n1", 2: "n2"}) {
		t.Fatalf("placement = %v", got)
	}
	u := e.waiting("web")[0]
	if u.ReplicaIndex != 3 || u.Code != scheduler.CodeHostPortConflict || !strings.HasPrefix(u.Message, "0/2 nodes can run web#3: 2 node(s) already use host port 8080/tcp") {
		t.Fatalf("explanation = %+v", u)
	}
	for _, nr := range u.Nodes {
		if nr.Code != scheduler.NodeHostPortInUse || !strings.Contains(nr.Message, "host port 8080/tcp is already used by web-r1-") {
			t.Errorf("node reason = %+v", nr)
		}
	}

	// The next pass sees the ports of the workloads the last one saved, and the
	// explanation does not flip between naming a replica and naming its workload.
	e.clock = e.clock.Add(time.Second)
	e.node("n1", 8000, 8*gib)
	e.node("n2", 8000, 8*gib)
	if report := e.run(); report.NewReasons != 0 || report.Unschedulable != 1 {
		t.Fatalf("second pass = %+v; the explanation should be stable", report)
	}

	// Another service wanting the same port finds both nodes taken, whatever its limits.
	e.service("other", 1, spec(100, fixed))
	e.run()
	if got := e.waiting("other"); len(got) != 1 || got[0].Code != scheduler.CodeHostPortConflict {
		t.Fatalf("explanation = %+v", got)
	}
	// A different protocol or a port the node chooses does not conflict.
	e.service("dns", 1, spec(100, store.ServicePort{ContainerPort: 53, Protocol: "udp", HostPort: 8080}))
	e.service("any", 1, spec(100, store.ServicePort{ContainerPort: 80, Protocol: "tcp"}))
	e.run()
	if len(e.placed("dns")) != 1 || len(e.placed("any")) != 1 {
		t.Fatalf("dns %v, any %v; both should have been placed", e.placed("dns"), e.placed("any"))
	}

	// Stopping a replica frees its port. The waiting replicas are tried in name
	// order, so other#1 gets it before web#3 does.
	_, err := e.st.SetWorkloadDesiredState(e.ctx(), e.workloadOf("web", 2), store.DesiredStopped, e.clock)
	e.must(err)
	if report := e.run(); report.Placed != 1 {
		t.Fatalf("report = %+v; the freed port should let one waiting replica in", report)
	}
	if got := e.placed("other"); !reflect.DeepEqual(got, map[int]string{1: "n2"}) {
		t.Fatalf("other = %v, want it on the node whose port was freed", got)
	}
	if len(e.waiting("web")) != 1 {
		t.Fatalf("web#3 should still be waiting: %+v", e.waiting("web"))
	}

	// Stopping that one in turn lets web#3 in.
	_, err = e.st.SetWorkloadDesiredState(e.ctx(), e.workloadOf("other", 1), store.DesiredStopped, e.clock)
	e.must(err)
	if report := e.run(); report.Placed != 1 || report.Unschedulable != 0 {
		t.Fatalf("report = %+v", report)
	}
	if got := e.placed("web"); got[3] != "n2" {
		t.Fatalf("placement = %v, want replica 3 on n2", got)
	}
}

func testExistingAssignments(t *testing.T, e *env) {
	e.node("n1", 1000, 1*gib)
	// A workload nobody scheduled through a service, already using most of the node.
	standalone, err := e.st.CreateWorkload(e.ctx(), store.Workload{ID: "solo", Name: "solo", Image: "x", CPUMillis: 700, MemoryBytes: 64 * mib, NodeID: "n1", CreatedAt: t0})
	e.must(err)

	e.service("web", 2, spec(200))
	if report := e.run(); report.Placed != 1 || report.Unschedulable != 1 {
		t.Fatalf("report = %+v; 300m are free, room for one 200m replica", report)
	}

	// A workload meant to stop reserves nothing, whatever it is doing now.
	_, err = e.st.SetWorkloadDesiredState(e.ctx(), standalone.ID, store.DesiredStopped, e.clock)
	e.must(err)
	if report := e.run(); report.Placed != 1 || report.Unschedulable != 0 {
		t.Fatalf("report after stopping the standalone workload = %+v", report)
	}

	// A failed container that is still meant to run keeps its reservation: with
	// the two 200m replicas counted, 600m are free and 700m do not fit; if the
	// failed one were forgotten, 800m would be.
	e.service("api", 1, spec(700))
	e.must(e.st.RecordObservations(e.ctx(), "n1", []store.Observation{{WorkloadID: e.workloadOf("web", 1), State: store.WorkloadFailed, ObservedAt: e.clock}}))
	if report := e.run(); report.Placed != 0 || report.Unschedulable != 1 {
		t.Fatalf("report = %+v; the failed replica is still desired and still holds its 200m", report)
	}
}

func (e *env) workloadOf(service string, index int) string {
	e.t.Helper()
	svc, err := e.st.GetServiceByName(e.ctx(), service)
	e.must(err)
	workloads, err := e.st.ListWorkloadsForService(e.ctx(), svc.ID)
	e.must(err)
	for _, w := range workloads {
		if w.ServiceRevision == svc.CurrentRevision && w.ReplicaIndex == index {
			return w.ID
		}
	}
	e.t.Fatalf("no workload for %s#%d", service, index)
	return ""
}

func testSelectors(t *testing.T, e *env) {
	e.node("a", 4000, 8*gib, "zone=a")
	e.node("b", 1000, 1*gib, "zone=b", "disk=ssd")
	e.node("c", 4000, 8*gib, "zone=b", "disk=hdd")

	selecting := func(labels ...string) store.ServiceSpec {
		s := spec(500)
		s.NodeSelector = map[string]string{}
		for _, l := range labels {
			k, v, _ := strings.Cut(l, "=")
			s.NodeSelector[k] = v
		}
		return s
	}
	e.service("ssd", 2, selecting("zone=b", "disk=ssd"))
	e.service("gpu", 1, selecting("gpu=true"))
	e.service("zoned", 3, selecting("zone=b"))
	report := e.run()
	if report.Placed != 5 || report.Unschedulable != 1 {
		t.Fatalf("report = %+v", report)
	}
	if got := e.placed("ssd"); !reflect.DeepEqual(got, map[int]string{1: "b", 2: "b"}) {
		t.Fatalf("ssd = %v", got)
	}
	for index, node := range e.placed("zoned") {
		if node == "a" {
			t.Errorf("zoned#%d went to a node outside its zone", index)
		}
	}
	u := e.waiting("gpu")[0]
	if u.Code != scheduler.CodeSelectorUnsatisfied || u.Message != "0/3 nodes can run gpu#1: 3 node(s) do not match the node selector {gpu=true}" {
		t.Fatalf("explanation = %+v", u)
	}

	// A node that gains the label is picked up by the next pass.
	e.node("a", 4000, 8*gib, "zone=a", "gpu=true")
	if report := e.run(); report.Placed != 1 || report.Unschedulable != 0 {
		t.Fatalf("report = %+v", report)
	}
	if got := e.placed("gpu"); !reflect.DeepEqual(got, map[int]string{1: "a"}) {
		t.Fatalf("gpu = %v", got)
	}
}

// Changing a service's spec makes a new revision whose replicas are placed like
// any other; the old revision's workloads keep running and keep their resources
// until something removes them.
func testNewRevision(t *testing.T, e *env) {
	e.node("n1", 1000, 4*gib)
	svc := e.service("web", 2, spec(400))
	e.run()
	if len(e.placed("web")) != 2 {
		t.Fatalf("placement = %v", e.placed("web"))
	}

	v2 := spec(400)
	v2.Image = "nginx:1.28"
	_, _, err := e.st.DeployService(e.ctx(), svc.ID, store.Deployment{Spec: v2, CreatedBy: "test", At: t0})
	e.must(err)
	report := e.run()
	// 800m of the node's 1000m are held by revision 1, so only one revision-2 replica fits.
	if report.Placed != 0 || report.Unschedulable != 2 {
		t.Fatalf("report = %+v", report)
	}
	for _, u := range e.waiting("web") {
		if u.Revision != 2 {
			t.Errorf("explanation for revision %d, want 2", u.Revision)
		}
	}

	// Retiring revision 1 (here by hand) lets revision 2 in.
	workloads, err := e.st.ListWorkloadsForService(e.ctx(), svc.ID)
	e.must(err)
	for _, w := range workloads {
		_, err := e.st.SetWorkloadDesiredState(e.ctx(), w.ID, store.DesiredStopped, e.clock)
		e.must(err)
	}
	if report := e.run(); report.Placed != 2 || report.Unschedulable != 0 {
		t.Fatalf("report = %+v", report)
	}
	all, err := e.st.ListWorkloadsForService(e.ctx(), svc.ID)
	e.must(err)
	if len(all) != 4 || all[2].ServiceRevision != 2 || all[2].Image != "nginx:1.28" {
		t.Fatalf("workloads = %+v", all)
	}
}

func testDeletedServices(t *testing.T, e *env) {
	e.node("n1", 500, 1*gib)
	svc := e.service("web", 3, spec(400))
	e.run()
	if len(e.waiting("web")) != 2 {
		t.Fatalf("waiting = %+v", e.waiting("web"))
	}

	_, err := e.st.DeleteService(e.ctx(), svc.ID, 0, t0)
	e.must(err)
	e.node("n2", 8000, 8*gib)
	if report := e.run(); report != (Report{}) {
		t.Fatalf("report = %+v; a deleted service has nothing to place", report)
	}
	if len(e.waiting("web")) != 0 || e.workloadCount() != 1 {
		t.Fatalf("explanations %+v, workloads %d; the explanations go, what was placed stays", e.waiting("web"), e.workloadCount())
	}

	// Other services are unaffected.
	e.service("web2", 1, spec(100))
	e.run()
	if len(e.placed("web2")) != 1 {
		t.Fatalf("placement = %v", e.placed("web2"))
	}
}

func testSpreadsEvenly(t *testing.T, e *env) {
	for _, id := range []string{"n01", "n02", "n03", "n04", "n05", "n06", "n07", "n08", "n09", "n10"} {
		e.node(id, 8000, 16*gib)
	}
	e.service("web", 100, spec(50))
	if report := e.run(); report.Placed != 100 {
		t.Fatalf("report = %+v", report)
	}
	perNode := map[string]int{}
	for _, node := range e.placed("web") {
		perNode[node]++
	}
	lo, hi := 100, 0
	for _, n := range perNode {
		lo, hi = min(lo, n), max(hi, n)
	}
	if len(perNode) != 10 || lo != 10 || hi != 10 {
		t.Fatalf("replicas per node = %v, want 10 on each of 10 nodes", perNode)
	}
}

func testWorkloadNames(t *testing.T, e *env) {
	e.node("n1", 64000, 64*gib)
	long := strings.Repeat("x", 63)
	e.service(long, 3, spec(10))
	e.service("web", 3, spec(10))
	e.run()
	seen := map[string]bool{}
	all, err := e.st.ListWorkloads(e.ctx())
	e.must(err)
	for _, w := range all {
		if seen[w.Name] || len(w.Name) > 63 || !store.ValidName(w.Name) {
			t.Errorf("workload name %q is duplicated, too long, or not a valid name", w.Name)
		}
		seen[w.Name] = true
	}
	if len(all) != 6 {
		t.Fatalf("workloads = %d", len(all))
	}
}

func testOvercommit(t *testing.T, newStore func(t *testing.T) store.Store) {
	setup := func(e *env) {
		e.node("n1", 1000, 1*gib)
		e.service("web", 3, spec(600))
	}
	strict := newEnv(t, newStore(t))
	setup(strict)
	if report := strict.run(); report.Placed != 1 {
		t.Fatalf("without overcommit: %+v; the node promises no more CPU than it has", report)
	}

	loose := newEnv(t, newStore(t), func(c *Config) { c.Overcommit = scheduler.Overcommit{CPU: 2} })
	setup(loose)
	if report := loose.run(); report.Placed != 3 {
		t.Fatalf("with CPU overcommit 2: %+v; 1800m fit in 2000m", report)
	}
	// Memory was not raised, so it still limits.
	loose.service("big", 1, store.ServiceSpec{Image: "x", CPUMillis: 10, MemoryBytes: 2 * gib})
	if report := loose.run(); report.Placed != 0 || report.Unschedulable != 1 {
		t.Fatalf("a memory-hungry replica: %+v", report)
	}
}

// Two clusters built the same way place the same replicas on the same nodes,
// and a cluster built in another order places them identically too.
func testDeterminism(t *testing.T, newStore func(t *testing.T) store.Store) {
	build := func(order []string) map[string]string {
		e := newEnv(t, newStore(t))
		for _, id := range order {
			e.node(id, 4000, 8*gib, "zone="+id[:1])
		}
		fixed := store.ServicePort{ContainerPort: 80, Protocol: "tcp", HostPort: 9000}
		e.service("web", 5, spec(500))
		e.service("api", 4, spec(900, fixed))
		e.service("batch", 3, spec(1500))
		e.run()
		out := map[string]string{}
		for _, name := range []string{"api", "batch", "web"} {
			for index, node := range e.placed(name) {
				out[name+"#"+string(rune('0'+index))] = node
			}
		}
		return out
	}
	want := build([]string{"a1", "b1", "c1", "d1"})
	if len(want) != 12 {
		t.Fatalf("placement = %v", want)
	}
	for _, order := range [][]string{{"a1", "b1", "c1", "d1"}, {"d1", "c1", "b1", "a1"}, {"c1", "a1", "d1", "b1"}} {
		if got := build(order); !reflect.DeepEqual(got, want) {
			t.Fatalf("registering nodes in order %v changed the placement:\n%v\nwant\n%v", order, got, want)
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
	e.node("n1", 1000, 1*gib)
	e.service("web", 3, spec(600))

	broken := New(Config{Store: failingEventStore{st}, NodeTTL: ttl, Now: func() time.Time { return t0 }})
	if _, err := broken.RunOnce(e.ctx()); err == nil {
		t.Fatal("a pass that cannot record its events must fail")
	}
	if e.workloadCount() != 0 || len(e.waiting("web")) != 0 {
		t.Fatalf("a failed pass left %d workloads and %d explanations behind", e.workloadCount(), len(e.waiting("web")))
	}
	// The next pass starts over and succeeds.
	if report := e.run(); report.Placed != 1 || report.Unschedulable != 2 {
		t.Fatalf("report = %+v", report)
	}
}

func testRunLoop(t *testing.T, newStore func(t *testing.T) store.Store) {
	st := newStore(t)
	// Real time here: the loop uses the real clock for its ticks, and nodes must look alive to it.
	live := newEnv(t, st, func(c *Config) { c.Now = time.Now; c.Interval = 5 * time.Millisecond })
	live.clock = time.Now()
	live.node("n1", 4000, 8*gib)
	live.node("n2", 4000, 8*gib)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		live.sched.Run(ctx)
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

	// A service defined while the loop is running is picked up.
	live.service("web", 3, spec(500))
	deadline := time.Now().Add(10 * time.Second)
	for len(live.placed("web")) < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("the loop did not place the replicas: %v", live.placed("web"))
		}
		time.Sleep(5 * time.Millisecond)
	}
	nodes := []string{}
	for _, node := range live.placed("web") {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	if !reflect.DeepEqual(nodes, []string{"n1", "n1", "n2"}) {
		t.Fatalf("nodes = %v", nodes)
	}

	// With no interval the loop does nothing and returns at once.
	idle := New(Config{Store: st})
	finished := make(chan struct{})
	go func() {
		idle.Run(context.Background())
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("Run with a zero interval must return immediately")
	}
}
