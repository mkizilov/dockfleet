package reconcile

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/scheduler"
	"github.com/dockfleet/dockfleet/internal/store"
)

var at = time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)

// definitions wraps services as definitions whose specs are empty.
func definitions(services []store.Service) []store.Definition {
	out := make([]store.Definition, 0, len(services))
	for _, s := range services {
		out = append(out, store.Definition{Service: s})
	}
	return out
}

func service(id string, replicas, revision int) store.Service {
	return store.Service{ID: id, Name: id, Replicas: replicas, CurrentRevision: revision}
}

func deletedService(id string, revision int) store.Service {
	s := service(id, 0, revision)
	s.DeletedAt = &at
	return s
}

// workload is replica index of revision rev of a service, meant to run, at generation 1.
func workload(svc string, rev, index int) store.Workload {
	return store.Workload{
		ID: fmt.Sprintf("%s-r%d-%d", svc, rev, index), Name: fmt.Sprintf("%s-r%d-%d", svc, rev, index), NodeID: "n1",
		DesiredState: store.DesiredRunning, Generation: 1, ServiceID: svc, ServiceRevision: rev, ReplicaIndex: index,
	}
}

func observed(w store.Workload, state string, acked int64) store.Workload {
	w.Observed = &store.Observation{WorkloadID: w.ID, NodeID: w.NodeID, State: state, AckedGeneration: acked, ContainerID: "c-" + w.ID}
	return w
}

func up(w store.Workload) store.Workload { return observed(w, store.WorkloadRunning, w.Generation) }

// stopped is a workload that was told to stop (generation 2).
func stopped(w store.Workload) store.Workload {
	w.DesiredState, w.Generation = store.DesiredStopped, 2
	return w
}

// gone is a stopped workload whose node has confirmed that the container is removed.
func gone(w store.Workload) store.Workload {
	w = stopped(w)
	w = observed(w, store.WorkloadStopped, 2)
	w.Observed.ContainerID = ""
	return w
}

func standalone(w store.Workload) store.Workload {
	w.ServiceID, w.ServiceRevision, w.ReplicaIndex = "", 0, 0
	return w
}

func TestPlan(t *testing.T) {
	type want struct{ kind, reason, workload string }
	tests := []struct {
		name      string
		services  []store.Service
		workloads []store.Workload
		want      []want
	}{
		{name: "nothing differs",
			services:  []store.Service{service("web", 2, 1)},
			workloads: []store.Workload{up(workload("web", 1, 1)), workload("web", 1, 2)}},
		{name: "scaling down stops the ones not running, then the highest numbered",
			services:  []store.Service{service("web", 1, 1)},
			workloads: []store.Workload{up(workload("web", 1, 1)), up(workload("web", 1, 2)), workload("web", 1, 3)},
			want:      []want{{ActionStop, ReasonScaledDown, "web-r1-2"}, {ActionStop, ReasonScaledDown, "web-r1-3"}}},
		{name: "scaling to zero stops everything",
			services:  []store.Service{service("web", 0, 1)},
			workloads: []store.Workload{up(workload("web", 1, 1)), up(workload("web", 1, 2))},
			want:      []want{{ActionStop, ReasonScaledDown, "web-r1-1"}, {ActionStop, ReasonScaledDown, "web-r1-2"}}},
		{name: "a deleted service stops every revision it has",
			services:  []store.Service{deletedService("web", 2)},
			workloads: []store.Workload{up(workload("web", 1, 1)), up(workload("web", 2, 1))},
			want:      []want{{ActionStop, ReasonServiceDeleted, "web-r1-1"}, {ActionStop, ReasonServiceDeleted, "web-r2-1"}}},

		{name: "a deployment retires one old replica for each new one that runs",
			services:  []store.Service{service("web", 2, 2)},
			workloads: []store.Workload{up(workload("web", 1, 1)), up(workload("web", 1, 2)), up(workload("web", 2, 1)), workload("web", 2, 2)},
			want:      []want{{ActionStop, ReasonSuperseded, "web-r1-2"}}},
		{name: "a new replica that finished successfully also counts",
			services:  []store.Service{service("web", 1, 2)},
			workloads: []store.Workload{up(workload("web", 1, 1)), observed(workload("web", 2, 1), store.WorkloadSucceeded, 1)},
			want:      []want{{ActionStop, ReasonSuperseded, "web-r1-1"}}},
		{name: "a new replica that fails leaves the old one running",
			services:  []store.Service{service("web", 1, 2)},
			workloads: []store.Workload{up(workload("web", 1, 1)), observed(workload("web", 2, 1), store.WorkloadFailed, 1)}},
		{name: "a new replica that is still pending leaves the old one running",
			services:  []store.Service{service("web", 1, 2)},
			workloads: []store.Workload{up(workload("web", 1, 1)), observed(workload("web", 2, 1), store.WorkloadPending, 1)}},
		{name: "a new replica that has not been assigned yet leaves the old one running",
			services:  []store.Service{service("web", 1, 2)},
			workloads: []store.Workload{up(workload("web", 1, 1))}},
		{name: "a running report from before the latest instruction does not count",
			services:  []store.Service{service("web", 1, 2)},
			workloads: []store.Workload{up(workload("web", 1, 1)), observed(func() store.Workload { w := workload("web", 2, 1); w.Generation = 3; return w }(), store.WorkloadRunning, 2)}},
		{name: "a replica of the current revision does not retire itself",
			services:  []store.Service{service("web", 1, 2)},
			workloads: []store.Workload{up(workload("web", 2, 1))}},
		{name: "every older revision's replica goes once the new one runs",
			services:  []store.Service{service("web", 1, 3)},
			workloads: []store.Workload{up(workload("web", 1, 1)), up(workload("web", 2, 1)), up(workload("web", 3, 1))},
			want:      []want{{ActionStop, ReasonSuperseded, "web-r1-1"}, {ActionStop, ReasonSuperseded, "web-r2-1"}}},
		{name: "an old replica beyond the replica count goes at once",
			services:  []store.Service{service("web", 1, 2)},
			workloads: []store.Workload{up(workload("web", 1, 1)), up(workload("web", 1, 2)), workload("web", 2, 1)},
			want:      []want{{ActionStop, ReasonScaledDown, "web-r1-2"}}},

		{name: "a stopped workload whose container is gone is deleted",
			services:  []store.Service{service("web", 1, 1)},
			workloads: []store.Workload{up(workload("web", 1, 1)), gone(workload("web", 1, 2))},
			want:      []want{{ActionDelete, ReasonRetired, "web-r1-2"}}},
		{name: "a stopped workload whose container is still there is kept",
			services:  []store.Service{service("web", 1, 1)},
			workloads: []store.Workload{up(workload("web", 1, 1)), observed(stopped(workload("web", 1, 2)), store.WorkloadStopped, 2)}},
		{name: "a stopped workload still reported running is kept",
			services:  []store.Service{service("web", 1, 1)},
			workloads: []store.Workload{up(workload("web", 1, 1)), observed(stopped(workload("web", 1, 2)), store.WorkloadRunning, 2)}},
		{name: "a stopped workload that was never reported is kept",
			services:  []store.Service{service("web", 1, 1)},
			workloads: []store.Workload{up(workload("web", 1, 1)), stopped(workload("web", 1, 2))}},
		{name: "a stopped report from before the instruction does not count",
			services: []store.Service{service("web", 1, 1)},
			workloads: []store.Workload{up(workload("web", 1, 1)), func() store.Workload {
				w := gone(workload("web", 1, 2))
				w.Observed.AckedGeneration = 1
				return w
			}()}},
		{name: "a retired workload of the current revision is deleted so its slot can be filled again",
			services:  []store.Service{service("web", 2, 1)},
			workloads: []store.Workload{up(workload("web", 1, 1)), gone(workload("web", 1, 2))},
			want:      []want{{ActionDelete, ReasonRetired, "web-r1-2"}}},
		{name: "a deleted service's retired workloads are removed too",
			services:  []store.Service{deletedService("web", 1)},
			workloads: []store.Workload{gone(workload("web", 1, 1)), up(workload("web", 1, 2))},
			want:      []want{{ActionStop, ReasonServiceDeleted, "web-r1-2"}, {ActionDelete, ReasonRetired, "web-r1-1"}}},
		{name: "nothing is stopped twice",
			services:  []store.Service{service("web", 0, 1)},
			workloads: []store.Workload{stopped(workload("web", 1, 1))}},

		{name: "workloads that belong to no service are left alone",
			services:  []store.Service{service("web", 0, 1)},
			workloads: []store.Workload{up(standalone(workload("solo", 1, 1))), gone(standalone(workload("done", 1, 1)))}},
		{name: "services are independent",
			services:  []store.Service{service("api", 1, 1), service("web", 1, 2)},
			workloads: []store.Workload{up(workload("api", 1, 1)), up(workload("web", 1, 1)), up(workload("web", 2, 1))},
			want:      []want{{ActionStop, ReasonSuperseded, "web-r1-1"}}},
		{name: "no services and no workloads"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []want
			for _, a := range Plan(definitions(tt.services), tt.workloads, nil, nil) {
				got = append(got, want{a.Kind, a.Reason, a.Workload.Name})
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("plan = %v\nwant   %v", got, tt.want)
			}
		})
	}
}

// The plan depends only on what is stored, not on the order it is read in.
func TestPlanDoesNotDependOnInputOrder(t *testing.T) {
	services := []store.Service{service("web", 1, 2), service("api", 2, 1)}
	workloads := []store.Workload{
		up(workload("web", 1, 1)), up(workload("web", 2, 1)), up(workload("web", 1, 2)), up(workload("api", 1, 1)),
		up(workload("api", 1, 2)), up(workload("api", 1, 3)), gone(workload("api", 1, 4)),
	}
	names := func(actions []Action) []string {
		var out []string
		for _, a := range actions {
			out = append(out, a.Kind+" "+a.Workload.Name)
		}
		return out
	}
	want := names(Plan(definitions(services), workloads, nil, nil))
	if len(want) != 4 {
		t.Fatalf("plan = %v", want)
	}
	reversed := make([]store.Workload, len(workloads))
	for i, w := range workloads {
		reversed[len(workloads)-1-i] = w
	}
	if got := names(Plan(definitions([]store.Service{services[1], services[0]}), reversed, nil, nil)); !reflect.DeepEqual(got, want) {
		t.Fatalf("plan = %v, want %v", got, want)
	}
}

// Planning twice from the same state gives the same plan, and planning from
// the state a plan leads to gives nothing more to do.
func TestPlanConverges(t *testing.T) {
	services := []store.Service{service("web", 1, 2)}
	workloads := []store.Workload{up(workload("web", 1, 1)), up(workload("web", 1, 2)), up(workload("web", 2, 1))}

	first := Plan(definitions(services), workloads, nil, nil)
	if len(first) != 2 || !reflect.DeepEqual(first, Plan(definitions(services), workloads, nil, nil)) {
		t.Fatalf("plan = %+v", first)
	}
	// Apply the stops, as the store would, then let the nodes confirm.
	for i, w := range workloads {
		for _, a := range first {
			if a.Kind == ActionStop && a.Workload.ID == w.ID {
				workloads[i] = gone(w)
			}
		}
	}
	second := Plan(definitions(services), workloads, nil, nil)
	if len(second) != 2 || second[0].Kind != ActionDelete || second[1].Kind != ActionDelete {
		t.Fatalf("after the nodes confirmed: %+v", second)
	}
	var left []store.Workload
	for _, w := range workloads {
		if w.DesiredState == store.DesiredRunning {
			left = append(left, w)
		}
	}
	if third := Plan(definitions(services), left, nil, nil); len(third) != 0 {
		t.Fatalf("the plan keeps asking for changes: %+v", third)
	}
}

// A replica that publishes a fixed host port cannot run beside its successor
// on the same node, so when the scheduler says the successor is blocked by
// that port, the old replica is stopped to free it.
func TestPlanHandsOverFixedHostPorts(t *testing.T) {
	port := func(host int, protocol string) store.ServicePort {
		return store.ServicePort{ContainerPort: 80, Protocol: protocol, HostPort: host}
	}
	withPorts := func(w store.Workload, ports ...store.ServicePort) store.Workload {
		w.Ports = ports
		return w
	}
	blocked := func(code string) []store.Unschedulable {
		return []store.Unschedulable{{ServiceID: "web", Revision: 2, ReplicaIndex: 1, Code: code}}
	}
	def := func(ports ...store.ServicePort) []store.Definition {
		return []store.Definition{{Service: service("web", 1, 2), Revision: store.Revision{ServiceID: "web", Revision: 2, Spec: store.ServiceSpec{Ports: ports}}}}
	}
	old := func(ports ...store.ServicePort) []store.Workload {
		return []store.Workload{withPorts(up(workload("web", 1, 1)), ports...)}
	}

	tests := []struct {
		name    string
		defs    []store.Definition
		current []store.Workload
		waiting []store.Unschedulable
		want    int
	}{
		{"the new replica is blocked by the port the old one holds", def(port(8080, "tcp")), old(port(8080, "tcp")), blocked(scheduler.CodeHostPortConflict), 1},
		{"an empty protocol is tcp on both sides", def(port(8080, "")), old(port(8080, "tcp")), blocked(scheduler.CodeHostPortConflict), 1},
		{"no explanation yet: the old replica stays", def(port(8080, "tcp")), old(port(8080, "tcp")), nil, 0},
		{"blocked by something else (resources)", def(port(8080, "tcp")), old(port(8080, "tcp")), blocked(scheduler.CodeInsufficientResources), 0},
		{"the old replica does not hold the port", def(port(8080, "tcp")), old(port(9090, "tcp")), blocked(scheduler.CodeHostPortConflict), 0},
		{"the same number on another protocol", def(port(8080, "udp")), old(port(8080, "tcp")), blocked(scheduler.CodeHostPortConflict), 0},
		{"a port the node chooses is never in conflict", def(port(0, "tcp")), old(port(0, "tcp")), blocked(scheduler.CodeHostPortConflict), 0},
		// Any replica of the new revision that is blocked needs an old one to let go; which one is blocked does not matter.
		{"the explanation is for another replica of the new revision", def(port(8080, "tcp")), old(port(8080, "tcp")),
			[]store.Unschedulable{{ServiceID: "web", Revision: 2, ReplicaIndex: 2, Code: scheduler.CodeHostPortConflict}}, 1},
		{"the explanation is for an older revision", def(port(8080, "tcp")), old(port(8080, "tcp")),
			[]store.Unschedulable{{ServiceID: "web", Revision: 1, ReplicaIndex: 1, Code: scheduler.CodeHostPortConflict}}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actions := Plan(tt.defs, tt.current, tt.waiting, nil)
			if len(actions) != tt.want {
				t.Fatalf("plan = %+v, want %d actions", actions, tt.want)
			}
			if tt.want == 1 && (actions[0].Kind != ActionStop || actions[0].Reason != ReasonPortHandover || actions[0].Workload.Name != "web-r1-1") {
				t.Fatalf("plan = %+v", actions)
			}
		})
	}
}

func onNode(w store.Workload, node string) store.Workload {
	w.NodeID = node
	return w
}

func describe(actions []Action) []string {
	out := []string{}
	for _, a := range actions {
		out = append(out, a.Kind+" "+a.Reason+" "+a.Workload.Name)
	}
	return out
}

// A node that has been silent for longer than its lease is given up on, and
// the replicas on it are told to stop so that the scheduler can start them on
// other nodes. What is on a lost node is not counted among the replicas a
// service has, so it never costs a healthy replica its place.
func TestPlanGivesUpOnLostNodes(t *testing.T) {
	lost := map[string]string{"dead": store.NodeUnavailable}
	tests := []struct {
		name      string
		services  []store.Service
		workloads []store.Workload
		want      []string
	}{
		{"a replica on a lost node is stopped",
			[]store.Service{service("web", 2, 1)},
			[]store.Workload{up(onNode(workload("web", 1, 1), "dead")), up(workload("web", 1, 2))},
			[]string{"stop node_unavailable web-r1-1"}},
		{"whatever state the lost node last reported",
			[]store.Service{service("web", 3, 1)},
			[]store.Workload{
				onNode(workload("web", 1, 1), "dead"),
				observed(onNode(workload("web", 1, 2), "dead"), store.WorkloadFailed, 1),
				up(onNode(workload("web", 1, 3), "dead"))},
			[]string{"stop node_unavailable web-r1-1", "stop node_unavailable web-r1-2", "stop node_unavailable web-r1-3"}},
		{"replicas on nodes that are not lost are left alone",
			[]store.Service{service("web", 2, 1)},
			[]store.Workload{up(workload("web", 1, 1)), up(onNode(workload("web", 1, 2), "other"))},
			[]string{}},
		{"a workload that was stopped on a lost node waits for the node: nothing confirms it",
			[]store.Service{service("web", 1, 1)},
			[]store.Workload{stopped(onNode(workload("web", 1, 1), "dead"))},
			[]string{}},
		{"a standalone workload is pinned to its node and is not moved",
			nil,
			[]store.Workload{standalone(onNode(workload("web", 1, 1), "dead"))},
			[]string{}},
		// What is on a lost node is gone: it is not counted when choosing what else to stop.
		{"a replica on a lost node is not counted as one of those wanted",
			[]store.Service{service("web", 1, 1)},
			[]store.Workload{up(workload("web", 1, 1)), onNode(workload("web", 1, 2), "dead")},
			[]string{"stop node_unavailable web-r1-2"}},
		{"so a healthy replica is not stopped for one that is lost",
			[]store.Service{service("web", 2, 1)},
			[]store.Workload{up(workload("web", 1, 1)), up(workload("web", 1, 2)), up(onNode(workload("web", 1, 3), "dead"))},
			[]string{"stop node_unavailable web-r1-3"}},
		{"an old revision's replica on a lost node goes whether or not the new one runs",
			[]store.Service{service("web", 1, 2)},
			[]store.Workload{up(onNode(workload("web", 1, 1), "dead")), up(workload("web", 2, 1))},
			[]string{"stop node_unavailable web-r1-1"}},
		{"an old revision's replica with no successor yet is still given up on",
			[]store.Service{service("web", 1, 2)},
			[]store.Workload{up(onNode(workload("web", 1, 1), "dead"))},
			[]string{"stop node_unavailable web-r1-1"}},
		{"stopping and deleting go on beside it",
			[]store.Service{service("web", 2, 1)},
			[]store.Workload{onNode(workload("web", 1, 1), "dead"), gone(workload("web", 1, 2))},
			[]string{"stop node_unavailable web-r1-1", "delete retired web-r1-2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := describe(Plan(definitions(tt.services), tt.workloads, nil, lost))
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("plan = %v, want %v", got, tt.want)
			}
		})
	}
}

// Each change of a node's health is reported once, against the state last recorded.
func TestPlanNodesReportsEachChangeOnce(t *testing.T) {
	timing := scheduler.Timing{SuspectAfter: 30 * time.Second, UnavailableAfter: 2 * time.Minute}
	registered := at.Add(-24 * time.Hour)
	node := func(id string, silent time.Duration, recorded string) store.Node {
		return store.Node{ID: id, LastHeartbeat: at.Add(-silent), RegisteredAt: registered, Health: recorded}
	}
	// a is fine, b went quiet, c is given up on, d and h came back, e is still
	// suspect, f has nothing recorded (which counts as healthy), g is still gone.
	nodes := []store.Node{
		node("e", time.Minute, store.NodeSuspect),
		node("d", 0, store.NodeUnavailable),
		node("c", time.Hour, store.NodeSuspect),
		node("b", time.Minute, store.NodeHealthy),
		node("a", 0, store.NodeHealthy),
		node("f", 0, ""),
		node("g", time.Hour, store.NodeUnavailable),
		node("h", 10*time.Second, store.NodeUnavailable),
	}
	var got []string
	for _, c := range PlanNodes(nodes, at, timing) {
		got = append(got, c.Node.ID+": "+c.From+" -> "+c.To)
	}
	want := []string{
		"b: healthy -> suspect",
		"c: suspect -> unavailable",
		"d: unavailable -> healthy",
		"h: unavailable -> healthy",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("changes = %v, want %v", got, want)
	}
	if again := PlanNodes(nodes, at, timing); !reflect.DeepEqual(again, PlanNodes(nodes, at, timing)) {
		t.Fatal("planning twice from the same snapshot gave different answers")
	}
}

func failedW(w store.Workload) store.Workload { return observed(w, store.WorkloadFailed, w.Generation) }

func unhealthyW(w store.Workload) store.Workload {
	w = up(w)
	w.Observed.Health = store.HealthUnhealthy
	return w
}

func restarted(w store.Workload, n int) store.Workload {
	w = up(w)
	w.Observed.RestartCount = n
	return w
}

func createdAt(w store.Workload, t time.Time) store.Workload {
	w.CreatedAt = t
	return w
}

func describeWhy(actions []Action) []string {
	out := []string{}
	for _, a := range actions {
		out = append(out, a.Kind+" "+a.Reason+" "+a.Workload.Name+" "+a.Why)
	}
	return out
}

// A service that has more replicas than it wants stops the ones doing the least
// good, not the highest numbered: failed, then unhealthy, then those on a node
// that has gone quiet, then those not running yet. Among equals the node that
// carries most of the service gives one up, then the replica that was restarted
// most, then the newest, then the highest numbered.
func TestPlanChoosesWhichReplicasToStop(t *testing.T) {
	quiet := map[string]string{"slow": store.NodeSuspect}
	tests := []struct {
		name      string
		want      int
		workloads []store.Workload
		nodes     map[string]string
		stops     []string
	}{
		{"a failed replica goes before a healthy one with a higher number", 2,
			[]store.Workload{up(workload("web", 1, 1)), failedW(workload("web", 1, 2)), up(workload("web", 1, 3))}, nil,
			[]string{"stop scaled_down web-r1-2 failed"}},
		{"an unhealthy replica goes before a healthy one", 2,
			[]store.Workload{up(workload("web", 1, 1)), unhealthyW(workload("web", 1, 2)), up(workload("web", 1, 3))}, nil,
			[]string{"stop scaled_down web-r1-2 unhealthy"}},
		{"a replica on a node that has gone quiet goes before one that is fine", 2,
			[]store.Workload{up(workload("web", 1, 1)), up(onNode(workload("web", 1, 2), "slow")), up(workload("web", 1, 3))}, quiet,
			[]string{"stop scaled_down web-r1-2 suspect_node"}},
		{"a replica that is not running yet goes before one that is", 2,
			[]store.Workload{up(workload("web", 1, 1)), workload("web", 1, 2), up(workload("web", 1, 3))}, nil,
			[]string{"stop scaled_down web-r1-2 not_running"}},
		{"failed, unhealthy, quiet and starting go in that order, and the one that serves stays", 1,
			[]store.Workload{
				failedW(workload("web", 1, 1)), unhealthyW(workload("web", 1, 2)), up(onNode(workload("web", 1, 3), "slow")),
				workload("web", 1, 4), up(workload("web", 1, 5)),
			}, quiet,
			[]string{
				"stop scaled_down web-r1-1 failed", "stop scaled_down web-r1-2 unhealthy",
				"stop scaled_down web-r1-3 suspect_node", "stop scaled_down web-r1-4 not_running",
			}},
		{"when only some have to go, the worst of them go first", 3,
			[]store.Workload{failedW(workload("web", 1, 1)), up(workload("web", 1, 2)), workload("web", 1, 3), up(workload("web", 1, 4)), up(workload("web", 1, 5))}, nil,
			[]string{"stop scaled_down web-r1-1 failed", "stop scaled_down web-r1-3 not_running"}},

		{"the node that carries most gives up a replica", 2,
			[]store.Workload{up(workload("web", 1, 1)), up(workload("web", 1, 2)), up(onNode(workload("web", 1, 3), "n2"))}, nil,
			[]string{"stop scaled_down web-r1-2 spread"}},
		{"each choice sees the loads the one before it left", 1,
			[]store.Workload{up(workload("web", 1, 1)), up(workload("web", 1, 2)), up(onNode(workload("web", 1, 3), "n2"))}, nil,
			[]string{"stop scaled_down web-r1-2 spread", "stop scaled_down web-r1-3 newest"}},
		{"on nodes with the same load, the replica restarted most goes", 1,
			[]store.Workload{restarted(workload("web", 1, 1), 3), up(onNode(workload("web", 1, 2), "n2"))}, nil,
			[]string{"stop scaled_down web-r1-1 restarts"}},
		{"then the newest", 1,
			[]store.Workload{
				createdAt(up(workload("web", 1, 1)), at.Add(time.Hour)),
				createdAt(up(onNode(workload("web", 1, 2), "n2")), at),
			}, nil,
			[]string{"stop scaled_down web-r1-1 newest"}},
		{"then the highest numbered", 1,
			[]store.Workload{up(workload("web", 1, 1)), up(onNode(workload("web", 1, 2), "n2"))}, nil,
			[]string{"stop scaled_down web-r1-2 newest"}},
		{"a bad replica goes before balance is considered", 2,
			[]store.Workload{up(workload("web", 1, 1)), up(workload("web", 1, 2)), failedW(onNode(workload("web", 1, 3), "n2"))}, nil,
			[]string{"stop scaled_down web-r1-3 failed"}},
		{"nothing goes while the count is right", 3,
			[]store.Workload{failedW(workload("web", 1, 1)), up(workload("web", 1, 2)), up(workload("web", 1, 3))}, nil,
			[]string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			services := []store.Service{service("web", tt.want, 1)}
			got := describeWhy(Plan(definitions(services), tt.workloads, nil, tt.nodes))
			if !reflect.DeepEqual(got, tt.stops) {
				t.Fatalf("plan = %v\nwant   %v", got, tt.stops)
			}
			// The choice does not depend on the order the workloads are read in.
			reversed := make([]store.Workload, len(tt.workloads))
			for i, w := range tt.workloads {
				reversed[len(tt.workloads)-1-i] = w
			}
			if again := describeWhy(Plan(definitions(services), reversed, nil, tt.nodes)); !reflect.DeepEqual(again, tt.stops) {
				t.Fatalf("plan from the reversed input = %v\nwant %v", again, tt.stops)
			}
		})
	}
}

// Scaling down while a deployment is going on keeps the replicas that serve:
// surplus new replicas go first, then old ones as new ones come up.
func TestPlanScalesDownDuringADeployment(t *testing.T) {
	// Four were wanted and became two. Three of the four new replicas are up, and two old ones still run.
	services := []store.Service{service("web", 2, 2)}
	workloads := []store.Workload{
		up(workload("web", 1, 1)), up(workload("web", 1, 2)),
		up(workload("web", 2, 1)), up(workload("web", 2, 2)), up(workload("web", 2, 3)), workload("web", 2, 4),
	}
	got := describeWhy(Plan(definitions(services), workloads, nil, nil))
	want := []string{
		// Two of the four new replicas are surplus; the one not running goes first, then the highest numbered.
		"stop scaled_down web-r2-3 newest",
		"stop scaled_down web-r2-4 not_running",
		// Two new ones are up, so no old one is needed.
		"stop superseded web-r1-1 newest",
		"stop superseded web-r1-2 newest",
	}
	sort.Strings(want)
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("plan = %v\nwant   %v", got, want)
	}
}

// Old replicas that hold a host port the new ones need are let go as many as there are blocked new ones.
func TestPlanHandsOverAsManyHostPortsAsAreBlocked(t *testing.T) {
	port := store.ServicePort{ContainerPort: 80, Protocol: "tcp", HostPort: 8080}
	held := func(w store.Workload) store.Workload {
		w.Ports = []store.ServicePort{port}
		return up(w)
	}
	defs := []store.Definition{{
		Service:  service("web", 3, 2),
		Revision: store.Revision{ServiceID: "web", Revision: 2, Spec: store.ServiceSpec{Ports: []store.ServicePort{port}}},
	}}
	old := []store.Workload{held(workload("web", 1, 1)), held(workload("web", 1, 2)), up(workload("web", 1, 3))}
	blocked := func(n int) []store.Unschedulable {
		var out []store.Unschedulable
		for i := 1; i <= n; i++ {
			out = append(out, store.Unschedulable{ServiceID: "web", Revision: 2, ReplicaIndex: i, Code: scheduler.CodeHostPortConflict})
		}
		return out
	}
	tests := []struct {
		blocked int
		want    []string
	}{
		{0, []string{}},
		{1, []string{"stop port_handover web-r1-2"}},
		{2, []string{"stop port_handover web-r1-1", "stop port_handover web-r1-2"}},
		// Only two old replicas hold the port, however many new ones wait for it.
		{3, []string{"stop port_handover web-r1-1", "stop port_handover web-r1-2"}},
	}
	for _, tt := range tests {
		got := describe(Plan(defs, old, blocked(tt.blocked), nil))
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%d blocked: plan = %v, want %v", tt.blocked, got, tt.want)
		}
	}
}
