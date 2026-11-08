package reconcile

import (
	"fmt"
	"reflect"
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
		{name: "scaling down stops the highest replicas",
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

		{name: "a deployment retires an old replica when the new one runs",
			services:  []store.Service{service("web", 2, 2)},
			workloads: []store.Workload{up(workload("web", 1, 1)), up(workload("web", 1, 2)), up(workload("web", 2, 1)), workload("web", 2, 2)},
			want:      []want{{ActionStop, ReasonSuperseded, "web-r1-1"}}},
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
			for _, a := range Plan(definitions(tt.services), tt.workloads, nil) {
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
	want := names(Plan(definitions(services), workloads, nil))
	if len(want) != 4 {
		t.Fatalf("plan = %v", want)
	}
	reversed := make([]store.Workload, len(workloads))
	for i, w := range workloads {
		reversed[len(workloads)-1-i] = w
	}
	if got := names(Plan(definitions([]store.Service{services[1], services[0]}), reversed, nil)); !reflect.DeepEqual(got, want) {
		t.Fatalf("plan = %v, want %v", got, want)
	}
}

// Planning twice from the same state gives the same plan, and planning from
// the state a plan leads to gives nothing more to do.
func TestPlanConverges(t *testing.T) {
	services := []store.Service{service("web", 1, 2)}
	workloads := []store.Workload{up(workload("web", 1, 1)), up(workload("web", 1, 2)), up(workload("web", 2, 1))}

	first := Plan(definitions(services), workloads, nil)
	if len(first) != 2 || !reflect.DeepEqual(first, Plan(definitions(services), workloads, nil)) {
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
	second := Plan(definitions(services), workloads, nil)
	if len(second) != 2 || second[0].Kind != ActionDelete || second[1].Kind != ActionDelete {
		t.Fatalf("after the nodes confirmed: %+v", second)
	}
	var left []store.Workload
	for _, w := range workloads {
		if w.DesiredState == store.DesiredRunning {
			left = append(left, w)
		}
	}
	if third := Plan(definitions(services), left, nil); len(third) != 0 {
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
		{"the explanation is for another replica", def(port(8080, "tcp")), old(port(8080, "tcp")),
			[]store.Unschedulable{{ServiceID: "web", Revision: 2, ReplicaIndex: 2, Code: scheduler.CodeHostPortConflict}}, 0},
		{"the explanation is for an older revision", def(port(8080, "tcp")), old(port(8080, "tcp")),
			[]store.Unschedulable{{ServiceID: "web", Revision: 1, ReplicaIndex: 1, Code: scheduler.CodeHostPortConflict}}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actions := Plan(tt.defs, tt.current, tt.waiting)
			if len(actions) != tt.want {
				t.Fatalf("plan = %+v, want %d actions", actions, tt.want)
			}
			if tt.want == 1 && (actions[0].Kind != ActionStop || actions[0].Reason != ReasonPortHandover || actions[0].Workload.Name != "web-r1-1") {
				t.Fatalf("plan = %+v", actions)
			}
		})
	}
}
