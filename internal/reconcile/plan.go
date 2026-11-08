// Package reconcile compares what service definitions ask for with what the
// agents have reported, and changes the desired state of workloads until they
// agree. It decides and records; agents carry the decisions out, and the
// scheduler (internal/placement) creates the workloads that are missing.
//
// A workload is one container's lifetime. The reconciler moves it one way only:
// running, then stopped (an instruction to its node to stop and remove the
// container), then deleted once the node has confirmed that the container is
// gone. A workload is never started again after it was stopped, so a workload
// ID never names two containers. The slot a deleted workload held is free again,
// and the scheduler fills it with a new workload when the service still wants a
// replica there.
//
// Everything here is derived from the store on every pass, so a pass can be
// repeated, interrupted or run by another manager without harm.
package reconcile

import (
	"sort"
	"strconv"

	"github.com/dockfleet/dockfleet/internal/scheduler"
	"github.com/dockfleet/dockfleet/internal/store"
)

// Kinds of action.
const (
	// ActionStop sets a workload's desired state to stopped.
	ActionStop = "stop"
	// ActionDelete removes the record of a stopped workload whose container is gone.
	ActionDelete = "delete"
)

// Reasons for an action.
const (
	ReasonScaledDown     = "scaled_down"     // the replica index is beyond the service's replica count
	ReasonServiceDeleted = "service_deleted" // the service was deleted
	ReasonSuperseded     = "superseded"      // the same replica of the current revision is up
	ReasonPortHandover   = "port_handover"   // the new replica needs the fixed host port the old one holds
	ReasonRetired        = "retired"         // the node confirmed that the container is gone
)

// Action is one change to a workload.
type Action struct {
	Kind     string
	Reason   string
	Workload store.Workload
}

type slot struct {
	serviceID string
	index     int
}

// replicaUp is whether a workload's replica has done what it was started for:
// its agent has received the latest instruction and reports the container
// running, or finished successfully.
func replicaUp(w store.Workload) bool {
	o := w.Observed
	return w.DesiredState == store.DesiredRunning && o != nil && o.AckedGeneration >= w.Generation &&
		(o.State == store.WorkloadRunning || o.State == store.WorkloadSucceeded)
}

// containerGone is whether the node has confirmed that a workload that is
// meant to be stopped no longer has a container. Only a report made after the
// node received the instruction counts.
func containerGone(w store.Workload) bool {
	o := w.Observed
	return w.DesiredState == store.DesiredStopped && o != nil && o.AckedGeneration >= w.Generation &&
		o.State == store.WorkloadStopped && o.ContainerID == ""
}

// sharesHostPort is whether any of a workload's fixed host ports is also in the new spec's.
func sharesHostPort(held, wanted []store.ServicePort) bool {
	key := func(p store.ServicePort) string {
		if p.Protocol == "" {
			p.Protocol = "tcp"
		}
		return p.Protocol + "/" + strconv.Itoa(p.HostPort)
	}
	used := map[string]bool{}
	for _, p := range held {
		if p.HostPort != 0 {
			used[key(p)] = true
		}
	}
	for _, p := range wanted {
		if p.HostPort != 0 && used[key(p)] {
			return true
		}
	}
	return false
}

// Plan lists the changes that bring workloads closer to the services' definitions.
// waiting holds the scheduler's explanations for replicas it could not place.
//
//   - Replicas beyond a service's replica count are stopped, and so are all
//     of a deleted service's.
//   - A replica of an older revision is stopped once the same replica of the
//     current revision is up, so a deployment replaces replicas one by one and
//     never leaves a gap; if the new replica never comes up, the old one stays.
//   - The exception is a fixed host port: when the new replica cannot be placed
//     because every node already uses a host port that the old replica holds
//     and the new one needs too, the old one is stopped to free it. There is a
//     gap then, because the two cannot run side by side.
//   - A stopped workload whose node confirmed the container is gone is deleted.
//
// Workloads that belong to no service are not touched. The result is ordered
// by workload name, stops before deletes.
func Plan(defs []store.Definition, workloads []store.Workload, waiting []store.Unschedulable) []Action {
	byID := make(map[string]store.Definition, len(defs))
	for _, d := range defs {
		byID[d.Service.ID] = d
	}
	up := map[slot]bool{}
	for _, w := range workloads {
		if d, ok := byID[w.ServiceID]; ok && w.ServiceRevision == d.Service.CurrentRevision && replicaUp(w) {
			up[slot{d.Service.ID, w.ReplicaIndex}] = true
		}
	}
	portBlocked := map[slot]bool{}
	for _, u := range waiting {
		if d, ok := byID[u.ServiceID]; ok && u.Revision == d.Service.CurrentRevision && u.Code == scheduler.CodeHostPortConflict {
			portBlocked[slot{u.ServiceID, u.ReplicaIndex}] = true
		}
	}

	var actions []Action
	for _, w := range workloads {
		d, ok := byID[w.ServiceID]
		if !ok {
			continue
		}
		s := d.Service
		switch {
		case w.DesiredState == store.DesiredStopped:
			if containerGone(w) {
				actions = append(actions, Action{Kind: ActionDelete, Reason: ReasonRetired, Workload: w})
			}
		case w.ReplicaIndex > s.Replicas:
			reason := ReasonScaledDown
			if s.DeletedAt != nil {
				reason = ReasonServiceDeleted
			}
			actions = append(actions, Action{Kind: ActionStop, Reason: reason, Workload: w})
		case w.ServiceRevision < s.CurrentRevision && up[slot{s.ID, w.ReplicaIndex}]:
			actions = append(actions, Action{Kind: ActionStop, Reason: ReasonSuperseded, Workload: w})
		case w.ServiceRevision < s.CurrentRevision && portBlocked[slot{s.ID, w.ReplicaIndex}] && sharesHostPort(w.Ports, d.Revision.Spec.Ports):
			actions = append(actions, Action{Kind: ActionStop, Reason: ReasonPortHandover, Workload: w})
		}
	}
	sort.SliceStable(actions, func(i, j int) bool {
		a, b := actions[i], actions[j]
		if a.Kind != b.Kind {
			return a.Kind == ActionStop
		}
		return a.Workload.Name < b.Workload.Name
	})
	return actions
}
