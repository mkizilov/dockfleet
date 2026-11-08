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
// A service wants a number of replicas, not particular ones. When it has more
// than it wants (it was scaled down), the reconciler chooses which to stop: the
// ones doing the least good first (failed, unhealthy, on a node that has gone
// quiet, not ready yet), then the ones that make a node carry more than its
// share, then the ones that have been restarted most and are newest. A healthy
// replica that has run for a while is the last to go, and nothing that stays is
// touched, so scaling down restarts nothing.
//
// A change of spec is a rollout (internal/rollout): replicas of the new spec
// are created by the scheduler as far as the service's surge allows, and the
// reconciler retires old ones as far as its unavailable limit allows, so the
// service keeps the capacity the policy promises. Replicas that already run
// the current spec are kept whichever revision they were created for, which is
// what makes a rollback cheap: the replicas of the revision it returns to are
// still there. The reconciler also keeps the record of how far each rollout
// has come, and reports one that stalls.
//
// A node that has been silent for longer than its lease is given up on: its
// workloads are told to stop (generation up, so a node that comes back learns at
// once that it no longer owns them), and the scheduler starts the replicas on
// other nodes. The workload on the lost node keeps its record until that node,
// if it returns, has removed the container; if it never does, the record stays
// as the proof that the replica was moved.
//
// Everything here is derived from the store on every pass, so a pass can be
// repeated, interrupted or run by another manager without harm.
package reconcile

import (
	"sort"
	"time"

	"github.com/dockfleet/dockfleet/internal/rollout"
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
	ReasonScaledDown     = "scaled_down"      // the service has more replicas than it wants
	ReasonServiceDeleted = "service_deleted"  // the service was deleted
	ReasonSuperseded     = "superseded"       // the replica has an older spec and the rollout may retire it
	ReasonNodeLost       = "node_unavailable" // the node has been silent for longer than its lease
	ReasonRetired        = "retired"          // the node confirmed that the container is gone
)

// Why a replica was the one chosen when some had to go.
const (
	WhyFailed      = "failed"       // its container failed and is not running
	WhyUnhealthy   = "unhealthy"    // it runs but fails its health check
	WhySuspectNode = "suspect_node" // its node has gone quiet
	WhyNotRunning  = "not_running"  // it is not ready yet
	WhySpread      = "spread"       // its node carried more of the service than the others
	WhyRestarts    = "restarts"     // it had been restarted most
	WhyNewest      = "newest"       // it was the newest of equals
)

// Action is one change to a workload.
type Action struct {
	Kind   string
	Reason string
	// Why says what made this replica the one to go, when there was a choice.
	Why      string
	Workload store.Workload
}

// containerGone is whether the node has confirmed that a workload that is
// meant to be stopped no longer has a container. Only a report made after the
// node received the instruction counts.
func containerGone(w store.Workload) bool { return w.ContainerGone() }

// How much good a replica does, from least (removed first) to most.
const (
	tierFailed = iota
	tierUnhealthy
	tierSuspectNode
	tierNotRunning
	tierServing
)

func tierOf(w store.Workload, nodes map[string]string) int {
	o := w.Observed
	switch {
	case o != nil && o.State == store.WorkloadFailed:
		return tierFailed
	case o != nil && o.State == store.WorkloadRunning && o.Health == store.HealthUnhealthy:
		return tierUnhealthy
	case nodes[w.NodeID] == store.NodeSuspect:
		return tierSuspectNode
	case !rollout.Ready(w):
		return tierNotRunning
	}
	return tierServing
}

func restartsOf(w store.Workload) int {
	if w.Observed == nil {
		return 0
	}
	return w.Observed.RestartCount
}

// goesBefore is whether replica a should be removed before replica b. load is
// how many of the candidates each node carries now.
func goesBefore(a, b store.Workload, load map[string]int, nodes map[string]string) bool {
	if ta, tb := tierOf(a, nodes), tierOf(b, nodes); ta != tb {
		return ta < tb
	}
	if load[a.NodeID] != load[b.NodeID] {
		return load[a.NodeID] > load[b.NodeID]
	}
	if ra, rb := restartsOf(a), restartsOf(b); ra != rb {
		return ra > rb
	}
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.After(b.CreatedAt)
	}
	if a.ReplicaIndex != b.ReplicaIndex {
		return a.ReplicaIndex > b.ReplicaIndex
	}
	return a.ID > b.ID
}

// choice is a replica picked to go, and why.
type choice struct {
	w   store.Workload
	why string
}

// pick chooses k of the candidates to remove, one at a time, so that each choice
// sees the loads the earlier ones left behind. The answer depends only on the
// candidates, not on the order they come in.
func pick(candidates []store.Workload, k int, nodes map[string]string) []choice {
	k = min(k, len(candidates))
	if k <= 0 {
		return nil
	}
	load := map[string]int{}
	for _, w := range candidates {
		load[w.NodeID]++
	}
	rest := append([]store.Workload(nil), candidates...)
	out := make([]choice, 0, k)
	for len(out) < k {
		best := 0
		for i := 1; i < len(rest); i++ {
			if goesBefore(rest[i], rest[best], load, nodes) {
				best = i
			}
		}
		w := rest[best]
		out = append(out, choice{w: w, why: whyChosen(w, rest, best, load, nodes)})
		load[w.NodeID]--
		rest = append(rest[:best], rest[best+1:]...)
	}
	return out
}

// whyChosen names what set the chosen replica apart from the others left.
func whyChosen(w store.Workload, rest []store.Workload, chosen int, load map[string]int, nodes map[string]string) string {
	switch tierOf(w, nodes) {
	case tierFailed:
		return WhyFailed
	case tierUnhealthy:
		return WhyUnhealthy
	case tierSuspectNode:
		return WhySuspectNode
	case tierNotRunning:
		return WhyNotRunning
	}
	moreLoaded, moreRestarts := false, false
	for i, other := range rest {
		if i == chosen {
			continue
		}
		moreLoaded = moreLoaded || load[other.NodeID] < load[w.NodeID]
		moreRestarts = moreRestarts || restartsOf(other) < restartsOf(w)
	}
	switch {
	case moreLoaded:
		return WhySpread
	case moreRestarts:
		return WhyRestarts
	}
	return WhyNewest
}

// Plan lists the changes that bring workloads closer to the services' definitions.
// nodes is the health state of each node (a node that is not listed is healthy).
//
//   - A service with more replicas of its current spec than it wants stops the
//     surplus, the ones doing the least good first.
//   - Old replicas (another spec than the current one) are retired within the
//     service's rollout policy. Those beyond the wanted count go at once, which
//     is what a scale-down during a rollout leaves. Unless the rollout is
//     paused, old replicas that provide nothing (not ready) go too, and ready
//     ones go as long as at least the wanted count minus max_unavailable stay
//     ready. An old replica on a node that has gone quiet is left alone: it is
//     not counted on, and not given up on either until the node returns or the
//     failover takes over. A rollout therefore never leaves the service with
//     less capacity than its policy promises, and when it cannot retire an old
//     replica without doing so it waits and says why.
//   - Every replica of a deleted service is stopped, whatever its spec.
//   - A workload on a lost node is stopped, so that its replica is started on
//     another node. It is not counted when choosing what else to stop.
//   - A stopped workload whose node confirmed the container is gone is deleted.
//
// Workloads that belong to no service are not touched: a standalone workload
// is pinned to the node an administrator chose, so there is nowhere to move it.
// The result is ordered by workload name, stops before deletes.
func Plan(defs []store.Definition, workloads []store.Workload, nodes map[string]string) []Action {
	byService := make(map[string][]store.Workload, len(defs))
	for _, w := range workloads {
		if w.ServiceID != "" {
			byService[w.ServiceID] = append(byService[w.ServiceID], w)
		}
	}
	var actions []Action
	for _, d := range defs {
		actions = append(actions, planService(d, byService[d.Service.ID], nodes)...)
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

// tally counts workloads and the ones among them that can be counted on.
func tally(ws []store.Workload, nodes map[string]string) (n, counted int) {
	for _, w := range ws {
		if rollout.Available(w, nodes) {
			counted++
		}
	}
	return len(ws), counted
}

// without returns ws minus the workloads in chosen.
func without(ws []store.Workload, chosen map[string]bool) []store.Workload {
	var out []store.Workload
	for _, w := range ws {
		if !chosen[w.ID] {
			out = append(out, w)
		}
	}
	return out
}

// planService plans one service.
func planService(d store.Definition, workloads []store.Workload, nodes map[string]string) []Action {
	s := d.Service
	want, down := s.Replicas, ReasonScaledDown
	if s.DeletedAt != nil {
		want, down = 0, ReasonServiceDeleted
	}

	var actions []Action
	var current, old []store.Workload
	for _, w := range workloads {
		switch {
		case w.DesiredState == store.DesiredStopped:
			if containerGone(w) {
				actions = append(actions, Action{Kind: ActionDelete, Reason: ReasonRetired, Workload: w})
			}
		case nodes[w.NodeID] == store.NodeUnavailable:
			actions = append(actions, Action{Kind: ActionStop, Reason: ReasonNodeLost, Workload: w})
		case d.Serves(w.ServiceRevision):
			current = append(current, w)
		default:
			old = append(old, w)
		}
	}

	chosen := map[string]bool{}
	stop := func(cs []choice, reason func(store.Workload) string) {
		for _, c := range cs {
			chosen[c.w.ID] = true
			actions = append(actions, Action{Kind: ActionStop, Reason: reason(c.w), Why: c.why, Workload: c.w})
		}
	}
	// A replica that is beyond the wanted count was scaled away; any other old
	// replica is retired by the rollout.
	retired := func(w store.Workload) string {
		if w.ReplicaIndex > want {
			return down
		}
		return ReasonSuperseded
	}

	// Replicas of the current spec beyond what is wanted.
	stop(pick(current, len(current)-want, nodes), func(store.Workload) string { return down })
	if s.DeletedAt != nil {
		stop(pick(old, len(old), nodes), func(store.Workload) string { return down })
		return actions
	}
	kept := without(current, chosen)

	// Old replicas beyond the wanted count go whatever their state.
	c := rollout.Census{Want: want, Old: len(old)}
	c.Updated, c.UpdatedReady = tally(kept, nodes)
	stop(pick(old, rollout.Excess(c), nodes), retired)
	if s.Paused {
		return actions
	}

	// An old replica that is not ready provides nothing, so retiring it costs
	// nothing. A ready one goes as far as the unavailable limit allows, and only
	// if its node is not under suspicion: a replica that cannot be counted on is
	// not a reason to retire it, nor is it counted as capacity.
	var unready, counted []store.Workload
	for _, w := range without(old, chosen) {
		switch {
		case !rollout.Ready(w):
			unready = append(unready, w)
		case rollout.Available(w, nodes):
			counted = append(counted, w)
		}
	}
	stop(pick(unready, len(unready), nodes), retired)
	c.Old, c.OldReady = len(counted), len(counted)
	stop(pick(counted, rollout.Stoppable(s.Rollout, false, c), nodes), retired)
	return actions
}

// NodeChange is a node whose health state is not the one recorded for it.
type NodeChange struct {
	Node     store.Node
	From, To string
}

// PlanNodes lists the nodes whose health at now differs from the state last
// recorded for them, ordered by node ID. The recorded state is what makes a
// change an event: a node that stays suspect is reported once.
func PlanNodes(nodes []store.Node, now time.Time, timing scheduler.Timing) []NodeChange {
	var out []NodeChange
	for _, n := range nodes {
		from := n.Health
		if from == "" {
			from = store.NodeHealthy
		}
		if to := timing.Health(n, now); to != from {
			out = append(out, NodeChange{Node: n, From: from, To: to})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Node.ID < out[j].Node.ID })
	return out
}
