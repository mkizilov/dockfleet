package scheduler

import (
	"fmt"
	"time"

	"github.com/dockfleet/dockfleet/internal/store"
)

// Unavailability says why a stored node cannot take new replicas at now, or ""
// when it can: it must have sent a heartbeat within ttl. The text names the
// limit, not the node's age, so it stays the same for as long as the reason does.
func Unavailability(n store.Node, now time.Time, ttl time.Duration) string {
	switch {
	case n.LastHeartbeat.IsZero() || n.LastHeartbeat.Year() <= 1:
		return "the node has deregistered or has never sent a heartbeat"
	case now.Sub(n.LastHeartbeat) > ttl:
		return fmt.Sprintf("no heartbeat within the last %s", ttl)
	}
	return ""
}

// Ready reports whether a node has heartbeated within ttl of now.
func Ready(n store.Node, now time.Time, ttl time.Duration) bool {
	return Unavailability(n, now, ttl) == ""
}

// Timing says how long a silent node is trusted. A node that has not been
// heard from for longer than SuspectAfter is suspect: it gets no new replicas
// but keeps the ones it has. After UnavailableAfter it is unavailable: its
// lease has run out (an agent that lost contact with the manager stops its
// containers before then), and its replicas may be started on other nodes.
type Timing struct {
	SuspectAfter time.Duration
	// UnavailableAfter is zero when nodes are never given up on.
	UnavailableAfter time.Duration
	// Since is when this manager began to watch. Silence from before then is
	// not counted: a manager that was down cannot tell a dead node from one that
	// had nobody to talk to. Zero means no limit.
	Since time.Time
	// Outages, when set, is told when the manager could not use its store, and
	// silence from before the latest such time is not counted either: a heartbeat
	// cannot be recorded while the store is down, so every node would look silent
	// for the whole outage and the fleet would be failed over when the store came
	// back. Nil counts all silence after Since.
	Outages Outages
}

// Outages remembers when the manager's store last failed. *outage.Tracker
// implements it.
type Outages interface {
	// Since is the time of the latest failure, or the zero time.
	Since() time.Time
	// Mark records a failure at the given time.
	Mark(at time.Time)
}

// watchedFrom is the moment from which silence counts.
func (t Timing) watchedFrom() time.Time {
	if t.Outages == nil {
		return t.Since
	}
	return max2(t.Since, t.Outages.Since())
}

// StoreFailed tells the timing that the store could not be used at the given
// time (see Outages). It does nothing without an Outages.
func (t Timing) StoreFailed(at time.Time) {
	if t.Outages != nil {
		t.Outages.Mark(at)
	}
}

// lastContact is when the node was last known to be alive: its last heartbeat,
// or when its agent left cleanly, or when it first registered.
func lastContact(n store.Node) time.Time {
	switch {
	case !n.LastHeartbeat.IsZero() && n.LastHeartbeat.Year() > 1:
		return n.LastHeartbeat
	case !n.DeregisteredAt.IsZero():
		return n.DeregisteredAt
	}
	return n.RegisteredAt
}

// Health is the node's state at now: healthy while it heartbeats, suspect once
// it has been silent for more than SuspectAfter (at once when its agent has
// left), unavailable once it has been silent for more than UnavailableAfter.
func (t Timing) Health(n store.Node, now time.Time) string {
	if t.SuspectAfter <= 0 && t.UnavailableAfter <= 0 {
		return store.NodeHealthy // the zero value does not track health
	}
	silence := now.Sub(max2(lastContact(n), t.watchedFrom()))
	left := n.LastHeartbeat.IsZero() || n.LastHeartbeat.Year() <= 1
	switch {
	case t.UnavailableAfter > 0 && silence > t.UnavailableAfter:
		return store.NodeUnavailable
	case left || silence > t.SuspectAfter:
		return store.NodeSuspect
	}
	return store.NodeHealthy
}

func max2(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// HealthReason says in words why a node is not healthy, or "" when it is. The
// text names the limits, not the node's age, so it stays the same for as long
// as the state does.
func (t Timing) HealthReason(state string) string {
	switch state {
	case store.NodeSuspect:
		return fmt.Sprintf("no heartbeat within the last %s, or the agent has left; no new replicas are placed on it", t.SuspectAfter)
	case store.NodeUnavailable:
		return fmt.Sprintf("no sign of life for more than %s; its replicas are started on other nodes", t.UnavailableAfter)
	}
	return ""
}

// States is every node's health at now, by node ID.
func (t Timing) States(nodes []store.Node, now time.Time) map[string]string {
	out := make(map[string]string, len(nodes))
	for _, n := range nodes {
		out[n.ID] = t.Health(n, now)
	}
	return out
}

// Lost returns the IDs of the nodes that are unavailable at now.
func (t Timing) Lost(nodes []store.Node, now time.Time) map[string]bool {
	lost := map[string]bool{}
	for _, n := range nodes {
		if t.Health(n, now) == store.NodeUnavailable {
			lost[n.ID] = true
		}
	}
	return lost
}

// NodeFor is a stored node as the scheduler sees it at now.
func NodeFor(n store.Node, now time.Time, ttl time.Duration) Node {
	return Node{
		ID: n.ID, CPUMillis: n.CPUMillis, MemoryBytes: n.MemoryBytes, Labels: n.Labels,
		Unavailable: Unavailability(n, now, ttl),
	}
}

// AssignmentFrom is a stored workload as an assignment. It returns false for a
// workload whose desired state is stopped: it no longer reserves anything.
func AssignmentFrom(w store.Workload) (Assignment, bool) {
	if w.DesiredState == store.DesiredStopped {
		return Assignment{}, false
	}
	return Assignment{
		Name: w.Name, NodeID: w.NodeID, Group: w.ServiceID,
		CPUMillis: w.CPUMillis, MemoryBytes: w.MemoryBytes, Ports: w.Ports,
	}, true
}

// Used is what the assignments reserve on one node.
type Used struct {
	CPUMillis   int64
	MemoryBytes int64
	Workloads   int
}

// UsedByNode adds up the reservations of the workloads that are meant to run.
func UsedByNode(workloads []store.Workload) map[string]Used {
	used := map[string]Used{}
	for _, w := range workloads {
		if a, ok := AssignmentFrom(w); ok {
			u := used[a.NodeID]
			u.CPUMillis += a.CPUMillis
			u.MemoryBytes += a.MemoryBytes
			u.Workloads++
			used[a.NodeID] = u
		}
	}
	return used
}
