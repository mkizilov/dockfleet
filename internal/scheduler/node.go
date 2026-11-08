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
