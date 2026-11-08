package scheduler

import (
	"errors"
	"time"

	"github.com/dockfleet/dockfleet/internal/store"
)

// ErrNoCapacity means no ready node satisfies the request.
var ErrNoCapacity = errors.New("no ready node can satisfy the request")

type Request struct {
	CPUMillis    int64
	MemoryBytes  int64
	NodeSelector map[string]string
}

// Candidate is a ready node together with the capacity already reserved on it.
type Candidate struct {
	Node            store.Node
	UsedCPUMillis   int64
	UsedMemoryBytes int64
}

// Ready reports whether a node has heartbeated within ttl of now.
func Ready(n store.Node, now time.Time, ttl time.Duration) bool {
	return now.Sub(n.LastHeartbeat) <= ttl
}

// Candidates keeps only ready nodes and sums the resources of workloads placed
// on them. Only workloads whose desired state is running reserve resources: a
// workload that is meant to be stopped is not counted, whatever its observed
// phase is.
func Candidates(nodes []store.Node, workloads []store.Workload, now time.Time, ttl time.Duration) []Candidate {
	type usage struct{ cpu, mem int64 }
	used := map[string]usage{}
	for _, w := range workloads {
		if w.DesiredState == store.DesiredStopped {
			continue
		}
		u := used[w.NodeID]
		u.cpu += w.CPUMillis
		u.mem += w.MemoryBytes
		used[w.NodeID] = u
	}
	out := make([]Candidate, 0, len(nodes))
	for _, n := range nodes {
		if !Ready(n, now, ttl) {
			continue
		}
		u := used[n.ID]
		out = append(out, Candidate{Node: n, UsedCPUMillis: u.cpu, UsedMemoryBytes: u.mem})
	}
	return out
}

// Place picks the node with the most free CPU after label and capacity filtering
// (spread strategy). Ties break on free memory, then on node ID, so the choice
// is deterministic.
func Place(req Request, candidates []Candidate) (store.Node, error) {
	var best *Candidate
	var bestCPU, bestMem int64
	for i := range candidates {
		c := &candidates[i]
		if !matches(c.Node.Labels, req.NodeSelector) {
			continue
		}
		freeCPU := c.Node.CPUMillis - c.UsedCPUMillis
		freeMem := c.Node.MemoryBytes - c.UsedMemoryBytes
		if freeCPU < req.CPUMillis || freeMem < req.MemoryBytes {
			continue
		}
		if best == nil || freeCPU > bestCPU ||
			(freeCPU == bestCPU && freeMem > bestMem) ||
			(freeCPU == bestCPU && freeMem == bestMem && c.Node.ID < best.Node.ID) {
			best, bestCPU, bestMem = c, freeCPU, freeMem
		}
	}
	if best == nil {
		return store.Node{}, ErrNoCapacity
	}
	return best.Node, nil
}

func matches(labels, selector map[string]string) bool {
	for k, v := range selector {
		if got, ok := labels[k]; !ok || got != v {
			return false
		}
	}
	return true
}
