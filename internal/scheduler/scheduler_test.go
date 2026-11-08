package scheduler

import (
	"errors"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/store"
)

var now = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func node(id string, cpu, mem int64, labels map[string]string) store.Node {
	return store.Node{ID: id, CPUMillis: cpu, MemoryBytes: mem, Labels: labels, LastHeartbeat: now}
}

func TestPlacePrefersMostFreeCPU(t *testing.T) {
	cands := []Candidate{
		{Node: node("a", 4000, 8<<30, nil), UsedCPUMillis: 3000},
		{Node: node("b", 4000, 8<<30, nil), UsedCPUMillis: 1000},
		{Node: node("c", 2000, 8<<30, nil)},
	}
	got, err := Place(Request{CPUMillis: 500, MemoryBytes: 1 << 20}, cands)
	if err != nil || got.ID != "b" {
		t.Fatalf("Place = %q, %v; want b", got.ID, err)
	}
}

func TestPlaceTieBreaksDeterministically(t *testing.T) {
	cands := []Candidate{
		{Node: node("b", 1000, 1<<30, nil)},
		{Node: node("a", 1000, 1<<30, nil)},
		{Node: node("c", 1000, 2<<30, nil)},
	}
	got, err := Place(Request{CPUMillis: 100, MemoryBytes: 1}, cands)
	if err != nil || got.ID != "c" {
		t.Fatalf("more free memory should win: %q, %v", got.ID, err)
	}
	got, err = Place(Request{CPUMillis: 100, MemoryBytes: 1}, cands[:2])
	if err != nil || got.ID != "a" {
		t.Fatalf("lowest ID should win ties: %q, %v", got.ID, err)
	}
}

func TestPlaceRespectsCapacity(t *testing.T) {
	cands := []Candidate{{Node: node("a", 1000, 1<<30, nil), UsedCPUMillis: 800, UsedMemoryBytes: 0}}
	if _, err := Place(Request{CPUMillis: 300, MemoryBytes: 1}, cands); !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("cpu overcommit: error = %v, want ErrNoCapacity", err)
	}
	if _, err := Place(Request{CPUMillis: 100, MemoryBytes: 2 << 30}, cands); !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("memory overcommit: error = %v, want ErrNoCapacity", err)
	}
	if _, err := Place(Request{CPUMillis: 200, MemoryBytes: 1 << 30}, cands); err != nil {
		t.Fatalf("exact fit should succeed: %v", err)
	}
}

func TestPlaceRespectsSelector(t *testing.T) {
	cands := []Candidate{
		{Node: node("a", 4000, 1<<30, map[string]string{"zone": "x"})},
		{Node: node("b", 1000, 1<<30, map[string]string{"zone": "y", "tier": "gold"})},
	}
	got, err := Place(Request{CPUMillis: 100, MemoryBytes: 1, NodeSelector: map[string]string{"zone": "y"}}, cands)
	if err != nil || got.ID != "b" {
		t.Fatalf("Place = %q, %v; want b", got.ID, err)
	}
	if _, err := Place(Request{CPUMillis: 1, MemoryBytes: 1, NodeSelector: map[string]string{"zone": "z"}}, cands); !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("unmatched selector: error = %v, want ErrNoCapacity", err)
	}
}

func TestPlaceWithoutCandidates(t *testing.T) {
	if _, err := Place(Request{CPUMillis: 1, MemoryBytes: 1}, nil); !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("error = %v, want ErrNoCapacity", err)
	}
}

func TestCandidatesFiltersStaleNodesAndSumsUsage(t *testing.T) {
	stale := node("stale", 1000, 1<<30, nil)
	stale.LastHeartbeat = now.Add(-time.Hour)
	nodes := []store.Node{node("a", 4000, 1<<30, nil), stale}
	workloads := []store.Workload{
		{NodeID: "a", CPUMillis: 100, MemoryBytes: 10},
		{NodeID: "a", CPUMillis: 200, MemoryBytes: 20},
		{NodeID: "stale", CPUMillis: 999, MemoryBytes: 999},
	}
	got := Candidates(nodes, workloads, now, 30*time.Second)
	if len(got) != 1 || got[0].Node.ID != "a" || got[0].UsedCPUMillis != 300 || got[0].UsedMemoryBytes != 30 {
		t.Fatalf("Candidates = %+v", got)
	}
}

func TestCandidatesIgnoreWorkloadsThatShouldBeStopped(t *testing.T) {
	nodes := []store.Node{node("a", 4000, 1<<30, nil)}
	workloads := []store.Workload{
		{NodeID: "a", CPUMillis: 100, MemoryBytes: 10, DesiredState: store.DesiredRunning},
		{NodeID: "a", CPUMillis: 200, MemoryBytes: 20}, // an unset desired state means running
		{NodeID: "a", CPUMillis: 4000, MemoryBytes: 1 << 29, DesiredState: store.DesiredStopped},
	}
	got := Candidates(nodes, workloads, now, 30*time.Second)
	if len(got) != 1 || got[0].UsedCPUMillis != 300 || got[0].UsedMemoryBytes != 30 {
		t.Fatalf("Candidates = %+v; a stopped workload must not reserve capacity", got)
	}
}

func TestReadyBoundary(t *testing.T) {
	n := node("a", 1, 1, nil)
	n.LastHeartbeat = now.Add(-30 * time.Second)
	if !Ready(n, now, 30*time.Second) {
		t.Error("node exactly at the TTL should still be ready")
	}
	if Ready(n, now, 29*time.Second) {
		t.Error("node past the TTL should not be ready")
	}
}
