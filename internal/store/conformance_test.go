package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

var testTime = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// runStoreConformance exercises behavior every Store implementation must share.
// The store must be empty.
func runStoreConformance(t *testing.T, s Store) {
	t.Helper()
	ctx := context.Background()

	if err := s.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if _, err := s.GetNode(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetNode(missing) error = %v, want ErrNotFound", err)
	}
	if err := s.Heartbeat(ctx, "missing", testTime); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Heartbeat(missing) error = %v, want ErrNotFound", err)
	}

	node := Node{
		ID: "node-b", Name: "B", CPUMillis: 4000, MemoryBytes: 8 << 30,
		Labels: map[string]string{"zone": "a"}, AgentVersion: "v1",
		RegisteredAt: testTime, LastHeartbeat: testTime,
	}
	got, err := s.UpsertNode(ctx, node)
	if err != nil {
		t.Fatalf("UpsertNode: %v", err)
	}
	if got.ID != "node-b" || got.Labels["zone"] != "a" || !got.RegisteredAt.Equal(testTime) {
		t.Fatalf("unexpected node: %+v", got)
	}
	if _, err := s.UpsertNode(ctx, Node{ID: "node-a", Name: "A", CPUMillis: 1000, MemoryBytes: 1 << 30, RegisteredAt: testTime, LastHeartbeat: testTime}); err != nil {
		t.Fatalf("UpsertNode node-a: %v", err)
	}

	later := testTime.Add(time.Minute)
	node.Name = "B2"
	node.RegisteredAt = later
	node.LastHeartbeat = later
	updated, err := s.UpsertNode(ctx, node)
	if err != nil {
		t.Fatalf("UpsertNode update: %v", err)
	}
	if updated.Name != "B2" || !updated.RegisteredAt.Equal(testTime) || !updated.LastHeartbeat.Equal(later) {
		t.Fatalf("update must keep RegisteredAt and refresh the rest: %+v", updated)
	}

	hb := later.Add(time.Minute)
	if err := s.Heartbeat(ctx, "node-b", hb); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	fetched, err := s.GetNode(ctx, "node-b")
	if err != nil || !fetched.LastHeartbeat.Equal(hb) {
		t.Fatalf("GetNode after heartbeat = %+v, %v", fetched, err)
	}

	nodes, err := s.ListNodes(ctx)
	if err != nil || len(nodes) != 2 || nodes[0].ID != "node-a" || nodes[1].ID != "node-b" {
		t.Fatalf("ListNodes = %+v, %v", nodes, err)
	}
	if nodes[0].Labels == nil {
		t.Fatal("Labels must never be nil")
	}

	w := Workload{ID: "w1", Name: "web", Image: "nginx:1", CPUMillis: 500, MemoryBytes: 256 << 20, NodeID: "node-a", State: WorkloadScheduled, CreatedAt: testTime}
	if _, err := s.CreateWorkload(ctx, w); err != nil {
		t.Fatalf("CreateWorkload: %v", err)
	}
	dupName := w
	dupName.ID = "w2"
	if _, err := s.CreateWorkload(ctx, dupName); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate name error = %v, want ErrConflict", err)
	}
	dupID := w
	dupID.Name = "other"
	if _, err := s.CreateWorkload(ctx, dupID); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate id error = %v, want ErrConflict", err)
	}
	orphan := Workload{ID: "w3", Name: "orphan", Image: "x", CPUMillis: 1, MemoryBytes: 1, NodeID: "ghost", State: WorkloadScheduled, CreatedAt: testTime}
	if _, err := s.CreateWorkload(ctx, orphan); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown node error = %v, want ErrNotFound", err)
	}

	gw, err := s.GetWorkload(ctx, "w1")
	if err != nil || gw.Name != "web" || gw.NodeID != "node-a" || !gw.CreatedAt.Equal(testTime) {
		t.Fatalf("GetWorkload = %+v, %v", gw, err)
	}
	if _, err := s.GetWorkload(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetWorkload(nope) error = %v, want ErrNotFound", err)
	}
	ws, err := s.ListWorkloads(ctx)
	if err != nil || len(ws) != 1 || ws[0].ID != "w1" {
		t.Fatalf("ListWorkloads = %+v, %v", ws, err)
	}
}
