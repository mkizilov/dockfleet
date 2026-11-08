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
	if gw.StatusUpdatedAt != nil || gw.StatusMessage != "" {
		t.Fatalf("new workload must have no status report: %+v", gw)
	}

	forNode, err := s.ListWorkloadsForNode(ctx, "node-a")
	if err != nil || len(forNode) != 1 || forNode[0].ID != "w1" {
		t.Fatalf("ListWorkloadsForNode(node-a) = %+v, %v", forNode, err)
	}
	other, err := s.ListWorkloadsForNode(ctx, "node-b")
	if err != nil || other == nil || len(other) != 0 {
		t.Fatalf("ListWorkloadsForNode(node-b) = %+v, %v; want empty non-nil", other, err)
	}

	reported := testTime.Add(time.Hour)
	if err := s.UpdateWorkloadStatus(ctx, "w1", "node-a", WorkloadPending, "accepted", reported); err != nil {
		t.Fatalf("UpdateWorkloadStatus: %v", err)
	}
	gw, err = s.GetWorkload(ctx, "w1")
	if err != nil || gw.State != WorkloadPending || gw.StatusMessage != "accepted" || gw.StatusUpdatedAt == nil || !gw.StatusUpdatedAt.Equal(reported) {
		t.Fatalf("workload after status update = %+v, %v", gw, err)
	}
	if err := s.UpdateWorkloadStatus(ctx, "w1", "node-b", WorkloadRunning, "", reported); !errors.Is(err, ErrNotFound) {
		t.Fatalf("status from the wrong node: error = %v, want ErrNotFound", err)
	}
	if err := s.UpdateWorkloadStatus(ctx, "nope", "node-a", WorkloadRunning, "", reported); !errors.Is(err, ErrNotFound) {
		t.Fatalf("status for unknown workload: error = %v, want ErrNotFound", err)
	}

	runEnrollmentConformance(t, s)
}

func runEnrollmentConformance(t *testing.T, s Store) {
	t.Helper()
	ctx := context.Background()

	if _, err := s.GetEnrollment(ctx, "worker-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetEnrollment(unenrolled) error = %v, want ErrNotFound", err)
	}
	if _, err := s.RevokeEnrollment(ctx, "worker-1", testTime); !errors.Is(err, ErrNotFound) {
		t.Fatalf("RevokeEnrollment(unenrolled) error = %v, want ErrNotFound", err)
	}

	created, err := s.CreateEnrollment(ctx, Enrollment{NodeID: "worker-1", Description: "rack 4", CreatedAt: testTime})
	if err != nil || !created.Active() || created.Description != "rack 4" || !created.CreatedAt.Equal(testTime) {
		t.Fatalf("CreateEnrollment = %+v, %v", created, err)
	}
	if _, err := s.CreateEnrollment(ctx, Enrollment{NodeID: "worker-1", CreatedAt: testTime}); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate active enrollment error = %v, want ErrConflict", err)
	}
	if _, err := s.CreateEnrollment(ctx, Enrollment{NodeID: "worker-0", CreatedAt: testTime}); err != nil {
		t.Fatalf("CreateEnrollment worker-0: %v", err)
	}

	revokedAt := testTime.Add(time.Hour)
	revoked, err := s.RevokeEnrollment(ctx, "worker-1", revokedAt)
	if err != nil || revoked.Active() || !revoked.RevokedAt.Equal(revokedAt) {
		t.Fatalf("RevokeEnrollment = %+v, %v", revoked, err)
	}
	again, err := s.RevokeEnrollment(ctx, "worker-1", revokedAt.Add(time.Hour))
	if err != nil || !again.RevokedAt.Equal(revokedAt) {
		t.Fatalf("revoking twice must keep the first revocation time: %+v, %v", again, err)
	}
	got, err := s.GetEnrollment(ctx, "worker-1")
	if err != nil || got.Active() {
		t.Fatalf("GetEnrollment after revoke = %+v, %v", got, err)
	}

	reenrolled, err := s.CreateEnrollment(ctx, Enrollment{NodeID: "worker-1", Description: "rack 5", CreatedAt: revokedAt.Add(2 * time.Hour)})
	if err != nil || !reenrolled.Active() || reenrolled.Description != "rack 5" {
		t.Fatalf("re-enrollment after revocation = %+v, %v", reenrolled, err)
	}

	list, err := s.ListEnrollments(ctx)
	if err != nil || len(list) != 2 || list[0].NodeID != "worker-0" || list[1].NodeID != "worker-1" || !list[1].Active() {
		t.Fatalf("ListEnrollments = %+v, %v", list, err)
	}
}
