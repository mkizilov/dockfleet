package store

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

var testTime = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// runStoreConformance exercises behavior every Store implementation must share.
// newStore returns an empty store; it is called once per subtest.
func runStoreConformance(t *testing.T, newStore func(t *testing.T) Store) {
	t.Helper()
	suites := []struct {
		name string
		run  func(*testing.T, Store)
	}{
		{"nodes", testNodes},
		{"node health and departure", testNodeHealthAndDeparture},
		{"enrollments", testEnrollments},
		{"workloads", testWorkloads},
		{"observations", testObservations},
		{"observation health and workload checks", testObservationHealth},
		{"services", testServices},
		{"service revisions", testRevisions},
		{"service definitions", testDefinitions},
		{"rollout policy and pausing", testRolloutPolicy},
		{"definitions know which revisions are the same spec", testDefinitionSameSpec},
		{"rollout records", testRollouts},
		{"workload ports", testWorkloadPorts},
		{"workload lifecycle", testWorkloadLifecycle},
		{"workload slots are freed by deletion", testWorkloadSlotsAreFreedByDeletion},
		{"unschedulable replicas", testUnschedulable},
		{"service workloads", testServiceWorkloads},
		{"service deletion", testServiceDeletion},
		{"events", testEvents},
		{"leases", testLeases},
		{"control plane epochs", testControlPlane},
		{"control plane epochs in transactions", testControlPlaneInTransactions},
		{"control plane epochs are handed out one at a time", testBeginTermsConcurrently},
		{"the next term waits for transactions that checked the epoch", testNextTermWaitsForTransactionsThatCheckedTheEpoch},
		{"transactions", testTransactions},
		{"transaction rollbacks", testTransactionRollbacks},
	}
	for _, suite := range suites {
		t.Run(suite.name, func(t *testing.T) {
			suite.run(t, newStore(t))
		})
	}
}

func wantErr(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

func noErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func addNode(t *testing.T, s Repositories, id string) Node {
	t.Helper()
	n, err := s.UpsertNode(context.Background(), Node{
		ID: id, Name: id, CPUMillis: 4000, MemoryBytes: 8 << 30,
		RegisteredAt: testTime, LastHeartbeat: testTime,
	})
	noErr(t, err)
	return n
}

func testNodes(t *testing.T, s Store) {
	ctx := context.Background()
	_, err := s.GetNode(ctx, "missing")
	wantErr(t, err, ErrNotFound)
	wantErr(t, s.Heartbeat(ctx, "missing", testTime), ErrNotFound)

	node := Node{
		ID: "node-b", Name: "B", CPUMillis: 4000, MemoryBytes: 8 << 30,
		Labels: map[string]string{"zone": "a"}, AgentVersion: "v1",
		RegisteredAt: testTime, LastHeartbeat: testTime,
	}
	got, err := s.UpsertNode(ctx, node)
	noErr(t, err)
	if got.ID != "node-b" || got.Labels["zone"] != "a" || !got.RegisteredAt.Equal(testTime) || got.CPUMillis != 4000 {
		t.Fatalf("unexpected node: %+v", got)
	}
	addNode(t, s, "node-a")

	later := testTime.Add(time.Minute)
	node.Name, node.RegisteredAt, node.LastHeartbeat = "B2", later, later
	updated, err := s.UpsertNode(ctx, node)
	noErr(t, err)
	if updated.Name != "B2" || !updated.RegisteredAt.Equal(testTime) || !updated.LastHeartbeat.Equal(later) {
		t.Fatalf("update must keep RegisteredAt and refresh the rest: %+v", updated)
	}

	hb := later.Add(time.Minute)
	noErr(t, s.Heartbeat(ctx, "node-b", hb))
	fetched, err := s.GetNode(ctx, "node-b")
	if err != nil || !fetched.LastHeartbeat.Equal(hb) {
		t.Fatalf("GetNode after heartbeat = %+v, %v", fetched, err)
	}

	nodes, err := s.ListNodes(ctx)
	if err != nil || len(nodes) != 2 || nodes[0].ID != "node-a" || nodes[1].ID != "node-b" || nodes[0].Labels == nil {
		t.Fatalf("ListNodes = %+v, %v", nodes, err)
	}
}

func testEnrollments(t *testing.T, s Store) {
	ctx := context.Background()
	_, err := s.GetEnrollment(ctx, "worker-1")
	wantErr(t, err, ErrNotFound)
	_, err = s.RevokeEnrollment(ctx, "worker-1", testTime)
	wantErr(t, err, ErrNotFound)

	created, err := s.CreateEnrollment(ctx, Enrollment{NodeID: "worker-1", Description: "rack 4", CreatedAt: testTime})
	if err != nil || !created.Active() || created.Description != "rack 4" || !created.CreatedAt.Equal(testTime) {
		t.Fatalf("CreateEnrollment = %+v, %v", created, err)
	}
	_, err = s.CreateEnrollment(ctx, Enrollment{NodeID: "worker-1", CreatedAt: testTime})
	wantErr(t, err, ErrConflict)
	_, err = s.CreateEnrollment(ctx, Enrollment{NodeID: "worker-0", CreatedAt: testTime})
	noErr(t, err)

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

func standalone(id, name, node string) Workload {
	return Workload{ID: id, Name: name, Image: "nginx:1", CPUMillis: 500, MemoryBytes: 256 << 20, NodeID: node, CreatedAt: testTime}
}

func testWorkloads(t *testing.T, s Store) {
	ctx := context.Background()
	addNode(t, s, "node-a")
	addNode(t, s, "node-b")

	w, err := s.CreateWorkload(ctx, standalone("w1", "web", "node-a"))
	noErr(t, err)
	if w.DesiredState != DesiredRunning || !w.CreatedAt.Equal(testTime) || !w.UpdatedAt.Equal(testTime) || w.Observed != nil || w.Phase() != WorkloadScheduled {
		t.Fatalf("new workload = %+v", w)
	}
	if w.ServiceID != "" || w.ServiceRevision != 0 || w.ReplicaIndex != 0 {
		t.Fatalf("a standalone workload belongs to no service: %+v", w)
	}

	dupName := standalone("w2", "web", "node-a")
	_, err = s.CreateWorkload(ctx, dupName)
	wantErr(t, err, ErrConflict)
	dupID := standalone("w1", "other", "node-a")
	_, err = s.CreateWorkload(ctx, dupID)
	wantErr(t, err, ErrConflict)
	_, err = s.CreateWorkload(ctx, standalone("w3", "orphan", "ghost"))
	wantErr(t, err, ErrNotFound)

	invalid := map[string]func(*Workload){
		"no cpu":            func(w *Workload) { w.CPUMillis = 0 },
		"no memory":         func(w *Workload) { w.MemoryBytes = 0 },
		"no image":          func(w *Workload) { w.Image = "" },
		"bad desired state": func(w *Workload) { w.DesiredState = "paused" },
		"service id alone":  func(w *Workload) { w.ServiceID = "svc" },
		"revision alone":    func(w *Workload) { w.ServiceRevision = 1 },
		"negative index":    func(w *Workload) { w.ReplicaIndex = -1 },
	}
	for name, mutate := range invalid {
		bad := standalone("w-bad", "bad-"+name, "node-a")
		mutate(&bad)
		if _, err := s.CreateWorkload(ctx, bad); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: error = %v, want ErrInvalid", name, err)
		}
	}

	_, err = s.CreateWorkload(ctx, standalone("w4", "api", "node-b"))
	noErr(t, err)
	got, err := s.GetWorkload(ctx, "w1")
	if err != nil || got.Name != "web" || got.NodeID != "node-a" || got.DesiredState != DesiredRunning {
		t.Fatalf("GetWorkload = %+v, %v", got, err)
	}
	_, err = s.GetWorkload(ctx, "nope")
	wantErr(t, err, ErrNotFound)

	all, err := s.ListWorkloads(ctx)
	if err != nil || len(all) != 2 || all[0].Name != "api" || all[1].Name != "web" {
		t.Fatalf("ListWorkloads = %+v, %v", all, err)
	}
	forA, err := s.ListWorkloadsForNode(ctx, "node-a")
	if err != nil || len(forA) != 1 || forA[0].ID != "w1" {
		t.Fatalf("ListWorkloadsForNode(node-a) = %+v, %v", forA, err)
	}
	none, err := s.ListWorkloadsForNode(ctx, "nobody")
	if err != nil || none == nil || len(none) != 0 {
		t.Fatalf("ListWorkloadsForNode(nobody) = %+v, %v; want empty and non-nil", none, err)
	}

	// Changing the desired state records when, and leaves everything else alone.
	stopAt := testTime.Add(time.Hour)
	stopped, err := s.SetWorkloadDesiredState(ctx, "w1", DesiredStopped, stopAt)
	if err != nil || stopped.DesiredState != DesiredStopped || !stopped.UpdatedAt.Equal(stopAt) || !stopped.CreatedAt.Equal(testTime) {
		t.Fatalf("SetWorkloadDesiredState = %+v, %v", stopped, err)
	}
	same, err := s.SetWorkloadDesiredState(ctx, "w1", DesiredStopped, stopAt.Add(time.Hour))
	if !errors.Is(err, ErrUnchanged) || !same.UpdatedAt.Equal(stopAt) {
		t.Fatalf("setting the current state: %+v, %v; want ErrUnchanged and no change", same, err)
	}
	_, err = s.SetWorkloadDesiredState(ctx, "w1", "paused", stopAt)
	wantErr(t, err, ErrInvalid)
	_, err = s.SetWorkloadDesiredState(ctx, "nope", DesiredRunning, stopAt)
	wantErr(t, err, ErrNotFound)
}

func intPtr(v int) *int { return &v }

func timePtr(v time.Time) *time.Time { return &v }

func observation(id, state string, at time.Time) Observation {
	return Observation{WorkloadID: id, State: state, ObservedAt: at}
}

func testObservations(t *testing.T, s Store) {
	ctx := context.Background()
	addNode(t, s, "node-a")
	addNode(t, s, "node-b")
	_, err := s.CreateWorkload(ctx, standalone("w1", "web", "node-a"))
	noErr(t, err)
	_, err = s.CreateWorkload(ctx, standalone("w2", "api", "node-b"))
	noErr(t, err)

	// An observation passed to CreateWorkload is not stored: only agents' reports are observed state.
	smuggled := standalone("w3", "smuggled", "node-a")
	smuggled.Observed = &Observation{WorkloadID: "w3", State: WorkloadRunning, ObservedAt: testTime}
	created, err := s.CreateWorkload(ctx, smuggled)
	noErr(t, err)
	got, _ := s.GetWorkload(ctx, "w3")
	if created.Observed != nil || got.Observed != nil {
		t.Fatalf("CreateWorkload must ignore Observed: %+v / %+v", created.Observed, got.Observed)
	}

	// A full report round-trips.
	started, finished := testTime.Add(-time.Hour), testTime.Add(-time.Minute)
	report := Observation{
		WorkloadID: "w1", State: WorkloadFailed, Message: "exited", ContainerID: "c0ffee",
		ExitCode: intPtr(137), OOMKilled: true, RestartCount: 2,
		StartedAt: timePtr(started), FinishedAt: timePtr(finished), ObservedAt: testTime.Add(10 * time.Second),
	}
	noErr(t, s.RecordObservations(ctx, "node-a", []Observation{report}))
	w, err := s.GetWorkload(ctx, "w1")
	noErr(t, err)
	o := w.Observed
	if o == nil || o.WorkloadID != "w1" || o.NodeID != "node-a" || o.State != WorkloadFailed || o.Message != "exited" || o.ContainerID != "c0ffee" ||
		o.ExitCode == nil || *o.ExitCode != 137 || !o.OOMKilled || o.RestartCount != 2 ||
		o.StartedAt == nil || !o.StartedAt.Equal(started) || o.FinishedAt == nil || !o.FinishedAt.Equal(finished) || !o.ObservedAt.Equal(testTime.Add(10*time.Second)) {
		t.Fatalf("observation = %+v", o)
	}
	if w.Phase() != WorkloadFailed {
		t.Errorf("Phase = %q", w.Phase())
	}

	// Desired and observed state are independent: reporting "stopped" does not
	// change what was asked for, and asking for "stopped" is not a report.
	if w.DesiredState != DesiredRunning {
		t.Fatalf("an observation changed the desired state: %+v", w)
	}
	_, err = s.SetWorkloadDesiredState(ctx, "w1", DesiredStopped, testTime.Add(time.Hour))
	noErr(t, err)
	w, _ = s.GetWorkload(ctx, "w1")
	if w.Observed == nil || w.Observed.State != WorkloadFailed {
		t.Fatalf("a desired-state change altered the observation: %+v", w.Observed)
	}
	if w2, _ := s.GetWorkload(ctx, "w2"); w2.Observed != nil || w2.Phase() != WorkloadScheduled {
		t.Fatalf("a workload nobody reported on has no observation: %+v", w2)
	}

	// Newer reports replace older ones; late arrivals are ignored.
	noErr(t, s.RecordObservations(ctx, "node-a", []Observation{observation("w1", WorkloadRunning, testTime.Add(20*time.Second))}))
	noErr(t, s.RecordObservations(ctx, "node-a", []Observation{observation("w1", WorkloadPending, testTime.Add(15*time.Second))}))
	w, _ = s.GetWorkload(ctx, "w1")
	if w.Observed.State != WorkloadRunning || w.Observed.ExitCode != nil || w.Observed.OOMKilled {
		t.Fatalf("expected the newest report to win, replacing every field: %+v", w.Observed)
	}

	// Reports for workloads the node does not own, or that do not exist, are ignored.
	noErr(t, s.RecordObservations(ctx, "node-b", []Observation{observation("w1", WorkloadStopped, testTime.Add(time.Hour))}))
	noErr(t, s.RecordObservations(ctx, "node-a", []Observation{observation("ghost", WorkloadRunning, testTime.Add(time.Hour))}))
	w, _ = s.GetWorkload(ctx, "w1")
	if w.Observed.State != WorkloadRunning || w.Observed.NodeID != "node-a" {
		t.Fatalf("a foreign report was accepted: %+v", w.Observed)
	}

	// A batch is all or nothing.
	batch := []Observation{observation("w2", WorkloadRunning, testTime), observation("w2", "exploded", testTime)}
	wantErr(t, s.RecordObservations(ctx, "node-b", batch), ErrInvalid)
	if w2, _ := s.GetWorkload(ctx, "w2"); w2.Observed != nil {
		t.Fatalf("part of a rejected batch was stored: %+v", w2.Observed)
	}
	wantErr(t, s.RecordObservations(ctx, "node-b", []Observation{observation("w2", WorkloadScheduled, testTime)}), ErrInvalid)
	wantErr(t, s.RecordObservations(ctx, "node-b", []Observation{{WorkloadID: "w2", State: WorkloadRunning}}), ErrInvalid)
	noErr(t, s.RecordObservations(ctx, "node-b", nil))
	noErr(t, s.RecordObservations(ctx, "node-b", []Observation{
		observation("w2", WorkloadPending, testTime),
		observation("w2", WorkloadRunning, testTime.Add(time.Second)),
	}))
	if w2, _ := s.GetWorkload(ctx, "w2"); w2.Observed == nil || w2.Observed.State != WorkloadRunning {
		t.Fatalf("batch order must be respected: %+v", w2.Observed)
	}

	// Listings carry the observations.
	list, err := s.ListWorkloadsForNode(ctx, "node-b")
	if err != nil || len(list) != 1 || list[0].Observed == nil || list[0].Observed.State != WorkloadRunning {
		t.Fatalf("ListWorkloadsForNode = %+v, %v", list, err)
	}
	all, _ := s.ListWorkloads(ctx)
	seen := map[string]string{}
	for _, w := range all {
		seen[w.ID] = w.Phase()
	}
	want := map[string]string{"w1": WorkloadRunning, "w2": WorkloadRunning, "w3": WorkloadScheduled}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("phases = %v, want %v", seen, want)
	}
}
