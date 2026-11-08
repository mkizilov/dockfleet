package store

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func testWorkloadLifecycle(t *testing.T, s Store) {
	ctx := context.Background()
	addNode(t, s, "node-a")

	w := standalone("w1", "web", "node-a")
	w.Command = []string{"sleep", "300"}
	w.Env = map[string]string{"A": "1"}
	w.RestartPolicy = &RestartPolicy{Condition: RestartOnFailure, MaxRetries: 2}
	created, err := s.CreateWorkload(ctx, w)
	noErr(t, err)
	if created.Generation != 1 {
		t.Fatalf("a new workload starts at generation 1, got %d", created.Generation)
	}

	// How to run the container comes back on every read path, as a copy.
	w.Command[0], w.Env["A"] = "changed", "2"
	wantRun := func(what string, got Workload) {
		t.Helper()
		if !reflect.DeepEqual(got.Command, []string{"sleep", "300"}) || !reflect.DeepEqual(got.Env, map[string]string{"A": "1"}) ||
			got.RestartPolicy == nil || got.RestartPolicy.Condition != RestartOnFailure || got.RestartPolicy.MaxRetries != 2 {
			t.Fatalf("%s: command %v, env %v, restart policy %+v", what, got.Command, got.Env, got.RestartPolicy)
		}
	}
	got, err := s.GetWorkload(ctx, "w1")
	noErr(t, err)
	wantRun("GetWorkload", got)
	got.Env["A"] = "mutated"
	got.Command[0] = "mutated"
	again, err := s.GetWorkload(ctx, "w1")
	noErr(t, err)
	wantRun("GetWorkload after the caller changed a result", again)
	all, err := s.ListWorkloads(ctx)
	noErr(t, err)
	wantRun("ListWorkloads", all[0])
	forNode, err := s.ListWorkloadsForNode(ctx, "node-a")
	noErr(t, err)
	wantRun("ListWorkloadsForNode", forNode[0])

	// Every change of the desired state is a new generation; repeating it is not.
	stopped, err := s.SetWorkloadDesiredState(ctx, "w1", DesiredStopped, testTime.Add(time.Minute))
	noErr(t, err)
	if stopped.Generation != 2 || stopped.DesiredState != DesiredStopped {
		t.Fatalf("stopped = %+v", stopped)
	}
	same, err := s.SetWorkloadDesiredState(ctx, "w1", DesiredStopped, testTime.Add(2*time.Minute))
	wantErr(t, err, ErrUnchanged)
	if same.Generation != 2 {
		t.Fatalf("a repeated instruction changed the generation: %+v", same)
	}

	// The agent's acknowledgment is stored with its observation and does not touch intent.
	ack := Observation{WorkloadID: "w1", State: WorkloadStopped, AckedGeneration: 2, ObservedAt: testTime.Add(3 * time.Minute)}
	noErr(t, s.RecordObservations(ctx, "node-a", []Observation{ack}))
	got, err = s.GetWorkload(ctx, "w1")
	noErr(t, err)
	if got.Observed == nil || got.Observed.AckedGeneration != 2 || got.Observed.State != WorkloadStopped || got.Generation != 2 || got.DesiredState != DesiredStopped {
		t.Fatalf("workload after the acknowledgment = %+v observed %+v", got, got.Observed)
	}

	// Only a workload that is meant to be stopped can be deleted, and its observation goes with it.
	_, err = s.CreateWorkload(ctx, standalone("w2", "keeper", "node-a"))
	noErr(t, err)
	wantErr(t, s.DeleteWorkload(ctx, "w2"), ErrInvalid)
	wantErr(t, s.DeleteWorkload(ctx, "nobody"), ErrNotFound)
	noErr(t, s.DeleteWorkload(ctx, "w1"))
	_, err = s.GetWorkload(ctx, "w1")
	wantErr(t, err, ErrNotFound)
	wantErr(t, s.DeleteWorkload(ctx, "w1"), ErrNotFound)
	left, err := s.ListWorkloads(ctx)
	noErr(t, err)
	if len(left) != 1 || left[0].ID != "w2" {
		t.Fatalf("workloads left = %+v", left)
	}
	reborn, err := s.CreateWorkload(ctx, standalone("w1", "web", "node-a"))
	noErr(t, err)
	if reborn.Observed != nil || reborn.Generation != 1 {
		t.Fatalf("a deleted workload left state behind: %+v", reborn)
	}
	if got, _ := s.GetWorkload(ctx, "w1"); got.Observed != nil {
		t.Fatalf("a deleted workload's observation came back: %+v", got.Observed)
	}
}

// A replica slot belongs to the one workload in it that is meant to run. A
// workload that was stopped lets go of it at once, even while its record (and
// perhaps its container, on a node that cannot be reached) is still there; the
// replica that takes the slot is a new workload with a new ID, and the stopped
// one can never be started again beside it.
func testWorkloadSlotsAreFreedByDeletion(t *testing.T, s Store) {
	ctx := context.Background()
	addNode(t, s, "node-a")
	svc := newService(t, s, "slots", 1)

	first := standalone("s1", "slot-1", "node-a")
	first.ServiceID, first.ServiceRevision, first.ReplicaIndex = svc.ID, 1, 1
	_, err := s.CreateWorkload(ctx, first)
	noErr(t, err)
	second := standalone("s2", "slot-1-again", "node-a")
	second.ServiceID, second.ServiceRevision, second.ReplicaIndex = svc.ID, 1, 1
	_, err = s.CreateWorkload(ctx, second)
	wantErr(t, err, ErrConflict)

	_, err = s.SetWorkloadDesiredState(ctx, "s1", DesiredStopped, testTime)
	noErr(t, err)
	_, err = s.CreateWorkload(ctx, second)
	noErr(t, err)

	// Both are on record, one meant to run and one meant to stop.
	replicas, err := s.ListWorkloadsForService(ctx, svc.ID)
	noErr(t, err)
	if len(replicas) != 2 || replicas[0].ID != "s1" && replicas[1].ID != "s1" {
		t.Fatalf("replicas = %+v", replicas)
	}

	// Another one meant to run in the slot conflicts, and so does starting the stopped one again.
	third := standalone("s3", "slot-1-third", "node-a")
	third.ServiceID, third.ServiceRevision, third.ReplicaIndex = svc.ID, 1, 1
	_, err = s.CreateWorkload(ctx, third)
	wantErr(t, err, ErrConflict)
	_, err = s.SetWorkloadDesiredState(ctx, "s1", DesiredRunning, testTime)
	wantErr(t, err, ErrConflict)
	if got, _ := s.GetWorkload(ctx, "s1"); got.DesiredState != DesiredStopped || got.Generation != 2 {
		t.Fatalf("a refused instruction changed the workload: %+v", got)
	}

	// Another stopped workload may share the slot with it.
	fourth := standalone("s4", "slot-1-fourth", "node-a")
	fourth.ServiceID, fourth.ServiceRevision, fourth.ReplicaIndex = svc.ID, 1, 1
	fourth.DesiredState = DesiredStopped
	_, err = s.CreateWorkload(ctx, fourth)
	noErr(t, err)

	noErr(t, s.DeleteWorkload(ctx, "s1"))
	noErr(t, s.DeleteWorkload(ctx, "s4"))
	replicas, err = s.ListWorkloadsForService(ctx, svc.ID)
	noErr(t, err)
	if len(replicas) != 1 || replicas[0].ID != "s2" {
		t.Fatalf("replicas = %+v", replicas)
	}
}

func testNodeHealthAndDeparture(t *testing.T, s Store) {
	ctx := context.Background()
	wantErr(t, s.SetNodeHealth(ctx, "missing", NodeSuspect, testTime), ErrNotFound)
	wantErr(t, s.DeregisterNode(ctx, "missing", testTime), ErrNotFound)

	node, err := s.UpsertNode(ctx, Node{ID: "node-h", Name: "h", CPUMillis: 1000, MemoryBytes: 1 << 30, RegisteredAt: testTime, LastHeartbeat: testTime})
	noErr(t, err)
	if node.Health != NodeHealthy || !node.HealthSince.Equal(testTime) || !node.DeregisteredAt.IsZero() {
		t.Fatalf("a new node starts healthy since it registered: %+v", node)
	}

	since := testTime.Add(time.Minute)
	noErr(t, s.SetNodeHealth(ctx, "node-h", NodeUnavailable, since))
	wantErr(t, s.SetNodeHealth(ctx, "node-h", "sleeping", since), ErrInvalid)
	got, err := s.GetNode(ctx, "node-h")
	noErr(t, err)
	if got.Health != NodeUnavailable || !got.HealthSince.Equal(since) {
		t.Fatalf("node after SetNodeHealth = %+v", got)
	}

	// Registering again keeps the recorded state, so the reconciler sees the recovery and reports it.
	again, err := s.UpsertNode(ctx, Node{ID: "node-h", Name: "h2", CPUMillis: 1000, MemoryBytes: 1 << 30, RegisteredAt: since, LastHeartbeat: since})
	noErr(t, err)
	if again.Health != NodeUnavailable || !again.HealthSince.Equal(since) {
		t.Fatalf("registration reset the recorded health: %+v", again)
	}

	// A clean departure clears the heartbeat and remembers when; registering again forgets it.
	left := since.Add(time.Minute)
	noErr(t, s.DeregisterNode(ctx, "node-h", left))
	got, err = s.GetNode(ctx, "node-h")
	noErr(t, err)
	if !got.LastHeartbeat.IsZero() && got.LastHeartbeat.Year() > 1 || !got.DeregisteredAt.Equal(left) {
		t.Fatalf("node after DeregisterNode = %+v", got)
	}
	back, err := s.UpsertNode(ctx, Node{ID: "node-h", Name: "h2", CPUMillis: 1000, MemoryBytes: 1 << 30, RegisteredAt: left, LastHeartbeat: left.Add(time.Minute)})
	noErr(t, err)
	if !back.DeregisteredAt.IsZero() || !back.LastHeartbeat.Equal(left.Add(time.Minute)) {
		t.Fatalf("registering again must clear the departure: %+v", back)
	}
}

// What an agent says about a container's health and how often it was restarted
// is kept with the observation, and a change of either is a change of the report.
func testObservationHealth(t *testing.T, s Store) {
	ctx := context.Background()
	addNode(t, s, "node-a")
	_, err := s.CreateWorkload(ctx, standalone("h1", "probe", "node-a"))
	noErr(t, err)

	obs := Observation{WorkloadID: "h1", State: WorkloadRunning, Health: HealthUnhealthy, RestartCount: 2, ObservedAt: testTime}
	noErr(t, s.RecordObservations(ctx, "node-a", []Observation{obs}))
	got, err := s.GetWorkload(ctx, "h1")
	noErr(t, err)
	if got.Observed == nil || got.Observed.Health != HealthUnhealthy || got.Observed.RestartCount != 2 {
		t.Fatalf("observation = %+v", got.Observed)
	}
	healthy := obs
	healthy.Health = HealthHealthy
	if healthy.SameReport(*got.Observed) {
		t.Fatal("a change of health is a change of the report")
	}
	noErr(t, s.RecordObservations(ctx, "node-a", []Observation{healthy}))
	got, _ = s.GetWorkload(ctx, "h1")
	if got.Observed.Health != HealthHealthy {
		t.Fatalf("observation = %+v", got.Observed)
	}

	// A workload keeps the health check it was placed with, as a copy.
	w := standalone("h2", "probe-2", "node-a")
	w.HealthCheck = &HealthCheck{Command: []string{"curl", "-f", "localhost"}, IntervalSeconds: 10, TimeoutSeconds: 2, StartPeriodSeconds: 5, Retries: 4}
	w.RestartPolicy = &RestartPolicy{Condition: RestartAlways, MaxRetries: 7, BackoffSeconds: 2, MaxBackoffSeconds: 90}
	_, err = s.CreateWorkload(ctx, w)
	noErr(t, err)
	w.HealthCheck.Command[0] = "changed"
	got, err = s.GetWorkload(ctx, "h2")
	noErr(t, err)
	wantHC := &HealthCheck{Command: []string{"curl", "-f", "localhost"}, IntervalSeconds: 10, TimeoutSeconds: 2, StartPeriodSeconds: 5, Retries: 4}
	wantRP := &RestartPolicy{Condition: RestartAlways, MaxRetries: 7, BackoffSeconds: 2, MaxBackoffSeconds: 90}
	if !reflect.DeepEqual(got.HealthCheck, wantHC) || !reflect.DeepEqual(got.RestartPolicy, wantRP) {
		t.Fatalf("health check %+v, restart policy %+v", got.HealthCheck, got.RestartPolicy)
	}
	got.HealthCheck.Command[0] = "mutated"
	if again, _ := s.GetWorkload(ctx, "h2"); again.HealthCheck.Command[0] != "curl" {
		t.Fatal("a caller changed the stored health check through a result")
	}
}
