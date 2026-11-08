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

// Deleting a retired replica frees its slot; the replica that takes the slot
// is a new workload with a new ID.
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
	wantErr(t, err, ErrConflict) // stopped, but its container may still exist
	noErr(t, s.DeleteWorkload(ctx, "s1"))
	_, err = s.CreateWorkload(ctx, second)
	noErr(t, err)

	replicas, err := s.ListWorkloadsForService(ctx, svc.ID)
	noErr(t, err)
	if len(replicas) != 1 || replicas[0].ID != "s2" {
		t.Fatalf("replicas = %+v", replicas)
	}
}
