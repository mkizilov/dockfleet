package store

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func webSpec() ServiceSpec {
	return ServiceSpec{
		Image:        "nginx:1.27",
		Command:      []string{"nginx", "-g", "daemon off;"},
		Env:          map[string]string{"MODE": "prod"},
		CPUMillis:    500,
		MemoryBytes:  128 << 20,
		NodeSelector: map[string]string{"zone": "a"},
	}
}

func newService(t *testing.T, s Repositories, name string, replicas int) Service {
	t.Helper()
	svc, err := s.CreateService(context.Background(), NewService{
		ID: "svc-" + name, Name: name, Replicas: replicas, Spec: webSpec(),
		Message: "first deploy", CreatedBy: "admin", At: testTime,
	})
	noErr(t, err)
	return svc
}

func testServices(t *testing.T, s Store) {
	ctx := context.Background()
	svc := newService(t, s, "web", 3)
	if svc.ID != "svc-web" || svc.Name != "web" || svc.Replicas != 3 || svc.CurrentRevision != 1 || svc.Version != 1 ||
		!svc.CreatedAt.Equal(testTime) || !svc.UpdatedAt.Equal(testTime) || svc.DeletedAt != nil {
		t.Fatalf("new service = %+v", svc)
	}

	// The first revision is created with the service.
	rev, err := s.GetRevision(ctx, svc.ID, 1)
	noErr(t, err)
	if rev.ServiceID != svc.ID || rev.Revision != 1 || !reflect.DeepEqual(rev.Spec, webSpec()) || rev.SpecHash != webSpec().Hash() ||
		rev.Message != "first deploy" || rev.CreatedBy != "admin" || !rev.CreatedAt.Equal(testTime) {
		t.Fatalf("revision 1 = %+v", rev)
	}

	byID, err := s.GetService(ctx, svc.ID)
	noErr(t, err)
	byName, err := s.GetServiceByName(ctx, "web")
	noErr(t, err)
	for _, got := range []Service{byID, byName} {
		if got.ID != svc.ID || got.Name != svc.Name || got.Replicas != svc.Replicas || got.CurrentRevision != svc.CurrentRevision ||
			got.Version != svc.Version || !got.CreatedAt.Equal(svc.CreatedAt) || got.DeletedAt != nil {
			t.Fatalf("lookups disagree: created %+v, looked up %+v", svc, got)
		}
	}
	_, err = s.GetService(ctx, "nope")
	wantErr(t, err, ErrNotFound)
	_, err = s.GetServiceByName(ctx, "nope")
	wantErr(t, err, ErrNotFound)

	// Names and IDs are unique; invalid input is refused.
	_, err = s.CreateService(ctx, NewService{ID: "other", Name: "web", Replicas: 1, Spec: webSpec(), At: testTime})
	wantErr(t, err, ErrConflict)
	_, err = s.CreateService(ctx, NewService{ID: svc.ID, Name: "web2", Replicas: 1, Spec: webSpec(), At: testTime})
	wantErr(t, err, ErrConflict)
	for name, n := range map[string]NewService{
		"no id":             {Name: "a", Spec: webSpec()},
		"bad name":          {ID: "x", Name: "a b", Spec: webSpec()},
		"negative replicas": {ID: "x", Name: "a", Replicas: -1, Spec: webSpec()},
		"no image":          {ID: "x", Name: "a", Spec: ServiceSpec{CPUMillis: 1, MemoryBytes: 1}},
		"no cpu":            {ID: "x", Name: "a", Spec: ServiceSpec{Image: "i", MemoryBytes: 1}},
	} {
		if _, err := s.CreateService(ctx, n); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: error = %v, want ErrInvalid", name, err)
		}
	}
	// A refused creation leaves nothing behind, not even the service row without its revision.
	if list, _ := s.ListServices(ctx, true); len(list) != 1 {
		t.Fatalf("services after refused creations = %+v", list)
	}

	// Scaling changes the desired replicas and the version.
	at := testTime.Add(time.Minute)
	scaled, err := s.ScaleService(ctx, svc.ID, 5, 1, at)
	noErr(t, err)
	if scaled.Replicas != 5 || scaled.Version != 2 || !scaled.UpdatedAt.Equal(at) || scaled.CurrentRevision != 1 || !scaled.CreatedAt.Equal(testTime) {
		t.Fatalf("scaled = %+v", scaled)
	}
	same, err := s.ScaleService(ctx, svc.ID, 5, 0, at.Add(time.Minute))
	if !errors.Is(err, ErrUnchanged) || same.Version != 2 || !same.UpdatedAt.Equal(at) {
		t.Fatalf("scaling to the current count: %+v, %v; want ErrUnchanged and no new version", same, err)
	}
	_, err = s.ScaleService(ctx, svc.ID, 7, 1, at) // read at version 1, but it is 2 now
	wantErr(t, err, ErrVersionConflict)
	if cur, _ := s.GetService(ctx, svc.ID); cur.Replicas != 5 {
		t.Fatalf("a conflicting update was applied: %+v", cur)
	}
	zero, err := s.ScaleService(ctx, svc.ID, 0, 2, at)
	if err != nil || zero.Replicas != 0 || zero.Version != 3 {
		t.Fatalf("scale to zero = %+v, %v", zero, err)
	}
	_, err = s.ScaleService(ctx, svc.ID, -1, 0, at)
	wantErr(t, err, ErrInvalid)
	_, err = s.ScaleService(ctx, "nope", 1, 0, at)
	wantErr(t, err, ErrNotFound)

	// Listing is ordered by name.
	newService(t, s, "api", 1)
	list, err := s.ListServices(ctx, false)
	if err != nil || len(list) != 2 || list[0].Name != "api" || list[1].Name != "web" {
		t.Fatalf("ListServices = %+v, %v", list, err)
	}
}

func testRevisions(t *testing.T, s Store) {
	ctx := context.Background()
	svc := newService(t, s, "web", 2)

	v2 := webSpec()
	v2.Image = "nginx:1.28"
	at := testTime.Add(time.Hour)
	deployed, rev2, err := s.DeployService(ctx, svc.ID, Deployment{Spec: v2, Message: "upgrade", CreatedBy: "alice", ExpectedVersion: 1, At: at})
	noErr(t, err)
	if deployed.CurrentRevision != 2 || deployed.Version != 2 || !deployed.UpdatedAt.Equal(at) || deployed.Replicas != 2 {
		t.Fatalf("deployed = %+v", deployed)
	}
	if rev2.Revision != 2 || rev2.Spec.Image != "nginx:1.28" || rev2.SpecHash != v2.Hash() || rev2.Message != "upgrade" || rev2.CreatedBy != "alice" || !rev2.CreatedAt.Equal(at) {
		t.Fatalf("revision 2 = %+v", rev2)
	}

	// Revisions are immutable snapshots: revision 1 still has its own spec.
	rev1, err := s.GetRevision(ctx, svc.ID, 1)
	noErr(t, err)
	if rev1.Spec.Image != "nginx:1.27" || rev1.Message != "first deploy" {
		t.Fatalf("revision 1 changed: %+v", rev1)
	}

	// Deploying the spec that is already current records nothing.
	again, same, err := s.DeployService(ctx, svc.ID, Deployment{Spec: v2, At: at.Add(time.Minute)})
	if !errors.Is(err, ErrUnchanged) || again.Version != 2 || same.Revision != 2 {
		t.Fatalf("redeploying the current spec: %+v %+v %v; want ErrUnchanged", again, same, err)
	}
	if revs, _ := s.ListRevisions(ctx, svc.ID); len(revs) != 2 {
		t.Fatalf("revisions = %+v, want 2", revs)
	}

	// Optimistic locking: a writer holding an old version loses.
	v3 := webSpec()
	v3.Image = "nginx:1.29"
	_, _, err = s.DeployService(ctx, svc.ID, Deployment{Spec: v3, ExpectedVersion: 1, At: at})
	wantErr(t, err, ErrVersionConflict)
	if revs, _ := s.ListRevisions(ctx, svc.ID); len(revs) != 2 {
		t.Fatalf("a conflicting deployment left a revision behind: %+v", revs)
	}

	// Rolling back deploys an old spec as a new revision: numbers only grow.
	rolled, rev3, err := s.DeployService(ctx, svc.ID, Deployment{Spec: rev1.Spec, Message: "rollback to 1", ExpectedVersion: 2, At: at.Add(time.Hour)})
	noErr(t, err)
	if rolled.CurrentRevision != 3 || rolled.Version != 3 || rev3.Revision != 3 || rev3.SpecHash != rev1.SpecHash || !reflect.DeepEqual(rev3.Spec, rev1.Spec) {
		t.Fatalf("rollback = %+v / %+v", rolled, rev3)
	}
	revs, err := s.ListRevisions(ctx, svc.ID)
	if err != nil || len(revs) != 3 || revs[0].Revision != 1 || revs[1].Revision != 2 || revs[2].Revision != 3 {
		t.Fatalf("ListRevisions = %+v, %v", revs, err)
	}

	// Returned values do not alias stored ones.
	revs[0].Spec.Env["MODE"] = "mutated"
	revs[0].Spec.Command[0] = "mutated"
	if fresh, _ := s.GetRevision(ctx, svc.ID, 1); fresh.Spec.Env["MODE"] != "prod" || fresh.Spec.Command[0] != "nginx" {
		t.Fatalf("the store aliases a caller's maps: %+v", fresh.Spec)
	}

	_, err = s.GetRevision(ctx, svc.ID, 4)
	wantErr(t, err, ErrNotFound)
	_, err = s.GetRevision(ctx, svc.ID, 0)
	wantErr(t, err, ErrNotFound)
	_, err = s.GetRevision(ctx, "nope", 1)
	wantErr(t, err, ErrNotFound)
	none, err := s.ListRevisions(ctx, "nope")
	if err != nil || none == nil || len(none) != 0 {
		t.Fatalf("ListRevisions(unknown) = %+v, %v; want empty and non-nil", none, err)
	}
	_, _, err = s.DeployService(ctx, "nope", Deployment{Spec: v3, At: at})
	wantErr(t, err, ErrNotFound)
	_, _, err = s.DeployService(ctx, svc.ID, Deployment{Spec: ServiceSpec{Image: "x"}, At: at})
	wantErr(t, err, ErrInvalid)
}

// fullSpec uses every field of a spec, to prove each one is stored.
func fullSpec() ServiceSpec {
	spec := webSpec()
	spec.Ports = []ServicePort{{ContainerPort: 80, Protocol: "tcp", HostPort: 8080}, {ContainerPort: 53, Protocol: "udp"}}
	spec.HealthCheck = &HealthCheck{Command: []string{"curl", "-f", "http://localhost/"}, IntervalSeconds: 30, TimeoutSeconds: 5, StartPeriodSeconds: 10, Retries: 3}
	spec.RestartPolicy = &RestartPolicy{Condition: RestartOnFailure, MaxRetries: 5}
	return spec
}

func testDefinitions(t *testing.T, s Store) {
	ctx := context.Background()
	none, err := s.ListDefinitions(ctx, true)
	if err != nil || none == nil || len(none) != 0 {
		t.Fatalf("ListDefinitions on an empty store = %+v, %v; want empty and non-nil", none, err)
	}

	// A spec with every field survives storage unchanged.
	full, err := s.CreateService(ctx, NewService{ID: "svc-full", Name: "full", Replicas: 2, Spec: fullSpec(), CreatedBy: "admin", At: testTime})
	noErr(t, err)
	rev, err := s.GetRevision(ctx, full.ID, 1)
	noErr(t, err)
	if !reflect.DeepEqual(rev.Spec, fullSpec()) || rev.SpecHash != fullSpec().Hash() {
		t.Fatalf("stored spec = %+v\nwant %+v", rev.Spec, fullSpec())
	}

	// Deploying a change to only the new fields is a new revision.
	changed := fullSpec()
	changed.RestartPolicy = &RestartPolicy{Condition: RestartAlways}
	changed.Ports = changed.Ports[:1]
	changed.HealthCheck.Retries = 5
	_, rev2, err := s.DeployService(ctx, full.ID, Deployment{Spec: changed, At: testTime.Add(time.Minute)})
	noErr(t, err)
	if rev2.Revision != 2 || rev2.SpecHash == rev.SpecHash {
		t.Fatalf("revision 2 = %+v; a change to ports, health check or restart policy must make a new revision", rev2)
	}

	newService(t, s, "api", 1)
	gone := newService(t, s, "old", 1)
	_, err = s.DeleteService(ctx, gone.ID, 0, testTime.Add(time.Hour))
	noErr(t, err)

	live, err := s.ListDefinitions(ctx, false)
	noErr(t, err)
	if len(live) != 2 || live[0].Service.Name != "api" || live[1].Service.Name != "full" {
		t.Fatalf("live definitions = %+v", live)
	}
	// Each definition carries the revision the service currently runs, not the first.
	f := live[1]
	if f.Service.CurrentRevision != 2 || f.Revision.Revision != 2 || f.Revision.ServiceID != full.ID ||
		!reflect.DeepEqual(f.Revision.Spec, changed) || f.Revision.SpecHash != changed.Hash() {
		t.Fatalf("definition of full = %+v", f)
	}
	all, err := s.ListDefinitions(ctx, true)
	noErr(t, err)
	if len(all) != 3 || all[1].Service.Name != "full" || all[2].Service.Name != "old" || all[2].Service.DeletedAt == nil {
		t.Fatalf("all definitions = %+v", all)
	}

	// What was returned is a copy.
	live[1].Revision.Spec.Ports[0].ContainerPort = 1
	live[1].Revision.Spec.HealthCheck.Command[0] = "mutated"
	live[1].Revision.Spec.RestartPolicy.Condition = "mutated"
	again, _ := s.GetRevision(ctx, full.ID, 2)
	if !reflect.DeepEqual(again.Spec, changed) {
		t.Fatalf("the store aliases returned specs: %+v", again.Spec)
	}
}

func testServiceWorkloads(t *testing.T, s Store) {
	ctx := context.Background()
	addNode(t, s, "node-a")
	svc := newService(t, s, "web", 2)
	v2 := webSpec()
	v2.Image = "nginx:1.28"
	_, _, err := s.DeployService(ctx, svc.ID, Deployment{Spec: v2, At: testTime})
	noErr(t, err)

	replica := func(id string, revision, index int) Workload {
		w := standalone(id, "web-r"+string(rune('0'+revision))+"-"+string(rune('0'+index)), "node-a")
		w.ServiceID, w.ServiceRevision, w.ReplicaIndex = svc.ID, revision, index
		return w
	}
	for _, w := range []Workload{replica("b", 2, 1), replica("a", 1, 2), replica("c", 1, 1)} {
		created, err := s.CreateWorkload(ctx, w)
		noErr(t, err)
		if created.ServiceID != svc.ID || created.ServiceRevision != w.ServiceRevision || created.ReplicaIndex != w.ReplicaIndex {
			t.Fatalf("created = %+v", created)
		}
	}

	// A replica slot is taken once per revision.
	dup := replica("d", 1, 1)
	dup.Name = "different-name"
	_, err = s.CreateWorkload(ctx, dup)
	wantErr(t, err, ErrConflict)

	// The revision and service must exist.
	_, err = s.CreateWorkload(ctx, replica("e", 3, 1))
	wantErr(t, err, ErrNotFound)
	ghost := replica("f", 1, 3)
	ghost.ServiceID = "no-such-service"
	_, err = s.CreateWorkload(ctx, ghost)
	wantErr(t, err, ErrNotFound)

	got, err := s.ListWorkloadsForService(ctx, svc.ID)
	if err != nil || len(got) != 3 {
		t.Fatalf("ListWorkloadsForService = %+v, %v", got, err)
	}
	order := []string{got[0].ID, got[1].ID, got[2].ID}
	if !reflect.DeepEqual(order, []string{"c", "a", "b"}) {
		t.Fatalf("replicas must be ordered by revision then index, got %v", order)
	}
	// Standalone workloads are not replicas of anything.
	_, err = s.CreateWorkload(ctx, standalone("solo", "solo", "node-a"))
	noErr(t, err)
	if again, _ := s.ListWorkloadsForService(ctx, svc.ID); len(again) != 3 {
		t.Fatalf("a standalone workload showed up as a replica: %+v", again)
	}
	empty, err := s.ListWorkloadsForService(ctx, "nope")
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("ListWorkloadsForService(unknown) = %+v, %v; want empty and non-nil", empty, err)
	}
}

func testServiceDeletion(t *testing.T, s Store) {
	ctx := context.Background()
	addNode(t, s, "node-a")
	svc := newService(t, s, "web", 2)
	replica := standalone("w1", "web-r1-1", "node-a")
	replica.ServiceID, replica.ServiceRevision, replica.ReplicaIndex = svc.ID, 1, 1
	_, err := s.CreateWorkload(ctx, replica)
	noErr(t, err)

	at := testTime.Add(time.Hour)
	_, err = s.DeleteService(ctx, svc.ID, 99, at)
	wantErr(t, err, ErrVersionConflict)
	deleted, err := s.DeleteService(ctx, svc.ID, 1, at)
	if err != nil || deleted.DeletedAt == nil || !deleted.DeletedAt.Equal(at) || deleted.Replicas != 0 || deleted.Version != 2 {
		t.Fatalf("DeleteService = %+v, %v", deleted, err)
	}

	// A deleted service is gone by name and from listings, but its history stays.
	_, err = s.GetServiceByName(ctx, "web")
	wantErr(t, err, ErrNotFound)
	if live, _ := s.ListServices(ctx, false); len(live) != 0 {
		t.Fatalf("live services = %+v", live)
	}
	if all, _ := s.ListServices(ctx, true); len(all) != 1 || all[0].DeletedAt == nil {
		t.Fatalf("all services = %+v", all)
	}
	byID, err := s.GetService(ctx, svc.ID)
	if err != nil || byID.DeletedAt == nil {
		t.Fatalf("GetService of a deleted service = %+v, %v", byID, err)
	}
	if _, err := s.GetRevision(ctx, svc.ID, 1); err != nil {
		t.Fatalf("revisions must be kept: %v", err)
	}
	if ws, _ := s.ListWorkloadsForService(ctx, svc.ID); len(ws) != 1 {
		t.Fatalf("workloads must be kept for the reconciler: %+v", ws)
	}

	// Nothing can change a deleted service.
	_, err = s.ScaleService(ctx, svc.ID, 3, 0, at)
	wantErr(t, err, ErrNotFound)
	_, _, err = s.DeployService(ctx, svc.ID, Deployment{Spec: ServiceSpec{Image: "x", CPUMillis: 1, MemoryBytes: 1}, At: at})
	wantErr(t, err, ErrNotFound)
	_, err = s.DeleteService(ctx, svc.ID, 0, at)
	wantErr(t, err, ErrNotFound)

	// The name can be reused by a new service with a new identity.
	reused, err := s.CreateService(ctx, NewService{ID: "svc-web-2", Name: "web", Replicas: 1, Spec: webSpec(), At: at})
	if err != nil || reused.ID != "svc-web-2" || reused.CurrentRevision != 1 {
		t.Fatalf("reusing the name: %+v, %v", reused, err)
	}
	byName, err := s.GetServiceByName(ctx, "web")
	if err != nil || byName.ID != "svc-web-2" {
		t.Fatalf("GetServiceByName = %+v, %v", byName, err)
	}
}

func testEvents(t *testing.T, s Store) {
	ctx := context.Background()
	add := func(action, resourceType, resourceID string) Event {
		e, err := s.AppendEvent(ctx, Event{
			At: testTime, InstanceID: "manager-1", Actor: "admin", Action: action,
			ResourceType: resourceType, ResourceID: resourceID, Message: "msg",
			Details: map[string]string{"k": "v"},
		})
		noErr(t, err)
		return e
	}
	e1 := add("service.created", "service", "svc-1")
	e2 := add("service.scaled", "service", "svc-1")
	e3 := add("enrollment.created", "enrollment", "worker-1")
	e4 := add("service.created", "service", "svc-2")
	if !(e1.ID > 0 && e2.ID > e1.ID && e3.ID > e2.ID && e4.ID > e3.ID) {
		t.Fatalf("event IDs must increase: %d %d %d %d", e1.ID, e2.ID, e3.ID, e4.ID)
	}
	if e1.Details["k"] != "v" || e1.InstanceID != "manager-1" || !e1.At.Equal(testTime) {
		t.Fatalf("appended event = %+v", e1)
	}

	ids := func(f EventFilter) []int64 {
		events, err := s.ListEvents(ctx, f)
		noErr(t, err)
		out := []int64{}
		for _, e := range events {
			out = append(out, e.ID)
		}
		return out
	}
	if got := ids(EventFilter{}); !reflect.DeepEqual(got, []int64{e4.ID, e3.ID, e2.ID, e1.ID}) {
		t.Fatalf("all events newest first = %v", got)
	}
	if got := ids(EventFilter{ResourceType: "service"}); !reflect.DeepEqual(got, []int64{e4.ID, e2.ID, e1.ID}) {
		t.Fatalf("service events = %v", got)
	}
	if got := ids(EventFilter{ResourceType: "service", ResourceID: "svc-1"}); !reflect.DeepEqual(got, []int64{e2.ID, e1.ID}) {
		t.Fatalf("svc-1 events = %v", got)
	}
	if got := ids(EventFilter{ResourceID: "worker-1"}); !reflect.DeepEqual(got, []int64{e3.ID}) {
		t.Fatalf("worker-1 events = %v", got)
	}
	// Paging: the last ID of a page is the cursor for the next, older one.
	page1 := ids(EventFilter{Limit: 2})
	page2 := ids(EventFilter{Limit: 2, BeforeID: page1[len(page1)-1]})
	if !reflect.DeepEqual(page1, []int64{e4.ID, e3.ID}) || !reflect.DeepEqual(page2, []int64{e2.ID, e1.ID}) {
		t.Fatalf("pages = %v, %v", page1, page2)
	}
	if got := ids(EventFilter{BeforeID: e1.ID}); len(got) != 0 {
		t.Fatalf("events before the first = %v", got)
	}
	if got := ids(EventFilter{Limit: 1_000_000}); len(got) != 4 {
		t.Fatalf("a huge limit is capped, not an error: %v", got)
	}

	// Details are always a map, and the audit trail refuses anonymous entries.
	bare, err := s.AppendEvent(ctx, Event{At: testTime, Actor: "system", Action: "node.registered", ResourceType: "node", ResourceID: "n"})
	noErr(t, err)
	listed, _ := s.ListEvents(ctx, EventFilter{ResourceType: "node"})
	if len(listed) != 1 || listed[0].ID != bare.ID || listed[0].Details == nil || len(listed[0].Details) != 0 {
		t.Fatalf("bare event = %+v", listed)
	}
	for name, e := range map[string]Event{
		"no actor":    {Action: "a", ResourceType: "r"},
		"no action":   {Actor: "a", ResourceType: "r"},
		"no resource": {Actor: "a", Action: "a"},
	} {
		e.At = testTime
		if _, err := s.AppendEvent(ctx, e); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: error = %v, want ErrInvalid", name, err)
		}
	}
}

func testLeases(t *testing.T, s Store) {
	ctx := context.Background()
	_, err := s.GetLease(ctx, "reconciler")
	wantErr(t, err, ErrNotFound)

	first, err := s.AcquireLease(ctx, "reconciler", "manager-1", 10*time.Second, testTime)
	if err != nil || first.Holder != "manager-1" || first.Token != 1 || first.Name != "reconciler" || !first.ExpiresAt.Equal(testTime.Add(10*time.Second)) {
		t.Fatalf("first acquisition = %+v, %v", first, err)
	}

	// The holder renews and keeps its token; others are turned away while it is live.
	renewed, err := s.AcquireLease(ctx, "reconciler", "manager-1", 10*time.Second, testTime.Add(5*time.Second))
	if err != nil || renewed.Token != 1 || !renewed.ExpiresAt.Equal(testTime.Add(15*time.Second)) {
		t.Fatalf("renewal = %+v, %v", renewed, err)
	}
	_, err = s.AcquireLease(ctx, "reconciler", "manager-2", 10*time.Second, testTime.Add(14*time.Second))
	wantErr(t, err, ErrLeaseHeld)

	// Once it has expired another manager takes over, and the token tells them apart.
	taken, err := s.AcquireLease(ctx, "reconciler", "manager-2", 10*time.Second, testTime.Add(15*time.Second))
	if err != nil || taken.Holder != "manager-2" || taken.Token != 2 {
		t.Fatalf("takeover = %+v, %v", taken, err)
	}
	_, err = s.AcquireLease(ctx, "reconciler", "manager-1", 10*time.Second, testTime.Add(16*time.Second))
	wantErr(t, err, ErrLeaseHeld)
	if cur, err := s.GetLease(ctx, "reconciler"); err != nil || cur.Holder != "manager-2" || cur.Token != 2 {
		t.Fatalf("GetLease = %+v, %v", cur, err)
	}

	// Releasing frees it at once, for others only; the next holder gets a new token.
	wantErr(t, s.ReleaseLease(ctx, "reconciler", "manager-1", testTime.Add(17*time.Second)), ErrNotFound)
	wantErr(t, s.ReleaseLease(ctx, "other", "manager-2", testTime), ErrNotFound)
	noErr(t, s.ReleaseLease(ctx, "reconciler", "manager-2", testTime.Add(17*time.Second)))
	again, err := s.AcquireLease(ctx, "reconciler", "manager-1", 10*time.Second, testTime.Add(17*time.Second))
	if err != nil || again.Holder != "manager-1" || again.Token != 3 {
		t.Fatalf("acquisition after release = %+v, %v", again, err)
	}

	// An expired lease taken back by its old holder is also a new term.
	back, err := s.AcquireLease(ctx, "reconciler", "manager-1", 10*time.Second, testTime.Add(time.Hour))
	if err != nil || back.Token != 4 {
		t.Fatalf("the holder returning after its lease expired = %+v, %v", back, err)
	}

	for name, args := range map[string][3]any{
		"bad name":   {"bad name", "m", time.Second},
		"bad holder": {"lease", "bad holder", time.Second},
		"no ttl":     {"lease", "m", time.Duration(0)},
	} {
		if _, err := s.AcquireLease(ctx, args[0].(string), args[1].(string), args[2].(time.Duration), testTime); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: error = %v, want ErrInvalid", name, err)
		}
	}
}

var errAbort = errors.New("abort the transaction")

func testTransactions(t *testing.T, s Store) {
	ctx := context.Background()
	err := s.InTx(ctx, func(tx Tx) error {
		if err := tx.LockPlacement(ctx); err != nil {
			return err
		}
		addNode(t, tx, "node-a")
		svc := newService(t, tx, "web", 1)
		if _, err := tx.AppendEvent(ctx, Event{At: testTime, Actor: "admin", Action: "service.created", ResourceType: "service", ResourceID: svc.ID}); err != nil {
			return err
		}
		// A transaction sees its own writes.
		if got, err := tx.GetServiceByName(ctx, "web"); err != nil || got.ID != svc.ID {
			t.Errorf("read your own write: %+v, %v", got, err)
		}
		return nil
	})
	noErr(t, err)

	// Committed changes are visible to everyone afterwards, together.
	if _, err := s.GetNode(ctx, "node-a"); err != nil {
		t.Errorf("committed node missing: %v", err)
	}
	if _, err := s.GetServiceByName(ctx, "web"); err != nil {
		t.Errorf("committed service missing: %v", err)
	}
	if events, _ := s.ListEvents(ctx, EventFilter{}); len(events) != 1 {
		t.Errorf("committed events = %+v", events)
	}
}

func testTransactionRollbacks(t *testing.T, s Store) {
	ctx := context.Background()
	addNode(t, s, "keep")

	// An error from the function undoes every write made in it, whichever repository it went to.
	err := s.InTx(ctx, func(tx Tx) error {
		addNode(t, tx, "gone")
		newService(t, tx, "ghost", 2)
		if _, err := tx.AppendEvent(ctx, Event{At: testTime, Actor: "a", Action: "x", ResourceType: "r", ResourceID: "1"}); err != nil {
			return err
		}
		if _, err := tx.CreateEnrollment(ctx, Enrollment{NodeID: "gone", CreatedAt: testTime}); err != nil {
			return err
		}
		return errAbort
	})
	wantErr(t, err, errAbort)
	if _, err := s.GetNode(ctx, "gone"); !errors.Is(err, ErrNotFound) {
		t.Errorf("rolled-back node survived: %v", err)
	}
	if _, err := s.GetServiceByName(ctx, "ghost"); !errors.Is(err, ErrNotFound) {
		t.Errorf("rolled-back service survived: %v", err)
	}
	if _, err := s.GetRevision(ctx, "svc-ghost", 1); !errors.Is(err, ErrNotFound) {
		t.Errorf("rolled-back revision survived: %v", err)
	}
	if _, err := s.GetEnrollment(ctx, "gone"); !errors.Is(err, ErrNotFound) {
		t.Errorf("rolled-back enrollment survived: %v", err)
	}
	if events, _ := s.ListEvents(ctx, EventFilter{}); len(events) != 0 {
		t.Errorf("rolled-back events survived: %+v", events)
	}
	if _, err := s.GetNode(ctx, "keep"); err != nil {
		t.Errorf("a rollback must not touch earlier committed data: %v", err)
	}

	// A failed statement inside the transaction (a duplicate) rolls back what came before it.
	err = s.InTx(ctx, func(tx Tx) error {
		newService(t, tx, "dup", 1)
		_, err := tx.CreateService(ctx, NewService{ID: "svc-dup-again", Name: "dup", Replicas: 1, Spec: webSpec(), At: testTime})
		return err
	})
	wantErr(t, err, ErrConflict)
	if _, err := s.GetServiceByName(ctx, "dup"); !errors.Is(err, ErrNotFound) {
		t.Errorf("the first half of a failed transaction survived: %v", err)
	}

	// A panic rolls back too and is not swallowed.
	func() {
		defer func() {
			if recover() == nil {
				t.Error("the panic was swallowed")
			}
		}()
		_ = s.InTx(ctx, func(tx Tx) error {
			addNode(t, tx, "panicked")
			panic("boom")
		})
	}()
	if _, err := s.GetNode(ctx, "panicked"); !errors.Is(err, ErrNotFound) {
		t.Errorf("a node written before a panic survived: %v", err)
	}

	// The store is usable after rollbacks.
	err = s.InTx(ctx, func(tx Tx) error {
		newService(t, tx, "after", 1)
		return nil
	})
	noErr(t, err)
	if _, err := s.GetServiceByName(ctx, "after"); err != nil {
		t.Errorf("transaction after rollbacks failed: %v", err)
	}
}
