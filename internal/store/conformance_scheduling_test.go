package store

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func testWorkloadPorts(t *testing.T, s Store) {
	ctx := context.Background()
	addNode(t, s, "node-a")

	ports := []ServicePort{{ContainerPort: 80, Protocol: "tcp", HostPort: 8080}, {ContainerPort: 53, Protocol: "udp"}}
	w := standalone("w1", "web", "node-a")
	w.Ports = ports
	created, err := s.CreateWorkload(ctx, w)
	noErr(t, err)
	if !reflect.DeepEqual(created.Ports, ports) {
		t.Fatalf("created ports = %+v", created.Ports)
	}

	// The caller's slice is not kept: changing it later does not change what is stored.
	ports[0].HostPort = 9999
	got, err := s.GetWorkload(ctx, "w1")
	noErr(t, err)
	want := []ServicePort{{ContainerPort: 80, Protocol: "tcp", HostPort: 8080}, {ContainerPort: 53, Protocol: "udp"}}
	if !reflect.DeepEqual(got.Ports, want) {
		t.Fatalf("stored ports = %+v, want %+v", got.Ports, want)
	}
	// And what is returned is a copy.
	got.Ports[1].ContainerPort = 1
	if again, _ := s.GetWorkload(ctx, "w1"); !reflect.DeepEqual(again.Ports, want) {
		t.Fatalf("the store aliases returned ports: %+v", again.Ports)
	}

	// Every read path carries them; a workload without ports has none.
	_, err = s.CreateWorkload(ctx, standalone("w2", "plain", "node-a"))
	noErr(t, err)
	all, err := s.ListWorkloads(ctx)
	noErr(t, err)
	byName := map[string][]ServicePort{}
	for _, w := range all {
		byName[w.Name] = w.Ports
	}
	if !reflect.DeepEqual(byName["web"], want) || byName["plain"] != nil {
		t.Fatalf("ports by workload = %+v", byName)
	}
	forNode, err := s.ListWorkloadsForNode(ctx, "node-a")
	noErr(t, err)
	if len(forNode) != 2 || !reflect.DeepEqual(forNode[1].Ports, want) {
		t.Fatalf("workloads for the node = %+v", forNode)
	}
}

func unschedulable(service string, revision, index int, message string) Unschedulable {
	return Unschedulable{
		ServiceID: service, Revision: revision, ReplicaIndex: index, Code: "insufficient_resources", Message: message,
		Nodes: []NodeReason{{NodeID: "node-a", Code: "insufficient_resources", Message: "needs more"}},
	}
}

func testUnschedulable(t *testing.T, s Store) {
	ctx := context.Background()
	empty, err := s.ListUnschedulable(ctx, "")
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("ListUnschedulable on an empty store = %+v, %v; want empty and non-nil", empty, err)
	}
	web := newService(t, s, "web", 3)
	api := newService(t, s, "api", 1)
	v2 := webSpec()
	v2.Image = "nginx:1.28"
	_, _, err = s.DeployService(ctx, web.ID, Deployment{Spec: v2, At: testTime})
	noErr(t, err)

	t1, t2, t3, t4 := testTime.Add(time.Minute), testTime.Add(2*time.Minute), testTime.Add(3*time.Minute), testTime.Add(4*time.Minute)

	// A first pass records everything it found.
	items := []Unschedulable{unschedulable(web.ID, 2, 2, "no room"), unschedulable(web.ID, 2, 3, "no room"), unschedulable(api.ID, 1, 1, "no room")}
	changed, err := s.ReplaceUnschedulable(ctx, items, t1)
	noErr(t, err)
	if len(changed) != 3 || changed[0].ServiceID != api.ID || changed[1].ReplicaIndex != 2 || changed[2].ReplicaIndex != 3 {
		t.Fatalf("changed = %+v, want the three new records ordered by service, revision and index", changed)
	}
	for _, c := range changed {
		if !c.Since.Equal(t1) {
			t.Errorf("Since = %v, want %v", c.Since, t1)
		}
	}
	all, err := s.ListUnschedulable(ctx, "")
	noErr(t, err)
	if len(all) != 3 || all[0].ServiceID != api.ID || all[1].ServiceID != web.ID || all[2].ReplicaIndex != 3 {
		t.Fatalf("all = %+v", all)
	}
	if !reflect.DeepEqual(all[1].Nodes, []NodeReason{{NodeID: "node-a", Code: "insufficient_resources", Message: "needs more"}}) {
		t.Fatalf("node reasons = %+v", all[1].Nodes)
	}
	forWeb, err := s.ListUnschedulable(ctx, web.ID)
	noErr(t, err)
	if len(forWeb) != 2 {
		t.Fatalf("records of web = %+v", forWeb)
	}
	none, err := s.ListUnschedulable(ctx, "nobody")
	if err != nil || none == nil || len(none) != 0 {
		t.Fatalf("records of an unknown service = %+v, %v; want empty and non-nil", none, err)
	}

	// The same findings again change nothing, so nothing is reported and Since stays.
	changed, err = s.ReplaceUnschedulable(ctx, items, t2)
	noErr(t, err)
	if len(changed) != 0 {
		t.Fatalf("an identical pass reported %+v", changed)
	}
	if again, _ := s.ListUnschedulable(ctx, ""); !again[0].Since.Equal(t1) || !again[2].Since.Equal(t1) {
		t.Fatalf("an identical pass moved Since: %+v", again)
	}

	// A changed explanation is rewritten with a new Since and reported; an
	// unchanged one keeps its Since; a record that is gone is removed.
	items = []Unschedulable{unschedulable(web.ID, 2, 2, "still no room"), unschedulable(api.ID, 1, 1, "no room")}
	changed, err = s.ReplaceUnschedulable(ctx, items, t3)
	noErr(t, err)
	if len(changed) != 1 || changed[0].ServiceID != web.ID || changed[0].ReplicaIndex != 2 || !changed[0].Since.Equal(t3) {
		t.Fatalf("changed = %+v, want only web replica 2", changed)
	}
	all, _ = s.ListUnschedulable(ctx, "")
	if len(all) != 2 || all[0].ServiceID != api.ID || !all[0].Since.Equal(t1) || all[1].Message != "still no room" || !all[1].Since.Equal(t3) {
		t.Fatalf("after a change = %+v", all)
	}

	// Returned records are copies.
	all[0].Nodes[0].Message = "mutated"
	if fresh, _ := s.ListUnschedulable(ctx, ""); fresh[0].Nodes[0].Message != "needs more" {
		t.Fatalf("the store aliases returned records: %+v", fresh[0])
	}

	// No node reasons at all is one state, whether it is nil or empty.
	bare := unschedulable(api.ID, 1, 1, "no room")
	bare.Nodes = nil
	changed, err = s.ReplaceUnschedulable(ctx, []Unschedulable{bare}, t4)
	noErr(t, err)
	if len(changed) != 1 || changed[0].ServiceID != api.ID {
		t.Fatalf("dropping the node reasons: changed = %+v", changed)
	}
	for _, nodes := range [][]NodeReason{nil, {}} {
		again := bare
		again.Nodes = nodes
		if changed, err = s.ReplaceUnschedulable(ctx, []Unschedulable{again}, t4.Add(time.Minute)); err != nil || len(changed) != 0 {
			t.Fatalf("nodes %#v: changed = %+v, %v; want no change", nodes, changed, err)
		}
	}

	// Bad input is refused and leaves the stored set as it was.
	good := []Unschedulable{bare}
	invalid := []struct {
		name string
		bad  Unschedulable
	}{
		{"no service", unschedulable("", 1, 1, "x")},
		{"revision zero", unschedulable(web.ID, 0, 1, "x")},
		{"replica zero", unschedulable(web.ID, 1, 0, "x")},
		{"no code", Unschedulable{ServiceID: web.ID, Revision: 1, ReplicaIndex: 1}},
		{"the same slot as another item", unschedulable(api.ID, 1, 1, "again")},
	}
	for _, tt := range invalid {
		if _, err := s.ReplaceUnschedulable(ctx, append(append([]Unschedulable{}, good...), tt.bad), t4); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: error = %v, want ErrInvalid", tt.name, err)
		}
	}
	missing := []struct {
		name string
		item Unschedulable
	}{
		{"unknown revision", unschedulable(web.ID, 9, 1, "x")},
		{"unknown service", unschedulable("svc-nope", 1, 1, "x")},
	}
	for _, tt := range missing {
		if _, err := s.ReplaceUnschedulable(ctx, []Unschedulable{tt.item}, t4); !errors.Is(err, ErrNotFound) {
			t.Errorf("%s: error = %v, want ErrNotFound", tt.name, err)
		}
	}
	if kept, _ := s.ListUnschedulable(ctx, ""); len(kept) != 1 || kept[0].ServiceID != api.ID || !kept[0].Since.Equal(t4) {
		t.Fatalf("a refused replacement changed the stored set: %+v", kept)
	}

	// Replacing with nothing clears the set.
	changed, err = s.ReplaceUnschedulable(ctx, nil, t4)
	if err != nil || len(changed) != 0 {
		t.Fatalf("clearing: %+v, %v", changed, err)
	}
	if left, _ := s.ListUnschedulable(ctx, ""); len(left) != 0 {
		t.Fatalf("records left after clearing: %+v", left)
	}
}
