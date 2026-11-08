package api

import (
	"context"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/placement"
	"github.com/dockfleet/dockfleet/internal/store"
)

// These tests run the scheduler against the store the API reads, then look at
// what the existing state APIs show. The API itself never schedules.

func (e *serviceEnv) addNode(id string, cpu, mem int64, labels ...string) {
	e.t.Helper()
	node := store.Node{ID: id, Name: id, CPUMillis: cpu, MemoryBytes: mem, RegisteredAt: e.clock, LastHeartbeat: e.clock}
	if len(labels) > 0 {
		node.Labels = map[string]string{}
		for _, l := range labels {
			k, v, _ := strings.Cut(l, "=")
			node.Labels[k] = v
		}
	}
	if _, err := e.st.UpsertNode(context.Background(), node); err != nil {
		e.t.Fatal(err)
	}
}

// schedule makes one scheduling pass at the env's clock.
func (e *serviceEnv) schedule() placement.Report {
	e.t.Helper()
	report, err := placement.New(placement.Config{
		Store: e.st, NodeTTL: 30 * time.Second, InstanceID: "test-manager", Now: func() time.Time { return e.clock },
	}).RunOnce(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	return report
}

// webService is a service definition of n replicas, each limited to cpu millicores and 128 MiB.
func webService(n, cpu int) string {
	return `{"name":"web","replicas":` + strconv.Itoa(n) + `,"spec":{"image":"nginx:1.27","cpu_millis":` + strconv.Itoa(cpu) + `,"memory_bytes":134217728}}`
}

func webUpdate(image string, n, cpu int) string {
	return `{"replicas":` + strconv.Itoa(n) + `,"spec":{"image":"` + image + `","cpu_millis":` + strconv.Itoa(cpu) + `,"memory_bytes":134217728}}`
}

func testServiceStatusShowsAssignments(t *testing.T, e *serviceEnv) {
	e.addNode("n1", 4000, 8<<30)
	e.addNode("n2", 4000, 8<<30)
	e.create(webService(3, 500))

	// Defining a service decides nothing.
	before := e.get("web").Status
	if before.DesiredReplicas != 3 || before.ScheduledReplicas != 0 || before.UnschedulableReplicas != 0 || len(before.Replicas) != 0 || len(before.Unschedulable) != 0 {
		t.Fatalf("status before scheduling = %+v", before)
	}

	if report := e.schedule(); report.Placed != 3 || report.Unschedulable != 0 {
		t.Fatalf("report = %+v", report)
	}
	svc := e.get("web")
	st := svc.Status
	if st.DesiredReplicas != 3 || st.ScheduledReplicas != 3 || st.UnschedulableReplicas != 0 || len(st.Replicas) != 3 || len(st.Unschedulable) != 0 {
		t.Fatalf("status = %+v", st)
	}
	var nodes []string
	for i, r := range st.Replicas {
		nodes = append(nodes, r.NodeID)
		if r.Index != i+1 || r.Revision != 1 || r.DesiredState != store.DesiredRunning || r.Phase != store.WorkloadScheduled ||
			r.WorkloadID == "" || !strings.HasPrefix(r.WorkloadName, "web-r1-"+strconv.Itoa(i+1)+"-") {
			t.Errorf("replica %d = %+v", i+1, r)
		}
	}
	// Replicas of one service are spread over the nodes before any node gets a second one.
	if !reflect.DeepEqual(nodes, []string{"n1", "n2", "n1"}) {
		t.Fatalf("replicas are on %v, want n1, n2, n1", nodes)
	}
	if svc.Version != 1 || svc.Revision != 1 {
		t.Fatalf("scheduling changed the definition: %+v", svc)
	}

	// The list shows the same status.
	var list struct {
		Services []serviceView `json:"services"`
	}
	decode(t, e.request(http.MethodGet, "/v1/services", ""), &list)
	if len(list.Services) != 1 || !reflect.DeepEqual(list.Services[0].Status, st) {
		t.Fatalf("list status = %+v, want %+v", list.Services, st)
	}

	// The assignments are ordinary workloads, so the workload API shows them.
	var workloads struct {
		Workloads []store.Workload `json:"workloads"`
	}
	decode(t, e.request(http.MethodGet, "/v1/workloads", ""), &workloads)
	if len(workloads.Workloads) != 3 {
		t.Fatalf("workloads = %+v", workloads.Workloads)
	}
	for _, w := range workloads.Workloads {
		if w.ServiceID != svc.ID || w.ServiceRevision != 1 || w.CPUMillis != 500 || w.MemoryBytes != 134217728 || w.Image != "nginx:1.27" ||
			w.DesiredState != store.DesiredRunning || w.Observed != nil {
			t.Errorf("workload = %+v", w)
		}
	}

	// And the node view shows what they reserve.
	var n1 struct {
		Status      string `json:"status"`
		Allocatable struct {
			CPUMillis   int64 `json:"cpu_millis"`
			MemoryBytes int64 `json:"memory_bytes"`
		} `json:"allocatable"`
		Allocated struct {
			CPUMillis   int64 `json:"cpu_millis"`
			MemoryBytes int64 `json:"memory_bytes"`
			Workloads   int   `json:"workloads"`
		} `json:"allocated"`
	}
	decode(t, e.request(http.MethodGet, "/v1/nodes/n1", ""), &n1)
	if n1.Status != "ready" || n1.Allocatable.CPUMillis != 4000 || n1.Allocatable.MemoryBytes != 8<<30 ||
		n1.Allocated.CPUMillis != 1000 || n1.Allocated.MemoryBytes != 2*134217728 || n1.Allocated.Workloads != 2 {
		t.Fatalf("node n1 = %+v", n1)
	}

	// What an agent reports shows up beside the decision, not instead of it.
	if err := e.st.RecordObservations(context.Background(), "n1", []store.Observation{{WorkloadID: st.Replicas[0].WorkloadID, State: store.WorkloadRunning, ObservedAt: e.clock}}); err != nil {
		t.Fatal(err)
	}
	after := e.get("web").Status
	if after.Replicas[0].Phase != store.WorkloadRunning || after.Replicas[0].NodeID != "n1" || after.Replicas[1].Phase != store.WorkloadScheduled {
		t.Fatalf("status after a report = %+v", after.Replicas)
	}

	// Every assignment is in the audit trail.
	var events struct {
		Events []store.Event `json:"events"`
	}
	decode(t, e.request(http.MethodGet, "/v1/events?resource_type=workload", ""), &events)
	if len(events.Events) != 3 {
		t.Fatalf("events = %+v", events.Events)
	}
	for _, ev := range events.Events {
		if ev.Action != "replica.scheduled" || ev.Actor != "scheduler" || ev.Details["service"] != "web" || ev.Details["node_id"] == "" || ev.Details["service_id"] != svc.ID {
			t.Errorf("event = %+v", ev)
		}
	}

	// A second pass has nothing to do.
	if report := e.schedule(); report.Placed != 0 || report.Unschedulable != 0 {
		t.Fatalf("second pass = %+v", report)
	}
	if got := e.get("web").Status.Replicas; len(got) != 3 {
		t.Fatalf("replicas after a second pass = %+v", got)
	}
}

func testServiceStatusShowsAcknowledgments(t *testing.T, e *serviceEnv) {
	e.addNode("n1", 4000, 8<<30)
	e.create(webService(2, 500))
	e.schedule()

	st := e.get("web").Status
	if st.ScheduledReplicas != 2 || st.RunningReplicas != 0 || len(st.Replicas) != 2 || st.Replicas[0].Generation != 1 || st.Replicas[0].Acknowledged {
		t.Fatalf("status before any report = %+v", st)
	}

	// The agent receives the assignment and starts the container.
	obs := store.Observation{
		WorkloadID: st.Replicas[0].WorkloadID, State: store.WorkloadRunning, Message: "running",
		ContainerID: "c1", AckedGeneration: 1, ObservedAt: e.clock,
	}
	if err := e.st.RecordObservations(context.Background(), "n1", []store.Observation{obs}); err != nil {
		t.Fatal(err)
	}
	st = e.get("web").Status
	first, second := st.Replicas[0], st.Replicas[1]
	if st.RunningReplicas != 1 || !first.Acknowledged || first.ContainerID != "c1" || first.Message != "running" || first.Phase != store.WorkloadRunning ||
		second.Acknowledged || second.Phase != store.WorkloadScheduled || second.ContainerID != "" {
		t.Fatalf("status after one agent report = %+v", st)
	}
}

func testServiceStatusExplainsWhyNot(t *testing.T, e *serviceEnv) {
	e.addNode("n1", 1000, 4<<30)
	e.create(webService(3, 600))
	if report := e.schedule(); report.Placed != 1 || report.Unschedulable != 2 || report.NewReasons != 2 {
		t.Fatalf("report = %+v", report)
	}

	st := e.get("web").Status
	if st.DesiredReplicas != 3 || st.ScheduledReplicas != 1 || st.UnschedulableReplicas != 2 || len(st.Replicas) != 1 || len(st.Unschedulable) != 2 {
		t.Fatalf("status = %+v", st)
	}
	for i, u := range st.Unschedulable {
		index := i + 2
		if u.ReplicaIndex != index || u.Revision != 1 || u.Code != "insufficient_resources" || !u.Since.Equal(fixedNow) ||
			u.Message != "0/1 nodes can run web#"+strconv.Itoa(index)+": 1 node(s) lack CPU (needs 600m)" {
			t.Errorf("unschedulable %d = %+v", index, u)
		}
		if len(u.Nodes) != 1 || u.Nodes[0].NodeID != "n1" || u.Nodes[0].Code != "insufficient_resources" ||
			u.Nodes[0].Message != "insufficient resources (CPU: needs 600m, 400m free)" {
			t.Errorf("node reasons of replica %d = %+v", index, u.Nodes)
		}
	}

	// Time passes, nothing changes: the explanation keeps its start and is not announced again.
	e.clock = e.clock.Add(10 * time.Second)
	e.addNode("n1", 1000, 4<<30) // a heartbeat
	if report := e.schedule(); report.Placed != 0 || report.Unschedulable != 2 || report.NewReasons != 0 {
		t.Fatalf("second pass = %+v", report)
	}
	if got := e.get("web").Status.Unschedulable; !got[0].Since.Equal(fixedNow) {
		t.Fatalf("Since moved to %v", got[0].Since)
	}
	e.wantEvents("service.created", "replica.scheduled", "replica.unschedulable", "replica.unschedulable")

	// A node with room joins; the waiting replicas are placed and the explanations go away.
	e.addNode("n2", 2000, 4<<30)
	if report := e.schedule(); report.Placed != 2 || report.Unschedulable != 0 {
		t.Fatalf("pass after a node joined = %+v", report)
	}
	st = e.get("web").Status
	if st.ScheduledReplicas != 3 || st.UnschedulableReplicas != 0 || len(st.Unschedulable) != 0 || len(st.Replicas) != 3 {
		t.Fatalf("status = %+v", st)
	}
	e.wantEvents("service.created", "replica.scheduled", "replica.unschedulable", "replica.unschedulable", "replica.scheduled", "replica.scheduled")
}

// Four nodes, each ruled out for a different reason, and the API names all four.
func testServiceStatusNamesEveryReason(t *testing.T, e *serviceEnv) {
	e.addNode("a-stale", 4000, 8<<30, "zone=y")
	e.clock = e.clock.Add(time.Hour) // a-stale has not been heard from since
	e.addNode("b-zone-x", 4000, 8<<30, "zone=x")
	e.addNode("c-port-taken", 4000, 8<<30, "zone=y")
	e.addNode("d-full", 4000, 8<<30, "zone=y")

	definition := func(name string, cpu int, extra string) string {
		return `{"name":"` + name + `","replicas":1,"spec":{"image":"x","cpu_millis":` + strconv.Itoa(cpu) +
			`,"memory_bytes":134217728,"node_selector":{"zone":"y"}` + extra + `}}`
	}
	const port = `,"ports":[{"container_port":5432,"host_port":15432}]`
	// Services are placed in name order. a-holder takes the port on c-port-taken; b-hog,
	// which has to go elsewhere to fit, fills d-full; z-proxy then finds no node.
	e.create(definition("a-holder", 100, port))
	e.create(definition("b-hog", 3500, ""))
	e.create(definition("z-proxy", 1000, port))
	if report := e.schedule(); report.Placed != 2 || report.Unschedulable != 1 {
		t.Fatalf("report = %+v", report)
	}
	if got := e.get("a-holder").Status.Replicas[0].NodeID; got != "c-port-taken" {
		t.Fatalf("a-holder is on %s", got)
	}
	if got := e.get("b-hog").Status.Replicas[0].NodeID; got != "d-full" {
		t.Fatalf("b-hog is on %s", got)
	}

	proxy := e.get("z-proxy")
	if proxy.Status.ScheduledReplicas != 0 || proxy.Status.UnschedulableReplicas != 1 || len(proxy.Status.Replicas) != 0 {
		t.Fatalf("status = %+v", proxy.Status)
	}
	u := proxy.Status.Unschedulable[0]
	const wantMessage = "0/4 nodes can run z-proxy#1: 1 node(s) not available; 1 node(s) do not match the node selector {zone=y}; " +
		"1 node(s) already use host port 15432/tcp; 1 node(s) lack CPU (needs 1000m)"
	if u.Code != "insufficient_resources" || u.Message != wantMessage {
		t.Fatalf("code %q, message %q\nwant message %q", u.Code, u.Message, wantMessage)
	}
	if len(u.Nodes) != 4 {
		t.Fatalf("node reasons = %+v", u.Nodes)
	}
	want := []struct{ node, code, text string }{
		{"a-stale", "unavailable", "no heartbeat within the last 30s"},
		{"b-zone-x", "selector_mismatch", `label zone is "x" (want "y")`},
		{"c-port-taken", "host_port_in_use", "host port 15432/tcp is already used by a-holder-r1-1-"},
		{"d-full", "insufficient_resources", "CPU: needs 1000m, 500m free"},
	}
	for i, w := range want {
		if u.Nodes[i].NodeID != w.node || u.Nodes[i].Code != w.code || !strings.Contains(u.Nodes[i].Message, w.text) {
			t.Errorf("node reason %d = %+v, want node %s, code %s, containing %q", i, u.Nodes[i], w.node, w.code, w.text)
		}
	}
}

func testServiceStatusAcrossRevisionsScalingAndDeletion(t *testing.T, e *serviceEnv) {
	e.addNode("n1", 4000, 8<<30)
	e.create(webService(2, 500))
	e.schedule()

	// A new revision is a new set of replicas; the old ones are not removed by scheduling.
	put(e, "web", webUpdate("nginx:1.28", 2, 500))
	if report := e.schedule(); report.Placed != 2 {
		t.Fatalf("report = %+v", report)
	}
	st := e.get("web").Status
	var seen [][2]int
	for _, r := range st.Replicas {
		seen = append(seen, [2]int{r.Revision, r.Index})
	}
	if st.ScheduledReplicas != 2 || !reflect.DeepEqual(seen, [][2]int{{1, 1}, {1, 2}, {2, 1}, {2, 2}}) {
		t.Fatalf("status = %+v, replicas %v; the old revision's replicas are listed and only the current revision counts", st, seen)
	}
	if v := e.get("web"); v.Status.Replicas[2].WorkloadName == v.Status.Replicas[0].WorkloadName {
		t.Fatal("workload names must be unique")
	}

	// Fewer replicas wanted: nothing is removed here, and the count follows the definition.
	put(e, "web", webUpdate("nginx:1.28", 1, 500))
	if report := e.schedule(); report.Placed != 0 {
		t.Fatalf("report = %+v", report)
	}
	st = e.get("web").Status
	if st.DesiredReplicas != 1 || st.ScheduledReplicas != 1 || len(st.Replicas) != 4 {
		t.Fatalf("status after scaling down = %+v", st)
	}

	// A deleted service is not scheduled any more; what it already has is still shown.
	e.request(http.MethodDelete, "/v1/services/web", "")
	e.addNode("n2", 4000, 8<<30)
	if report := e.schedule(); report.Placed != 0 || report.Unschedulable != 0 {
		t.Fatalf("report after deletion = %+v", report)
	}
	var list struct {
		Services []serviceView `json:"services"`
	}
	decode(t, e.request(http.MethodGet, "/v1/services?include_deleted=true", ""), &list)
	if len(list.Services) != 1 || list.Services[0].DeletedAt == nil || list.Services[0].Status.DesiredReplicas != 0 || list.Services[0].Status.ScheduledReplicas != 0 || len(list.Services[0].Status.Replicas) != 4 {
		t.Fatalf("deleted service = %+v", list.Services)
	}
}

func testWorkloadsUseTheSameScheduler(t *testing.T, e *serviceEnv) {
	// Without nodes there is nowhere to go, and the answer says so.
	rec := e.request(http.MethodPost, "/v1/workloads", `{"name":"solo","image":"x","cpu_millis":100,"memory_bytes":1048576}`)
	var refused struct {
		Error string `json:"error"`
		Code  string `json:"code"`
		Nodes []struct {
			NodeID string `json:"node_id"`
			Code   string `json:"code"`
		} `json:"nodes"`
	}
	decode(t, rec, &refused)
	if rec.Code != http.StatusConflict || refused.Code != "no_nodes" || refused.Error == "" || len(refused.Nodes) != 0 {
		t.Fatalf("no nodes: %d %s", rec.Code, rec.Body.String())
	}

	e.addNode("n1", 1000, 1<<30)
	rec = e.request(http.MethodPost, "/v1/workloads", `{"name":"huge","image":"x","cpu_millis":4000,"memory_bytes":1048576}`)
	decode(t, rec, &refused)
	if rec.Code != http.StatusConflict || refused.Code != "exceeds_capacity" || len(refused.Nodes) != 1 || refused.Nodes[0].NodeID != "n1" || refused.Nodes[0].Code != "insufficient_resources" {
		t.Fatalf("too big: %d %s", rec.Code, rec.Body.String())
	}

	// What a service's replicas reserve counts against standalone workloads, and the other way round.
	e.create(webService(1, 600))
	e.schedule()
	rec = e.request(http.MethodPost, "/v1/workloads", `{"name":"late","image":"x","cpu_millis":600,"memory_bytes":1048576}`)
	decode(t, rec, &refused)
	if rec.Code != http.StatusConflict || refused.Code != "insufficient_resources" || !strings.Contains(refused.Error, "lack CPU (needs 600m)") {
		t.Fatalf("no room beside the replica: %d %s", rec.Code, rec.Body.String())
	}
	if rec := e.request(http.MethodPost, "/v1/workloads", `{"name":"fits","image":"x","cpu_millis":400,"memory_bytes":1048576}`); rec.Code != http.StatusCreated {
		t.Fatalf("a workload that fits beside the replica = %d: %s", rec.Code, rec.Body.String())
	}
	put(e, "web", webUpdate("nginx:1.27", 2, 600))
	if report := e.schedule(); report.Placed != 0 || report.Unschedulable != 1 {
		t.Fatalf("the second replica has no room beside the workload: %+v", report)
	}
}

func testNodeViewExplainsAvailability(t *testing.T, e *serviceEnv) {
	e.addNode("fresh", 1000, 1<<30)
	e.clock = e.clock.Add(time.Hour)
	e.addNode("current", 1000, 1<<30)

	var list struct {
		Nodes []struct {
			ID           string `json:"id"`
			Status       string `json:"status"`
			StatusReason string `json:"status_reason"`
		} `json:"nodes"`
	}
	decode(t, e.request(http.MethodGet, "/v1/nodes", ""), &list)
	if len(list.Nodes) != 2 || list.Nodes[0].ID != "current" || list.Nodes[0].Status != "ready" || list.Nodes[0].StatusReason != "" ||
		list.Nodes[1].ID != "fresh" || list.Nodes[1].Status != "not_ready" || list.Nodes[1].StatusReason != "no heartbeat within the last 30s" {
		t.Fatalf("nodes = %+v", list.Nodes)
	}

	// A silent node gets no replicas, and the explanation says why.
	e.create(webService(1, 100))
	e.schedule()
	st := e.get("web").Status
	if len(st.Replicas) != 1 || st.Replicas[0].NodeID != "current" {
		t.Fatalf("status = %+v", st)
	}
}
