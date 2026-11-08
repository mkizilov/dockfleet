package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/placement"
	"github.com/dockfleet/dockfleet/internal/reconcile"
	"github.com/dockfleet/dockfleet/internal/scheduler"
	"github.com/dockfleet/dockfleet/internal/store"
)

// These tests set the replica count through the API and then run the manager's
// loops (the scheduler, the reconciler) and simulated agents, to see the whole
// of a scaling: what is created, which replicas are stopped, that the replicas
// that stay are never touched, and what the API says on the way. The API itself
// only records the count.

var scaleTiming = scheduler.Timing{SuspectAfter: 30 * time.Second, UnavailableAfter: 2 * time.Minute}

// useTiming makes the API judge a node's health the way the manager does.
func (e *serviceEnv) useTiming() {
	e.h = New(Config{
		Store: e.st, Token: token, NodeTTL: 30 * time.Second, Timing: scaleTiming,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), InstanceID: "test-manager",
		Now: func() time.Time { return e.clock },
	})
}

func (e *serviceEnv) scale(name, body string, headers ...string) *httptest.ResponseRecorder {
	return e.request(http.MethodPut, "/v1/services/"+name+"/scale", body, headers...)
}

func (e *serviceEnv) scaleOf(rec *httptest.ResponseRecorder, code int) scaleView {
	e.t.Helper()
	if rec.Code != code {
		e.t.Fatalf("status = %d, want %d: %s", rec.Code, code, rec.Body.String())
	}
	var v scaleView
	decode(e.t, rec, &v)
	return v
}

func (e *serviceEnv) getScale(name string) scaleView {
	e.t.Helper()
	return e.scaleOf(e.request(http.MethodGet, "/v1/services/"+name+"/scale", ""), http.StatusOK)
}

// placeOnce makes one scheduling pass.
func (e *serviceEnv) placeOnce() {
	e.t.Helper()
	_, err := placement.New(placement.Config{
		Store: e.st, NodeTTL: 30 * time.Second, Timing: scaleTiming, InstanceID: "test-manager", Now: func() time.Time { return e.clock },
	}).RunOnce(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
}

// reconcileOnce makes one reconciliation pass.
func (e *serviceEnv) reconcileOnce() {
	e.t.Helper()
	_, err := reconcile.New(reconcile.Config{
		Store: e.st, InstanceID: "test-manager", Timing: scaleTiming, Now: func() time.Time { return e.clock },
	}).RunOnce(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
}

// agents answers for every node that is not in e.down: it heartbeats and
// reports each workload the way an agent does. A workload meant to run is
// running, a workload meant to be stopped is stopped with no container, and
// the instruction's generation is acknowledged.
func (e *serviceEnv) agents() {
	e.t.Helper()
	ctx := context.Background()
	nodes, err := e.st.ListNodes(ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	for _, n := range nodes {
		if e.down[n.ID] {
			continue
		}
		if err := e.st.Heartbeat(ctx, n.ID, e.clock); err != nil {
			e.t.Fatal(err)
		}
		workloads, err := e.st.ListWorkloadsForNode(ctx, n.ID)
		if err != nil {
			e.t.Fatal(err)
		}
		var reports []store.Observation
		for _, w := range workloads {
			e.reports++
			o := store.Observation{WorkloadID: w.ID, AckedGeneration: w.Generation, ObservedAt: e.clock.Add(time.Duration(e.reports) * time.Millisecond)}
			if w.DesiredState == store.DesiredStopped {
				o.State = store.WorkloadStopped
			} else {
				o.State, o.ContainerID, o.Message = store.WorkloadRunning, "c-"+w.ID, "running"
			}
			reports = append(reports, o)
		}
		if len(reports) == 0 {
			continue
		}
		if err := e.st.RecordObservations(ctx, n.ID, reports); err != nil {
			e.t.Fatal(err)
		}
	}
}

// cycle is one round of the control loop: place what is missing, stop and
// remove what should go, and let the agents answer.
func (e *serviceEnv) cycle() {
	e.t.Helper()
	e.placeOnce()
	e.reconcileOnce()
	e.agents()
}

// settle runs the loop long enough for any change to reach its end: placing,
// reporting, stopping, confirming and removing are at most a few rounds apart.
func (e *serviceEnv) settle() {
	e.t.Helper()
	for i := 0; i < 5; i++ {
		e.cycle()
	}
}

func (e *serviceEnv) eventsOf(action string) []store.Event {
	e.t.Helper()
	all, err := e.st.ListEvents(context.Background(), store.EventFilter{Limit: store.MaxEventLimit})
	if err != nil {
		e.t.Fatal(err)
	}
	var out []store.Event
	for _, ev := range all {
		if ev.Action == action {
			out = append(out, ev)
		}
	}
	return out
}

// running are the replicas of the current revision that are meant to run, by index.
func (e *serviceEnv) running(name string) map[int]replicaStatus {
	e.t.Helper()
	v := e.get(name)
	out := map[int]replicaStatus{}
	for _, r := range v.Status.Replicas {
		if r.Revision == v.Revision && r.DesiredState == store.DesiredRunning {
			out[r.Index] = r
		}
	}
	return out
}

func (e *serviceEnv) wantScaling(got scalingStatus, want scalingStatus) {
	e.t.Helper()
	if got != want {
		e.t.Fatalf("scaling = %+v\nwant      %+v", got, want)
	}
}

// untouched is whether a replica that was there before is still the very same
// workload, never told anything new, and with the container it had.
func untouched(before, after replicaStatus) bool {
	return after.WorkloadID == before.WorkloadID && after.Generation == 1 && after.DesiredState == store.DesiredRunning &&
		after.ContainerID == before.ContainerID && after.ContainerID != "" && after.RestartCount == 0
}

func testScaleEndpoint(t *testing.T, e *serviceEnv) {
	e.addNode("n1", 8000, 8<<30)
	e.create(webService(2, 500))

	rec := e.request(http.MethodGet, "/v1/services/web/scale", "")
	v := e.scaleOf(rec, http.StatusOK)
	if v.Name != "web" || v.Replicas != 2 || v.Version != 1 || v.Revision != 1 || v.PreviousReplicas != nil || v.Changed != nil || rec.Header().Get("ETag") != `"1"` {
		t.Fatalf("scale = %+v, ETag %q", v, rec.Header().Get("ETag"))
	}
	e.wantScaling(v.Scaling, scalingStatus{State: scalingUp, Desired: 2, Message: "0 ready, 2 wanted; 2 to create, 0 starting"})

	e.settle()
	e.wantScaling(e.getScale("web").Scaling, scalingStatus{State: scalingStable, Desired: 2, Ready: 2, Message: "2 ready, 2 wanted"})
	before := e.running("web")

	// Up to five. The answer says what the count was and that this request changed it.
	rec = e.scale("web", `{"replicas":5}`)
	v = e.scaleOf(rec, http.StatusOK)
	if v.Replicas != 5 || v.Version != 2 || v.Revision != 1 || v.PreviousReplicas == nil || *v.PreviousReplicas != 2 || v.Changed == nil || !*v.Changed ||
		rec.Header().Get("ETag") != `"2"` {
		t.Fatalf("scale = %+v, ETag %q", v, rec.Header().Get("ETag"))
	}
	e.wantScaling(v.Scaling, scalingStatus{State: scalingUp, Desired: 5, Ready: 2, Message: "2 ready, 5 wanted; 3 to create, 0 starting"})
	if got := e.eventsOf("service.scaled"); len(got) != 1 || got[0].Actor != adminActor || got[0].ResourceType != "service" ||
		got[0].Details["name"] != "web" || got[0].Details["from_replicas"] != "2" || got[0].Details["to_replicas"] != "5" {
		t.Fatalf("scaled events = %+v", got)
	}

	// Defining the count decides nothing: the scheduler assigns the new replicas, and they start.
	if len(e.running("web")) != 2 {
		t.Fatalf("the request created replicas itself: %+v", e.running("web"))
	}
	e.placeOnce()
	e.wantScaling(e.getScale("web").Scaling, scalingStatus{State: scalingUp, Desired: 5, Ready: 2, Starting: 3, Message: "2 ready, 5 wanted; 0 to create, 3 starting"})
	e.settle()
	e.wantScaling(e.getScale("web").Scaling, scalingStatus{State: scalingStable, Desired: 5, Ready: 5, Message: "5 ready, 5 wanted"})
	after := e.running("web")
	if len(after) != 5 {
		t.Fatalf("replicas = %+v", after)
	}
	for i := 1; i <= 2; i++ {
		if !untouched(before[i], after[i]) {
			t.Fatalf("replica %d was touched by the scale-up: before %+v, after %+v", i, before[i], after[i])
		}
	}
	svc := e.get("web")
	var revisions struct {
		Revisions []json.RawMessage `json:"revisions"`
	}
	decode(t, e.request(http.MethodGet, "/v1/services/web/revisions", ""), &revisions)
	if svc.Replicas != 5 || svc.Revision != 1 || svc.Spec.Image != "nginx:1.27" || len(revisions.Revisions) != 1 {
		t.Fatalf("scaling changed the definition: %+v, %d revisions", svc, len(revisions.Revisions))
	}

	// Down to three: two of the five go, the other three are left alone.
	v = e.scaleOf(e.scale("web", `{"replicas":3}`), http.StatusOK)
	e.wantScaling(v.Scaling, scalingStatus{State: scalingDown, Desired: 3, Ready: 5, Surplus: 2, Message: "5 ready, 3 wanted; 2 to stop, 0 stopping"})
	e.settle()
	e.wantScaling(e.getScale("web").Scaling, scalingStatus{State: scalingStable, Desired: 3, Ready: 3, Message: "3 ready, 3 wanted"})
	left := e.running("web")
	if len(left) != 3 || len(e.get("web").Status.Replicas) != 3 {
		t.Fatalf("replicas = %+v", e.get("web").Status.Replicas)
	}
	for i := 1; i <= 3; i++ {
		if !untouched(after[i], left[i]) {
			t.Fatalf("replica %d was touched by the scale-down: before %+v, after %+v", i, after[i], left[i])
		}
	}
	if n := e.countEvents("replica.scheduled"); n != 5 {
		t.Fatalf("%d replicas were ever scheduled, want 5", n)
	}
	if stopped, removed := e.countEvents("replica.stopped"), e.countEvents("replica.removed"); stopped != 2 || removed != 2 {
		t.Fatalf("%d stopped and %d removed, want 2 and 2", stopped, removed)
	}
}

func (e *serviceEnv) countEvents(action string) int { return len(e.eventsOf(action)) }

// The request says what the count should be, not by how much to change it, so
// sending it again, after a lost answer or by a script run twice, is harmless.
func testScaleIsIdempotent(t *testing.T, e *serviceEnv) {
	e.addNode("n1", 8000, 8<<30)
	e.create(webService(2, 500))
	e.settle()

	first := e.scaleOf(e.scale("web", `{"replicas":4}`), http.StatusOK)
	if first.Changed == nil || !*first.Changed || first.Version != 2 {
		t.Fatalf("first request = %+v", first)
	}

	// The same request again changes nothing and says so.
	rec := e.scale("web", `{"replicas":4}`)
	again := e.scaleOf(rec, http.StatusOK)
	if again.Changed == nil || *again.Changed || again.PreviousReplicas == nil || *again.PreviousReplicas != 4 || again.Replicas != 4 || again.Version != 2 ||
		rec.Header().Get("ETag") != `"2"` {
		t.Fatalf("repeated request = %+v, ETag %q", again, rec.Header().Get("ETag"))
	}
	// Even with a version that is out of date: the count is already what was asked for.
	stale := e.scaleOf(e.scale("web", `{"replicas":4}`, "If-Match", `"1"`), http.StatusOK)
	if stale.Changed == nil || *stale.Changed || stale.Version != 2 {
		t.Fatalf("repeated request with a stale version = %+v", stale)
	}
	// But a different count with a stale version is refused, and the service is as it was.
	if rec := e.scale("web", `{"replicas":6}`, "If-Match", `"1"`); rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale version = %d, want 412: %s", rec.Code, rec.Body.String())
	}
	if v := e.get("web"); v.Replicas != 4 || v.Version != 2 {
		t.Fatalf("service = %+v", v)
	}
	// With the current version, and with *, it is applied.
	if v := e.scaleOf(e.scale("web", `{"replicas":6}`, "If-Match", `"2"`), http.StatusOK); v.Changed == nil || !*v.Changed || v.Version != 3 {
		t.Fatalf("with the current version = %+v", v)
	}
	if v := e.scaleOf(e.scale("web", `{"replicas":7}`, "If-Match", "*"), http.StatusOK); v.Changed == nil || !*v.Changed || v.Version != 4 {
		t.Fatalf("with * = %+v", v)
	}

	// Three changes were made and recorded; the repeats were not.
	if got := e.eventsOf("service.scaled"); len(got) != 3 {
		t.Fatalf("%d scaled events, want 3 (4, 6 and 7)", len(got))
	}

	// However often the loops run, the replicas are created once.
	for i := 0; i < 3; i++ {
		e.settle()
	}
	workloads, err := e.st.ListWorkloads(context.Background())
	if err != nil || len(workloads) != 7 {
		t.Fatalf("%d workloads, %v; want 7", len(workloads), err)
	}
	if n := e.countEvents("replica.scheduled"); n != 7 {
		t.Fatalf("%d replicas were scheduled, want 7", n)
	}
	e.wantScaling(e.getScale("web").Scaling, scalingStatus{State: scalingStable, Desired: 7, Ready: 7, Message: "7 ready, 7 wanted"})

	// Asking for the count it has at the end changes nothing at all.
	version := e.get("web").Version
	e.scaleOf(e.scale("web", `{"replicas":7}`), http.StatusOK)
	if e.get("web").Version != version || e.countEvents("service.scaled") != 3 || e.countEvents("replica.scheduled") != 7 {
		t.Fatal("a request for the current count changed something")
	}
}

func testScaleValidation(t *testing.T, e *serviceEnv) {
	e.create(minimalService)
	bad := []struct{ name, body, field string }{
		{"no body", ``, ""},
		{"not JSON", `replicas=3`, ""},
		{"an array", `[3]`, ""},
		{"null", `null`, ""},
		{"a number", `3`, ""},
		{"no replicas", `{}`, "replicas"},
		{"null replicas", `{"replicas":null}`, "replicas"},
		{"negative", `{"replicas":-1}`, "replicas"},
		{"too many", `{"replicas":1001}`, "replicas"},
		{"a string", `{"replicas":"3"}`, "replicas"},
		{"a fraction", `{"replicas":2.5}`, "replicas"},
		{"a boolean", `{"replicas":true}`, "replicas"},
		{"another field", `{"replicas":3,"image":"nginx:2"}`, "image"},
		{"a spec", `{"replicas":3,"spec":{"image":"nginx:2"}}`, "spec"},
		{"a status", `{"replicas":3,"status":{}}`, "status"},
	}
	for _, tt := range bad {
		rec := e.scale("web", tt.body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400: %s", tt.name, rec.Code, rec.Body.String())
			continue
		}
		var out struct {
			Error   string   `json:"error"`
			Details problems `json:"details"`
		}
		decode(t, rec, &out)
		if out.Error != "invalid scale request" || len(out.Details) == 0 {
			t.Errorf("%s: answer = %s", tt.name, rec.Body.String())
		}
		found := tt.field == ""
		for _, p := range out.Details {
			found = found || p.Field == tt.field
		}
		if !found {
			t.Errorf("%s: no problem names %q: %+v", tt.name, tt.field, out.Details)
		}
	}
	if rec := e.scale("web", `{"replicas":3,"pad":"`+strings.Repeat("x", maxBodyBytes)+`"}`); rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("a huge body = %d, want 413", rec.Code)
	}
	if rec := e.scale("web", `{"replicas":3}`, "If-Match", "abc"); rec.Code != http.StatusBadRequest {
		t.Errorf("a malformed If-Match = %d, want 400", rec.Code)
	}
	if rec := e.scale("web", `{"replicas":0}`); rec.Code != http.StatusOK {
		t.Errorf("zero replicas = %d, want 200: scaling to zero is allowed", rec.Code)
	}
	if rec := e.scale("web", `{"replicas":1000}`); rec.Code != http.StatusOK {
		t.Errorf("1000 replicas = %d, want 200: it is the largest count allowed", rec.Code)
	}

	// Nothing the bad requests did survived: only the two good ones are recorded.
	if v := e.get("web"); v.Version != 3 || v.Replicas != 1000 {
		t.Fatalf("service = %+v", v)
	}
	e.wantEvents("service.created", "service.scaled", "service.scaled")

	// Unknown and deleted services.
	if rec := e.scale("nobody", `{"replicas":3}`); rec.Code != http.StatusNotFound {
		t.Errorf("unknown service = %d, want 404", rec.Code)
	}
	if rec := e.request(http.MethodGet, "/v1/services/nobody/scale", ""); rec.Code != http.StatusNotFound {
		t.Errorf("reading an unknown service = %d, want 404", rec.Code)
	}
	e.request(http.MethodDelete, "/v1/services/web", "")
	if rec := e.scale("web", `{"replicas":3}`); rec.Code != http.StatusNotFound {
		t.Errorf("deleted service = %d, want 404", rec.Code)
	}
	if rec := e.request(http.MethodGet, "/v1/services/web/scale", ""); rec.Code != http.StatusNotFound {
		t.Errorf("reading a deleted service = %d, want 404", rec.Code)
	}
}

// A scale-down and a scale-up on two nodes, from the request to the final
// state, with what the API says at each step.
func testScaleReportsProgress(t *testing.T, e *serviceEnv) {
	e.addNode("n1", 4000, 8<<30)
	e.addNode("n2", 4000, 8<<30)
	e.create(webService(4, 500))
	e.settle()
	first := e.running("web")
	if len(first) != 4 || first[1].NodeID != "n1" || first[2].NodeID != "n2" || first[3].NodeID != "n1" || first[4].NodeID != "n2" {
		t.Fatalf("replicas = %+v; they should be spread over the two nodes", first)
	}

	// Two fewer: the answer already says what is going to happen.
	v := e.scaleOf(e.scale("web", `{"replicas":2}`), http.StatusOK)
	e.wantScaling(v.Scaling, scalingStatus{State: scalingDown, Desired: 2, Ready: 4, Surplus: 2, Message: "4 ready, 2 wanted; 2 to stop, 0 stopping"})

	// The reconciler picks the replicas: the highest numbers on the nodes that carry
	// most, so each node keeps one, and nothing that stays is touched.
	e.reconcileOnce()
	e.wantScaling(e.getScale("web").Scaling, scalingStatus{State: scalingDown, Desired: 2, Ready: 2, Stopping: 2, Message: "2 ready, 2 wanted; 0 to stop, 2 stopping"})
	stopping := 0
	for _, r := range e.get("web").Status.Replicas {
		if r.DesiredState == store.DesiredStopped {
			stopping++
			if r.Index != 3 && r.Index != 4 || r.Generation != 2 {
				t.Fatalf("stopping replica = %+v, want replica 3 or 4 at generation 2", r)
			}
		}
	}
	if stopping != 2 {
		t.Fatalf("%d replicas are being stopped, want 2", stopping)
	}

	// Once the agents have removed the containers the service is at its final state,
	// whether or not the records are cleared yet.
	e.agents()
	e.wantScaling(e.getScale("web").Scaling, scalingStatus{State: scalingStable, Desired: 2, Ready: 2, Message: "2 ready, 2 wanted"})
	e.settle()
	st := e.get("web").Status
	if len(st.Replicas) != 2 || st.RunningReplicas != 2 || st.ScheduledReplicas != 2 {
		t.Fatalf("status = %+v", st)
	}
	for i := 1; i <= 2; i++ {
		if r := e.running("web")[i]; !untouched(first[i], r) || r.NodeID != first[i].NodeID {
			t.Fatalf("replica %d was touched: before %+v, after %+v", i, first[i], r)
		}
	}

	// One more: it takes the lowest free number.
	v = e.scaleOf(e.scale("web", `{"replicas":3}`), http.StatusOK)
	e.wantScaling(v.Scaling, scalingStatus{State: scalingUp, Desired: 3, Ready: 2, Message: "2 ready, 3 wanted; 1 to create, 0 starting"})
	e.settle()
	e.wantScaling(e.getScale("web").Scaling, scalingStatus{State: scalingStable, Desired: 3, Ready: 3, Message: "3 ready, 3 wanted"})
	third := e.running("web")
	if len(third) != 3 || !untouched(first[1], third[1]) || !untouched(first[2], third[2]) || third[3].WorkloadID == first[3].WorkloadID {
		t.Fatalf("replicas = %+v; 1 and 2 stay and 3 is a new workload", third)
	}

	// None: everything goes, and the service ends with no replicas and no records.
	v = e.scaleOf(e.scale("web", `{"replicas":0}`), http.StatusOK)
	e.wantScaling(v.Scaling, scalingStatus{State: scalingDown, Desired: 0, Ready: 3, Surplus: 3, Message: "3 ready, 0 wanted; 3 to stop, 0 stopping"})
	e.settle()
	e.wantScaling(e.getScale("web").Scaling, scalingStatus{State: scalingStable, Message: "none wanted, none left"})
	if st := e.get("web").Status; len(st.Replicas) != 0 || st.ScheduledReplicas != 0 {
		t.Fatalf("status = %+v", st)
	}

	// Every container was created once and stopped once: 4 first, 1 more, 5 removed.
	if scheduled, stopped, removed := e.countEvents("replica.scheduled"), e.countEvents("replica.stopped"), e.countEvents("replica.removed"); scheduled != 5 || stopped != 5 || removed != 5 {
		t.Fatalf("%d scheduled, %d stopped, %d removed; want 5 of each", scheduled, stopped, removed)
	}
	// A service can be scaled up again from nothing.
	e.scaleOf(e.scale("web", `{"replicas":2}`), http.StatusOK)
	e.settle()
	e.wantScaling(e.getScale("web").Scaling, scalingStatus{State: scalingStable, Desired: 2, Ready: 2, Message: "2 ready, 2 wanted"})
}

// More replicas than the cluster can hold: those that fit run, the others are
// reported as blocked with the scheduler's reason, and the replica that was
// already running is not disturbed.
func testScaleBeyondCapacity(t *testing.T, e *serviceEnv) {
	e.addNode("n1", 1000, 4<<30)
	e.create(webService(1, 600))
	e.settle()
	first := e.running("web")[1]

	// The request is accepted: it is a wish, and says nothing yet about the cluster.
	v := e.scaleOf(e.scale("web", `{"replicas":3}`), http.StatusOK)
	e.wantScaling(v.Scaling, scalingStatus{State: scalingUp, Desired: 3, Ready: 1, Message: "1 ready, 3 wanted; 2 to create, 0 starting"})

	// Then the scheduler looks, and the status says why two cannot run.
	e.settle()
	e.wantScaling(e.getScale("web").Scaling, scalingStatus{
		State: scalingBlocked, Desired: 3, Ready: 1, Waiting: 2,
		Message: "1 ready, 3 wanted; 2 cannot be placed: 0/1 nodes can run web#2: 1 node(s) lack CPU (needs 600m)",
	})
	st := e.get("web").Status
	if st.ScheduledReplicas != 1 || st.UnschedulableReplicas != 2 || len(st.Unschedulable) != 2 || len(st.Replicas) != 1 {
		t.Fatalf("status = %+v", st)
	}
	for i, u := range st.Unschedulable {
		if u.ReplicaIndex != i+2 || u.Code != "insufficient_resources" || len(u.Nodes) != 1 || u.Nodes[0].NodeID != "n1" ||
			u.Nodes[0].Message != "insufficient resources (CPU: needs 600m, 400m free)" {
			t.Fatalf("explanation %d = %+v", i, u)
		}
	}
	if !untouched(first, e.running("web")[1]) {
		t.Fatalf("the running replica was disturbed: %+v", e.running("web"))
	}
	// Nothing was oversubscribed, and the reasons were announced once however often the loops ran.
	if n := e.countEvents("replica.unschedulable"); n != 2 {
		t.Fatalf("%d unschedulable events, want 2", n)
	}

	// The same request again changes nothing, blocked or not.
	if v := e.scaleOf(e.scale("web", `{"replicas":3}`), http.StatusOK); v.Changed == nil || *v.Changed || v.Scaling.State != scalingBlocked || v.Scaling.Waiting != 2 {
		t.Fatalf("repeated request = %+v", v)
	}

	// Fewer wanted: one still does not fit, and only its explanation is left.
	e.scaleOf(e.scale("web", `{"replicas":2}`), http.StatusOK)
	e.settle()
	got := e.getScale("web").Scaling
	e.wantScaling(got, scalingStatus{
		State: scalingBlocked, Desired: 2, Ready: 1, Waiting: 1,
		Message: "1 ready, 2 wanted; 1 cannot be placed: 0/1 nodes can run web#2: 1 node(s) lack CPU (needs 600m)",
	})

	// Back to what fits: the explanation goes and the service is whole.
	e.scaleOf(e.scale("web", `{"replicas":1}`), http.StatusOK)
	e.settle()
	e.wantScaling(e.getScale("web").Scaling, scalingStatus{State: scalingStable, Desired: 1, Ready: 1, Message: "1 ready, 1 wanted"})
	if st := e.get("web").Status; len(st.Unschedulable) != 0 || st.UnschedulableReplicas != 0 {
		t.Fatalf("status = %+v; the explanations should be gone", st)
	}

	// A node with room joins and a larger count is asked for: the new replicas go there.
	e.addNode("n2", 4000, 8<<30)
	e.scaleOf(e.scale("web", `{"replicas":4}`), http.StatusOK)
	e.settle()
	e.wantScaling(e.getScale("web").Scaling, scalingStatus{State: scalingStable, Desired: 4, Ready: 4, Message: "4 ready, 4 wanted"})
	byNode := map[string]int{}
	for _, r := range e.running("web") {
		byNode[r.NodeID]++
	}
	if byNode["n1"] != 1 || byNode["n2"] != 3 {
		t.Fatalf("replicas per node = %v; n1 has 400m left and takes no more", byNode)
	}
	if !untouched(first, e.running("web")[1]) {
		t.Fatalf("the first replica was disturbed: %+v", e.running("web"))
	}
}

// Replica counts can be changed while a node is away: the nodes that answer are
// used, what cannot fit says so, and when the node returns the rest is placed.
func testScaleWithANodeUnavailable(t *testing.T, e *serviceEnv) {
	e.useTiming()
	e.addNode("n1", 4000, 8<<30)
	e.addNode("n2", 4000, 8<<30)
	e.create(webService(2, 500))
	e.settle()
	before := e.running("web")
	if before[1].NodeID != "n1" || before[2].NodeID != "n2" {
		t.Fatalf("replicas = %+v", before)
	}

	// n2 goes silent and three minutes pass while n1 keeps answering: n2 is given up on and its replica moves.
	e.down = map[string]bool{"n2": true}
	e.clock = e.clock.Add(3 * time.Minute)
	e.agents() // n1 answers at once; without this its own heartbeat would be three minutes old too
	e.settle()
	var onN2 []replicaStatus
	for _, r := range e.get("web").Status.Replicas {
		if r.NodeID == "n2" {
			onN2 = append(onN2, r)
		}
	}
	if len(onN2) != 1 || onN2[0].DesiredState != store.DesiredStopped || onN2[0].Generation != 2 || onN2[0].NodeHealth != store.NodeUnavailable {
		t.Fatalf("the replica on n2 = %+v", onN2)
	}
	moved := e.running("web")
	if len(moved) != 2 || moved[1].NodeID != "n1" || moved[2].NodeID != "n1" || !untouched(before[1], moved[1]) {
		t.Fatalf("replicas = %+v; both belong on n1 now and the one that was there is untouched", moved)
	}
	e.wantScaling(e.getScale("web").Scaling, scalingStatus{State: scalingStable, Desired: 2, Ready: 2, Message: "2 ready, 2 wanted"})

	// More replicas while n2 is away: they go where there is room and an answer.
	e.scaleOf(e.scale("web", `{"replicas":5}`), http.StatusOK)
	e.settle()
	five := e.running("web")
	if len(five) != 5 {
		t.Fatalf("replicas = %+v", five)
	}
	for i, r := range five {
		if r.NodeID != "n1" {
			t.Fatalf("replica %d is on %s; n2 does not answer", i, r.NodeID)
		}
	}
	if !untouched(moved[1], five[1]) || !untouched(moved[2], five[2]) {
		t.Fatalf("replicas that were running were touched: %+v", five)
	}
	e.wantScaling(e.getScale("web").Scaling, scalingStatus{State: scalingStable, Desired: 5, Ready: 5, Message: "5 ready, 5 wanted"})

	// More than n1 holds: one waits, and the reason names n2 as unavailable.
	e.scaleOf(e.scale("web", `{"replicas":9}`), http.StatusOK)
	e.settle()
	v := e.getScale("web")
	if v.Scaling.State != scalingBlocked || v.Scaling.Waiting != 1 || v.Scaling.Ready != 8 || v.Scaling.Desired != 9 {
		t.Fatalf("scaling = %+v", v.Scaling)
	}
	waiting := e.get("web").Status.Unschedulable
	if len(waiting) != 1 || len(waiting[0].Nodes) != 2 {
		t.Fatalf("explanations = %+v", waiting)
	}
	for _, n := range waiting[0].Nodes {
		switch n.NodeID {
		case "n2":
			if n.Code != "unavailable" {
				t.Errorf("n2 = %+v, want unavailable", n)
			}
		case "n1":
			if n.Code != "insufficient_resources" {
				t.Errorf("n1 = %+v, want insufficient_resources", n)
			}
		default:
			t.Errorf("unexpected node %+v", n)
		}
	}

	// n2 returns: it removes the stale container, takes the waiting replica and the service is whole.
	delete(e.down, "n2")
	e.settle()
	e.wantScaling(e.getScale("web").Scaling, scalingStatus{State: scalingStable, Desired: 9, Ready: 9, Message: "9 ready, 9 wanted"})
	byNode := map[string]int{}
	for _, r := range e.running("web") {
		byNode[r.NodeID]++
	}
	if byNode["n1"] != 8 || byNode["n2"] != 1 || len(e.get("web").Status.Replicas) != 9 {
		t.Fatalf("replicas per node = %v, records %d; want n1 8, n2 1, and the stale record gone", byNode, len(e.get("web").Status.Replicas))
	}
}
