package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/api"
	"github.com/dockfleet/dockfleet/internal/dockerfake"
	"github.com/dockfleet/dockfleet/internal/scheduler"
	"github.com/dockfleet/dockfleet/internal/store"
)

// These tests change a service's replica count through the administrative REST
// API of a running fleet: real agents and a real manager over gRPC, the
// scheduler and the reconciler running as in the manager process, and an
// in-memory Docker engine on each node. They look at what the engines were told
// to do (exact counts of created, started, stopped and removed containers), at
// what the API says along the way, and at the audit trail.

// scaleAdmin is an administrator talking to the API of a manager.
type scaleAdmin struct {
	t *testing.T
	h http.Handler
	m *manager
}

func newScaleAdmin(t *testing.T, m *manager, timing scheduler.Timing, lease time.Duration) *scaleAdmin {
	nodeTTL := time.Minute
	if timing.SuspectAfter > 0 {
		nodeTTL = timing.SuspectAfter
	}
	return &scaleAdmin{t: t, m: m, h: api.New(api.Config{
		Store: m.store, Token: adminToken, NodeTTL: nodeTTL, Timing: timing, NodeLease: lease, Logger: discardLogger(),
	})}
}

func (a *scaleAdmin) do(method, path, body string, headers ...string) *httptest.ResponseRecorder {
	a.t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+adminToken)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	a.h.ServeHTTP(rec, req)
	return rec
}

// scaleJSON is the answer of GET and PUT /v1/services/{name}/scale.
type scaleJSON struct {
	Name             string `json:"name"`
	Revision         int    `json:"revision"`
	Version          int64  `json:"version"`
	Replicas         int    `json:"replicas"`
	PreviousReplicas *int   `json:"previous_replicas"`
	Changed          *bool  `json:"changed"`
	Scaling          struct {
		State    string `json:"state"`
		Desired  int    `json:"desired"`
		Ready    int    `json:"ready"`
		Starting int    `json:"starting"`
		Waiting  int    `json:"waiting"`
		Surplus  int    `json:"surplus"`
		Stopping int    `json:"stopping"`
		Outdated int    `json:"outdated"`
		Message  string `json:"message"`
	} `json:"scaling"`
}

func (a *scaleAdmin) create(name string, replicas int, extraSpec string) {
	a.t.Helper()
	body := fmt.Sprintf(`{"name":%q,"replicas":%d,"spec":{"image":"nginx:1.0","cpu_millis":500,"memory_bytes":134217728%s}}`, name, replicas, extraSpec)
	if rec := a.do(http.MethodPost, "/v1/services", body); rec.Code != http.StatusCreated {
		a.t.Fatalf("create %s = %d: %s", name, rec.Code, rec.Body)
	}
}

// scale asks for a replica count and returns the answer.
func (a *scaleAdmin) scale(name string, replicas int, headers ...string) scaleJSON {
	a.t.Helper()
	rec := a.do(http.MethodPut, "/v1/services/"+name+"/scale", fmt.Sprintf(`{"replicas":%d}`, replicas), headers...)
	if rec.Code != http.StatusOK {
		a.t.Fatalf("scale %s to %d = %d: %s", name, replicas, rec.Code, rec.Body)
	}
	var v scaleJSON
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		a.t.Fatal(err)
	}
	return v
}

// progress reads GET /v1/services/{name}/scale; ok is false if the answer is not a success.
func (a *scaleAdmin) progress(name string) (v scaleJSON, ok bool) {
	rec := a.do(http.MethodGet, "/v1/services/"+name+"/scale", "")
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &v) != nil {
		return v, false
	}
	return v, true
}

// converged is true once the service has reached exactly n replicas: the API
// says it is stable at n, and the store and the engines agree.
func (a *scaleAdmin) converged(name string, n int, engines ...*dockerfake.Fake) func() bool {
	return func() bool {
		v, ok := a.progress(name)
		if !ok || v.Scaling.State != "stable" || v.Scaling.Desired != n || v.Scaling.Ready != n {
			return false
		}
		svc, err := a.m.store.GetServiceByName(context.Background(), name)
		if err != nil {
			return false
		}
		ws, err := a.m.store.ListWorkloadsForService(context.Background(), svc.ID)
		return err == nil && len(ws) == n && len(managedOn(engines...)) == n
	}
}

// blocked is true once n replicas wait for room and r are ready.
func (a *scaleAdmin) blocked(name string, waiting, ready int) func() bool {
	return func() bool {
		v, ok := a.progress(name)
		return ok && v.Scaling.State == "blocked" && v.Scaling.Waiting == waiting && v.Scaling.Ready == ready
	}
}

func (a *scaleAdmin) countEvents(action string) int {
	a.t.Helper()
	all, err := a.m.store.ListEvents(context.Background(), store.EventFilter{Limit: store.MaxEventLimit})
	if err != nil {
		a.t.Fatal(err)
	}
	n := 0
	for _, e := range all {
		if e.Action == action {
			n++
		}
	}
	return n
}

// workloads are the service's workloads in the store.
func (a *scaleAdmin) workloads(name string) []store.Workload {
	a.t.Helper()
	svc, err := a.m.store.GetServiceByName(context.Background(), name)
	if err != nil {
		a.t.Fatal(err)
	}
	ws, err := a.m.store.ListWorkloadsForService(context.Background(), svc.ID)
	if err != nil {
		a.t.Fatal(err)
	}
	return ws
}

// Scaling up creates the missing replicas and nothing else: the containers that
// run keep running, and no container is created twice however often the loops run.
func TestScaleUpCreatesOnlyTheMissingContainers(t *testing.T) {
	m := newManager(t, nil)
	m.start()
	a1 := m.startAgent("sim-1", nil)
	a2 := m.startAgent("sim-2", nil)
	waitFor(t, "both agents to register", func() bool { return m.sessionID("sim-1") != "" && m.sessionID("sim-2") != "" })
	m.controlLoops()
	admin := newScaleAdmin(t, m, scheduler.Timing{}, 0)
	engines := []*dockerfake.Fake{a1.engine, a2.engine}

	admin.create("web", 2, "")
	waitFor(t, "two replicas", admin.converged("web", 2, engines...))
	first := workloadIDs(managedOn(engines...))

	v := admin.scale("web", 5)
	if v.Replicas != 5 || v.PreviousReplicas == nil || *v.PreviousReplicas != 2 || v.Changed == nil || !*v.Changed || v.Scaling.Desired != 5 || v.Version != 2 {
		t.Fatalf("answer = %+v", v)
	}
	waitFor(t, "five replicas", admin.converged("web", 5, engines...))
	m.heartbeats("sim-1", 10) // many more passes of every loop: nothing else may happen

	managed := managedOn(engines...)
	if len(managed) != 5 || countOn("create", engines...) != 5 || countOn("start", engines...) != 5 ||
		countOn("stop", engines...) != 0 || countOn("remove", engines...) != 0 {
		t.Fatalf("containers %d, creates %d, starts %d, stops %d, removes %d; want 5, 5, 5, 0, 0",
			len(managed), countOn("create", engines...), countOn("start", engines...), countOn("stop", engines...), countOn("remove", engines...))
	}
	now := workloadIDs(managed)
	for _, id := range first {
		if !slices.Contains(now, id) {
			t.Fatalf("replica %s, which ran before the scale-up, is gone: %v", id, now)
		}
	}
	// Five replicas of one service over two nodes: three and two.
	if n1, n2 := len(a1.engine.Managed()), len(a2.engine.Managed()); n1+n2 != 5 || n1 < 2 || n2 < 2 {
		t.Fatalf("containers per node: %d and %d", n1, n2)
	}
	// Each new replica was announced once.
	if n := admin.countEvents("replica.scheduled"); n != 5 {
		t.Fatalf("%d replicas scheduled, want 5", n)
	}
	if v, _ := admin.progress("web"); v.Scaling.Message != "5 ready, 5 wanted" {
		t.Fatalf("scaling = %+v", v.Scaling)
	}
}

// Scaling down removes the replicas that do the least good first, here one that
// failed, and leaves the others as they are: not restarted, not replaced.
func TestScaleDownRemovesTheFailedReplicaAndLeavesTheRestAlone(t *testing.T) {
	m := newManager(t, nil)
	m.start()
	a1 := m.startAgent("sim-1", nil)
	a2 := m.startAgent("sim-2", nil)
	waitFor(t, "both agents to register", func() bool { return m.sessionID("sim-1") != "" && m.sessionID("sim-2") != "" })
	m.controlLoops()
	admin := newScaleAdmin(t, m, scheduler.Timing{}, 0)
	engines := []*dockerfake.Fake{a1.engine, a2.engine}
	ctx := context.Background()

	// A container that is not restarted when it fails, so the failure stays.
	admin.create("web", 4, `,"restart_policy":{"condition":"no"}`)
	waitFor(t, "four replicas", admin.converged("web", 4, engines...))
	all := admin.workloads("web")
	victim := all[1] // replica 2: not the highest numbered, so choosing by number would pick another
	engineOf := func(node string) *dockerfake.Fake {
		if node == "sim-1" {
			return a1.engine
		}
		return a2.engine
	}
	engineOf(victim.NodeID).Crash(victim.ID, 1)
	waitFor(t, "the crashed replica to be reported failed and the service degraded", func() bool {
		w, err := m.store.GetWorkload(ctx, victim.ID)
		v, ok := admin.progress("web")
		return err == nil && w.Observed != nil && w.Observed.State == store.WorkloadFailed && ok && v.Scaling.State == "degraded" && v.Scaling.Ready == 3
	})

	// One fewer is wanted: the failed one goes.
	v := admin.scale("web", 3)
	if v.Changed == nil || !*v.Changed || v.Scaling.State != "scaling_down" && v.Scaling.State != "stable" {
		t.Fatalf("answer = %+v", v)
	}
	waitFor(t, "three replicas", admin.converged("web", 3, engines...))
	if _, err := m.store.GetWorkload(ctx, victim.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the failed replica is still there: %v", err)
	}
	stops, err := m.store.ListEvents(ctx, store.EventFilter{ResourceType: "workload", ResourceID: victim.ID, Limit: store.MaxEventLimit})
	if err != nil {
		t.Fatal(err)
	}
	var why string
	for _, e := range stops {
		if e.Action == "replica.stopped" {
			why = e.Details["why"] + "/" + e.Details["reason"]
		}
	}
	if why != "failed/scaled_down" {
		t.Fatalf("the failed replica's stop was recorded as %q, want failed/scaled_down", why)
	}
	survivors := workloadIDs(managedOn(engines...))
	for _, w := range all {
		if w.ID != victim.ID && !slices.Contains(survivors, w.ID) {
			t.Fatalf("replica %s was removed although it was healthy: %v", w.Name, survivors)
		}
	}
	if countOn("create", engines...) != 4 || countOn("start", engines...) != 4 {
		t.Fatalf("creates %d, starts %d; the replicas that stay must not be restarted or replaced", countOn("create", engines...), countOn("start", engines...))
	}

	// Down to one, all healthy: one of the survivors stays, still the same container, and the other two go.
	admin.scale("web", 1)
	waitFor(t, "one replica", admin.converged("web", 1, engines...))
	last := managedOn(engines...)
	if len(last) != 1 || !slices.Contains(survivors, last[0].WorkloadID) || countOn("create", engines...) != 4 || countOn("start", engines...) != 4 {
		t.Fatalf("containers %+v, creates %d, starts %d", last, countOn("create", engines...), countOn("start", engines...))
	}

	// And to none: everything of the service goes, with nothing left on either node.
	admin.scale("web", 0)
	waitFor(t, "no replicas", admin.converged("web", 0, engines...))
	if v, _ := admin.progress("web"); v.Scaling.Message != "none wanted, none left" || countOn("remove", engines...) != 4 {
		t.Fatalf("scaling = %+v, removes %d", v.Scaling, countOn("remove", engines...))
	}
	if n := admin.countEvents("replica.removed"); n != 4 {
		t.Fatalf("%d replica.removed events, want 4", n)
	}
}

// More replicas than a node holds: the ones that fit run, the others are
// reported as blocked with the scheduler's reason, and nothing is oversubscribed.
// A node that joins takes the rest without disturbing what runs.
func TestScaleBeyondTheCapacityIsReportedAndCompletedWhenRoomAppears(t *testing.T) {
	m := newManager(t, nil)
	m.start()
	a1 := m.startAgent("sim-1", nil)
	waitFor(t, "registration", func() bool { return m.sessionID("sim-1") != "" })
	m.controlLoops()
	admin := newScaleAdmin(t, m, scheduler.Timing{}, 0)

	admin.create("web", 2, "")
	waitFor(t, "two replicas", admin.converged("web", 2, a1.engine))
	running := workloadIDs(managedOn(a1.engine))

	// A node has 2000m; each replica needs 500m.
	if v := admin.scale("web", 6); v.Changed == nil || !*v.Changed || v.Scaling.Desired != 6 {
		t.Fatalf("answer = %+v", v)
	}
	waitFor(t, "four to run and two to be blocked", admin.blocked("web", 2, 4))
	m.heartbeats("sim-1", 10)
	if got := managedOn(a1.engine); len(got) != 4 || countOn("create", a1.engine) != 4 {
		t.Fatalf("containers %d, creates %d; the node holds four", len(got), countOn("create", a1.engine))
	}
	v, _ := admin.progress("web")
	if !strings.Contains(v.Scaling.Message, "2 cannot be placed") || !strings.Contains(v.Scaling.Message, "lack CPU (needs 500m)") {
		t.Fatalf("message = %q", v.Scaling.Message)
	}
	var svc struct {
		Status struct {
			ScheduledReplicas     int `json:"scheduled_replicas"`
			UnschedulableReplicas int `json:"unschedulable_replicas"`
			Unschedulable         []struct {
				ReplicaIndex int    `json:"replica_index"`
				Code         string `json:"code"`
				Nodes        []struct {
					NodeID  string `json:"node_id"`
					Code    string `json:"code"`
					Message string `json:"message"`
				} `json:"nodes"`
			} `json:"unschedulable"`
		} `json:"status"`
	}
	if rec := admin.do(http.MethodGet, "/v1/services/web", ""); rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &svc) != nil {
		t.Fatalf("GET = %d %s", rec.Code, rec.Body)
	}
	st := svc.Status
	if st.ScheduledReplicas != 4 || st.UnschedulableReplicas != 2 || len(st.Unschedulable) != 2 {
		t.Fatalf("status = %+v", st)
	}
	for i, u := range st.Unschedulable {
		if u.ReplicaIndex != 5+i || u.Code != "insufficient_resources" || len(u.Nodes) != 1 || u.Nodes[0].NodeID != "sim-1" || u.Nodes[0].Code != "insufficient_resources" {
			t.Fatalf("explanation %d = %+v", i, u)
		}
	}
	if n := admin.countEvents("replica.unschedulable"); n != 2 {
		t.Fatalf("%d unschedulable events, want 2 (each reason is announced once)", n)
	}

	// A node with room joins: the two waiting replicas go there, and sim-1 is not touched.
	a2 := m.startAgent("sim-2", nil)
	waitFor(t, "sim-2 to register", func() bool { return m.sessionID("sim-2") != "" })
	engines := []*dockerfake.Fake{a1.engine, a2.engine}
	waitFor(t, "six replicas", admin.converged("web", 6, engines...))
	if len(a1.engine.Managed()) != 4 || len(a2.engine.Managed()) != 2 {
		t.Fatalf("containers per node: %d and %d, want 4 and 2", len(a1.engine.Managed()), len(a2.engine.Managed()))
	}
	if countOn("create", engines...) != 6 || countOn("start", engines...) != 6 || countOn("stop", engines...) != 0 || countOn("remove", engines...) != 0 {
		t.Fatalf("creates %d, starts %d, stops %d, removes %d; want 6, 6, 0, 0",
			countOn("create", engines...), countOn("start", engines...), countOn("stop", engines...), countOn("remove", engines...))
	}
	now := workloadIDs(managedOn(a1.engine))
	for _, id := range running {
		if !slices.Contains(now, id) {
			t.Fatalf("replica %s, which ran from the start, was disturbed: %v", id, now)
		}
	}
	if v, _ := admin.progress("web"); v.Scaling.Message != "6 ready, 6 wanted" {
		t.Fatalf("scaling = %+v", v.Scaling)
	}
}

// The same request, sent again and again, in the middle of a scaling and after
// it, changes nothing: no new version, no new event, no new container.
func TestRepeatedScaleRequestsAreIdempotent(t *testing.T) {
	m := newManager(t, nil)
	m.start()
	a1 := m.startAgent("sim-1", nil)
	waitFor(t, "registration", func() bool { return m.sessionID("sim-1") != "" })
	m.controlLoops()
	admin := newScaleAdmin(t, m, scheduler.Timing{}, 0)
	ctx := context.Background()

	admin.create("web", 1, "")
	waitFor(t, "one replica", admin.converged("web", 1, a1.engine))
	original := admin.workloads("web")[0]

	version := int64(1)
	for _, n := range []int{3, 1, 3} {
		changed := admin.scale("web", n)
		version++
		if changed.Changed == nil || !*changed.Changed || changed.Version != version {
			t.Fatalf("scale to %d = %+v, want a change to version %d", n, changed, version)
		}
		// Again at once, while the loops are still at work, and with the version it was given.
		for _, headers := range [][]string{nil, {"If-Match", fmt.Sprintf(`"%d"`, version)}, {"If-Match", `"1"`}} {
			again := admin.scale("web", n, headers...)
			if again.Changed == nil || *again.Changed || again.Version != version || again.Replicas != n {
				t.Fatalf("repeated scale to %d (headers %v) = %+v, want no change", n, headers, again)
			}
		}
		waitFor(t, fmt.Sprintf("%d replicas", n), admin.converged("web", n, a1.engine))
		// And again once it has converged.
		if again := admin.scale("web", n); again.Changed == nil || *again.Changed || again.Version != version {
			t.Fatalf("repeated scale to %d after convergence = %+v", n, again)
		}
		m.heartbeats("sim-1", 5)
		if got := managedOn(a1.engine); len(got) != n {
			t.Fatalf("scale to %d: %d containers", n, len(got))
		}
	}

	// Three changes were made; the nine repeats were nothing. The first replica never moved.
	if n := admin.countEvents("service.scaled"); n != 3 {
		t.Fatalf("%d service.scaled events, want 3", n)
	}
	// 1 at the start, 2 for the first scale-up, and 2 more for the second after two were removed.
	if creates, removes := a1.engine.Count("create"), a1.engine.Count("remove"); creates != 5 || removes != 2 || a1.engine.Count("start") != 5 {
		t.Fatalf("creates %d, starts %d, removes %d; want 5, 5, 2", creates, a1.engine.Count("start"), removes)
	}
	if n := admin.countEvents("replica.scheduled"); n != 5 {
		t.Fatalf("%d replicas scheduled, want 5", n)
	}
	w, err := m.store.GetWorkload(ctx, original.ID)
	if err != nil || w.Generation != 1 || !slices.Contains(workloadIDs(managedOn(a1.engine)), original.ID) {
		t.Fatalf("the first replica = %+v, %v; it should have run throughout", w, err)
	}
}

// A replica count can be changed while a node is away. The nodes that answer
// are used; what does not fit says why; when the node returns it removes what it
// was told to stop and takes the rest, and no replica runs on two nodes.
func TestScaleWhileANodeIsUnavailable(t *testing.T) {
	f := newFleet(t, "sim-1", "sim-2")
	admin := newScaleAdmin(t, f.m, failureTiming, failureLease)
	engines := []*dockerfake.Fake{f.engines["sim-1"], f.engines["sim-2"]}
	sim1, sim2 := f.engines["sim-1"], f.engines["sim-2"]

	admin.create("web", 2, "")
	waitFor(t, "two replicas", admin.converged("web", 2, engines...))
	if len(sim1.Managed()) != 1 || len(sim2.Managed()) != 1 {
		t.Fatalf("containers: sim-1 %d, sim-2 %d; the replicas should be spread", len(sim1.Managed()), len(sim2.Managed()))
	}
	stale := sim2.Managed()[0].WorkloadID
	createdOnSim2 := sim2.Count("create")

	// sim-2 drops off the network. It is first doubted and then given up on, and its replica starts on sim-1.
	f.cuts["sim-2"].cutOff()
	waitFor(t, "sim-2 to be unavailable and its replica to run on sim-1", func() bool {
		v, ok := admin.progress("web")
		return ok && f.nodeViews()["sim-2"].Health == "unavailable" && v.Scaling.State == "stable" && v.Scaling.Ready == 2 && len(sim1.Managed()) == 2
	})

	// More replicas are wanted: they go to sim-1, which has room for four, and sim-2 gets nothing.
	if v := admin.scale("web", 4); v.Changed == nil || !*v.Changed {
		t.Fatalf("answer = %+v", v)
	}
	waitFor(t, "four replicas on sim-1", func() bool {
		v, ok := admin.progress("web")
		return ok && v.Scaling.State == "stable" && v.Scaling.Ready == 4 && len(sim1.Managed()) == 4
	})
	if sim2.Count("create") != createdOnSim2 {
		t.Fatalf("sim-2 was given a replica although it does not answer: %v", sim2.Calls())
	}

	// More than sim-1 holds: two wait, and the reason says that sim-2 is unavailable.
	admin.scale("web", 6)
	waitFor(t, "two replicas to be blocked", admin.blocked("web", 2, 4))
	var svc struct {
		Status struct {
			Unschedulable []struct {
				Nodes []struct {
					NodeID string `json:"node_id"`
					Code   string `json:"code"`
				} `json:"nodes"`
			} `json:"unschedulable"`
		} `json:"status"`
	}
	if rec := admin.do(http.MethodGet, "/v1/services/web", ""); rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &svc) != nil {
		t.Fatalf("GET = %d %s", rec.Code, rec.Body)
	}
	reasons := map[string]string{}
	for _, n := range svc.Status.Unschedulable[0].Nodes {
		reasons[n.NodeID] = n.Code
	}
	if reasons["sim-2"] != "unavailable" || reasons["sim-1"] != "insufficient_resources" {
		t.Fatalf("reasons = %v", reasons)
	}

	// sim-2 returns: it removes the container it was told to stop and takes the two waiting replicas.
	f.cuts["sim-2"].heal()
	waitFor(t, "six replicas, four on sim-1 and two on sim-2", func() bool {
		v, ok := admin.progress("web")
		return ok && v.Scaling.State == "stable" && v.Scaling.Ready == 6 && len(sim1.Managed()) == 4 && len(sim2.Managed()) == 2
	})
	waitFor(t, "the records of the moved replica to go", admin.converged("web", 6, engines...))
	for _, c := range sim2.Managed() {
		if c.WorkloadID == stale {
			t.Fatalf("the replica that was moved still runs on sim-2: %+v", sim2.Managed())
		}
	}
	// No replica is on two nodes.
	seen := map[string]bool{}
	for _, id := range workloadIDs(managedOn(engines...)) {
		if seen[id] {
			t.Fatalf("workload %s has containers on two nodes", id)
		}
		seen[id] = true
	}
	// Four on sim-1 were created once; sim-2 created its first, then the two new ones.
	if sim1.Count("create") != 4 || sim2.Count("create") != 3 {
		t.Fatalf("creates: sim-1 %d, sim-2 %d; want 4 and 3", sim1.Count("create"), sim2.Count("create"))
	}
}
