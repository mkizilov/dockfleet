package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/store"
)

// These tests cover the rolling-deployment part of the services API: the
// policy in the definition, pausing and resuming, rolling back, and the
// progress of a rollout as the API reports it. As in the scaling tests, the
// scheduler, the reconciler and simulated agents run beside the handlers,
// which only record intent.

func (e *serviceEnv) rollout(name string) rolloutView {
	e.t.Helper()
	rec := e.request(http.MethodGet, "/v1/services/"+name+"/rollout", "")
	if rec.Code != http.StatusOK {
		e.t.Fatalf("GET rollout of %s = %d: %s", name, rec.Code, rec.Body.String())
	}
	var v rolloutView
	decode(e.t, rec, &v)
	return v
}

// post sends a POST to a service's sub-resource and decodes a successful answer.
func (e *serviceEnv) post(name, action, body string, headers ...string) (*httptest.ResponseRecorder, rolloutView) {
	e.t.Helper()
	rec := e.request(http.MethodPost, "/v1/services/"+name+"/"+action, body, headers...)
	var v rolloutView
	if rec.Code == http.StatusOK {
		decode(e.t, rec, &v)
	}
	return rec, v
}

// until runs the control loop until done is true (at most 60 rounds).
func (e *serviceEnv) until(what string, done func() bool) {
	e.t.Helper()
	for i := 0; i < 60; i++ {
		e.cycle()
		if done() {
			return
		}
	}
	e.t.Fatalf("%s did not happen in 60 rounds", what)
}

func (e *serviceEnv) rolloutIs(name, state string, revision int) func() bool {
	return func() bool {
		v := e.rollout(name)
		return v.Rollout.State == state && v.Rollout.Revision == revision
	}
}

func problemFields(t *testing.T, rec *httptest.ResponseRecorder) []string {
	t.Helper()
	var out struct {
		Details problems `json:"details"`
	}
	decode(t, rec, &out)
	var fields []string
	for _, p := range out.Details {
		fields = append(fields, p.Field)
	}
	return fields
}

func hasField(fields []string, want string) bool {
	for _, f := range fields {
		if f == want {
			return true
		}
	}
	return false
}

func testRolloutPolicyInTheDefinition(t *testing.T, e *serviceEnv) {
	created := e.create(minimalService)
	if created.Rollout != store.DefaultRolloutPolicy() || created.Rollout != (store.RolloutPolicy{MaxUnavailable: 0, MaxSurge: 1, ProgressDeadlineSeconds: 600}) {
		t.Fatalf("default policy = %+v", created.Rollout)
	}

	// Fields left out keep the default; the policy is part of the service, not of a revision.
	const api = `{"name":"api","replicas":2,"spec":{"image":"nginx:1.27","cpu_millis":500,"memory_bytes":134217728},"rollout":{"max_unavailable":1,"max_surge":0}}`
	custom := e.create(api)
	if custom.Rollout != (store.RolloutPolicy{MaxUnavailable: 1, MaxSurge: 0, ProgressDeadlineSeconds: 600}) || custom.Revision != 1 {
		t.Fatalf("custom policy = %+v", custom.Rollout)
	}

	// An update that leaves the policy out leaves it alone; one that sets part of it changes that part.
	if rec := put(e, "web", serviceBodyWith("nginx:1.27", "2")); rec.Code != http.StatusOK || e.get("web").Version != 1 {
		t.Fatalf("an update without a policy = %d, version %d", rec.Code, e.get("web").Version)
	}
	rec := put(e, "web", `{"replicas":2,"spec":{"image":"nginx:1.27","cpu_millis":500,"memory_bytes":134217728},"rollout":{"max_surge":3}}`)
	var v serviceView
	decode(t, rec, &v)
	if rec.Code != http.StatusOK || v.Rollout != (store.RolloutPolicy{MaxUnavailable: 0, MaxSurge: 3, ProgressDeadlineSeconds: 600}) ||
		v.Version != 2 || v.Revision != 1 {
		t.Fatalf("setting max_surge = %d %+v", rec.Code, v)
	}
	if got := e.eventsOf("service.rollout_configured"); len(got) != 1 || got[0].Details["max_surge"] != "3" || got[0].Details["max_unavailable"] != "0" ||
		got[0].Details["progress_deadline_seconds"] != "600" || got[0].Details["name"] != "web" {
		t.Fatalf("rollout_configured events = %+v", got)
	}
	// The same policy again changes nothing.
	rec = put(e, "web", `{"replicas":2,"spec":{"image":"nginx:1.27","cpu_millis":500,"memory_bytes":134217728},"rollout":{"max_surge":3,"max_unavailable":0}}`)
	decode(t, rec, &v)
	if rec.Code != http.StatusOK || v.Version != 2 || e.countEvents("service.rollout_configured") != 1 {
		t.Fatalf("the same policy again = %d, version %d, %d events", rec.Code, v.Version, e.countEvents("service.rollout_configured"))
	}
	rec = put(e, "web", `{"replicas":2,"spec":{"image":"nginx:1.27","cpu_millis":500,"memory_bytes":134217728},"rollout":{"progress_deadline_seconds":30}}`)
	decode(t, rec, &v)
	if rec.Code != http.StatusOK || v.Rollout != (store.RolloutPolicy{MaxUnavailable: 0, MaxSurge: 3, ProgressDeadlineSeconds: 30}) {
		t.Fatalf("setting the deadline = %d %+v; the rest of the policy stays", rec.Code, v.Rollout)
	}

	// One request can change the policy, the spec and the count; each is recorded.
	rec = put(e, "web", `{"replicas":4,"spec":{"image":"nginx:1.28","cpu_millis":500,"memory_bytes":134217728},"rollout":{"max_unavailable":1}}`)
	decode(t, rec, &v)
	if rec.Code != http.StatusOK || v.Revision != 2 || v.Replicas != 4 || v.Rollout.MaxUnavailable != 1 || v.Rollout.MaxSurge != 3 {
		t.Fatalf("a combined update = %d %+v", rec.Code, v)
	}
	if e.countEvents("service.rollout_configured") != 3 || e.countEvents("service.deployed") != 1 || e.countEvents("service.scaled") != 1 {
		t.Fatalf("events: %v", e.events())
	}
	// What GET returns is a valid PUT body, the policy included.
	got := e.get("web")
	rec = put(e, "web", `{"replicas":4,"spec":`+specJSON(got.Spec)+`,"rollout":{"max_unavailable":`+strconv.Itoa(got.Rollout.MaxUnavailable)+
		`,"max_surge":`+strconv.Itoa(got.Rollout.MaxSurge)+`,"progress_deadline_seconds":`+strconv.Itoa(got.Rollout.ProgressDeadlineSeconds)+`}}`)
	decode(t, rec, &v)
	if rec.Code != http.StatusOK || v.Version != got.Version {
		t.Fatalf("writing the policy back = %d, version %d, want %d", rec.Code, v.Version, got.Version)
	}
	// A listing shows it too.
	var list struct {
		Services []serviceView `json:"services"`
	}
	decode(t, e.request(http.MethodGet, "/v1/services", ""), &list)
	if len(list.Services) != 2 || list.Services[0].Rollout != custom.Rollout {
		t.Fatalf("list = %+v", list.Services)
	}
}

func testRolloutPolicyValidation(t *testing.T, e *serviceEnv) {
	e.create(minimalService)
	cases := []struct{ name, rollout, field string }{
		{"negative unavailable", `{"max_unavailable":-1}`, "rollout.max_unavailable"},
		{"too many unavailable", `{"max_unavailable":1001}`, "rollout.max_unavailable"},
		{"negative surge", `{"max_surge":-1}`, "rollout.max_surge"},
		{"too much surge", `{"max_surge":1001}`, "rollout.max_surge"},
		{"a deadline of zero", `{"progress_deadline_seconds":0}`, "rollout.progress_deadline_seconds"},
		{"a deadline above a day", `{"progress_deadline_seconds":86401}`, "rollout.progress_deadline_seconds"},
		{"a string", `{"max_surge":"1"}`, "rollout.max_surge"},
		{"a fraction", `{"max_surge":1.5}`, "rollout.max_surge"},
		{"a percentage", `{"max_surge":"25%"}`, "rollout.max_surge"},
		{"an unknown field", `{"max_pods":1}`, "rollout.max_pods"},
		{"not an object", `"fast"`, "rollout"},
		{"neither limit, once applied to the defaults", `{"max_surge":0}`, "rollout"},
	}
	for _, tt := range cases {
		e.run(t, tt.name, func(t *testing.T) {
			body := `{"name":"other","replicas":1,"spec":{"image":"nginx:1.27","cpu_millis":500,"memory_bytes":134217728},"rollout":` + tt.rollout + `}`
			rec := e.request(http.MethodPost, "/v1/services", body)
			if rec.Code != http.StatusBadRequest || !hasField(problemFields(t, rec), tt.field) {
				t.Fatalf("POST = %d %s, want a 400 naming %s", rec.Code, rec.Body.String(), tt.field)
			}
			rec = put(e, "web", `{"replicas":2,"spec":{"image":"nginx:1.27","cpu_millis":500,"memory_bytes":134217728},"rollout":`+tt.rollout+`}`)
			if rec.Code != http.StatusBadRequest || !hasField(problemFields(t, rec), tt.field) {
				t.Fatalf("PUT = %d %s, want a 400 naming %s", rec.Code, rec.Body.String(), tt.field)
			}
			if v := e.get("web"); v.Version != 1 || v.Rollout != store.DefaultRolloutPolicy() {
				t.Fatalf("a refused policy changed the service: %+v", v)
			}
		})
	}
	if services, _ := e.st.ListServices(context.Background(), true); len(services) != 1 {
		t.Fatalf("a refused creation left a service behind: %+v", services)
	}
	e.wantEvents("service.created")

	// Applied to the policy a service has, the two limits can both end up zero.
	e.run(t, "neither limit, once applied to the policy the service has", func(t *testing.T) {
		const body = `{"replicas":2,"spec":{"image":"nginx:1.27","cpu_millis":500,"memory_bytes":134217728},"rollout":`
		if rec := put(e, "web", body+`{"max_unavailable":1,"max_surge":0}}`); rec.Code != http.StatusOK {
			t.Fatalf("PUT = %d: %s", rec.Code, rec.Body.String())
		}
		rec := put(e, "web", body+`{"max_unavailable":0}}`)
		if rec.Code != http.StatusBadRequest || !hasField(problemFields(t, rec), "rollout") || !strings.Contains(rec.Body.String(), "cannot both be 0") {
			t.Fatalf("PUT = %d: %s", rec.Code, rec.Body.String())
		}
		if v := e.get("web"); v.Rollout.MaxUnavailable != 1 || v.Rollout.MaxSurge != 0 {
			t.Fatalf("service = %+v", v)
		}
	})
	e.run(t, "the policy cannot be set through the read-only fields", func(t *testing.T) {
		rec := e.request(http.MethodPost, "/v1/services", `{"name":"x","replicas":1,"paused":true,"spec":{"image":"nginx:1.27","cpu_millis":500,"memory_bytes":134217728}}`)
		if rec.Code != http.StatusBadRequest || !hasField(problemFields(t, rec), "paused") {
			t.Fatalf("POST = %d: %s", rec.Code, rec.Body.String())
		}
	})
}

func testRolloutPauseResume(t *testing.T, e *serviceEnv) {
	e.addNode("n1", 4000, 8<<30)
	e.addNode("n2", 4000, 8<<30)
	e.create(webService(2, 500))
	e.until("the first rollout", e.rolloutIs("web", store.RolloutComplete, 1))

	rec, v := e.post("web", "pause", "")
	if rec.Code != http.StatusOK || !v.Paused || v.Changed == nil || !*v.Changed || v.Version != 2 || rec.Header().Get("ETag") != `"2"` {
		t.Fatalf("pause = %d %+v", rec.Code, v)
	}
	if got := e.eventsOf("service.paused"); len(got) != 1 || got[0].Actor != adminActor || got[0].Details["name"] != "web" {
		t.Fatalf("paused events = %+v", got)
	}
	// Pausing again, even with an out-of-date version, changes nothing and succeeds.
	for _, headers := range [][]string{nil, {"If-Match", `"1"`}, {"If-Match", `"2"`}} {
		rec, v = e.post("web", "pause", "", headers...)
		if rec.Code != http.StatusOK || !v.Paused || v.Changed == nil || *v.Changed || v.Version != 2 {
			t.Fatalf("pause again (%v) = %d %+v", headers, rec.Code, v)
		}
	}
	if e.countEvents("service.paused") != 1 {
		t.Fatal("a repeated pause was recorded")
	}
	// Resuming with a stale version is refused; the service stays paused.
	if rec, _ := e.post("web", "resume", "", "If-Match", `"1"`); rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("resume with a stale version = %d, want 412", rec.Code)
	}
	if !e.get("web").Status.Rollout.Paused {
		t.Fatal("the service was resumed by a refused request")
	}

	// A revision deployed while paused is prepared and starts nothing.
	put(e, "web", webUpdate("nginx:1.28", 2, 500))
	for i := 0; i < 5; i++ {
		e.cycle()
	}
	st := e.get("web").Status
	if len(st.Replicas) != 2 || st.Replicas[0].Revision != 1 || st.Rollout.State != rolloutPaused || !st.Rollout.Paused || st.Rollout.Revision != 2 ||
		!strings.HasPrefix(st.Rollout.Message, "paused; 0 of 2 replicas of revision 2 are ready") {
		t.Fatalf("status while paused = %+v", st)
	}
	if st.Rollout.DeadlineAt != nil {
		t.Fatalf("a paused rollout has a deadline: %+v", st.Rollout)
	}
	if view := e.rollout("web"); !view.Paused || view.Rollout.State != rolloutPaused || len(view.History) != 2 || view.History[0].Revision != 2 {
		t.Fatalf("rollout view = %+v", view)
	}

	// Resumed, it goes through.
	rec, v = e.post("web", "resume", "")
	if rec.Code != http.StatusOK || v.Paused || v.Changed == nil || !*v.Changed {
		t.Fatalf("resume = %d %+v", rec.Code, v)
	}
	e.until("the second rollout", e.rolloutIs("web", store.RolloutComplete, 2))
	for i := 0; i < 5; i++ {
		e.cycle()
	}
	for _, r := range e.get("web").Status.Replicas {
		if r.Revision != 2 {
			t.Fatalf("a replica of revision %d is left after the rollout: %+v", r.Revision, e.get("web").Status.Replicas)
		}
	}
	if rec, v = e.post("web", "resume", ""); rec.Code != http.StatusOK || *v.Changed || e.countEvents("service.resumed") != 1 {
		t.Fatalf("resuming again = %d %+v, %d events", rec.Code, v, e.countEvents("service.resumed"))
	}

	// Unknown and deleted services.
	if rec, _ := e.post("nobody", "pause", ""); rec.Code != http.StatusNotFound {
		t.Errorf("pausing an unknown service = %d, want 404", rec.Code)
	}
	if rec := e.request(http.MethodGet, "/v1/services/nobody/rollout", ""); rec.Code != http.StatusNotFound {
		t.Errorf("reading the rollout of an unknown service = %d, want 404", rec.Code)
	}
	e.request(http.MethodDelete, "/v1/services/web", "")
	if rec, _ := e.post("web", "pause", ""); rec.Code != http.StatusNotFound {
		t.Errorf("pausing a deleted service = %d, want 404", rec.Code)
	}
}

func testRolloutRollback(t *testing.T, e *serviceEnv) {
	// A service with one revision has nothing to go back to.
	e.create(strings.Replace(minimalService, `"name":"web"`, `"name":"solo"`, 1))
	if rec, _ := e.post("solo", "rollback", ""); rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "no earlier revision") {
		t.Fatalf("rollback of a service with one revision = %d: %s", rec.Code, rec.Body.String())
	}

	e.create(minimalService)                          // revision 1: nginx:1.27
	put(e, "web", serviceBodyWith("nginx:1.28", "2")) // revision 2
	put(e, "web", serviceBodyWith("nginx:1.29", "2")) // revision 3

	// By default it goes back to the latest revision with a different spec.
	rec, v := e.post("web", "rollback", "")
	if rec.Code != http.StatusOK || v.Changed == nil || !*v.Changed || v.RolledBackTo != 2 || v.Revision != 4 || rec.Header().Get("ETag") == "" {
		t.Fatalf("rollback = %d %+v", rec.Code, v)
	}
	if got := e.get("web"); got.Revision != 4 || got.Spec.Image != "nginx:1.28" {
		t.Fatalf("service after the rollback = %+v", got)
	}
	var revisions struct {
		Revisions []store.Revision `json:"revisions"`
	}
	decode(t, e.request(http.MethodGet, "/v1/services/web/revisions", ""), &revisions)
	if len(revisions.Revisions) != 4 || revisions.Revisions[3].Message != "rollback to revision 2" || revisions.Revisions[3].CreatedBy != adminActor ||
		revisions.Revisions[3].SpecHash != revisions.Revisions[1].SpecHash {
		t.Fatalf("revisions = %+v; a rollback is a new revision with the old spec", revisions.Revisions)
	}
	if got := e.eventsOf("service.rolled_back"); len(got) != 1 || got[0].Details["from_revision"] != "3" || got[0].Details["to_revision"] != "4" ||
		got[0].Details["target_revision"] != "2" || got[0].Details["image"] != "nginx:1.28" {
		t.Fatalf("rolled_back events = %+v", got)
	}

	// Naming the revision makes it repeatable: the service already runs that spec.
	rec, v = e.post("web", "rollback", `{"revision":2}`)
	if rec.Code != http.StatusOK || v.Changed == nil || *v.Changed || v.RolledBackTo != 2 || v.Revision != 4 {
		t.Fatalf("rollback to the revision it is on = %d %+v", rec.Code, v)
	}
	rec, v = e.post("web", "rollback", `{"revision":4}`)
	if rec.Code != http.StatusOK || *v.Changed {
		t.Fatalf("rollback to the current revision = %d %+v", rec.Code, v)
	}
	decode(t, e.request(http.MethodGet, "/v1/services/web/revisions", ""), &revisions)
	if len(revisions.Revisions) != 4 || e.countEvents("service.rolled_back") != 1 {
		t.Fatalf("a repeated rollback added a revision or an event")
	}

	// Another revision, by name; a stale version is refused unless nothing would change.
	version := e.get("web").Version
	if rec, _ := e.post("web", "rollback", `{"revision":1}`, "If-Match", `"1"`); rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("rollback with a stale version = %d, want 412", rec.Code)
	}
	if e.get("web").Version != version {
		t.Fatal("a refused rollback changed the service")
	}
	rec, v = e.post("web", "rollback", `{"revision":1}`, "If-Match", `"`+strconv.Itoa(int(version))+`"`)
	if rec.Code != http.StatusOK || !*v.Changed || v.RolledBackTo != 1 || v.Revision != 5 || e.get("web").Spec.Image != "nginx:1.27" {
		t.Fatalf("rollback to revision 1 = %d %+v", rec.Code, v)
	}

	// A rollback is the way out of a bad rollout, so it clears a pause.
	e.post("web", "pause", "")
	rec, v = e.post("web", "rollback", `{"revision":3}`)
	if rec.Code != http.StatusOK || !*v.Changed || v.Paused || e.get("web").Status.Rollout.Paused {
		t.Fatalf("rollback of a paused service = %d %+v", rec.Code, v)
	}

	// Bad requests change nothing.
	version = e.get("web").Version
	if rec, _ := e.post("web", "rollback", `{"revision":99}`); rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "revision 99 does not exist") {
		t.Errorf("unknown revision = %d: %s", rec.Code, rec.Body.String())
	}
	for name, body := range map[string]string{
		"zero": `{"revision":0}`, "negative": `{"revision":-1}`, "a string": `{"revision":"1"}`, "a fraction": `{"revision":1.5}`,
		"another field": `{"revision":1,"image":"x"}`, "no revision": `{}`, "an array": `[1]`, "null": `null`, "not JSON": `back`,
	} {
		if rec, _ := e.post("web", "rollback", body); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: rollback = %d, want 400: %s", name, rec.Code, rec.Body.String())
		}
	}
	if rec, _ := e.post("web", "rollback", `{"revision":1,"pad":"`+strings.Repeat("x", maxBodyBytes)+`"}`); rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("a huge body = %d, want 413", rec.Code)
	}
	if e.get("web").Version != version {
		t.Fatal("a refused rollback changed the service")
	}

	e.request(http.MethodDelete, "/v1/services/web", "")
	if rec, _ := e.post("web", "rollback", ""); rec.Code != http.StatusNotFound {
		t.Errorf("rollback of a deleted service = %d, want 404", rec.Code)
	}
}

// The progress of a rollout as the API reports it: the surge in use, the
// replicas ready, the deadline, the failure and its cause, and how a rollback
// ends it. The replicas that already run the spec rolled back to are kept.
func testRolloutProgressAndFailure(t *testing.T, e *serviceEnv) {
	e.addNode("n1", 4000, 8<<30)
	e.addNode("n2", 4000, 8<<30)
	e.create(webService(3, 500))
	e.until("the first rollout", e.rolloutIs("web", store.RolloutComplete, 1))
	first := e.rollout("web")
	if first.Rollout.Revision != 1 || first.Rollout.Ready != 3 || first.Rollout.Updated != 3 || first.Rollout.Outdated != 0 ||
		first.Rollout.Surge != 0 || first.Rollout.Unavailable != 0 || first.Rollout.FinishedAt == nil || first.Rollout.DeadlineAt != nil ||
		len(first.History) != 1 || first.Policy != store.DefaultRolloutPolicy() {
		t.Fatalf("rollout after the first deployment = %+v", first)
	}

	// A new revision: nothing has happened yet, and the API says what is going to.
	put(e, "web", `{"replicas":3,"spec":{"image":"nginx:1.28","cpu_millis":500,"memory_bytes":134217728},"rollout":{"progress_deadline_seconds":30}}`)
	st := e.rollout("web").Rollout
	if st.State != store.RolloutProgressing || st.Revision != 2 || st.Updated != 0 || st.Outdated != 3 || st.Desired != 3 ||
		st.Message != "0 of 3 replicas of revision 2 are ready; 3 replicas of an older spec still run" {
		t.Fatalf("rollout before anything happened = %+v", st)
	}
	// The first step: one extra replica, and nothing retired.
	e.placeOnce()
	st = e.rollout("web").Rollout
	if st.Updated != 1 || st.Ready != 0 || st.Outdated != 3 || st.Surge != 1 || st.Unavailable != 0 || !strings.Contains(st.Message, "1 replica is not ready yet: web-r2-1-") {
		t.Fatalf("rollout after the first step = %+v", st)
	}
	if view := e.get("web"); view.Status.Scaling.State != scalingUpdating || view.Status.RunningReplicas != 0 {
		t.Fatalf("scaling = %+v, running %d", view.Status.Scaling, view.Status.RunningReplicas)
	}
	e.until("the second rollout", e.rolloutIs("web", store.RolloutComplete, 2))
	for i := 0; i < 5; i++ {
		e.cycle()
	}
	second := e.rollout("web")
	if second.Rollout.Ready != 3 || second.Rollout.Outdated != 0 || second.Rollout.Surge != 0 || len(second.History) != 2 ||
		second.History[0].Revision != 2 || second.History[0].State != store.RolloutComplete || second.History[1].Revision != 1 {
		t.Fatalf("rollout after the second deployment = %+v", second)
	}

	// A revision whose containers fail never takes over: the old replicas stay, and
	// when the deadline passes the rollout is reported failed, with the replica that failed.
	e.failImage = "bad:1"
	put(e, "web", webUpdate("bad:1", 3, 500))
	for i := 0; i < 5; i++ {
		e.cycle()
	}
	st = e.rollout("web").Rollout
	progress := time.Time{}
	if st.ProgressAt != nil {
		progress = *st.ProgressAt
	}
	if st.State != store.RolloutProgressing || st.Revision != 3 || st.Updated != 1 || st.Ready != 0 || st.Outdated != 3 || st.DeadlineAt == nil ||
		!st.DeadlineAt.Equal(progress.Add(30*time.Second)) {
		t.Fatalf("rollout with a failing replacement = %+v", st)
	}
	if view := e.get("web"); view.Status.RunningReplicas != 0 || len(view.Status.Replicas) != 4 || view.Status.FailedReplicas != 1 {
		t.Fatalf("status = %+v; the old replicas are outdated and stay, the new one failed", view.Status)
	}
	e.clock = e.clock.Add(31 * time.Second)
	e.agents()
	for i := 0; i < 5; i++ {
		e.cycle()
	}
	failed := e.rollout("web")
	st = failed.Rollout
	if st.State != store.RolloutFailed || st.Reason != store.RolloutDeadlineExceeded || st.DeadlineAt != nil || st.FinishedAt != nil ||
		!strings.Contains(st.Message, "no progress for 30s") || !strings.Contains(st.Message, "1 replica failed: web-r3-1-") ||
		!strings.Contains(st.Message, "exited with code 1") {
		t.Fatalf("failed rollout = %+v", st)
	}
	if failed.History[0].State != store.RolloutFailed || failed.History[0].Reason != store.RolloutDeadlineExceeded || e.countEvents("rollout.failed") != 1 {
		t.Fatalf("history = %+v, %d failure events", failed.History, e.countEvents("rollout.failed"))
	}
	if got := e.get("web").Status.Rollout; got.State != store.RolloutFailed {
		t.Fatalf("the service view says %+v", got)
	}
	for _, r := range e.get("web").Status.Replicas {
		if r.Revision == 2 && (r.DesiredState != store.DesiredRunning || r.Generation != 1) {
			t.Fatalf("an old replica was touched by the failing rollout: %+v", r)
		}
	}

	// Rolling back returns to the spec the three old replicas still run: they are kept, only the failed replica goes.
	scheduled := e.countEvents("replica.scheduled")
	rec, v := e.post("web", "rollback", "")
	if rec.Code != http.StatusOK || !*v.Changed || v.RolledBackTo != 2 || v.Revision != 4 {
		t.Fatalf("rollback = %d %+v", rec.Code, v)
	}
	e.until("the rollback", e.rolloutIs("web", store.RolloutComplete, 4))
	for i := 0; i < 5; i++ {
		e.cycle()
	}
	done := e.rollout("web")
	replicas := e.get("web").Status.Replicas
	if len(replicas) != 3 || done.Rollout.Ready != 3 || done.Rollout.Outdated != 0 || e.countEvents("replica.scheduled") != scheduled {
		t.Fatalf("after the rollback: %+v, %d replicas created", replicas, e.countEvents("replica.scheduled")-scheduled)
	}
	for _, r := range replicas {
		if r.Revision != 2 || r.Generation != 1 || r.RestartCount != 0 {
			t.Fatalf("a replica was touched by the rollback: %+v", r)
		}
	}
	var states []string
	for _, h := range done.History {
		states = append(states, strconv.Itoa(h.Revision)+":"+h.State)
	}
	if want := "4:complete 3:superseded 2:complete 1:complete"; strings.Join(states, " ") != want {
		t.Fatalf("history = %v, want %s", states, want)
	}
}
