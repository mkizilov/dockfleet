package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/docker"
	"github.com/dockfleet/dockfleet/internal/dockerfake"
	"github.com/dockfleet/dockfleet/internal/rollout"
	"github.com/dockfleet/dockfleet/internal/scheduler"
	"github.com/dockfleet/dockfleet/internal/store"
)

// These tests deploy new revisions of a service through the REST API of a
// running fleet: real agents and a real manager over gRPC, the scheduler and the
// reconciler running as in the manager process, and an in-memory Docker engine
// on each node. They look at what the engines were told to do, at what the API
// says along the way, and, from a sampler that reads the store while the
// rollout runs, at whether the policy's two promises held at every moment:
// never more replicas meant to run than the wanted count plus max_surge, never
// fewer ready than the wanted count minus max_unavailable.

const healthSpec = `,"health_check":{"command":["true"],"interval_seconds":1,"timeout_seconds":1,"retries":1}`

// rolloutJSON is the answer of GET /v1/services/{name}/rollout and of the POSTs next to it.
type rolloutJSON struct {
	Name         string `json:"name"`
	Revision     int    `json:"revision"`
	Version      int64  `json:"version"`
	Paused       bool   `json:"paused"`
	Changed      *bool  `json:"changed"`
	RolledBackTo int    `json:"rolled_back_to"`
	Policy       struct {
		MaxUnavailable          int `json:"max_unavailable"`
		MaxSurge                int `json:"max_surge"`
		ProgressDeadlineSeconds int `json:"progress_deadline_seconds"`
	} `json:"policy"`
	Rollout struct {
		Revision    int    `json:"revision"`
		State       string `json:"state"`
		Reason      string `json:"reason"`
		Message     string `json:"message"`
		Paused      bool   `json:"paused"`
		Desired     int    `json:"desired"`
		Updated     int    `json:"updated"`
		Ready       int    `json:"ready"`
		Outdated    int    `json:"outdated"`
		Surge       int    `json:"surge"`
		Unavailable int    `json:"unavailable"`
	} `json:"rollout"`
	History []struct {
		Revision int    `json:"revision"`
		State    string `json:"state"`
		Reason   string `json:"reason"`
	} `json:"history"`
}

// definition is a service definition without its name: replicas of an image,
// extra spec fields and extra top-level fields (for example the rollout policy).
func definition(replicas int, image, extraSpec, extraTop string) string {
	return fmt.Sprintf(`{"replicas":%d,"spec":{"image":%q,"cpu_millis":500,"memory_bytes":134217728%s}%s}`, replicas, image, extraSpec, extraTop)
}

func (a *scaleAdmin) createDefinition(name, def string) {
	a.t.Helper()
	body := fmt.Sprintf(`{"name":%q,`, name) + strings.TrimPrefix(def, "{")
	if rec := a.do(http.MethodPost, "/v1/services", body); rec.Code != http.StatusCreated {
		a.t.Fatalf("create %s = %d: %s", name, rec.Code, rec.Body)
	}
}

func (a *scaleAdmin) putDefinition(name, def string) {
	a.t.Helper()
	if rec := a.do(http.MethodPut, "/v1/services/"+name, def); rec.Code != http.StatusOK {
		a.t.Fatalf("update %s = %d: %s", name, rec.Code, rec.Body)
	}
}

func (a *scaleAdmin) rollout(name string) (v rolloutJSON, ok bool) {
	rec := a.do(http.MethodGet, "/v1/services/"+name+"/rollout", "")
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &v) != nil {
		return v, false
	}
	return v, true
}

func (a *scaleAdmin) rolloutNow(name string) rolloutJSON {
	a.t.Helper()
	v, ok := a.rollout(name)
	if !ok {
		a.t.Fatalf("no rollout view for %s", name)
	}
	return v
}

func (a *scaleAdmin) post(name, action, body string) (int, rolloutJSON) {
	a.t.Helper()
	rec := a.do(http.MethodPost, "/v1/services/"+name+"/"+action, body)
	var v rolloutJSON
	if rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
			a.t.Fatal(err)
		}
	}
	return rec.Code, v
}

// rolloutIs is true once the rollout of the revision is in the state.
func (a *scaleAdmin) rolloutIs(name, state string, revision int) func() bool {
	return func() bool {
		v, ok := a.rollout(name)
		return ok && v.Rollout.State == state && v.Rollout.Revision == revision
	}
}

// rolloutWatch samples a service's workloads while a test runs, and remembers
// the most replicas that were meant to run and the fewest that were ready.
type rolloutWatch struct {
	mu        sync.Mutex
	peak, low int
	once      sync.Once
	stop      chan struct{}
	done      chan struct{}
}

// watch starts sampling. Start it once the service has its wanted replicas
// ready, so that the lowest count is the rollout's and not the deployment's.
func (a *scaleAdmin) watch(name string, want int) *rolloutWatch {
	a.t.Helper()
	svc, err := a.m.store.GetServiceByName(context.Background(), name)
	if err != nil {
		a.t.Fatal(err)
	}
	w := &rolloutWatch{low: want, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		for {
			select {
			case <-w.stop:
				return
			default:
			}
			if ws, err := a.m.store.ListWorkloadsForService(context.Background(), svc.ID); err == nil {
				alive, ready := 0, 0
				for _, wl := range ws {
					if wl.DesiredState == store.DesiredRunning {
						alive++
						if rollout.Ready(wl) {
							ready++
						}
					}
				}
				w.mu.Lock()
				w.peak, w.low = max(w.peak, alive), min(w.low, ready)
				w.mu.Unlock()
			}
			time.Sleep(time.Millisecond)
		}
	}()
	a.t.Cleanup(w.halt)
	return w
}

func (w *rolloutWatch) halt() {
	w.once.Do(func() { close(w.stop) })
	<-w.done
}

// result stops the sampling and returns what it saw.
func (w *rolloutWatch) result() (peak, low int) {
	w.halt()
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.peak, w.low
}

// surgeNeverExceeded reads the audit trail in order: counting the replicas of
// the new revision that were scheduled against the replicas of the old one that
// were stopped, the first never ran ahead of the second by more than the surge.
// Unlike the sampler it cannot miss a moment.
func (a *scaleAdmin) surgeNeverExceeded(name string, oldRevision, newRevision, surge int) {
	a.t.Helper()
	svc, err := a.m.store.GetServiceByName(context.Background(), name)
	if err != nil {
		a.t.Fatal(err)
	}
	all, err := a.m.store.ListEvents(context.Background(), store.EventFilter{Limit: store.MaxEventLimit})
	if err != nil {
		a.t.Fatal(err)
	}
	ahead, worst := 0, 0
	for i := len(all) - 1; i >= 0; i-- { // oldest first
		e := all[i]
		if e.Details["service_id"] != svc.ID {
			continue
		}
		switch {
		case e.Action == "replica.scheduled" && e.Details["revision"] == strconv.Itoa(newRevision):
			ahead++
		case e.Action == "replica.stopped" && e.Details["revision"] == strconv.Itoa(oldRevision):
			ahead--
		}
		worst = max(worst, ahead)
	}
	if worst > surge {
		a.t.Fatalf("%d replicas of revision %d were scheduled before the same number of revision %d were stopped; the surge is %d",
			worst, newRevision, oldRevision, surge)
	}
}

func newRolloutFleet(t *testing.T, nodes ...string) (*manager, *scaleAdmin, []*dockerfake.Fake) {
	t.Helper()
	m := newManager(t, nil)
	m.start()
	var engines []*dockerfake.Fake
	for _, id := range nodes {
		engines = append(engines, m.startAgent(id, nil).engine)
	}
	for _, id := range nodes {
		waitFor(t, id+" to register", func() bool { return m.sessionID(id) != "" })
	}
	m.controlLoops()
	return m, newScaleAdmin(t, m, scheduler.Timing{}, 0), engines
}

func imagesOf(engines ...*dockerfake.Fake) []string {
	var out []string
	for _, c := range managedOn(engines...) {
		out = append(out, c.Image)
	}
	return out
}

func allImages(t *testing.T, engines []*dockerfake.Fake, want string) {
	t.Helper()
	for _, image := range imagesOf(engines...) {
		if image != want {
			t.Fatalf("containers run %v, want only %s", imagesOf(engines...), want)
		}
	}
}

// A new image replaces the running replicas a few at a time: at no moment are
// there more replicas than the wanted count plus the surge, or fewer ready than
// the wanted count minus the unavailable limit, and every container is created
// once and retired once.
func TestRollingUpdateReplacesGraduallyWithinThePolicy(t *testing.T) {
	const want = 4
	for _, p := range []struct{ unavailable, surge int }{{0, 1}, {1, 0}, {2, 2}} {
		t.Run(fmt.Sprintf("unavailable %d surge %d", p.unavailable, p.surge), func(t *testing.T) {
			m, admin, engines := newRolloutFleet(t, "sim-1", "sim-2")
			policy := fmt.Sprintf(`,"rollout":{"max_unavailable":%d,"max_surge":%d}`, p.unavailable, p.surge)
			admin.createDefinition("web", definition(want, "app:1", "", policy))
			waitFor(t, "four replicas", admin.converged("web", want, engines...))
			first := workloadIDs(managedOn(engines...))
			watch := admin.watch("web", want)

			admin.putDefinition("web", definition(want, "app:2", "", policy))
			waitFor(t, "the rollout to complete", admin.rolloutIs("web", store.RolloutComplete, 2))
			waitFor(t, "four replicas of the new image", admin.converged("web", want, engines...))
			m.heartbeats("sim-1", 10) // many more passes of every loop: nothing else may happen

			peak, low := watch.result()
			if peak > want+p.surge || low < want-p.unavailable {
				t.Fatalf("peak %d replicas meant to run (limit %d), lowest %d ready (limit %d)", peak, want+p.surge, low, want-p.unavailable)
			}
			admin.surgeNeverExceeded("web", 1, 2, p.surge)

			allImages(t, engines, "app:2")
			for _, id := range workloadIDs(managedOn(engines...)) {
				for _, old := range first {
					if id == old {
						t.Fatalf("old workload %s still has a container", id)
					}
				}
			}
			if creates, starts, removes := countOn("create", engines...), countOn("start", engines...), countOn("remove", engines...); creates != 2*want || starts != 2*want || removes != want {
				t.Fatalf("creates %d, starts %d, removes %d; want %d, %d and %d: every container created once and retired once", creates, starts, removes, 2*want, 2*want, want)
			}
			v := admin.rolloutNow("web")
			if v.Rollout.Ready != want || v.Rollout.Outdated != 0 || v.Rollout.Surge != 0 || v.Rollout.Unavailable != 0 || len(v.History) != 2 ||
				v.History[0].Revision != 2 || v.History[0].State != store.RolloutComplete || v.History[1].State != store.RolloutComplete {
				t.Fatalf("rollout = %+v", v)
			}
		})
	}
}

// Replacements that fail never take over. The old replicas keep running and are
// never touched; once the deadline passes without progress the rollout is
// reported failed, with the replica that failed. Rolling back keeps the old
// replicas, since they run the spec it returns to, and removes the failed one.
func TestFailingReplacementsAreReportedAndRolledBack(t *testing.T) {
	m, admin, engines := newRolloutFleet(t, "sim-1", "sim-2")
	for _, e := range engines {
		e.SetImageBehavior("bad:1", dockerfake.ImageBehavior{ExitCode: 1})
	}
	ctx := context.Background()

	admin.createDefinition("web", definition(3, "app:1", "", `,"rollout":{"progress_deadline_seconds":2}`))
	waitFor(t, "three replicas", admin.converged("web", 3, engines...))
	first := admin.workloads("web")
	watch := admin.watch("web", 3)

	// The new revision's containers exit as soon as they start, and are not restarted.
	admin.putDefinition("web", definition(3, "bad:1", `,"restart_policy":{"condition":"no"}`, ""))
	waitFor(t, "the rollout to be reported failed", admin.rolloutIs("web", store.RolloutFailed, 2))
	v := admin.rolloutNow("web")
	if v.Rollout.Reason != store.RolloutDeadlineExceeded || v.Rollout.Updated != 1 || v.Rollout.Ready != 0 || v.Rollout.Outdated != 3 ||
		!strings.Contains(v.Rollout.Message, "no progress for 2s") || !strings.Contains(v.Rollout.Message, "1 replica failed: web-r2-1-") ||
		!strings.Contains(v.Rollout.Message, "exited with code 1") {
		t.Fatalf("rollout = %+v", v.Rollout)
	}
	// Nothing old was given up: one container was created beside the three, and none was stopped or removed.
	if creates, stops, removes := countOn("create", engines...), countOn("stop", engines...), countOn("remove", engines...); creates != 4 || stops != 0 || removes != 0 {
		t.Fatalf("creates %d, stops %d, removes %d; want 4, 0 and 0", creates, stops, removes)
	}
	for _, w := range first {
		got, err := m.store.GetWorkload(ctx, w.ID)
		if err != nil || got.DesiredState != store.DesiredRunning || got.Generation != 1 || !rollout.Ready(got) {
			t.Fatalf("old replica %s = %+v, %v", w.Name, got, err)
		}
	}
	var svc struct {
		Status struct {
			FailedReplicas int `json:"failed_replicas"`
			Rollout        struct {
				State string `json:"state"`
			} `json:"rollout"`
		} `json:"status"`
	}
	if rec := admin.do(http.MethodGet, "/v1/services/web", ""); rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &svc) != nil ||
		svc.Status.FailedReplicas != 1 || svc.Status.Rollout.State != store.RolloutFailed {
		t.Fatalf("GET = %d %s", rec.Code, rec.Body)
	}
	if n := admin.countEvents("rollout.failed"); n != 1 {
		t.Fatalf("%d failure events, want 1", n)
	}

	// Back to the previous revision.
	code, back := admin.post("web", "rollback", "")
	if code != http.StatusOK || back.Changed == nil || !*back.Changed || back.RolledBackTo != 1 || back.Revision != 3 {
		t.Fatalf("rollback = %d %+v", code, back)
	}
	waitFor(t, "the rollback to complete", admin.rolloutIs("web", store.RolloutComplete, 3))
	waitFor(t, "three replicas", admin.converged("web", 3, engines...))
	peak, low := watch.result()
	if peak > 4 || low < 3 {
		t.Fatalf("peak %d replicas meant to run, lowest %d ready; the old replicas serve throughout", peak, low)
	}

	// The three replicas are the original three, never restarted; only the failed container went.
	now := workloadIDs(managedOn(engines...))
	for _, w := range first {
		found := false
		for _, id := range now {
			found = found || id == w.ID
		}
		if !found {
			t.Fatalf("old replica %s lost its container in the rollback: %v", w.Name, now)
		}
	}
	allImages(t, engines, "app:1")
	if creates, starts, removes := countOn("create", engines...), countOn("start", engines...), countOn("remove", engines...); creates != 4 || starts != 4 || removes != 1 {
		t.Fatalf("creates %d, starts %d, removes %d; want 4, 4 and 1: the rollback created nothing", creates, starts, removes)
	}
	done := admin.rolloutNow("web")
	var states []string
	for _, h := range done.History {
		states = append(states, strconv.Itoa(h.Revision)+":"+h.State)
	}
	if want := "3:complete 2:superseded 1:complete"; strings.Join(states, " ") != want {
		t.Fatalf("history = %v, want %s", states, want)
	}
	if n := admin.countEvents("service.rolled_back"); n != 1 {
		t.Fatalf("%d rollback events, want 1", n)
	}
	// Naming the revision makes the request repeatable.
	if code, again := admin.post("web", "rollback", `{"revision":1}`); code != http.StatusOK || again.Changed == nil || *again.Changed || again.Revision != 3 {
		t.Fatalf("repeated rollback = %d %+v", code, again)
	}
}

// With no room for the extra replica and no replica allowed to be missing, the
// rollout waits instead of breaking its policy: it says what it waits for, gives
// nothing up, and is reported failed when the deadline passes. Allowing one
// replica to be missing lets it through, one step at a time.
func TestRolloutWithoutRoomWaitsAndSaysSo(t *testing.T) {
	const want = 4 // four replicas of 500m fill a 2000m node
	m, admin, engines := newRolloutFleet(t, "sim-1")
	admin.createDefinition("web", definition(want, "app:1", "", `,"rollout":{"progress_deadline_seconds":2}`))
	waitFor(t, "four replicas", admin.converged("web", want, engines...))
	first := admin.workloads("web")
	watch := admin.watch("web", want)

	admin.putDefinition("web", definition(want, "app:2", "", ""))
	waitFor(t, "the rollout to be reported failed", admin.rolloutIs("web", store.RolloutFailed, 2))
	v := admin.rolloutNow("web")
	if v.Rollout.Updated != 0 || v.Rollout.Outdated != want || v.Rollout.Ready != 0 ||
		!strings.Contains(v.Rollout.Message, "1 replica cannot be placed") || !strings.Contains(v.Rollout.Message, "lack CPU") ||
		!strings.Contains(v.Rollout.Message, "max_unavailable is 0") {
		t.Fatalf("rollout = %+v", v.Rollout)
	}
	var svc struct {
		Status struct {
			Unschedulable []struct {
				Revision int    `json:"revision"`
				Code     string `json:"code"`
			} `json:"unschedulable"`
		} `json:"status"`
	}
	if rec := admin.do(http.MethodGet, "/v1/services/web", ""); rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &svc) != nil ||
		len(svc.Status.Unschedulable) != 1 || svc.Status.Unschedulable[0].Revision != 2 || svc.Status.Unschedulable[0].Code != "insufficient_resources" {
		t.Fatalf("GET = %d %s", rec.Code, rec.Body)
	}
	// The policy held: nothing was created beside the four, nothing was stopped.
	if creates, stops, removes := countOn("create", engines...), countOn("stop", engines...), countOn("remove", engines...); creates != want || stops != 0 || removes != 0 {
		t.Fatalf("creates %d, stops %d, removes %d; want %d, 0 and 0", creates, stops, removes, want)
	}
	for _, w := range first {
		got, err := m.store.GetWorkload(context.Background(), w.ID)
		if err != nil || got.DesiredState != store.DesiredRunning || got.Generation != 1 {
			t.Fatalf("old replica %s = %+v, %v", w.Name, got, err)
		}
	}

	// One replica may be missing: the rollout goes on, never below three ready.
	admin.putDefinition("web", definition(want, "app:2", "", `,"rollout":{"max_unavailable":1}`))
	waitFor(t, "the rollout to complete", admin.rolloutIs("web", store.RolloutComplete, 2))
	waitFor(t, "four replicas of the new image", admin.converged("web", want, engines...))
	peak, low := watch.result()
	if peak > want+1 || low < want-1 {
		t.Fatalf("peak %d replicas meant to run (limit %d), lowest %d ready (limit %d)", peak, want+1, low, want-1)
	}
	allImages(t, engines, "app:2")
	if admin.countEvents("rollout.failed") != 1 || admin.countEvents("rollout.recovered") != 1 {
		t.Fatalf("%d failures, %d recoveries; the rollout failed once and recovered once", admin.countEvents("rollout.failed"), admin.countEvents("rollout.recovered"))
	}
}

// A paused rollout stands still: no replica is added or retired, the deadline
// does not run, and resuming carries it through.
func TestPausedRolloutStandsStillUntilResumed(t *testing.T) {
	_, admin, engines := newRolloutFleet(t, "sim-1", "sim-2")
	admin.createDefinition("web", definition(3, "app:1", healthSpec, `,"rollout":{"progress_deadline_seconds":2}`))
	waitFor(t, "three replicas", admin.converged("web", 3, engines...))
	watch := admin.watch("web", 3)

	// The new image's health check is still starting, so the rollout stops at its first step.
	for _, e := range engines {
		e.SetImageBehavior("app:2", dockerfake.ImageBehavior{Health: docker.HealthStarting})
	}
	admin.putDefinition("web", definition(3, "app:2", healthSpec, ""))
	waitFor(t, "the first new replica", func() bool {
		v, ok := admin.rollout("web")
		return ok && v.Rollout.Updated == 1 && v.Rollout.Ready == 0
	})
	if code, v := admin.post("web", "pause", ""); code != http.StatusOK || !v.Paused || v.Changed == nil || !*v.Changed {
		t.Fatalf("pause = %d %+v", code, v)
	}

	// Its health check starts passing. The replica is ready; the rollout takes no further step.
	for _, e := range engines {
		e.SetImageBehavior("app:2", dockerfake.ImageBehavior{})
		for _, c := range e.Managed() {
			if c.Image == "app:2" {
				e.SetHealth(c.WorkloadID, docker.HealthHealthy)
			}
		}
	}
	waitFor(t, "the new replica to be ready", func() bool {
		v, ok := admin.rollout("web")
		return ok && v.Rollout.Ready == 1
	})
	// Longer than the deadline: nothing moves, and nothing is reported failed.
	until := time.Now().Add(2500 * time.Millisecond)
	for time.Now().Before(until) {
		v := admin.rolloutNow("web")
		if v.Rollout.State != "paused" || !v.Paused || v.Rollout.Updated != 1 || v.Rollout.Outdated != 3 || len(managedOn(engines...)) != 4 {
			t.Fatalf("while paused: %+v with %d containers", v.Rollout, len(managedOn(engines...)))
		}
		time.Sleep(20 * time.Millisecond)
	}
	if creates, stops, removes := countOn("create", engines...), countOn("stop", engines...), countOn("remove", engines...); creates != 4 || stops != 0 || removes != 0 {
		t.Fatalf("while paused: creates %d, stops %d, removes %d; want 4, 0 and 0", creates, stops, removes)
	}
	if admin.countEvents("rollout.failed") != 0 {
		t.Fatal("a paused rollout was reported failed")
	}
	// Pausing again is harmless.
	if code, v := admin.post("web", "pause", ""); code != http.StatusOK || v.Changed == nil || *v.Changed {
		t.Fatalf("pause again = %d %+v", code, v)
	}

	// Resumed, the rollout goes through, and the time it was paused did not count against it.
	if code, v := admin.post("web", "resume", ""); code != http.StatusOK || v.Paused || v.Changed == nil || !*v.Changed {
		t.Fatalf("resume = %d %+v", code, v)
	}
	waitFor(t, "the rollout to complete", admin.rolloutIs("web", store.RolloutComplete, 2))
	waitFor(t, "three replicas of the new image", admin.converged("web", 3, engines...))
	peak, low := watch.result()
	if peak > 4 || low < 3 {
		t.Fatalf("peak %d replicas meant to run, lowest %d ready; want at most 4 and at least 3", peak, low)
	}
	allImages(t, engines, "app:2")
	if admin.countEvents("rollout.failed") != 0 || admin.countEvents("service.paused") != 1 || admin.countEvents("service.resumed") != 1 {
		t.Fatalf("%d failures, %d pauses, %d resumes", admin.countEvents("rollout.failed"), admin.countEvents("service.paused"), admin.countEvents("service.resumed"))
	}
}

// Rolling back after a rollout has finished is a rollout like any other: the
// previous spec is deployed again as a new revision, gradually, within the
// policy.
func TestRollbackAfterACompletedRolloutIsGradual(t *testing.T) {
	_, admin, engines := newRolloutFleet(t, "sim-1", "sim-2")
	admin.createDefinition("web", definition(3, "app:1", "", ""))
	waitFor(t, "three replicas", admin.converged("web", 3, engines...))
	admin.putDefinition("web", definition(3, "app:2", "", ""))
	waitFor(t, "the first rollout to complete", admin.rolloutIs("web", store.RolloutComplete, 2))
	waitFor(t, "three replicas of the new image", admin.converged("web", 3, engines...))
	allImages(t, engines, "app:2")
	watch := admin.watch("web", 3)

	code, v := admin.post("web", "rollback", "")
	if code != http.StatusOK || v.Changed == nil || !*v.Changed || v.RolledBackTo != 1 || v.Revision != 3 {
		t.Fatalf("rollback = %d %+v", code, v)
	}
	waitFor(t, "the rollback to complete", admin.rolloutIs("web", store.RolloutComplete, 3))
	waitFor(t, "three replicas of the old image", admin.converged("web", 3, engines...))
	peak, low := watch.result()
	if peak > 4 || low < 3 {
		t.Fatalf("peak %d replicas meant to run, lowest %d ready; the rollback is as careful as any rollout", peak, low)
	}
	admin.surgeNeverExceeded("web", 2, 3, 1)
	allImages(t, engines, "app:1")
	// Three at the start, three for the second revision, three for the rollback; the first two sets are gone.
	if creates, removes := countOn("create", engines...), countOn("remove", engines...); creates != 9 || removes != 6 {
		t.Fatalf("creates %d, removes %d; want 9 and 6", creates, removes)
	}

	var revisions struct {
		Revisions []store.Revision `json:"revisions"`
	}
	if rec := admin.do(http.MethodGet, "/v1/services/web/revisions", ""); rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &revisions) != nil ||
		len(revisions.Revisions) != 3 || revisions.Revisions[2].Message != "rollback to revision 1" || revisions.Revisions[2].SpecHash != revisions.Revisions[0].SpecHash {
		t.Fatalf("revisions = %+v; a rollback is a new revision with the old spec", revisions.Revisions)
	}
	done := admin.rolloutNow("web")
	if len(done.History) != 3 || done.History[0].State != store.RolloutComplete || done.History[1].State != store.RolloutComplete {
		t.Fatalf("history = %+v", done.History)
	}
}
