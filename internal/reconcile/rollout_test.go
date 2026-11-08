package reconcile

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/rollout"
	"github.com/dockfleet/dockfleet/internal/store"
)

// These tests run whole rollouts through the control loop: the scheduler
// creates replicas as far as the surge allows, the reconciler retires old ones
// as far as the unavailable limit allows, and simulated agents report. After
// every round they check the two promises of the policy: never more replicas
// meant to run than the wanted count plus max_surge, never fewer ready than the
// wanted count minus max_unavailable.

func healthSpec(image string) store.ServiceSpec {
	s := spec(image)
	s.HealthCheck = &store.HealthCheck{Command: []string{"true"}, IntervalSeconds: 5, TimeoutSeconds: 1, Retries: 1}
	return s
}

func (e *env) serviceWith(name string, replicas int, s store.ServiceSpec) store.Service {
	e.t.Helper()
	svc, err := e.st.CreateService(e.ctx(), store.NewService{ID: "svc-" + name, Name: name, Replicas: replicas, Spec: s, CreatedBy: "test", At: e.clock})
	e.must(err)
	return svc
}

func (e *env) deploySpec(name string, s store.ServiceSpec) {
	e.t.Helper()
	svc, err := e.st.GetServiceByName(e.ctx(), name)
	e.must(err)
	_, _, err = e.st.DeployService(e.ctx(), svc.ID, store.Deployment{Spec: s, CreatedBy: "test", At: e.clock})
	e.must(err)
}

// rollbackTo deploys an old revision's spec as a new revision, as the API does.
func (e *env) rollbackTo(name string, revision int) {
	e.t.Helper()
	svc, err := e.st.GetServiceByName(e.ctx(), name)
	e.must(err)
	rev, err := e.st.GetRevision(e.ctx(), svc.ID, revision)
	e.must(err)
	_, _, err = e.st.DeployService(e.ctx(), svc.ID, store.Deployment{
		Spec: rev.Spec, Message: fmt.Sprintf("rollback to revision %d", revision), CreatedBy: "test", At: e.clock,
	})
	e.must(err)
}

// policy sets a service's rollout policy; paused holds its rollouts.
func (e *env) policy(name string, unavailable, surge, deadline int, paused bool) {
	e.t.Helper()
	svc, err := e.st.GetServiceByName(e.ctx(), name)
	e.must(err)
	p := store.RolloutPolicy{MaxUnavailable: unavailable, MaxSurge: surge, ProgressDeadlineSeconds: deadline}
	if _, err := e.st.SetRollout(e.ctx(), svc.ID, p, paused, 0, e.clock); err != nil && !errors.Is(err, store.ErrUnchanged) {
		e.t.Fatal(err)
	}
}

func (e *env) pause(name string, paused bool) {
	e.t.Helper()
	svc, err := e.st.GetServiceByName(e.ctx(), name)
	e.must(err)
	if _, err := e.st.SetRollout(e.ctx(), svc.ID, svc.Rollout, paused, 0, e.clock); err != nil && !errors.Is(err, store.ErrUnchanged) {
		e.t.Fatal(err)
	}
}

func (e *env) rolloutOf(name string, revision int) store.Rollout {
	e.t.Helper()
	svc, err := e.st.GetServiceByName(e.ctx(), name)
	e.must(err)
	list, err := e.st.ListRollouts(e.ctx(), svc.ID)
	e.must(err)
	for _, r := range list {
		if r.Revision == revision {
			return r
		}
	}
	e.t.Fatalf("service %s has no rollout record for revision %d: %+v", name, revision, list)
	return store.Rollout{}
}

// census counts the replicas of a service that are meant to run and the ones among them that are ready.
func (e *env) census(name string) (alive, ready int) {
	for _, w := range e.workloads(name) {
		if w.DesiredState != store.DesiredRunning {
			continue
		}
		alive++
		if rollout.Ready(w) {
			ready++
		}
	}
	return alive, ready
}

// runWithin runs the control loop until done is true, and fails the test if, after any round, more replicas
// are meant to run than want plus the surge, or fewer are ready than want minus the unavailable limit. It
// returns the most replicas that were meant to run and the fewest that were ready.
func (e *env) runWithin(name string, want int, p store.RolloutPolicy, done func() bool) (peakAlive, lowReady int) {
	e.t.Helper()
	lowReady = want
	for round := 1; round <= 120; round++ {
		e.cycle()
		alive, ready := e.census(name)
		peakAlive, lowReady = max(peakAlive, alive), min(lowReady, ready)
		if alive > want+p.MaxSurge {
			e.t.Fatalf("round %d: %d replicas are meant to run, the surge allows %d:\n%s", round, alive, want+p.MaxSurge, e.describe())
		}
		if ready < want-p.MaxUnavailable {
			e.t.Fatalf("round %d: %d replicas are ready, at least %d must be:\n%s", round, ready, want-p.MaxUnavailable, e.describe())
		}
		if done() {
			return peakAlive, lowReady
		}
	}
	e.t.Fatalf("the rollout did not finish in 120 rounds:\n%s", e.describe())
	return peakAlive, lowReady
}

func (e *env) rolloutComplete(name string, revision int) func() bool {
	return func() bool { return e.rolloutOf(name, revision).State == store.RolloutComplete }
}

func (e *env) idsOf(ws []store.Workload) map[string]bool {
	out := map[string]bool{}
	for _, w := range ws {
		out[w.ID] = true
	}
	return out
}

// Two replicas cannot publish the same host port on one node. With the default
// policy (one extra replica, none missing) the new revision therefore cannot
// start beside the old one: the rollout waits, says why, and gives up nothing.
// Allowing one replica to be missing lets it through.
func testFixedHostPortNeedsMaxUnavailable(t *testing.T, newStore func(t *testing.T) store.Store) {
	e := newEnv(t, newStore(t))
	e.node("n1")
	withPort := func(image string) store.ServiceSpec {
		s := spec(image)
		s.Ports = []store.ServicePort{{ContainerPort: 80, Protocol: "tcp", HostPort: 18080}}
		return s
	}
	svc := e.serviceWith("web", 1, withPort("nginx:1.0"))
	e.policy("web", 0, 1, 60, false)
	e.settle()
	old := e.wantReplicas("web", 1, 1, "nginx:1.0")

	e.deploySpec("web", withPort("nginx:2.0"))
	e.settle()
	if list := e.workloads("web"); len(list) != 1 || list[0].ID != old[0].ID || list[0].Generation != 1 || e.events("replica.stopped") != 0 {
		t.Fatalf("the old replica was given up although the new one cannot start:\n%s", e.describe())
	}
	waiting, err := e.st.ListUnschedulable(e.ctx(), svc.ID)
	e.must(err)
	if len(waiting) != 1 || waiting[0].Revision != 2 || waiting[0].Code != "host_port_conflict" {
		t.Fatalf("explanation = %+v; the new replica should be reported blocked by the host port", waiting)
	}

	// Nothing happens, and after the deadline the rollout is reported failed, with the reason.
	e.elapse(70 * time.Second)
	e.settle()
	rec := e.rolloutOf("web", 2)
	if rec.State != store.RolloutFailed || rec.Reason != store.RolloutDeadlineExceeded ||
		!strings.Contains(rec.Message, "cannot be placed") || !strings.Contains(rec.Message, "host port") || !strings.Contains(rec.Message, "max_unavailable") {
		t.Fatalf("rollout = %+v", rec)
	}
	if list := e.workloads("web"); len(list) != 1 || list[0].ID != old[0].ID || e.events("replica.stopped") != 0 || e.events("rollout.failed") != 1 {
		t.Fatalf("a failed rollout gave something up:\n%s", e.describe())
	}

	// With one replica allowed to be missing the old one lets go of the port, once.
	e.policy("web", 1, 1, 60, false)
	e.settle()
	rolled := e.wantReplicas("web", 2, 1, "nginx:2.0")
	if rolled[0].ID == old[0].ID {
		t.Fatal("the new revision reused the old workload")
	}
	events, err := e.st.ListEvents(e.ctx(), store.EventFilter{ResourceType: "workload", Limit: store.MaxEventLimit})
	e.must(err)
	stopped := 0
	for _, ev := range events {
		if ev.Action == "replica.stopped" {
			stopped++
			if ev.ResourceID != old[0].ID || ev.Details["reason"] != ReasonSuperseded {
				t.Fatalf("event = %+v", ev)
			}
		}
	}
	if stopped != 1 || e.rolloutOf("web", 2).State != store.RolloutComplete || e.events("rollout.recovered") != 1 {
		t.Fatalf("%d replicas stopped, rollout %+v, %d recoveries", stopped, e.rolloutOf("web", 2), e.events("rollout.recovered"))
	}
}

// For each policy the rollout finishes, and the replicas that are meant to run
// and the ones that are ready stay within it after every round.
func testRollingUpdateStaysWithinThePolicy(t *testing.T, newStore func(t *testing.T) store.Store) {
	const want = 4
	for _, p := range []struct{ unavailable, surge int }{{0, 1}, {1, 0}, {1, 2}, {2, 1}, {0, 4}, {3, 0}} {
		t.Run(fmt.Sprintf("unavailable %d surge %d", p.unavailable, p.surge), func(t *testing.T) {
			e := newEnv(t, newStore(t))
			e.node("n1")
			e.node("n2")
			e.service("web", want, "nginx:1.0")
			e.settle()
			first := e.wantReplicas("web", 1, want, "nginx:1.0")

			policy := store.RolloutPolicy{MaxUnavailable: p.unavailable, MaxSurge: p.surge, ProgressDeadlineSeconds: 600}
			e.policy("web", p.unavailable, p.surge, 600, false)
			e.deploy("web", "nginx:2.0")
			peak, low := e.runWithin("web", want, policy, e.rolloutComplete("web", 2))
			e.settle()

			rolled := e.wantReplicas("web", 2, want, "nginx:2.0")
			was := e.idsOf(first)
			for _, w := range rolled {
				if was[w.ID] {
					t.Fatalf("replica %s is an old workload", w.Name)
				}
			}
			// The policy is used, not just respected: with no unavailable replicas allowed the
			// surge is used in full, and with no surge the unavailable limit is.
			if p.unavailable == 0 && peak != want+min(p.surge, want) {
				t.Errorf("peak %d replicas meant to run, want %d: the surge was not used", peak, want+min(p.surge, want))
			}
			if p.surge == 0 && low != want-p.unavailable {
				t.Errorf("lowest %d ready, want %d: the unavailable limit was not used", low, want-p.unavailable)
			}
			if e.events("replica.scheduled") != 2*want || e.events("replica.stopped") != want || e.events("replica.removed") != want {
				t.Errorf("%d scheduled, %d stopped, %d removed; want %d, %d and %d: every container created once, retired once",
					e.events("replica.scheduled"), e.events("replica.stopped"), e.events("replica.removed"), 2*want, want, want)
			}
			if rec := e.rolloutOf("web", 2); rec.State != store.RolloutComplete || rec.FinishedAt == nil || e.events("rollout.completed") != 2 {
				t.Errorf("rollout = %+v, %d completions (one for each revision)", rec, e.events("rollout.completed"))
			}
		})
	}
}

// New replicas that fail never take over: the old ones keep running, and once
// the deadline passes without progress the rollout is reported failed, with the
// replicas that failed named.
func testFailingReplacementsAreReported(t *testing.T, newStore func(t *testing.T) store.Store) {
	e := newEnv(t, newStore(t))
	e.node("n1")
	e.node("n2")
	e.service("web", 3, "nginx:1.0")
	e.policy("web", 0, 1, 30, false)
	e.settle()
	old := e.wantReplicas("web", 1, 3, "nginx:1.0")

	e.breaks = func(w store.Workload) bool { return w.Image == "bad:1" }
	e.deploy("web", "bad:1")
	e.settle()
	untouched := func() {
		t.Helper()
		alive, ready := e.census("web")
		for _, w := range e.workloads("web") {
			if w.ServiceRevision == 1 && (w.DesiredState != store.DesiredRunning || w.Generation != 1 || !rollout.Ready(w)) {
				t.Fatalf("an old replica was touched: %+v", w)
			}
		}
		if alive != 4 || ready != 3 || e.events("replica.stopped") != 0 {
			t.Fatalf("%d replicas meant to run, %d ready, %d stopped:\n%s", alive, ready, e.events("replica.stopped"), e.describe())
		}
	}
	untouched() // one replica of the new revision, failed; the three old ones as they were
	if rec := e.rolloutOf("web", 2); rec.State != store.RolloutProgressing {
		t.Fatalf("rollout = %+v; the deadline has not passed", rec)
	}

	e.elapse(40 * time.Second)
	e.settle()
	rec := e.rolloutOf("web", 2)
	if rec.State != store.RolloutFailed || rec.Reason != store.RolloutDeadlineExceeded || rec.FinishedAt != nil ||
		!strings.Contains(rec.Message, "no progress for 30s") || !strings.Contains(rec.Message, "1 replica failed: web-r2-1-") ||
		!strings.Contains(rec.Message, "exited with code 1") || !strings.Contains(rec.Message, "3 replicas of an older spec still run") {
		t.Fatalf("rollout = %+v", rec)
	}
	untouched()
	// It is reported once, however long it goes on.
	e.elapse(5 * time.Minute)
	e.settle()
	if e.events("rollout.failed") != 1 || e.rolloutOf("web", 2).State != store.RolloutFailed {
		t.Fatalf("%d failure events, rollout %+v", e.events("rollout.failed"), e.rolloutOf("web", 2))
	}
	untouched()
	for _, w := range old {
		if got, err := e.st.GetWorkload(e.ctx(), w.ID); err != nil || got.Generation != 1 {
			t.Fatalf("old replica %s = %+v, %v", w.Name, got, err)
		}
	}
}

// With no room for the extra replica and no replica allowed to be missing, the
// rollout waits: it says what for, gives nothing up, and is reported failed
// when the deadline passes. Allowing a missing replica lets it through.
func testRolloutWithoutRoom(t *testing.T, newStore func(t *testing.T) store.Store) {
	const want = 8 // eight replicas of 500m fill the 4000m node
	e := newEnv(t, newStore(t))
	e.node("n1")
	svc := e.service("web", want, "nginx:1.0")
	e.policy("web", 0, 1, 60, false)
	e.settle()
	old := e.wantReplicas("web", 1, want, "nginx:1.0")

	e.deploy("web", "nginx:2.0")
	e.settle()
	waiting, err := e.st.ListUnschedulable(e.ctx(), svc.ID)
	e.must(err)
	if len(waiting) != 1 || waiting[0].Revision != 2 || waiting[0].Code != "insufficient_resources" {
		t.Fatalf("explanation = %+v; the extra replica does not fit", waiting)
	}
	if len(e.workloads("web")) != want || e.events("replica.stopped") != 0 {
		t.Fatalf("an old replica was given up although the policy forbids it:\n%s", e.describe())
	}

	e.elapse(70 * time.Second)
	e.settle()
	rec := e.rolloutOf("web", 2)
	if rec.State != store.RolloutFailed || !strings.Contains(rec.Message, "1 replica cannot be placed") ||
		!strings.Contains(rec.Message, "lack CPU") || !strings.Contains(rec.Message, "max_unavailable is 0") {
		t.Fatalf("rollout = %+v", rec)
	}
	ids := e.idsOf(e.workloads("web"))
	for _, w := range old {
		if !ids[w.ID] {
			t.Fatalf("old replica %s is gone", w.Name)
		}
	}

	// One replica may be missing: the rollout goes on, one step at a time, never below seven ready.
	policy := store.RolloutPolicy{MaxUnavailable: 1, MaxSurge: 1, ProgressDeadlineSeconds: 60}
	e.policy("web", 1, 1, 60, false)
	e.runWithin("web", want, policy, e.rolloutComplete("web", 2))
	e.settle()
	e.wantReplicas("web", 2, want, "nginx:2.0")
	if e.events("rollout.recovered") != 1 || e.events("rollout.failed") != 1 {
		t.Fatalf("%d recoveries, %d failures; the rollout failed once and then recovered", e.events("rollout.recovered"), e.events("rollout.failed"))
	}
}

// A paused rollout does not add a replica or retire one, its deadline does not
// run, and resuming it counts from the resume. A new revision deployed while the
// service is paused waits for the resume.
func testPausedRolloutStandsStill(t *testing.T, newStore func(t *testing.T) store.Store) {
	e := newEnv(t, newStore(t))
	e.node("n1")
	e.node("n2")
	e.serviceWith("web", 3, healthSpec("nginx:1.0"))
	e.policy("web", 0, 1, 60, false)
	e.settle()
	policy := store.RolloutPolicy{MaxUnavailable: 0, MaxSurge: 1, ProgressDeadlineSeconds: 60}

	// The new replica's health check is still starting, so the rollout is held at its first step.
	e.healthOf = func(w store.Workload) string {
		if w.Image == "nginx:2.0" {
			return store.HealthStarting
		}
		return ""
	}
	e.deploySpec("web", healthSpec("nginx:2.0"))
	e.settle()
	if alive, ready := e.census("web"); alive != 4 || ready != 3 {
		t.Fatalf("%d meant to run, %d ready; want the surge replica not ready yet:\n%s", alive, ready, e.describe())
	}

	// Paused, the replica becomes ready; the rollout does not take the next step.
	e.pause("web", true)
	e.healthOf = nil
	e.settle()
	if alive, ready := e.census("web"); alive != 4 || ready != 4 || e.events("replica.stopped") != 0 || e.events("replica.scheduled") != 4 {
		t.Fatalf("%d meant to run, %d ready, %d stopped, %d scheduled; a paused rollout takes no step:\n%s",
			alive, ready, e.events("replica.stopped"), e.events("replica.scheduled"), e.describe())
	}
	if rec := e.rolloutOf("web", 2); !rec.Paused || rec.State != store.RolloutProgressing {
		t.Fatalf("rollout = %+v", rec)
	}

	// Twenty minutes pass: a paused rollout does not time out.
	e.elapse(20 * time.Minute)
	e.settle()
	if rec := e.rolloutOf("web", 2); rec.State != store.RolloutProgressing || e.events("rollout.failed") != 0 {
		t.Fatalf("rollout = %+v, %d failures; the deadline must not run while paused", rec, e.events("rollout.failed"))
	}
	if alive, _ := e.census("web"); alive != 4 {
		t.Fatalf("%d meant to run while paused:\n%s", alive, e.describe())
	}

	// Resumed, it goes on, and the minute it had been given starts at the resume.
	e.pause("web", false)
	e.runWithin("web", 3, policy, e.rolloutComplete("web", 2))
	e.settle()
	e.wantReplicas("web", 2, 3, "nginx:2.0")
	if e.events("rollout.failed") != 0 {
		t.Fatal("the rollout was reported failed after a long pause")
	}

	// Deploying while paused prepares the revision and starts nothing.
	e.pause("web", true)
	e.deploySpec("web", healthSpec("nginx:3.0"))
	e.settle()
	if alive, _ := e.census("web"); alive != 3 || e.events("replica.scheduled") != 6 {
		t.Fatalf("%d meant to run, %d scheduled in all; a paused service starts no rollout:\n%s", alive, e.events("replica.scheduled"), e.describe())
	}
	if rec := e.rolloutOf("web", 3); !rec.Paused || rec.State != store.RolloutProgressing {
		t.Fatalf("rollout = %+v", rec)
	}
	e.pause("web", false)
	e.runWithin("web", 3, policy, e.rolloutComplete("web", 3))
	e.settle()
	e.wantReplicas("web", 3, 3, "nginx:3.0")
}

// A replica with a health check is ready only when the check passes, so a new
// replica that is unhealthy blocks the retirement of the old ones until it
// recovers.
func testHealthGatesReadiness(t *testing.T, newStore func(t *testing.T) store.Store) {
	e := newEnv(t, newStore(t))
	e.node("n1")
	e.node("n2")
	e.serviceWith("web", 2, healthSpec("nginx:1.0"))
	e.policy("web", 0, 1, 30, false)
	e.settle()
	policy := store.RolloutPolicy{MaxUnavailable: 0, MaxSurge: 1, ProgressDeadlineSeconds: 30}

	e.healthOf = func(w store.Workload) string {
		if w.Image == "nginx:2.0" {
			return store.HealthUnhealthy
		}
		return ""
	}
	e.deploySpec("web", healthSpec("nginx:2.0"))
	e.settle()
	e.elapse(40 * time.Second)
	e.settle()
	rec := e.rolloutOf("web", 2)
	if rec.State != store.RolloutFailed || !strings.Contains(rec.Message, "1 replica fails its health check") {
		t.Fatalf("rollout = %+v", rec)
	}
	if alive, ready := e.census("web"); alive != 3 || ready != 2 || e.events("replica.stopped") != 0 {
		t.Fatalf("%d meant to run, %d ready, %d stopped; an unhealthy replacement must not retire anything:\n%s",
			alive, ready, e.events("replica.stopped"), e.describe())
	}

	// The check starts passing: the rollout recovers and finishes.
	e.healthOf = nil
	e.runWithin("web", 2, policy, e.rolloutComplete("web", 2))
	e.settle()
	e.wantReplicas("web", 2, 2, "nginx:2.0")
	if e.events("rollout.recovered") != 1 {
		t.Fatalf("%d recoveries, want 1", e.events("rollout.recovered"))
	}
}

// A rollback deploys the old spec as a new revision. The replicas that still
// run it are kept; only what ran the spec being left is retired.
func testRollbackKeepsMatchingReplicas(t *testing.T, newStore func(t *testing.T) store.Store) {
	t.Run("after a rollout that surged", func(t *testing.T) {
		e := newEnv(t, newStore(t))
		e.node("n1")
		e.node("n2")
		e.service("web", 3, "nginx:1.0")
		e.settle()
		old := e.wantReplicas("web", 1, 3, "nginx:1.0")

		e.breaks = func(w store.Workload) bool { return w.Image == "bad:1" }
		e.deploy("web", "bad:1")
		e.settle()
		scheduled := e.events("replica.scheduled") // the three, and the one that failed

		e.rollbackTo("web", 1)
		e.runWithin("web", 3, store.RolloutPolicy{MaxUnavailable: 0, MaxSurge: 1}, e.rolloutComplete("web", 3))
		e.settle()
		list := e.workloads("web")
		if len(list) != 3 {
			t.Fatalf("workloads after the rollback:\n%s", e.describe())
		}
		for i, w := range list {
			if w.ID != old[i].ID || w.Generation != 1 || w.DesiredState != store.DesiredRunning {
				t.Fatalf("replica %d was touched by the rollback: %+v", i+1, w)
			}
		}
		if e.events("replica.scheduled") != scheduled {
			t.Fatalf("the rollback created %d replicas; the old ones are the right ones", e.events("replica.scheduled")-scheduled)
		}
		if got := e.rolloutOf("web", 2).State; got != store.RolloutSuperseded {
			t.Errorf("the rollout that went wrong is %s, want superseded", got)
		}
		if got := e.rolloutOf("web", 3).State; got != store.RolloutComplete {
			t.Errorf("the rollback is %s, want complete", got)
		}
		if e.events("rollout.superseded") != 1 {
			t.Errorf("%d superseded events, want 1", e.events("rollout.superseded"))
		}
	})

	t.Run("after a rollout that gave up a replica", func(t *testing.T) {
		e := newEnv(t, newStore(t))
		e.node("n1")
		e.node("n2")
		e.service("web", 3, "nginx:1.0")
		e.policy("web", 1, 0, 600, false) // no surge: one old replica goes before a new one exists
		e.settle()
		old := e.wantReplicas("web", 1, 3, "nginx:1.0")

		e.breaks = func(w store.Workload) bool { return w.Image == "bad:1" }
		e.deploy("web", "bad:1")
		e.settle()
		if alive, ready := e.census("web"); alive != 3 || ready != 2 {
			t.Fatalf("%d meant to run, %d ready; want two old replicas and a failed new one:\n%s", alive, ready, e.describe())
		}

		// Back to the old spec: the two old replicas stay and one new replica takes the third place.
		e.rollbackTo("web", 1)
		e.runWithin("web", 3, store.RolloutPolicy{MaxUnavailable: 1, MaxSurge: 0}, e.rolloutComplete("web", 3))
		e.settle()
		list := e.workloads("web")
		if len(list) != 3 {
			t.Fatalf("workloads after the rollback:\n%s", e.describe())
		}
		kept := 0
		was := e.idsOf(old)
		for _, w := range list {
			if w.Image != "nginx:1.0" || w.DesiredState != store.DesiredRunning {
				t.Fatalf("replica %+v does not run the old spec", w)
			}
			if was[w.ID] {
				kept++
				if w.Generation != 1 {
					t.Fatalf("a replica that stayed was told something: %+v", w)
				}
			}
		}
		if kept != 2 {
			t.Fatalf("%d of the old replicas were kept, want 2:\n%s", kept, e.describe())
		}
	})
}

// With nothing of the old spec left to keep, a rollback is a rollout like any
// other: gradual, and within the policy.
func testRollbackAfterACompletedRollout(t *testing.T, newStore func(t *testing.T) store.Store) {
	e := newEnv(t, newStore(t))
	e.node("n1")
	e.node("n2")
	e.service("web", 3, "nginx:1.0")
	policy := store.RolloutPolicy{MaxUnavailable: 0, MaxSurge: 1, ProgressDeadlineSeconds: 600}
	e.policy("web", 0, 1, 600, false)
	e.settle()

	e.deploy("web", "nginx:2.0")
	e.runWithin("web", 3, policy, e.rolloutComplete("web", 2))
	e.settle()
	second := e.wantReplicas("web", 2, 3, "nginx:2.0")

	e.rollbackTo("web", 1)
	peak, _ := e.runWithin("web", 3, policy, e.rolloutComplete("web", 3))
	e.settle()
	back := e.wantReplicas("web", 3, 3, "nginx:1.0")
	was := e.idsOf(second)
	for _, w := range back {
		if was[w.ID] {
			t.Fatalf("replica %s is a workload of the revision that was rolled back", w.Name)
		}
	}
	if peak != 4 {
		t.Errorf("peak %d replicas meant to run, want 4: the rollback is as gradual as a rollout", peak)
	}
	svc, err := e.st.GetServiceByName(e.ctx(), "web")
	e.must(err)
	revs, err := e.st.ListRevisions(e.ctx(), svc.ID)
	e.must(err)
	if len(revs) != 3 || revs[2].Message != "rollback to revision 1" || revs[2].SpecHash != revs[0].SpecHash {
		t.Fatalf("revisions = %+v; a rollback is a new revision with the old spec", revs)
	}
	for i, state := range []string{store.RolloutComplete, store.RolloutComplete, store.RolloutComplete} {
		if got := e.rolloutOf("web", i+1).State; got != state {
			t.Errorf("rollout of revision %d is %s, want %s", i+1, got, state)
		}
	}
}

// A revision deployed before the previous one finished takes over: the earlier
// rollout is closed as superseded, and its replicas that are not ready go at once.
func testRolloutIsSuperseded(t *testing.T, newStore func(t *testing.T) store.Store) {
	e := newEnv(t, newStore(t))
	e.node("n1")
	e.node("n2")
	e.service("web", 2, "nginx:1.0")
	e.policy("web", 0, 1, 600, false)
	e.settle()
	policy := store.RolloutPolicy{MaxUnavailable: 0, MaxSurge: 1, ProgressDeadlineSeconds: 600}

	e.holdBack = func(w store.Workload) bool { return w.ServiceRevision == 2 } // revision 2 never comes up
	e.deploy("web", "nginx:2.0")
	e.settle()
	if alive, ready := e.census("web"); alive != 3 || ready != 2 {
		t.Fatalf("%d meant to run, %d ready:\n%s", alive, ready, e.describe())
	}

	e.deploy("web", "nginx:3.0")
	e.runWithin("web", 2, policy, e.rolloutComplete("web", 3))
	e.settle()
	e.wantReplicas("web", 3, 2, "nginx:3.0")
	if got := e.rolloutOf("web", 2).State; got != store.RolloutSuperseded {
		t.Fatalf("revision 2 is %s, want superseded", got)
	}
	if rec := e.rolloutOf("web", 2); rec.FinishedAt == nil || e.events("rollout.superseded") != 1 {
		t.Fatalf("rollout = %+v, %d events", rec, e.events("rollout.superseded"))
	}
}
