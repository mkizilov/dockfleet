package rollout

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/store"
)

var t0 = time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)

func policy(unavailable, surge, deadline int) store.RolloutPolicy {
	return store.RolloutPolicy{MaxUnavailable: unavailable, MaxSurge: surge, ProgressDeadlineSeconds: deadline}
}

func TestReady(t *testing.T) {
	running := func(w store.Workload) store.Workload {
		w.DesiredState, w.Generation = store.DesiredRunning, 2
		w.Observed = &store.Observation{State: store.WorkloadRunning, AckedGeneration: 2}
		return w
	}
	check := &store.HealthCheck{Command: []string{"true"}}
	tests := []struct {
		name string
		w    store.Workload
		want bool
	}{
		{"running and acknowledged", running(store.Workload{}), true},
		{"finished successfully counts", func() store.Workload {
			w := running(store.Workload{})
			w.Observed.State = store.WorkloadSucceeded
			return w
		}(), true},
		{"not reported yet", store.Workload{DesiredState: store.DesiredRunning, Generation: 1}, false},
		{"the report predates the latest instruction", func() store.Workload {
			w := running(store.Workload{})
			w.Generation = 3
			return w
		}(), false},
		{"pending", func() store.Workload {
			w := running(store.Workload{})
			w.Observed.State = store.WorkloadPending
			return w
		}(), false},
		{"failed", func() store.Workload {
			w := running(store.Workload{})
			w.Observed.State = store.WorkloadFailed
			return w
		}(), false},
		{"meant to be stopped", func() store.Workload {
			w := running(store.Workload{})
			w.DesiredState = store.DesiredStopped
			return w
		}(), false},
		{"without a check, a report of unhealthy still counts against it", func() store.Workload {
			w := running(store.Workload{})
			w.Observed.Health = store.HealthUnhealthy
			return w
		}(), false},
		{"with a check, healthy", func() store.Workload {
			w := running(store.Workload{})
			w.HealthCheck, w.Observed.Health = check, store.HealthHealthy
			return w
		}(), true},
		{"with a check, still starting", func() store.Workload {
			w := running(store.Workload{})
			w.HealthCheck, w.Observed.Health = check, store.HealthStarting
			return w
		}(), false},
		{"with a check, no result yet", func() store.Workload {
			w := running(store.Workload{})
			w.HealthCheck = check
			return w
		}(), false},
		{"with a check, unhealthy", func() store.Workload {
			w := running(store.Workload{})
			w.HealthCheck, w.Observed.Health = check, store.HealthUnhealthy
			return w
		}(), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Ready(tt.w); got != tt.want {
				t.Fatalf("Ready = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAvailableIgnoresNodesUnderSuspicion(t *testing.T) {
	w := store.Workload{
		NodeID: "n1", DesiredState: store.DesiredRunning, Generation: 1,
		Observed: &store.Observation{State: store.WorkloadRunning, AckedGeneration: 1},
	}
	for state, want := range map[string]bool{"": true, store.NodeHealthy: true, store.NodeSuspect: false, store.NodeUnavailable: false} {
		if got := Available(w, map[string]string{"n1": state}); got != want {
			t.Errorf("node %q: Available = %v, want %v", state, got, want)
		}
	}
}

// The three budgets, as a table over census, policy and pause. Excess and
// Stoppable are each worked out from the same census: the reconciler takes the
// excess first and then asks again for what the rollout may stop.
func TestBudgets(t *testing.T) {
	tests := []struct {
		name   string
		p      store.RolloutPolicy
		paused bool
		c      Census

		create, excess, stoppable int
	}{
		{"not rolling out: everything missing is created", policy(0, 1, 60), false,
			Census{Want: 3}, 3, 0, 0},
		{"the first step creates the surge", policy(0, 1, 60), false,
			Census{Want: 3, Old: 3, OldReady: 3}, 1, 0, 0},
		{"a larger surge creates more at once", policy(0, 2, 60), false,
			Census{Want: 3, Old: 3, OldReady: 3}, 2, 0, 0},
		{"the surge is limited by what is missing", policy(0, 5, 60), false,
			Census{Want: 3, Old: 3, OldReady: 3}, 3, 0, 0},
		{"no more while the surge is in use", policy(0, 1, 60), false,
			Census{Want: 3, Updated: 1, Old: 3, OldReady: 3}, 0, 0, 0},
		{"a ready new replica lets one old replica go", policy(0, 1, 60), false,
			Census{Want: 3, Updated: 1, UpdatedReady: 1, Old: 3, OldReady: 3}, 0, 0, 1},
		{"...which makes room for the next new replica", policy(0, 1, 60), false,
			Census{Want: 3, Updated: 1, UpdatedReady: 1, Old: 2, OldReady: 2}, 1, 0, 0},

		{"no surge: nothing is created before an old replica goes", policy(1, 0, 60), false,
			Census{Want: 3, Old: 3, OldReady: 3}, 0, 0, 1},
		{"...then one is", policy(1, 0, 60), false,
			Census{Want: 3, Old: 2, OldReady: 2}, 1, 0, 0},
		{"...and when it is ready, another old one goes", policy(1, 0, 60), false,
			Census{Want: 3, Updated: 1, UpdatedReady: 1, Old: 2, OldReady: 2}, 0, 0, 1},
		{"the unavailable limit counts ready replicas of both kinds", policy(2, 1, 60), false,
			Census{Want: 4, Updated: 1, UpdatedReady: 1, Old: 3, OldReady: 3}, 1, 0, 2},

		{"a pause adds no replica", policy(0, 1, 60), true,
			Census{Want: 3, Old: 3, OldReady: 3}, 0, 0, 0},
		{"...and retires none", policy(1, 1, 60), true,
			Census{Want: 3, Updated: 1, UpdatedReady: 1, Old: 3, OldReady: 3}, 0, 0, 0},
		{"...but a pause on an empty service still creates its replicas", policy(0, 1, 60), true,
			Census{Want: 3}, 3, 0, 0},
		{"...and replaces a replica that was lost", policy(0, 1, 60), true,
			Census{Want: 3, Updated: 0, Old: 2, OldReady: 2}, 1, 0, 0},

		{"more old replicas than the count: the rest go whatever the state", policy(0, 1, 60), false,
			Census{Want: 2, Old: 3, OldReady: 3}, 0, 1, 1},
		{"...also while paused", policy(0, 1, 60), true,
			Census{Want: 2, Old: 3, OldReady: 3}, 0, 1, 0},
		// A rollout over its surge (the policy was tightened) is not cut short: the old replicas go as the new ones are ready.
		{"over the surge, with every new replica ready: all the old ones may go", policy(0, 1, 60), false,
			Census{Want: 2, Updated: 2, UpdatedReady: 2, Old: 2, OldReady: 2}, 0, 0, 2},
		{"over the surge, with the new replicas not ready: none may", policy(0, 1, 60), false,
			Census{Want: 2, Updated: 2, Old: 2, OldReady: 2}, 0, 0, 0},

		{"the zero policy is the default policy", store.RolloutPolicy{}, false,
			Census{Want: 3, Old: 3, OldReady: 3}, 1, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CreateBudget(tt.p, tt.paused, tt.c); got != tt.create {
				t.Errorf("CreateBudget = %d, want %d", got, tt.create)
			}
			if got := Excess(tt.c); got != tt.excess {
				t.Errorf("Excess = %d, want %d", got, tt.excess)
			}
			if got := Stoppable(tt.p, tt.paused, tt.c); got != tt.stoppable {
				t.Errorf("Stoppable = %d, want %d", got, tt.stoppable)
			}
		})
	}
}

// Walking a rollout step by step never exceeds the surge or goes below the
// unavailable limit, for any policy, which is the point of the budgets.
func TestAnyPolicyKeepsItsPromises(t *testing.T) {
	for want := 1; want <= 6; want++ {
		for unavailable := 0; unavailable <= 3; unavailable++ {
			for surge := 0; surge <= 3; surge++ {
				if unavailable == 0 && surge == 0 {
					continue
				}
				p := policy(unavailable, surge, 60)
				c := Census{Want: want, Old: want, OldReady: want}
				lowReady, peak := want, want
				for step := 0; step < 100 && (c.Old > 0 || c.UpdatedReady < want); step++ {
					// The scheduler creates what the surge allows; the reconciler retires what the unavailable
					// limit allows; then every new replica comes up.
					c.Updated += CreateBudget(p, false, c)
					stop := min(Excess(c)+Stoppable(p, false, c), c.OldReady)
					c.Old, c.OldReady = c.Old-stop, c.OldReady-stop
					// The moment after the stops, before the new replicas have come up.
					ready := c.Available()
					peak, lowReady = max(peak, c.Updated+c.Old), min(lowReady, ready)
					if c.Updated+c.Old > want+surge {
						t.Fatalf("want %d, unavailable %d, surge %d: %d replicas meant to run", want, unavailable, surge, c.Updated+c.Old)
					}
					if ready < want-unavailable {
						t.Fatalf("step %d, want %d, unavailable %d, surge %d: only %d ready", step, want, unavailable, surge, ready)
					}
					c.UpdatedReady = c.Updated
				}
				if c.Old != 0 || c.UpdatedReady != want {
					t.Errorf("want %d, unavailable %d, surge %d: the rollout stalled at %+v (peak %d, low %d)", want, unavailable, surge, c, peak, lowReady)
				}
			}
		}
	}
}

func workloadOf(name, node string, rev int, ready bool) store.Workload {
	w := store.Workload{
		ID: name, Name: name, NodeID: node, ServiceID: "svc", ServiceRevision: rev, ReplicaIndex: 1,
		DesiredState: store.DesiredRunning, Generation: 1,
	}
	if ready {
		w.Observed = &store.Observation{State: store.WorkloadRunning, AckedGeneration: 1}
	}
	return w
}

func definition(replicas, revision int, same ...int) store.Definition {
	return store.Definition{
		Service:  store.Service{ID: "svc", Name: "web", Replicas: replicas, CurrentRevision: revision, Rollout: policy(0, 1, 60)},
		SameSpec: same,
	}
}

func TestCountSortsReplicasBySpecAndNode(t *testing.T) {
	def := definition(3, 3, 1, 3) // revision 3 has revision 1's spec
	stopped := workloadOf("stopped", "n1", 1, true)
	stopped.DesiredState = store.DesiredStopped
	other := workloadOf("other-service", "n1", 3, true)
	other.ServiceID = "elsewhere"
	workloads := []store.Workload{
		workloadOf("a", "n1", 1, true),   // the right spec, ready
		workloadOf("b", "n1", 3, false),  // the right spec, not ready
		workloadOf("c", "n1", 2, true),   // another spec, ready
		workloadOf("d", "slow", 2, true), // another spec, ready, on a quiet node
		workloadOf("e", "dead", 2, true), // on a node that was given up on
		workloadOf("f", "dead", 3, true),
		stopped, other,
	}
	nodes := map[string]string{"slow": store.NodeSuspect, "dead": store.NodeUnavailable}
	got := Count(def, workloads, nodes)
	want := Census{Want: 3, Updated: 2, UpdatedReady: 1, Old: 2, OldReady: 1}
	if got != want {
		t.Fatalf("Count = %+v, want %+v", got, want)
	}
	// With no node states every node counts as healthy, and a node under suspicion does not
	// stop its replicas from being counted as meant to run.
	if got := Count(def, workloads, nil); got != (Census{Want: 3, Updated: 3, UpdatedReady: 2, Old: 3, OldReady: 3}) {
		t.Fatalf("Count without node states = %+v", got)
	}
}

func TestAssessAndDescribe(t *testing.T) {
	def := definition(3, 2)
	failed := workloadOf("web-r2-1", "n1", 2, true)
	failed.Observed = &store.Observation{State: store.WorkloadFailed, Message: "exited with code 1", AckedGeneration: 1}
	unhealthy := workloadOf("web-r2-2", "n1", 2, true)
	unhealthy.Observed.Health = store.HealthUnhealthy
	starting := workloadOf("web-r2-3", "n1", 2, false)
	workloads := []store.Workload{
		failed, unhealthy, starting,
		workloadOf("web-r1-1", "n1", 1, true), workloadOf("web-r1-2", "n1", 1, true), workloadOf("web-r1-3", "n1", 1, true),
	}
	waiting := []store.Unschedulable{
		{ServiceID: "svc", Revision: 2, ReplicaIndex: 4, Code: "insufficient_resources", Message: "0/1 nodes can run web#4: 1 node(s) lack CPU (needs 500m)"},
		{ServiceID: "svc", Revision: 1, ReplicaIndex: 9, Code: "x", Message: "an older revision's"},
		{ServiceID: "other", Revision: 2, ReplicaIndex: 1, Code: "x", Message: "another service's"},
	}
	f := Assess(def, workloads, waiting, nil)
	if f.Updated != 3 || f.UpdatedReady != 0 || f.Old != 3 || f.OldReady != 3 || f.Complete() {
		t.Fatalf("facts = %+v", f)
	}
	if !reflect.DeepEqual(f.Failed, []string{"web-r2-1 (exited with code 1)"}) || !reflect.DeepEqual(f.Unhealthy, []string{"web-r2-2"}) ||
		!reflect.DeepEqual(f.Starting, []string{"web-r2-3"}) || len(f.Waiting) != 1 || f.Waiting[0].ReplicaIndex != 4 {
		t.Fatalf("facts = %+v", f)
	}
	want := "0 of 3 replicas of revision 2 are ready; 3 replicas of an older spec still run; " +
		"1 replica cannot be placed: 0/1 nodes can run web#4: 1 node(s) lack CPU (needs 500m) " +
		"(the old replicas stay, since max_unavailable is 0 and max_surge is 1: free capacity, add nodes, or allow max_unavailable of 1 or more); " +
		"1 replica failed: web-r2-1 (exited with code 1); 1 replica fails its health check: web-r2-2; 1 replica is not ready yet: web-r2-3"
	if got := f.Describe(def.Service.Rollout, false); got != want {
		t.Fatalf("Describe =\n%s\nwant\n%s", got, want)
	}
	if got := f.Describe(policy(1, 1, 60), true); !strings.HasPrefix(got, "paused; 0 of 3") || strings.Contains(got, "the old replicas stay") {
		t.Fatalf("Describe with max_unavailable and a pause = %s", got)
	}

	// Naming is bounded.
	many := Facts{Revision: 2, Census: Census{Want: 5}, Failed: []string{"a", "b", "c", "d", "e"}}
	if got := many.Describe(policy(0, 1, 60), false); !strings.Contains(got, "5 replicas failed: a, b, c and 2 more") {
		t.Fatalf("Describe = %s", got)
	}

	done := Assess(definition(3, 2), []store.Workload{
		workloadOf("a", "n1", 2, true), workloadOf("b", "n1", 2, true), workloadOf("c", "n1", 2, true),
	}, nil, nil)
	if !done.Complete() || done.Describe(policy(0, 1, 60), false) != "revision 2 is rolled out: 3 of 3 replicas are ready" {
		t.Fatalf("complete facts = %+v: %s", done, done.Describe(policy(0, 1, 60), false))
	}
	if !(Facts{Census: Census{Want: 0}}).Complete() {
		t.Error("a service that wants nothing and has nothing is rolled out")
	}
}

func facts(updated, ready, old int) Facts {
	return Facts{ServiceID: "svc", Revision: 2, Census: Census{Want: 3, Updated: updated, UpdatedReady: ready, Old: old}}
}

// The record of a rollout: how it starts, what counts as progress, when it
// fails, how it recovers, what a pause does to the clock.
func TestAdvance(t *testing.T) {
	p := policy(0, 1, 60)
	at := func(sec int) time.Time { return t0.Add(time.Duration(sec) * time.Second) }

	// A new record starts at the facts it is first seen with.
	r, ev := Advance(nil, facts(0, 0, 3), p, false, at(0))
	if ev != "" || r.State != store.RolloutProgressing || r.ServiceID != "svc" || r.Revision != 2 || r.Old != 3 || !r.StartedAt.Equal(at(0)) || !r.ProgressAt.Equal(at(0)) {
		t.Fatalf("new record = %+v, %q", r, ev)
	}

	// Nothing changes: the record stays as it is, until the deadline.
	for _, sec := range []int{10, 30, 60} {
		next, ev := Advance(&r, facts(0, 0, 3), p, false, at(sec))
		if ev != "" || !next.Same(r) {
			t.Fatalf("after %ds: %+v, %q", sec, next, ev)
		}
	}
	failed, ev := Advance(&r, facts(0, 0, 3), p, false, at(61))
	if ev != EventFailed || failed.State != store.RolloutFailed || failed.Reason != store.RolloutDeadlineExceeded ||
		!strings.HasPrefix(failed.Message, "no progress for 1m0s: ") || failed.FinishedAt != nil {
		t.Fatalf("after the deadline: %+v, %q", failed, ev)
	}
	// It is reported once.
	again, ev := Advance(&failed, facts(0, 0, 3), p, false, at(500))
	if ev != "" || !again.Same(failed) {
		t.Fatalf("a failed rollout changed without progress: %+v, %q", again, ev)
	}

	// More replicas created, more ready, fewer old left: each is progress and moves the clock.
	for name, f := range map[string]Facts{"a replica created": facts(1, 0, 3), "a replica ready": facts(0, 1, 3), "an old replica retired": facts(0, 0, 2)} {
		next, ev := Advance(&r, f, p, false, at(50))
		if ev != "" || next.State != store.RolloutProgressing || !next.ProgressAt.Equal(at(50)) {
			t.Fatalf("%s: %+v, %q", name, next, ev)
		}
		if again, _ := Advance(&next, f, p, false, at(100)); again.State != store.RolloutProgressing {
			t.Fatalf("%s: failed 50s after the progress, deadline is 60s", name)
		}
		if again, _ := Advance(&next, f, p, false, at(111)); again.State != store.RolloutFailed {
			t.Fatalf("%s: not failed 61s after the progress", name)
		}
	}
	// A failed rollout that moves again recovers.
	recovered, ev := Advance(&failed, facts(1, 0, 3), p, false, at(600))
	if ev != EventRecovered || recovered.State != store.RolloutProgressing || recovered.Reason != "" || recovered.Message != "" || !recovered.ProgressAt.Equal(at(600)) {
		t.Fatalf("recovered = %+v, %q", recovered, ev)
	}

	// Coming and going is not progress: the best reached is what counts.
	best, _ := Advance(&r, facts(1, 1, 2), p, false, at(20))
	worse, _ := Advance(&best, facts(1, 0, 2), p, false, at(30))
	flapped, ev := Advance(&worse, facts(1, 1, 2), p, false, at(40))
	if ev != "" || !flapped.ProgressAt.Equal(at(20)) || flapped.Ready != 1 || flapped.Old != 2 {
		t.Fatalf("a replica that came back counted as progress: %+v", flapped)
	}

	// A pause stops the clock, resuming starts it again from the resume.
	paused, ev := Advance(&r, facts(0, 0, 3), p, true, at(10))
	if ev != "" || !paused.Paused || paused.State != store.RolloutProgressing {
		t.Fatalf("paused = %+v, %q", paused, ev)
	}
	still, ev := Advance(&paused, facts(0, 0, 3), p, true, at(10_000))
	if ev != "" || still.State != store.RolloutProgressing {
		t.Fatalf("a paused rollout timed out: %+v, %q", still, ev)
	}
	resumed, ev := Advance(&still, facts(0, 0, 3), p, false, at(10_100))
	if ev != "" || resumed.Paused || resumed.State != store.RolloutProgressing || !resumed.ProgressAt.Equal(at(10_100)) {
		t.Fatalf("resumed = %+v, %q", resumed, ev)
	}
	if late, _ := Advance(&resumed, facts(0, 0, 3), p, false, at(10_100+61)); late.State != store.RolloutFailed {
		t.Fatalf("a resumed rollout never times out: %+v", late)
	}

	// Complete is final, and says so once.
	done, ev := Advance(&r, facts(3, 3, 0), p, false, at(70))
	if ev != EventCompleted || done.State != store.RolloutComplete || done.FinishedAt == nil || !done.FinishedAt.Equal(at(70)) ||
		done.Ready != 3 || done.Old != 0 || !strings.Contains(done.Message, "is rolled out") {
		t.Fatalf("complete = %+v, %q", done, ev)
	}
	after, ev := Advance(&done, facts(3, 1, 2), p, false, at(9000))
	if ev != "" || !after.Same(done) {
		t.Fatalf("a complete rollout changed: %+v, %q", after, ev)
	}
	// A rollout that fails and then completes.
	fin, ev := Advance(&failed, facts(3, 3, 0), p, false, at(700))
	if ev != EventCompleted || fin.State != store.RolloutComplete || fin.Reason != "" {
		t.Fatalf("a failed rollout that completed = %+v, %q", fin, ev)
	}
	// A revision that is complete when first seen (a service with no replicas) is recorded complete.
	zero, ev := Advance(nil, Facts{ServiceID: "svc", Revision: 1}, p, false, at(0))
	if ev != EventCompleted || zero.State != store.RolloutComplete {
		t.Fatalf("an empty service = %+v, %q", zero, ev)
	}
}

func TestSupersede(t *testing.T) {
	open := store.Rollout{ServiceID: "svc", Revision: 2, State: store.RolloutProgressing, StartedAt: t0, ProgressAt: t0}
	closed, ok := Supersede(open, t0.Add(time.Hour))
	if !ok || closed.State != store.RolloutSuperseded || closed.FinishedAt == nil || !closed.FinishedAt.Equal(t0.Add(time.Hour)) {
		t.Fatalf("superseded = %+v, %v", closed, ok)
	}
	failed := open
	failed.State = store.RolloutFailed
	if c, ok := Supersede(failed, t0); !ok || c.State != store.RolloutSuperseded {
		t.Fatalf("a failed rollout can be superseded: %+v, %v", c, ok)
	}
	for _, state := range []string{store.RolloutComplete, store.RolloutSuperseded} {
		done := open
		done.State = state
		if c, ok := Supersede(done, t0.Add(time.Hour)); ok || c.State != state || c.FinishedAt != nil {
			t.Errorf("a %s rollout was changed: %+v, %v", state, c, ok)
		}
	}
}

// The rules are decisions, not actions: this package imports the standard
// library's pure parts and the store's data types, and never reads the clock.
// The scheduler pass and the reconciler call it from different managers, so it
// must give them the same answer from the same snapshot.
func TestRolloutStaysPure(t *testing.T) {
	allowed := map[string]bool{
		"fmt": true, "sort": true, "strings": true, "time": true,
		"github.com/dockfleet/dockfleet/internal/store": true,
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		checked++
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, spec := range parsed.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if !allowed[path] {
				t.Errorf("%s imports %s: rollout decisions must stay free of I/O, Docker and the clock", file, path)
			}
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "time" && sel.Sel.Name == "Now" {
					t.Errorf("%s reads the clock with time.Now; pass the time in instead", file)
				}
			}
			return true
		})
	}
	if checked < 1 {
		t.Fatalf("checked only %d source files; the test is not looking at the package", checked)
	}
}
