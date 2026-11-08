package reconcile

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/store"
)

// recordingObserver is what the manager's metrics are in these tests.
type recordingObserver struct {
	mu     sync.Mutex
	passes []observedPass
	failed []string
}

type observedPass struct {
	d      time.Duration
	report Report
	err    error
}

func (o *recordingObserver) ReconcilePass(d time.Duration, r Report, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.passes = append(o.passes, observedPass{d, r, err})
}

func (o *recordingObserver) RolloutFailed(service string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.failed = append(o.failed, service)
}

func (o *recordingObserver) seenPasses() []observedPass {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]observedPass(nil), o.passes...)
}

func (o *recordingObserver) failures() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.failed...)
}

// A rollout that is reported failed is told to the observer once, when the
// report is saved: not when a pass that would have saved it is rolled back, and
// not again on the passes after.
func testObserverIsToldOfAFailedRolloutOnceItIsSaved(t *testing.T, newStore func(t *testing.T) store.Store) {
	st := newStore(t)
	e := newEnv(t, st)
	obs := &recordingObserver{}
	now := func() time.Time { return e.clock }
	e.rec = New(Config{Store: st, InstanceID: "mgr-1", Timing: e.timing, Now: now, Observer: obs})
	e.node("n1")
	e.node("n2")
	e.service("web", 3, "nginx:1.0")
	e.policy("web", 0, 1, 30, false)
	e.settle()

	e.breaks = func(w store.Workload) bool { return w.Image == "bad:1" }
	e.deploy("web", "bad:1")
	e.settle()
	if got := obs.failures(); len(got) != 0 {
		t.Fatalf("failures %v reported before the deadline", got)
	}

	// The deadline passes; a pass that cannot save what it found changes nothing and tells nobody.
	e.elapse(40 * time.Second)
	broken := New(Config{Store: failingEventStore{st}, InstanceID: "mgr-1", Timing: e.timing, Now: now, Observer: obs})
	if _, err := broken.RunOnce(e.ctx()); err == nil {
		t.Fatal("a pass that cannot record its events must fail")
	}
	if got := obs.failures(); len(got) != 0 {
		t.Fatalf("a pass that was rolled back reported %v", got)
	}

	e.settle()
	if got := obs.failures(); !reflect.DeepEqual(got, []string{"web"}) {
		t.Fatalf("failures = %v, want web once", got)
	}
	e.elapse(5 * time.Minute)
	e.settle()
	if got := obs.failures(); !reflect.DeepEqual(got, []string{"web"}) {
		t.Fatalf("failures = %v; a rollout that stays failed is reported once", got)
	}
	if e.events("rollout.failed") != 1 {
		t.Fatalf("%d failure events; the observer and the audit trail must agree", e.events("rollout.failed"))
	}
}

// The loop tells its observer how every pass went, with the report of the pass
// and the error of a failed one, and says nothing about a pass that shutdown cut short.
func testObserverSeesEveryPassOfTheLoop(t *testing.T, newStore func(t *testing.T) store.Store) {
	st := newStore(t)
	e := newEnv(t, st)
	e.node("n1")
	e.service("web", 2, "nginx:1.0")
	e.settle()
	e.scale("web", 1)

	obs := &recordingObserver{}
	now := func() time.Time { return e.clock }
	loop := New(Config{Store: st, Timing: e.timing, Now: now, Observer: obs})
	loop.pass(e.ctx())
	passes := obs.seenPasses()
	if len(passes) != 1 || passes[0].err != nil || passes[0].report.Stopped != 1 || passes[0].d < 0 {
		t.Fatalf("passes = %+v; want one that stopped a replica", passes)
	}
	loop.pass(e.ctx())
	if passes = obs.seenPasses(); len(passes) != 2 || passes[1].err != nil || passes[1].report.Stopped != 0 {
		t.Fatalf("passes = %+v; a pass that stops nothing is reported too", passes)
	}

	// Now there is something to do that cannot be saved.
	e.scale("web", 0)
	broken := New(Config{Store: failingEventStore{st}, Timing: e.timing, Now: now, Observer: obs})
	broken.pass(e.ctx())
	if passes = obs.seenPasses(); len(passes) != 3 || passes[2].err == nil {
		t.Fatalf("passes = %+v; a failed pass is reported with its error", passes)
	}
	canceled, cancel := context.WithCancel(e.ctx())
	cancel()
	broken.pass(canceled)
	if got := len(obs.seenPasses()); got != 3 {
		t.Fatalf("%d passes reported; one that failed because the manager is shutting down is not a failure", got)
	}
	if got := obs.failures(); len(got) != 0 {
		t.Fatalf("failures = %v", got)
	}
}
