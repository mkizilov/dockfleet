package placement

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/store"
)

type observedPass struct {
	d      time.Duration
	report Report
	err    error
}

// recordingObserver is what the manager's metrics are in this test.
type recordingObserver struct {
	mu     sync.Mutex
	passes []observedPass
}

func (o *recordingObserver) SchedulingPass(d time.Duration, r Report, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.passes = append(o.passes, observedPass{d, r, err})
}

func (o *recordingObserver) seen() []observedPass {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]observedPass(nil), o.passes...)
}

// The loop tells its observer how every pass went: what it placed, what is
// newly stuck, and the error of a pass that failed. A pass that shutdown cut
// short is not a failure and is not reported.
func TestObserverSeesEveryPassOfTheLoop(t *testing.T) {
	st := store.NewMemory()
	obs := &recordingObserver{}
	e := newEnv(t, st, func(c *Config) { c.Observer = obs })
	e.node("n1", 1000, 4*gib)
	e.service("web", 2, spec(400))
	e.sched.pass(e.ctx())
	passes := obs.seen()
	if len(passes) != 1 || passes[0].err != nil || passes[0].report.Placed != 2 || passes[0].d < 0 {
		t.Fatalf("passes = %+v; want one that placed two replicas", passes)
	}

	e.service("big", 1, spec(5000))
	e.sched.pass(e.ctx())
	if passes = obs.seen(); len(passes) != 2 || passes[1].err != nil || passes[1].report.Placed != 0 || passes[1].report.NewReasons != 1 {
		t.Fatalf("passes = %+v; want one that placed nothing and found a new reason", passes)
	}

	// A replica waits that could be placed, but the pass cannot save its events.
	e.service("more", 1, spec(100))
	broken := New(Config{Store: failingEventStore{st}, NodeTTL: ttl, Now: func() time.Time { return t0 }, Observer: obs})
	broken.pass(e.ctx())
	if passes = obs.seen(); len(passes) != 3 || passes[2].err == nil {
		t.Fatalf("passes = %+v; a failed pass is reported with its error", passes)
	}
	canceled, cancel := context.WithCancel(e.ctx())
	cancel()
	broken.pass(canceled)
	if got := len(obs.seen()); got != 3 {
		t.Fatalf("%d passes reported; one that failed because the manager is shutting down is not a failure", got)
	}
}
