package reconcile

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/election"
	"github.com/dockfleet/dockfleet/internal/store"
)

// scriptedFence is the election as a pass sees it: the epoch it is told to act
// under, or the reason it may not act, and the epochs it was told were replaced.
type scriptedFence struct {
	mu     sync.Mutex
	epoch  int64
	err    error
	fenced []int64
}

func (f *scriptedFence) set(epoch int64, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.epoch, f.err = epoch, err
}

func (f *scriptedFence) Epoch() (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.epoch, f.err
}

func (f *scriptedFence) Fenced(epoch int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fenced = append(f.fenced, epoch)
}

func (f *scriptedFence) told() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.fenced...)
}

// A pass acts under the epoch of the leader's term. Without the leadership it
// does not start; under an epoch that another manager's election has replaced it
// stops, deletes and records nothing, and says so; under the current epoch it
// works as it always did.
func testReplacedLeaderChangesNothing(t *testing.T, newStore func(t *testing.T) store.Store) {
	st := newStore(t)
	e := newEnv(t, st)
	e.node("n1")
	e.service("web", 3, "nginx:1.0")
	e.settle()
	e.scale("web", 1) // two replicas are to be stopped

	fence := &scriptedFence{}
	obs := &recordingObserver{}
	leader := New(Config{Store: st, InstanceID: "mgr-1", Timing: e.timing, Now: func() time.Time { return e.clock }, Fence: fence, Observer: obs})
	before := e.fingerprint()

	// Not the leader: the pass is refused before it reads anything.
	fence.set(0, election.ErrNotLeader)
	if _, err := leader.RunOnce(e.ctx()); !errors.Is(err, election.ErrNotLeader) {
		t.Fatalf("a pass of a manager that does not lead = %v, want ErrNotLeader", err)
	}

	// Another manager has been elected twice over; this one still believes it leads under epoch 1.
	_, _, err := st.BeginTerm(e.ctx(), "mgr-1", e.clock)
	e.must(err)
	_, _, err = st.BeginTerm(e.ctx(), "mgr-2", e.clock)
	e.must(err)
	fence.set(1, nil)
	if _, err := leader.RunOnce(e.ctx()); !errors.Is(err, store.ErrStaleLeader) {
		t.Fatalf("a pass under a replaced epoch = %v, want ErrStaleLeader", err)
	}
	if e.fingerprint() != before || e.events("replica.stopped") != 0 {
		t.Fatalf("a replaced leader changed the cluster:\n%s", e.describe())
	}
	if got := fence.told(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("the election was told %v was replaced, want [1]", got)
	}

	// In the run loop that is the end of its term, not a failed pass.
	loop := New(Config{Store: st, InstanceID: "mgr-1", Timing: e.timing, Now: func() time.Time { return e.clock }, Fence: fence, Observer: obs})
	loop.pass(e.ctx())
	if len(obs.seenPasses()) != 0 {
		t.Errorf("a pass refused for want of leadership was reported as a reconciliation pass: %+v", obs.seenPasses())
	}

	// Under the current epoch the same pass does what the replaced one could not.
	fence.set(2, nil)
	report, err := leader.RunOnce(e.ctx())
	if err != nil || report.Stopped != 2 {
		t.Fatalf("the new leader's pass = %+v, %v; want it to stop the two extra replicas", report, err)
	}
	if got := obs.seenPasses(); len(got) != 0 {
		t.Errorf("RunOnce reports nothing to the observer; only the loop does: %+v", got)
	}
}
