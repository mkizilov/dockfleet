package placement

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
// does not start; with an epoch that another manager's election has replaced it
// saves nothing, whatever it decided, and says so; under the current epoch it
// works as it always did.
func testReplacedLeaderSavesNothing(t *testing.T, newStore func(t *testing.T) store.Store) {
	st := newStore(t)
	fence := &scriptedFence{}
	e := newEnv(t, st, func(c *Config) { c.Fence = fence })
	e.node("n1", 8000, 8*gib)
	svc := e.service("web", 2, spec(500))

	// Not the leader: the pass is refused before it reads anything.
	fence.set(0, election.ErrNotLeader)
	if _, err := e.sched.RunOnce(e.ctx()); !errors.Is(err, election.ErrNotLeader) {
		t.Fatalf("a pass of a manager that does not lead = %v, want ErrNotLeader", err)
	}
	if e.workloadCount() != 0 || len(fence.told()) != 0 {
		t.Fatalf("a manager that does not lead placed %d workloads and reported %v fenced", e.workloadCount(), fence.told())
	}

	// Elected under epoch 1, the manager works as a single one does.
	_, _, err := st.BeginTerm(e.ctx(), "mgr-1", t0)
	e.must(err)
	fence.set(1, nil)
	if report := e.run(); report.Placed != 2 {
		t.Fatalf("report under the current epoch = %+v", report)
	}

	// Another manager is elected (epoch 2) while this one still believes it leads
	// under epoch 1, and a replica is wanted that it would place if it could.
	_, _, err = st.BeginTerm(e.ctx(), "mgr-2", t0)
	e.must(err)
	_, err = st.ScaleService(e.ctx(), svc.ID, 4, 0, t0)
	e.must(err)
	_, err = e.sched.RunOnce(e.ctx())
	if !errors.Is(err, store.ErrStaleLeader) {
		t.Fatalf("a pass under a replaced epoch = %v, want ErrStaleLeader", err)
	}
	if e.workloadCount() != 2 || len(e.waiting("web")) != 0 || len(e.events("replica.scheduled")) != 2 {
		t.Fatalf("a replaced leader saved something: %d workloads, %d explanations, %d events", e.workloadCount(), len(e.waiting("web")), len(e.events("replica.scheduled")))
	}
	if got := fence.told(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("the election was told %v was replaced, want [1]", got)
	}

	// The run loop treats that as the end of its term, not as a failure of scheduling.
	obs := &recordingObserver{}
	loop := New(Config{Store: st, Fence: fence, NodeTTL: ttl, InstanceID: "mgr-1", Now: func() time.Time { return e.clock }, Observer: obs})
	loop.pass(e.ctx())
	if got := obs.seen(); len(got) != 0 {
		t.Errorf("a pass refused for want of leadership was reported as a scheduling pass: %+v", got)
	}

	// The new leader, under epoch 2, places what is missing.
	fence.set(2, nil)
	if report := e.run(); report.Placed != 2 {
		t.Fatalf("the new leader's report = %+v", report)
	}
	if e.workloadCount() != 4 {
		t.Fatalf("%d workloads, want 4", e.workloadCount())
	}
}
