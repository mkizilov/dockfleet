package reconcile

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/outage"
	"github.com/dockfleet/dockfleet/internal/store"
)

// downStore is a store that cannot be used: every transaction fails, as when the
// database is unreachable.
type downStore struct{ store.Store }

func (downStore) InTx(context.Context, func(store.Tx) error) error {
	return errors.New("the database is unreachable")
}

// A store outage longer than the time after which a silent node is given up on
// must not make the manager give up on the whole fleet when the store returns.
// While the store is down the agents' heartbeats cannot be recorded, so every
// node looks silent for the whole outage; the manager saw the passes fail and does
// not count that silence. The control run, without the record of outages, shows
// what would happen: every node is given up on and every replica moves.
func testAStoreOutageIsNotHeldAgainstTheNodes(t *testing.T, newStore func(t *testing.T) store.Store) {
	run := func(t *testing.T, tracked bool) (nodesGivenUp, replicasStopped int) {
		st := newStore(t)
		e := newEnv(t, st)
		if tracked {
			e.timing.Outages = outage.New()
			e.startManager()
		}
		e.node("n1")
		e.node("n2")
		e.service("web", 4, "nginx:1.0")
		e.settle()

		// The database is gone for ten minutes (the unavailable limit is two). Passes fail,
		// every five seconds, and the agents' heartbeats are not recorded.
		down := New(Config{Store: downStore{st}, Interval: 5 * time.Second, Timing: e.timing, Now: func() time.Time { return e.clock }})
		for i := 0; i < 120; i++ {
			e.advance(5 * time.Second)
			down.pass(e.ctx())
		}

		// It returns, and the reconciler runs before any agent has been heard again.
		if _, err := e.rec.RunOnce(e.ctx()); err != nil {
			t.Fatal(err)
		}
		// A moment later the agents report as usual.
		e.elapse(10 * time.Second)
		_, err := e.rec.RunOnce(e.ctx())
		e.must(err)
		return e.events("node.unavailable"), e.events("replica.stopped")
	}

	t.Run("with the record of outages", func(t *testing.T) {
		given, stopped := run(t, true)
		if given != 0 || stopped != 0 {
			t.Fatalf("after the outage %d nodes were given up on and %d replicas stopped, want none", given, stopped)
		}
	})
	t.Run("without it, as before", func(t *testing.T) {
		given, stopped := run(t, false)
		if given != 2 || stopped == 0 {
			t.Fatalf("the control run gave up on %d nodes and stopped %d replicas; this test would not notice the fault it is about", given, stopped)
		}
	})

	// A node that really dies during an outage is still given up on, once the manager
	// has watched it be silent for as long as it tolerates.
	t.Run("a node that is really gone is found out afterwards", func(t *testing.T) {
		st := newStore(t)
		e := newEnv(t, st)
		e.timing.Outages = outage.New()
		e.startManager()
		e.node("n1")
		e.node("n2")
		e.service("web", 2, "nginx:1.0")
		e.settle()
		e.down["n2"] = true
		down := New(Config{Store: downStore{st}, Interval: 5 * time.Second, Timing: e.timing, Now: func() time.Time { return e.clock }})
		for i := 0; i < 60; i++ {
			e.advance(5 * time.Second)
			down.pass(e.ctx())
		}
		_, err := e.rec.RunOnce(e.ctx())
		e.must(err)
		if e.events("node.unavailable") != 0 {
			t.Fatal("a node was given up on for silence the manager could not have heard")
		}
		e.elapse(3 * time.Minute) // longer than the unavailable limit, with the store back
		_, err = e.rec.RunOnce(e.ctx())
		e.must(err)
		if e.events("node.unavailable") != 1 {
			t.Fatalf("the dead node was not given up on: %d events", e.events("node.unavailable"))
		}
	})
}
