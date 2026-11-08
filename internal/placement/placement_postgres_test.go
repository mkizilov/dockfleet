//go:build integration

package placement

import (
	"context"
	"sync"
	"testing"

	"github.com/dockfleet/dockfleet/internal/pgtest"
	"github.com/dockfleet/dockfleet/internal/store"
)

// These tests need a PostgreSQL server: set DOCKFLEET_TEST_DATABASE_URL (see
// pgtest). Each test gets its own throwaway database.

func openMigrated(t *testing.T, dbURL string) *store.Postgres {
	t.Helper()
	ctx := context.Background()
	p, err := store.OpenPostgres(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	if err := p.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return p
}

// The same suite that runs against the in-memory store, on PostgreSQL.
func TestPlacementWithPostgres(t *testing.T) {
	runPlacementTests(t, func(t *testing.T) store.Store {
		return openMigrated(t, pgtest.NewDatabase(t))
	})
}

// runConcurrently makes n passes at once, alternating between the schedulers,
// and returns the errors they reported.
func runConcurrently(n int, schedulers ...*Scheduler) []error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(s *Scheduler) {
			defer wg.Done()
			if _, err := s.RunOnce(context.Background()); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(schedulers[i%len(schedulers)])
	}
	wg.Wait()
	return errs
}

// Two managers share one database. Their passes queue on the placement lock, so
// each replica is placed once, nothing fails on the one-workload-per-slot rule,
// and a node is never promised more than it has.
func TestTwoManagersNeverPlaceTheSameReplicaTwice(t *testing.T) {
	dbURL := pgtest.NewDatabase(t)
	a := newEnv(t, openMigrated(t, dbURL))
	b := newEnv(t, openMigrated(t, dbURL), func(c *Config) { c.InstanceID = "mgr-2" })
	for _, id := range []string{"n1", "n2", "n3"} {
		a.node(id, 4000, 8*gib)
	}
	a.service("web", 12, spec(500))

	if errs := runConcurrently(8, a.sched, b.sched); len(errs) != 0 {
		t.Fatalf("passes failed: %v", errs)
	}
	placed := a.placed("web")
	perNode := map[string]int{}
	for _, node := range placed {
		perNode[node]++
	}
	if len(placed) != 12 || a.workloadCount() != 12 || len(a.events("replica.scheduled")) != 12 {
		t.Fatalf("placed %v: %d workloads and %d events for 12 replicas", placed, a.workloadCount(), len(a.events("replica.scheduled")))
	}
	for node, n := range perNode {
		if n != 4 {
			t.Errorf("node %s holds %d replicas, want 4 on each of 3 nodes: %v", node, n, perNode)
		}
	}
	if got := b.workloadCount(); got != 12 {
		t.Fatalf("the second manager sees %d workloads", got)
	}
}

func TestTwoManagersNeverOversubscribeANode(t *testing.T) {
	dbURL := pgtest.NewDatabase(t)
	a := newEnv(t, openMigrated(t, dbURL))
	b := newEnv(t, openMigrated(t, dbURL), func(c *Config) { c.InstanceID = "mgr-2" })
	a.node("n1", 1000, 4*gib)
	for _, name := range []string{"first", "second", "third"} {
		a.service(name, 1, spec(600))
	}

	if errs := runConcurrently(9, a.sched, b.sched); len(errs) != 0 {
		t.Fatalf("passes failed: %v", errs)
	}
	reserved, count := int64(0), 0
	all, err := a.st.ListWorkloads(a.ctx())
	a.must(err)
	for _, w := range all {
		reserved += w.CPUMillis
		count++
	}
	if count != 1 || reserved != 600 {
		t.Fatalf("%d workloads reserve %dm of a 1000m node; exactly one 600m replica fits", count, reserved)
	}
	var waiting int
	for _, name := range []string{"first", "second", "third"} {
		waiting += len(a.waiting(name))
	}
	if waiting != 2 {
		t.Fatalf("%d replicas are recorded as unschedulable, want 2", waiting)
	}
}

// What a stopped manager scheduled is still there for the next one.
func TestAssignmentsSurviveAManagerRestart(t *testing.T) {
	dbURL := pgtest.NewDatabase(t)
	first := newEnv(t, openMigrated(t, dbURL))
	first.node("n1", 1000, 4*gib)
	first.service("web", 3, spec(400))
	first.run()
	before := first.placed("web")
	waitingBefore := first.waiting("web")
	first.st.Close()

	second := newEnv(t, openMigrated(t, dbURL))
	if got := second.placed("web"); len(got) != 2 || got[1] != "n1" || got[2] != "n1" {
		t.Fatalf("after a restart: %v, before %v", got, before)
	}
	waitingAfter := second.waiting("web")
	if len(waitingAfter) != 1 || waitingAfter[0].Message != waitingBefore[0].Message || !waitingAfter[0].Since.Equal(waitingBefore[0].Since) {
		t.Fatalf("explanations after a restart: %+v, before %+v", waitingAfter, waitingBefore)
	}
	if report := second.run(); report.Placed != 0 || report.NewReasons != 0 {
		t.Fatalf("the new manager's first pass = %+v; the old one's work is complete", report)
	}
}
