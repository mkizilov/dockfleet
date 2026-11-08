//go:build integration

package reconcile

import (
	"context"
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
func TestReconciliationWithPostgres(t *testing.T) {
	runReconcileTests(t, func(t *testing.T) store.Store {
		return openMigrated(t, pgtest.NewDatabase(t))
	})
}

// Two managers run the whole control loop against one database, at once. The
// placement lock makes their passes take turns, so the result is the same as
// with one manager: every replica exactly once, no duplicates, nothing lost.
func TestTwoManagersReconcileOneDatabase(t *testing.T) {
	dbURL := pgtest.NewDatabase(t)
	a := newEnv(t, openMigrated(t, dbURL))
	b := newEnv(t, openMigrated(t, dbURL))
	a.node("n1")
	a.node("n2")
	a.service("web", 4, "nginx:1.0")

	for i := 0; i < 12; i++ {
		done := make(chan error, 2)
		for _, e := range []*env{a, b} {
			go func() {
				if _, err := e.place.RunOnce(context.Background()); err != nil {
					done <- err
					return
				}
				_, err := e.rec.RunOnce(context.Background())
				done <- err
			}()
		}
		for range 2 {
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}
		a.agents()
	}
	a.wantReplicas("web", 1, 4, "nginx:1.0")
	if a.events("replica.scheduled") != 4 {
		t.Fatalf("scheduling events = %d; each replica must be placed once", a.events("replica.scheduled"))
	}

	a.scale("web", 1)
	for i := 0; i < 12; i++ {
		done := make(chan error, 2)
		for _, e := range []*env{a, b} {
			go func() {
				_, err := e.rec.RunOnce(context.Background())
				done <- err
			}()
		}
		for range 2 {
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}
		a.agents()
	}
	a.wantReplicas("web", 1, 1, "nginx:1.0")
	if a.events("replica.stopped") != 3 || a.events("replica.removed") != 3 {
		t.Fatalf("stopped %d, removed %d; each replica must be stopped and removed once", a.events("replica.stopped"), a.events("replica.removed"))
	}
}
