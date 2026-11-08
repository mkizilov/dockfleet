//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"

	"github.com/dockfleet/dockfleet/internal/etcdfake"
	"github.com/dockfleet/dockfleet/internal/pgtest"
	"github.com/dockfleet/dockfleet/internal/store"
)

// postgresWorld is what a real deployment shares: one PostgreSQL database, each
// manager with a connection pool of its own, and the fake etcd. It needs
// DOCKFLEET_TEST_DATABASE_URL like the other tests of the integration tag, and
// makes (and drops) a database of its own.
func postgresWorld(t *testing.T) haWorld {
	t.Helper()
	srv := etcdfake.New(nil)
	t.Cleanup(srv.Close)
	url := pgtest.NewDatabase(t)

	var mu sync.Mutex
	handles := map[string]store.Store{}
	open := func(name string) store.Store {
		mu.Lock()
		defer mu.Unlock()
		if s, ok := handles[name]; ok {
			return s
		}
		p, err := store.OpenPostgres(context.Background(), url)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		if len(handles) == 0 { // the first to open it makes the schema
			if err := p.Migrate(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		handles[name] = p
		return p
	}
	open("admin")
	return haWorld{store: open, backend: fakeEtcdBackend(t, srv), prefix: "/dockfleet-test"}
}

func TestHAOnlyTheLeaderActsOnPostgres(t *testing.T)   { haOnlyTheLeaderActs(t, postgresWorld) }
func TestHALeadershipTransfersOnPostgres(t *testing.T) { haLeadershipTransfers(t, postgresWorld) }
func TestHAFailoverAfterACrashOnPostgres(t *testing.T) { haFailoverAfterACrash(t, postgresWorld) }
func TestHAPartitionedLeaderStopsOnPostgres(t *testing.T) {
	haPartitionedLeaderStopsIssuingInstructions(t, postgresWorld)
}
