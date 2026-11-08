//go:build etcdintegration

package integration

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/election"
	"github.com/dockfleet/dockfleet/internal/etcd"
	"github.com/dockfleet/dockfleet/internal/store"
)

// realEtcdWorld elects through a real etcd, named by DOCKFLEET_TEST_ETCD_ENDPOINTS
// (for example http://127.0.0.1:21234 after `make etcd-test-up`). Every test uses a
// key prefix of its own, so tests do not see each other's leaders. The fake etcd
// is what the other runs of these scenarios use; this run is what keeps it honest.
func realEtcdWorld(t *testing.T) haWorld {
	t.Helper()
	raw := os.Getenv("DOCKFLEET_TEST_ETCD_ENDPOINTS")
	if raw == "" {
		t.Skip("DOCKFLEET_TEST_ETCD_ENDPOINTS not set")
	}
	mem := store.NewMemory()
	return haWorld{
		store: func(string) store.Store { return mem },
		backend: func(string) election.Backend {
			c, err := etcd.New(etcd.Config{Endpoints: strings.Split(raw, ","), Timeout: 2 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(c.Close)
			return c
		},
		prefix: "/dockfleet-test/" + store.NewID(),
	}
}

func TestHAOnlyTheLeaderActsOnRealEtcd(t *testing.T)   { haOnlyTheLeaderActs(t, realEtcdWorld) }
func TestHALeadershipTransfersOnRealEtcd(t *testing.T) { haLeadershipTransfers(t, realEtcdWorld) }
func TestHAFailoverAfterACrashOnRealEtcd(t *testing.T) { haFailoverAfterACrash(t, realEtcdWorld) }
func TestHAPartitionedLeaderStopsOnRealEtcd(t *testing.T) {
	haPartitionedLeaderStopsIssuingInstructions(t, realEtcdWorld)
}
