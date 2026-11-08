//go:build etcdintegration

package etcd_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/election"
	"github.com/dockfleet/dockfleet/internal/etcd"
	"github.com/dockfleet/dockfleet/internal/store"
)

// These tests need a real etcd and are skipped unless DOCKFLEET_TEST_ETCD_ENDPOINTS
// lists its client URLs (for example http://127.0.0.1:21234 after `make etcd-test-up`).
// Every test uses a key of its own and removes its leases. Run them with
// `make test-etcd`. They run the same contract the fake meets.

func realEtcd(t *testing.T) *etcd.Client {
	t.Helper()
	raw := os.Getenv("DOCKFLEET_TEST_ETCD_ENDPOINTS")
	if raw == "" {
		t.Skip("DOCKFLEET_TEST_ETCD_ENDPOINTS not set")
	}
	c, err := etcd.New(etcd.Config{Endpoints: strings.Split(raw, ",")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestRealEtcdMeetsTheBackendContract(t *testing.T) {
	c := realEtcd(t)
	contract(t, func(t *testing.T) backendEnv {
		key := "/dockfleet-test/" + store.NewID() + "/leader"
		return backendEnv{
			backend: c, key: key,
			expire: func(t *testing.T, l election.Lease) {
				// etcd expires leases on its own clock, a little after their time.
				deadline := time.Now().Add(l.TTL + 10*time.Second)
				for time.Now().Before(deadline) {
					if _, ok, err := c.Observe(context.Background(), key); err == nil && !ok {
						return
					}
					time.Sleep(100 * time.Millisecond)
				}
				t.Fatalf("the key was still there %v after its lease should have expired", l.TTL+10*time.Second)
			},
		}
	})
}
