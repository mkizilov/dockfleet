//go:build e2e

package e2e

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// The manager is a stateless process over the database. It is killed (no
// shutdown, no goodbye) and started again: the agents notice it is gone and
// reconnect, nothing is created twice or stopped, and no node is mistaken for
// dead because the manager was away. Then it is stopped the way a supervisor
// stops it, and has to finish within the time it was given.
func TestManagerRestartKeepsTheFleetAndStopsGracefully(t *testing.T) {
	c := newCluster(t, clusterOpts{timing: restartTiming, nodes: []nodeSpec{{id: "node-1"}, {id: "node-2"}}})
	m := c.managers[0]
	api := m.client
	api.must(http.MethodPost, "/v1/services", createBody("web", 3, "app:1"), http.StatusCreated, nil)
	before := api.waitStable(30*time.Second, "web", 3)
	evBefore := api.events()

	// The image's HEALTHCHECK, the binary asking itself, over TLS with its own certificate.
	if code, out := m.healthcheck(); code != 0 {
		t.Fatalf("manager healthcheck = %d: %s", code, out)
	}
	for id, a := range c.agents {
		if code, out := a.healthcheck(); code != 0 {
			t.Fatalf("agent %s healthcheck = %d: %s", id, code, out)
		}
	}

	killedAt := time.Now()
	m.p.kill()
	for _, a := range c.agents {
		a.waitNotReady(15 * time.Second)
		if code, _ := a.healthcheck(); code == 0 {
			t.Errorf("agent %s says it is healthy with no manager", a.spec.id)
		}
	}
	if code, _ := m.healthcheck(); code == 0 {
		t.Error("the manager healthcheck succeeded with the manager dead")
	}
	// Longer than the time after which a silent node is given up on (12 s). The agents keep trying
	// to register, with a wait that doubles from 1 s (1, 3, 7, 15 s after the crash), so the first
	// attempt after this lands 2 s after the restart, well inside the 12 s the manager allows.
	time.Sleep(time.Until(killedAt.Add(13 * time.Second)))

	m.start()
	m.waitReady(30 * time.Second)
	for _, a := range c.agents {
		a.waitReady(60 * time.Second)
	}
	after := api.waitStable(30*time.Second, "web", 3)
	for id := range ids(before.running()) {
		if !ids(after.running())[id] {
			t.Errorf("replica %s did not survive the manager's restart", id)
		}
	}
	evAfter := api.events()
	// A node may be suspect for a moment while its agent finds the manager again (that stops new
	// placements and moves nothing); what must not happen is that anything is moved.
	for _, id := range []string{"node-1", "node-2"} {
		api.waitNode(30*time.Second, id, "healthy")
	}
	for _, action := range []string{"replica.scheduled", "replica.stopped", "node.unavailable"} {
		if got, was := countAction(evAfter, action), countAction(evBefore, action); got != was {
			t.Errorf("%d %q events after the restart, %d before: the outage changed something", got, action, was)
		}
	}

	// A supervisor's stop: SIGTERM, and a kill only if it has not finished in time.
	began := time.Now()
	code, graceful := m.p.terminate(15 * time.Second)
	if !graceful || code != 0 {
		t.Fatalf("SIGTERM: exit %d, graceful %v\n%s", code, graceful, tailLines(m.p.logs(), 12))
	}
	if took := time.Since(began); took > 8*time.Second {
		t.Errorf("the manager took %s to stop with DOCKFLEET_SHUTDOWN_TIMEOUT=5s", took)
	}
	logs := m.p.logs()
	for _, want := range []string{"shutdown requested", "manager stopped"} {
		if !strings.Contains(logs, want) {
			t.Errorf("the manager's log lacks %q", want)
		}
	}
	m.start()
	m.waitReady(30 * time.Second)
	api.waitStable(30*time.Second, "web", 3)
}

// The database crashes (an immediate shutdown, as on a power failure) and comes
// back. The manager says it is not ready while it cannot reach it, never answers
// a request with made-up data, is alive throughout, and recovers by itself;
// the fleet is as it was.
func TestDatabaseOutage(t *testing.T) {
	if pgSrv == nil {
		t.Skip("needs the PostgreSQL server this test run started itself, so that it can crash it")
	}
	c := newCluster(t, clusterOpts{timing: slowHeartbeats, nodes: []nodeSpec{{id: "node-1"}, {id: "node-2"}}})
	m := c.managers[0]
	api := m.client
	api.must(http.MethodPost, "/v1/services", createBody("web", 3, "app:1"), http.StatusCreated, nil)
	before := api.waitStable(30*time.Second, "web", 3)

	t.Cleanup(func() {
		if !pgSrv.p.running() {
			_ = pgSrv.start() // the next test, and the cleanup that drops this one's database, need it
		}
	})
	pgSrv.crash()
	eventually(t, 15*time.Second, "the manager to report that its store is unavailable", func() (bool, string) {
		code, raw, err := api.try(http.MethodGet, "/readyz", "")
		return err == nil && code == http.StatusServiceUnavailable && strings.Contains(string(raw), "store unavailable"), string(raw)
	})
	if code, out := m.healthcheck(); code == 0 {
		t.Errorf("the manager healthcheck succeeded without a database: %s", out)
	}
	if code, _, err := api.try(http.MethodGet, "/healthz", ""); err != nil || code != http.StatusOK {
		t.Errorf("/healthz = %d %v: liveness must not depend on the database, or a supervisor restarts a manager that only waits", code, err)
	}
	if code, raw, err := api.try(http.MethodGet, "/v1/services/web", ""); err == nil && code == http.StatusOK {
		t.Errorf("a service was served without a database: %s", raw)
	}
	time.Sleep(6 * time.Second) // longer than the time after which a silent node is given up on (4 s)

	if err := pgSrv.start(); err != nil {
		t.Fatal(err)
	}
	m.waitReady(30 * time.Second)
	for _, a := range c.agents {
		a.waitReady(60 * time.Second)
	}
	after := api.waitStable(40*time.Second, "web", 3)
	for id := range ids(before.running()) {
		if !ids(after.running())[id] {
			t.Errorf("replica %s did not survive the database outage", id)
		}
	}
	evs := api.events()
	for _, action := range []string{"node.unavailable", "replica.stopped"} {
		if n := countAction(evs, action); n != 0 {
			t.Errorf("%d %q events: the outage of the database made the manager give up on nodes that were fine", n, action)
		}
	}
	consistently(t, 2*time.Second, "the fleet at rest after the outage", func() (bool, string) {
		return stable(api.service("web"), 3)
	})
}
