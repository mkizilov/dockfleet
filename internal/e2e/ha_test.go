//go:build e2e

package e2e

import (
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"
)

// Two managers share the database and elect a leader through etcd. The leader
// is killed, then the new leader is stopped gracefully: each time the other
// takes over under a higher epoch, the agents find it, nothing that runs is
// touched, and the service can be scaled through whoever leads.
func TestLeaderFailover(t *testing.T) {
	c := newCluster(t, clusterOpts{managers: 2, ha: true, timing: haTiming, nodes: []nodeSpec{{id: "node-1"}, {id: "node-2"}}})
	first := c.waitLeader(40*time.Second, 1)
	second := c.managers[1-first.index]
	eventually(t, 20*time.Second, "the follower to know who leads", func() (bool, string) {
		v, ok := second.cluster()
		return ok && v.Role == "follower" && v.Leader == first.id && v.ControlPlane.Epoch == 1, fmt.Sprintf("%+v", v)
	})

	// Desired state is accepted by the follower: it goes to the shared database.
	second.client.must(http.MethodPost, "/v1/services", createBody("web", 3, "app:1"), http.StatusCreated, nil)
	v := second.client.waitStable(60*time.Second, "web", 3)
	original := ids(v.running())

	// 1. The leader is killed.
	crashedAt := time.Now()
	first.p.kill()
	second = c.waitLeader(60*time.Second, 2)
	t.Logf("failover after a crash took %s", time.Since(crashedAt).Round(100*time.Millisecond))
	if second == first {
		t.Fatal("the dead manager leads")
	}
	for _, a := range c.agents {
		a.waitReady(60 * time.Second)
	}
	v = second.client.waitStable(60*time.Second, "web", 3)
	for id := range original {
		if !ids(v.running())[id] {
			t.Errorf("replica %s was replaced by the failover", id)
		}
	}
	evs := second.client.events()
	if n := countAction(evs, "replica.stopped"); n != 0 {
		t.Errorf("%d replica.stopped events: the failover moved replicas", n)
	}
	if n := countAction(evs, "replica.scheduled"); n != 3 {
		t.Errorf("%d replica.scheduled events, want the original 3", n)
	}

	// The new leader decides: scaling through it places the new replicas.
	second.client.must(http.MethodPut, "/v1/services/web/scale", `{"replicas":5}`, http.StatusOK, nil)
	v = second.client.waitStable(60*time.Second, "web", 5)
	for _, e := range second.client.events() {
		if e.Action == "replica.scheduled" && e.Details["replica"] != "" {
			if n, _ := strconv.Atoi(e.Details["replica"]); n > 3 && e.InstanceID != second.id {
				t.Errorf("replica %d was placed by %s, not by the leader %s", n, e.InstanceID, second.id)
			}
		}
	}
	atFive := ids(v.running())

	// 2. The killed manager returns as a follower of the one that took over.
	first.start()
	first.waitReady(30 * time.Second)
	eventually(t, 30*time.Second, "the returning manager to follow", func() (bool, string) {
		v, ok := first.cluster()
		return ok && v.Role == "follower" && v.Leader == second.id && v.ControlPlane.Epoch == 2, fmt.Sprintf("%+v", v)
	})

	// 3. The leader is stopped the way a supervisor stops it: it gives up the lease, so the
	// other takes over quickly.
	handoffAt := time.Now()
	code, graceful := second.p.terminate(15 * time.Second)
	if !graceful || code != 0 {
		t.Fatalf("SIGTERM of the leader: exit %d, graceful %v\n%s", code, graceful, tailLines(second.p.logs(), 12))
	}
	third := c.waitLeader(60*time.Second, 3)
	if third != first {
		t.Fatalf("epoch 3 is led by %s, want the returning manager %s", third.id, first.id)
	}
	t.Logf("handoff after a clean stop took %s", time.Since(handoffAt).Round(100*time.Millisecond))
	for _, a := range c.agents {
		a.waitReady(60 * time.Second)
	}
	v = third.client.waitStable(60*time.Second, "web", 5)
	for id := range atFive {
		if !ids(v.running())[id] {
			t.Errorf("replica %s was replaced by the handoff", id)
		}
	}

	// The record of leadership is complete: consecutive epochs, each naming its predecessor.
	var elected []eventView
	for _, e := range third.client.events() {
		if e.Action == "leader.elected" {
			elected = append(elected, e)
		}
	}
	if len(elected) != 3 {
		t.Fatalf("%d leader.elected events, want 3: %+v", len(elected), elected)
	}
	wantLeader := []string{first.id, second.id, first.id}
	for i, e := range elected {
		prev := ""
		if i > 0 {
			prev = wantLeader[i-1]
		}
		if e.InstanceID != wantLeader[i] || e.Details["epoch"] != strconv.Itoa(i+1) || e.Details["previous_leader"] != prev {
			t.Errorf("election %d = %s %v, want %s at epoch %d replacing %q", i+1, e.InstanceID, e.Details, wantLeader[i], i+1, prev)
		}
	}
	if v, ok := third.cluster(); !ok || v.ControlPlane.Epoch != 3 || v.Epoch != 3 {
		t.Errorf("the leader's cluster view = %+v", v)
	}
}

// etcd goes away. No manager can show that it leads, so none does: the control
// plane stops deciding (the fleet keeps running). When etcd is back a leader is
// elected again and the service is as it was.
func TestEtcdOutageStopsTheLeaderUntilItReturns(t *testing.T) {
	if etcdSrv == nil {
		t.Skip("needs the etcd this test run started itself, so that it can stop it")
	}
	c := newCluster(t, clusterOpts{managers: 2, ha: true, timing: haTiming, nodes: []nodeSpec{{id: "node-1"}, {id: "node-2"}}})
	leader := c.waitLeader(40*time.Second, 1)
	leader.client.must(http.MethodPost, "/v1/services", createBody("web", 2, "app:1"), http.StatusCreated, nil)
	leader.client.waitStable(60*time.Second, "web", 2)

	etcdSrv.stop()
	t.Cleanup(func() {
		if !etcdSrv.p.running() {
			_ = etcdSrv.start()
		}
	})
	eventually(t, 30*time.Second, "no manager to lead without etcd", func() (bool, string) {
		var seen []string
		for _, m := range c.managers {
			v, ok := m.cluster()
			if !ok || v.Role == "leader" {
				return false, fmt.Sprintf("%s: %+v ok=%v", m.id, v, ok)
			}
			seen = append(seen, v.Role)
		}
		return true, fmt.Sprint(seen)
	})
	// A request that only the leader may handle is refused, and says so.
	for _, m := range c.managers {
		code, raw := m.client.do(http.MethodPost, "/v1/workloads", `{"name":"solo","image":"app:1","cpu_millis":100,"memory_bytes":67108864}`)
		if code != http.StatusServiceUnavailable {
			t.Errorf("%s: POST /v1/workloads without a leader = %d: %s", m.id, code, raw)
		}
	}

	if err := etcdSrv.start(); err != nil {
		t.Fatal(err)
	}
	leaderAgain := c.waitLeader(90*time.Second, 2)
	_ = leaderAgain
	for _, a := range c.agents {
		a.waitReady(90 * time.Second)
	}
	c.managers[0].client.waitStable(90*time.Second, "web", 2)
}
