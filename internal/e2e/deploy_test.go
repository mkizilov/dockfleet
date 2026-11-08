//go:build e2e

package e2e

import (
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"
)

// A service is deployed, scaled up and down (twice to the same count, and past
// what the cluster can hold), updated in a rolling deployment and rolled back,
// through the REST API of real managers and agents over mutual TLS. Each step
// checks what the API says the cluster did, and that the replicas that had no
// reason to move did not.
func TestDeployScaleRolloutAndRollback(t *testing.T) {
	c := newCluster(t, clusterOpts{nodes: []nodeSpec{{id: "node-1"}, {id: "node-2", csr: true}}})
	api := c.managers[0].client

	for _, id := range []string{"node-1", "node-2"} {
		c.managers[0].client.waitNode(20*time.Second, id, "healthy")
	}

	// Deployment: two replicas, spread over the two nodes.
	api.must(http.MethodPost, "/v1/services", createBody("web", 2, "app:1"), http.StatusCreated, nil)
	v := api.waitStable(30*time.Second, "web", 2)
	nodes := map[string]bool{}
	for _, r := range v.running() {
		nodes[r.NodeID] = true
		if r.Revision != 1 || r.RestartCount != 0 {
			t.Errorf("replica %+v", r)
		}
	}
	if len(nodes) != 2 {
		t.Fatalf("two replicas of one service ran on %d node(s): %+v", len(nodes), v.running())
	}
	original := ids(v.running())

	// Scaling up keeps the replicas it has; they are not restarted or replaced.
	var scaled struct {
		Changed *bool `json:"changed"`
		Version int64 `json:"version"`
	}
	api.must(http.MethodPut, "/v1/services/web/scale", `{"replicas":4}`, http.StatusOK, &scaled)
	if scaled.Changed == nil || !*scaled.Changed {
		t.Fatalf("scaling 2 to 4: %+v", scaled)
	}
	v = api.waitStable(30*time.Second, "web", 4)
	for id := range original {
		if !ids(v.running())[id] {
			t.Errorf("scaling up replaced the replica %s", id)
		}
	}
	for _, r := range v.running() {
		if original[r.WorkloadID] && (r.Generation != 1 || r.RestartCount != 0) {
			t.Errorf("scaling up touched replica %+v", r)
		}
	}

	// The same request again changes nothing, not even the version.
	before := scaled.Version
	api.must(http.MethodPut, "/v1/services/web/scale", `{"replicas":4}`, http.StatusOK, &scaled)
	if scaled.Changed == nil || *scaled.Changed || scaled.Version != before {
		t.Fatalf("repeating the scale: %+v (version was %d)", scaled, before)
	}
	if got := countAction(api.events(), "service.scaled"); got != 1 {
		t.Errorf("%d service.scaled events after a scale and its repetition, want 1", got)
	}

	// Scaling down stops two replicas and leaves the other two running, untouched.
	api.must(http.MethodPut, "/v1/services/web/scale", `{"replicas":2}`, http.StatusOK, nil)
	v = api.waitStable(30*time.Second, "web", 2)
	for id := range ids(v.running()) {
		if !ids(v.meant())[id] {
			t.Errorf("replica %s is running but not meant to", id)
		}
	}

	// More replicas than the two nodes hold (8 of 500m each per node, 16 in all):
	// the rest wait, and the API says why.
	api.must(http.MethodPut, "/v1/services/web/scale", `{"replicas":18}`, http.StatusOK, nil)
	eventually(t, 30*time.Second, "web to be blocked on capacity", func() (bool, string) {
		v := api.service("web")
		s := v.Status.Scaling
		return s.State == "blocked" && s.Ready == 16 && s.Waiting == 2 && len(v.Status.Unschedulable) == 2,
			fmt.Sprintf("%s ready %d waiting %d unschedulable %d: %s", s.State, s.Ready, s.Waiting, len(v.Status.Unschedulable), s.Message)
	})
	v = api.service("web")
	for _, u := range v.Status.Unschedulable {
		if u.Code != "insufficient_resources" || len(u.Nodes) != 2 {
			t.Errorf("unschedulable replica: %+v, want insufficient_resources with a reason for each of the two nodes", u)
		}
	}
	// Nothing was oversubscribed to make room.
	perNode := map[string]int{}
	for _, r := range v.meant() {
		perNode[r.NodeID]++
	}
	for node, n := range perNode {
		if n > 8 {
			t.Errorf("node %s holds %d replicas of 500m on 4000m", node, n)
		}
	}
	api.must(http.MethodPut, "/v1/services/web/scale", `{"replicas":3}`, http.StatusOK, nil)
	v = api.waitStable(30*time.Second, "web", 3)
	if len(v.Status.Unschedulable) != 0 {
		t.Errorf("explanations remain after scaling down: %+v", v.Status.Unschedulable)
	}

	// A rolling update to app:2. A sampler reads the service while it runs: at no
	// moment may fewer than the wanted three replicas be running (max_unavailable
	// is 0), nor more than four be meant to (max_surge is 1). A reading that
	// breaks a limit has to be repeated by the next one, because a status is read
	// in several queries and one that straddles a change is not a violation.
	var (
		mu         sync.Mutex
		samples    int
		violations []string
		stopSample = make(chan struct{})
		sampled    sync.WaitGroup
	)
	sampled.Add(1)
	go func() {
		defer sampled.Done()
		last := false
		for {
			select {
			case <-stopSample:
				return
			case <-time.After(15 * time.Millisecond):
			}
			code, raw, err := api.try(http.MethodGet, "/v1/services/web", "")
			if err != nil || code != http.StatusOK {
				continue
			}
			var sv serviceView
			if jsonUnmarshal(raw, &sv) != nil {
				continue
			}
			bad := ""
			if got := len(sv.running()); got < 3 {
				bad = fmt.Sprintf("only %d running", got)
			} else if got := len(sv.meant()); got > 4 {
				bad = fmt.Sprintf("%d meant to run", got)
			}
			mu.Lock()
			samples++
			if bad != "" && last {
				violations = append(violations, bad)
			}
			last = bad != ""
			mu.Unlock()
		}
	}()

	api.must(http.MethodPut, "/v1/services/web", serviceBody(3, "app:2"), http.StatusOK, nil)
	eventually(t, 60*time.Second, "the rollout to app:2 to complete", func() (bool, string) {
		v := api.service("web")
		r := v.Status.Rollout
		if r.Revision != 2 || r.State != "complete" {
			return false, fmt.Sprintf("revision %d %s: %s", r.Revision, r.State, r.Message)
		}
		return stable(v, 3)
	})
	close(stopSample)
	sampled.Wait()
	mu.Lock()
	t.Logf("%d samples during the rollout", samples)
	if samples < 10 {
		t.Errorf("only %d samples were taken during the rollout; it was not watched", samples)
	}
	if len(violations) > 0 {
		t.Errorf("the rollout policy was broken: %v", violations)
	}
	mu.Unlock()
	v = api.service("web")
	if v.Spec.Image != "app:2" || v.Revision != 2 {
		t.Fatalf("service after the update: revision %d image %s", v.Revision, v.Spec.Image)
	}
	for _, r := range v.running() {
		if r.Revision != 2 {
			t.Errorf("replica of revision %d still runs after the rollout completed: %+v", r.Revision, r)
		}
	}

	// Rolling back returns to the previous spec as a new revision.
	var rolled struct {
		Revision     int   `json:"revision"`
		RolledBackTo int   `json:"rolled_back_to"`
		Changed      *bool `json:"changed"`
	}
	api.must(http.MethodPost, "/v1/services/web/rollback", "", http.StatusOK, &rolled)
	if rolled.Revision != 3 || rolled.RolledBackTo != 1 {
		t.Fatalf("rollback = %+v, want revision 3 returning to 1", rolled)
	}
	eventually(t, 60*time.Second, "the rollback to complete", func() (bool, string) {
		v := api.service("web")
		r := v.Status.Rollout
		if r.Revision != 3 || r.State != "complete" || v.Spec.Image != "app:1" {
			return false, fmt.Sprintf("revision %d %s image %s: %s", r.Revision, r.State, v.Spec.Image, r.Message)
		}
		return stable(v, 3)
	})
	for _, r := range api.service("web").running() {
		if r.Revision != 3 {
			t.Errorf("replica of revision %d after the rollback: %+v", r.Revision, r)
		}
	}

	// The audit trail tells the story, and every record names the manager that wrote it.
	evs := api.events()
	for action, min := range map[string]int{"service.created": 1, "service.scaled": 4, "service.deployed": 1, "service.rolled_back": 1, "replica.scheduled": 8, "replica.stopped": 4, "rollout.completed": 2} {
		if got := countAction(evs, action); got < min {
			t.Errorf("%d %q events, want at least %d", got, action, min)
		}
	}
	for _, e := range evs {
		if e.InstanceID != "manager-1" {
			t.Errorf("event %d (%s) names manager %q", e.ID, e.Action, e.InstanceID)
		}
	}
}
