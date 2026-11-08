//go:build e2e

package e2e

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// settled is true once the service has n replicas meant to run, all running and
// all on one of the given nodes (the records of replicas stopped on a node that
// was given up on may still be listed: its node has not confirmed their removal).
func settled(v serviceView, n int, nodes ...string) (bool, string) {
	allowed := map[string]bool{}
	for _, id := range nodes {
		allowed[id] = true
	}
	meant, running := v.meant(), v.running()
	if len(meant) != n || len(running) != n {
		return false, fmt.Sprintf("%d meant to run and %d running, want %d", len(meant), len(running), n)
	}
	for _, r := range running {
		if len(allowed) > 0 && !allowed[r.NodeID] {
			return false, fmt.Sprintf("replica %s is on %s", r.WorkloadID, r.NodeID)
		}
	}
	return true, ""
}

func (a *apiClient) waitSettled(timeout time.Duration, name string, n int, nodes ...string) serviceView {
	a.t.Helper()
	var v serviceView
	eventually(a.t, timeout, fmt.Sprintf("%s to have %d replicas running on %v", name, n, nodes), func() (bool, string) {
		code, raw, err := a.try(http.MethodGet, "/v1/services/"+name, "")
		if err != nil || code != http.StatusOK {
			return false, fmt.Sprintf("GET = %d %v", code, err)
		}
		v = serviceView{}
		if jsonUnmarshal(raw, &v) != nil {
			return false, "undecodable"
		}
		return settled(v, n, nodes...)
	})
	return v
}

// An agent that crashes, and the machine's containers with it, is given up on
// after the silence the manager is configured to tolerate; its replicas are
// started on the other node, exactly once each; and when the agent returns the
// node is healthy again, is told to remove what it no longer owns, and nothing
// moves back.
func TestNodeCrashIsFailedOverAndRecovers(t *testing.T) {
	c := newCluster(t, clusterOpts{nodes: []nodeSpec{{id: "node-1"}, {id: "node-2"}}})
	api := c.managers[0].client
	api.must(http.MethodPost, "/v1/services", createBody("web", 4, "app:1"), http.StatusCreated, nil)
	v := api.waitStable(30*time.Second, "web", 4)
	lostIDs := map[string]bool{}
	for _, r := range v.running() {
		if r.NodeID == "node-2" {
			lostIDs[r.WorkloadID] = true
		}
	}
	if len(lostIDs) == 0 || len(lostIDs) == 4 {
		t.Fatalf("replicas were not spread over the two nodes: %+v", v.running())
	}

	c.agents["node-2"].crash()
	api.waitNode(20*time.Second, "node-2", "suspect")
	api.waitNode(20*time.Second, "node-2", "unavailable")
	v = api.waitSettled(30*time.Second, "web", 4, "node-1")
	for _, r := range v.meant() {
		if lostIDs[r.WorkloadID] {
			t.Errorf("the replica %s of the lost node is still meant to run", r.WorkloadID)
		}
	}
	evs := api.events()
	for _, action := range []string{"node.suspect", "node.unavailable"} {
		if countAction(evs, action) != 1 {
			t.Errorf("%d %q events, want 1", countAction(evs, action), action)
		}
	}
	stoppedForTheNode := 0
	for _, e := range evs {
		if e.Action == "replica.stopped" && e.Details["node_id"] == "node-2" && e.Details["reason"] == "node_unavailable" {
			stoppedForTheNode++
		}
	}
	if stoppedForTheNode != len(lostIDs) {
		t.Errorf("%d replicas given up on, want %d", stoppedForTheNode, len(lostIDs))
	}
	if code, body := c.agents["node-1"].readyz(); code != http.StatusOK {
		t.Errorf("the surviving agent is not ready: %d %s", code, body)
	}

	// The node returns. Its old workloads are stopped records now; it confirms that
	// nothing runs for them and they are deleted. The replicas stay where they are.
	staying := ids(v.running())
	c.agents["node-2"].start()
	c.agents["node-2"].waitReady(30 * time.Second)
	api.waitNode(20*time.Second, "node-2", "healthy")
	eventually(t, 30*time.Second, "the lost node's records to be removed", func() (bool, string) {
		v := api.service("web")
		return len(v.Status.Replicas) == 4, fmt.Sprintf("%d replicas listed", len(v.Status.Replicas))
	})
	v = api.waitStable(30*time.Second, "web", 4)
	for id := range ids(v.running()) {
		if !staying[id] {
			t.Errorf("replica %s is new: the recovery moved something", id)
		}
	}
	if countAction(api.events(), "node.recovered") != 1 {
		t.Errorf("%d node.recovered events, want 1", countAction(api.events(), "node.recovered"))
	}
	consistently(t, 2*time.Second, "the fleet at rest after recovery", func() (bool, string) {
		return stable(api.service("web"), 4)
	})
}

// An agent cut off from the manager (a network partition, neither side told)
// stops its containers when its lease runs out, and only after that does the
// manager start the replicas elsewhere; when the network is back the agent
// registers again, learns that its old workloads are gone, and the replicas stay
// where they went. At no time are there more replicas than wanted.
func TestPartitionedAgentFencesItselfBeforeItsReplicasMove(t *testing.T) {
	c := newCluster(t, clusterOpts{nodes: []nodeSpec{{id: "node-1", viaProxy: true}, {id: "node-2"}}})
	api := c.managers[0].client
	api.must(http.MethodPost, "/v1/services", createBody("web", 4, "app:1"), http.StatusCreated, nil)
	v := api.waitStable(30*time.Second, "web", 4)
	if len(ids(v.running())) != 4 {
		t.Fatal("replicas missing")
	}
	was := map[string]string{}
	for _, r := range v.running() {
		was[r.WorkloadID] = r.NodeID
	}

	cutAt := time.Now()
	c.proxy.cutOff()
	c.agents["node-1"].waitNotReady(15 * time.Second)
	api.waitNode(20*time.Second, "node-1", "unavailable")
	v = api.waitSettled(40*time.Second, "web", 4, "node-2")

	fenced, ok := logTime(c.agents["node-1"].p.logs(), "the manager's lease on this node ran out")
	if !ok {
		t.Fatalf("the cut-off agent never stopped its containers:\n%s", tailLines(c.agents["node-1"].p.logs(), 12))
	}
	var firstReplacement time.Time
	for _, e := range api.events() {
		if e.Action == "replica.scheduled" && e.At.After(cutAt) && (firstReplacement.IsZero() || e.At.Before(firstReplacement)) {
			firstReplacement = e.At
		}
	}
	if firstReplacement.IsZero() {
		t.Fatal("no replacement was scheduled after the partition")
	}
	if !fenced.Before(firstReplacement) {
		t.Errorf("the agent fenced itself at %s, but the first replacement was scheduled at %s: a replica could have run twice", fenced.Format(time.RFC3339Nano), firstReplacement.Format(time.RFC3339Nano))
	}
	t.Logf("cut at %s; fenced %s after; first replacement %s after", cutAt.Format("15:04:05.000"), fenced.Sub(cutAt).Round(time.Millisecond), firstReplacement.Sub(cutAt).Round(time.Millisecond))

	// The network returns.
	c.proxy.heal()
	c.agents["node-1"].waitReady(60 * time.Second)
	api.waitNode(30*time.Second, "node-1", "healthy")
	eventually(t, 30*time.Second, "the records of the old workloads to be removed", func() (bool, string) {
		return len(api.service("web").Status.Replicas) == 4, "old records remain"
	})
	v = api.waitStable(30*time.Second, "web", 4)
	for _, r := range v.running() {
		if r.NodeID != "node-2" || was[r.WorkloadID] == "node-1" {
			t.Errorf("after the partition healed, replica %+v is not one of those moved to node-2", r)
		}
	}
}

// A hung agent (SIGSTOP: the process exists and answers nothing) is treated like
// a lost one, and when it wakes it stops the containers it was told it no longer
// owns instead of competing with their replacements.
func TestFrozenAgentIsReplacedAndRecovers(t *testing.T) {
	c := newCluster(t, clusterOpts{nodes: []nodeSpec{{id: "node-1"}, {id: "node-2"}}})
	api := c.managers[0].client
	api.must(http.MethodPost, "/v1/services", createBody("web", 4, "app:1"), http.StatusCreated, nil)
	api.waitStable(30*time.Second, "web", 4)

	c.agents["node-1"].freeze()
	t.Cleanup(c.agents["node-1"].unfreeze)
	api.waitNode(20*time.Second, "node-1", "unavailable")
	api.waitSettled(40*time.Second, "web", 4, "node-2")

	c.agents["node-1"].unfreeze()
	c.agents["node-1"].waitReady(60 * time.Second)
	api.waitNode(30*time.Second, "node-1", "healthy")
	v := api.waitStable(30*time.Second, "web", 4)
	for _, r := range v.running() {
		if r.NodeID != "node-2" {
			t.Errorf("replica %+v is not on the node it was moved to", r)
		}
	}
	// On waking the agent saw its lease had run out and stopped what it had before anything else.
	if _, ok := logTime(c.agents["node-1"].p.logs(), "the manager's lease on this node ran out"); !ok {
		t.Errorf("the agent that woke did not fence itself:\n%s", tailLines(c.agents["node-1"].p.logs(), 12))
	}
}

// A clean stop of an agent is not a failure: the manager is told, the node is
// suspect at once (no new replicas) and its replicas are not moved; when the
// agent starts again the node is healthy.
func TestAgentStopsCleanlyAndReturns(t *testing.T) {
	c := newCluster(t, clusterOpts{nodes: []nodeSpec{{id: "node-1"}, {id: "node-2"}}})
	api := c.managers[0].client
	api.must(http.MethodPost, "/v1/services", createBody("web", 2, "app:1"), http.StatusCreated, nil)
	before := ids(api.waitStable(30*time.Second, "web", 2).running())

	a := c.agents["node-2"]
	began := time.Now()
	code, graceful := a.p.terminate(10 * time.Second)
	if !graceful || code != 0 {
		t.Fatalf("SIGTERM: exit %d, graceful %v\n%s", code, graceful, tailLines(a.p.logs(), 10))
	}
	if took := time.Since(began); took > 5*time.Second {
		t.Errorf("the agent took %s to stop", took)
	}
	if !strings.Contains(a.p.logs(), "deregistered from manager") || !strings.Contains(a.p.logs(), "agent stopped") {
		t.Errorf("the agent did not say goodbye:\n%s", tailLines(a.p.logs(), 8))
	}
	api.waitNode(10*time.Second, "node-2", "suspect")
	if got := ids(api.service("web").running()); len(got) < 1 {
		t.Errorf("nothing runs after one agent left: %v", got)
	}
	a.start()
	a.waitReady(30 * time.Second)
	api.waitNode(20*time.Second, "node-2", "healthy")
	after := ids(api.waitStable(30*time.Second, "web", 2).running())
	for id := range before {
		if !after[id] {
			t.Errorf("replica %s did not survive a clean restart of its agent", id)
		}
	}
}
