package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/election"
	"github.com/dockfleet/dockfleet/internal/metrics"
	"github.com/dockfleet/dockfleet/internal/store"
)

// These tests run the API as one manager of a cluster, with the election
// scripted, on the in-memory store and on PostgreSQL.

// scriptedCluster is the election as the API sees it.
type scriptedCluster struct {
	mu     sync.Mutex
	status election.Status
	err    error
	fenced []int64
}

func (c *scriptedCluster) set(status election.Status, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status, c.err = status, err
}

func (c *scriptedCluster) Epoch() (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status.Epoch, c.err
}

func (c *scriptedCluster) Status() election.Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status
}

func (c *scriptedCluster) Fenced(epoch int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fenced = append(c.fenced, epoch)
}

func (c *scriptedCluster) told() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int64(nil), c.fenced...)
}

// inCluster rebuilds the handler as a manager that takes part in an election.
func (e *serviceEnv) inCluster(cluster Cluster, withMetrics bool) {
	e.t.Helper()
	cfg := Config{
		Store: e.st, Token: token, NodeTTL: 30 * time.Second, Timing: scaleTiming, Cluster: cluster,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), InstanceID: "manager-2", Now: func() time.Time { return e.clock },
	}
	if withMetrics {
		cfg.Metrics = metrics.New("test-version", e.clock)
	}
	e.h = New(cfg)
}

type clusterBody struct {
	Mode         string `json:"mode"`
	InstanceID   string `json:"instance_id"`
	Role         string `json:"role"`
	Leader       string `json:"leader"`
	Epoch        int64  `json:"epoch"`
	ControlPlane struct {
		Epoch     int64      `json:"epoch"`
		Leader    string     `json:"leader"`
		ElectedAt *time.Time `json:"elected_at"`
	} `json:"control_plane"`
	Transitions uint64     `json:"transitions"`
	ValidUntil  *time.Time `json:"valid_until"`
	Since       *time.Time `json:"since"`
}

func (e *serviceEnv) clusterView() clusterBody {
	e.t.Helper()
	rec := e.request(http.MethodGet, "/v1/cluster", "")
	if rec.Code != http.StatusOK {
		e.t.Fatalf("GET /v1/cluster = %d: %s", rec.Code, rec.Body)
	}
	var v clusterBody
	decode(e.t, rec, &v)
	return v
}

const soloWorkload = `{"name":"solo","image":"nginx:1.27","cpu_millis":100,"memory_bytes":67108864}`

func testSingleManagerReportsNoCluster(t *testing.T, e *serviceEnv) {
	if rec := e.request(http.MethodGet, "/v1/cluster", "", "Authorization", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("GET /v1/cluster without the token = %d, want 401", rec.Code)
	}
	v := e.clusterView()
	if v.Mode != "single" || v.Role != "single" || v.InstanceID != "test-manager" || v.Epoch != 0 || v.ControlPlane.Epoch != 0 || v.ControlPlane.ElectedAt != nil || v.Since != nil {
		t.Fatalf("a single manager's view = %+v", v)
	}
	// Placing a workload needs no leader, and the cluster metrics are not exported.
	e.addNode("n1", 4000, 8<<30)
	if rec := e.request(http.MethodPost, "/v1/workloads", soloWorkload); rec.Code != http.StatusCreated {
		t.Fatalf("POST /v1/workloads on a single manager = %d: %s", rec.Code, rec.Body)
	}
	e.withMetrics()
	text, _ := e.scrape()
	for _, name := range []string{"dockfleet_manager_is_leader", "dockfleet_control_plane_epoch", "dockfleet_leader_transitions_total"} {
		if strings.Contains(text, name) {
			t.Errorf("a single manager exports %s", name)
		}
	}
}

func testLeaderAndFollowerViews(t *testing.T, e *serviceEnv) {
	cluster := &scriptedCluster{}
	e.inCluster(cluster, false)
	at := time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)
	_, _, err := e.st.BeginTerm(context.Background(), "manager-1", at)
	if err != nil {
		t.Fatal(err)
	}

	// manager-2 follows manager-1, which leads under epoch 1.
	cluster.set(election.Status{ID: "manager-2", Role: election.Follower, Leader: "manager-1", Since: at, Transitions: 0}, election.ErrNotLeader)
	v := e.clusterView()
	if v.Mode != "high-availability" || v.Role != "follower" || v.InstanceID != "manager-2" || v.Leader != "manager-1" || v.Epoch != 0 ||
		v.ControlPlane.Epoch != 1 || v.ControlPlane.Leader != "manager-1" || v.ControlPlane.ElectedAt == nil || !v.ControlPlane.ElectedAt.Equal(at) || v.ValidUntil != nil {
		t.Fatalf("the follower's view = %+v", v)
	}

	// It becomes the leader under epoch 2.
	_, _, err = e.st.BeginTerm(context.Background(), "manager-2", at.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	until := at.Add(2 * time.Hour)
	cluster.set(election.Status{ID: "manager-2", Role: election.Leader, Leader: "manager-2", Epoch: 2, Since: at, ValidUntil: until, Transitions: 1}, nil)
	v = e.clusterView()
	if v.Role != "leader" || v.Leader != "manager-2" || v.Epoch != 2 || v.ControlPlane.Epoch != 2 || v.ControlPlane.Leader != "manager-2" ||
		v.ValidUntil == nil || !v.ValidUntil.Equal(until) || v.Transitions != 1 {
		t.Fatalf("the leader's view = %+v", v)
	}
}

func testWorkloadsArePlacedByTheLeaderOnly(t *testing.T, e *serviceEnv) {
	cluster := &scriptedCluster{}
	e.inCluster(cluster, false)
	e.addNode("n1", 4000, 8<<30)
	if _, _, err := e.st.BeginTerm(context.Background(), "manager-1", e.clock); err != nil {
		t.Fatal(err)
	}

	// A follower refuses, says who leads, and places nothing.
	cluster.set(election.Status{ID: "manager-2", Role: election.Follower, Leader: "manager-1"}, election.ErrNotLeader)
	rec := e.request(http.MethodPost, "/v1/workloads", soloWorkload)
	var body struct{ Error, Code, Leader string }
	decode(t, rec, &body)
	if rec.Code != http.StatusServiceUnavailable || body.Code != "not_leader" || body.Leader != "manager-1" || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("POST /v1/workloads on a follower = %d %+v (Retry-After %q)", rec.Code, body, rec.Header().Get("Retry-After"))
	}
	if list, _ := e.st.ListWorkloads(context.Background()); len(list) != 0 {
		t.Fatalf("a follower placed a workload: %+v", list)
	}

	// The leader, under the current epoch, places it, and so does it only once.
	cluster.set(election.Status{ID: "manager-2", Role: election.Leader, Leader: "manager-2", Epoch: 1}, nil)
	if rec := e.request(http.MethodPost, "/v1/workloads", soloWorkload); rec.Code != http.StatusCreated {
		t.Fatalf("POST /v1/workloads on the leader = %d: %s", rec.Code, rec.Body)
	}

	// A manager that believes it leads under an epoch that was replaced is refused
	// by the database, not by its own belief, and tells the election.
	if _, _, err := e.st.BeginTerm(context.Background(), "manager-3", e.clock); err != nil {
		t.Fatal(err)
	}
	rec = e.request(http.MethodPost, "/v1/workloads", `{"name":"late","image":"nginx:1.27","cpu_millis":100,"memory_bytes":67108864}`)
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "not_leader") {
		t.Fatalf("POST /v1/workloads under a replaced epoch = %d: %s", rec.Code, rec.Body)
	}
	if list, _ := e.st.ListWorkloads(context.Background()); len(list) != 1 {
		t.Fatalf("%d workloads, want only the one placed under the current epoch", len(list))
	}
	if got := cluster.told(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("the election was told %v was replaced, want [1]", got)
	}
	// Requests that only record what an administrator wants are not the leader's.
	cluster.set(election.Status{ID: "manager-2", Role: election.Follower, Leader: "manager-3"}, election.ErrNotLeader)
	if rec := e.request(http.MethodPost, "/v1/services", minimalService); rec.Code != http.StatusCreated {
		t.Fatalf("creating a service on a follower = %d: %s", rec.Code, rec.Body)
	}
}

func testClusterMetrics(t *testing.T, e *serviceEnv) {
	cluster := &scriptedCluster{}
	e.inCluster(cluster, true)
	if _, _, err := e.st.BeginTerm(context.Background(), "manager-1", e.clock); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.st.BeginTerm(context.Background(), "manager-2", e.clock); err != nil {
		t.Fatal(err)
	}
	cluster.set(election.Status{ID: "manager-2", Role: election.Leader, Leader: "manager-2", Epoch: 2, Transitions: 3, Errors: 5}, nil)
	_, series := e.scrape()
	wantSeries(t, series, "dockfleet_manager_is_leader", 1)
	wantSeries(t, series, "dockfleet_manager_epoch", 2)
	wantSeries(t, series, "dockfleet_control_plane_epoch", 2)
	wantSeries(t, series, `dockfleet_cluster_leader{leader="manager-2"}`, 1)
	wantSeries(t, series, "dockfleet_leader_transitions_total", 3)
	wantSeries(t, series, "dockfleet_election_errors_total", 5)

	// A follower that does not know the leader yet, and a leader that was replaced
	// without knowing: the gauges show both.
	cluster.set(election.Status{ID: "manager-2", Role: election.Follower}, election.ErrNotLeader)
	_, series = e.scrape()
	wantSeries(t, series, "dockfleet_manager_is_leader", 0)
	wantSeries(t, series, "dockfleet_manager_epoch", 0)
	wantSeries(t, series, "dockfleet_control_plane_epoch", 2)
	wantSeries(t, series, `dockfleet_cluster_leader{leader=""}`, 1)
	cluster.set(election.Status{ID: "manager-2", Role: election.Leader, Leader: "manager-2", Epoch: 1}, nil)
	_, series = e.scrape()
	if series["dockfleet_manager_epoch"] == series["dockfleet_control_plane_epoch"] {
		t.Errorf("a leader under a replaced epoch looks current: %v", series)
	}
}

// With several managers, silence from before the current term began is not
// counted against a node: the agents had nobody to talk to while the election
// ran. The start of the term is in the database, so every manager says the same.
func testNodeHealthIsCountedFromTheStartOfTheTerm(t *testing.T, e *serviceEnv) {
	// The node last spoke ten minutes ago; the leader's term began a minute ago.
	e.addNode("n1", 4000, 8<<30)
	e.clock = e.clock.Add(10 * time.Minute)
	health := func() string {
		rec := e.request(http.MethodGet, "/v1/nodes/n1", "")
		var v struct{ Health string }
		decode(t, rec, &v)
		return v.Health
	}

	e.useTiming() // single manager
	if got := health(); got != store.NodeUnavailable {
		t.Fatalf("a single manager says the silent node is %s, want unavailable", got)
	}

	cluster := &scriptedCluster{}
	e.inCluster(cluster, false)
	if _, _, err := e.st.BeginTerm(context.Background(), "manager-1", e.clock.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := health(); got != store.NodeSuspect {
		t.Fatalf("in a cluster, one minute into the term, the node is %s, want suspect (silent for the minute it had)", got)
	}
	e.clock = e.clock.Add(20 * time.Second) // 80 s into the term
	if got := health(); got != store.NodeSuspect {
		t.Fatalf("80 s into the term the node is %s, want suspect", got)
	}
	e.clock = e.clock.Add(2 * time.Minute) // well past the unavailable limit, counted from the term
	if got := health(); got != store.NodeUnavailable {
		t.Fatalf("over three minutes into the term the node is %s, want unavailable", got)
	}
}
