package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/agent"
	"github.com/dockfleet/dockfleet/internal/config"
	"github.com/dockfleet/dockfleet/internal/docker"
	"github.com/dockfleet/dockfleet/internal/dockerfake"
	"github.com/dockfleet/dockfleet/internal/election"
	"github.com/dockfleet/dockfleet/internal/etcd"
	"github.com/dockfleet/dockfleet/internal/etcdfake"
	dfmanager "github.com/dockfleet/dockfleet/internal/manager"
	"github.com/dockfleet/dockfleet/internal/protocol"
	"github.com/dockfleet/dockfleet/internal/store"
)

// These tests run several real managers (manager.Manager, the same code the
// binary runs) against one shared database and one etcd, with real agents on
// in-memory Docker engines. The managers listen on loopback ports of the project's
// range (21260 and up, two each). Time is real: the election lease is one second.
// The same scenarios run against the fake etcd, a real etcd (tag
// etcdintegration) and PostgreSQL (tag integration), see ha_*_test.go.

const (
	haElectionTTL = time.Second
	haFirstPort   = 21260
	haWait        = 20 * time.Second
)

// haWorld is what the managers share: the database they all read and write, and
// the etcd they elect through. store and backend give a handle for the named
// manager; for the in-memory store every name gets the same object.
type haWorld struct {
	store   func(name string) store.Store
	backend func(name string) election.Backend
	prefix  string
}

type haWorldFunc func(t *testing.T) haWorld

// fakeEtcdBackend is a client of the fake etcd for the named manager.
func fakeEtcdBackend(t *testing.T, srv *etcdfake.Server) func(name string) election.Backend {
	return func(name string) election.Backend {
		c, err := etcd.New(etcd.Config{Endpoints: []string{srv.URL()}, HTTPClient: etcdfake.HTTPClient(name), Timeout: 500 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(c.Close)
		return c
	}
}

// memoryWorld is the in-memory store and the fake etcd.
func memoryWorld(t *testing.T) haWorld {
	t.Helper()
	srv := etcdfake.New(nil)
	t.Cleanup(srv.Close)
	mem := store.NewMemory()
	return haWorld{store: func(string) store.Store { return mem }, backend: fakeEtcdBackend(t, srv), prefix: "/dockfleet-test"}
}

var errEtcdUnreachable = errors.New("etcd is unreachable")

// cutBackend is a manager's connection to etcd, which a test can cut: while it is
// cut every call fails, as it does for a manager on the wrong side of a partition.
type cutBackend struct {
	inner election.Backend
	mu    sync.Mutex
	down  bool
}

func (b *cutBackend) cut()  { b.set(true) }
func (b *cutBackend) heal() { b.set(false) }

func (b *cutBackend) set(down bool) {
	b.mu.Lock()
	b.down = down
	b.mu.Unlock()
}

func (b *cutBackend) check() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.down {
		return errEtcdUnreachable
	}
	return nil
}

func (b *cutBackend) Grant(ctx context.Context, ttl time.Duration) (election.Lease, error) {
	if err := b.check(); err != nil {
		return election.Lease{}, err
	}
	return b.inner.Grant(ctx, ttl)
}

func (b *cutBackend) KeepAlive(ctx context.Context, l election.Lease) (time.Duration, error) {
	if err := b.check(); err != nil {
		return 0, err
	}
	return b.inner.KeepAlive(ctx, l)
}

func (b *cutBackend) Revoke(ctx context.Context, l election.Lease) error {
	if err := b.check(); err != nil {
		return err
	}
	return b.inner.Revoke(ctx, l)
}

func (b *cutBackend) Claim(ctx context.Context, key, id string, l election.Lease) (election.Claim, error) {
	if err := b.check(); err != nil {
		return election.Claim{}, err
	}
	return b.inner.Claim(ctx, key, id, l)
}

func (b *cutBackend) Observe(ctx context.Context, key string) (election.Holder, bool, error) {
	if err := b.check(); err != nil {
		return election.Holder{}, false, err
	}
	return b.inner.Observe(ctx, key)
}

// haMember is one manager process.
type haMember struct {
	c       *haCluster
	id      string
	api     string
	grpc    string
	store   store.Store
	backend *cutBackend
	mgr     *dfmanager.Manager
	cancel  context.CancelFunc
	done    chan error
}

// haCluster is a set of managers, the agents they serve and the database behind them.
type haCluster struct {
	t       *testing.T
	world   haWorld
	members []*haMember
	admin   store.Store
	http    *http.Client
	engines map[string]*dockerfake.Fake
}

func newHACluster(t *testing.T, world haWorldFunc, managers int) *haCluster {
	t.Helper()
	c := &haCluster{t: t, world: world(t), http: &http.Client{Timeout: 10 * time.Second}, engines: map[string]*dockerfake.Fake{}}
	c.admin = c.world.store("admin")
	for _, node := range []string{"sim-1", "sim-2", "probe"} {
		if _, err := c.admin.CreateEnrollment(context.Background(), store.Enrollment{NodeID: node, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < managers; i++ {
		name := fmt.Sprintf("manager-%d", i+1)
		m := &haMember{
			c: c, id: name, store: c.world.store(name),
			api:     fmt.Sprintf("127.0.0.1:%d", haFirstPort+2*i),
			grpc:    fmt.Sprintf("127.0.0.1:%d", haFirstPort+2*i+1),
			backend: &cutBackend{inner: c.world.backend(name)},
		}
		c.members = append(c.members, m)
	}
	for _, m := range c.members {
		m.start()
	}
	t.Cleanup(c.shutdown)
	return c
}

func (m *haMember) config() config.Manager {
	return config.Manager{
		InstanceID: m.id, APIToken: adminToken, AgentToken: agentToken,
		HeartbeatInterval: heartbeatEvery, NodeTTL: 5 * time.Second, NodeUnavailableAfter: time.Minute,
		RequestTimeout: 5 * time.Second, ShutdownTimeout: 5 * time.Second,
		SchedulerInterval: 20 * time.Millisecond, ReconcileInterval: 20 * time.Millisecond,
		MetricsEnabled: true,
		// What the loader would set; the endpoints are not dialed (the backend is given).
		EtcdEndpoints: []string{"http://127.0.0.1:2379"}, EtcdPrefix: m.c.world.prefix, ElectionTTL: haElectionTTL,
	}
}

func (m *haMember) start() {
	m.c.t.Helper()
	mgr, err := dfmanager.New(m.config(), m.store, discardLogger(), dfmanager.WithElection(m.backend))
	if err != nil {
		m.c.t.Fatal(err)
	}
	apiLn, err := net.Listen("tcp", m.api)
	if err != nil {
		m.c.t.Fatal(err)
	}
	grpcLn, err := net.Listen("tcp", m.grpc)
	if err != nil {
		_ = apiLn.Close()
		m.c.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.mgr, m.cancel, m.done = mgr, cancel, make(chan error, 1)
	go func() { m.done <- mgr.Serve(ctx, apiLn, grpcLn) }()
}

// stop shuts the manager down the way a signal would.
func (m *haMember) stop() {
	m.c.t.Helper()
	if m.cancel == nil {
		return
	}
	m.cancel()
	select {
	case err := <-m.done:
		if err != nil {
			m.c.t.Errorf("%s: Serve returned %v after a requested shutdown", m.id, err)
		}
	case <-time.After(haWait):
		m.c.t.Errorf("%s did not stop", m.id)
	}
	m.mgr, m.cancel = nil, nil
}

func (c *haCluster) shutdown() {
	for _, m := range c.members {
		m.stop()
	}
}

func (c *haCluster) grpcAddrs() []string {
	var out []string
	for _, m := range c.members {
		out = append(out, m.grpc)
	}
	return out
}

// startAgent runs an agent that knows every manager, on an engine of its own.
func (c *haCluster) startAgent(id string) *dockerfake.Fake {
	c.t.Helper()
	engine := dockerfake.New(id)
	client, err := protocol.Dial(protocol.ClientConfig{Addrs: c.grpcAddrs(), Token: agentToken})
	if err != nil {
		c.t.Fatal(err)
	}
	a := agent.New(agent.Config{
		Node: nodeInfo(id), Manager: client, Docker: engine, Logger: discardLogger(),
		RPCTimeout: time.Second, RetryMin: 5 * time.Millisecond, RetryMax: 20 * time.Millisecond, Resync: 10 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = a.Run(ctx)
		close(done)
	}()
	c.t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(haWait):
			c.t.Errorf("agent %s did not stop", id)
		}
		_ = client.Close()
	})
	c.engines[id] = engine
	return engine
}

func waitLong(t *testing.T, what string, cond func() bool) {
	t.Helper()
	waitExplained(t, what, cond, nil)
}

func waitExplained(t *testing.T, what string, cond func() bool, explain func() string) {
	t.Helper()
	deadline := time.Now().Add(haWait)
	for !cond() {
		if time.Now().After(deadline) {
			if explain != nil {
				t.Log(explain())
			}
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// wait is waitLong that, when it times out, says what the cluster looked like.
func (c *haCluster) wait(what string, cond func() bool) {
	c.t.Helper()
	waitExplained(c.t, what, cond, c.describe)
}

// describe is the state of the managers, the workloads and the latest events.
func (c *haCluster) describe() string {
	var b strings.Builder
	ctx := context.Background()
	for _, m := range c.members {
		if m.mgr == nil {
			fmt.Fprintf(&b, "manager %s: stopped\n", m.id)
			continue
		}
		st := m.mgr.Leadership()
		fmt.Fprintf(&b, "manager %s: %s epoch %d leader %q errors %d %s sessions %d\n", m.id, st.Role, st.Epoch, st.Leader, st.Errors, st.LastError, len(m.mgr.Sessions()))
	}
	if cp, err := c.admin.GetControlPlane(ctx); err == nil {
		fmt.Fprintf(&b, "control plane: epoch %d leader %s\n", cp.Epoch, cp.Leader)
	}
	if ws, err := c.admin.ListWorkloads(ctx); err == nil {
		for _, w := range ws {
			state := "-"
			if w.Observed != nil {
				state = w.Observed.State
			}
			fmt.Fprintf(&b, "workload %s on %s: desired %s generation %d observed %s\n", w.Name, w.NodeID, w.DesiredState, w.Generation, state)
		}
	}
	if ul, err := c.admin.ListUnschedulable(ctx, ""); err == nil {
		for _, u := range ul {
			fmt.Fprintf(&b, "unschedulable: %s\n", u.Message)
		}
	}
	if evs, err := c.admin.ListEvents(ctx, store.EventFilter{Limit: 12}); err == nil {
		for i := len(evs) - 1; i >= 0; i-- {
			fmt.Fprintf(&b, "event %s by %s: %s\n", evs[i].Action, evs[i].InstanceID, evs[i].Message)
		}
	}
	for id, e := range c.engines {
		fmt.Fprintf(&b, "engine %s: %d containers, calls %v\n", id, len(e.Managed()), e.Calls())
	}
	return b.String()
}

// waitLeader waits until exactly one running manager leads under the epoch.
func (c *haCluster) waitLeader(epoch int64) *haMember {
	c.t.Helper()
	var leader *haMember
	c.wait(fmt.Sprintf("a single leader under epoch %d", epoch), func() bool {
		leader = nil
		for _, m := range c.members {
			if m.mgr == nil {
				continue
			}
			if st := m.mgr.Leadership(); st.Role == election.Leader && st.Epoch == epoch {
				if leader != nil {
					return false
				}
				leader = m
			}
		}
		return leader != nil
	})
	return leader
}

func (c *haCluster) runningContainers() int {
	n := 0
	for _, e := range c.engines {
		for _, info := range e.Managed() {
			if info.State == docker.StateRunning {
				n++
			}
		}
	}
	return n
}

// calls is how many times the engines were asked to do op, over all nodes.
func (c *haCluster) calls(op string) int {
	n := 0
	for _, e := range c.engines {
		n += e.Count(op)
	}
	return n
}

func (c *haCluster) events(action string) []store.Event {
	c.t.Helper()
	all, err := c.admin.ListEvents(context.Background(), store.EventFilter{Limit: store.MaxEventLimit})
	if err != nil {
		c.t.Fatal(err)
	}
	var out []store.Event
	for i := len(all) - 1; i >= 0; i-- { // oldest first
		if all[i].Action == action {
			out = append(out, all[i])
		}
	}
	return out
}

// wantElections checks the record of leaders: epoch i+1 was begun by leaders[i],
// which names its predecessor, with no epoch missing.
func (c *haCluster) wantElections(leaders ...string) {
	c.t.Helper()
	got := c.events("leader.elected")
	if len(got) != len(leaders) {
		c.t.Fatalf("%d leader.elected events, want %d (%v): %+v", len(got), len(leaders), leaders, got)
	}
	for i, e := range got {
		prev := ""
		if i > 0 {
			prev = leaders[i-1]
		}
		d := e.Details
		if e.InstanceID != leaders[i] || d["epoch"] != fmt.Sprint(i+1) || d["previous_leader"] != prev || d["previous_epoch"] != fmt.Sprint(i) {
			c.t.Errorf("election %d = %s %v, want %s at epoch %d replacing %q at epoch %d", i, e.InstanceID, d, leaders[i], i+1, prev, i)
		}
	}
	cp, err := c.admin.GetControlPlane(context.Background())
	if err != nil || cp.Epoch != int64(len(leaders)) || cp.Leader != leaders[len(leaders)-1] {
		c.t.Errorf("control plane = %+v, %v; want epoch %d led by %s", cp, err, len(leaders), leaders[len(leaders)-1])
	}
}

// do calls a manager's API with the admin token.
func (m *haMember) do(method, path, body string) (int, []byte) {
	m.c.t.Helper()
	req, err := http.NewRequest(method, "http://"+m.api+path, strings.NewReader(body))
	if err != nil {
		m.c.t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+adminToken)
	resp, err := m.c.http.Do(req)
	if err != nil {
		m.c.t.Fatalf("%s %s on %s: %v", method, path, m.id, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		m.c.t.Fatal(err)
	}
	return resp.StatusCode, raw
}

type clusterJSON struct {
	Mode         string `json:"mode"`
	InstanceID   string `json:"instance_id"`
	Role         string `json:"role"`
	Leader       string `json:"leader"`
	Epoch        int64  `json:"epoch"`
	ControlPlane struct {
		Epoch  int64  `json:"epoch"`
		Leader string `json:"leader"`
	} `json:"control_plane"`
	Transitions uint64 `json:"transitions"`
}

func (m *haMember) cluster() clusterJSON {
	m.c.t.Helper()
	code, raw := m.do(http.MethodGet, "/v1/cluster", "")
	var v clusterJSON
	if code != http.StatusOK || json.Unmarshal(raw, &v) != nil {
		m.c.t.Fatalf("GET /v1/cluster on %s = %d: %s", m.id, code, raw)
	}
	return v
}

func (m *haMember) series() map[string]float64 {
	m.c.t.Helper()
	code, raw := m.do(http.MethodGet, "/metrics", "")
	if code != http.StatusOK {
		m.c.t.Fatalf("GET /metrics on %s = %d", m.id, code)
	}
	return seriesOf(m.c.t, string(raw))
}

func (c *haCluster) createService(via *haMember, name string, replicas int) {
	c.t.Helper()
	body := fmt.Sprintf(`{"name":%q,`, name) + strings.TrimPrefix(definition(replicas, "app:1", "", ""), "{")
	if code, raw := via.do(http.MethodPost, "/v1/services", body); code != http.StatusCreated {
		c.t.Fatalf("create %s on %s = %d: %s", name, via.id, code, raw)
	}
}

func (c *haCluster) scale(via *haMember, name string, replicas int) {
	c.t.Helper()
	if code, raw := via.do(http.MethodPut, "/v1/services/"+name+"/scale", fmt.Sprintf(`{"replicas":%d}`, replicas)); code != http.StatusOK {
		c.t.Fatalf("scale %s on %s = %d: %s", name, via.id, code, raw)
	}
}

// waitSessions waits until the manager holds a session for every node.
func (m *haMember) waitSessions(nodes ...string) {
	m.c.t.Helper()
	m.c.wait(m.id+" to hold the sessions of "+strings.Join(nodes, ", "), func() bool {
		if m.mgr == nil {
			return false
		}
		have := map[string]bool{}
		for _, s := range m.mgr.Sessions() {
			have[s.NodeID] = true
		}
		for _, n := range nodes {
			if !have[n] {
				return false
			}
		}
		return true
	})
}

// other returns the running managers that are not the given one.
func (c *haCluster) others(not *haMember) []*haMember {
	var out []*haMember
	for _, m := range c.members {
		if m != not && m.mgr != nil {
			out = append(out, m)
		}
	}
	return out
}

// schedulerOf is who made the placement decisions recorded as replica.scheduled
// for a service's replicas after the given time, as a set of manager names.
func (c *haCluster) schedulersAfter(at time.Time) map[string]bool {
	c.t.Helper()
	out := map[string]bool{}
	for _, e := range c.events("replica.scheduled") {
		if e.At.After(at) {
			out[e.InstanceID] = true
		}
	}
	return out
}

// ---- scenarios ----

// Of three managers sharing a database only the elected one schedules,
// reconciles and answers agents. The others serve the API, and refuse what only
// a leader may do.
func haOnlyTheLeaderActs(t *testing.T, world haWorldFunc) {
	c := newHACluster(t, world, 3)
	c.startAgent("sim-1")
	c.startAgent("sim-2")
	leader := c.waitLeader(1)
	followers := c.others(leader)
	leader.waitSessions("sim-1", "sim-2")

	for _, m := range followers {
		c.wait(m.id+" to know who leads", func() bool { return m.cluster().Leader == leader.id })
	}
	lv := leader.cluster()
	if lv.Mode != "high-availability" || lv.Role != "leader" || lv.Epoch != 1 || lv.Leader != leader.id || lv.ControlPlane.Epoch != 1 || lv.ControlPlane.Leader != leader.id {
		t.Fatalf("the leader's view = %+v", lv)
	}
	for _, m := range followers {
		fv := m.cluster()
		if fv.Role != "follower" || fv.Epoch != 0 || fv.Leader != leader.id || fv.ControlPlane.Epoch != 1 || fv.InstanceID != m.id {
			t.Fatalf("%s's view = %+v", m.id, fv)
		}
		if got := m.mgr.Sessions(); len(got) != 0 {
			t.Errorf("a follower holds sessions: %+v", got)
		}
	}

	// Desired state is accepted by any manager: it is recorded in the shared database.
	c.createService(followers[0], "web", 3)
	c.wait("three running replicas", func() bool { return c.runningContainers() == 3 })

	// Every decision of the scheduler and the reconciler was the leader's.
	all, err := c.admin.ListEvents(context.Background(), store.EventFilter{Limit: store.MaxEventLimit})
	if err != nil {
		t.Fatal(err)
	}
	decided := 0
	for _, e := range all {
		if e.Actor == "scheduler" || e.Actor == "reconciler" {
			decided++
			if e.InstanceID != leader.id {
				t.Errorf("%s decided by %s, not by the leader %s: %s", e.Action, e.InstanceID, leader.id, e.Message)
			}
		}
	}
	if decided < 3 {
		t.Errorf("only %d decisions recorded; the three placements are missing", decided)
	}
	c.wantElections(leader.id)

	// The metrics tell the same story, manager by manager.
	ls := leader.series()
	if ls[`dockfleet_scheduling_passes_total{result="ok"}`] == 0 || ls[`dockfleet_reconcile_passes_total{result="ok"}`] == 0 ||
		ls["dockfleet_manager_is_leader"] != 1 || ls["dockfleet_manager_epoch"] != 1 || ls["dockfleet_control_plane_epoch"] != 1 ||
		ls[`dockfleet_cluster_leader{leader="`+leader.id+`"}`] != 1 || ls["dockfleet_leader_transitions_total"] != 1 {
		t.Errorf("the leader's election metrics = %v", ls)
	}
	for _, m := range followers {
		fs := m.series()
		if fs[`dockfleet_scheduling_passes_total{result="ok"}`] != 0 || fs[`dockfleet_reconcile_passes_total{result="ok"}`] != 0 ||
			fs["dockfleet_manager_is_leader"] != 0 || fs["dockfleet_manager_epoch"] != 0 || fs["dockfleet_control_plane_epoch"] != 1 {
			t.Errorf("%s ran passes or claims to lead: %v", m.id, fs)
		}
	}

	// Placing a workload by request is scheduling too.
	solo := `{"name":"solo","image":"app:1","cpu_millis":100,"memory_bytes":67108864}`
	code, raw := followers[1].do(http.MethodPost, "/v1/workloads", solo)
	var refusal struct{ Code, Leader string }
	if code != http.StatusServiceUnavailable || json.Unmarshal(raw, &refusal) != nil || refusal.Code != "not_leader" || refusal.Leader != leader.id {
		t.Fatalf("POST /v1/workloads on a follower = %d: %s; want 503 naming the leader %s", code, raw, leader.id)
	}
	if code, raw := leader.do(http.MethodPost, "/v1/workloads", solo); code != http.StatusCreated {
		t.Fatalf("POST /v1/workloads on the leader = %d: %s", code, raw)
	}
}

// When the leader shuts down, another is elected at once; the agents find it,
// nothing they run is touched, and the new leader carries on from the database.
// The manager that left comes back as a follower.
func haLeadershipTransfers(t *testing.T, world haWorldFunc) {
	c := newHACluster(t, world, 3)
	c.startAgent("sim-1")
	c.startAgent("sim-2")
	first := c.waitLeader(1)
	first.waitSessions("sim-1", "sim-2")
	c.createService(first, "web", 3)
	c.wait("three running replicas", func() bool { return c.runningContainers() == 3 })
	if c.calls("create") != 3 {
		t.Fatalf("%d containers created for three replicas", c.calls("create"))
	}

	first.stop()
	second := c.waitLeader(2)
	if second == first {
		t.Fatal("the manager that was shut down still leads")
	}
	second.waitSessions("sim-1", "sim-2")
	c.wantElections(first.id, second.id)
	lost := c.events("leader.lost")
	if len(lost) != 1 || lost[0].InstanceID != first.id || lost[0].Details["epoch"] != "1" || lost[0].Details["reason"] != "shutdown" {
		t.Errorf("leader.lost events = %+v", lost)
	}

	// Nothing the agents run was touched by the change of leader.
	if c.calls("create") != 3 || c.calls("stop") != 0 || c.calls("remove") != 0 || c.runningContainers() != 3 {
		t.Errorf("the failover touched containers: %d created, %d stopped, %d removed, %d running", c.calls("create"), c.calls("stop"), c.calls("remove"), c.runningContainers())
	}

	// The control plane works under the new leader, for a request to any manager.
	third := c.others(second)[0]
	c.scale(third, "web", 4)
	c.wait("a fourth replica", func() bool { return c.runningContainers() == 4 })
	scheduled := c.events("replica.scheduled")
	if got := scheduled[len(scheduled)-1].InstanceID; got != second.id {
		t.Errorf("the fourth replica was placed by %s, not by the new leader %s", got, second.id)
	}

	// The manager that left rejoins as a follower and does not take the leadership back.
	first.start()
	c.wait("the returning manager to follow", func() bool {
		v := first.cluster()
		return v.Role == "follower" && v.Leader == second.id && v.ControlPlane.Epoch == 2
	})
	if first.series()[`dockfleet_scheduling_passes_total{result="ok"}`] != 0 {
		t.Error("the returning manager scheduled although it follows")
	}
	c.wantElections(first.id, second.id)
}

// A leader that crashes cannot give up its lease; the others take over when it
// has run out, and not before. Everything that was decided is still in the
// database, so nothing is lost.
func haFailoverAfterACrash(t *testing.T, world haWorldFunc) {
	c := newHACluster(t, world, 3)
	c.startAgent("sim-1")
	c.startAgent("sim-2")
	first := c.waitLeader(1)
	first.waitSessions("sim-1", "sim-2")
	c.createService(first, "web", 2)
	c.wait("two running replicas", func() bool { return c.runningContainers() == 2 })

	// The crash: the process goes and so does its way to etcd, so the lease is not revoked.
	crashed := time.Now()
	first.backend.cut()
	first.stop()
	second := c.waitLeader(2)
	waited := time.Since(crashed)
	if waited < haElectionTTL/2 {
		t.Errorf("a new leader was elected %v after the crash, before the old lease could have run out", waited)
	}
	second.waitSessions("sim-1", "sim-2")
	c.wantElections(first.id, second.id)

	if c.calls("create") != 2 || c.calls("stop") != 0 || c.calls("remove") != 0 {
		t.Errorf("containers were touched: %d created, %d stopped, %d removed", c.calls("create"), c.calls("stop"), c.calls("remove"))
	}
	c.scale(c.others(second)[0], "web", 3)
	c.wait("a third replica", func() bool { return c.runningContainers() == 3 })
	if got := c.schedulersAfter(crashed); len(got) != 1 || !got[second.id] {
		t.Errorf("replicas were placed after the crash by %v, want only %s", got, second.id)
	}
}

// A leader that is cut off from etcd stops acting before anyone else can start:
// it refuses agents and requests, its decisions end, and when it is back it
// follows the leader that took over.
func haPartitionedLeaderStopsIssuingInstructions(t *testing.T, world haWorldFunc) {
	c := newHACluster(t, world, 3)
	c.startAgent("sim-1")
	c.startAgent("sim-2")
	old := c.waitLeader(1)
	old.waitSessions("sim-1", "sim-2")
	c.createService(old, "web", 2)
	c.wait("two running replicas", func() bool { return c.runningContainers() == 2 })

	// A probe talks to the old leader and to nobody else.
	probe, err := protocol.Dial(protocol.ClientConfig{Addrs: []string{old.grpc}, Token: agentToken})
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*haWait)
	defer cancel()
	// The probe is a node too, so it is given a size nothing fits on: it must not be
	// handed replicas that no engine runs.
	tiny := nodeInfo("probe")
	tiny.CPUMillis, tiny.MemoryBytes = 1, 1
	session, err := probe.Register(ctx, tiny)
	if err != nil || session.Epoch != 1 {
		t.Fatalf("the probe's registration = %+v, %v", session, err)
	}
	if _, err := probe.Heartbeat(ctx, "probe", session.ID, nil); err != nil {
		t.Fatalf("the leader refused a heartbeat: %v", err)
	}

	// The old leader loses etcd. It keeps answering until its lease can no longer be trusted, and no longer.
	old.backend.cut()
	var refusedAt time.Time
	c.wait("the partitioned leader to refuse the probe", func() bool {
		_, err := probe.Heartbeat(ctx, "probe", session.ID, nil)
		if protocol.IsNotLeader(err) {
			refusedAt = time.Now()
			return true
		}
		return false
	})
	next := c.waitLeader(2)
	elected := c.events("leader.elected")
	if len(elected) != 2 || !refusedAt.Before(elected[1].At) {
		t.Fatalf("the old leader refused at %v, the new one was elected at %v: the old one must stop first", refusedAt, elected[1:])
	}
	if code, raw := old.do(http.MethodPost, "/v1/workloads", `{"name":"late","image":"app:1","cpu_millis":100,"memory_bytes":67108864}`); code != http.StatusServiceUnavailable || !strings.Contains(string(raw), "not_leader") {
		t.Errorf("POST /v1/workloads on the old leader = %d: %s", code, raw)
	}
	c.wait("the old leader to say it follows", func() bool { return old.cluster().Role == "follower" })
	lost := c.events("leader.lost")
	if len(lost) != 1 || lost[0].InstanceID != old.id || lost[0].Details["reason"] != "lease could not be renewed in time" {
		t.Errorf("leader.lost events = %+v", lost)
	}

	// From the election on, every decision is the new leader's.
	next.waitSessions("sim-1", "sim-2")
	c.scale(c.others(next)[0], "web", 3)
	c.wait("a third replica", func() bool { return c.runningContainers() == 3 })
	for _, e := range c.events("replica.scheduled") {
		if e.At.After(elected[1].At) && e.InstanceID != next.id {
			t.Errorf("%s scheduled after the election of %s: %s", e.InstanceID, next.id, e.Message)
		}
	}

	// When etcd is back the old leader follows; it does not take the leadership back.
	old.backend.heal()
	c.wait("the old leader to follow the new one", func() bool { return old.cluster().Leader == next.id })
	if v := old.cluster(); v.Role != "follower" || v.ControlPlane.Epoch != 2 || v.ControlPlane.Leader != next.id {
		t.Errorf("the healed manager's view = %+v", v)
	}
	c.wantElections(old.id, next.id)
	if got := c.runningContainers(); got != 3 {
		t.Errorf("%d containers run, want 3: a partition must not duplicate replicas", got)
	}
}

func TestHAOnlyTheLeaderActs(t *testing.T)   { haOnlyTheLeaderActs(t, memoryWorld) }
func TestHALeadershipTransfers(t *testing.T) { haLeadershipTransfers(t, memoryWorld) }
func TestHAFailoverAfterACrash(t *testing.T) { haFailoverAfterACrash(t, memoryWorld) }
func TestHAPartitionedLeaderStops(t *testing.T) {
	haPartitionedLeaderStopsIssuingInstructions(t, memoryWorld)
}
