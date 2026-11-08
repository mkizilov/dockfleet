package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/api"
	"github.com/dockfleet/dockfleet/internal/docker"
	"github.com/dockfleet/dockfleet/internal/dockerfake"
	"github.com/dockfleet/dockfleet/internal/protocol"
	"github.com/dockfleet/dockfleet/internal/scheduler"
	"github.com/dockfleet/dockfleet/internal/store"
)

// These tests break a running fleet: agents crash, networks drop heartbeats,
// containers fail, managers go away. Real agents and a real manager talk over
// gRPC, the scheduler and the reconciler run as in the manager process, and
// each node has an in-memory Docker engine. Times are short so that the tests
// converge quickly; the proportions are the manager's own: a node is suspect
// after 0.4 s of silence, its lease lasts 0.6 s, and it is given up on after
// 1.5 s, which is more than the lease and a heartbeat.

const (
	failureLease = 600 * time.Millisecond
	adminToken   = "admin-token-0123456789"
)

var failureTiming = scheduler.Timing{SuspectAfter: 400 * time.Millisecond, UnavailableAfter: 1500 * time.Millisecond}

var errPartitioned = errors.New("network partition")

// partition is a network between one agent and the manager that can be cut and
// mended. Cutting it closes the open connections and makes new ones fail, with
// neither side told why.
type partition struct {
	mu       sync.Mutex
	cut      bool
	conns    []net.Conn
	attempts int // connection attempts made while it was cut
}

func (p *partition) option() func(*protocol.ClientConfig) {
	return func(cfg *protocol.ClientConfig) {
		dial := cfg.Dialer
		cfg.Dialer = func(ctx context.Context, addr string) (net.Conn, error) {
			p.mu.Lock()
			if p.cut {
				p.attempts++
				p.mu.Unlock()
				return nil, errPartitioned
			}
			p.mu.Unlock()
			c, err := dial(ctx, addr)
			if err != nil {
				return nil, err
			}
			p.mu.Lock()
			p.conns = append(p.conns, c)
			p.mu.Unlock()
			return c, nil
		}
	}
}

func (p *partition) cutOff() {
	p.mu.Lock()
	p.cut = true
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (p *partition) heal() {
	p.mu.Lock()
	p.cut = false
	p.mu.Unlock()
}

func (p *partition) failedAttempts() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.attempts
}

// fleet is a manager with a few agents, each on its own engine, all reporting
// to one journal so a test can tell which node acted first.
type fleet struct {
	t       *testing.T
	m       *manager
	journal *dockerfake.Journal
	engines map[string]*dockerfake.Fake
	cuts    map[string]*partition
	agents  map[string]*simAgent
}

// newFleet starts a manager that grants leases and agents that fence themselves
// when theirs runs out, then the control loops. The nodes must be sim-1 to sim-3.
func newFleet(t *testing.T, nodes ...string) *fleet {
	t.Helper()
	m := newManager(t, nil)
	m.lease.Store(int64(failureLease))
	m.fence = true
	m.backoff = 20 * time.Millisecond
	m.start()
	f := &fleet{
		t: t, m: m, journal: &dockerfake.Journal{},
		engines: map[string]*dockerfake.Fake{}, cuts: map[string]*partition{}, agents: map[string]*simAgent{},
	}
	for _, id := range nodes {
		e := dockerfake.New(id).WithJournal(f.journal)
		e.AddImage("nginx:1.0")
		f.engines[id], f.cuts[id] = e, &partition{}
		f.agents[id] = m.startAgentOn(e, id, f.cuts[id].option())
	}
	for _, id := range nodes {
		waitFor(t, id+" to register", func() bool { return m.sessionID(id) != "" })
	}
	m.controlLoopsWith(failureTiming)
	return f
}

// restartAgent starts a new agent process on a node's engine, as after a crash.
func (f *fleet) restartAgent(id string) {
	f.t.Helper()
	f.agents[id] = f.m.startAgentOn(f.engines[id], id, f.cuts[id].option())
}

// crash kills a node's agent without a word: the network drops and the process
// ends before it can deregister. The containers it started keep running.
func (f *fleet) crash(id string) {
	f.t.Helper()
	f.cuts[id].cutOff()
	if err := f.agents[id].stop(); err != nil {
		f.t.Fatalf("the crashed agent returned %v", err)
	}
}

func (f *fleet) health(id string) string {
	n, ok := f.m.node(id)
	if !ok {
		return ""
	}
	return n.Health
}

func (f *fleet) workload(id string) (store.Workload, error) {
	return f.m.store.GetWorkload(context.Background(), id)
}

// meantToRun are the service's workloads that are meant to run, by node.
func (f *fleet) meantToRun(svc store.Service) map[string][]store.Workload {
	f.t.Helper()
	ws, err := f.m.store.ListWorkloadsForService(context.Background(), svc.ID)
	if err != nil {
		f.t.Fatal(err)
	}
	out := map[string][]store.Workload{}
	for _, w := range ws {
		if w.DesiredState == store.DesiredRunning {
			out[w.NodeID] = append(out[w.NodeID], w)
		}
	}
	return out
}

// runningOn is true when exactly n workloads of the service are meant to run, all
// of them on the given nodes, and each is reported running by an agent that has
// seen the latest instruction.
func (f *fleet) runningOn(svc store.Service, n int, nodes ...string) func() bool {
	return func() bool {
		allowed := map[string]bool{}
		for _, id := range nodes {
			allowed[id] = true
		}
		count := 0
		for node, ws := range f.meantToRun(svc) {
			for _, w := range ws {
				if !allowed[node] || w.Observed == nil || w.Observed.State != store.WorkloadRunning || w.Observed.AckedGeneration != w.Generation {
					return false
				}
				count++
			}
		}
		return count == n
	}
}

func (f *fleet) containers() int {
	n := 0
	for _, e := range f.engines {
		n += len(e.Managed())
	}
	return n
}

func (f *fleet) events(resourceType, resourceID string) []store.Event {
	f.t.Helper()
	list, err := f.m.store.ListEvents(context.Background(), store.EventFilter{ResourceType: resourceType, ResourceID: resourceID, Limit: store.MaxEventLimit})
	if err != nil {
		f.t.Fatal(err)
	}
	return list
}

func countAction(events []store.Event, action string) int {
	n := 0
	for _, e := range events {
		if e.Action == action {
			n++
		}
	}
	return n
}

// get asks the administrative API, as an administrator would, and decodes the answer.
func (f *fleet) get(path string, into any) {
	f.t.Helper()
	h := api.New(api.Config{
		Store: f.m.store, Token: adminToken, NodeTTL: failureTiming.SuspectAfter, Timing: failureTiming, NodeLease: failureLease,
		Logger: discardLogger(),
	})
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		f.t.Fatalf("GET %s = %d %s", path, rec.Code, rec.Body)
	}
	if err := json.Unmarshal(rec.Body.Bytes(), into); err != nil {
		f.t.Fatalf("GET %s: %v\n%s", path, err, rec.Body)
	}
}

type nodeJSON struct {
	ID             string     `json:"id"`
	Status         string     `json:"status"`
	Health         string     `json:"health"`
	HealthReason   string     `json:"health_reason"`
	LeaseExpiresAt *time.Time `json:"lease_expires_at"`
}

func (f *fleet) nodeViews() map[string]nodeJSON {
	f.t.Helper()
	var out struct {
		Nodes []nodeJSON `json:"nodes"`
	}
	f.get("/v1/nodes", &out)
	views := map[string]nodeJSON{}
	for _, n := range out.Nodes {
		views[n.ID] = n
	}
	return views
}

type replicaJSON struct {
	WorkloadID   string `json:"workload_id"`
	NodeID       string `json:"node_id"`
	DesiredState string `json:"desired_state"`
	Phase        string `json:"phase"`
	Message      string `json:"message"`
	Health       string `json:"health"`
	RestartCount int    `json:"restart_count"`
	ExitCode     *int   `json:"exit_code"`
	NodeHealth   string `json:"node_health"`
}

type serviceJSON struct {
	Status struct {
		RunningReplicas   int           `json:"running_replicas"`
		FailedReplicas    int           `json:"failed_replicas"`
		UnhealthyReplicas int           `json:"unhealthy_replicas"`
		LostReplicas      int           `json:"lost_replicas"`
		Replicas          []replicaJSON `json:"replicas"`
	} `json:"status"`
}

func (f *fleet) serviceView(name string) serviceJSON {
	f.t.Helper()
	var out serviceJSON
	f.get("/v1/services/"+name, &out)
	return out
}

// A crashed agent cannot say goodbye or stop its containers. The manager notices
// the silence, first doubting the node and then giving up on it, and starts the
// replica on a node that answers. When an agent comes back on the node, it
// learns that the workload it still holds was stopped, removes the container, and
// nothing is moved back.
func TestCrashedAgentsReplicasMoveAndTheNodeIsCleanedUpWhenItReturns(t *testing.T) {
	f := newFleet(t, "sim-1", "sim-2")
	svc := f.m.createService("web", 2, "nginx:1.0")
	waitFor(t, "two replicas running", f.runningOn(svc, 2, "sim-1", "sim-2"))
	on := f.meantToRun(svc)
	if len(on["sim-1"]) != 1 || len(on["sim-2"]) != 1 {
		t.Fatalf("two replicas over two nodes must be spread: %v", on)
	}
	victim, survivor := on["sim-1"][0], on["sim-2"][0]

	f.crash("sim-1")

	waitFor(t, "sim-1 to be suspect", func() bool { return f.health("sim-1") == store.NodeSuspect })
	if got := f.nodeViews()["sim-1"]; got.Health != store.NodeSuspect || got.Status != "not_ready" || !strings.Contains(got.HealthReason, "no new replicas") {
		t.Fatalf("the API's view of the silent node = %+v", got)
	}
	waitFor(t, "sim-1 to be unavailable", func() bool { return f.health("sim-1") == store.NodeUnavailable })
	if got := f.nodeViews()["sim-1"]; got.Health != store.NodeUnavailable || got.LeaseExpiresAt == nil || !strings.Contains(got.HealthReason, "started on other nodes") {
		t.Fatalf("the API's view of the lost node = %+v", got)
	}
	waitFor(t, "both replicas to run on sim-2", f.runningOn(svc, 2, "sim-2"))

	// The survivor was never touched; the lost node's workload was stopped at generation 2.
	if w, err := f.workload(survivor.ID); err != nil || w.Generation != 1 || w.DesiredState != store.DesiredRunning {
		t.Fatalf("the replica on the healthy node changed: %+v, %v", w, err)
	}
	old, err := f.workload(victim.ID)
	if err != nil || old.DesiredState != store.DesiredStopped || old.Generation != 2 || old.NodeID != "sim-1" {
		t.Fatalf("the lost node's workload = %+v, %v; it must be stopped, and kept until the node confirms", old, err)
	}
	// Nobody could stop the container of a crashed agent. That is the one window in
	// which a replica runs twice, and what a lease can only close while the agent lives.
	if got := f.engines["sim-1"].Managed(); len(got) != 1 || got[0].State != docker.StateRunning {
		t.Fatalf("the crashed agent's container = %+v", got)
	}

	// Failures are on the record.
	nodeEvents := f.events("node", "sim-1")
	if countAction(nodeEvents, "node.suspect") != 1 || countAction(nodeEvents, "node.unavailable") != 1 {
		t.Fatalf("node events = %+v", nodeEvents)
	}
	stops := f.events("workload", victim.ID)
	if countAction(stops, "replica.stopped") != 1 || stops[0].Details["reason"] != "node_unavailable" {
		t.Fatalf("events of the moved workload = %+v", stops)
	}
	view := f.serviceView("web")
	var seen bool
	for _, r := range view.Status.Replicas {
		if r.WorkloadID == victim.ID {
			seen = true
			if r.DesiredState != store.DesiredStopped || r.NodeHealth != store.NodeUnavailable || r.NodeID != "sim-1" {
				t.Fatalf("the moved replica in the service's status = %+v", r)
			}
		}
	}
	if !seen || view.Status.RunningReplicas != 2 {
		t.Fatalf("service status = %+v", view.Status)
	}

	// A new agent on sim-1.
	f.cuts["sim-1"].heal()
	f.restartAgent("sim-1")
	waitFor(t, "sim-1 to remove the container it no longer owns", func() bool { return len(f.engines["sim-1"].Managed()) == 0 })
	waitFor(t, "the record of the old workload to go and sim-1 to be healthy again", func() bool {
		_, err := f.workload(victim.ID)
		return errors.Is(err, store.ErrNotFound) && f.health("sim-1") == store.NodeHealthy
	})
	nodeEvents = f.events("node", "sim-1")
	if countAction(nodeEvents, "node.recovered") != 1 {
		t.Fatalf("node events after recovery = %+v", nodeEvents)
	}
	if countAction(f.events("workload", victim.ID), "replica.removed") != 1 {
		t.Fatal("the removal of the old workload is not on the record")
	}
	// One container for each replica in the whole fleet, and the replicas stayed where they were put.
	if f.containers() != 2 || len(f.engines["sim-2"].Managed()) != 2 || !f.runningOn(svc, 2, "sim-2")() {
		t.Fatalf("containers: sim-1 %v, sim-2 %v", f.engines["sim-1"].Managed(), f.engines["sim-2"].Managed())
	}
}

// With the network cut but both processes alive, the agent's lease runs out
// first and it stops its container; only then does the manager give up on the
// node and start the replica elsewhere. The two never run at the same time.
func TestPartitionedAgentStopsItsContainerBeforeTheReplicaStartsElsewhere(t *testing.T) {
	f := newFleet(t, "sim-1", "sim-2")
	svc := f.m.createService("web", 2, "nginx:1.0")
	waitFor(t, "two replicas running", f.runningOn(svc, 2, "sim-1", "sim-2"))
	victim := f.meantToRun(svc)["sim-1"][0]
	survivor := f.meantToRun(svc)["sim-2"][0]

	f.cuts["sim-1"].cutOff()

	waitFor(t, "sim-1's container to be stopped by its own agent", func() bool {
		got := f.engines["sim-1"].Managed()
		return len(got) == 1 && got[0].State == docker.StateExited
	})
	waitFor(t, "the replica to run on sim-2", f.runningOn(svc, 2, "sim-2"))
	var replacement store.Workload
	for _, w := range f.meantToRun(svc)["sim-2"] {
		if w.ReplicaIndex == victim.ReplicaIndex {
			replacement = w
		}
	}
	if replacement.ID == "" || replacement.ID == victim.ID {
		t.Fatalf("no replacement for replica %d: %v", victim.ReplicaIndex, f.meantToRun(svc))
	}

	stopped, created := f.journal.Index("sim-1 stop "+victim.ID), f.journal.Index("sim-2 create "+replacement.ID)
	if stopped < 0 || created < 0 || stopped > created {
		t.Fatalf("the old container must be stopped (entry %d) before the new one is created (entry %d):\n%s",
			stopped, created, strings.Join(f.journal.Entries(), "\n"))
	}
	// Stopped, not removed: a node that gets back in time keeps its work.
	if got := f.engines["sim-1"].Managed(); len(got) != 1 || got[0].WorkloadID != victim.ID {
		t.Fatalf("the fenced container = %+v", got)
	}

	// The network is mended. The agent learns that the workload was moved and removes the
	// container; it does not start it again.
	f.cuts["sim-1"].heal()
	waitFor(t, "sim-1 to remove its container", func() bool { return len(f.engines["sim-1"].Managed()) == 0 })
	waitFor(t, "sim-1 to be healthy and the old record gone", func() bool {
		_, err := f.workload(victim.ID)
		return errors.Is(err, store.ErrNotFound) && f.health("sim-1") == store.NodeHealthy
	})
	if f.engines["sim-1"].Count("start") != 1 {
		t.Fatalf("the fenced container was started again: %v", f.engines["sim-1"].Calls())
	}
	if w, err := f.workload(survivor.ID); err != nil || w.Generation != 1 {
		t.Fatalf("the replica on the healthy node changed: %+v, %v", w, err)
	}
	if f.containers() != 2 {
		t.Fatalf("%d containers for two replicas:\n%s", f.containers(), strings.Join(f.journal.Entries(), "\n"))
	}
}

// A short break in the network is no failure: the lease outlasts it, nothing is
// stopped, nothing is moved.
func TestABriefPartitionIsRiddenOut(t *testing.T) {
	f := newFleet(t, "sim-1", "sim-2")
	svc := f.m.createService("web", 2, "nginx:1.0")
	waitFor(t, "two replicas running", f.runningOn(svc, 2, "sim-1", "sim-2"))
	before := f.meantToRun(svc)

	cut := f.cuts["sim-1"]
	cut.cutOff()
	waitFor(t, "heartbeats to fail a few times", func() bool { return cut.failedAttempts() >= 3 })
	cut.heal()

	waitFor(t, "sim-1 to heartbeat again", func() bool {
		n, ok := f.m.node("sim-1")
		return ok && time.Since(n.LastHeartbeat) < failureLease/4
	})
	if f.engines["sim-1"].Count("stop") != 0 || f.engines["sim-1"].Count("remove") != 0 {
		t.Fatalf("a brief partition stopped something: %v", f.engines["sim-1"].Calls())
	}
	after := f.meantToRun(svc)
	for node, ws := range before {
		if len(after[node]) != len(ws) || after[node][0].ID != ws[0].ID || after[node][0].Generation != 1 {
			t.Fatalf("replicas moved during a brief partition: %v -> %v", before, after)
		}
	}
	if countAction(f.events("node", "sim-1"), "node.unavailable") != 0 {
		t.Fatal("a node that was away for a moment was given up on")
	}
}

// If the manager is away for longer than the leases, every agent stops its
// containers rather than risk running what the manager may start elsewhere.
// When the manager is back they register again and start the same containers: the
// workloads are not replaced, and the pause is not counted as a failure.
func TestAgentsFenceThemselvesDuringALongManagerOutageAndCarryOnAfterwards(t *testing.T) {
	f := newFleet(t, "sim-1")
	svc := f.m.createService("web", 1, "nginx:1.0")
	waitFor(t, "the replica to run", f.runningOn(svc, 1, "sim-1"))
	w := f.meantToRun(svc)["sim-1"][0]
	oldSession := f.m.sessionID("sim-1")

	f.m.stop()
	waitFor(t, "the agent to stop its container when the lease runs out", func() bool {
		got := f.engines["sim-1"].Managed()
		return len(got) == 1 && got[0].State == docker.StateExited
	})
	f.m.start()

	waitFor(t, "the agent to register again", func() bool {
		id := f.m.sessionID("sim-1")
		return id != "" && id != oldSession
	})
	// The store still holds the report from before the outage, so only the engine
	// says whether the container really runs again.
	waitFor(t, "the container to run again", func() bool {
		got := f.engines["sim-1"].Managed()
		return len(got) == 1 && got[0].State == docker.StateRunning
	})
	waitFor(t, "the replica to be reported running again", f.runningOn(svc, 1, "sim-1"))
	e := f.engines["sim-1"]
	if e.Count("create") != 1 || e.Count("start") != 2 || e.Count("remove") != 0 {
		t.Fatalf("the same container must run again, not a new one: %v", e.Calls())
	}
	got, err := f.workload(w.ID)
	if err != nil || got.Generation != 1 || got.DesiredState != store.DesiredRunning || got.Observed.RestartCount != 0 {
		t.Fatalf("the workload after the outage = %+v, %v", got, err)
	}
	waitFor(t, "sim-1 to be healthy", func() bool { return f.health("sim-1") == store.NodeHealthy })
}

// replica is the only replica of a service, as an administrator sees it in the API.
func (f *fleet) replica(name string) replicaJSON {
	f.t.Helper()
	view := f.serviceView(name)
	if len(view.Status.Replicas) != 1 {
		f.t.Fatalf("replicas of %s = %+v", name, view.Status.Replicas)
	}
	return view.Status.Replicas[0]
}

// A container that keeps failing is restarted, each time after a longer wait,
// until its policy's limit is reached; then it is reported failed, with the
// reason, and left alone.
func TestFailingContainerIsRestartedUpToTheLimitAndThenReportedFailed(t *testing.T) {
	f := newFleet(t, "sim-1")
	svc, err := f.m.store.CreateService(context.Background(), store.NewService{
		ID: "svc-flaky", Name: "flaky", Replicas: 1, CreatedBy: "itest", At: time.Now(),
		Spec: store.ServiceSpec{
			Image: "nginx:1.0", CPUMillis: 100, MemoryBytes: 64 << 20,
			RestartPolicy: &store.RestartPolicy{Condition: store.RestartOnFailure, MaxRetries: 3},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the replica to run", f.runningOn(svc, 1, "sim-1"))
	id := f.meantToRun(svc)["sim-1"][0].ID
	engine := f.engines["sim-1"]

	for i := 1; i <= 3; i++ {
		engine.Crash(id, 1)
		waitFor(t, fmt.Sprintf("restart %d", i), func() bool {
			w, err := f.workload(id)
			return err == nil && w.Observed != nil && w.Observed.State == store.WorkloadRunning && w.Observed.RestartCount == i
		})
	}
	engine.Crash(id, 1)
	waitFor(t, "the agent to give up", func() bool {
		w, err := f.workload(id)
		return err == nil && w.Observed != nil && w.Observed.State == store.WorkloadFailed && strings.Contains(w.Observed.Message, "gave up after 3 restarts")
	})
	// Left alone: one container, started once and restarted three times.
	if engine.Count("create") != 1 || engine.Count("start") != 4 {
		t.Fatalf("calls = %v", engine.Calls())
	}

	// The failure is visible: in the service's status ...
	view := f.serviceView("flaky")
	r := f.replica("flaky")
	if view.Status.RunningReplicas != 0 || view.Status.FailedReplicas != 1 || r.Phase != store.WorkloadFailed ||
		r.RestartCount != 3 || r.ExitCode == nil || *r.ExitCode != 1 || !strings.Contains(r.Message, "gave up") {
		t.Fatalf("service status = %+v, replica %+v", view.Status, r)
	}
	// ... and in the audit trail, where each restart was recorded once.
	events := f.events("workload", id)
	if countAction(events, "workload.restarted") != 3 || countAction(events, "workload.failed") < 1 {
		t.Fatalf("events = %+v", events)
	}
	if got := f.workloadsOnRecord(); got != 1 {
		t.Fatalf("a failed replica must not be replaced by the manager: %d workloads", got)
	}
}

func (f *fleet) workloadsOnRecord() int {
	f.t.Helper()
	all, err := f.m.store.ListWorkloads(context.Background())
	if err != nil {
		f.t.Fatal(err)
	}
	return len(all)
}

// A container that runs but fails its health check is stopped and restarted
// like one that crashed, and the manager is told.
func TestUnhealthyContainerIsRestarted(t *testing.T) {
	f := newFleet(t, "sim-1")
	svc, err := f.m.store.CreateService(context.Background(), store.NewService{
		ID: "svc-probe", Name: "probe", Replicas: 1, CreatedBy: "itest", At: time.Now(),
		Spec: store.ServiceSpec{
			Image: "nginx:1.0", CPUMillis: 100, MemoryBytes: 64 << 20,
			HealthCheck: &store.HealthCheck{Command: []string{"/bin/true"}, IntervalSeconds: 1, TimeoutSeconds: 1, Retries: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the replica to run", f.runningOn(svc, 1, "sim-1"))
	id := f.meantToRun(svc)["sim-1"][0].ID
	engine := f.engines["sim-1"]
	if spec, ok := engine.Spec(id); !ok || spec.HealthCheck == nil || spec.HealthCheck.Retries != 1 || spec.HealthCheck.Command[0] != "/bin/true" {
		t.Fatalf("the health check did not reach the container: %+v", spec.HealthCheck)
	}
	waitFor(t, "the health check result to be reported", func() bool {
		w, err := f.workload(id)
		return err == nil && w.Observed != nil && w.Observed.Health == store.HealthHealthy
	})

	engine.SetHealth(id, docker.HealthUnhealthy)
	waitFor(t, "the container to be restarted", func() bool {
		w, err := f.workload(id)
		return err == nil && w.Observed != nil && w.Observed.State == store.WorkloadRunning && w.Observed.RestartCount == 1 && w.Observed.Health == store.HealthHealthy
	})
	if engine.Count("create") != 1 || engine.Count("stop") != 1 || engine.Count("start") != 2 {
		t.Fatalf("calls = %v", engine.Calls())
	}
	events := f.events("workload", id)
	// The restart is always reported, through the restart count. The unhealthy
	// interval before it is only reported if a heartbeat falls into it, which a
	// prompt restart makes unlikely (the registry's tests cover that event).
	if countAction(events, "workload.restarted") != 1 || countAction(events, "workload.unhealthy") > 1 {
		t.Fatalf("events = %+v", events)
	}
	if r := f.replica("probe"); r.Health != store.HealthHealthy || r.RestartCount != 1 {
		t.Fatalf("replica = %+v", r)
	}
}
