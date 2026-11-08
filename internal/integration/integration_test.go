// These tests use an in-memory bufconn transport, so no ports are used; the
// agents are the real agent.Agent loop on an in-memory Docker engine.
package integration

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dockfleet/dockfleet/internal/agent"
	"github.com/dockfleet/dockfleet/internal/docker"
	"github.com/dockfleet/dockfleet/internal/dockerfake"
	"github.com/dockfleet/dockfleet/internal/logstream"
	"github.com/dockfleet/dockfleet/internal/placement"
	"github.com/dockfleet/dockfleet/internal/protocol"
	dockfleetv1 "github.com/dockfleet/dockfleet/internal/protocol/dockfleet/v1"
	"github.com/dockfleet/dockfleet/internal/registry"
	"github.com/dockfleet/dockfleet/internal/scheduler"
	"github.com/dockfleet/dockfleet/internal/store"
)

const (
	agentToken = "agent-token-0123456789"
	// Short so tests converge quickly; the manager hands it to agents on registration.
	heartbeatEvery = 20 * time.Millisecond
)

var errManagerDown = errors.New("manager is down")

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// manager is a restartable in-process manager: node and workload state lives
// in the store and survives restarts, sessions (the registry) do not.
type manager struct {
	t         *testing.T
	store     *store.Memory
	version   *dockfleetv1.ProtocolVersion
	serverTLS *tls.Config
	logs      *logCounter
	dialsTry  atomic.Int64
	// skew shifts the clock the server uses for per-call certificate checks.
	skew atomic.Int64
	// lease is the lease, in nanoseconds, the manager grants each session; zero grants none.
	lease atomic.Int64
	// fence makes the agents started afterwards stop their containers when their lease runs out.
	fence bool
	// backoff, when set, is the shortest wait between restarts of a container whose
	// restart policy sets none (the longest is four times as long).
	backoff time.Duration

	mu  sync.Mutex
	lis *bufconn.Listener
	srv *grpc.Server
	reg *registry.Registry
	hub *logstream.Hub
}

// newManager returns a stopped manager; call start to bring it up. The nodes
// sim-1, sim-2 and sim-3 are already enrolled.
func newManager(t *testing.T, version *dockfleetv1.ProtocolVersion) *manager {
	t.Helper()
	m := &manager{t: t, store: store.NewMemory(), version: version}
	m.enroll("sim-1", "sim-2", "sim-3")
	t.Cleanup(m.stop)
	return m
}

func (m *manager) enroll(ids ...string) {
	m.t.Helper()
	for _, id := range ids {
		if _, err := m.store.CreateEnrollment(context.Background(), store.Enrollment{NodeID: id, CreatedAt: time.Now()}); err != nil {
			m.t.Fatal(err)
		}
	}
}

func (m *manager) now() time.Time {
	return time.Now().Add(time.Duration(m.skew.Load()))
}

// logCounter is a slog handler that counts messages, so tests can observe
// decisions the manager makes (for example rejections) without log parsing.
type logCounter struct {
	mu     sync.Mutex
	counts map[string]int
}

func (l *logCounter) Enabled(context.Context, slog.Level) bool { return true }

func (l *logCounter) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.counts == nil {
		l.counts = map[string]int{}
	}
	l.counts[r.Message]++
	return nil
}

func (l *logCounter) WithAttrs([]slog.Attr) slog.Handler { return l }

func (l *logCounter) WithGroup(string) slog.Handler { return l }

func (l *logCounter) count(msg string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.counts[msg]
}

func (m *manager) start() {
	logger := discardLogger()
	if m.logs != nil {
		logger = slog.New(m.logs)
	}
	reg := registry.New(m.store, logger)
	hub := logstream.NewHub(logstream.HubConfig{Logger: logger, ReconnectWait: 5 * time.Second})
	node := protocol.NewNodeServer(reg, heartbeatEvery, logger).WithLease(time.Duration(m.lease.Load())).WithLogs(hub, 10*time.Millisecond)
	if m.version != nil {
		node.WithVersion(m.version)
	}
	lis := bufconn.Listen(1 << 20)
	opts := protocol.ServerOptions{HandlerTimeout: 2 * time.Second, Now: m.now}
	if m.serverTLS != nil {
		opts.TLS = m.serverTLS
	} else {
		opts.Token = agentToken
	}
	srv, err := protocol.NewGRPCServer(node, opts)
	if err != nil {
		m.t.Fatal(err)
	}
	go func() { _ = srv.Serve(lis) }()

	m.mu.Lock()
	m.lis, m.srv, m.reg, m.hub = lis, srv, reg, hub
	m.mu.Unlock()
}

func (m *manager) stop() {
	m.mu.Lock()
	srv, hub := m.srv, m.hub
	m.lis, m.srv, m.hub = nil, nil, nil
	m.mu.Unlock()
	if hub != nil {
		hub.Shutdown()
	}
	if srv != nil {
		srv.Stop()
	}
}

// restart simulates a manager crash: sessions are lost, stored state is kept.
func (m *manager) restart() {
	m.stop()
	m.start()
}

func (m *manager) sessions() []registry.Session {
	m.mu.Lock()
	reg := m.reg
	up := m.srv != nil
	m.mu.Unlock()
	if reg == nil || !up {
		return nil
	}
	return reg.Sessions()
}

func (m *manager) dial(mutate func(*protocol.ClientConfig)) *protocol.Client {
	m.t.Helper()
	cfg := protocol.ClientConfig{
		Addr:  "passthrough:///bufnet",
		Token: agentToken,
		Dialer: func(ctx context.Context, _ string) (net.Conn, error) {
			m.dialsTry.Add(1)
			m.mu.Lock()
			lis := m.lis
			m.mu.Unlock()
			if lis == nil {
				return nil, errManagerDown
			}
			return lis.DialContext(ctx)
		},
		DialOptions: []grpc.DialOption{grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 5 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: 50 * time.Millisecond},
			MinConnectTimeout: time.Second,
		})},
	}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := protocol.Dial(cfg)
	if err != nil {
		m.t.Fatal(err)
	}
	m.t.Cleanup(func() { _ = c.Close() })
	return c
}

func nodeInfo(id string) protocol.NodeInfo {
	return protocol.NodeInfo{
		ID:           id,
		Name:         "simulated " + id,
		CPUMillis:    2000,
		MemoryBytes:  1 << 30,
		Labels:       map[string]string{"role": "sim"},
		AgentVersion: "sim-1.0",
	}
}

// simAgent is a real agent.Agent loop running in the background, on its own
// in-memory Docker engine.
type simAgent struct {
	t      *testing.T
	agent  *agent.Agent
	cancel context.CancelFunc
	exited chan struct{}
	result error
	engine *dockerfake.Fake
}

// registered reports whether the agent itself holds a session. The manager has
// one a moment earlier, as soon as it has handled the request; an agent that is
// stopped in between has not yet been told its session ID and cannot end it.
func (s *simAgent) registered() bool {
	return s.agent.Readiness(context.Background()).Registered
}

func (m *manager) startAgent(id string, mutate func(*protocol.ClientConfig)) *simAgent {
	m.t.Helper()
	return m.startAgentOn(dockerfake.New(id), id, mutate)
}

// startAgentOn starts an agent for node id on an existing engine, which is how
// a restarted agent finds the containers its predecessor left behind.
func (m *manager) startAgentOn(engine *dockerfake.Fake, id string, mutate func(*protocol.ClientConfig)) *simAgent {
	m.t.Helper()
	s := m.runAgent(engine, id, mutate)
	s.engine = engine
	return s
}

// runAgent starts an agent for node id on any container runtime.
func (m *manager) runAgent(rt docker.Runtime, id string, mutate func(*protocol.ClientConfig)) *simAgent {
	m.t.Helper()
	a := agent.New(agent.Config{
		Node:       nodeInfo(id),
		Manager:    m.dial(mutate),
		Docker:     rt,
		Logger:     discardLogger(),
		RPCTimeout: time.Second,
		RetryMin:   5 * time.Millisecond,
		RetryMax:   20 * time.Millisecond,
		Resync:     10 * time.Millisecond,

		FenceOnLeaseLoss: m.fence,
		BackoffBase:      m.backoff,
		BackoffMax:       4 * m.backoff,
	})
	ctx, cancel := context.WithCancel(context.Background())
	s := &simAgent{t: m.t, agent: a, cancel: cancel, exited: make(chan struct{})}
	go func() {
		s.result = a.Run(ctx)
		close(s.exited)
	}()
	m.t.Cleanup(func() { s.stop() })
	return s
}

// stop cancels the agent, waits for it to exit and returns Run's result.
func (s *simAgent) stop() error {
	s.cancel()
	return s.wait()
}

// wait returns Run's result without canceling.
func (s *simAgent) wait() error {
	select {
	case <-s.exited:
		return s.result
	case <-time.After(5 * time.Second):
		s.t.Error("agent did not exit")
		return errors.New("agent did not exit")
	}
}

func (s *simAgent) running() bool {
	select {
	case <-s.exited:
		return false
	default:
		return true
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func (m *manager) node(id string) (store.Node, bool) {
	n, err := m.store.GetNode(context.Background(), id)
	return n, err == nil
}

func (m *manager) sessionID(nodeID string) string {
	for _, s := range m.sessions() {
		if s.NodeID == nodeID {
			return s.ID
		}
	}
	return ""
}

func TestSimulatedAgentsRegisterAndHeartbeat(t *testing.T) {
	m := newManager(t, nil)
	m.start()
	ids := []string{"sim-1", "sim-2", "sim-3"}
	for _, id := range ids {
		m.startAgent(id, nil)
	}

	waitFor(t, "all agents to register", func() bool { return len(m.sessions()) == len(ids) })
	first := map[string]time.Time{}
	for _, id := range ids {
		n, ok := m.node(id)
		if !ok {
			t.Fatalf("node %s was not stored", id)
		}
		if n.CPUMillis != 2000 || n.MemoryBytes != 1<<30 || n.Labels["role"] != "sim" || n.AgentVersion != "sim-1.0" || n.Name != "simulated "+id {
			t.Fatalf("stored node lost its identity or capacity: %+v", n)
		}
		first[id] = n.LastHeartbeat
	}
	waitFor(t, "every node to heartbeat again", func() bool {
		for _, id := range ids {
			if n, _ := m.node(id); !n.LastHeartbeat.After(first[id]) {
				return false
			}
		}
		return true
	})
}

func TestAssignedWorkloadsAreReportedBack(t *testing.T) {
	m := newManager(t, nil)
	m.start()
	m.startAgent("sim-1", nil)
	waitFor(t, "registration", func() bool { return m.sessionID("sim-1") != "" })

	ctx := context.Background()
	now := time.Now()
	if _, err := m.store.UpsertNode(ctx, store.Node{ID: "elsewhere", Name: "elsewhere", CPUMillis: 1000, MemoryBytes: 1 << 30, RegisteredAt: now, LastHeartbeat: now}); err != nil {
		t.Fatal(err)
	}
	for _, w := range []store.Workload{
		{ID: "w-mine", Name: "mine", Image: "nginx:1", CPUMillis: 100, MemoryBytes: 64 << 20, NodeID: "sim-1", CreatedAt: now},
		{ID: "w-theirs", Name: "theirs", Image: "nginx:1", CPUMillis: 100, MemoryBytes: 64 << 20, NodeID: "elsewhere", CreatedAt: now},
	} {
		if _, err := m.store.CreateWorkload(ctx, w); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, "the assigned workload to be running and reported", func() bool {
		w, err := m.store.GetWorkload(ctx, "w-mine")
		return err == nil && w.Phase() == store.WorkloadRunning && w.Observed != nil && w.Observed.Message != "" && w.Observed.NodeID == "sim-1" &&
			w.Observed.AckedGeneration == w.Generation && w.Observed.ContainerID != ""
	})

	// Let the agent heartbeat a few more times, then check it left the other node's workload alone.
	n, _ := m.node("sim-1")
	waitFor(t, "more heartbeats", func() bool {
		cur, _ := m.node("sim-1")
		return cur.LastHeartbeat.After(n.LastHeartbeat.Add(2 * heartbeatEvery))
	})
	other, err := m.store.GetWorkload(ctx, "w-theirs")
	if err != nil || other.Phase() != store.WorkloadScheduled || other.Observed != nil {
		t.Fatalf("another node's workload must not be touched: %+v, %v", other, err)
	}
}

// The scheduler only decides and records. The node it names learns of the
// assignment through the protocol, like any other workload, and an agent here
// has no Docker at all, only an in-memory engine.
func TestScheduledReplicasReachTheirAgents(t *testing.T) {
	m := newManager(t, nil)
	m.start()
	m.startAgent("sim-1", nil)
	m.startAgent("sim-2", nil)
	waitFor(t, "registration", func() bool { return m.sessionID("sim-1") != "" && m.sessionID("sim-2") != "" })

	ctx := context.Background()
	// Each simulated node has 2000m, so two replicas of 1500m go on separate
	// nodes and the third has nowhere to go.
	svc, err := m.store.CreateService(ctx, store.NewService{
		ID: "svc-web", Name: "web", Replicas: 3, At: time.Now(),
		Spec: store.ServiceSpec{Image: "nginx:1", CPUMillis: 1500, MemoryBytes: 64 << 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	report, err := placement.New(placement.Config{Store: m.store, NodeTTL: time.Minute}).RunOnce(ctx)
	if err != nil || report.Placed != 2 || report.Unschedulable != 1 {
		t.Fatalf("scheduling pass = %+v, %v", report, err)
	}

	workloads, err := m.store.ListWorkloadsForService(ctx, svc.ID)
	if err != nil || len(workloads) != 2 || workloads[0].NodeID == workloads[1].NodeID {
		t.Fatalf("workloads = %+v, %v; want one replica on each node", workloads, err)
	}
	waitFor(t, "both agents to report their replica", func() bool {
		for _, w := range workloads {
			got, err := m.store.GetWorkload(ctx, w.ID)
			if err != nil || got.Observed == nil || got.Observed.NodeID != w.NodeID || got.Phase() != store.WorkloadRunning {
				return false
			}
		}
		return true
	})

	waiting, err := m.store.ListUnschedulable(ctx, svc.ID)
	if err != nil || len(waiting) != 1 || waiting[0].ReplicaIndex != 3 || waiting[0].Code != scheduler.CodeInsufficientResources {
		t.Fatalf("unschedulable = %+v, %v", waiting, err)
	}
}

func TestAgentReconnectsAfterManagerRestart(t *testing.T) {
	m := newManager(t, nil)
	m.start()
	a := m.startAgent("sim-1", nil)
	waitFor(t, "registration", func() bool { return m.sessionID("sim-1") != "" })
	before := m.sessionID("sim-1")
	n, _ := m.node("sim-1")

	m.restart()

	waitFor(t, "a new session after the restart", func() bool {
		id := m.sessionID("sim-1")
		return id != "" && id != before
	})
	waitFor(t, "heartbeats on the new session", func() bool {
		cur, _ := m.node("sim-1")
		return cur.LastHeartbeat.After(n.LastHeartbeat)
	})
	if !a.running() {
		t.Fatalf("agent exited during the restart: %v", a.result)
	}
}

func TestAgentSurvivesLongManagerOutage(t *testing.T) {
	m := newManager(t, nil)
	m.start()
	a := m.startAgent("sim-1", nil)
	waitFor(t, "registration", func() bool { return m.sessionID("sim-1") != "" })

	m.stop()
	dials := m.dialsTry.Load()
	waitFor(t, "the agent to keep trying to reach the manager", func() bool { return m.dialsTry.Load() >= dials+3 })
	if !a.running() {
		t.Fatalf("agent gave up during the outage: %v", a.result)
	}

	m.start()
	waitFor(t, "registration after the outage", func() bool { return m.sessionID("sim-1") != "" })
}

func TestAgentRegistersOnceManagerComesUp(t *testing.T) {
	m := newManager(t, nil)
	a := m.startAgent("sim-1", nil)

	waitFor(t, "several failed connection attempts", func() bool { return m.dialsTry.Load() >= 3 })
	if len(m.sessions()) != 0 || !a.running() {
		t.Fatalf("agent must keep retrying while the manager is down (running=%v)", a.running())
	}

	m.start()
	waitFor(t, "registration", func() bool { return m.sessionID("sim-1") != "" })
}

func TestIncompatibleAgentIsRejected(t *testing.T) {
	m := newManager(t, nil)
	m.start()

	tests := map[string]*dockfleetv1.ProtocolVersion{
		"newer major": {Major: protocol.VersionMajor + 1},
		"older major": {Major: protocol.VersionMajor - 1},
		"newer minor": {Major: protocol.VersionMajor, Minor: protocol.VersionMinor + 1},
	}
	for name, version := range tests {
		t.Run(name, func(t *testing.T) {
			id := "old-agent-" + strings.ReplaceAll(name, " ", "-")

			client := m.dial(func(cfg *protocol.ClientConfig) { cfg.Version = version })
			_, err := client.Register(context.Background(), nodeInfo(id))
			var pe *protocol.Error
			if !errors.As(err, &pe) || !protocol.IsIncompatibleVersion(err) {
				t.Fatalf("Register error = %v, want an INCOMPATIBLE_VERSION protocol error", err)
			}
			if pe.Supported.GetMajor() != protocol.VersionMajor || pe.Supported.GetMinor() != protocol.VersionMinor {
				t.Errorf("rejection must report the manager's version, got %v", pe.Supported)
			}

			// The agent loop treats the rejection as permanent and exits with it.
			sim := m.startAgent(id, func(cfg *protocol.ClientConfig) { cfg.Version = version })
			if err := sim.wait(); err == nil || !protocol.IsIncompatibleVersion(err) {
				t.Fatalf("Run returned %v, want an INCOMPATIBLE_VERSION error", err)
			}
			if _, ok := m.node(id); ok {
				t.Error("a rejected agent must not be stored")
			}
			if m.sessionID(id) != "" {
				t.Error("a rejected agent must not get a session")
			}
		})
	}
}

func TestCompatibleMinorVersionsAreAccepted(t *testing.T) {
	// A manager one minor ahead still serves an agent on the older minor.
	m := newManager(t, &dockfleetv1.ProtocolVersion{Major: protocol.VersionMajor, Minor: protocol.VersionMinor + 1})
	m.start()
	m.startAgent("sim-1", nil)
	waitFor(t, "registration with an older-minor agent", func() bool { return m.sessionID("sim-1") != "" })
}

func TestAgentRejectsIncompatibleManager(t *testing.T) {
	// A manager on another major rejects the agent first; the agent surfaces it as a fatal error.
	m := newManager(t, &dockfleetv1.ProtocolVersion{Major: protocol.VersionMajor + 1})
	m.start()
	sim := m.startAgent("sim-1", nil)
	if err := sim.wait(); err == nil || !protocol.IsIncompatibleVersion(err) {
		t.Fatalf("Run returned %v, want an INCOMPATIBLE_VERSION error", err)
	}
	if len(m.sessions()) != 0 {
		t.Error("no session may exist across incompatible versions")
	}
}

func TestBadCredentialsAreFatalForAgents(t *testing.T) {
	m := newManager(t, nil)
	m.start()
	sim := m.startAgent("sim-1", func(cfg *protocol.ClientConfig) { cfg.Token = "wrong-token-0123456789" })

	err := sim.wait()
	if err == nil || status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Run returned %v, want an Unauthenticated error", err)
	}
	if _, ok := m.node("sim-1"); ok {
		t.Error("an unauthenticated agent must not be stored")
	}
}

func TestNewerRegistrationSupersedesOlderSession(t *testing.T) {
	m := newManager(t, nil)
	m.start()
	ctx := context.Background()
	first, second := m.dial(nil), m.dial(nil)

	s1, err := first.Register(ctx, nodeInfo("sim-1"))
	if err != nil {
		t.Fatal(err)
	}
	s2, err := second.Register(ctx, nodeInfo("sim-1"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = first.Heartbeat(ctx, "sim-1", s1.ID, nil)
	if protocol.ErrorCode(err) != dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_SESSION_SUPERSEDED || !protocol.NeedsRegistration(err) {
		t.Fatalf("old session heartbeat error = %v, want SESSION_SUPERSEDED", err)
	}
	if _, err := second.Heartbeat(ctx, "sim-1", s2.ID, nil); err != nil {
		t.Fatalf("new session heartbeat: %v", err)
	}
	_, err = second.Heartbeat(ctx, "ghost", s2.ID, nil)
	if protocol.ErrorCode(err) != dockfleetv1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_NOT_REGISTERED {
		t.Fatalf("unknown node heartbeat error = %v, want NOT_REGISTERED", err)
	}
	if got := m.sessions(); len(got) != 1 || got[0].ID != s2.ID {
		t.Fatalf("sessions = %+v, want only the newest", got)
	}
}

func TestCleanShutdownDeregistersNode(t *testing.T) {
	m := newManager(t, nil)
	m.start()
	a := m.startAgent("sim-1", nil)
	waitFor(t, "the agent to hold its session", func() bool { return m.sessionID("sim-1") != "" && a.registered() })

	if err := a.stop(); err != nil {
		t.Fatalf("Run returned %v after a requested shutdown, want nil", err)
	}
	if got := m.sessions(); len(got) != 0 {
		t.Fatalf("session must be released on clean shutdown: %+v", got)
	}
	n, ok := m.node("sim-1")
	if !ok {
		t.Fatal("the node record must be kept")
	}
	if scheduler.Ready(n, time.Now(), time.Hour) {
		t.Fatalf("a deregistered node must not be schedulable: %+v", n)
	}
}

func TestWorkloadsStayAssignedAcrossReRegistration(t *testing.T) {
	m := newManager(t, nil)
	m.start()
	ctx := context.Background()
	now := time.Now()
	if _, err := m.store.UpsertNode(ctx, store.Node{ID: "sim-1", Name: "sim-1", CPUMillis: 2000, MemoryBytes: 1 << 30, RegisteredAt: now, LastHeartbeat: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.store.CreateWorkload(ctx, store.Workload{ID: "w1", Name: "web", Image: "nginx", CPUMillis: 1, MemoryBytes: 1, NodeID: "sim-1", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}

	client := m.dial(nil)
	session, err := client.Register(ctx, nodeInfo("sim-1"))
	if err != nil {
		t.Fatal(err)
	}
	if len(session.Assignments) != 1 || session.Assignments[0].GetWorkloadId() != "w1" || session.Assignments[0].GetImage() != "nginx" {
		t.Fatalf("registration must return existing assignments: %v", session.Assignments)
	}

	m.restart()
	again, err := m.dial(nil).Register(ctx, nodeInfo("sim-1"))
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Assignments) != 1 || again.ID == session.ID {
		t.Fatalf("after a manager restart: assignments %v, session %q (was %q)", again.Assignments, again.ID, session.ID)
	}
}
