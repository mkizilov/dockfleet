package integration

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dockfleet/dockfleet/internal/agent"
	"github.com/dockfleet/dockfleet/internal/docker"
	"github.com/dockfleet/dockfleet/internal/dockerfake"
	"github.com/dockfleet/dockfleet/internal/election"
	"github.com/dockfleet/dockfleet/internal/protocol"
	"github.com/dockfleet/dockfleet/internal/registry"
	"github.com/dockfleet/dockfleet/internal/store"
)

// settableFence is the fencing source of a protocol server whose leadership the
// test decides, so that a manager can be made to believe, wrongly, that it leads.
type settableFence struct {
	epoch   atomic.Int64
	leading atomic.Bool
}

func (f *settableFence) Epoch() (int64, error) {
	if !f.leading.Load() {
		return 0, election.ErrNotLeader
	}
	return f.epoch.Load(), nil
}

func (f *settableFence) lead(epoch int64) {
	f.epoch.Store(epoch)
	f.leading.Store(true)
}

func (f *settableFence) follow() { f.leading.Store(false) }

// view is a manager as an agent sees it: a protocol server over its own picture of
// the cluster. Two views differ when one manager has not heard what the other did.
type view struct {
	name      string
	store     *store.Memory
	fence     *settableFence
	lis       *bufconn.Listener
	registers atomic.Int64
	beats     atomic.Int64
}

func newView(t *testing.T, name string) *view {
	t.Helper()
	v := &view{name: name, store: store.NewMemory(), fence: &settableFence{}, lis: bufconn.Listen(1 << 20)}
	if _, err := v.store.CreateEnrollment(context.Background(), store.Enrollment{NodeID: "sim-1", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	logger := discardLogger()
	node := protocol.NewNodeServer(registry.New(v.store, logger), heartbeatEvery, logger).WithFencing(v.fence)
	srv, err := protocol.NewGRPCServer(node, protocol.ServerOptions{
		Token: agentToken, HandlerTimeout: 2 * time.Second,
		Observe: func(method string, _ codes.Code, _ time.Duration) {
			switch {
			case strings.HasSuffix(method, "/RegisterNode"):
				v.registers.Add(1)
			case strings.HasSuffix(method, "/Heartbeat"):
				v.beats.Add(1)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(v.lis) }()
	t.Cleanup(srv.Stop)
	return v
}

func (v *view) hasNode(id string) bool {
	_, err := v.store.GetNode(context.Background(), id)
	return err == nil
}

func (v *view) addWorkload(t *testing.T, desired string) {
	t.Helper()
	ctx := context.Background()
	if _, err := v.store.CreateWorkload(ctx, store.Workload{ID: "w1", Name: "web", Image: "img", CPUMillis: 100, MemoryBytes: 64 << 20, NodeID: "sim-1", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if desired == store.DesiredStopped {
		if _, err := v.store.SetWorkloadDesiredState(ctx, "w1", store.DesiredStopped, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
}

// An agent that has heard from a control plane of some epoch refuses what a
// control plane of a lower one tells it, so a manager that was replaced and does
// not know it cannot undo what its successor decided. A control plane of a higher
// epoch is obeyed, whoever it is.
func TestAgentsIgnoreInstructionsFromAControlPlaneThatWasReplaced(t *testing.T) {
	a, b := newView(t, "a"), newView(t, "b")
	byName := map[string]*view{"a": a, "b": b}
	engine := dockerfake.New("sim-1")

	client, err := protocol.Dial(protocol.ClientConfig{
		Addrs: []string{"passthrough:///a", "passthrough:///b"}, Token: agentToken,
		Dialer: func(ctx context.Context, addr string) (net.Conn, error) { return byName[addr].lis.DialContext(ctx) },
		// A refused connection to a dead manager should not slow the test down.
		DialOptions: []grpc.DialOption{grpc.WithDisableRetry()},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	ag := agent.New(agent.Config{
		Node: nodeInfo("sim-1"), Manager: client, Docker: engine, Logger: discardLogger(),
		RPCTimeout: time.Second, RetryMin: 5 * time.Millisecond, RetryMax: 20 * time.Millisecond, Resync: 10 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan error, 1)
	go func() { exited <- ag.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-exited:
		case <-time.After(haWait):
			t.Error("the agent did not stop")
		}
	})
	running := func() int {
		n := 0
		for _, info := range engine.Managed() {
			if info.State == docker.StateRunning {
				n++
			}
		}
		return n
	}

	// 1. a leads under epoch 1 and tells the agent to run w1.
	a.fence.lead(1)
	waitLong(t, "the agent to register with a", func() bool { return a.hasNode("sim-1") })
	a.addWorkload(t, store.DesiredRunning)
	waitLong(t, "w1 to run", func() bool { return running() == 1 })
	if client.Epoch() != 1 {
		t.Fatalf("the agent has heard epoch %d, want 1", client.Epoch())
	}

	// 2. b is elected under epoch 2 and, knowing better, wants w1 gone.
	a.fence.follow()
	b.fence.lead(2)
	waitLong(t, "the agent to register with b", func() bool { return b.hasNode("sim-1") })
	b.addWorkload(t, store.DesiredStopped)
	waitLong(t, "w1 to be stopped and removed on b's say", func() bool { return len(engine.Managed()) == 0 })
	if client.Epoch() != 2 {
		t.Fatalf("the agent has heard epoch %d, want 2", client.Epoch())
	}
	if engine.Count("create") != 1 || engine.Count("remove") != 1 {
		t.Fatalf("calls = %v", engine.Calls())
	}

	// 3. a, which still has the old picture, believes it leads under epoch 1 again,
	// and b has gone quiet. The agent is offered a's instructions (w1 runs) many
	// times, and takes none of them.
	b.fence.follow()
	a.fence.lead(1)
	offered := a.registers.Load()
	waitLong(t, "a to offer its instructions again and again", func() bool { return a.registers.Load() >= offered+8 })
	if len(engine.Managed()) != 0 || engine.Count("create") != 1 || engine.Count("start") != 1 {
		t.Fatalf("the agent obeyed a control plane that was replaced: %v", engine.Calls())
	}
	if client.Epoch() != 2 {
		t.Fatalf("the agent has heard epoch %d, want it to stay at 2", client.Epoch())
	}
	select {
	case err := <-exited:
		t.Fatalf("the agent gave up (%v) instead of waiting for a leader it may listen to", err)
	default:
	}

	// 4. b leads again, and the agent goes on with it.
	a.fence.follow()
	b.fence.lead(2)
	served := b.registers.Load()
	waitLong(t, "the agent to be served by b again", func() bool { return b.registers.Load() > served })
	if len(engine.Managed()) != 0 || engine.Count("create") != 1 {
		t.Fatalf("calls = %v", engine.Calls())
	}

	// 5. a is elected again, under epoch 3. Its epoch is higher, so the agent listens
	// to it, but its picture of w1 is still the old one (generation 1): an instruction
	// older than the one in force is not obeyed, whoever sends it, so nothing starts.
	b.fence.follow()
	a.fence.lead(3)
	waitLong(t, "the agent to listen to epoch 3", func() bool { return client.Epoch() == 3 })
	beaten := a.beats.Load()
	waitLong(t, "the agent to heartbeat with a again and again", func() bool { return a.beats.Load() >= beaten+5 })
	if running() != 0 || engine.Count("create") != 1 {
		t.Fatalf("an instruction older than the one in force was obeyed: %v", engine.Calls())
	}

	// 6. a's picture moves on: w1 is stopped and then wanted again, which makes generation
	// 3. That is newer than anything the agent holds, and from the newest epoch: obeyed.
	a.setDesired(t, store.DesiredStopped)
	a.setDesired(t, store.DesiredRunning)
	waitLong(t, "the agent to obey a newer instruction from epoch 3", func() bool { return running() == 1 })
	if client.Epoch() != 3 || engine.Count("create") != 2 {
		t.Fatalf("epoch %d, calls %v", client.Epoch(), engine.Calls())
	}
}

// setDesired changes what this view wants of w1, which raises its generation.
func (v *view) setDesired(t *testing.T, desired string) {
	t.Helper()
	if _, err := v.store.SetWorkloadDesiredState(context.Background(), "w1", desired, time.Now()); err != nil {
		t.Fatal(err)
	}
}
