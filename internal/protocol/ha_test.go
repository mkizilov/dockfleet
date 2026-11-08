package protocol

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dockfleet/dockfleet/internal/election"
	"github.com/dockfleet/dockfleet/internal/logstream"
	"github.com/dockfleet/dockfleet/internal/registry"
	"github.com/dockfleet/dockfleet/internal/store"
)

// These tests put several managers behind one client: they share the database,
// each has sessions of its own, and what each is allowed to say is scripted.

type fenceStep struct {
	epoch int64
	err   error
}

// scriptFence answers Epoch with its steps in turn and repeats the last one.
type scriptFence struct {
	mu    sync.Mutex
	steps []fenceStep
	next  int
}

func (f *scriptFence) set(steps ...fenceStep) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.steps, f.next = steps, 0
}

func leading(epoch int64) fenceStep { return fenceStep{epoch: epoch} }
func following() fenceStep          { return fenceStep{err: election.ErrNotLeader} }

func (f *scriptFence) Epoch() (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	step := f.steps[min(f.next, len(f.steps)-1)]
	f.next++
	return step.epoch, step.err
}

type member struct {
	name  string
	lis   *bufconn.Listener
	srv   *grpc.Server
	fence *scriptFence
	hub   *logstream.Hub
}

type cluster struct {
	store   *store.Memory
	members []*member
}

func newCluster(t *testing.T, n int, withLogs bool) *cluster {
	t.Helper()
	cl := &cluster{store: store.NewMemory()}
	if _, err := cl.store.CreateEnrollment(context.Background(), store.Enrollment{NodeID: "node-1", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		m := &member{name: string(rune('a' + i)), lis: bufconn.Listen(1 << 20), fence: &scriptFence{steps: []fenceStep{following()}}}
		logger := discardLogger()
		node := NewNodeServer(registry.New(cl.store, logger), 5*time.Second, logger).WithFencing(m.fence)
		if withLogs {
			m.hub = logstream.NewHub(logstream.HubConfig{})
			t.Cleanup(m.hub.Shutdown)
			node.WithLogs(m.hub, 10*time.Millisecond)
		}
		srv, err := NewGRPCServer(node, ServerOptions{Token: serverToken, HandlerTimeout: 2 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		m.srv = srv
		go func() { _ = srv.Serve(m.lis) }()
		t.Cleanup(srv.Stop)
		cl.members = append(cl.members, m)
	}
	return cl
}

func (cl *cluster) target(name string) string { return "passthrough:///" + name }

// client is a client that knows the managers in the given order (all of them if none).
func (cl *cluster) client(t *testing.T, order ...int) *Client {
	t.Helper()
	if len(order) == 0 {
		for i := range cl.members {
			order = append(order, i)
		}
	}
	var addrs []string
	for _, i := range order {
		addrs = append(addrs, cl.target(cl.members[i].name))
	}
	c, err := Dial(ClientConfig{
		Addrs: addrs, Token: serverToken,
		Dialer: func(ctx context.Context, addr string) (net.Conn, error) {
			for _, m := range cl.members {
				if addr == m.name {
					return m.lis.DialContext(ctx)
				}
			}
			return nil, errors.New("no such manager: " + addr)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestOnlyTheLeaderAnswersAgentsAndItStampsItsEpoch(t *testing.T) {
	cl := newCluster(t, 3, false)
	cl.members[1].fence.set(leading(7))
	ctx := ctxWithTimeout(t)

	// A follower refuses every call, and changes nothing.
	for _, i := range []int{0, 2} {
		only := cl.client(t, i)
		_, err := only.Register(ctx, testNode())
		if !IsNotLeader(err) || status.Code(err) != codes.FailedPrecondition || IsPermanent(err) || !NeedsRegistration(err) {
			t.Fatalf("Register at a follower = %v, want a retryable NOT_LEADER that asks for a new registration", err)
		}
		if _, err := only.Heartbeat(ctx, "node-1", "any", nil); !IsNotLeader(err) {
			t.Errorf("Heartbeat at a follower = %v", err)
		}
		if err := only.Deregister(ctx, "node-1", "any"); !IsNotLeader(err) {
			t.Errorf("Deregister at a follower = %v", err)
		}
	}
	if _, err := cl.store.GetNode(ctx, "node-1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("a follower registered the node: %v", err)
	}

	// The client that knows all three finds the leader, and says where it is.
	c := cl.client(t)
	session, err := c.Register(ctx, testNode())
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if session.Epoch != 7 || c.Epoch() != 7 || c.Endpoint() != cl.target("b") {
		t.Fatalf("session epoch %d, client epoch %d, endpoint %s; want 7, 7, b", session.Epoch, c.Epoch(), c.Endpoint())
	}
	if _, err := cl.store.CreateWorkload(ctx, store.Workload{ID: "w1", Name: "web", Image: "nginx", CPUMillis: 1, MemoryBytes: 1, NodeID: "node-1", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	assigned, err := c.Heartbeat(ctx, "node-1", session.ID, nil)
	if err != nil || len(assigned) != 1 || assigned[0].GetWorkloadId() != "w1" {
		t.Fatalf("Heartbeat = %v, %v", assigned, err)
	}
	if err := c.Deregister(ctx, "node-1", session.ID); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
}

func TestAgentRefusesAnswersFromAControlPlaneThatWasReplaced(t *testing.T) {
	cl := newCluster(t, 1, false)
	leader := cl.members[0].fence
	leader.set(leading(5))
	c := cl.client(t)
	ctx := ctxWithTimeout(t)
	session, err := c.Register(ctx, testNode())
	if err != nil || c.Epoch() != 5 {
		t.Fatalf("Register = %+v, %v (client epoch %d)", session, err, c.Epoch())
	}
	if _, err := cl.store.CreateWorkload(ctx, store.Workload{ID: "w1", Name: "web", Image: "nginx", CPUMillis: 1, MemoryBytes: 1, NodeID: "node-1", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}

	// The same manager answers under an older epoch, as one that was replaced would.
	leader.set(leading(4))
	assigned, err := c.Heartbeat(ctx, "node-1", session.ID, nil)
	if !IsStaleControlPlane(err) || !NeedsRegistration(err) || IsPermanent(err) || assigned != nil {
		t.Fatalf("Heartbeat under epoch 4 after 5 = %v, %v; want its instructions refused with STALE_CONTROL_PLANE", assigned, err)
	}
	if c.Epoch() != 5 {
		t.Fatalf("a refused answer changed the client's epoch to %d", c.Epoch())
	}
	if _, err := c.Register(ctx, testNode()); !IsStaleControlPlane(err) {
		t.Fatalf("Register under epoch 4 after 5 = %v, want STALE_CONTROL_PLANE", err)
	}
	// That manager did register the node before the client looked at the epoch, and
	// its new session replaced the old one: the agent registers again, as it does.
	leader.set(leading(5))
	if _, err := c.Heartbeat(ctx, "node-1", session.ID, nil); !NeedsRegistration(err) {
		t.Fatalf("Heartbeat on the replaced session = %v, want a request to register again", err)
	}
	if session, err = c.Register(ctx, testNode()); err != nil || session.Epoch != 5 {
		t.Fatalf("Register under epoch 5 = %+v, %v", session, err)
	}

	// The same epoch is fine, a newer one is taken over, and then the old one is stale again.
	if assigned, err := c.Heartbeat(ctx, "node-1", session.ID, nil); err != nil || len(assigned) != 1 {
		t.Fatalf("Heartbeat under epoch 5 = %v, %v", assigned, err)
	}
	leader.set(leading(6))
	if _, err := c.Heartbeat(ctx, "node-1", session.ID, nil); err != nil || c.Epoch() != 6 {
		t.Fatalf("Heartbeat under epoch 6 = %v (client epoch %d)", err, c.Epoch())
	}
	leader.set(leading(5))
	if _, err := c.Heartbeat(ctx, "node-1", session.ID, nil); !IsStaleControlPlane(err) {
		t.Fatalf("Heartbeat under epoch 5 after 6 = %v", err)
	}
}

func TestRegistrationPassesOverAReplacedManagerAndFindsTheCurrentLeader(t *testing.T) {
	cl := newCluster(t, 3, false)
	a, b, cc := cl.members[0], cl.members[1], cl.members[2]
	b.fence.set(leading(2))
	c := cl.client(t, 1, 0, 2) // b, a, c
	ctx := ctxWithTimeout(t)
	if s, err := c.Register(ctx, testNode()); err != nil || s.Epoch != 2 {
		t.Fatalf("Register = %+v, %v", s, err)
	}

	// b stops leading; a has not noticed that it was replaced long ago and still
	// says it leads, under epoch 1; c follows. Nobody the agent may listen to leads.
	b.fence.set(following())
	a.fence.set(leading(1))
	cc.fence.set(following())
	_, err := c.Register(ctx, testNode())
	if err == nil || !NeedsRegistration(err) {
		t.Fatalf("Register with no current leader = %v, want a refusal that asks to try again", err)
	}
	if c.Epoch() != 2 {
		t.Fatalf("the client's epoch is %d; it must not go back to 1", c.Epoch())
	}

	// A new leader is elected, under epoch 3: found, wherever the client was looking.
	cc.fence.set(leading(3))
	s, err := c.Register(ctx, testNode())
	if err != nil || s.Epoch != 3 || c.Endpoint() != cl.target("c") {
		t.Fatalf("Register after the election = %+v, %v (endpoint %s)", s, err, c.Endpoint())
	}
}

func TestNoAnswerIsSentByAManagerThatStoppedLeadingWhileMakingIt(t *testing.T) {
	cl := newCluster(t, 1, false)
	f := cl.members[0].fence
	c := cl.client(t)
	ctx := ctxWithTimeout(t)

	// Still the leader at the start, no longer when the answer is ready.
	f.set(leading(4), following())
	if s, err := c.Register(ctx, testNode()); !IsNotLeader(err) || s.ID != "" {
		t.Fatalf("Register = %+v, %v; the answer of a term that ended must not be sent", s, err)
	}
	// Or a different term: the answer would carry instructions of the one before.
	f.set(leading(4), leading(5))
	if _, err := c.Register(ctx, testNode()); !IsNotLeader(err) {
		t.Fatalf("Register across two terms = %v, want NOT_LEADER", err)
	}
	f.set(leading(4))
	session, err := c.Register(ctx, testNode())
	if err != nil {
		t.Fatal(err)
	}
	f.set(leading(4), following())
	if assigned, err := c.Heartbeat(ctx, "node-1", session.ID, nil); !IsNotLeader(err) || assigned != nil {
		t.Fatalf("Heartbeat = %v, %v", assigned, err)
	}
}

func TestAnAgentWhoseManagerGoesAwayMovesOnAndFindsTheNewLeader(t *testing.T) {
	cl := newCluster(t, 2, false)
	a, b := cl.members[0], cl.members[1]
	a.fence.set(leading(1))
	c := cl.client(t)
	ctx := ctxWithTimeout(t)
	session, err := c.Register(ctx, testNode())
	if err != nil || c.Endpoint() != cl.target("a") {
		t.Fatalf("Register = %+v, %v (endpoint %s)", session, err, c.Endpoint())
	}

	// a dies. The heartbeat fails the way a dead peer's does, and the client turns to b.
	a.srv.Stop()
	_, err = c.Heartbeat(ctx, "node-1", session.ID, nil)
	if err == nil || NeedsRegistration(err) || c.Endpoint() != cl.target("b") {
		t.Fatalf("Heartbeat to a dead manager = %v (endpoint %s); want a plain failure, and the client on b", err, c.Endpoint())
	}
	// b does not lead yet: it tells the agent to look for the leader again.
	b.fence.set(following())
	if _, err := c.Heartbeat(ctx, "node-1", session.ID, nil); !IsNotLeader(err) || !NeedsRegistration(err) {
		t.Fatalf("Heartbeat to a follower = %v", err)
	}
	// b is elected; registering finds it, with the higher epoch.
	b.fence.set(leading(2))
	s, err := c.Register(ctx, testNode())
	if err != nil || s.Epoch != 2 || c.Endpoint() != cl.target("b") {
		t.Fatalf("Register = %+v, %v (endpoint %s)", s, err, c.Endpoint())
	}
	if _, err := c.Heartbeat(ctx, "node-1", s.ID, nil); err != nil {
		t.Fatalf("Heartbeat to the new leader: %v", err)
	}
}

func TestOneManagerDoesNotMoveTheClientOnItsFailures(t *testing.T) {
	cl := newCluster(t, 1, false)
	cl.members[0].fence.set(leading(1))
	c := cl.client(t)
	ctx := ctxWithTimeout(t)
	session, err := c.Register(ctx, testNode())
	if err != nil {
		t.Fatal(err)
	}
	cl.members[0].srv.Stop()
	if _, err := c.Heartbeat(ctx, "node-1", session.ID, nil); err == nil {
		t.Fatal("Heartbeat to a stopped manager succeeded")
	}
	if c.Endpoint() != cl.target("a") {
		t.Errorf("endpoint = %s", c.Endpoint())
	}
}

func TestLogStreamsAreServedByTheLeaderOnly(t *testing.T) {
	t.Run("a follower refuses the stream", func(t *testing.T) {
		cl := newCluster(t, 1, true)
		c := cl.client(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, err := c.Logs(ctx, "node-1", "some-session")
		if err == nil {
			_, err = conn.Recv()
		}
		if !IsNotLeader(err) {
			t.Fatalf("Logs at a follower = %v, want NOT_LEADER", err)
		}
	})
	t.Run("the stream of a manager that stops leading is closed", func(t *testing.T) {
		cl := newCluster(t, 1, true)
		m := cl.members[0]
		m.fence.set(leading(3))
		c := cl.client(t)
		session, err := c.Register(ctxWithTimeout(t), testNode())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		conn, err := c.Logs(ctx, "node-1", session.ID)
		if err != nil {
			t.Fatal(err)
		}
		waitUntil(t, "the stream to attach", func() bool { return m.hub.Connected("node-1") })

		m.fence.set(following())
		if _, err := conn.Recv(); !IsNotLeader(err) {
			t.Fatalf("Recv = %v, want NOT_LEADER once the manager stopped leading", err)
		}
		waitUntil(t, "the stream to detach", func() bool { return !m.hub.Connected("node-1") })
	})
}

func TestEpochsOnlyEverGoUpOnTheClient(t *testing.T) {
	c, err := Dial(ClientConfig{Addr: "127.0.0.1:1", Token: serverToken})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, step := range []struct {
		epoch int64
		ok    bool
		seen  int64
	}{
		{0, true, 0}, {3, true, 3}, {3, true, 3}, {2, false, 3}, {0, false, 3}, {-1, false, 3}, {4, true, 4}, {3, false, 4}, {9, true, 9},
	} {
		err := c.admit(step.epoch)
		if (err == nil) != step.ok || c.Epoch() != step.seen {
			t.Errorf("admit(%d) = %v, epoch now %d; want ok=%v, epoch %d", step.epoch, err, c.Epoch(), step.ok, step.seen)
		}
		if err != nil && !IsStaleControlPlane(err) {
			t.Errorf("admit(%d) = %v, want STALE_CONTROL_PLANE", step.epoch, err)
		}
	}
	var wg sync.WaitGroup
	for i := int64(1); i <= 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = c.admit(100 + i) }()
	}
	wg.Wait()
	if c.Epoch() != 150 {
		t.Errorf("the highest of many concurrent epochs is %d, want 150", c.Epoch())
	}
}

func TestEveryManagerAddressIsCheckedForPlaintext(t *testing.T) {
	if _, err := Dial(ClientConfig{Addrs: []string{"127.0.0.1:21281", "manager.example.com:21281"}, Token: serverToken}); !errors.Is(err, ErrInsecureRemote) {
		t.Fatalf("a remote address in the list: %v, want ErrInsecureRemote", err)
	}
	c, err := Dial(ClientConfig{Addrs: []string{"127.0.0.1:21281", "[::1]:21282"}, Token: serverToken})
	if err != nil {
		t.Fatalf("loopback addresses: %v", err)
	}
	defer c.Close()
	if c.Endpoint() != "127.0.0.1:21281" {
		t.Errorf("the client starts with %s, want the first address", c.Endpoint())
	}
}

func TestVersionAdvertisesHighAvailability(t *testing.T) {
	v := CurrentVersion()
	if v.GetMajor() != 1 || v.GetMinor() < 5 {
		t.Fatalf("version = %v; the epoch and NOT_LEADER arrived in 1.5", v)
	}
}
