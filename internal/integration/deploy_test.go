package integration

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/dockerfake"
	"github.com/dockfleet/dockfleet/internal/placement"
	"github.com/dockfleet/dockfleet/internal/reconcile"
	"github.com/dockfleet/dockfleet/internal/scheduler"
	"github.com/dockfleet/dockfleet/internal/store"
)

// These tests deploy services through the whole control loop: the scheduler
// places replicas, the reconciler retires them, real agents carry the
// assignments out on in-memory Docker engines and report back over gRPC. The
// same flow runs against a real Docker daemon in docker_test.go (build tag
// dockerintegration).

// controlLoops runs the manager's scheduler and reconciler on the store, as the manager process does.
func (m *manager) controlLoops() {
	m.t.Helper()
	m.controlLoopsWith(scheduler.Timing{})
}

// controlLoopsWith runs them with the given trust in silent nodes. Timing.Since
// is when they start, as for a manager process, unless the test sets it. The
// returned function stops them; it is also called when the test ends.
func (m *manager) controlLoopsWith(timing scheduler.Timing) (stop func()) {
	m.t.Helper()
	if timing.Since.IsZero() {
		timing.Since = time.Now()
	}
	ctx, cancel := context.WithCancel(context.Background())
	nodeTTL := time.Minute
	if timing.SuspectAfter > 0 {
		nodeTTL = timing.SuspectAfter
	}
	sched := placement.New(placement.Config{
		Store: m.store, Logger: discardLogger(), NodeTTL: nodeTTL, Timing: timing, Interval: 10 * time.Millisecond, InstanceID: "itest",
	})
	rec := reconcile.New(reconcile.Config{
		Store: m.store, Logger: discardLogger(), Timing: timing, Interval: 10 * time.Millisecond, InstanceID: "itest",
	})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		sched.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		rec.Run(ctx)
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			wg.Wait()
		})
	}
	m.t.Cleanup(stop)
	return stop
}

func (m *manager) createService(name string, replicas int, image string) store.Service {
	m.t.Helper()
	svc, err := m.store.CreateService(context.Background(), store.NewService{
		ID: "svc-" + name, Name: name, Replicas: replicas, CreatedBy: "itest", At: time.Now(),
		Spec: store.ServiceSpec{Image: image, CPUMillis: 500, MemoryBytes: 64 << 20, Command: []string{"sleep", "300"}},
	})
	if err != nil {
		m.t.Fatal(err)
	}
	return svc
}

// replicasRunning is true once the service has exactly n workloads, all meant to run the image, all
// reported running by agents that have received the latest instruction.
func (m *manager) replicasRunning(svc store.Service, n int, image string) func() bool {
	return func() bool {
		ws, err := m.store.ListWorkloadsForService(context.Background(), svc.ID)
		if err != nil || len(ws) != n {
			return false
		}
		for _, w := range ws {
			if w.DesiredState != store.DesiredRunning || w.Image != image || w.Observed == nil ||
				w.Observed.State != store.WorkloadRunning || w.Observed.AckedGeneration != w.Generation {
				return false
			}
		}
		return true
	}
}

// managedOn lists the containers DockFleet manages on the engines, ordered by workload ID.
func managedOn(engines ...*dockerfake.Fake) []dockerfake.Info {
	var out []dockerfake.Info
	for _, e := range engines {
		out = append(out, e.Managed()...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].WorkloadID < out[j].WorkloadID })
	return out
}

func countOn(op string, engines ...*dockerfake.Fake) int {
	n := 0
	for _, e := range engines {
		n += e.Count(op)
	}
	return n
}

// heartbeats waits until a node has heartbeated several more times, which
// gives the control loops and the agents many passes.
func (m *manager) heartbeats(nodeID string, n int) {
	m.t.Helper()
	before, _ := m.node(nodeID)
	waitFor(m.t, "more heartbeats", func() bool {
		cur, _ := m.node(nodeID)
		return cur.LastHeartbeat.After(before.LastHeartbeat.Add(time.Duration(n) * heartbeatEvery))
	})
}

func workloadIDs(infos []dockerfake.Info) []string {
	ids := make([]string, 0, len(infos))
	for _, c := range infos {
		ids = append(ids, c.WorkloadID)
	}
	return ids
}

func TestServiceDeploymentEndToEnd(t *testing.T) {
	m := newManager(t, nil)
	m.start()
	a1 := m.startAgent("sim-1", nil)
	a2 := m.startAgent("sim-2", nil)
	waitFor(t, "both agents to register", func() bool { return m.sessionID("sim-1") != "" && m.sessionID("sim-2") != "" })
	ctx := context.Background()

	// A container that is not DockFleet's, on a node DockFleet manages: it must survive everything below.
	a1.engine.AddUnmanaged("redis", "redis:7", nil)
	bystanderIntact := func() {
		t.Helper()
		for _, c := range a1.engine.Containers() {
			if c.Name == "redis" && !c.Managed && c.State == "running" {
				return
			}
		}
		t.Fatalf("the bystander container is gone or changed: %+v", a1.engine.Containers())
	}
	m.controlLoops()

	// Deploy three replicas. Each becomes exactly one container, on the node it was assigned to.
	svc := m.createService("web", 3, "nginx:1.0")
	waitFor(t, "three replicas running", m.replicasRunning(svc, 3, "nginx:1.0"))
	m.heartbeats("sim-1", 10)
	workloads, err := m.store.ListWorkloadsForService(ctx, svc.ID)
	if err != nil {
		t.Fatal(err)
	}
	containers := managedOn(a1.engine, a2.engine)
	if len(containers) != 3 || countOn("create", a1.engine, a2.engine) != 3 || countOn("start", a1.engine, a2.engine) != 3 {
		t.Fatalf("containers %+v, creates %d, starts %d; repeated reconciliation must not duplicate anything",
			containers, countOn("create", a1.engine, a2.engine), countOn("start", a1.engine, a2.engine))
	}
	wantIDs := make([]string, 0, 3)
	for _, w := range workloads {
		wantIDs = append(wantIDs, w.ID)
		engine := a1.engine
		if w.NodeID != "sim-1" {
			engine = a2.engine
		}
		// The container is on the node the workload was assigned to, made from the service's spec.
		spec, ok := engine.Spec(w.ID)
		if !ok || spec.Image != "nginx:1.0" || len(spec.Command) != 2 || spec.CPUMillis != 500 || spec.MemoryBytes != 64<<20 {
			t.Fatalf("workload %s is assigned to %s, which has container spec %+v (found %v)", w.ID, w.NodeID, spec, ok)
		}
	}
	sort.Strings(wantIDs)
	if got := workloadIDs(containers); len(got) != 3 || got[0] != wantIDs[0] || got[1] != wantIDs[1] || got[2] != wantIDs[2] {
		t.Fatalf("container workload IDs %v, workloads %v", got, wantIDs)
	}
	bystanderIntact()

	// The manager restarts: sessions are lost, the agents register again, nothing changes on the nodes.
	oldSession := m.sessionID("sim-1")
	m.restart()
	waitFor(t, "agents to register again", func() bool {
		s1, s2 := m.sessionID("sim-1"), m.sessionID("sim-2")
		return s1 != "" && s1 != oldSession && s2 != ""
	})
	m.heartbeats("sim-1", 10)
	if got := managedOn(a1.engine, a2.engine); len(got) != 3 || countOn("create", a1.engine, a2.engine) != 3 {
		t.Fatalf("a manager restart changed the containers: %+v, creates %d", got, countOn("create", a1.engine, a2.engine))
	}
	waitFor(t, "three replicas still running", m.replicasRunning(svc, 3, "nginx:1.0"))

	// An agent restarts while its container has crashed: the new agent finds the
	// container, starts it again, and creates no second one.
	onSim1 := a1.engine.Managed()
	if len(onSim1) == 0 {
		t.Fatal("three replicas over two nodes must put at least one on sim-1")
	}
	victim := onSim1[0]
	if err := a1.stop(); err != nil {
		t.Fatalf("agent returned %v", err)
	}
	a1.engine.Crash(victim.WorkloadID, 1)
	a1 = m.startAgentOn(a1.engine, "sim-1", nil)
	waitFor(t, "the crashed container to be started again and reported", func() bool {
		w, err := m.store.GetWorkload(ctx, victim.WorkloadID)
		return err == nil && w.Observed != nil && w.Observed.State == store.WorkloadRunning && w.Observed.RestartCount == 1
	})
	if countOn("create", a1.engine, a2.engine) != 3 || len(managedOn(a1.engine, a2.engine)) != 3 {
		t.Fatalf("a restarted agent created a container: %v", a1.engine.Calls())
	}
	bystanderIntact()

	// Scale down: the extra replicas' containers are stopped and removed, their workloads deleted.
	if _, err := m.store.ScaleService(ctx, svc.ID, 1, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "one replica left", func() bool {
		ws, err := m.store.ListWorkloadsForService(ctx, svc.ID)
		return err == nil && len(ws) == 1 && len(managedOn(a1.engine, a2.engine)) == 1
	})
	waitFor(t, "the survivor running", m.replicasRunning(svc, 1, "nginx:1.0"))
	bystanderIntact()

	// Deploy a new revision: the new replica runs before the old one is retired.
	oldContainer := managedOn(a1.engine, a2.engine)[0]
	if _, _, err := m.store.DeployService(ctx, svc.ID, store.Deployment{
		Spec: store.ServiceSpec{Image: "nginx:2.0", CPUMillis: 500, MemoryBytes: 64 << 20}, CreatedBy: "itest", At: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "revision 2 running and revision 1 gone", func() bool {
		got := managedOn(a1.engine, a2.engine)
		return m.replicasRunning(svc, 1, "nginx:2.0")() && len(got) == 1 && got[0].Image == "nginx:2.0"
	})
	if managedOn(a1.engine, a2.engine)[0].WorkloadID == oldContainer.WorkloadID {
		t.Fatal("the new revision reused the old workload")
	}
	bystanderIntact()

	// Delete the service: everything of it goes, and only that.
	if _, err := m.store.DeleteService(ctx, svc.ID, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the service's containers and workloads to be gone", func() bool {
		ws, err := m.store.ListWorkloadsForService(ctx, svc.ID)
		return err == nil && len(ws) == 0 && len(managedOn(a1.engine, a2.engine)) == 0
	})
	m.heartbeats("sim-1", 10)

	// Over the whole run, every container that was created was removed once, and no more were created than needed.
	creates, removes := countOn("create", a1.engine, a2.engine), countOn("remove", a1.engine, a2.engine)
	if creates != 4 || removes != 4 {
		t.Fatalf("creates %d, removes %d; want 4 and 4 (three replicas, then one for the new revision)", creates, removes)
	}
	bystanderIntact()
	if left := len(a2.engine.Containers()); left != 0 {
		t.Fatalf("containers left on sim-2: %+v", a2.engine.Containers())
	}
}

// A node that is away while its replicas are removed finishes the job when it
// comes back, and the manager keeps the records until it has.
func TestRemovalWaitsForTheNodeToConfirm(t *testing.T) {
	m := newManager(t, nil)
	m.start()
	agent := m.startAgent("sim-1", nil)
	waitFor(t, "registration", func() bool { return m.sessionID("sim-1") != "" })
	m.controlLoops()
	ctx := context.Background()

	svc := m.createService("web", 2, "nginx:1.0")
	waitFor(t, "two replicas running", m.replicasRunning(svc, 2, "nginx:1.0"))

	if err := agent.stop(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.store.DeleteService(ctx, svc.ID, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the workloads to be told to stop", func() bool {
		ws, err := m.store.ListWorkloadsForService(ctx, svc.ID)
		if err != nil || len(ws) != 2 {
			return false
		}
		for _, w := range ws {
			if w.DesiredState != store.DesiredStopped {
				return false
			}
		}
		return true
	})
	// Many more passes, to show that nothing is deleted before the node has confirmed.
	rec := reconcile.New(reconcile.Config{Store: m.store, Logger: discardLogger()})
	for i := 0; i < 20; i++ {
		if report, err := rec.RunOnce(ctx); err != nil || report.Deleted != 0 {
			t.Fatalf("pass %d = %+v, %v", i+1, report, err)
		}
	}
	if ws, _ := m.store.ListWorkloadsForService(ctx, svc.ID); len(ws) != 2 || len(managedOn(agent.engine)) != 2 {
		t.Fatalf("workloads %d, containers %d; the records stay until the node has removed the containers", len(ws), len(managedOn(agent.engine)))
	}

	m.startAgentOn(agent.engine, "sim-1", nil)
	waitFor(t, "the node to remove its containers and the manager its records", func() bool {
		ws, err := m.store.ListWorkloadsForService(ctx, svc.ID)
		return err == nil && len(ws) == 0 && len(managedOn(agent.engine)) == 0
	})
}

// A failing engine delays a deployment; it does not duplicate or lose it.
func TestFailedCreatesAreRetriedUntilTheyWork(t *testing.T) {
	m := newManager(t, nil)
	m.start()
	agent := m.startAgent("sim-1", nil)
	waitFor(t, "registration", func() bool { return m.sessionID("sim-1") != "" })
	agent.engine.FailNext("create", errors.New("daemon busy"), 1)
	m.controlLoops()

	svc := m.createService("web", 1, "nginx:1.0")
	waitFor(t, "the replica to be reported failed", func() bool {
		ws, err := m.store.ListWorkloadsForService(context.Background(), svc.ID)
		return err == nil && len(ws) == 1 && ws[0].Observed != nil && ws[0].Observed.State == store.WorkloadFailed
	})
	waitFor(t, "the retry to succeed", m.replicasRunning(svc, 1, "nginx:1.0"))
	if len(managedOn(agent.engine)) != 1 || agent.engine.Count("create") != 1 {
		t.Fatalf("calls = %v", agent.engine.Calls())
	}
}
