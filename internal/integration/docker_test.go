//go:build dockerintegration

package integration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/docker"
	"github.com/dockfleet/dockfleet/internal/store"
)

// This test deploys a service with the real control loop and a real agent on a
// real Docker daemon. It is skipped unless DOCKFLEET_TEST_DOCKER_HOST names one
// (a unix:// socket or a loopback tcp:// address; see `make docker-test-up`
// for a disposable daemon). It pulls DOCKFLEET_TEST_DOCKER_IMAGE (default
// alpine:3.20) and removes every container it creates. Run it with
// `make test-docker`.

func randomSuffix(t *testing.T) string {
	t.Helper()
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// waitUntil is waitFor with a deadline suited to a real daemon, which pulls images and starts processes.
func waitUntil(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func containerIDs(t *testing.T, a *docker.Adapter) []string {
	t.Helper()
	list, err := a.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(list))
	for _, c := range list {
		ids = append(ids, c.ID)
	}
	sort.Strings(ids)
	return ids
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestServiceDeploymentAgainstDocker(t *testing.T) {
	host := os.Getenv("DOCKFLEET_TEST_DOCKER_HOST")
	if host == "" {
		t.Skip("DOCKFLEET_TEST_DOCKER_HOST not set")
	}
	image := os.Getenv("DOCKFLEET_TEST_DOCKER_IMAGE")
	if image == "" {
		image = "alpine:3.20"
	}
	suffix := randomSuffix(t)
	nodeID := "itest-" + suffix
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	engine, err := docker.NewAdapter(docker.AdapterConfig{Host: host, NodeID: nodeID})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	if err := engine.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if err := engine.PullImage(ctx, image); err != nil {
		t.Fatalf("PullImage: %v", err)
	}
	removeAll := func(a *docker.Adapter) {
		list, _ := a.List(context.Background())
		for _, c := range list {
			_ = a.Remove(context.Background(), c.WorkloadID, true)
		}
	}
	t.Cleanup(func() { removeAll(engine) })

	// A container that belongs to another node's agent: to this agent it is not DockFleet's, and it must survive.
	bystanderEngine, err := docker.NewAdapter(docker.AdapterConfig{Host: host, NodeID: "bystander-" + suffix})
	if err != nil {
		t.Fatal(err)
	}
	defer bystanderEngine.Close()
	t.Cleanup(func() { removeAll(bystanderEngine) })
	sleeper := docker.ContainerSpec{WorkloadID: "bystander-" + suffix, Image: image, Command: []string{"sleep", "600"}, CPUMillis: 100, MemoryBytes: 32 << 20}
	if _, err := bystanderEngine.Create(ctx, sleeper); err != nil {
		t.Fatalf("create the bystander: %v", err)
	}
	if err := bystanderEngine.Start(ctx, sleeper.WorkloadID); err != nil {
		t.Fatalf("start the bystander: %v", err)
	}
	bystanderIntact := func() {
		t.Helper()
		st, err := bystanderEngine.Inspect(ctx, sleeper.WorkloadID)
		if err != nil || !st.Running {
			t.Fatalf("the bystander container was touched: %+v, %v", st, err)
		}
	}

	m := newManager(t, nil)
	m.enroll(nodeID)
	m.start()
	a := m.runAgent(engine, nodeID, nil)
	waitUntil(t, "registration", 30*time.Second, func() bool { return m.sessionID(nodeID) != "" })
	m.controlLoops()

	deploy := func(replicas int, command ...string) store.Service {
		svc, err := m.store.CreateService(ctx, store.NewService{
			ID: "svc-web", Name: "web", Replicas: replicas, CreatedBy: "itest", At: time.Now(),
			Spec: store.ServiceSpec{Image: image, CPUMillis: 100, MemoryBytes: 32 << 20, Command: command},
		})
		if err != nil {
			t.Fatal(err)
		}
		return svc
	}
	running := func(svc store.Service, n int) func() bool {
		return func() bool {
			ws, err := m.store.ListWorkloadsForService(ctx, svc.ID)
			if err != nil || len(ws) != n {
				return false
			}
			for _, w := range ws {
				if w.DesiredState != store.DesiredRunning || w.Observed == nil || w.Observed.State != store.WorkloadRunning || w.Observed.AckedGeneration != w.Generation {
					return false
				}
			}
			return true
		}
	}

	// Deploy two replicas: exactly two containers, each labeled with its workload.
	svc := deploy(2, "sleep", "300")
	waitUntil(t, "two replicas running", 3*time.Minute, running(svc, 2))
	first := containerIDs(t, engine)
	if len(first) != 2 {
		t.Fatalf("containers = %v, want 2", first)
	}

	// Repeated reconciliation changes nothing: the same two containers, a long while later.
	m.heartbeats(nodeID, 30)
	if got := containerIDs(t, engine); !sameStrings(got, first) {
		t.Fatalf("containers changed under repeated reconciliation: %v -> %v", first, got)
	}

	// The agent restarts: it finds its containers and creates no others.
	if err := a.stop(); err != nil {
		t.Fatal(err)
	}
	a = m.runAgent(engine, nodeID, nil)
	waitUntil(t, "the new agent to register", 30*time.Second, func() bool { return m.sessionID(nodeID) != "" })
	waitUntil(t, "two replicas running again", time.Minute, running(svc, 2))
	m.heartbeats(nodeID, 30)
	if got := containerIDs(t, engine); !sameStrings(got, first) {
		t.Fatalf("a restarted agent changed the containers: %v -> %v", first, got)
	}

	// A container is stopped behind the agent's back: the agent starts the same container again.
	list, err := engine.List(ctx)
	if err != nil || len(list) == 0 {
		t.Fatalf("List = %+v, %v", list, err)
	}
	victim := list[0]
	if err := engine.Stop(ctx, victim.WorkloadID, 0); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "the stopped container to be started again", time.Minute, func() bool {
		st, err := engine.Inspect(ctx, victim.WorkloadID)
		w, werr := m.store.GetWorkload(ctx, victim.WorkloadID)
		return err == nil && st.Running && st.ID == victim.ID && werr == nil && w.Observed != nil && w.Observed.RestartCount >= 1
	})
	if got := containerIDs(t, engine); !sameStrings(got, first) {
		t.Fatalf("restarting a container created another: %v -> %v", first, got)
	}
	bystanderIntact()

	// Scale down: one container is stopped and removed, its workload deleted.
	if _, err := m.store.ScaleService(ctx, svc.ID, 1, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "one replica left", time.Minute, func() bool {
		ws, err := m.store.ListWorkloadsForService(ctx, svc.ID)
		return err == nil && len(ws) == 1 && len(containerIDs(t, engine)) == 1
	})
	waitUntil(t, "the survivor running", time.Minute, running(svc, 1))

	// A new revision replaces the replica: the new container runs before the old one goes.
	old := containerIDs(t, engine)
	if _, _, err := m.store.DeployService(ctx, svc.ID, store.Deployment{
		Spec: store.ServiceSpec{Image: image, CPUMillis: 100, MemoryBytes: 32 << 20, Command: []string{"sleep", "301"}}, CreatedBy: "itest", At: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "the new revision to replace the old", 2*time.Minute, func() bool {
		got := containerIDs(t, engine)
		return running(svc, 1)() && len(got) == 1 && !sameStrings(got, old)
	})
	bystanderIntact()

	// Delete the service: its containers go, the bystander stays.
	if _, err := m.store.DeleteService(ctx, svc.ID, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "every container and workload of the service to be gone", time.Minute, func() bool {
		ws, err := m.store.ListWorkloadsForService(ctx, svc.ID)
		return err == nil && len(ws) == 0 && len(containerIDs(t, engine)) == 0
	})
	bystanderIntact()
}
