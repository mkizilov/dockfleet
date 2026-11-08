package dockerfake

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/docker"
)

func spec(id string) docker.ContainerSpec {
	return docker.ContainerSpec{WorkloadID: id, WorkloadName: id, Image: "img:1", CPUMillis: 100, MemoryBytes: 64 << 20}
}

func TestLifecycleMatchesTheAdapter(t *testing.T) {
	ctx := context.Background()
	f := New("node-1")

	if _, err := f.Create(ctx, spec("w1")); !errors.Is(err, docker.ErrImageNotFound) {
		t.Fatalf("create without the image = %v, want ErrImageNotFound", err)
	}
	if err := f.PullImage(ctx, "img:1"); err != nil {
		t.Fatal(err)
	}
	c, err := f.Create(ctx, spec("w1"))
	if err != nil || c.State != docker.StateCreated || c.WorkloadID != "w1" || c.NodeID != "node-1" {
		t.Fatalf("create = %+v, %v", c, err)
	}
	if _, err := f.Create(ctx, spec("w1")); !errors.Is(err, docker.ErrConflict) {
		t.Fatalf("a second container for the workload = %v, want ErrConflict", err)
	}
	if err := f.Start(ctx, "w1"); err != nil {
		t.Fatal(err)
	}
	if st, err := f.Inspect(ctx, "w1"); err != nil || !st.Running {
		t.Fatalf("inspect = %+v, %v", st, err)
	}
	if err := f.Remove(ctx, "w1", false); !errors.Is(err, docker.ErrStateConflict) {
		t.Fatalf("removing a running container without force = %v, want ErrStateConflict", err)
	}
	if err := f.Stop(ctx, "w1", 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Remove(ctx, "w1", false); err != nil {
		t.Fatal(err)
	}
	if err := f.Start(ctx, "w1"); !errors.Is(err, docker.ErrNotFound) {
		t.Fatalf("start after removal = %v, want ErrNotFound", err)
	}
	if got := f.Calls(); len(got) != 5 || got[0] != "pull img:1" || got[4] != "remove w1" || f.Count("create") != 1 || f.Count("pull") != 1 {
		t.Fatalf("calls = %v", got)
	}
}

func TestPolicyApplies(t *testing.T) {
	f := New("node-1")
	f.AddImage("img:1")
	bad := spec("w1")
	bad.Privileged = true
	if _, err := f.Create(context.Background(), bad); !errors.Is(err, docker.ErrRejected) {
		t.Fatalf("a privileged container = %v, want ErrRejected", err)
	}
	if len(f.Containers()) != 0 {
		t.Fatalf("a rejected spec created %v", f.Containers())
	}
}

func TestBystandersAreInvisibleAndUntouchable(t *testing.T) {
	ctx := context.Background()
	f := New("node-1")
	f.AddImage("img:1")
	other := map[string]string{docker.LabelManaged: "true", docker.LabelNodeID: "node-2", docker.LabelWorkloadID: "w9"}
	f.AddUnmanaged("redis", "redis:7", nil)
	f.AddUnmanaged("dockfleet-w2", "squatter:1", nil)
	f.AddUnmanaged("dockfleet-w9", "img:1", other)

	if list, err := f.List(ctx); err != nil || len(list) != 0 {
		t.Fatalf("List = %+v, %v; only this node's managed containers are visible", list, err)
	}
	for name, err := range map[string]error{
		"start":  f.Start(ctx, "w9"),
		"stop":   f.Stop(ctx, "w9", 0),
		"remove": f.Remove(ctx, "w9", true),
	} {
		if !errors.Is(err, docker.ErrNotFound) {
			t.Errorf("%s of another node's container = %v, want ErrNotFound", name, err)
		}
	}
	if _, err := f.Create(ctx, spec("w2")); !errors.Is(err, docker.ErrConflict) {
		t.Fatalf("create over an unmanaged name = %v, want ErrConflict", err)
	}
	if got := len(f.Containers()); got != 3 || len(f.Managed()) != 0 || f.Count("remove") != 0 {
		t.Fatalf("containers = %+v", f.Containers())
	}
}

func TestFailuresCrashesAndPing(t *testing.T) {
	ctx := context.Background()
	f := New("node-1")
	f.AddImage("img:1")
	boom := errors.New("boom")

	f.FailNext("create", boom, 2)
	for i := 0; i < 2; i++ {
		if _, err := f.Create(ctx, spec("w1")); !errors.Is(err, boom) {
			t.Fatalf("create %d = %v, want the injected failure", i, err)
		}
	}
	if _, err := f.Create(ctx, spec("w1")); err != nil {
		t.Fatalf("create after the failures = %v", err)
	}
	f.FailNext("ping", boom, 1)
	if err := f.Ping(ctx); !errors.Is(err, boom) {
		t.Fatalf("ping = %v", err)
	}
	if err := f.Ping(ctx); err != nil {
		t.Fatalf("ping = %v", err)
	}

	if err := f.Start(ctx, "w1"); err != nil {
		t.Fatal(err)
	}
	f.Crash("w1", 3)
	if st, _ := f.Inspect(ctx, "w1"); st.Running || st.ExitCode != 3 || st.State != docker.StateExited {
		t.Fatalf("after a crash: %+v", st)
	}
	if err := f.Start(ctx, "w1"); err != nil {
		t.Fatal(err)
	}
	f.OOMKill("w1")
	if st, _ := f.Inspect(ctx, "w1"); !st.OOMKilled || st.ExitCode != 137 {
		t.Fatalf("after an OOM kill: %+v", st)
	}
}

// An image can be made to crash on start, or to fail its health check, which is
// how tests give a rollout replacements that do not work.
func TestImageBehavior(t *testing.T) {
	ctx := context.Background()
	f := New("node-1")
	f.AddImage("good:1")
	f.AddImage("bad:1")
	f.SetImageBehavior("bad:1", ImageBehavior{ExitCode: 3})
	f.AddImage("sick:1")
	f.SetImageBehavior("sick:1", ImageBehavior{Health: docker.HealthUnhealthy})

	withImage := func(id, image string, check bool) docker.ContainerSpec {
		s := spec(id)
		s.Image = image
		if check {
			s.HealthCheck = &docker.HealthCheck{Command: []string{"true"}, Interval: time.Second, Timeout: time.Second, Retries: 1}
		}
		return s
	}
	for _, s := range []docker.ContainerSpec{
		withImage("good", "good:1", true), withImage("bad", "bad:1", false), withImage("sick", "sick:1", true), withImage("sick-no-check", "sick:1", false),
	} {
		if _, err := f.Create(ctx, s); err != nil {
			t.Fatalf("create %s: %v", s.WorkloadID, err)
		}
		if err := f.Start(ctx, s.WorkloadID); err != nil {
			t.Fatalf("start %s: %v", s.WorkloadID, err)
		}
	}
	want := map[string]struct {
		running bool
		exit    int
		health  string
	}{
		"good":          {true, 0, docker.HealthHealthy},
		"bad":           {false, 3, ""},
		"sick":          {true, 0, docker.HealthUnhealthy},
		"sick-no-check": {true, 0, ""},
	}
	for id, w := range want {
		st, err := f.Inspect(ctx, id)
		if err != nil || st.Running != w.running || st.ExitCode != w.exit || st.Health != w.health {
			t.Errorf("%s = %+v, %v; want running %v, exit %d, health %q", id, st, err, w.running, w.exit, w.health)
		}
	}
	// It applies at every start, not only the first.
	if err := f.Start(ctx, "bad"); err != nil {
		t.Fatal(err)
	}
	if st, _ := f.Inspect(ctx, "bad"); st.Running || st.ExitCode != 3 {
		t.Fatalf("a second start of a crashing image = %+v", st)
	}
	// Clearing it restores the default for the next start.
	f.SetImageBehavior("bad:1", ImageBehavior{})
	if err := f.Start(ctx, "bad"); err != nil {
		t.Fatal(err)
	}
	if st, _ := f.Inspect(ctx, "bad"); !st.Running {
		t.Fatalf("after clearing the behavior = %+v", st)
	}
}
