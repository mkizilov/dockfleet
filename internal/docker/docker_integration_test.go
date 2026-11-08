//go:build dockerintegration

package docker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

// These tests need a real Docker daemon and are skipped unless
// DOCKFLEET_TEST_DOCKER_HOST names one (for example unix:///var/run/docker.sock).
// They create short-lived containers from DOCKFLEET_TEST_DOCKER_IMAGE (default
// alpine:3.20), pulling it if needed, and remove everything they create. Run
// them with `make test-docker`.

func randomSuffix(t *testing.T) string {
	t.Helper()
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func eventually(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestDockerEngine(t *testing.T) {
	host := os.Getenv("DOCKFLEET_TEST_DOCKER_HOST")
	if host == "" {
		t.Skip("DOCKFLEET_TEST_DOCKER_HOST not set")
	}
	image := os.Getenv("DOCKFLEET_TEST_DOCKER_IMAGE")
	if image == "" {
		image = "alpine:3.20"
	}
	suffix := randomSuffix(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	a, err := NewAdapter(AdapterConfig{Host: host, NodeID: "itest-" + suffix})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err := a.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if err := a.PullImage(ctx, image); err != nil {
		t.Fatalf("PullImage: %v", err)
	}

	// A container that is not DockFleet's, created beside it. It must survive everything below.
	bystanderBackend, err := newSDKBackend(host)
	if err != nil {
		t.Fatal(err)
	}
	defer bystanderBackend.close()
	sleeper := ContainerSpec{WorkloadID: "bystander", Image: image, Command: []string{"sleep", "300"}, CPUMillis: 100, MemoryBytes: 32 << 20}
	bystander, err := bystanderBackend.create(ctx, createRequest{
		Name:   "dockfleet-itest-bystander-" + suffix,
		Labels: map[string]string{"dockfleet.itest": suffix},
		Spec:   sleeper,
	})
	if err != nil {
		t.Fatalf("create bystander: %v", err)
	}
	t.Cleanup(func() { _ = bystanderBackend.remove(context.Background(), bystander, true) })
	if err := bystanderBackend.start(ctx, bystander); err != nil {
		t.Fatalf("start bystander: %v", err)
	}

	workload := "itest-" + suffix
	t.Cleanup(func() { _ = a.Remove(context.Background(), workload, true) })
	spec := ContainerSpec{
		WorkloadID:     workload,
		WorkloadName:   "integration",
		Image:          image,
		Command:        []string{"sh", "-c", "echo hello; echo oops >&2; sleep 300"},
		Env:            map[string]string{"GREETING": "hi"},
		CPUMillis:      200,
		MemoryBytes:    64 << 20,
		ReadOnlyRootFS: true,
		Mounts:         []Mount{{Type: MountTmpfs, Target: "/tmp"}},
	}

	created, err := a.Create(ctx, spec)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.State != StateCreated || created.WorkloadID != workload {
		t.Fatalf("created = %+v", created)
	}
	if _, err := a.Create(ctx, spec); !errors.Is(err, ErrConflict) {
		t.Fatalf("second Create error = %v, want ErrConflict", err)
	}

	list, err := a.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].WorkloadID != workload {
		t.Fatalf("List = %+v, want only the DockFleet container", list)
	}

	if err := a.Start(ctx, workload); err != nil {
		t.Fatalf("Start: %v", err)
	}
	st, err := a.Inspect(ctx, workload)
	if err != nil || !st.Running || st.State != StateRunning {
		t.Fatalf("Inspect after start = %+v, %v", st, err)
	}
	eventually(t, "the container's output", 15*time.Second, func() bool {
		logs, err := a.Logs(ctx, workload, LogOptions{})
		if err != nil {
			return false
		}
		var out, errOut strings.Builder
		for _, c := range logs.Chunks {
			if c.Stream == "stdout" {
				out.WriteString(c.Text)
			} else {
				errOut.WriteString(c.Text)
			}
		}
		return strings.Contains(out.String(), "hello") && strings.Contains(errOut.String(), "oops")
	})

	usage, err := a.Stats(ctx, workload)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if usage.MemoryLimitBytes != 64<<20 || usage.Pids == 0 {
		t.Errorf("usage = %+v, want the 64 MiB limit and at least one process", usage)
	}

	if err := a.Remove(ctx, workload, false); !errors.Is(err, ErrStateConflict) {
		t.Errorf("removing a running container without force: error = %v, want ErrStateConflict", err)
	}
	if err := a.Stop(ctx, workload, 2*time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	st, err = a.Inspect(ctx, workload)
	if err != nil || st.Running {
		t.Fatalf("Inspect after stop = %+v, %v", st, err)
	}
	if err := a.Remove(ctx, workload, false); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := a.Inspect(ctx, workload); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Inspect after remove: error = %v, want ErrNotFound", err)
	}

	// The unrelated container was never touched and the adapter cannot reach it.
	info, err := bystanderBackend.inspect(ctx, bystander)
	if err != nil || !info.Running {
		t.Fatalf("the unrelated container was disturbed: %+v, %v", info, err)
	}
	for _, id := range []string{"bystander", "dockfleet-itest-bystander-" + suffix} {
		if err := a.Remove(ctx, id, true); !errors.Is(err, ErrNotFound) {
			t.Errorf("Remove(%q) error = %v, want ErrNotFound", id, err)
		}
	}
	if info, err := bystanderBackend.inspect(ctx, bystander); err != nil || !info.Running {
		t.Fatalf("the unrelated container was disturbed: %+v, %v", info, err)
	}

	// Dangerous requests are rejected before reaching the daemon.
	bad := spec
	bad.WorkloadID = "itest-bad-" + suffix
	bad.Privileged = true
	bad.Mounts = []Mount{{Type: MountBind, Source: "/var/run/docker.sock", Target: "/var/run/docker.sock"}}
	if _, err := a.Create(ctx, bad); !errors.Is(err, ErrRejected) {
		t.Fatalf("dangerous Create error = %v, want ErrRejected", err)
	}
	if list, err := a.List(ctx); err != nil || len(list) != 0 {
		t.Fatalf("List after rejection = %+v, %v", list, err)
	}
}
