//go:build dockerintegration

package docker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
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

// StreamLogs against a real daemon: timestamps, both output streams, a follow
// that sees lines written later, and a Close that ends a read that is waiting.
func TestDockerEngineStreamsLogs(t *testing.T) {
	host := os.Getenv("DOCKFLEET_TEST_DOCKER_HOST")
	if host == "" {
		t.Skip("DOCKFLEET_TEST_DOCKER_HOST not set")
	}
	image := os.Getenv("DOCKFLEET_TEST_DOCKER_IMAGE")
	if image == "" {
		image = "alpine:3.20"
	}
	suffix := randomSuffix(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	a, err := NewAdapter(AdapterConfig{Host: host, NodeID: "itest-logs-" + suffix})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err := a.PullImage(ctx, image); err != nil {
		t.Fatalf("PullImage: %v", err)
	}
	workload := "itest-logs-" + suffix
	t.Cleanup(func() { _ = a.Remove(context.Background(), workload, true) })
	if _, err := a.Create(ctx, ContainerSpec{
		WorkloadID: workload, WorkloadName: "logs", Image: image, CPUMillis: 100, MemoryBytes: 32 << 20,
		Command: []string{"sh", "-c", "i=0; while true; do i=$((i+1)); echo \"out $i\"; echo \"err $i\" >&2; sleep 0.2; done"},
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := a.Start(ctx, workload); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait until the container has written a few lines.
	eventually(t, "a few lines of output", 15*time.Second, func() bool {
		logs, err := a.Logs(ctx, workload, LogOptions{})
		return err == nil && len(logs.Chunks) >= 6
	})

	// The history: the last lines, with the time the daemon recorded, and no wait.
	r, err := a.StreamLogs(ctx, workload, LogStreamOptions{Tail: 4})
	if err != nil {
		t.Fatalf("StreamLogs: %v", err)
	}
	var history []LogLine
	for {
		l, err := r.Next()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("Next: %v", err)
			}
			break
		}
		history = append(history, l)
	}
	_ = r.Close()
	if len(history) != 4 {
		t.Fatalf("%d lines of history, want 4: %+v", len(history), history)
	}
	var sawErr, sawOut bool
	for i, l := range history {
		if l.Time.IsZero() || time.Since(l.Time) > time.Minute || (i > 0 && l.Time.Before(history[i-1].Time)) {
			t.Fatalf("line %d has time %v; times must be real and not go backwards", i, l.Time)
		}
		sawErr = sawErr || (l.Stream == "stderr" && strings.HasPrefix(l.Text, "err "))
		sawOut = sawOut || (l.Stream == "stdout" && strings.HasPrefix(l.Text, "out "))
	}
	if !sawErr || !sawOut {
		t.Fatalf("history = %+v; both output streams must come through", history)
	}

	// Following: lines written after the read began arrive, and Close ends the wait.
	last := history[len(history)-1].Time
	f, err := a.StreamLogs(ctx, workload, LogStreamOptions{Tail: -1, Since: last, Follow: true})
	if err != nil {
		t.Fatalf("StreamLogs (follow): %v", err)
	}
	var fresh int
	for fresh < 3 {
		l, err := f.Next()
		if err != nil {
			t.Fatalf("Next while following: %v", err)
		}
		if l.Time.After(last) {
			fresh++
		}
	}
	done := make(chan error, 1)
	go func() {
		for {
			if _, err := f.Next(); err != nil {
				done <- err
				return
			}
		}
	}()
	time.Sleep(300 * time.Millisecond)
	_ = f.Close()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not end a read that was waiting for output")
	}
}
