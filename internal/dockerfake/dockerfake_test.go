package dockerfake

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

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

func logsOf(t *testing.T, r docker.LogReader) []string {
	t.Helper()
	var out []string
	for {
		l, err := r.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("Next after %v: %v", out, err)
		}
		out = append(out, l.Text)
	}
}

func startedContainer(t *testing.T, f *Fake, id string) {
	t.Helper()
	ctx := context.Background()
	f.AddImage("img:1")
	if _, err := f.Create(ctx, spec(id)); err != nil {
		t.Fatal(err)
	}
	if err := f.Start(ctx, id); err != nil {
		t.Fatal(err)
	}
}

func TestStreamLogsServesTailAndSince(t *testing.T) {
	ctx := context.Background()
	f := New("node-1")
	startedContainer(t, f, "w1")
	base := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	for i := 1; i <= 5; i++ {
		f.AppendLog("w1", "stdout", fmt.Sprintf("line %d", i), base.Add(time.Duration(i)*time.Second))
	}
	tests := []struct {
		name string
		opts docker.LogStreamOptions
		want []string
	}{
		{"everything", docker.LogStreamOptions{Tail: -1}, []string{"line 1", "line 2", "line 3", "line 4", "line 5"}},
		{"the last two", docker.LogStreamOptions{Tail: 2}, []string{"line 4", "line 5"}},
		{"none", docker.LogStreamOptions{Tail: 0}, nil},
		{"more than there are", docker.LogStreamOptions{Tail: 99}, []string{"line 1", "line 2", "line 3", "line 4", "line 5"}},
		{"since is inclusive", docker.LogStreamOptions{Tail: -1, Since: base.Add(3 * time.Second)}, []string{"line 3", "line 4", "line 5"}},
		{"the last one since a time", docker.LogStreamOptions{Tail: 1, Since: base.Add(2 * time.Second)}, []string{"line 5"}},
		{"since after the end", docker.LogStreamOptions{Tail: -1, Since: base.Add(time.Hour)}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := f.StreamLogs(ctx, "w1", tt.opts)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			if got := logsOf(t, r); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("lines = %v, want %v", got, tt.want)
			}
		})
	}
	if f.OpenLogReaders() != 0 {
		t.Fatalf("%d readers left open", f.OpenLogReaders())
	}
}

func TestStreamLogsFollowsUntilTheContainerStops(t *testing.T) {
	ctx := context.Background()
	f := New("node-1")
	startedContainer(t, f, "w1")
	f.Log("w1", "before")
	r, err := f.StreamLogs(ctx, "w1", docker.LogStreamOptions{Tail: 0, Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got := make(chan string, 4)
	done := make(chan error, 1)
	go func() {
		for {
			l, err := r.Next()
			if err != nil {
				done <- err
				return
			}
			got <- l.Text
		}
	}()
	f.Log("w1", "one")
	f.Log("w1", "two")
	for _, want := range []string{"one", "two"} {
		select {
		case text := <-got:
			if text != want {
				t.Fatalf("line = %q, want %q", text, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%q did not arrive", want)
		}
	}
	f.Crash("w1", 1)
	select {
	case err := <-done:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("a stopped container ends the follow with %v, want io.EOF", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the follow did not end when the container stopped")
	}
}

func TestStreamLogsCloseAndRemoveEndAFollow(t *testing.T) {
	ctx := context.Background()
	f := New("node-1")
	startedContainer(t, f, "w1")

	closing, err := f.StreamLogs(ctx, "w1", docker.LogStreamOptions{Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := closing.Next()
		result <- err
	}()
	time.Sleep(10 * time.Millisecond)
	closing.Close()
	select {
	case err := <-result:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("Next after Close = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not end the blocked read")
	}

	removed, err := f.StreamLogs(ctx, "w1", docker.LogStreamOptions{Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	defer removed.Close()
	go func() {
		_, err := removed.Next()
		result <- err
	}()
	time.Sleep(10 * time.Millisecond)
	if err := f.Stop(ctx, "w1", 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Remove(ctx, "w1", true); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("Next after the container was removed = %v, want io.EOF", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("removing the container did not end the follow")
	}
}

func TestStreamLogsContextEndsTheReader(t *testing.T) {
	f := New("node-1")
	startedContainer(t, f, "w1")
	ctx, cancel := context.WithCancel(context.Background())
	r, err := f.StreamLogs(ctx, "w1", docker.LogStreamOptions{Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	result := make(chan error, 1)
	go func() {
		_, err := r.Next()
		result <- err
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("Next after the context ended = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceling the context did not end the blocked read")
	}
}

func TestStreamLogsOnlyReachesManagedContainers(t *testing.T) {
	f := New("node-1")
	startedContainer(t, f, "w1")
	f.AddUnmanaged("bystander", "img:1", map[string]string{"app": "shop"})
	ctx := context.Background()
	if _, err := f.StreamLogs(ctx, "bystander", docker.LogStreamOptions{}); !errors.Is(err, docker.ErrNotFound) {
		t.Fatalf("a container that is not DockFleet's: %v, want ErrNotFound", err)
	}
	if _, err := f.StreamLogs(ctx, "nobody", docker.LogStreamOptions{}); !errors.Is(err, docker.ErrNotFound) {
		t.Fatalf("an unknown workload: %v, want ErrNotFound", err)
	}
	if f.AppendLog("bystander", "stdout", "x", time.Now()) {
		t.Fatal("a line was written to a container that is not DockFleet's")
	}
	f.FailNext("logs", errors.New("daemon down"), 1)
	if _, err := f.StreamLogs(ctx, "w1", docker.LogStreamOptions{}); err == nil || f.Pending("logs") != 0 {
		t.Fatalf("injected failure: %v, %d pending", err, f.Pending("logs"))
	}
}

func TestStreamLogsCutsLongLines(t *testing.T) {
	f := New("node-1")
	startedContainer(t, f, "w1")
	f.Log("w1", strings.Repeat("é", 100))
	r, err := f.StreamLogs(context.Background(), "w1", docker.LogStreamOptions{Tail: -1, MaxLineBytes: 51})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	l, err := r.Next()
	if err != nil || !l.Truncated || len(l.Text) != 50 || !utf8.ValidString(l.Text) {
		t.Fatalf("line = %d bytes, truncated %v, %v", len(l.Text), l.Truncated, err)
	}
}
