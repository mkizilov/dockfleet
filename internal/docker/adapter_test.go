package docker

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"
)

const testNode = "node-1"

// fakeBackend is an in-memory Docker Engine. It deliberately ignores the label
// filter it is given (returning everything), to prove the adapter does not
// rely on the daemon for ownership checks.
type fakeBackend struct {
	containers []fakeContainer

	// calls records every call as "op id" so tests can assert exactly which
	// containers were touched.
	calls []string

	createErr   error
	listFilters []map[string]string
	created     []createRequest
	logStream   []byte
	// logBody, when set, is what logs returns instead of logStream, and logReqs
	// records the requests, so a test can drive a stream by hand.
	logBody     io.ReadCloser
	logReqs     []logRequest
	statsJSON   string
	stopTimeout time.Duration
	forceRemove bool
	pulled      []string
	closed      bool
	failList    error
}

type fakeContainer struct {
	rawInspect
}

func managedBy(node, workload string) map[string]string {
	return map[string]string{LabelManaged: "true", LabelNodeID: node, LabelWorkloadID: workload, LabelWorkloadName: "name-" + workload, LabelSchema: "1"}
}

func (f *fakeBackend) add(id, state string, labels map[string]string) {
	f.containers = append(f.containers, fakeContainer{rawInspect{
		rawContainer: rawContainer{ID: id, Names: []string{"/n-" + id}, Image: "img", Labels: labels, State: state, Created: time.Unix(1700000000, 0).UTC()},
		Running:      state == "running",
	}})
}

func (f *fakeBackend) record(op, id string) { f.calls = append(f.calls, op+" "+id) }

func (f *fakeBackend) mutating() []string {
	var out []string
	for _, c := range f.calls {
		if !strings.HasPrefix(c, "list") && !strings.HasPrefix(c, "inspect") && !strings.HasPrefix(c, "ping") && !strings.HasPrefix(c, "logs") && !strings.HasPrefix(c, "stats") {
			out = append(out, c)
		}
	}
	return out
}

func (f *fakeBackend) get(id string) (fakeContainer, error) {
	for _, c := range f.containers {
		if c.ID == id {
			return c, nil
		}
	}
	return fakeContainer{}, ErrNotFound
}

func (f *fakeBackend) ping(context.Context) error { f.record("ping", ""); return nil }

func (f *fakeBackend) list(_ context.Context, labels map[string]string) ([]rawContainer, error) {
	f.record("list", "")
	f.listFilters = append(f.listFilters, labels)
	if f.failList != nil {
		return nil, f.failList
	}
	out := make([]rawContainer, 0, len(f.containers))
	for _, c := range f.containers {
		out = append(out, c.rawContainer)
	}
	return out, nil
}

func (f *fakeBackend) create(_ context.Context, req createRequest) (string, error) {
	f.record("create", req.Name)
	if f.createErr != nil {
		return "", f.createErr
	}
	f.created = append(f.created, req)
	return "id-" + req.Spec.WorkloadID, nil
}

func (f *fakeBackend) start(_ context.Context, id string) error { f.record("start", id); return nil }

func (f *fakeBackend) stop(_ context.Context, id string, timeout time.Duration) error {
	f.record("stop", id)
	f.stopTimeout = timeout
	return nil
}

func (f *fakeBackend) remove(_ context.Context, id string, force bool) error {
	f.record("remove", id)
	f.forceRemove = force
	return nil
}

func (f *fakeBackend) inspect(_ context.Context, id string) (rawInspect, error) {
	f.record("inspect", id)
	c, err := f.get(id)
	return c.rawInspect, err
}

func (f *fakeBackend) logs(_ context.Context, id string, req logRequest) (io.ReadCloser, error) {
	f.record("logs", id)
	f.logReqs = append(f.logReqs, req)
	if f.logBody != nil {
		return f.logBody, nil
	}
	return io.NopCloser(bytes.NewReader(f.logStream)), nil
}

func (f *fakeBackend) stats(_ context.Context, id string) (io.ReadCloser, error) {
	f.record("stats", id)
	return io.NopCloser(strings.NewReader(f.statsJSON)), nil
}

func (f *fakeBackend) pull(_ context.Context, ref string) error {
	f.record("pull", ref)
	f.pulled = append(f.pulled, ref)
	return nil
}

func (f *fakeBackend) close() error { f.closed = true; return nil }

func newTestAdapter(t *testing.T, f *fakeBackend) *Adapter {
	t.Helper()
	a, err := newAdapter(f, testNode, nil)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// seed adds two managed containers plus every kind of container the adapter must ignore.
func seed(f *fakeBackend) {
	f.add("aaa", "running", managedBy(testNode, "w-b"))
	f.add("bbb", "exited", managedBy(testNode, "w-a"))
	f.add("ccc", "running", nil)                                                                                     // not ours at all
	f.add("ddd", "running", map[string]string{"com.docker.compose.project": "shop"})                                 // someone else's labels
	f.add("eee", "running", managedBy("other-node", "w-c"))                                                          // ours, but another node's
	f.add("fff", "running", map[string]string{LabelManaged: "false", LabelNodeID: testNode, LabelWorkloadID: "w-d"}) // label says unmanaged
	f.add("ggg", "running", map[string]string{LabelManaged: "true", LabelNodeID: testNode})                          // no workload identity
}

func TestListReturnsOnlyThisNodesManagedContainers(t *testing.T) {
	f := &fakeBackend{}
	seed(f)
	a := newTestAdapter(t, f)

	got, err := a.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].WorkloadID != "w-a" || got[1].WorkloadID != "w-b" {
		t.Fatalf("List = %+v, want the two managed containers ordered by workload ID", got)
	}
	if got[0].ID != "bbb" || got[0].State != StateExited || got[0].NodeID != testNode || got[0].WorkloadName != "name-w-a" || got[0].Name != "n-bbb" || got[0].Image != "img" {
		t.Errorf("unexpected container: %+v", got[0])
	}
	if got[1].State != StateRunning {
		t.Errorf("unexpected state: %v", got[1].State)
	}
	want := map[string]string{LabelManaged: "true", LabelNodeID: testNode}
	if len(f.listFilters) != 1 || !reflect.DeepEqual(f.listFilters[0], want) {
		t.Errorf("list must always filter by the managed and node labels, got %v", f.listFilters)
	}
}

func TestListPropagatesBackendFailures(t *testing.T) {
	f := &fakeBackend{failList: errors.New("daemon down")}
	if _, err := newTestAdapter(t, f).List(context.Background()); err == nil || !strings.Contains(err.Error(), "daemon down") {
		t.Fatalf("error = %v", err)
	}
}

func TestUnknownStatesAreReportedAsUnknown(t *testing.T) {
	f := &fakeBackend{}
	f.add("aaa", "teleporting", managedBy(testNode, "w-1"))
	got, _ := newTestAdapter(t, f).List(context.Background())
	if len(got) != 1 || got[0].State != StateUnknown {
		t.Fatalf("List = %+v", got)
	}
}

func TestMutatingCallsTargetOnlyTheWorkloadsManagedContainer(t *testing.T) {
	f := &fakeBackend{}
	seed(f)
	a := newTestAdapter(t, f)
	ctx := context.Background()

	if err := a.Start(ctx, "w-b"); err != nil {
		t.Fatal(err)
	}
	if err := a.Stop(ctx, "w-b", 3*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := a.Remove(ctx, "w-a", true); err != nil {
		t.Fatal(err)
	}
	want := []string{"start aaa", "stop aaa", "remove bbb"}
	if got := f.mutating(); !reflect.DeepEqual(got, want) {
		t.Fatalf("mutating calls = %v, want %v", got, want)
	}
	if f.stopTimeout != 3*time.Second || !f.forceRemove {
		t.Errorf("stop timeout = %v, force = %v", f.stopTimeout, f.forceRemove)
	}
}

func TestUnrelatedContainersAreNeverTouched(t *testing.T) {
	f := &fakeBackend{}
	seed(f)
	a := newTestAdapter(t, f)
	ctx := context.Background()

	// Workload IDs that match only containers the adapter must ignore, plus Docker IDs and names.
	for _, id := range []string{"w-c", "w-d", "w-missing", "ccc", "ddd", "n-ccc", "shop"} {
		for name, op := range map[string]func() error{
			"start":   func() error { return a.Start(ctx, id) },
			"stop":    func() error { return a.Stop(ctx, id, time.Second) },
			"remove":  func() error { return a.Remove(ctx, id, true) },
			"inspect": func() error { _, err := a.Inspect(ctx, id); return err },
			"logs":    func() error { _, err := a.Logs(ctx, id, LogOptions{}); return err },
			"stats":   func() error { _, err := a.Stats(ctx, id); return err },
		} {
			if err := op(); !errors.Is(err, ErrNotFound) {
				t.Errorf("%s(%q) error = %v, want ErrNotFound", name, id, err)
			}
		}
	}
	if got := f.mutating(); len(got) != 0 {
		t.Fatalf("unrelated containers were mutated: %v", got)
	}
}

func TestOperationsRejectMalformedWorkloadIDsWithoutCallingDocker(t *testing.T) {
	f := &fakeBackend{}
	a := newTestAdapter(t, f)
	for _, id := range []string{"", "a/b", "a b", "-x", strings.Repeat("a", 64), "a;rm"} {
		if err := a.Start(context.Background(), id); !errors.Is(err, ErrInvalid) {
			t.Errorf("Start(%q) error = %v, want ErrInvalid", id, err)
		}
	}
	if len(f.calls) != 0 {
		t.Fatalf("Docker must not be called for malformed IDs: %v", f.calls)
	}
}

func TestAmbiguousWorkloadIDsAreRefused(t *testing.T) {
	f := &fakeBackend{}
	f.add("aaa", "running", managedBy(testNode, "w-1"))
	f.add("bbb", "running", managedBy(testNode, "w-1"))
	err := newTestAdapter(t, f).Stop(context.Background(), "w-1", time.Second)
	if !errors.Is(err, ErrAmbiguous) {
		t.Fatalf("error = %v, want ErrAmbiguous", err)
	}
	if len(f.mutating()) != 0 {
		t.Fatalf("an ambiguous workload must not be touched: %v", f.mutating())
	}
}

func TestStopDefaultsItsTimeout(t *testing.T) {
	f := &fakeBackend{}
	f.add("aaa", "running", managedBy(testNode, "w-1"))
	a := newTestAdapter(t, f)
	if err := a.Stop(context.Background(), "w-1", -1); err != nil {
		t.Fatal(err)
	}
	if f.stopTimeout != defaultStopTimeout {
		t.Errorf("negative timeout became %v, want %v", f.stopTimeout, defaultStopTimeout)
	}
	if err := a.Stop(context.Background(), "w-1", 0); err != nil || f.stopTimeout != 0 {
		t.Errorf("zero timeout must be passed through (kill now): %v, %v", err, f.stopTimeout)
	}
}

func TestCreateLabelsAndNamesTheContainer(t *testing.T) {
	f := &fakeBackend{}
	a := newTestAdapter(t, f)
	spec := validSpec()

	c, err := a.Create(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.created) != 1 {
		t.Fatalf("created %d containers", len(f.created))
	}
	req := f.created[0]
	wantLabels := map[string]string{
		LabelManaged:      "true",
		LabelSchema:       "1",
		LabelNodeID:       testNode,
		LabelWorkloadID:   "w-1",
		LabelWorkloadName: "web",
	}
	if !reflect.DeepEqual(req.Labels, wantLabels) {
		t.Errorf("labels = %v, want %v", req.Labels, wantLabels)
	}
	if req.Name != "dockfleet-w-1" || !reflect.DeepEqual(req.Spec, spec) {
		t.Errorf("request = %+v", req)
	}
	if c.ID != "id-w-1" || c.State != StateCreated || c.WorkloadID != "w-1" || c.NodeID != testNode || c.Name != "dockfleet-w-1" || c.Image != "nginx:1.27" {
		t.Errorf("returned container = %+v", c)
	}
	// Creating does not start: the only mutating call is the create itself.
	if got := f.mutating(); !reflect.DeepEqual(got, []string{"create dockfleet-w-1"}) {
		t.Errorf("mutating calls = %v", got)
	}
}

func TestCreateRejectsPolicyViolationsBeforeTouchingDocker(t *testing.T) {
	f := &fakeBackend{}
	a := newTestAdapter(t, f)
	spec := validSpec()
	spec.Privileged = true
	spec.Mounts = []Mount{{Type: MountBind, Source: "/var/run/docker.sock", Target: "/var/run/docker.sock"}}

	_, err := a.Create(context.Background(), spec)
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("error = %v, want ErrRejected", err)
	}
	for _, field := range []string{"Privileged", "Mounts"} {
		if !strings.Contains(err.Error(), field+":") {
			t.Errorf("error should explain %s: %v", field, err)
		}
	}
	if len(f.calls) != 0 {
		t.Fatalf("a rejected spec must not reach Docker at all: %v", f.calls)
	}
}

func TestCreateConflicts(t *testing.T) {
	t.Run("workload already has a container", func(t *testing.T) {
		f := &fakeBackend{}
		f.add("aaa", "exited", managedBy(testNode, "w-1"))
		_, err := newTestAdapter(t, f).Create(context.Background(), validSpec())
		if !errors.Is(err, ErrConflict) || len(f.created) != 0 {
			t.Fatalf("error = %v, created = %d", err, len(f.created))
		}
	})
	t.Run("name used by an unmanaged container", func(t *testing.T) {
		f := &fakeBackend{createErr: ErrConflict}
		f.add("zzz", "running", map[string]string{})
		_, err := newTestAdapter(t, f).Create(context.Background(), validSpec())
		if !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "left untouched") {
			t.Fatalf("error = %v", err)
		}
		if got := f.mutating(); !reflect.DeepEqual(got, []string{"create dockfleet-w-1"}) {
			t.Errorf("only the create attempt may happen, got %v", got)
		}
	})
	t.Run("image missing", func(t *testing.T) {
		f := &fakeBackend{createErr: ErrImageNotFound}
		_, err := newTestAdapter(t, f).Create(context.Background(), validSpec())
		if !errors.Is(err, ErrImageNotFound) || !strings.Contains(err.Error(), "nginx:1.27") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("other failures keep their cause", func(t *testing.T) {
		cause := errors.New("no space left on device")
		f := &fakeBackend{createErr: cause}
		if _, err := newTestAdapter(t, f).Create(context.Background(), validSpec()); !errors.Is(err, cause) {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestInspectReportsState(t *testing.T) {
	f := &fakeBackend{}
	f.containers = append(f.containers, fakeContainer{rawInspect{
		rawContainer: rawContainer{ID: "aaa", Names: []string{"/dockfleet-w-1"}, Image: "nginx", Labels: managedBy(testNode, "w-1"), State: "exited"},
		ExitCode:     137,
		Error:        "oom",
		OOMKilled:    true,
		RestartCount: 2,
		Health:       "unhealthy",
		StartedAt:    time.Unix(1700000100, 0).UTC(),
		FinishedAt:   time.Unix(1700000200, 0).UTC(),
	}})
	got, err := newTestAdapter(t, f).Inspect(context.Background(), "w-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "aaa" || got.State != StateExited || got.Running || got.ExitCode != 137 || !got.OOMKilled || got.RestartCount != 2 ||
		got.Health != "unhealthy" || got.Error != "oom" || got.WorkloadID != "w-1" || got.Name != "dockfleet-w-1" ||
		!got.StartedAt.Equal(time.Unix(1700000100, 0)) || !got.FinishedAt.Equal(time.Unix(1700000200, 0)) {
		t.Fatalf("unexpected state: %+v", got)
	}
}

func TestInspectRechecksOwnership(t *testing.T) {
	// The list says the container is ours, but the inspected labels disagree.
	f := &fakeBackend{}
	f.add("aaa", "running", managedBy(testNode, "w-1"))
	a, err := newAdapter(&relabeling{fakeBackend: f, labels: map[string]string{LabelManaged: "false"}}, testNode, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Inspect(context.Background(), "w-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("error = %v, want ErrNotFound", err)
	}
}

// relabeling wraps a backend and swaps the labels returned by inspect.
type relabeling struct {
	*fakeBackend
	labels map[string]string
}

func (r *relabeling) inspect(ctx context.Context, id string) (rawInspect, error) {
	info, err := r.fakeBackend.inspect(ctx, id)
	info.Labels = r.labels
	return info, err
}

func TestLogsAreDemultiplexedAndBounded(t *testing.T) {
	f := &fakeBackend{}
	f.add("aaa", "running", managedBy(testNode, "w-1"))
	f.logStream = append(frame(streamStdout, "hello\n"), frame(streamStderr, "oops\n")...)
	a := newTestAdapter(t, f)

	logs, err := a.Logs(context.Background(), "w-1", LogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []LogChunk{{Stream: "stdout", Text: "hello\n"}, {Stream: "stderr", Text: "oops\n"}}
	if !reflect.DeepEqual(logs.Chunks, want) || logs.Truncated {
		t.Fatalf("logs = %+v", logs)
	}

	logs, err = a.Logs(context.Background(), "w-1", LogOptions{MaxBytes: 8})
	if err != nil {
		t.Fatal(err)
	}
	if !logs.Truncated || totalBytes(logs) != 8 {
		t.Fatalf("logs = %+v, want 8 bytes and Truncated", logs)
	}
}

func TestStatsReadsUsage(t *testing.T) {
	f := &fakeBackend{statsJSON: `{"read":"2026-01-01T00:00:00Z","cpu_stats":{"cpu_usage":{"total_usage":500},"system_cpu_usage":2000,"online_cpus":2},"precpu_stats":{"cpu_usage":{"total_usage":300},"system_cpu_usage":1000},"memory_stats":{"usage":300,"limit":1000,"stats":{"inactive_file":100}},"pids_stats":{"current":4}}`}
	f.add("aaa", "running", managedBy(testNode, "w-1"))
	u, err := newTestAdapter(t, f).Stats(context.Background(), "w-1")
	if err != nil {
		t.Fatal(err)
	}
	if u.MemoryUsageBytes != 200 || u.MemoryLimitBytes != 1000 || u.Pids != 4 || !u.HasCPUPercent {
		t.Fatalf("unexpected usage: %+v", u)
	}
}

func TestPullImageValidatesTheReference(t *testing.T) {
	f := &fakeBackend{}
	a := newTestAdapter(t, f)
	if err := a.PullImage(context.Background(), "alpine:3.20"); err != nil || !reflect.DeepEqual(f.pulled, []string{"alpine:3.20"}) {
		t.Fatalf("PullImage: %v, pulled %v", err, f.pulled)
	}
	for _, ref := range []string{"", "-x", "a b", strings.Repeat("a", 256)} {
		if err := a.PullImage(context.Background(), ref); !errors.Is(err, ErrInvalid) {
			t.Errorf("PullImage(%q) error = %v, want ErrInvalid", ref, err)
		}
	}
}

func TestPingAndClosePassThrough(t *testing.T) {
	f := &fakeBackend{}
	a := newTestAdapter(t, f)
	if err := a.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil || !f.closed {
		t.Fatalf("Close: %v, closed = %v", err, f.closed)
	}
}

func TestNewAdapterValidatesNodeIDAndHost(t *testing.T) {
	if _, err := newAdapter(&fakeBackend{}, "bad id", nil); !errors.Is(err, ErrInvalid) {
		t.Errorf("error = %v, want ErrInvalid", err)
	}
	if _, err := NewAdapter(AdapterConfig{Host: "tcp://10.0.0.5:2375", NodeID: testNode}); err == nil {
		t.Error("a remote Docker host must be refused")
	}
	if _, err := NewAdapter(AdapterConfig{Host: "unix:///var/run/docker.sock", NodeID: "bad id"}); err == nil {
		t.Error("an invalid node ID must be refused")
	}
}

func TestValidateHost(t *testing.T) {
	for _, h := range []string{"unix:///var/run/docker.sock", "unix:///home/u/.docker/run/docker.sock", "tcp://127.0.0.1:2375", "tcp://localhost:2375", "tcp://[::1]:2375"} {
		if err := validateHost(h); err != nil {
			t.Errorf("validateHost(%q) = %v", h, err)
		}
	}
	for _, h := range []string{"", "unix://", "tcp://10.0.0.5:2375", "tcp://docker.example.com:2375", "tcp://127.0.0.1", "http://127.0.0.1:2375", "ssh://host", "npipe:////./pipe/docker_engine"} {
		if err := validateHost(h); err == nil {
			t.Errorf("validateHost(%q) succeeded, want an error", h)
		}
	}
}

func frame(stream byte, text string) []byte {
	header := make([]byte, frameHeaderLen)
	header[0] = stream
	binary.BigEndian.PutUint32(header[4:], uint32(len(text)))
	return append(header, text...)
}

func totalBytes(l Logs) int {
	n := 0
	for _, c := range l.Chunks {
		n += len(c.Text)
	}
	return n
}
