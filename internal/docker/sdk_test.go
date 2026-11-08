package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/client"
)

func TestBuildConfigsHardensEveryContainer(t *testing.T) {
	req := createRequest{
		Name:   "dockfleet-w-1",
		Labels: map[string]string{LabelManaged: "true", LabelWorkloadID: "w-1"},
		Spec:   validSpec(),
	}
	cfg, host := buildConfigs(req)

	if cfg.Image != "nginx:1.27" || !reflect.DeepEqual([]string(cfg.Cmd), []string{"nginx", "-g", "daemon off;"}) || !reflect.DeepEqual(cfg.Env, []string{"MODE=test"}) {
		t.Errorf("unexpected container config: %+v", cfg)
	}
	if !reflect.DeepEqual(cfg.Labels, req.Labels) || cfg.Tty || cfg.OpenStdin {
		t.Errorf("labels = %v, tty = %v", cfg.Labels, cfg.Tty)
	}

	if host.Privileged {
		t.Error("privileged mode must always be off")
	}
	if !reflect.DeepEqual([]string(host.CapDrop), []string{"ALL"}) || len(host.CapAdd) != 0 {
		t.Errorf("capabilities: drop %v add %v, want drop ALL and add nothing", host.CapDrop, host.CapAdd)
	}
	if !reflect.DeepEqual(host.SecurityOpt, []string{"no-new-privileges:true"}) {
		t.Errorf("SecurityOpt = %v", host.SecurityOpt)
	}
	if host.NetworkMode != "bridge" || host.RestartPolicy.Name != "no" || host.AutoRemove || host.ReadonlyRootfs {
		t.Errorf("network %q restart %q autoremove %v readonly %v", host.NetworkMode, host.RestartPolicy.Name, host.AutoRemove, host.ReadonlyRootfs)
	}
	if host.Memory != 128<<20 || host.MemorySwap != 128<<20 || host.NanoCPUs != 500_000_000 || host.PidsLimit == nil || *host.PidsLimit != 512 {
		t.Errorf("resources: memory %d swap %d nanocpus %d pids %v", host.Memory, host.MemorySwap, host.NanoCPUs, host.PidsLimit)
	}
	if host.LogConfig.Type != "json-file" || host.LogConfig.Config["max-size"] != "10m" || host.LogConfig.Config["max-file"] != "3" {
		t.Errorf("logs are not bounded: %+v", host.LogConfig)
	}
	if len(host.Binds) != 0 || len(host.Devices) != 0 || host.PidMode != "" || host.IpcMode != "" || host.UTSMode != "" || host.UsernsMode != "" || host.CgroupnsMode != "" {
		t.Errorf("no binds, devices or host namespaces may be configured: %+v", host)
	}

	bindings := host.PortBindings["80/tcp"]
	if len(bindings) != 1 || bindings[0].HostIP != "127.0.0.1" || bindings[0].HostPort != "8080" {
		t.Errorf("port bindings = %+v, want 127.0.0.1:8080", host.PortBindings)
	}
	if _, ok := cfg.ExposedPorts["80/tcp"]; !ok {
		t.Errorf("exposed ports = %v", cfg.ExposedPorts)
	}
	if opts := host.Tmpfs["/tmp"]; !strings.Contains(opts, "noexec") || !strings.Contains(opts, "nosuid") || !strings.Contains(opts, "size=67108864") {
		t.Errorf("tmpfs options = %q", opts)
	}
	if len(host.Mounts) != 0 {
		t.Errorf("a tmpfs-only spec has no mounts: %+v", host.Mounts)
	}
}

func TestBuildConfigsTurnsAHealthCheckIntoDockersExecForm(t *testing.T) {
	spec := validSpec()
	cfg, _ := buildConfigs(createRequest{Name: "x", Spec: spec})
	if cfg.Healthcheck != nil {
		t.Fatalf("a container without a health check got %+v", cfg.Healthcheck)
	}

	spec.HealthCheck = validHealth()
	cfg, _ = buildConfigs(createRequest{Name: "x", Spec: spec})
	h := cfg.Healthcheck
	if h == nil || !reflect.DeepEqual(h.Test, []string{"CMD", "curl", "-f", "http://localhost/"}) ||
		h.Interval != 10*time.Second || h.Timeout != 2*time.Second || h.StartPeriod != 5*time.Second || h.Retries != 3 {
		t.Fatalf("health config = %+v; the command must run directly (CMD), never through a shell", h)
	}
	// The spec's own slice is not shared with the request to Docker.
	h.Test[1] = "changed"
	if spec.HealthCheck.Command[0] != "curl" {
		t.Fatal("the health config shares the spec's command")
	}
}

func TestBuildConfigsIgnoresUnsafeFieldsEvenIfPolicyWereBypassed(t *testing.T) {
	spec := validSpec()
	spec.Privileged = true
	spec.Devices = []string{"/dev/mem"}
	spec.PidMode, spec.IpcMode, spec.UTSMode, spec.UsernsMode = "host", "host", "host", "host"
	spec.SecurityOpt = []string{"seccomp=unconfined"}
	spec.NetworkMode = "host"
	_, host := buildConfigs(createRequest{Name: "x", Spec: spec})

	if host.Privileged || len(host.Devices) != 0 || host.PidMode != "" || host.IpcMode != "" || host.UTSMode != "" || host.UsernsMode != "" {
		t.Errorf("unsafe fields leaked into the Docker configuration: %+v", host)
	}
	if !reflect.DeepEqual(host.SecurityOpt, []string{"no-new-privileges:true"}) || host.NetworkMode != "bridge" {
		t.Errorf("security options %v, network %q", host.SecurityOpt, host.NetworkMode)
	}
}

func TestBuildConfigsMapsPortsMountsAndLimits(t *testing.T) {
	spec := validSpec()
	spec.CapAdd = []string{"cap_net_bind_service"}
	spec.ReadOnlyRootFS = true
	spec.NetworkMode = "none"
	spec.PidsLimit = 64
	spec.User = "1000:1000"
	spec.Ports = []PortMapping{{ContainerPort: 53, Protocol: "udp"}, {ContainerPort: 80, HostPort: 8080, HostIP: "::1"}}
	spec.Mounts = []Mount{
		{Type: MountBind, Source: "/srv/data", Target: "/data", ReadOnly: true},
		{Type: MountVolume, Source: "dockfleet-cache", Target: "/cache"},
		{Type: MountTmpfs, Target: "/scratch", SizeBytes: 1 << 20},
	}
	cfg, host := buildConfigs(createRequest{Name: "x", Spec: spec})

	if !reflect.DeepEqual([]string(host.CapAdd), []string{"NET_BIND_SERVICE"}) || !host.ReadonlyRootfs || host.NetworkMode != "none" || *host.PidsLimit != 64 || cfg.User != "1000:1000" {
		t.Errorf("unexpected host config: %+v", host)
	}
	udp := host.PortBindings["53/udp"]
	if len(udp) != 1 || udp[0].HostIP != "127.0.0.1" || udp[0].HostPort != "" {
		t.Errorf("a udp port without host port binds loopback and lets Docker pick: %+v", udp)
	}
	if v6 := host.PortBindings["80/tcp"]; len(v6) != 1 || v6[0].HostIP != "::1" {
		t.Errorf("explicit loopback address lost: %+v", v6)
	}
	if len(host.Mounts) != 2 {
		t.Fatalf("mounts = %+v", host.Mounts)
	}
	bind := host.Mounts[0]
	if string(bind.Type) != "bind" || bind.Source != "/srv/data" || bind.Target != "/data" || !bind.ReadOnly || bind.BindOptions == nil || string(bind.BindOptions.Propagation) != "rprivate" {
		t.Errorf("bind mount = %+v", bind)
	}
	if vol := host.Mounts[1]; string(vol.Type) != "volume" || vol.Source != "dockfleet-cache" || vol.Target != "/cache" {
		t.Errorf("volume mount = %+v", vol)
	}
	if !strings.Contains(host.Tmpfs["/scratch"], "size=1048576") {
		t.Errorf("tmpfs = %v", host.Tmpfs)
	}
}

// fakeDaemon is a Docker Engine API stand-in behind an http.RoundTripper, so
// the SDK-backed code runs for real (request building, JSON, error mapping)
// without a daemon, a socket or a port.
type fakeDaemon struct {
	mu         sync.Mutex
	containers []daemonContainer
	requests   []daemonRequest
	failures   map[string]int // "METHOD /path-regexp" -> status to answer with
}

type daemonContainer struct {
	ID     string
	Name   string
	Labels map[string]string
	State  string
}

type daemonRequest struct {
	Method string
	Path   string
	Query  url.Values
	Body   []byte
}

var versionPrefix = regexp.MustCompile(`^/v[0-9.]+`)

func (d *fakeDaemon) RoundTrip(r *http.Request) (*http.Response, error) {
	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
		_ = r.Body.Close()
	}
	req := daemonRequest{Method: r.Method, Path: versionPrefix.ReplaceAllString(r.URL.Path, ""), Query: r.URL.Query(), Body: body}
	d.mu.Lock()
	d.requests = append(d.requests, req)
	d.mu.Unlock()

	status, payload, contentType := d.answer(req)
	header := http.Header{"Content-Type": {contentType}, "Api-Version": {"1.45"}, "Ostype": {"linux"}}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(payload)), Request: r}, nil
}

func (d *fakeDaemon) sent() []daemonRequest {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]daemonRequest(nil), d.requests...)
}

// writes lists the requests that change something on the daemon.
func (d *fakeDaemon) writes() []string {
	var out []string
	for _, r := range d.sent() {
		if r.Method == http.MethodPost || r.Method == http.MethodDelete {
			out = append(out, r.Method+" "+r.Path)
		}
	}
	return out
}

func errorBody(msg string) string {
	b, _ := json.Marshal(map[string]string{"message": msg})
	return string(b)
}

func (d *fakeDaemon) find(id string) *daemonContainer {
	for i := range d.containers {
		if d.containers[i].ID == id {
			return &d.containers[i]
		}
	}
	return nil
}

var containerPath = regexp.MustCompile(`^/containers/([^/]+)(/[a-z]+)?$`)

func (d *fakeDaemon) answer(r daemonRequest) (int, string, string) {
	const jsonType = "application/json"
	for pattern, status := range d.failures {
		parts := strings.SplitN(pattern, " ", 2)
		if parts[0] == r.Method && regexp.MustCompile(parts[1]).MatchString(r.Path) {
			return status, errorBody("simulated failure for " + pattern), jsonType
		}
	}

	switch {
	case r.Path == "/_ping":
		return 200, "OK", "text/plain"

	case r.Method == http.MethodGet && r.Path == "/containers/json":
		want := map[string]map[string]bool{}
		_ = json.Unmarshal([]byte(r.Query.Get("filters")), &want)
		var out []map[string]any
		for _, c := range d.containers {
			match := true
			for label := range want["label"] {
				k, v, _ := strings.Cut(label, "=")
				if c.Labels[k] != v {
					match = false
				}
			}
			if match {
				out = append(out, map[string]any{"Id": c.ID, "Names": []string{"/" + c.Name}, "Image": "nginx:1.27", "State": c.State, "Created": 1700000000, "Labels": c.Labels})
			}
		}
		b, _ := json.Marshal(out)
		return 200, string(b), jsonType

	case r.Method == http.MethodPost && r.Path == "/containers/create":
		return 201, `{"Id":"c-new","Warnings":[]}`, jsonType

	case r.Method == http.MethodPost && r.Path == "/images/create":
		return 200, `{"status":"Pulling from library/alpine"}` + "\n" + `{"status":"Download complete"}` + "\n", jsonType
	}

	m := containerPath.FindStringSubmatch(r.Path)
	if m == nil {
		return 404, errorBody("unexpected request " + r.Method + " " + r.Path), jsonType
	}
	c := d.find(m[1])
	if c == nil {
		return 404, errorBody("No such container: " + m[1]), jsonType
	}
	switch {
	case r.Method == http.MethodPost && m[2] == "/start", r.Method == http.MethodPost && m[2] == "/stop":
		return 204, "", jsonType
	case r.Method == http.MethodDelete && m[2] == "":
		if c.State == "running" && r.Query.Get("force") != "1" {
			return 409, errorBody("cannot remove a running container"), jsonType
		}
		return 204, "", jsonType
	case r.Method == http.MethodGet && m[2] == "/json":
		return 200, `{"Id":"` + c.ID + `","Name":"/` + c.Name + `","Created":"2026-03-01T10:00:00.123456789Z","RestartCount":1,` +
			`"State":{"Status":"exited","Running":false,"ExitCode":137,"Error":"","OOMKilled":true,"StartedAt":"2026-03-01T10:00:01Z","FinishedAt":"0001-01-01T00:00:00Z","Health":{"Status":"unhealthy"}},` +
			`"Config":{"Image":"nginx:1.27","Labels":` + mustJSON(c.Labels) + `}}`, jsonType
	case r.Method == http.MethodGet && m[2] == "/logs":
		return 200, string(append(frame(streamStdout, "hello\n"), frame(streamStderr, "oops\n")...)), "application/vnd.docker.multiplexed-stream"
	case r.Method == http.MethodGet && m[2] == "/stats":
		return 200, `{"read":"2026-03-01T10:00:02Z","memory_stats":{"usage":2048,"limit":4096},"cpu_stats":{"cpu_usage":{"total_usage":10},"system_cpu_usage":100,"online_cpus":1},"precpu_stats":{"cpu_usage":{"total_usage":5},"system_cpu_usage":50},"pids_stats":{"current":3}}`, jsonType
	}
	return 404, errorBody("unexpected request " + r.Method + " " + r.Path), jsonType
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func newDaemonAdapter(t *testing.T, d *fakeDaemon) *Adapter {
	t.Helper()
	b, err := newSDKBackend("tcp://127.0.0.1:2375", client.WithHTTPClient(&http.Client{Transport: d}), client.WithVersion("1.45"))
	if err != nil {
		t.Fatal(err)
	}
	a, err := newAdapter(b, testNode, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a
}

func daemonWithContainers() *fakeDaemon {
	return &fakeDaemon{containers: []daemonContainer{
		{ID: "c1", Name: "dockfleet-w-1", Labels: managedBy(testNode, "w-1"), State: "running"},
		{ID: "c2", Name: "postgres", Labels: map[string]string{"com.docker.compose.project": "shop"}, State: "running"},
		{ID: "c3", Name: "dockfleet-w-9", Labels: managedBy("other-node", "w-9"), State: "running"},
	}}
}

func TestSDKListSendsLabelFilters(t *testing.T) {
	d := daemonWithContainers()
	got, err := newDaemonAdapter(t, d).List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "c1" || got[0].WorkloadID != "w-1" || got[0].State != StateRunning || got[0].Name != "dockfleet-w-1" {
		t.Fatalf("List = %+v, want only c1", got)
	}
	reqs := d.sent()
	if len(reqs) != 1 || reqs[0].Method != http.MethodGet || reqs[0].Path != "/containers/json" || reqs[0].Query.Get("all") != "1" {
		t.Fatalf("requests = %+v", reqs)
	}
	var filters map[string]map[string]bool
	if err := json.Unmarshal([]byte(reqs[0].Query.Get("filters")), &filters); err != nil {
		t.Fatal(err)
	}
	want := map[string]map[string]bool{"label": {"io.dockfleet.managed=true": true, "io.dockfleet.node-id=" + testNode: true}}
	if !reflect.DeepEqual(filters, want) {
		t.Fatalf("filters = %v, want %v", filters, want)
	}
}

func TestSDKLifecycleOnlyTouchesTheManagedContainer(t *testing.T) {
	d := daemonWithContainers()
	a := newDaemonAdapter(t, d)
	ctx := context.Background()

	if err := a.Start(ctx, "w-1"); err != nil {
		t.Fatal(err)
	}
	if err := a.Stop(ctx, "w-1", 3*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := a.Remove(ctx, "w-1", true); err != nil {
		t.Fatal(err)
	}
	want := []string{"POST /containers/c1/start", "POST /containers/c1/stop", "DELETE /containers/c1"}
	if got := d.writes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("writes = %v, want %v", got, want)
	}
	for _, r := range d.sent() {
		if r.Method == http.MethodPost && r.Path == "/containers/c1/stop" && r.Query.Get("t") != "3" {
			t.Errorf("stop timeout = %q, want 3", r.Query.Get("t"))
		}
		if r.Method == http.MethodDelete && (r.Query.Get("force") != "1" || r.Query.Get("v") != "1") {
			t.Errorf("remove query = %v, want force and volumes", r.Query)
		}
		if strings.Contains(r.Path, "c2") || strings.Contains(r.Path, "c3") {
			t.Errorf("a request reached an unrelated container: %s %s", r.Method, r.Path)
		}
	}

	// Workloads that only exist as unrelated or foreign containers cannot be acted on.
	before := len(d.writes())
	for _, id := range []string{"postgres", "w-9", "c2", "shop"} {
		if err := a.Remove(ctx, id, true); !errors.Is(err, ErrNotFound) {
			t.Errorf("Remove(%q) error = %v, want ErrNotFound", id, err)
		}
	}
	if len(d.writes()) != before {
		t.Fatalf("unrelated containers were modified: %v", d.writes())
	}
}

func TestSDKRemoveWithoutForceRefusesRunningContainers(t *testing.T) {
	d := daemonWithContainers()
	err := newDaemonAdapter(t, d).Remove(context.Background(), "w-1", false)
	if !errors.Is(err, ErrStateConflict) {
		t.Fatalf("error = %v, want ErrStateConflict", err)
	}
}

func TestSDKCreateSendsTheHardenedBody(t *testing.T) {
	d := daemonWithContainers()
	a := newDaemonAdapter(t, d)
	spec := validSpec()
	spec.WorkloadID, spec.WorkloadName = "w-new", "fresh"

	c, err := a.Create(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if c.ID != "c-new" || c.Name != "dockfleet-w-new" || c.State != StateCreated {
		t.Errorf("container = %+v", c)
	}
	if got := d.writes(); !reflect.DeepEqual(got, []string{"POST /containers/create"}) {
		t.Fatalf("writes = %v, want only the create", got)
	}

	var create daemonRequest
	for _, r := range d.sent() {
		if r.Path == "/containers/create" {
			create = r
		}
	}
	if create.Query.Get("name") != "dockfleet-w-new" {
		t.Errorf("container name = %q", create.Query.Get("name"))
	}
	var body struct {
		Image      string
		Labels     map[string]string
		Tty        bool
		HostConfig struct {
			Privileged    bool
			CapDrop       []string
			CapAdd        []string
			SecurityOpt   []string
			NetworkMode   string
			Binds         []string
			Devices       []any
			PidMode       string
			Memory        int64
			NanoCpus      int64
			PidsLimit     int64
			RestartPolicy struct{ Name string }
			PortBindings  map[string][]struct{ HostIp, HostPort string }
		}
	}
	if err := json.Unmarshal(create.Body, &body); err != nil {
		t.Fatalf("create body is not JSON: %v\n%s", err, create.Body)
	}
	hc := body.HostConfig
	if body.Image != "nginx:1.27" || body.Tty {
		t.Errorf("image %q tty %v", body.Image, body.Tty)
	}
	if body.Labels[LabelManaged] != "true" || body.Labels[LabelNodeID] != testNode || body.Labels[LabelWorkloadID] != "w-new" || body.Labels[LabelWorkloadName] != "fresh" || body.Labels[LabelSchema] != "1" {
		t.Errorf("labels = %v", body.Labels)
	}
	if hc.Privileged || !reflect.DeepEqual(hc.CapDrop, []string{"ALL"}) || len(hc.CapAdd) != 0 || !reflect.DeepEqual(hc.SecurityOpt, []string{"no-new-privileges:true"}) {
		t.Errorf("host config is not hardened: %+v", hc)
	}
	if hc.NetworkMode != "bridge" || len(hc.Binds) != 0 || len(hc.Devices) != 0 || hc.PidMode != "" || hc.RestartPolicy.Name != "no" {
		t.Errorf("host config exposes the host: %+v", hc)
	}
	if hc.Memory != 128<<20 || hc.NanoCpus != 500_000_000 || hc.PidsLimit != 512 {
		t.Errorf("resource limits missing: %+v", hc)
	}
	if pb := hc.PortBindings["80/tcp"]; len(pb) != 1 || pb[0].HostIp != "127.0.0.1" || pb[0].HostPort != "8080" {
		t.Errorf("port bindings = %+v", hc.PortBindings)
	}
}

func TestSDKCreateErrorMapping(t *testing.T) {
	tests := []struct {
		name   string
		status int
		want   error
	}{
		{"image missing", 404, ErrImageNotFound},
		{"name taken", 409, ErrConflict},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := daemonWithContainers()
			d.failures = map[string]int{"POST ^/containers/create$": tt.status}
			spec := validSpec()
			spec.WorkloadID = "w-new"
			if _, err := newDaemonAdapter(t, d).Create(context.Background(), spec); !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestSDKInspectLogsAndStats(t *testing.T) {
	d := daemonWithContainers()
	a := newDaemonAdapter(t, d)
	ctx := context.Background()

	st, err := a.Inspect(ctx, "w-1")
	if err != nil {
		t.Fatal(err)
	}
	if st.ID != "c1" || st.State != StateExited || st.Running || st.ExitCode != 137 || !st.OOMKilled || st.RestartCount != 1 || st.Health != "unhealthy" || st.Name != "dockfleet-w-1" || st.WorkloadID != "w-1" {
		t.Errorf("state = %+v", st)
	}
	if !st.StartedAt.Equal(time.Date(2026, 3, 1, 10, 0, 1, 0, time.UTC)) || !st.FinishedAt.IsZero() {
		t.Errorf("times: started %v finished %v (Docker's zero time must become zero)", st.StartedAt, st.FinishedAt)
	}

	logs, err := a.Logs(ctx, "w-1", LogOptions{Tail: 50, Timestamps: true, Since: time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(logs.Chunks, []LogChunk{{"stdout", "hello\n"}, {"stderr", "oops\n"}}) {
		t.Errorf("logs = %+v", logs)
	}
	usage, err := a.Stats(ctx, "w-1")
	if err != nil {
		t.Fatal(err)
	}
	if usage.MemoryUsageBytes != 2048 || usage.MemoryLimitBytes != 4096 || usage.Pids != 3 || !usage.HasCPUPercent || usage.MemoryPercent != 50 {
		t.Errorf("usage = %+v", usage)
	}

	for _, r := range d.sent() {
		switch r.Path {
		case "/containers/c1/logs":
			q := r.Query
			if q.Get("tail") != "50" || q.Get("stdout") != "1" || q.Get("stderr") != "1" || q.Get("timestamps") != "1" || q.Get("since") != "1772355600.000000000" { // the SDK sends 2026-03-01T09:00:00Z as Unix time
				t.Errorf("logs query = %v", q)
			}
		case "/containers/c1/stats":
			if q := r.Query.Get("stream"); q != "0" && q != "false" {
				t.Errorf("stats must not stream, query = %v", r.Query)
			}
		}
	}
}

func TestSDKPullAndPing(t *testing.T) {
	d := daemonWithContainers()
	a := newDaemonAdapter(t, d)
	if err := a.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if err := a.PullImage(context.Background(), "alpine:3.20"); err != nil {
		t.Fatalf("PullImage: %v", err)
	}
	var pulled bool
	for _, r := range d.sent() {
		if r.Path == "/images/create" {
			pulled = true
			if !strings.Contains(r.Query.Get("fromImage"), "alpine") || r.Query.Get("tag") != "3.20" {
				t.Errorf("pull query = %v", r.Query)
			}
		}
	}
	if !pulled {
		t.Error("no pull request was sent")
	}
}

func TestSDKPullReportsFailuresFromTheStream(t *testing.T) {
	b := &pullStream{body: `{"status":"Pulling"}` + "\n" + `{"error":"manifest unknown","errorDetail":{"message":"manifest unknown"}}` + "\n"}
	backend, err := newSDKBackend("tcp://127.0.0.1:2375", client.WithHTTPClient(&http.Client{Transport: b}), client.WithVersion("1.45"))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.close()
	if err := backend.pull(context.Background(), "alpine:nope"); !errors.Is(err, ErrImageNotFound) {
		t.Fatalf("error = %v, want ErrImageNotFound", err)
	}
}

type pullStream struct{ body string }

func (p *pullStream) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewBufferString(p.body)), Request: r}, nil
}
