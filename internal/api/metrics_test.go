package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/metrics"
)

// These tests read the manager's metrics and audit records through the API's own
// handler, on the in-memory store and on PostgreSQL: the metrics of the fleet are
// worked out from the store on every scrape, so what they say must be what the
// API says about the same state.

// withMetrics puts the metrics behind the handlers, with the logs of the handler
// going to the returned buffer as JSON.
func (e *serviceEnv) withMetrics() (*metrics.Metrics, *bytes.Buffer) {
	e.t.Helper()
	m := metrics.New("test-version", e.clock)
	logs := &bytes.Buffer{}
	e.h = New(Config{
		Store: e.st, Token: token, NodeTTL: 30 * time.Second, Timing: scaleTiming, Logger: slog.New(slog.NewJSONHandler(logs, nil)),
		InstanceID: "test-manager", Now: func() time.Time { return e.clock }, Metrics: m,
	})
	return m, logs
}

// scrape reads GET /metrics as an administrator.
func (e *serviceEnv) scrape() (text string, series map[string]float64) {
	e.t.Helper()
	rec := e.request(http.MethodGet, "/metrics", "")
	if rec.Code != http.StatusOK {
		e.t.Fatalf("GET /metrics = %d: %s", rec.Code, rec.Body)
	}
	text = rec.Body.String()
	series = map[string]float64{}
	for _, line := range strings.Split(text, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.LastIndex(line, " ")
		v, err := strconv.ParseFloat(line[i+1:], 64)
		if i < 0 || err != nil {
			e.t.Fatalf("line %q is not a sample", line)
		}
		series[line[:i]] = v
	}
	return text, series
}

func wantSeries(t *testing.T, series map[string]float64, name string, want float64) {
	t.Helper()
	got, ok := series[name]
	if !ok {
		t.Errorf("no series %s", name)
		return
	}
	if got != want {
		t.Errorf("%s = %v, want %v", name, got, want)
	}
}

func testMetricsNeedTheAdminTokenAndAreOffWithoutMetrics(t *testing.T, e *serviceEnv) {
	e.create(minimalService)
	// Without metrics there is no endpoint.
	if rec := e.request(http.MethodGet, "/metrics", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("GET /metrics without metrics = %d, want 404", rec.Code)
	}

	e.withMetrics()
	for _, auth := range []string{"", "Bearer", "Bearer wrong-token-0123456789", "bearer " + token, "Basic " + token, token, "Bearer " + token + "x"} {
		rec := e.request(http.MethodGet, "/metrics", "", "Authorization", auth)
		if rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") != "Bearer" {
			t.Errorf("GET /metrics with %q = %d, WWW-Authenticate %q; want 401 with a challenge", auth, rec.Code, rec.Header().Get("WWW-Authenticate"))
		}
		if strings.Contains(rec.Body.String(), "dockfleet_") || strings.Contains(rec.Body.String(), "web") {
			t.Errorf("GET /metrics with %q leaked metrics: %s", auth, rec.Body)
		}
	}
	// A token in the query string is not a credential.
	if rec := e.request(http.MethodGet, "/metrics?token="+token, "", "Authorization", ""); rec.Code != http.StatusUnauthorized {
		t.Errorf("a token in the URL = %d, want 401", rec.Code)
	}
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		if rec := e.request(method, "/metrics", ""); rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s /metrics = %d, want 405", method, rec.Code)
		}
	}

	rec := e.request(http.MethodGet, "/metrics", "")
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != metrics.ContentType || rec.Header().Get("Cache-Control") != "no-store" ||
		rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("GET /metrics = %d, headers %v", rec.Code, rec.Header())
	}
	// The refused scrapes are in the latency metrics, by the code they got.
	_, series := e.scrape()
	wantSeries(t, series, `dockfleet_http_request_duration_seconds_count{method="GET",route="/metrics",code="401"}`, 8)
	wantSeries(t, series, `dockfleet_http_request_duration_seconds_count{method="GET",route="/metrics",code="200"}`, 1)
}

// The state of the fleet: nodes by health with their capacity and what is
// promised on them, and a service's replicas by state.
func testMetricsDescribeTheFleet(t *testing.T, e *serviceEnv) {
	e.withMetrics()
	e.addNode("n1", 4000, 8<<30)
	e.create(webService(2, 500))
	e.settle()

	// Two more nodes join, and then go quiet: n3 for a minute, n2 for three.
	e.addNode("n2", 2000, 4<<30)
	e.addNode("n3", 2000, 4<<30)
	e.down = map[string]bool{"n2": true}
	e.clock = e.clock.Add(2 * time.Minute)
	e.agents()
	e.down = map[string]bool{"n2": true, "n3": true}
	e.clock = e.clock.Add(time.Minute)
	e.agents()

	text, s := e.scrape()
	for _, family := range []string{
		"# TYPE dockfleet_nodes gauge", "# TYPE dockfleet_node_healthy gauge", "# TYPE dockfleet_node_cpu_millicores gauge",
		"# TYPE dockfleet_node_memory_bytes gauge", "# TYPE dockfleet_node_allocation_ratio gauge", "# TYPE dockfleet_services gauge",
		"# TYPE dockfleet_service_replicas gauge", "# TYPE dockfleet_service_replicas_desired gauge", "# TYPE dockfleet_rollouts gauge",
		"# TYPE dockfleet_http_request_duration_seconds histogram", "# TYPE dockfleet_build_info gauge",
	} {
		if !strings.Contains(text, family+"\n") {
			t.Errorf("no %q in:\n%s", family, text)
		}
	}

	// Nodes by health, and each node's own.
	wantSeries(t, s, `dockfleet_nodes{health="healthy"}`, 1)
	wantSeries(t, s, `dockfleet_nodes{health="suspect"}`, 1)
	wantSeries(t, s, `dockfleet_nodes{health="unavailable"}`, 1)
	wantSeries(t, s, `dockfleet_node_healthy{node="n1"}`, 1)
	wantSeries(t, s, `dockfleet_node_healthy{node="n2"}`, 0)
	wantSeries(t, s, `dockfleet_node_healthy{node="n3"}`, 0)

	// Capacity, what may be promised and what is promised: two replicas of 500m and 128 MiB are on n1.
	wantSeries(t, s, `dockfleet_node_cpu_millicores{node="n1",kind="capacity"}`, 4000)
	wantSeries(t, s, `dockfleet_node_cpu_millicores{node="n1",kind="allocatable"}`, 4000)
	wantSeries(t, s, `dockfleet_node_cpu_millicores{node="n1",kind="allocated"}`, 1000)
	wantSeries(t, s, `dockfleet_node_memory_bytes{node="n1",kind="capacity"}`, 8<<30)
	wantSeries(t, s, `dockfleet_node_memory_bytes{node="n1",kind="allocated"}`, 256<<20)
	wantSeries(t, s, `dockfleet_node_allocation_ratio{node="n1",resource="cpu"}`, 0.25)
	wantSeries(t, s, `dockfleet_node_allocation_ratio{node="n1",resource="memory"}`, 0.03125)
	wantSeries(t, s, `dockfleet_node_workloads{node="n1"}`, 2)
	wantSeries(t, s, `dockfleet_node_cpu_millicores{node="n2",kind="capacity"}`, 2000)
	wantSeries(t, s, `dockfleet_node_cpu_millicores{node="n2",kind="allocated"}`, 0)
	wantSeries(t, s, `dockfleet_node_allocation_ratio{node="n2",resource="cpu"}`, 0)
	wantSeries(t, s, `dockfleet_node_workloads{node="n3"}`, 0)

	// Replicas: wanted and, by state, what there is.
	wantSeries(t, s, `dockfleet_services`, 1)
	wantSeries(t, s, `dockfleet_service_replicas_desired{service="web"}`, 2)
	wantSeries(t, s, `dockfleet_service_replicas{service="web",state="running"}`, 2)
	for _, state := range []string{"starting", "failed", "unhealthy", "lost", "unschedulable", "outdated", "stopping"} {
		wantSeries(t, s, `dockfleet_service_replicas{service="web",state="`+state+`"}`, 0)
	}
	wantSeries(t, s, `dockfleet_service_rollout_failed{service="web"}`, 0)
	var rollouts float64
	for _, state := range []string{"progressing", "paused", "complete", "failed"} {
		v, ok := s[`dockfleet_rollouts{state="`+state+`"}`]
		if !ok {
			t.Errorf("no rollouts series for the state %s", state)
		}
		rollouts += v
	}
	if rollouts != 1 {
		t.Errorf("%v rollouts counted, want the one service's", rollouts)
	}
	wantSeries(t, s, `dockfleet_build_info{version="test-version"}`, 1)

	// What the metrics say is what the API says.
	var nodes struct {
		Nodes []nodeView `json:"nodes"`
	}
	decode(t, e.request(http.MethodGet, "/v1/nodes", ""), &nodes)
	if len(nodes.Nodes) != 3 {
		t.Fatalf("nodes = %+v", nodes.Nodes)
	}
	for _, n := range nodes.Nodes {
		healthy := 0.0
		if n.Health == "healthy" {
			healthy = 1
		}
		wantSeries(t, s, fmt.Sprintf(`dockfleet_node_healthy{node=%q}`, n.ID), healthy)
		wantSeries(t, s, fmt.Sprintf(`dockfleet_node_cpu_millicores{node=%q,kind="allocated"}`, n.ID), float64(n.Allocated.CPUMillis))
		wantSeries(t, s, fmt.Sprintf(`dockfleet_node_memory_bytes{node=%q,kind="allocatable"}`, n.ID), float64(n.Allocatable.MemoryBytes))
		wantSeries(t, s, fmt.Sprintf(`dockfleet_node_workloads{node=%q}`, n.ID), float64(n.Allocated.Workloads))
	}
	web := e.get("web")
	wantSeries(t, s, `dockfleet_service_replicas{service="web",state="running"}`, float64(web.Status.Scaling.Ready))
	wantSeries(t, s, `dockfleet_service_replicas_desired{service="web"}`, float64(web.Replicas))
}

// Metrics follow the state: more replicas wanted than fit, then containers that fail.
func testMetricsFollowTheFleetAsItChanges(t *testing.T, e *serviceEnv) {
	e.withMetrics()
	e.addNode("n1", 4000, 8<<30)
	e.create(webService(2, 500))
	e.settle()

	// Ten wanted, eight fit (4000m of 500m each): two wait for a node.
	e.scale("web", `{"replicas":10}`)
	_, s := e.scrape()
	wantSeries(t, s, `dockfleet_service_replicas_desired{service="web"}`, 10)
	wantSeries(t, s, `dockfleet_service_replicas{service="web",state="running"}`, 2) // the scheduler has not looked yet

	e.settle()
	_, s = e.scrape()
	wantSeries(t, s, `dockfleet_service_replicas_desired{service="web"}`, 10)
	wantSeries(t, s, `dockfleet_service_replicas{service="web",state="running"}`, 8)
	wantSeries(t, s, `dockfleet_service_replicas{service="web",state="unschedulable"}`, 2)
	wantSeries(t, s, `dockfleet_node_cpu_millicores{node="n1",kind="allocated"}`, 4000)
	wantSeries(t, s, `dockfleet_node_allocation_ratio{node="n1",resource="cpu"}`, 1)
	wantSeries(t, s, `dockfleet_node_workloads{node="n1"}`, 8)

	// The containers start to fail.
	e.failImage = "nginx:1.27"
	e.agents()
	_, s = e.scrape()
	wantSeries(t, s, `dockfleet_service_replicas{service="web",state="running"}`, 0)
	wantSeries(t, s, `dockfleet_service_replicas{service="web",state="failed"}`, 8)
	wantSeries(t, s, `dockfleet_service_replicas{service="web",state="unschedulable"}`, 2)

	// A deleted service is gone from the metrics.
	e.request(http.MethodDelete, "/v1/services/web", "")
	text, s := e.scrape()
	wantSeries(t, s, `dockfleet_services`, 0)
	if strings.Contains(text, `service="web"`) {
		t.Errorf("a deleted service is still exported:\n%s", text)
	}
}

func testRequestLatencyIsLabeledByRouteNotByPath(t *testing.T, e *serviceEnv) {
	e.withMetrics()
	e.create(minimalService)
	e.request(http.MethodGet, "/v1/nodes", "")
	e.request(http.MethodGet, "/v1/services/web", "")
	for i := 0; i < 30; i++ {
		e.request(http.MethodGet, fmt.Sprintf("/v1/services/nobody-%d", i), "") // 404 from the handler
	}
	e.request(http.MethodGet, "/no/such/page/xyz", "")
	e.request("PROPFIND", "/v1/nodes", "")
	e.request("X-ATTACK", "/v1/nodes", "")

	text, s := e.scrape()
	const prefix = "dockfleet_http_request_duration_seconds_count"
	wantSeries(t, s, prefix+`{method="GET",route="/v1/nodes",code="200"}`, 1)
	wantSeries(t, s, prefix+`{method="GET",route="/v1/services/{name}",code="200"}`, 1)
	wantSeries(t, s, prefix+`{method="GET",route="/v1/services/{name}",code="404"}`, 30)
	wantSeries(t, s, prefix+`{method="POST",route="/v1/services",code="201"}`, 1)
	wantSeries(t, s, prefix+`{method="GET",route="unmatched",code="404"}`, 1)
	wantSeries(t, s, prefix+`{method="OTHER",route="unmatched",code="405"}`, 2)
	for _, invented := range []string{"nobody-", "xyz", "PROPFIND", "X-ATTACK"} {
		if strings.Contains(text, invented) {
			t.Errorf("%q, which a client made up, is a label value:\n%s", invented, text)
		}
	}
	// The histogram has buckets, a sum and a count, and the buckets add up.
	wantSeries(t, s, `dockfleet_http_request_duration_seconds_bucket{method="GET",route="/v1/nodes",code="200",le="+Inf"}`, 1)
	if _, ok := s[`dockfleet_http_request_duration_seconds_sum{method="GET",route="/v1/nodes",code="200"}`]; !ok {
		t.Error("no sum for the latency of GET /v1/nodes")
	}
}

// auditRecord is a log record of the structured audit trail.
type auditRecord struct {
	Msg       string `json:"msg"`
	RequestID string `json:"request_id"`
	Actor     string `json:"actor"`
	Action    string `json:"action"`
	Path      string `json:"path"`
	Status    int    `json:"status"`
	Outcome   string `json:"outcome"`
	Remote    string `json:"remote"`
	Instance  string `json:"instance_id"`
}

func auditRecords(t *testing.T, logs *bytes.Buffer) []auditRecord {
	t.Helper()
	var out []auditRecord
	dec := json.NewDecoder(bytes.NewReader(logs.Bytes()))
	for {
		var rec auditRecord
		if err := dec.Decode(&rec); err != nil {
			if err == io.EOF {
				return out
			}
			t.Fatalf("the log is not JSON: %v\n%s", err, logs)
		}
		if rec.Msg == "audit" {
			out = append(out, rec)
		}
	}
}

func testAdministrativeChangesAreAudited(t *testing.T, e *serviceEnv) {
	_, logs := e.withMetrics()
	e.addNode("n1", 4000, 8<<30)

	// A change carries the request ID the client chose, and the event and the log record share it.
	rec := e.request(http.MethodPost, "/v1/services", webService(2, 500), "X-Request-ID", "change-0001")
	if rec.Code != http.StatusCreated || rec.Header().Get("X-Request-ID") != "change-0001" {
		t.Fatalf("create = %d, X-Request-ID %q", rec.Code, rec.Header().Get("X-Request-ID"))
	}
	created := e.eventsOf("service.created")
	if len(created) != 1 || created[0].Actor != adminActor || created[0].Details["request_id"] != "change-0001" || created[0].Details["remote"] != "192.0.2.1" ||
		created[0].Details["name"] != "web" {
		t.Fatalf("service.created events = %+v; want the actor, the request and where it came from, besides the usual details", created)
	}

	// Every kind of change records the same.
	e.scale("web", `{"replicas":3}`, "X-Request-ID", "change-0002")
	e.request(http.MethodPost, "/v1/services/web/pause", "", "X-Request-ID", "change-0003")
	e.request(http.MethodPost, "/v1/enrollments", `{"node_id":"worker-9","description":"rack 4"}`, "X-Request-ID", "change-0004")
	e.request(http.MethodDelete, "/v1/enrollments/worker-9", "", "X-Request-ID", "change-0005")
	e.request(http.MethodPost, "/v1/workloads", `{"name":"solo","image":"nginx:1.27","cpu_millis":100,"memory_bytes":1048576}`, "X-Request-ID", "change-0006")
	e.request(http.MethodDelete, "/v1/services/web", "", "X-Request-ID", "change-0007")
	for _, tt := range []struct{ action, request string }{
		{"service.scaled", "change-0002"}, {"service.paused", "change-0003"}, {"enrollment.created", "change-0004"},
		{"enrollment.revoked", "change-0005"}, {"workload.created", "change-0006"}, {"service.deleted", "change-0007"},
	} {
		events := e.eventsOf(tt.action)
		if len(events) != 1 || events[0].Details["request_id"] != tt.request || events[0].Details["remote"] != "192.0.2.1" || events[0].Actor != adminActor {
			t.Errorf("%s events = %+v; want one from %s", tt.action, events, tt.request)
		}
	}

	// The same changes are in the log, one record each, with the outcome.
	byID := map[string]auditRecord{}
	for _, r := range auditRecords(t, logs) {
		byID[r.RequestID] = r
	}
	for id, want := range map[string]auditRecord{
		"change-0001": {Action: "POST /v1/services", Path: "/v1/services", Status: 201},
		"change-0002": {Action: "PUT /v1/services/{name}/scale", Path: "/v1/services/web/scale", Status: 200},
		"change-0004": {Action: "POST /v1/enrollments", Path: "/v1/enrollments", Status: 201},
		"change-0005": {Action: "DELETE /v1/enrollments/{id}", Path: "/v1/enrollments/worker-9", Status: 200},
		"change-0007": {Action: "DELETE /v1/services/{name}", Path: "/v1/services/web", Status: 200},
	} {
		got, ok := byID[id]
		if !ok {
			t.Errorf("no audit record for %s", id)
			continue
		}
		if got.Action != want.Action || got.Path != want.Path || got.Status != want.Status || got.Actor != "admin" || got.Outcome != "accepted" ||
			got.Remote != "192.0.2.1" || got.Instance != "test-manager" {
			t.Errorf("audit record %s = %+v, want %+v as admin, accepted, from 192.0.2.1", id, got, want)
		}
	}
}

func testAuditRecordsRefusalsAndKeepSecretsOut(t *testing.T, e *serviceEnv) {
	_, logs := e.withMetrics()
	e.create(minimalService)
	before := len(auditRecords(t, logs))

	// Refused for lack of credentials: recorded as anonymous and denied, and it changes nothing.
	rec := e.request(http.MethodPut, "/v1/services/web/scale?note=hunter2", `{"replicas":9}`, "Authorization", "Bearer wrong-token-0123456789", "X-Request-ID", "denied-1")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status %d", rec.Code)
	}
	// Refused as invalid, and out of date.
	e.request(http.MethodPost, "/v1/services", `{"name":"BAD NAME"}`, "X-Request-ID", "invalid-1")
	e.request(http.MethodPut, "/v1/services/web", serviceBodyWith("nginx:1.28", "3"), "If-Match", `"99"`, "X-Request-ID", "stale-1")
	// Reads leave no audit record.
	e.request(http.MethodGet, "/v1/services", "", "X-Request-ID", "read-1")
	// A made-up ID is replaced.
	e.request(http.MethodDelete, "/v1/services/nobody", "", "X-Request-ID", "not valid!")
	e.request(http.MethodDelete, "/v1/services/nobody", "", "X-Request-ID", strings.Repeat("x", 65))

	byID := map[string]auditRecord{}
	var all []auditRecord
	for _, r := range auditRecords(t, logs)[before:] {
		byID[r.RequestID] = r
		all = append(all, r)
	}
	if r := byID["denied-1"]; r.Actor != "anonymous" || r.Outcome != "denied" || r.Status != http.StatusUnauthorized || r.Action != "PUT /v1/services/{name}/scale" {
		t.Errorf("denied request = %+v", r)
	}
	if r := byID["invalid-1"]; r.Actor != "admin" || r.Outcome != "rejected" || r.Status != http.StatusBadRequest {
		t.Errorf("invalid request = %+v", r)
	}
	if r := byID["stale-1"]; r.Outcome != "rejected" || r.Status != http.StatusPreconditionFailed {
		t.Errorf("stale request = %+v", r)
	}
	if _, ok := byID["read-1"]; ok {
		t.Error("a read left an audit record")
	}
	if len(all) != 5 {
		t.Errorf("%d audit records for 5 changes: %+v", len(all), all)
	}
	for _, r := range all {
		if r.RequestID == "not valid!" || len(r.RequestID) > 64 || r.RequestID == "" {
			t.Errorf("record %+v kept a request ID the client should not choose", r)
		}
	}

	// Nothing of the body, the query or the credentials is in the log.
	for _, secret := range []string{"hunter2", "wrong-token-0123456789", token, "nginx:1.27", "BAD NAME"} {
		if strings.Contains(logs.String(), secret) {
			t.Errorf("the log contains %q", secret)
		}
	}
	// And the refused requests changed nothing and left no event.
	if got := e.get("web"); got.Replicas != 2 || got.Version != 1 {
		t.Errorf("service = %+v", got)
	}
	e.wantEvents("service.created")
}
