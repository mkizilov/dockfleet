package integration

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/api"
	"github.com/dockfleet/dockfleet/internal/dashboard"
	"github.com/dockfleet/dockfleet/internal/metrics"
)

// These tests look at a fleet that really runs (real agents on in-memory
// engines, a real manager over gRPC, the scheduler and the reconciler as the
// manager process runs them) through the metrics and the dashboard, instead of
// rows written into a store by hand.

func fetch(t *testing.T, h http.Handler, path, token string) (int, http.Header, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code, rec.Header(), rec.Body.String()
}

func seriesOf(t *testing.T, body string) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	for _, line := range strings.Split(body, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.LastIndex(line, " ")
		v, err := strconv.ParseFloat(line[i+1:], 64)
		if i < 0 || err != nil {
			t.Fatalf("%q is not a sample", line)
		}
		out[line[:i]] = v
	}
	return out
}

func TestMetricsAndTheDashboardDescribeARealDeployment(t *testing.T) {
	f := newLogFleet(t, "sim-1", "sim-2")
	f.deploy("web", 2)

	admin := api.New(api.Config{
		Store: f.m.store, Token: adminToken, NodeTTL: time.Minute, Logger: discardLogger(), InstanceID: "itest",
		Metrics: metrics.New("itest", time.Now()),
	})
	dash, err := dashboard.New(dashboard.Config{API: admin, Token: adminToken})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("metrics", func(t *testing.T) {
		if code, _, body := fetch(t, admin, "/metrics", ""); code != http.StatusUnauthorized || strings.Contains(body, "dockfleet_") {
			t.Fatalf("/metrics without the token = %d: %s", code, body)
		}
		if code, _, _ := fetch(t, admin, "/metrics", agentToken); code != http.StatusUnauthorized {
			t.Fatalf("/metrics with the agents' token = %d", code)
		}
		code, header, body := fetch(t, admin, "/metrics", adminToken)
		if code != http.StatusOK || header.Get("Content-Type") != metrics.ContentType {
			t.Fatalf("/metrics = %d %q", code, header.Get("Content-Type"))
		}
		s := seriesOf(t, body)
		for _, tt := range []struct {
			series string
			want   float64
		}{
			{`dockfleet_nodes{health="healthy"}`, 2},
			{`dockfleet_nodes{health="suspect"}`, 0},
			{`dockfleet_nodes{health="unavailable"}`, 0},
			{`dockfleet_node_healthy{node="sim-1"}`, 1},
			{`dockfleet_node_healthy{node="sim-2"}`, 1},
			{`dockfleet_services`, 1},
			{`dockfleet_service_replicas_desired{service="web"}`, 2},
			{`dockfleet_service_replicas{service="web",state="running"}`, 2},
			{`dockfleet_service_replicas{service="web",state="failed"}`, 0},
			{`dockfleet_service_replicas{service="web",state="unschedulable"}`, 0},
			{`dockfleet_service_rollout_failed{service="web"}`, 0},
			{`dockfleet_http_request_duration_seconds_count{method="GET",route="/metrics",code="401"}`, 2},
		} {
			if got, ok := s[tt.series]; !ok || got != tt.want {
				t.Errorf("%s = %v (present: %v), want %v", tt.series, got, ok, tt.want)
			}
		}
		// Each replica is promised 500m, and both are on the nodes the metrics list.
		var cpu, workloads float64
		for _, node := range []string{"sim-1", "sim-2"} {
			cpu += s[`dockfleet_node_cpu_millicores{node="`+node+`",kind="allocated"}`]
			workloads += s[`dockfleet_node_workloads{node="`+node+`"}`]
			if s[`dockfleet_node_cpu_millicores{node="`+node+`",kind="capacity"}`] <= 0 {
				t.Errorf("%s has no capacity in the metrics", node)
			}
		}
		if cpu != 1000 || workloads != 2 {
			t.Errorf("%v millicores promised to %v workloads, want 1000 and 2", cpu, workloads)
		}
	})

	t.Run("dashboard", func(t *testing.T) {
		if code, header, _ := fetch(t, dash, "/dashboard", ""); code != http.StatusSeeOther || !strings.HasPrefix(header.Get("Location"), "/dashboard/login") {
			t.Fatalf("/dashboard without a login = %d, Location %q", code, header.Get("Location"))
		}
		if code, _, _ := fetch(t, dash, "/dashboard", agentToken); code != http.StatusUnauthorized {
			t.Fatalf("/dashboard with the agents' token = %d, want 401", code)
		}

		code, _, overview := fetch(t, dash, "/dashboard", adminToken)
		if code != http.StatusOK {
			t.Fatalf("/dashboard = %d: %s", code, overview)
		}
		for _, want := range []string{
			`<span class="badge ok">2 healthy</span>`, "2 running of 2 desired", `<span class="badge ok">no problems</span>`,
			`href="/dashboard/nodes/sim-1"`, `href="/dashboard/nodes/sim-2"`, `href="/dashboard/services/web"`,
			"2 of 2", `<span class="badge ok">stable</span>`, "replica.scheduled",
		} {
			if !strings.Contains(overview, want) {
				t.Errorf("the overview lacks %q:\n%s", want, overview)
			}
		}

		code, _, service := fetch(t, dash, "/dashboard/services/web", adminToken)
		if code != http.StatusOK || strings.Count(service, `<span class="badge ok">running</span>`) != 2 ||
			!strings.Contains(service, "<code>nginx:1.0</code> (revision 1)") || !strings.Contains(service, "2 running of 2 wanted") {
			t.Fatalf("the service page = %d (want two running replicas of nginx:1.0):\n%s", code, service)
		}
		placed := 0
		for _, node := range []string{"sim-1", "sim-2"} {
			code, _, page := fetch(t, dash, "/dashboard/nodes/"+node, adminToken)
			if code != http.StatusOK || !strings.Contains(page, `<span class="badge ok">healthy</span>`) {
				t.Fatalf("the page of %s = %d:\n%s", node, code, page)
			}
			placed += strings.Count(page, "web-r1-")
		}
		if placed != 2 {
			t.Errorf("%d replicas on the node pages, want the 2 the service has", placed)
		}
	})

	t.Run("a scale-down is visible in both", func(t *testing.T) {
		f.admin.scale("web", 1)
		waitFor(t, "the service to settle at one replica", f.admin.converged("web", 1, f.engines()...))
		_, _, body := fetch(t, admin, "/metrics", adminToken)
		s := seriesOf(t, body)
		if s[`dockfleet_service_replicas_desired{service="web"}`] != 1 || s[`dockfleet_service_replicas{service="web",state="running"}`] != 1 {
			t.Errorf("after scaling to one: desired %v, running %v", s[`dockfleet_service_replicas_desired{service="web"}`], s[`dockfleet_service_replicas{service="web",state="running"}`])
		}
		_, _, overview := fetch(t, dash, "/dashboard", adminToken)
		if !strings.Contains(overview, "1 running of 1 desired") || !strings.Contains(overview, "replica.stopped") {
			t.Errorf("the overview after scaling to one lacks the new state:\n%s", overview)
		}
	})
}
