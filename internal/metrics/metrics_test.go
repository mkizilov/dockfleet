package metrics

import (
	"errors"
	"testing"
	"time"
)

var started = time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

func TestHTTPRequestsAreCountedByMethodRouteAndCode(t *testing.T) {
	m := New("v1.2.3", started)
	m.ObserveHTTP("GET", "/v1/nodes", 200, 3*time.Millisecond)
	m.ObserveHTTP("GET", "/v1/nodes", 200, 30*time.Millisecond)
	m.ObserveHTTP("GET", "/v1/nodes", 500, time.Second)
	m.ObserveHTTP("PUT", "/v1/services/{name}/scale", 412, time.Millisecond)
	out := scrape(t, m.Registry)

	wantLine(t, out, `dockfleet_http_request_duration_seconds_count{method="GET",route="/v1/nodes",code="200"} 2`)
	wantLine(t, out, `dockfleet_http_request_duration_seconds_count{method="GET",route="/v1/nodes",code="500"} 1`)
	wantLine(t, out, `dockfleet_http_request_duration_seconds_count{method="PUT",route="/v1/services/{name}/scale",code="412"} 1`)
	// 3 ms falls in the le=0.005 bucket, 30 ms in le=0.05.
	wantLine(t, out, `dockfleet_http_request_duration_seconds_bucket{method="GET",route="/v1/nodes",code="200",le="0.005"} 1`)
	wantLine(t, out, `dockfleet_http_request_duration_seconds_bucket{method="GET",route="/v1/nodes",code="200",le="0.05"} 2`)
	wantLine(t, out, `dockfleet_http_request_duration_seconds_bucket{method="GET",route="/v1/nodes",code="200",le="+Inf"} 2`)
	wantLine(t, out, `dockfleet_http_request_duration_seconds_sum{method="GET",route="/v1/nodes",code="500"} 1`)
}

func TestAClientCannotMakeUpMethods(t *testing.T) {
	m := New("dev", started)
	for _, method := range []string{"PROPFIND", "get", "", "X-ATTACK"} {
		m.ObserveHTTP(method, "unmatched", 405, time.Millisecond)
	}
	m.ObserveHTTP("DELETE", "unmatched", 405, time.Millisecond)
	out := scrape(t, m.Registry)
	wantLine(t, out, `dockfleet_http_request_duration_seconds_count{method="OTHER",route="unmatched",code="405"} 4`)
	wantLine(t, out, `dockfleet_http_request_duration_seconds_count{method="DELETE",route="unmatched",code="405"} 1`)
}

func TestGRPCCallsAreObserved(t *testing.T) {
	m := New("dev", started)
	m.ObserveGRPC("/dockfleet.v1.NodeService/Heartbeat", "OK", 2*time.Millisecond)
	m.ObserveGRPC("/dockfleet.v1.NodeService/Heartbeat", "Unauthenticated", time.Millisecond)
	out := scrape(t, m.Registry)
	wantLine(t, out, `dockfleet_grpc_request_duration_seconds_count{method="/dockfleet.v1.NodeService/Heartbeat",code="OK"} 1`)
	wantLine(t, out, `dockfleet_grpc_request_duration_seconds_count{method="/dockfleet.v1.NodeService/Heartbeat",code="Unauthenticated"} 1`)
}

func TestSchedulingPassesAreCountedWithTheirLatencyAndDecisions(t *testing.T) {
	m := New("dev", started)
	out := scrape(t, m.Registry)
	// Everything that can happen already shows as zero, so rate() works from the first scrape.
	wantLine(t, out, `dockfleet_scheduling_passes_total{result="ok"} 0`)
	wantLine(t, out, `dockfleet_scheduling_passes_total{result="error"} 0`)
	wantLine(t, out, `dockfleet_scheduling_decisions_total{result="placed"} 0`)
	wantLine(t, out, `dockfleet_scheduling_decisions_total{result="unschedulable"} 0`)
	wantLine(t, out, "dockfleet_scheduling_errors_total 0")
	wantLine(t, out, "dockfleet_scheduling_duration_seconds_count 0")

	m.ObserveSchedulingPass(40*time.Millisecond, 3, 1, nil)
	m.ObserveSchedulingPass(2*time.Millisecond, 0, 0, nil)
	m.ObserveSchedulingPass(time.Second, 5, 5, errors.New("the store is down"))
	out = scrape(t, m.Registry)
	wantLine(t, out, `dockfleet_scheduling_passes_total{result="ok"} 2`)
	wantLine(t, out, `dockfleet_scheduling_passes_total{result="error"} 1`)
	wantLine(t, out, "dockfleet_scheduling_errors_total 1")
	// A failed pass saved nothing, so its numbers are not decisions.
	wantLine(t, out, `dockfleet_scheduling_decisions_total{result="placed"} 3`)
	wantLine(t, out, `dockfleet_scheduling_decisions_total{result="unschedulable"} 1`)
	wantLine(t, out, "dockfleet_scheduling_duration_seconds_count 3")
	wantLine(t, out, `dockfleet_scheduling_duration_seconds_bucket{le="0.05"} 2`)
	wantLine(t, out, `dockfleet_scheduling_duration_seconds_bucket{le="1"} 3`)
}

func TestReconciliationErrorsAreCounted(t *testing.T) {
	m := New("dev", started)
	wantLine(t, scrape(t, m.Registry), "dockfleet_reconcile_errors_total 0")
	m.ObserveReconcilePass(10*time.Millisecond, nil)
	m.ObserveReconcilePass(10*time.Millisecond, errors.New("serialization failure"))
	m.ObserveReconcilePass(20*time.Millisecond, errors.New("deadline exceeded"))
	out := scrape(t, m.Registry)
	wantLine(t, out, "dockfleet_reconcile_errors_total 2")
	wantLine(t, out, `dockfleet_reconcile_passes_total{result="ok"} 1`)
	wantLine(t, out, `dockfleet_reconcile_passes_total{result="error"} 2`)
	wantLine(t, out, "dockfleet_reconcile_duration_seconds_count 3")
}

func TestDeploymentFailuresAreCountedPerService(t *testing.T) {
	m := New("dev", started)
	wantNoLine(t, scrape(t, m.Registry), "dockfleet_deployment_failures_total{")
	m.DeploymentFailed("web")
	m.DeploymentFailed("web")
	m.DeploymentFailed("api")
	out := scrape(t, m.Registry)
	wantLine(t, out, `dockfleet_deployment_failures_total{service="web"} 2`)
	wantLine(t, out, `dockfleet_deployment_failures_total{service="api"} 1`)
}

func TestBuildInfo(t *testing.T) {
	out := scrape(t, New("v1.2.3", started).Registry)
	wantLine(t, out, `dockfleet_build_info{version="v1.2.3"} 1`)
	// 2026-03-01T09:00:00Z is 1772355600 seconds after the epoch.
	wantLine(t, out, "dockfleet_start_time_seconds 1.7723556e+09")
}
