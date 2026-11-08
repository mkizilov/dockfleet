package metrics

import (
	"context"
	"strconv"
	"time"
)

// requestBuckets are the upper bounds, in seconds, of the request latency histograms.
var requestBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// passBuckets are those of the control loop passes, which read and write the store.
var passBuckets = []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 15, 30}

// Metrics is the set of instruments the manager reports through: request and
// control loop latency, errors and deployment failures. The state of the fleet
// (nodes, replicas) is not kept here: collectors read it from the store on every
// scrape (see Registry.AddCollector), so it is always what the store says.
type Metrics struct {
	Registry *Registry

	httpDuration *HistogramVec
	grpcDuration *HistogramVec

	schedulingDuration  *HistogramVec
	schedulingPasses    *CounterVec
	schedulingErrors    *CounterVec
	schedulingDecisions *CounterVec

	reconcileDuration *HistogramVec
	reconcilePasses   *CounterVec
	reconcileErrors   *CounterVec

	deploymentFailures *CounterVec
}

// New returns the instruments in a new registry. version and the start time
// are exported as dockfleet_build_info and dockfleet_start_time_seconds.
func New(version string, now time.Time) *Metrics {
	r := NewRegistry()
	m := &Metrics{
		Registry: r,
		httpDuration: r.NewHistogramVec("dockfleet_http_request_duration_seconds",
			"Latency of administrative API requests, by method, route pattern and status code.", requestBuckets, "method", "route", "code"),
		grpcDuration: r.NewHistogramVec("dockfleet_grpc_request_duration_seconds",
			"Latency of agent gRPC calls, by method and status code.", requestBuckets, "method", "code"),

		schedulingDuration: r.NewHistogramVec("dockfleet_scheduling_duration_seconds",
			"Duration of scheduling passes: reading the cluster, placing waiting replicas and recording why others have no node.", passBuckets),
		schedulingPasses: r.NewCounterVec("dockfleet_scheduling_passes_total", "Scheduling passes, by result (ok or error).", "result"),
		schedulingErrors: r.NewCounterVec("dockfleet_scheduling_errors_total", "Scheduling passes that failed; nothing was saved by them."),
		schedulingDecisions: r.NewCounterVec("dockfleet_scheduling_decisions_total",
			"Scheduling decisions: replicas placed on a node, and replicas found to have no valid node (new or changed reasons).", "result"),

		reconcileDuration: r.NewHistogramVec("dockfleet_reconcile_duration_seconds",
			"Duration of reconciliation passes: comparing services with what agents report and stopping or removing what should go.", passBuckets),
		reconcilePasses: r.NewCounterVec("dockfleet_reconcile_passes_total", "Reconciliation passes, by result (ok or error).", "result"),
		reconcileErrors: r.NewCounterVec("dockfleet_reconcile_errors_total", "Reconciliation passes that failed; nothing was saved by them."),

		deploymentFailures: r.NewCounterVec("dockfleet_deployment_failures_total",
			"Rollouts this manager reported failed (no progress within the deadline), by service.", "service"),
	}
	// The series that always exist start at zero, so rate() has a first sample.
	for _, result := range []string{"ok", "error"} {
		m.schedulingPasses.Add(0, result)
		m.reconcilePasses.Add(0, result)
	}
	for _, result := range []string{"placed", "unschedulable"} {
		m.schedulingDecisions.Add(0, result)
	}
	r.AddCollector("build", func(context.Context) ([]Family, error) {
		info := NewFamily("dockfleet_build_info", "The version of this manager, as the value of the version label.", GaugeType)
		info.Add(1, "version", version)
		start := NewFamily("dockfleet_start_time_seconds", "When this manager started, in seconds since the Unix epoch.", GaugeType)
		start.Add(float64(now.Unix()))
		return []Family{*info, *start}, nil
	})
	return m
}

// ObserveHTTP records one administrative API request. route is the mux pattern
// (a bounded set), never the raw path, and an unusual method is folded into
// OTHER, so a client cannot make up label values.
func (m *Metrics) ObserveHTTP(method, route string, code int, d time.Duration) {
	m.httpDuration.Observe(d.Seconds(), normalizeMethod(method), route, strconv.Itoa(code))
}

// ObserveGRPC records one unary call from an agent. method is the full RPC name.
func (m *Metrics) ObserveGRPC(method, code string, d time.Duration) {
	m.grpcDuration.Observe(d.Seconds(), method, code)
}

// ObserveSchedulingPass records one pass of the scheduler: how long it took,
// how many replicas it placed, how many have a new or changed reason for having
// no node, and whether it failed.
func (m *Metrics) ObserveSchedulingPass(d time.Duration, placed, newReasons int, err error) {
	m.schedulingDuration.Observe(d.Seconds())
	if err != nil {
		m.schedulingPasses.Inc("error")
		m.schedulingErrors.Inc()
		return
	}
	m.schedulingPasses.Inc("ok")
	m.schedulingDecisions.Add(float64(placed), "placed")
	m.schedulingDecisions.Add(float64(newReasons), "unschedulable")
}

// ObserveReconcilePass records one pass of the reconciler.
func (m *Metrics) ObserveReconcilePass(d time.Duration, err error) {
	m.reconcileDuration.Observe(d.Seconds())
	if err != nil {
		m.reconcilePasses.Inc("error")
		m.reconcileErrors.Inc()
		return
	}
	m.reconcilePasses.Inc("ok")
}

// DeploymentFailed counts a rollout that was reported failed.
func (m *Metrics) DeploymentFailed(service string) { m.deploymentFailures.Inc(service) }

func normalizeMethod(method string) string {
	switch method {
	case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS":
		return method
	}
	return "OTHER"
}
