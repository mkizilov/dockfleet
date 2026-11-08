package api

import (
	"bytes"
	"context"
	"net/http"
	"time"

	"github.com/dockfleet/dockfleet/internal/metrics"
	"github.com/dockfleet/dockfleet/internal/scheduler"
	"github.com/dockfleet/dockfleet/internal/store"
)

// metricsTimeout bounds one scrape, whatever the store is doing.
const metricsTimeout = 10 * time.Second

// serveMetrics answers GET /metrics in the Prometheus text format. It needs the
// admin token like every other endpoint: node names, service names and their
// state are not for anyone who can reach the port. The response is built before
// anything is sent, so a scrape that fails is an error and not a short answer.
func (s *server) serveMetrics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), metricsTimeout)
	defer cancel()
	var buf bytes.Buffer
	if err := s.metrics.Registry.Expose(ctx, &buf); err != nil {
		s.internalError(w, r, err)
		return
	}
	h := w.Header()
	h.Set("Content-Type", metrics.ContentType)
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	_, _ = w.Write(buf.Bytes())
}

// replicaStates are the states of a service's replicas that are exported, in the
// order they are written. They are counted the way the service's status counts
// them (services_status.go), so the metrics and the API cannot disagree.
var replicaStates = []string{"running", "starting", "failed", "unhealthy", "lost", "unschedulable", "outdated", "stopping"}

func replicaCounts(st serviceStatus) map[string]int {
	return map[string]int{
		"running": st.Scaling.Ready, "starting": st.Scaling.Starting, "failed": st.FailedReplicas, "unhealthy": st.UnhealthyReplicas,
		"lost": st.LostReplicas, "unschedulable": st.UnschedulableReplicas, "outdated": st.Scaling.Outdated, "stopping": st.Scaling.Stopping,
	}
}

func bool01(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// collectFleet reads the nodes, workloads and services from the store and reports
// them. It runs on every scrape and uses what the API uses (the node view, the
// service status), so what is exported is what GET /v1/nodes and /v1/services
// say at that moment, not a copy kept up to date by hand.
func (s *server) collectFleet(ctx context.Context) ([]metrics.Family, error) {
	nodes, err := s.store.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	workloads, err := s.store.ListWorkloads(ctx)
	if err != nil {
		return nil, err
	}
	defs, err := s.store.ListDefinitions(ctx, false)
	if err != nil {
		return nil, err
	}
	statuses, err := s.statusesOf(ctx, defs)
	if err != nil {
		return nil, err
	}

	byHealth := metrics.NewFamily("dockfleet_nodes",
		"Registered nodes, by health: healthy, suspect (silent for a while; keeps its replicas) or unavailable (its replicas are started elsewhere).", metrics.GaugeType)
	healthy := metrics.NewFamily("dockfleet_node_healthy", "1 if the node is healthy, 0 if it is suspect or unavailable.", metrics.GaugeType)
	cpu := metrics.NewFamily("dockfleet_node_cpu_millicores",
		"CPU of a node in millicores, by kind: capacity (declared by the agent), allocatable (capacity times the overcommit factor) and allocated (the limits of the workloads meant to run on it).", metrics.GaugeType)
	mem := metrics.NewFamily("dockfleet_node_memory_bytes",
		"Memory of a node in bytes, by kind: capacity (declared by the agent), allocatable (capacity times the overcommit factor) and allocated (the limits of the workloads meant to run on it).", metrics.GaugeType)
	ratio := metrics.NewFamily("dockfleet_node_allocation_ratio",
		"Allocated divided by allocatable, by resource. It is what is promised to workloads (their declared limits), not measured usage.", metrics.GaugeType)
	onNode := metrics.NewFamily("dockfleet_node_workloads", "Workloads meant to run on the node.", metrics.GaugeType)

	counts := map[string]int{}
	used := scheduler.UsedByNode(workloads)
	timing := s.healthTiming(ctx)
	for _, n := range nodes {
		v := s.view(n, used[n.ID], timing)
		counts[v.Health]++
		healthy.Add(bool01(v.Health == store.NodeHealthy), "node", n.ID)
		cpu.Add(float64(n.CPUMillis), "node", n.ID, "kind", "capacity")
		cpu.Add(float64(v.Allocatable.CPUMillis), "node", n.ID, "kind", "allocatable")
		cpu.Add(float64(v.Allocated.CPUMillis), "node", n.ID, "kind", "allocated")
		mem.Add(float64(n.MemoryBytes), "node", n.ID, "kind", "capacity")
		mem.Add(float64(v.Allocatable.MemoryBytes), "node", n.ID, "kind", "allocatable")
		mem.Add(float64(v.Allocated.MemoryBytes), "node", n.ID, "kind", "allocated")
		if v.Allocatable.CPUMillis > 0 {
			ratio.Add(float64(v.Allocated.CPUMillis)/float64(v.Allocatable.CPUMillis), "node", n.ID, "resource", "cpu")
		}
		if v.Allocatable.MemoryBytes > 0 {
			ratio.Add(float64(v.Allocated.MemoryBytes)/float64(v.Allocatable.MemoryBytes), "node", n.ID, "resource", "memory")
		}
		onNode.Add(float64(v.Allocated.Workloads), "node", n.ID)
	}
	for _, h := range []string{store.NodeHealthy, store.NodeSuspect, store.NodeUnavailable} {
		byHealth.Add(float64(counts[h]), "health", h)
	}

	services := metrics.NewFamily("dockfleet_services", "Services that exist (deleted ones are not counted).", metrics.GaugeType)
	desired := metrics.NewFamily("dockfleet_service_replicas_desired", "Replicas a service is meant to have.", metrics.GaugeType)
	replicas := metrics.NewFamily("dockfleet_service_replicas",
		"Replicas of a service by state: running (ready), starting, failed, unhealthy, lost (on an unavailable node), unschedulable (no node can take it), outdated (of an older revision, still running) and stopping.", metrics.GaugeType)
	rollouts := metrics.NewFamily("dockfleet_rollouts", "Services by the state of the rollout of their current revision.", metrics.GaugeType)
	rolloutFailed := metrics.NewFamily("dockfleet_service_rollout_failed",
		"1 if the rollout of the service's current revision made no progress within its deadline and was reported failed.", metrics.GaugeType)

	services.Add(float64(len(defs)))
	rolloutCounts := map[string]int{store.RolloutProgressing: 0, rolloutPaused: 0, store.RolloutComplete: 0, store.RolloutFailed: 0}
	for _, d := range defs {
		name, st := d.Service.Name, statuses[d.Service.ID]
		desired.Add(float64(d.Service.Replicas), "service", name)
		counted := replicaCounts(st)
		for _, state := range replicaStates {
			replicas.Add(float64(counted[state]), "service", name, "state", state)
		}
		if _, known := rolloutCounts[st.Rollout.State]; known {
			rolloutCounts[st.Rollout.State]++
		}
		rolloutFailed.Add(bool01(st.Rollout.State == store.RolloutFailed), "service", name)
	}
	// (Family samples are sorted when the registry gathers them, so the map's order does not matter.)
	for state, n := range rolloutCounts {
		rollouts.Add(float64(n), "state", state)
	}

	return []metrics.Family{*byHealth, *healthy, *cpu, *mem, *ratio, *onNode, *services, *desired, *replicas, *rollouts, *rolloutFailed}, nil
}
