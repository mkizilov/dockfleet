package api

import (
	"context"
	"sort"

	"github.com/dockfleet/dockfleet/internal/store"
)

// A service's definition says what is wanted; its status says what the
// scheduler has decided about it. The scheduler's decisions are not stored
// apart from the workloads it creates and the explanations it records, so the
// status is read from those: nothing here is kept in step by hand.

// replicaStatus is one workload of the service.
type replicaStatus struct {
	// Revision is the revision the workload was created for; replicas of an
	// older revision are listed too, since they still hold their resources.
	Revision     int    `json:"revision"`
	Index        int    `json:"index"`
	WorkloadID   string `json:"workload_id"`
	WorkloadName string `json:"workload_name"`
	NodeID       string `json:"node_id"`
	DesiredState string `json:"desired_state"`

	// Generation counts the instructions given for the workload (its desired
	// state changing), and Acknowledged says that its node has received the
	// latest one.
	Generation   int64 `json:"generation"`
	Acknowledged bool  `json:"acknowledged"`

	// Phase is what the node's agent last reported, or "scheduled" if it has not yet.
	Phase string `json:"phase"`
	// Message is the agent's own words for the phase, and ContainerID the
	// container it reported; empty when the node has none.
	Message     string `json:"message,omitempty"`
	ContainerID string `json:"container_id,omitempty"`

	// Health is the result of the container's health check (empty without
	// one), RestartCount how often its agent restarted it, and ExitCode how it
	// last ended. NodeHealth is the state of the node the replica is on
	// (healthy, suspect or unavailable), which says why a replica that last
	// reported running may not be.
	Health       string `json:"health,omitempty"`
	RestartCount int    `json:"restart_count"`
	ExitCode     *int   `json:"exit_code,omitempty"`
	NodeHealth   string `json:"node_health,omitempty"`
}

// serviceStatus summarizes the scheduling of one service.
type serviceStatus struct {
	DesiredReplicas int `json:"desired_replicas"`

	// ScheduledReplicas counts the wanted replicas of the current revision that
	// have a workload meant to run; UnschedulableReplicas those that have none
	// because no node could take them.
	ScheduledReplicas     int `json:"scheduled_replicas"`
	UnschedulableReplicas int `json:"unschedulable_replicas"`

	// RunningReplicas counts the wanted replicas of the current revision whose
	// agent reports the container running.
	RunningReplicas int `json:"running_replicas"`

	// FailedReplicas counts the wanted replicas whose container failed and was
	// not restarted (yet); UnhealthyReplicas those that run but fail their
	// health check; LostReplicas those on a node that is unavailable, which the
	// manager is moving. They are not part of RunningReplicas.
	FailedReplicas    int `json:"failed_replicas"`
	UnhealthyReplicas int `json:"unhealthy_replicas"`
	LostReplicas      int `json:"lost_replicas"`

	// Replicas are the service's workloads, by revision and index.
	Replicas []replicaStatus `json:"replicas"`

	// Unschedulable says why each waiting replica has no node.
	Unschedulable []store.Unschedulable `json:"unschedulable"`
}

// buildStatus summarizes a service from its own workloads and its own
// unschedulable records. nodes maps a node ID to its health.
func buildStatus(svc store.Service, workloads []store.Workload, waiting []store.Unschedulable, nodes map[string]string) serviceStatus {
	st := serviceStatus{
		DesiredReplicas: svc.Replicas,
		Replicas:        make([]replicaStatus, 0, len(workloads)),
		Unschedulable:   append([]store.Unschedulable{}, waiting...),
	}
	for _, w := range workloads {
		r := replicaStatus{
			Revision: w.ServiceRevision, Index: w.ReplicaIndex, WorkloadID: w.ID, WorkloadName: w.Name,
			NodeID: w.NodeID, DesiredState: w.DesiredState, Generation: w.Generation, Phase: w.Phase(),
			NodeHealth: nodes[w.NodeID],
		}
		if w.Observed != nil {
			r.Acknowledged = w.Observed.AckedGeneration >= w.Generation
			r.Message, r.ContainerID = w.Observed.Message, w.Observed.ContainerID
			r.Health, r.RestartCount, r.ExitCode = w.Observed.Health, w.Observed.RestartCount, w.Observed.ExitCode
		}
		st.Replicas = append(st.Replicas, r)
		if w.ServiceRevision == svc.CurrentRevision && w.ReplicaIndex <= svc.Replicas && w.DesiredState == store.DesiredRunning {
			st.ScheduledReplicas++
			switch {
			case r.NodeHealth == store.NodeUnavailable:
				st.LostReplicas++
			case w.Phase() == store.WorkloadFailed:
				st.FailedReplicas++
			case w.Phase() == store.WorkloadRunning && r.Health == store.HealthUnhealthy:
				st.UnhealthyReplicas++
			case w.Phase() == store.WorkloadRunning:
				st.RunningReplicas++
			}
		}
	}
	sort.Slice(st.Replicas, func(i, j int) bool {
		a, b := st.Replicas[i], st.Replicas[j]
		if a.Revision != b.Revision {
			return a.Revision < b.Revision
		}
		return a.Index < b.Index
	})
	st.UnschedulableReplicas = len(st.Unschedulable)
	return st
}

// statusOf reads one service's status.
func (s *server) statusOf(ctx context.Context, svc store.Service) (serviceStatus, error) {
	workloads, err := s.store.ListWorkloadsForService(ctx, svc.ID)
	if err != nil {
		return serviceStatus{}, err
	}
	waiting, err := s.store.ListUnschedulable(ctx, svc.ID)
	if err != nil {
		return serviceStatus{}, err
	}
	nodes, err := s.nodeStates(ctx)
	if err != nil {
		return serviceStatus{}, err
	}
	return buildStatus(svc, workloads, waiting, nodes), nil
}

// nodeStates is every node's health now.
func (s *server) nodeStates(ctx context.Context) (map[string]string, error) {
	nodes, err := s.store.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	now := s.now()
	out := make(map[string]string, len(nodes))
	for _, n := range nodes {
		out[n.ID] = s.timing.Health(n, now)
	}
	return out, nil
}

// statusesOf reads the status of many services in two queries, not two per service.
func (s *server) statusesOf(ctx context.Context, services []store.Service) (map[string]serviceStatus, error) {
	workloads, err := s.store.ListWorkloads(ctx)
	if err != nil {
		return nil, err
	}
	waiting, err := s.store.ListUnschedulable(ctx, "")
	if err != nil {
		return nil, err
	}
	workloadsOf := map[string][]store.Workload{}
	for _, w := range workloads {
		if w.ServiceID != "" {
			workloadsOf[w.ServiceID] = append(workloadsOf[w.ServiceID], w)
		}
	}
	waitingOf := map[string][]store.Unschedulable{}
	for _, u := range waiting {
		waitingOf[u.ServiceID] = append(waitingOf[u.ServiceID], u)
	}
	nodes, err := s.nodeStates(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]serviceStatus, len(services))
	for _, svc := range services {
		out[svc.ID] = buildStatus(svc, workloadsOf[svc.ID], waitingOf[svc.ID], nodes)
	}
	return out, nil
}
