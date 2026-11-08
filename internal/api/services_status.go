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

	// Phase is what the node's agent last reported, or "scheduled" if it has not yet.
	Phase string `json:"phase"`
}

// serviceStatus summarizes the scheduling of one service.
type serviceStatus struct {
	DesiredReplicas int `json:"desired_replicas"`

	// ScheduledReplicas counts the wanted replicas of the current revision that
	// have a workload meant to run; UnschedulableReplicas those that have none
	// because no node could take them.
	ScheduledReplicas     int `json:"scheduled_replicas"`
	UnschedulableReplicas int `json:"unschedulable_replicas"`

	// Replicas are the service's workloads, by revision and index.
	Replicas []replicaStatus `json:"replicas"`

	// Unschedulable says why each waiting replica has no node.
	Unschedulable []store.Unschedulable `json:"unschedulable"`
}

// buildStatus summarizes a service from its own workloads and its own
// unschedulable records.
func buildStatus(svc store.Service, workloads []store.Workload, waiting []store.Unschedulable) serviceStatus {
	st := serviceStatus{
		DesiredReplicas: svc.Replicas,
		Replicas:        make([]replicaStatus, 0, len(workloads)),
		Unschedulable:   append([]store.Unschedulable{}, waiting...),
	}
	for _, w := range workloads {
		st.Replicas = append(st.Replicas, replicaStatus{
			Revision: w.ServiceRevision, Index: w.ReplicaIndex, WorkloadID: w.ID, WorkloadName: w.Name,
			NodeID: w.NodeID, DesiredState: w.DesiredState, Phase: w.Phase(),
		})
		if w.ServiceRevision == svc.CurrentRevision && w.ReplicaIndex <= svc.Replicas && w.DesiredState == store.DesiredRunning {
			st.ScheduledReplicas++
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
	return buildStatus(svc, workloads, waiting), nil
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
	out := make(map[string]serviceStatus, len(services))
	for _, svc := range services {
		out[svc.ID] = buildStatus(svc, workloadsOf[svc.ID], waitingOf[svc.ID])
	}
	return out, nil
}
