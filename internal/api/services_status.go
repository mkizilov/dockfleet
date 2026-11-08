package api

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/dockfleet/dockfleet/internal/rollout"
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

// Scaling states: where a service is between the replica count it has and the
// one it was asked for.
const (
	scalingStable   = "stable"       // as many replicas are ready as are wanted, and nothing is on its way out
	scalingUp       = "scaling_up"   // replicas are being created or are starting
	scalingDown     = "scaling_down" // replicas are being stopped, or are about to be
	scalingUpdating = "updating"     // replicas of an older revision still run
	scalingBlocked  = "blocked"      // some replicas cannot be placed, and progress waits for the cluster
	scalingDegraded = "degraded"     // fewer are ready than wanted, and nothing is on its way to fix it
)

// scalingStatus is the progress of a service towards the replica count it
// asked for. The count is the one in the definition; this is how close the
// cluster is to it.
type scalingStatus struct {
	State string `json:"state"`

	// Desired is the wanted count. Ready is how many replicas of the current
	// revision run (or finished successfully) on a node that answers.
	Desired int `json:"desired"`
	Ready   int `json:"ready"`

	// Starting are replicas that have a node but do not run yet; Waiting those
	// that have none because no node can take them. Surplus are the replicas
	// above the wanted count that are not yet told to stop, and Stopping those
	// that are, whose containers are not confirmed gone. Outdated are replicas
	// of an older revision that still run.
	Starting int `json:"starting"`
	Waiting  int `json:"waiting"`
	Surplus  int `json:"surplus"`
	Stopping int `json:"stopping"`
	Outdated int `json:"outdated"`

	Message string `json:"message"`
}

// serviceStatus summarizes the scheduling of one service.
type serviceStatus struct {
	DesiredReplicas int `json:"desired_replicas"`

	// ScheduledReplicas counts the replicas of the current revision that have a
	// workload meant to run, up to the wanted count; UnschedulableReplicas those
	// that have none because no node could take them.
	ScheduledReplicas     int `json:"scheduled_replicas"`
	UnschedulableReplicas int `json:"unschedulable_replicas"`

	// RunningReplicas counts the replicas of the current revision whose agent
	// reports the container running. While a service is scaled down it can be
	// more than the wanted count, until the surplus is stopped.
	RunningReplicas int `json:"running_replicas"`

	// FailedReplicas counts the replicas whose container failed and was not
	// restarted (yet); UnhealthyReplicas those that run but fail their health
	// check; LostReplicas those on a node that is unavailable, which the manager
	// is moving. They are not part of RunningReplicas.
	FailedReplicas    int `json:"failed_replicas"`
	UnhealthyReplicas int `json:"unhealthy_replicas"`
	LostReplicas      int `json:"lost_replicas"`

	// Scaling is the progress towards the wanted count, and Rollout the
	// progress of the current revision's deployment.
	Scaling scalingStatus `json:"scaling"`
	Rollout rolloutStatus `json:"rollout"`

	// Replicas are the service's workloads, by revision and index.
	Replicas []replicaStatus `json:"replicas"`

	// Unschedulable says why each waiting replica has no node.
	Unschedulable []store.Unschedulable `json:"unschedulable"`
}

// Rollout states a status adds to the stored ones.
const (
	// rolloutPaused is a rollout that is progressing, held by the service's pause.
	rolloutPaused = "paused"
	// rolloutDeleted is the state of a deleted service's rollout.
	rolloutDeleted = "deleted"
)

// rolloutStatus is the progress of the deployment of the service's current
// revision: how many replicas of its spec exist and are ready, how many of an
// older spec are left, and, if it is waiting, what for.
type rolloutStatus struct {
	Revision int `json:"revision"`
	// State is progressing, paused, complete or failed, or deleted for a deleted
	// service. A failed rollout is still being carried out.
	State   string `json:"state"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message"`
	Paused  bool   `json:"paused"`

	Desired  int `json:"desired"`
	Updated  int `json:"updated"`
	Ready    int `json:"ready"`
	Outdated int `json:"outdated"`
	// Surge is how many replicas above the wanted count are meant to run now,
	// and Unavailable how many of the wanted count are not ready.
	Surge       int `json:"surge"`
	Unavailable int `json:"unavailable"`

	StartedAt  *time.Time `json:"started_at,omitempty"`
	ProgressAt *time.Time `json:"progress_at,omitempty"`
	// DeadlineAt is when a rollout that makes no more progress is reported failed.
	DeadlineAt *time.Time `json:"deadline_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// rolloutStatusOf describes the deployment of the service's current revision
// from the facts and from the reconciler's record of it (nil before it has
// made one). The words are the live ones, except for a failed rollout, whose
// own words say why it was given up on.
func rolloutStatusOf(def store.Definition, workloads []store.Workload, waiting []store.Unschedulable, nodes map[string]string, records []store.Rollout) rolloutStatus {
	svc := def.Service
	facts := rollout.Assess(def, workloads, waiting, nodes)
	out := rolloutStatus{
		Revision: svc.CurrentRevision, State: store.RolloutProgressing, Paused: svc.Paused, Desired: svc.Replicas,
		Updated: facts.Updated, Ready: facts.UpdatedReady, Outdated: facts.Old,
		Surge:       max(0, facts.Updated+facts.Old-svc.Replicas),
		Unavailable: max(0, svc.Replicas-facts.Available()),
		Message:     facts.Describe(svc.Rollout, svc.Paused),
	}
	if svc.DeletedAt != nil {
		out.State, out.Message = rolloutDeleted, "the service is deleted"
		return out
	}
	var rec *store.Rollout
	for i := range records {
		if records[i].Revision == svc.CurrentRevision {
			rec = &records[i]
		}
	}
	if rec == nil {
		// The reconciler has not looked at this revision yet.
		if facts.Complete() {
			out.State = store.RolloutComplete
		}
		return out
	}
	out.State, out.Reason = rec.State, rec.Reason
	started, progressed := rec.StartedAt, rec.ProgressAt
	out.StartedAt, out.ProgressAt, out.FinishedAt = &started, &progressed, rec.FinishedAt
	switch {
	case rec.State == store.RolloutFailed:
		out.Message = rec.Message
	case rec.State == store.RolloutProgressing && svc.Paused:
		out.State = rolloutPaused
	case rec.State == store.RolloutProgressing:
		deadline := progressed.Add(time.Duration(svc.Rollout.Effective().ProgressDeadlineSeconds) * time.Second)
		out.DeadlineAt = &deadline
	case rec.State == store.RolloutComplete:
		out.Message = rec.Message
	}
	return out
}

// buildStatus summarizes a service from its own workloads and its own
// unschedulable records. nodes maps a node ID to its health. records are the
// service's rollout records. def.SameSpec says which revisions run what the
// service asks for now, so that replicas of a revision a rollback returned to
// are not counted as old.
func buildStatus(def store.Definition, workloads []store.Workload, waiting []store.Unschedulable, nodes map[string]string, records []store.Rollout) serviceStatus {
	svc := def.Service
	st := serviceStatus{
		DesiredReplicas: svc.Replicas,
		Replicas:        make([]replicaStatus, 0, len(workloads)),
		Unschedulable:   append([]store.Unschedulable{}, waiting...),
	}
	// Counted by what each workload is for: the current revision's that are
	// meant to run, those of older revisions that still run, and the ones being
	// stopped. A workload on a node that is unavailable counts for nothing but
	// as lost: its node cannot confirm anything, and the manager has moved it.
	var active, starting, finished, stopping, outdated int
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

		lost := r.NodeHealth == store.NodeUnavailable
		switch {
		case w.DesiredState == store.DesiredStopped:
			if !lost && !w.ContainerGone() {
				stopping++
			}
		case !def.Serves(w.ServiceRevision):
			if !lost {
				outdated++
			}
		default:
			active++
			switch {
			case lost:
				st.LostReplicas++
			case w.Phase() == store.WorkloadFailed:
				st.FailedReplicas++
			case w.Phase() == store.WorkloadRunning && r.Health == store.HealthUnhealthy:
				st.UnhealthyReplicas++
			case w.Phase() == store.WorkloadRunning:
				st.RunningReplicas++
			case w.Phase() == store.WorkloadSucceeded:
				finished++
			default:
				starting++
			}
		}
	}
	st.ScheduledReplicas = min(active, svc.Replicas)
	sort.Slice(st.Replicas, func(i, j int) bool {
		a, b := st.Replicas[i], st.Replicas[j]
		if a.Revision != b.Revision {
			return a.Revision < b.Revision
		}
		return a.Index < b.Index
	})
	st.UnschedulableReplicas = len(st.Unschedulable)
	st.Scaling = scalingOf(svc, st, active, starting, finished, stopping, outdated)
	st.Rollout = rolloutStatusOf(def, workloads, waiting, nodes, records)
	return st
}

// scalingOf says how far a service is from the replica count it was asked for.
func scalingOf(svc store.Service, st serviceStatus, active, starting, finished, stopping, outdated int) scalingStatus {
	sc := scalingStatus{
		Desired: svc.Replicas, Ready: st.RunningReplicas + finished, Starting: starting, Waiting: st.UnschedulableReplicas,
		Surplus: max(0, active-svc.Replicas), Stopping: stopping, Outdated: outdated,
	}
	// Nothing is wanted of a deleted service, so what it still runs is all on its way out.
	if svc.DeletedAt != nil {
		sc.Surplus, sc.Outdated = sc.Surplus+sc.Outdated, 0
	}
	// Replicas the scheduler has not yet created a workload for.
	creating := max(0, svc.Replicas-active-sc.Waiting)
	ready := fmt.Sprintf("%d ready, %d wanted", sc.Ready, sc.Desired)

	switch {
	case sc.Waiting > 0:
		sc.State = scalingBlocked
		sc.Message = fmt.Sprintf("%s; %d cannot be placed: %s", ready, sc.Waiting, st.Unschedulable[0].Message)
	case sc.Outdated > 0:
		sc.State = scalingUpdating
		sc.Message = fmt.Sprintf("%s; revision %d is rolling out, %d of an older revision still run", ready, svc.CurrentRevision, sc.Outdated)
	case sc.Surplus > 0 || sc.Stopping > 0:
		sc.State = scalingDown
		sc.Message = fmt.Sprintf("%s; %d to stop, %d stopping", ready, sc.Surplus, sc.Stopping)
	case creating > 0 || sc.Starting > 0:
		sc.State = scalingUp
		sc.Message = fmt.Sprintf("%s; %d to create, %d starting", ready, creating, sc.Starting)
	case sc.Ready < sc.Desired:
		sc.State = scalingDegraded
		sc.Message = fmt.Sprintf("%s; %d failed, %d unhealthy, %d on unavailable nodes", ready, st.FailedReplicas, st.UnhealthyReplicas, st.LostReplicas)
	default:
		sc.State = scalingStable
		sc.Message = ready
		if sc.Desired == 0 {
			sc.Message = "none wanted, none left"
		}
	}
	return sc
}

// definitionOf is a service with the revisions that run its current spec.
func (s *server) definitionOf(ctx context.Context, svc store.Service) (store.Definition, error) {
	revs, err := s.store.ListRevisions(ctx, svc.ID)
	if err != nil {
		return store.Definition{}, err
	}
	return store.Definition{Service: svc, SameSpec: store.SameSpecRevisions(revs, svc.CurrentRevision)}, nil
}

// statusOf reads one service's status.
func (s *server) statusOf(ctx context.Context, svc store.Service) (serviceStatus, error) {
	def, err := s.definitionOf(ctx, svc)
	if err != nil {
		return serviceStatus{}, err
	}
	workloads, err := s.store.ListWorkloadsForService(ctx, svc.ID)
	if err != nil {
		return serviceStatus{}, err
	}
	waiting, err := s.store.ListUnschedulable(ctx, svc.ID)
	if err != nil {
		return serviceStatus{}, err
	}
	records, err := s.store.ListRollouts(ctx, svc.ID)
	if err != nil {
		return serviceStatus{}, err
	}
	nodes, err := s.nodeStates(ctx)
	if err != nil {
		return serviceStatus{}, err
	}
	return buildStatus(def, workloads, waiting, nodes, records), nil
}

// nodeStates is every node's health now.
func (s *server) nodeStates(ctx context.Context) (map[string]string, error) {
	nodes, err := s.store.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	now, timing := s.now(), s.healthTiming(ctx)
	out := make(map[string]string, len(nodes))
	for _, n := range nodes {
		out[n.ID] = timing.Health(n, now)
	}
	return out, nil
}

// statusesOf reads the status of many services in a few queries, not a few per service.
func (s *server) statusesOf(ctx context.Context, defs []store.Definition) (map[string]serviceStatus, error) {
	workloads, err := s.store.ListWorkloads(ctx)
	if err != nil {
		return nil, err
	}
	waiting, err := s.store.ListUnschedulable(ctx, "")
	if err != nil {
		return nil, err
	}
	records, err := s.store.ListRollouts(ctx, "")
	if err != nil {
		return nil, err
	}
	recordsOf := map[string][]store.Rollout{}
	for _, r := range records {
		recordsOf[r.ServiceID] = append(recordsOf[r.ServiceID], r)
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
	out := make(map[string]serviceStatus, len(defs))
	for _, d := range defs {
		out[d.Service.ID] = buildStatus(d, workloadsOf[d.Service.ID], waitingOf[d.Service.ID], nodes, recordsOf[d.Service.ID])
	}
	return out, nil
}
