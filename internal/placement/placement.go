// Package placement carries out scheduling decisions. It reads the cluster from
// the store, asks the scheduler where each waiting replica should go, and
// records the answer: an assignment (a workload, which is desired state) for a
// replica that has a node, and an explanation for one that has none. Running
// the containers is the agents' job and the Docker adapter's; nothing here
// touches Docker.
//
// A pass is idempotent and safe to run on several managers at once: it holds
// the cluster-wide placement lock for the whole transaction, so passes queue up
// and each sees the previous one's work.
//
// The unit of work is a replica slot: replica i of a service's current
// revision. A service that wants N replicas and has fewer workloads meant to run
// lacks some; each lacking replica goes into the lowest slot that is free, and
// workload that is meant to run per slot.
//
// What a pass does not do is remove anything: scaling a service down, retiring
// the replicas of an older revision and stopping the workloads of a node that
// was given up on belong to the reconciler (internal/reconcile). A slot whose
// only workload was stopped on such a lost node is free again, so that the
// replica is started on another node at once instead of when the lost node
// returns.
package placement

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"time"

	"github.com/dockfleet/dockfleet/internal/election"
	"github.com/dockfleet/dockfleet/internal/rollout"
	"github.com/dockfleet/dockfleet/internal/scheduler"
	"github.com/dockfleet/dockfleet/internal/store"
)

// Actor is who the audit trail names for what the scheduler does.
const Actor = "scheduler"

const (
	// RevokedEnrollment is why a node whose enrollment was revoked gets no new replicas.
	RevokedEnrollment = "the node's enrollment has been revoked"

	// maxPassDuration bounds one pass in the loop, whatever the interval.
	maxPassDuration = 30 * time.Second

	// maxNameService keeps generated workload names within 63 characters.
	maxNameService = 40
)

// Reader is the part of the store a snapshot of the cluster is read from.
type Reader interface {
	ListNodes(ctx context.Context) ([]store.Node, error)
	ListEnrollments(ctx context.Context) ([]store.Enrollment, error)
	ListWorkloads(ctx context.Context) ([]store.Workload, error)
}

// Cluster is what the scheduler needs to know about the cluster at one moment.
type Cluster struct {
	Nodes       []scheduler.Node
	Assignments []scheduler.Assignment
	// Workloads is every workload, including the ones that no longer reserve anything.
	Workloads []store.Workload
	// Stored are the nodes as the store has them, for the checks that need more
	// than the scheduler's view (how long a node has been silent).
	Stored []store.Node
}

// ReadCluster takes a snapshot. A node is available when it has sent a
// heartbeat within ttl of now and its enrollment has not been revoked; the
// assignments are the workloads that are meant to be running.
func ReadCluster(ctx context.Context, r Reader, now time.Time, ttl time.Duration) (Cluster, error) {
	nodes, err := r.ListNodes(ctx)
	if err != nil {
		return Cluster{}, fmt.Errorf("list nodes: %w", err)
	}
	enrollments, err := r.ListEnrollments(ctx)
	if err != nil {
		return Cluster{}, fmt.Errorf("list enrollments: %w", err)
	}
	workloads, err := r.ListWorkloads(ctx)
	if err != nil {
		return Cluster{}, fmt.Errorf("list workloads: %w", err)
	}
	revoked := make(map[string]bool, len(enrollments))
	for _, e := range enrollments {
		revoked[e.NodeID] = !e.Active()
	}
	c := Cluster{Workloads: workloads, Stored: nodes}
	for _, n := range nodes {
		sn := scheduler.NodeFor(n, now, ttl)
		if sn.Unavailable == "" && revoked[n.ID] {
			sn.Unavailable = RevokedEnrollment
		}
		c.Nodes = append(c.Nodes, sn)
	}
	for _, w := range workloads {
		if a, ok := scheduler.AssignmentFrom(w); ok {
			c.Assignments = append(c.Assignments, a)
		}
	}
	return c, nil
}

// waiting is a replica that needs a node, with the service it belongs to and
// the ID its workload will have. The ID is chosen before the decision, so the
// workload has its final name while the scheduler is still explaining things.
type waiting struct {
	id      string
	def     store.Definition
	replica scheduler.Replica
}

// replicaKey identifies a waiting replica in one pass: services are told apart
// by their group (the service ID), replicas of a service by their index.
type replicaKey struct {
	group string
	index int
}

// findWaiting lists the replicas that need a node: for each live service, as
// many replicas of its current spec as it lacks, each in the lowest slot
// (replica index) that is free. A service that wants 3 and has 2 workloads
// meant to run lacks one, whichever slots the two hold: scaling down stops the
// replicas that are doing the least good, not the highest numbered, so the slots
// in use need not be 1 to N. A slot that has a workload is not free, whatever
// state the workload is in, except that a workload that was stopped on a lost
// node no longer holds its slot: that node cannot be asked to remove the
// container, and the replica must not wait for it.
//
// A replica runs the current spec if it was created for the current revision or
// for one with the same spec, which is what a rollback returns to: the replicas
// that still run it are the ones wanted, and the service lacks only the rest.
// While old replicas run, the service's rollout policy limits how many may be
// created now (internal/rollout): the extra replicas of a rolling update, and no
// more, so a rollout is gradual, and none at all while it is paused.
func findWaiting(defs []store.Definition, workloads []store.Workload, lost map[string]bool) []waiting {
	byService := make(map[string][]store.Workload, len(defs))
	for _, w := range workloads {
		if w.ServiceID != "" {
			byService[w.ServiceID] = append(byService[w.ServiceID], w)
		}
	}
	var out []waiting
	for _, d := range defs {
		svc, spec := d.Service, d.Revision.Spec
		taken := map[int]bool{}
		for _, w := range byService[svc.ID] {
			if d.Serves(w.ServiceRevision) && !(w.DesiredState == store.DesiredStopped && lost[w.NodeID]) {
				taken[w.ReplicaIndex] = true
			}
		}
		lacking := rollout.CreateBudget(svc.Rollout, svc.Paused, rollout.Count(d, byService[svc.ID], nil))
		for i := 1; lacking > 0; i++ {
			if taken[i] {
				continue
			}
			lacking--
			id := store.NewID()
			out = append(out, waiting{id: id, def: d, replica: scheduler.Replica{
				Service: svc.Name, Group: svc.ID, Index: i, Name: replicaName(svc.Name, svc.CurrentRevision, i, id),
				CPUMillis: spec.CPUMillis, MemoryBytes: spec.MemoryBytes,
				NodeSelector: spec.NodeSelector, Ports: spec.Ports,
			}})
		}
	}
	return out
}

// Config configures a Scheduler.
type Config struct {
	Store  store.Store
	Logger *slog.Logger
	// NodeTTL is how long after its last heartbeat a node still counts as available.
	NodeTTL time.Duration
	// Timing says when a silent node is given up on. The zero value never does.
	Timing scheduler.Timing
	// Overcommit lets nodes be promised more than they have. The zero value, no overcommit, is the safe one.
	Overcommit scheduler.Overcommit
	// Interval is the time between passes of Run; Run does nothing when it is zero.
	Interval time.Duration
	// InstanceID names this manager in the audit trail.
	InstanceID string
	// Observer, when set, is told how each pass of Run went.
	Observer Observer
	// Fence, when set, makes every pass act under the epoch of the leader's term:
	// it asks for the epoch before it starts (a pass of a manager that does not
	// lead fails with election.ErrNotLeader and changes nothing) and checks inside
	// its transaction that the epoch is still the current one, so a leader that
	// has been replaced cannot save a placement. Nil is a single manager.
	Fence election.Fence
	// Now defaults to time.Now; tests set it.
	Now func() time.Time
}

// Observer is told how the scheduler is doing; the manager turns it into metrics.
type Observer interface {
	// SchedulingPass is called after every pass of Run with how long it took and
	// why it failed, if it did. A pass cut short by shutdown is not reported.
	SchedulingPass(d time.Duration, report Report, err error)
}

// Scheduler places the replicas that are waiting for a node.
type Scheduler struct {
	cfg Config
}

// New returns a Scheduler.
func New(cfg Config) *Scheduler {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Scheduler{cfg: cfg}
}

// Report summarizes one pass.
type Report struct {
	// Placed is how many waiting replicas got a node.
	Placed int
	// Unschedulable is how many are still waiting, with a recorded reason.
	Unschedulable int
	// NewReasons is how many of those reasons are new or changed since the last pass.
	NewReasons int
}

// replicaName names a replica's workload. The suffix comes from the workload's
// own random ID, so the name cannot collide with one an administrator chose.
func replicaName(service string, revision, index int, id string) string {
	if len(service) > maxNameService {
		service = service[:maxNameService]
	}
	return fmt.Sprintf("%s-r%d-%d-%s", service, revision, index, id[:6])
}

func (s *Scheduler) event(at time.Time, action, resourceType, resourceID, message string, details map[string]string) store.Event {
	return store.Event{
		At: at, InstanceID: s.cfg.InstanceID, Actor: Actor, Action: action,
		ResourceType: resourceType, ResourceID: resourceID, Message: message, Details: details,
	}
}

// RunOnce makes one scheduling pass in a single transaction: it assigns every
// waiting replica that has a valid node, and records why each of the others
// has none. If anything fails nothing is saved and the next pass starts over.
func (s *Scheduler) RunOnce(ctx context.Context) (Report, error) {
	var report Report
	var epoch int64
	if s.cfg.Fence != nil {
		var err error
		if epoch, err = s.cfg.Fence.Epoch(); err != nil {
			return Report{}, err
		}
	}
	err := s.cfg.Store.InTx(ctx, func(tx store.Tx) error {
		report = Report{}
		if err := tx.LockPlacement(ctx); err != nil {
			return err
		}
		if s.cfg.Fence != nil {
			if err := tx.CheckEpoch(ctx, epoch); err != nil {
				return err
			}
		}
		now := s.cfg.Now().UTC()
		cluster, err := ReadCluster(ctx, tx, now, s.cfg.NodeTTL)
		if err != nil {
			return err
		}
		defs, err := tx.ListDefinitions(ctx, false)
		if err != nil {
			return fmt.Errorf("list services: %w", err)
		}

		pending := findWaiting(defs, cluster.Workloads, s.cfg.Timing.Lost(cluster.Stored, now))
		byReplica := make(map[replicaKey]waiting, len(pending))
		replicas := make([]scheduler.Replica, 0, len(pending))
		for _, w := range pending {
			byReplica[replicaKey{w.replica.Group, w.replica.Index}] = w
			replicas = append(replicas, w.replica)
		}
		decisions := scheduler.Schedule(scheduler.Input{
			Nodes: cluster.Nodes, Assignments: cluster.Assignments, Replicas: replicas, Overcommit: s.cfg.Overcommit,
		})

		var unplaced []store.Unschedulable
		for _, d := range decisions {
			w := byReplica[replicaKey{d.Replica.Group, d.Replica.Index}]
			svc, spec := w.def.Service, w.def.Revision.Spec
			if d.Placed() {
				created, err := tx.CreateWorkload(ctx, store.Workload{
					ID: w.id, Name: d.Replica.Name,
					Image: spec.Image, CPUMillis: spec.CPUMillis, MemoryBytes: spec.MemoryBytes,
					NodeID: d.NodeID, DesiredState: store.DesiredRunning,
					ServiceID: svc.ID, ServiceRevision: svc.CurrentRevision, ReplicaIndex: d.Replica.Index,
					Ports: spec.Ports, Command: spec.Command, Env: spec.Env, RestartPolicy: spec.RestartPolicy, HealthCheck: spec.HealthCheck, CreatedAt: now,
				})
				if err != nil {
					return fmt.Errorf("assign %s: %w", d.Replica.Label(), err)
				}
				if _, err := tx.AppendEvent(ctx, s.event(now, "replica.scheduled", "workload", created.ID,
					fmt.Sprintf("%s was assigned to node %s", d.Replica.Label(), d.NodeID),
					map[string]string{
						"service": svc.Name, "service_id": svc.ID, "revision": strconv.Itoa(svc.CurrentRevision),
						"replica": strconv.Itoa(d.Replica.Index), "node_id": d.NodeID,
					})); err != nil {
					return err
				}
				report.Placed++
				continue
			}
			unplaced = append(unplaced, toRecord(svc, d))
		}

		changed, err := tx.ReplaceUnschedulable(ctx, unplaced, now)
		if err != nil {
			return fmt.Errorf("record unschedulable replicas: %w", err)
		}
		for _, u := range changed {
			if _, err := tx.AppendEvent(ctx, s.event(now, "replica.unschedulable", "service", u.ServiceID, u.Message,
				map[string]string{"revision": strconv.Itoa(u.Revision), "replica": strconv.Itoa(u.ReplicaIndex), "code": u.Code})); err != nil {
				return err
			}
		}
		report.Unschedulable, report.NewReasons = len(unplaced), len(changed)
		return nil
	})
	if errors.Is(err, store.ErrStaleLeader) {
		s.cfg.Fence.Fenced(epoch)
	}
	if err != nil {
		return Report{}, err
	}
	return report, nil
}

func toRecord(svc store.Service, d scheduler.Decision) store.Unschedulable {
	u := store.Unschedulable{
		ServiceID: svc.ID, Revision: svc.CurrentRevision, ReplicaIndex: d.Replica.Index,
		Code: d.Unschedulable.Code, Message: d.Unschedulable.Message,
		Nodes: make([]store.NodeReason, 0, len(d.Unschedulable.Nodes)),
	}
	for _, nr := range d.Unschedulable.Nodes {
		u.Nodes = append(u.Nodes, store.NodeReason{NodeID: nr.NodeID, Code: nr.Code, Message: nr.Message})
	}
	return u
}

// leadershipLost is whether a pass was refused because this manager is not (or
// no longer) the leader.
func leadershipLost(err error) bool {
	return errors.Is(err, election.ErrNotLeader) || errors.Is(err, store.ErrStaleLeader)
}

// Run makes a pass at once and then every Interval until ctx ends. A pass that
// fails is logged and tried again at the next tick. With a zero Interval it
// returns immediately.
func (s *Scheduler) Run(ctx context.Context) {
	if s.cfg.Interval <= 0 {
		return
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		s.pass(ctx)
		timer.Reset(s.cfg.Interval)
	}
}

func (s *Scheduler) pass(ctx context.Context) {
	passCtx, cancel := context.WithTimeout(ctx, max(s.cfg.Interval, maxPassDuration))
	defer cancel()
	began := time.Now()
	report, err := s.RunOnce(passCtx)
	if leadershipLost(err) {
		// Not a failure of scheduling: another manager leads, or this one stopped
		// leading while the pass ran, and nothing was saved.
		s.cfg.Logger.Info("scheduling pass skipped: this manager does not lead", "reason", err.Error())
		return
	}
	if s.cfg.Observer != nil && (err == nil || ctx.Err() == nil) {
		s.cfg.Observer.SchedulingPass(time.Since(began), report, err)
	}
	switch {
	case err != nil && ctx.Err() != nil:
		// Shutting down; nothing was saved.
	case err != nil:
		// Most likely the store: a node's silence while it could not be used is not held against it.
		s.cfg.Timing.StoreFailed(s.cfg.Now())
		s.cfg.Logger.Error("scheduling pass failed", "error", err)
	case report.Placed > 0 || report.NewReasons > 0:
		s.cfg.Logger.Info("scheduling pass", "placed", report.Placed, "unschedulable", report.Unschedulable, "new_reasons", report.NewReasons)
	default:
		s.cfg.Logger.Debug("scheduling pass found nothing to do", "unschedulable", report.Unschedulable)
	}
}
