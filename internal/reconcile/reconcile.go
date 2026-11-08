package reconcile

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

// Actor is who the audit trail names for what the reconciler does.
const Actor = "reconciler"

// maxPassDuration bounds one pass in the loop, whatever the interval.
const maxPassDuration = 30 * time.Second

// Config configures a Reconciler.
type Config struct {
	Store  store.Store
	Logger *slog.Logger
	// Interval is the time between passes of Run; Run does nothing when it is zero.
	Interval time.Duration
	// InstanceID names this manager in the audit trail.
	InstanceID string
	// Timing says when a silent node is suspect and when it is given up on and
	// its replicas are started elsewhere. The zero value never gives up on one.
	Timing scheduler.Timing
	// Observer, when set, is told how the loop is doing.
	Observer Observer
	// Fence, when set, makes every pass act under the epoch of the leader's term:
	// the pass asks for the epoch before it starts (a manager that does not lead
	// fails it with election.ErrNotLeader and nothing changes) and checks inside
	// its transaction that the epoch is still the current one, so a leader that
	// has been replaced cannot stop, delete or record anything. Nil is a single
	// manager.
	Fence election.Fence
	// Now defaults to time.Now; tests set it.
	Now func() time.Time
}

// Observer is told how the reconciler is doing; the manager turns it into metrics.
type Observer interface {
	// ReconcilePass is called after every pass of Run with how long it took and
	// why it failed, if it did. A pass cut short by shutdown is not reported.
	ReconcilePass(d time.Duration, report Report, err error)
	// RolloutFailed is called for each rollout a pass reported failed, once the
	// pass is saved (a pass that is rolled back reports none).
	RolloutFailed(service string)
}

// Reconciler applies Plan to the store.
type Reconciler struct {
	cfg Config
}

// New returns a Reconciler.
func New(cfg Config) *Reconciler {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Reconciler{cfg: cfg}
}

// Report summarizes one pass.
type Report struct {
	// Stopped is how many workloads were told to stop.
	Stopped int
	// Deleted is how many stopped workloads were removed because their node
	// confirmed that the container is gone.
	Deleted int
	// NodeChanges is how many nodes changed health state.
	NodeChanges int
	// Rollouts is how many rollout records were written: a rollout began,
	// made progress, completed, failed or was overtaken.
	Rollouts int
}

// nodeEvent names a node's new health state in the audit trail.
func (r *Reconciler) nodeEvent(at time.Time, c NodeChange, reason string) store.Event {
	action := "node." + c.To
	message := fmt.Sprintf("node %s is %s", c.Node.ID, c.To)
	if c.To == store.NodeHealthy {
		action, message = "node.recovered", fmt.Sprintf("node %s is healthy again (it was %s)", c.Node.ID, c.From)
	} else if reason != "" {
		message += ": " + reason
	}
	details := map[string]string{"from": c.From, "to": c.To}
	if !c.Node.LastHeartbeat.IsZero() && c.Node.LastHeartbeat.Year() > 1 {
		details["last_heartbeat"] = c.Node.LastHeartbeat.UTC().Format(time.RFC3339)
	}
	return store.Event{
		At: at, InstanceID: r.cfg.InstanceID, Actor: Actor, Action: action,
		ResourceType: "node", ResourceID: c.Node.ID, Message: message, Details: details,
	}
}

func (r *Reconciler) event(at time.Time, action string, a Action, message string) store.Event {
	w := a.Workload
	e := store.Event{
		At: at, InstanceID: r.cfg.InstanceID, Actor: Actor, Action: action,
		ResourceType: "workload", ResourceID: w.ID, Message: message,
		Details: map[string]string{
			"service_id": w.ServiceID, "revision": strconv.Itoa(w.ServiceRevision), "replica": strconv.Itoa(w.ReplicaIndex),
			"node_id": w.NodeID, "reason": a.Reason,
		},
	}
	if a.Why != "" {
		e.Details["why"] = a.Why
	}
	return e
}

func (r *Reconciler) rolloutEvent(at time.Time, svc store.Service, rec store.Rollout, action string) store.Event {
	details := map[string]string{"revision": strconv.Itoa(rec.Revision), "state": rec.State}
	if rec.Reason != "" {
		details["reason"] = rec.Reason
	}
	message := fmt.Sprintf("revision %d of %s: %s", rec.Revision, svc.Name, rec.State)
	if rec.Message != "" {
		message += ": " + rec.Message
	}
	return store.Event{
		At: at, InstanceID: r.cfg.InstanceID, Actor: Actor, Action: action,
		ResourceType: "service", ResourceID: svc.ID, Message: message, Details: details,
	}
}

// trackRollouts keeps the record of how far each service's rollout has come:
// it starts one for a revision that has none, notes progress, reports a rollout
// that has made none for longer than its deadline (or that has recovered, or
// completed), and closes the records of revisions that were overtaken. It
// returns how many records it wrote. Nothing here changes a workload.
func (r *Reconciler) trackRollouts(ctx context.Context, tx store.Tx, defs []store.Definition, workloads []store.Workload,
	waiting []store.Unschedulable, nodes map[string]string, now time.Time, failed *[]string) (int, error) {
	records, err := tx.ListRollouts(ctx, "")
	if err != nil {
		return 0, fmt.Errorf("list rollouts: %w", err)
	}
	recordsOf := map[string][]store.Rollout{}
	for _, rec := range records {
		recordsOf[rec.ServiceID] = append(recordsOf[rec.ServiceID], rec)
	}
	workloadsOf := map[string][]store.Workload{}
	for _, w := range workloads {
		if w.ServiceID != "" {
			workloadsOf[w.ServiceID] = append(workloadsOf[w.ServiceID], w)
		}
	}

	written := 0
	save := func(svc store.Service, rec store.Rollout, event string) error {
		if err := tx.SaveRollout(ctx, rec); err != nil {
			return fmt.Errorf("save the rollout of revision %d of %s: %w", rec.Revision, svc.Name, err)
		}
		written++
		if event == rollout.EventFailed {
			*failed = append(*failed, svc.Name)
		}
		if event == "" {
			return nil
		}
		_, err := tx.AppendEvent(ctx, r.rolloutEvent(now, svc, rec, event))
		return err
	}
	for _, d := range defs {
		svc := d.Service
		var current *store.Rollout
		for _, rec := range recordsOf[svc.ID] {
			if rec.Revision == svc.CurrentRevision && svc.DeletedAt == nil {
				kept := rec
				current = &kept
				continue
			}
			// Another revision has overtaken this one, or the service is gone.
			if closed, ok := rollout.Supersede(rec, now); ok {
				if err := save(svc, closed, rollout.EventSuperseded); err != nil {
					return 0, err
				}
			}
		}
		if svc.DeletedAt != nil {
			continue
		}
		facts := rollout.Assess(d, workloadsOf[svc.ID], waiting, nodes)
		next, event := rollout.Advance(current, facts, svc.Rollout, svc.Paused, now)
		if current == nil || !next.Same(*current) {
			if err := save(svc, next, event); err != nil {
				return 0, err
			}
		}
	}
	return written, nil
}

// RunOnce makes one pass in a single transaction under the placement lock, so
// it never interleaves with a scheduling pass, here or on another manager. It
// reads the services and workloads, plans, and applies the plan. Applying is
// idempotent: a workload that is already stopped or already gone is skipped,
// and if anything fails nothing is saved and the next pass starts over.
func (r *Reconciler) RunOnce(ctx context.Context) (Report, error) {
	var report Report
	var failed []string
	var epoch int64
	if r.cfg.Fence != nil {
		var err error
		if epoch, err = r.cfg.Fence.Epoch(); err != nil {
			return Report{}, err
		}
	}
	err := r.cfg.Store.InTx(ctx, func(tx store.Tx) error {
		report, failed = Report{}, nil
		if err := tx.LockPlacement(ctx); err != nil {
			return err
		}
		if r.cfg.Fence != nil {
			if err := tx.CheckEpoch(ctx, epoch); err != nil {
				return err
			}
		}
		defs, err := tx.ListDefinitions(ctx, true)
		if err != nil {
			return fmt.Errorf("list services: %w", err)
		}
		workloads, err := tx.ListWorkloads(ctx)
		if err != nil {
			return fmt.Errorf("list workloads: %w", err)
		}
		waiting, err := tx.ListUnschedulable(ctx, "")
		if err != nil {
			return fmt.Errorf("list unschedulable replicas: %w", err)
		}
		nodes, err := tx.ListNodes(ctx)
		if err != nil {
			return fmt.Errorf("list nodes: %w", err)
		}
		now := r.cfg.Now().UTC()
		for _, c := range PlanNodes(nodes, now, r.cfg.Timing) {
			if err := tx.SetNodeHealth(ctx, c.Node.ID, c.To, now); err != nil {
				return fmt.Errorf("record that node %s is %s: %w", c.Node.ID, c.To, err)
			}
			if _, err := tx.AppendEvent(ctx, r.nodeEvent(now, c, r.cfg.Timing.HealthReason(c.To))); err != nil {
				return err
			}
			report.NodeChanges++
		}
		states := r.cfg.Timing.States(nodes, now)
		for _, a := range Plan(defs, workloads, states) {
			w := a.Workload
			label := fmt.Sprintf("%s (revision %d, replica %d)", w.Name, w.ServiceRevision, w.ReplicaIndex)
			switch a.Kind {
			case ActionStop:
				_, err := tx.SetWorkloadDesiredState(ctx, w.ID, store.DesiredStopped, now)
				if errors.Is(err, store.ErrUnchanged) {
					continue
				}
				if err != nil {
					return fmt.Errorf("stop %s: %w", w.Name, err)
				}
				message := fmt.Sprintf("%s on node %s will be stopped: %s", label, w.NodeID, a.Reason)
				if a.Reason == ReasonNodeLost {
					message = fmt.Sprintf("%s was given up on: node %s is unavailable, so the replica is started on another node and the node is told to remove the container if it returns", label, w.NodeID)
				}
				if a.Why != "" {
					message += " (" + a.Why + ")"
				}
				if _, err := tx.AppendEvent(ctx, r.event(now, "replica.stopped", a, message)); err != nil {
					return err
				}
				report.Stopped++
			case ActionDelete:
				err := tx.DeleteWorkload(ctx, w.ID)
				if errors.Is(err, store.ErrNotFound) {
					continue
				}
				if err != nil {
					return fmt.Errorf("delete %s: %w", w.Name, err)
				}
				if _, err := tx.AppendEvent(ctx, r.event(now, "replica.removed", a,
					fmt.Sprintf("%s was removed: node %s confirmed that its container is gone", label, w.NodeID))); err != nil {
					return err
				}
				report.Deleted++
			}
		}
		changed, err := r.trackRollouts(ctx, tx, defs, workloads, waiting, states, now, &failed)
		if err != nil {
			return err
		}
		report.Rollouts = changed
		return nil
	})
	if errors.Is(err, store.ErrStaleLeader) {
		r.cfg.Fence.Fenced(epoch)
	}
	if err != nil {
		return Report{}, err
	}
	if r.cfg.Observer != nil {
		for _, service := range failed {
			r.cfg.Observer.RolloutFailed(service)
		}
	}
	return report, nil
}

// leadershipLost is whether a pass was refused because this manager is not (or
// no longer) the leader.
func leadershipLost(err error) bool {
	return errors.Is(err, election.ErrNotLeader) || errors.Is(err, store.ErrStaleLeader)
}

// Run makes a pass at once and then every Interval until ctx ends. A pass that
// fails is logged and tried again at the next tick. With a zero Interval it
// returns immediately.
func (r *Reconciler) Run(ctx context.Context) {
	if r.cfg.Interval <= 0 {
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
		r.pass(ctx)
		timer.Reset(r.cfg.Interval)
	}
}

func (r *Reconciler) pass(ctx context.Context) {
	passCtx, cancel := context.WithTimeout(ctx, max(r.cfg.Interval, maxPassDuration))
	defer cancel()
	began := time.Now()
	report, err := r.RunOnce(passCtx)
	if leadershipLost(err) {
		// Not a failure of reconciling: another manager leads, or this one stopped
		// leading while the pass ran, and nothing was saved.
		r.cfg.Logger.Info("reconciliation pass skipped: this manager does not lead", "reason", err.Error())
		return
	}
	if r.cfg.Observer != nil && (err == nil || ctx.Err() == nil) {
		r.cfg.Observer.ReconcilePass(time.Since(began), report, err)
	}
	switch {
	case err != nil && ctx.Err() != nil:
		// Shutting down; nothing was saved.
	case err != nil:
		r.cfg.Logger.Error("reconciliation pass failed", "error", err)
	case report != (Report{}):
		r.cfg.Logger.Info("reconciliation pass", "stopped", report.Stopped, "deleted", report.Deleted, "node_changes", report.NodeChanges)
	default:
		r.cfg.Logger.Debug("reconciliation pass found nothing to do")
	}
}
