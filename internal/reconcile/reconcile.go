package reconcile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"time"

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
	// Now defaults to time.Now; tests set it.
	Now func() time.Time
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
}

func (r *Reconciler) event(at time.Time, action string, a Action, message string) store.Event {
	w := a.Workload
	return store.Event{
		At: at, InstanceID: r.cfg.InstanceID, Actor: Actor, Action: action,
		ResourceType: "workload", ResourceID: w.ID, Message: message,
		Details: map[string]string{
			"service_id": w.ServiceID, "revision": strconv.Itoa(w.ServiceRevision), "replica": strconv.Itoa(w.ReplicaIndex),
			"node_id": w.NodeID, "reason": a.Reason,
		},
	}
}

// RunOnce makes one pass in a single transaction under the placement lock, so
// it never interleaves with a scheduling pass, here or on another manager. It
// reads the services and workloads, plans, and applies the plan. Applying is
// idempotent: a workload that is already stopped or already gone is skipped,
// and if anything fails nothing is saved and the next pass starts over.
func (r *Reconciler) RunOnce(ctx context.Context) (Report, error) {
	var report Report
	err := r.cfg.Store.InTx(ctx, func(tx store.Tx) error {
		report = Report{}
		if err := tx.LockPlacement(ctx); err != nil {
			return err
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
		now := r.cfg.Now().UTC()
		for _, a := range Plan(defs, workloads, waiting) {
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
				if _, err := tx.AppendEvent(ctx, r.event(now, "replica.stopped", a,
					fmt.Sprintf("%s on node %s will be stopped: %s", label, w.NodeID, a.Reason))); err != nil {
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
		return nil
	})
	if err != nil {
		return Report{}, err
	}
	return report, nil
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
	report, err := r.RunOnce(passCtx)
	switch {
	case err != nil && ctx.Err() != nil:
		// Shutting down; nothing was saved.
	case err != nil:
		r.cfg.Logger.Error("reconciliation pass failed", "error", err)
	case report != (Report{}):
		r.cfg.Logger.Info("reconciliation pass", "stopped", report.Stopped, "deleted", report.Deleted)
	default:
		r.cfg.Logger.Debug("reconciliation pass found nothing to do")
	}
}
