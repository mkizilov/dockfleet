package manager

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/dockfleet/dockfleet/internal/election"
	"github.com/dockfleet/dockfleet/internal/placement"
	"github.com/dockfleet/dockfleet/internal/reconcile"
	"github.com/dockfleet/dockfleet/internal/store"
)

// electionActor is who the audit trail names for changes of leadership.
const electionActor = "election"

// beginTerm starts a term in the shared database once this manager holds the
// leadership key: it takes the next epoch and records that it is the leader,
// in one transaction, so the audit trail and the fencing record cannot differ.
// It is the only place an epoch is made.
func (m *Manager) beginTerm(ctx context.Context, etcdRevision int64) (election.Term, error) {
	var term election.Term
	err := m.store.InTx(ctx, func(tx store.Tx) error {
		now := time.Now().UTC()
		cur, prev, err := tx.BeginTerm(ctx, m.cfg.InstanceID, now)
		if err != nil {
			return err
		}
		term = election.Term{Epoch: cur.Epoch, Previous: prev.Leader, PreviousEpoch: prev.Epoch}
		message := fmt.Sprintf("%s became the leader (epoch %d)", m.cfg.InstanceID, cur.Epoch)
		if prev.Epoch > 0 {
			message += fmt.Sprintf(", replacing %s (epoch %d)", prev.Leader, prev.Epoch)
		}
		_, err = tx.AppendEvent(ctx, store.Event{
			At: now, InstanceID: m.cfg.InstanceID, Actor: electionActor, Action: "leader.elected",
			ResourceType: "cluster", ResourceID: "control-plane", Message: message,
			Details: map[string]string{
				"epoch": strconv.FormatInt(cur.Epoch, 10), "previous_leader": prev.Leader,
				"previous_epoch": strconv.FormatInt(prev.Epoch, 10), "etcd_revision": strconv.FormatInt(etcdRevision, 10),
			},
		})
		return err
	})
	return term, err
}

// leaderDuty is what only the leader does: the scheduling and reconciliation
// loops. They are built for the term, not kept from an earlier one, and act under
// its epoch (Fence), so every pass is checked against the database. When the term
// ends the context ends and both loops return before the next term can start.
//
// Silence from before the term began is not counted against a node: the agents
// had no leader to talk to while the election ran, and need a little time to find
// the new one. The same start is read from the database by the API.
func (m *Manager) leaderDuty(ctx context.Context, term election.Term) {
	timing := m.timing
	if term.Since.After(timing.Since) {
		timing.Since = term.Since
	}
	var wg sync.WaitGroup
	if m.cfg.SchedulerInterval > 0 {
		scheduler := placement.New(placement.Config{
			Store: m.store, Logger: m.logger, NodeTTL: m.cfg.NodeTTL, Timing: timing, Overcommit: overcommit(m.cfg),
			Interval: m.cfg.SchedulerInterval, InstanceID: m.cfg.InstanceID, Observer: m.schedulingObserver, Fence: m.elector,
		})
		wg.Add(1)
		go func() {
			defer wg.Done()
			scheduler.Run(ctx)
		}()
	}
	if m.cfg.ReconcileInterval > 0 {
		reconciler := reconcile.New(reconcile.Config{
			Store: m.store, Logger: m.logger, Interval: m.cfg.ReconcileInterval, InstanceID: m.cfg.InstanceID, Timing: timing,
			Observer: m.reconcileObserver, Fence: m.elector,
		})
		wg.Add(1)
		go func() {
			defer wg.Done()
			reconciler.Run(ctx)
		}()
	}
	wg.Wait()
}

// onLeadershipChange records that this manager stopped leading. (Becoming the
// leader is recorded by beginTerm, with the epoch, in the term's transaction.)
// The record is made without any fence: it says what this manager did, not
// something it decided for the cluster, and a manager that was replaced must
// still be able to say so.
func (m *Manager) onLeadershipChange(c election.Change) {
	if c.To == election.Leader {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := m.store.AppendEvent(ctx, store.Event{
		At: c.At.UTC(), InstanceID: m.cfg.InstanceID, Actor: electionActor, Action: "leader.lost",
		ResourceType: "cluster", ResourceID: "control-plane",
		Message: fmt.Sprintf("%s stopped being the leader (epoch %d): %s", m.cfg.InstanceID, c.Term.Epoch, c.Reason),
		Details: map[string]string{"epoch": strconv.FormatInt(c.Term.Epoch, 10), "reason": c.Reason},
	})
	if err != nil {
		m.logger.Error("recording the loss of leadership failed", "epoch", c.Term.Epoch, "error", err)
	}
}
