// Package registry tracks which nodes are connected to the manager.
//
// Node records, workloads and observations are durable and live in the store.
// The registry adds the volatile part: one session per node, created at
// registration and lost when the manager restarts, which forces agents to
// register again. With several managers sharing a database, each keeps the
// sessions of the agents connected to it; everything else is shared.
package registry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/dockfleet/dockfleet/internal/store"
)

var (
	ErrNotRegistered     = errors.New("node is not registered")
	ErrSessionSuperseded = errors.New("session was superseded by a newer registration")
	// ErrNotEnrolled means no active enrollment exists for the node: it was
	// never enrolled or an administrator revoked it.
	ErrNotEnrolled = errors.New("node is not enrolled")
)

// Store is the subset of store.Store the registry uses.
type Store interface {
	GetEnrollment(ctx context.Context, nodeID string) (store.Enrollment, error)
	Heartbeat(ctx context.Context, id string, at time.Time) error
	ListWorkloadsForNode(ctx context.Context, nodeID string) ([]store.Workload, error)
	RecordObservations(ctx context.Context, nodeID string, obs []store.Observation) error
	InTx(ctx context.Context, fn func(tx store.Tx) error) error
}

// Session identifies one registration of a node.
type Session struct {
	ID        string
	NodeID    string
	StartedAt time.Time
	LastSeen  time.Time
}

// StatusReport is an agent's observation of one workload.
type StatusReport struct {
	WorkloadID string
	State      string
	Message    string

	// ContainerID is empty when the node has no container for the workload.
	ContainerID  string
	ExitCode     *int
	OOMKilled    bool
	RestartCount int
	// Health is the container's health check result: empty without a check,
	// else "starting", "healthy" or "unhealthy".
	Health string

	// AckedGeneration is the highest generation of the workload's assignment
	// the agent has received.
	AckedGeneration int64
}

type Registry struct {
	store      Store
	logger     *slog.Logger
	now        func() time.Time
	instanceID string

	mu       sync.Mutex
	sessions map[string]Session
}

func New(st Store, logger *slog.Logger) *Registry {
	return &Registry{store: st, logger: logger, now: time.Now, sessions: map[string]Session{}}
}

// WithInstanceID names this manager in the events the registry records.
func (r *Registry) WithInstanceID(id string) *Registry {
	r.instanceID = id
	return r
}

func newSessionID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func (r *Registry) event(at time.Time, nodeID, action string, details map[string]string) store.Event {
	return store.Event{
		At: at, InstanceID: r.instanceID, Actor: "node:" + nodeID, Action: action,
		ResourceType: "node", ResourceID: nodeID, Details: details,
	}
}

// checkEnrolled returns ErrNotEnrolled unless an active enrollment exists.
func (r *Registry) checkEnrolled(ctx context.Context, nodeID string) error {
	e, err := r.store.GetEnrollment(ctx, nodeID)
	switch {
	case errors.Is(err, store.ErrNotFound):
		return ErrNotEnrolled
	case err != nil:
		return fmt.Errorf("check enrollment: %w", err)
	case !e.Active():
		return ErrNotEnrolled
	}
	return nil
}

// Register persists the node, starts a new session for it (replacing any
// earlier one) and returns the workloads assigned to it. Only enrolled nodes
// may register; nothing is stored for others. Saving the node and recording
// the event happen in one transaction. The caller must have validated n.
func (r *Registry) Register(ctx context.Context, n store.Node) (Session, []store.Workload, error) {
	if err := r.checkEnrolled(ctx, n.ID); err != nil {
		return Session{}, nil, err
	}
	now := r.now()
	n.RegisteredAt = now
	n.LastHeartbeat = now
	var workloads []store.Workload
	err := r.store.InTx(ctx, func(tx store.Tx) error {
		if _, err := tx.UpsertNode(ctx, n); err != nil {
			return fmt.Errorf("persist node: %w", err)
		}
		var err error
		if workloads, err = tx.ListWorkloadsForNode(ctx, n.ID); err != nil {
			return fmt.Errorf("list assignments: %w", err)
		}
		_, err = tx.AppendEvent(ctx, r.event(now, n.ID, "node.registered", map[string]string{"agent_version": n.AgentVersion}))
		return err
	})
	if err != nil {
		return Session{}, nil, err
	}
	id, err := newSessionID()
	if err != nil {
		return Session{}, nil, fmt.Errorf("create session id: %w", err)
	}
	s := Session{ID: id, NodeID: n.ID, StartedAt: now, LastSeen: now}

	r.mu.Lock()
	prev, replaced := r.sessions[n.ID]
	r.sessions[n.ID] = s
	r.mu.Unlock()

	if replaced {
		r.logger.InfoContext(ctx, "node session superseded", "node_id", n.ID, "old_session", prev.ID, "new_session", s.ID)
	}
	return s, workloads, nil
}

// Heartbeat refreshes the node's liveness, stores status reports as
// observations and returns the workloads assigned to the node: every workload
// placed on it, each with the desired state its container must be brought to
// (running ones created and started, stopped ones stopped and removed). A
// workload only leaves the list when the manager deletes it, after the node
// confirmed the container is gone. Reports for workloads that are not placed
// on the node are ignored, and so are reports that repeat the stored
// observation. Observed state is the only thing a heartbeat writes about
// workloads: it never touches desired state.
func (r *Registry) Heartbeat(ctx context.Context, nodeID, sessionID string, reports []StatusReport) ([]store.Workload, error) {
	if err := r.check(nodeID, sessionID); err != nil {
		return nil, err
	}
	// Re-checked on every heartbeat so a revocation takes effect immediately.
	if err := r.checkEnrolled(ctx, nodeID); err != nil {
		if errors.Is(err, ErrNotEnrolled) {
			r.endSession(ctx, nodeID, sessionID, "enrollment revoked")
		}
		return nil, err
	}
	now := r.now()
	if err := r.store.Heartbeat(ctx, nodeID, now); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			r.drop(nodeID, sessionID)
			return nil, ErrNotRegistered
		}
		return nil, fmt.Errorf("record heartbeat: %w", err)
	}
	r.touch(nodeID, sessionID, now)

	workloads, err := r.store.ListWorkloadsForNode(ctx, nodeID)
	if err != nil {
		return nil, fmt.Errorf("list assignments: %w", err)
	}
	placed := make(map[string]*store.Workload, len(workloads))
	for i := range workloads {
		placed[workloads[i].ID] = &workloads[i]
	}
	var changed []store.Observation
	var events []store.Event
	for _, rep := range reports {
		w, ok := placed[rep.WorkloadID]
		if !ok {
			r.logger.DebugContext(ctx, "ignoring status for a workload not placed on the node", "node_id", nodeID, "workload_id", rep.WorkloadID)
			continue
		}
		// An agent cannot have seen a generation that does not exist yet, and
		// an acknowledgment never goes back.
		acked := min(max(rep.AckedGeneration, 0), w.Generation)
		if w.Observed != nil {
			acked = max(acked, w.Observed.AckedGeneration)
		}
		obs := store.Observation{
			WorkloadID: w.ID, NodeID: nodeID, State: rep.State, Message: rep.Message,
			ContainerID: rep.ContainerID, ExitCode: rep.ExitCode, OOMKilled: rep.OOMKilled, RestartCount: rep.RestartCount,
			Health: rep.Health, AckedGeneration: acked, ObservedAt: now,
		}
		if w.Observed != nil && obs.SameReport(*w.Observed) {
			continue
		}
		events = append(events, r.failureEvents(now, *w, obs)...)
		changed = append(changed, obs)
		w.Observed = &obs
	}
	if len(changed) == 0 {
		return workloads, nil
	}
	// The observations and the events about them are saved together.
	err = r.store.InTx(ctx, func(tx store.Tx) error {
		if err := tx.RecordObservations(ctx, nodeID, changed); err != nil {
			return err
		}
		for _, ev := range events {
			if _, err := tx.AppendEvent(ctx, ev); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("record workload observations: %w", err)
	}
	return workloads, nil
}

// failureEvents are the audit events a new report of a workload calls for: it
// failed, turned unhealthy, was restarted, or recovered. The first report of a
// workload that is fine says nothing, and neither does a repeat of a state
// already recorded. w holds the previous observation.
func (r *Registry) failureEvents(at time.Time, w store.Workload, obs store.Observation) []store.Event {
	prev := w.Observed
	var out []store.Event
	add := func(action, message string) {
		out = append(out, store.Event{
			At: at, InstanceID: r.instanceID, Actor: "node:" + obs.NodeID, Action: action,
			ResourceType: "workload", ResourceID: w.ID, Message: message,
			Details: map[string]string{
				"node_id": obs.NodeID, "service_id": w.ServiceID, "restart_count": strconv.Itoa(obs.RestartCount),
			},
		})
	}
	wasFailed := prev != nil && prev.State == store.WorkloadFailed
	wasUnhealthy := prev != nil && prev.Health == store.HealthUnhealthy
	switch {
	case obs.State == store.WorkloadFailed && !wasFailed:
		add("workload.failed", fmt.Sprintf("%s failed on node %s: %s", w.Name, obs.NodeID, obs.Message))
	case obs.Health == store.HealthUnhealthy && !wasUnhealthy:
		add("workload.unhealthy", fmt.Sprintf("%s is unhealthy on node %s", w.Name, obs.NodeID))
	case obs.State == store.WorkloadRunning && obs.Health != store.HealthUnhealthy && (wasFailed || wasUnhealthy):
		add("workload.recovered", fmt.Sprintf("%s runs again on node %s", w.Name, obs.NodeID))
	}
	if prev != nil && obs.RestartCount > prev.RestartCount {
		add("workload.restarted", fmt.Sprintf("%s was restarted on node %s (%d so far)", w.Name, obs.NodeID, obs.RestartCount))
	}
	return out
}

// Deregister ends the session and takes the node out of scheduling at once. Its
// containers keep running, so its replicas are not moved until the node has been
// gone for as long as a node that stopped heartbeating.
func (r *Registry) Deregister(ctx context.Context, nodeID, sessionID string) error {
	if err := r.check(nodeID, sessionID); err != nil {
		return err
	}
	r.drop(nodeID, sessionID)
	now := r.now()
	err := r.store.InTx(ctx, func(tx store.Tx) error {
		// The node is not ready until it registers again; how long it has been
		// gone is counted from now.
		if err := tx.DeregisterNode(ctx, nodeID, now); err != nil && !errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("mark node deregistered: %w", err)
		}
		_, err := tx.AppendEvent(ctx, r.event(now, nodeID, "node.deregistered", nil))
		return err
	})
	if err != nil {
		return err
	}
	r.logger.InfoContext(ctx, "node deregistered", "node_id", nodeID, "session", sessionID)
	return nil
}

// endSession drops a session the manager is ending on its own initiative.
func (r *Registry) endSession(ctx context.Context, nodeID, sessionID, reason string) {
	r.drop(nodeID, sessionID)
	r.logger.WarnContext(ctx, "session ended", "node_id", nodeID, "session", sessionID, "reason", reason)
	err := r.store.InTx(ctx, func(tx store.Tx) error {
		_, err := tx.AppendEvent(ctx, r.event(r.now(), nodeID, "node.session_ended", map[string]string{"reason": reason}))
		return err
	})
	if err != nil {
		r.logger.ErrorContext(ctx, "recording the end of a session failed", "node_id", nodeID, "error", err)
	}
}

// Sessions returns the live sessions ordered by node ID.
func (r *Registry) Sessions() []Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Session, 0, len(r.sessions))
	for _, s := range r.sessions {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

func (r *Registry) check(nodeID, sessionID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.sessions[nodeID]
	switch {
	case !ok:
		return ErrNotRegistered
	case s.ID != sessionID:
		return ErrSessionSuperseded
	}
	return nil
}

func (r *Registry) touch(nodeID, sessionID string, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.sessions[nodeID]; ok && s.ID == sessionID {
		s.LastSeen = at
		r.sessions[nodeID] = s
	}
}

func (r *Registry) drop(nodeID, sessionID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.sessions[nodeID]; ok && s.ID == sessionID {
		delete(r.sessions, nodeID)
	}
}
