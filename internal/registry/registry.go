// Package registry tracks which nodes are connected to the manager.
//
// Node records and workloads are durable and live in the store. The registry
// adds the volatile part: one session per node, created at registration and
// lost when the manager restarts, which forces agents to register again.
package registry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
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
	UpsertNode(ctx context.Context, n store.Node) (store.Node, error)
	Heartbeat(ctx context.Context, id string, at time.Time) error
	ListWorkloadsForNode(ctx context.Context, nodeID string) ([]store.Workload, error)
	UpdateWorkloadStatus(ctx context.Context, id, nodeID, state, message string, at time.Time) error
	GetEnrollment(ctx context.Context, nodeID string) (store.Enrollment, error)
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
}

type Registry struct {
	store  Store
	logger *slog.Logger
	now    func() time.Time

	mu       sync.Mutex
	sessions map[string]Session
}

func New(st Store, logger *slog.Logger) *Registry {
	return &Registry{store: st, logger: logger, now: time.Now, sessions: map[string]Session{}}
}

func newSessionID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
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
// earlier one) and returns the workloads currently assigned to it. Only
// enrolled nodes may register; nothing is stored for others. The caller must
// have validated n.
func (r *Registry) Register(ctx context.Context, n store.Node) (Session, []store.Workload, error) {
	if err := r.checkEnrolled(ctx, n.ID); err != nil {
		return Session{}, nil, err
	}
	now := r.now()
	n.RegisteredAt = now
	n.LastHeartbeat = now
	if _, err := r.store.UpsertNode(ctx, n); err != nil {
		return Session{}, nil, fmt.Errorf("persist node: %w", err)
	}
	workloads, err := r.store.ListWorkloadsForNode(ctx, n.ID)
	if err != nil {
		return Session{}, nil, fmt.Errorf("list assignments: %w", err)
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

// Heartbeat refreshes the node's liveness, applies status reports and returns
// the full set of workloads assigned to the node. Reports for workloads that
// are not assigned to the node are ignored.
func (r *Registry) Heartbeat(ctx context.Context, nodeID, sessionID string, reports []StatusReport) ([]store.Workload, error) {
	if err := r.check(nodeID, sessionID); err != nil {
		return nil, err
	}
	// Re-checked on every heartbeat so a revocation takes effect immediately.
	if err := r.checkEnrolled(ctx, nodeID); err != nil {
		if errors.Is(err, ErrNotEnrolled) {
			r.drop(nodeID, sessionID)
			r.logger.WarnContext(ctx, "session ended: node enrollment is gone", "node_id", nodeID, "session", sessionID)
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
	index := make(map[string]int, len(workloads))
	for i, w := range workloads {
		index[w.ID] = i
	}
	for _, rep := range reports {
		i, ok := index[rep.WorkloadID]
		if !ok {
			r.logger.DebugContext(ctx, "ignoring status for a workload not assigned to the node", "node_id", nodeID, "workload_id", rep.WorkloadID)
			continue
		}
		w := &workloads[i]
		if w.State == rep.State && w.StatusMessage == rep.Message {
			continue
		}
		if err := r.store.UpdateWorkloadStatus(ctx, w.ID, nodeID, rep.State, rep.Message, now); err != nil {
			return nil, fmt.Errorf("record workload status: %w", err)
		}
		w.State = rep.State
		w.StatusMessage = rep.Message
		reportedAt := now
		w.StatusUpdatedAt = &reportedAt
	}
	return workloads, nil
}

// Deregister ends the session and marks the node unavailable immediately.
func (r *Registry) Deregister(ctx context.Context, nodeID, sessionID string) error {
	if err := r.check(nodeID, sessionID); err != nil {
		return err
	}
	r.drop(nodeID, sessionID)
	// The zero time makes the node not ready until it registers again.
	if err := r.store.Heartbeat(ctx, nodeID, time.Time{}); err != nil && !errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("mark node unavailable: %w", err)
	}
	r.logger.InfoContext(ctx, "node deregistered", "node_id", nodeID, "session", sessionID)
	return nil
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
