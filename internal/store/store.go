package store

import (
	"context"
	"errors"
	"regexp"
	"time"
)

var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("already exists")
)

// Workload states. The manager sets scheduled; the rest are reported by agents.
const (
	WorkloadScheduled = "scheduled"
	WorkloadPending   = "pending"
	WorkloadRunning   = "running"
	WorkloadSucceeded = "succeeded"
	WorkloadFailed    = "failed"
	WorkloadStopped   = "stopped"
)

var namePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$`)

// ValidName reports whether s is acceptable as a node ID or workload name.
func ValidName(s string) bool {
	return namePattern.MatchString(s)
}

type Node struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	CPUMillis     int64             `json:"cpu_millis"`
	MemoryBytes   int64             `json:"memory_bytes"`
	Labels        map[string]string `json:"labels"`
	AgentVersion  string            `json:"agent_version"`
	RegisteredAt  time.Time         `json:"registered_at"`
	LastHeartbeat time.Time         `json:"last_heartbeat"`
}

// Enrollment is an administrator's explicit permission for a node ID to join
// the fleet. Only enrolled nodes may register; revoking blocks a node again.
type Enrollment struct {
	NodeID      string     `json:"node_id"`
	Description string     `json:"description"`
	CreatedAt   time.Time  `json:"created_at"`
	RevokedAt   *time.Time `json:"revoked_at,omitempty"`
}

// Active reports whether the enrollment has not been revoked.
func (e Enrollment) Active() bool {
	return e.RevokedAt == nil
}

// Workload is desired state only: a placement decision, nothing is run yet.
type Workload struct {
	ID              string     `json:"id"`
	Name            string     `json:"name"`
	Image           string     `json:"image"`
	CPUMillis       int64      `json:"cpu_millis"`
	MemoryBytes     int64      `json:"memory_bytes"`
	NodeID          string     `json:"node_id"`
	State           string     `json:"state"`
	StatusMessage   string     `json:"status_message"`
	StatusUpdatedAt *time.Time `json:"status_updated_at,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
}

// Store persists cluster state. Implementations must be safe for concurrent use.
type Store interface {
	// UpsertNode inserts or updates a node. An existing node keeps its RegisteredAt.
	UpsertNode(ctx context.Context, n Node) (Node, error)
	// Heartbeat sets LastHeartbeat; it returns ErrNotFound for unknown nodes.
	Heartbeat(ctx context.Context, id string, at time.Time) error
	GetNode(ctx context.Context, id string) (Node, error)
	ListNodes(ctx context.Context) ([]Node, error)

	// CreateWorkload returns ErrConflict for a duplicate ID or name and
	// ErrNotFound when the target node does not exist.
	CreateWorkload(ctx context.Context, w Workload) (Workload, error)
	GetWorkload(ctx context.Context, id string) (Workload, error)
	ListWorkloads(ctx context.Context) ([]Workload, error)
	// ListWorkloadsForNode returns the workloads assigned to nodeID, ordered by name.
	ListWorkloadsForNode(ctx context.Context, nodeID string) ([]Workload, error)
	// UpdateWorkloadStatus records an agent-reported state. It returns
	// ErrNotFound when the workload does not exist or is assigned to another node.
	UpdateWorkloadStatus(ctx context.Context, id, nodeID, state, message string, at time.Time) error

	// CreateEnrollment enrolls a node ID. It returns ErrConflict while an
	// active enrollment exists; a revoked one is replaced (re-enrollment).
	CreateEnrollment(ctx context.Context, e Enrollment) (Enrollment, error)
	GetEnrollment(ctx context.Context, nodeID string) (Enrollment, error)
	ListEnrollments(ctx context.Context) ([]Enrollment, error)
	// RevokeEnrollment sets RevokedAt (keeping an earlier revocation time) and
	// returns ErrNotFound for a node that was never enrolled.
	RevokeEnrollment(ctx context.Context, nodeID string, at time.Time) (Enrollment, error)

	Ping(ctx context.Context) error
	Close()
}
