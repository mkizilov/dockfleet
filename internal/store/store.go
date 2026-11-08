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

const WorkloadScheduled = "scheduled"

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

// Workload is desired state only: a placement decision, nothing is run yet.
type Workload struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Image       string    `json:"image"`
	CPUMillis   int64     `json:"cpu_millis"`
	MemoryBytes int64     `json:"memory_bytes"`
	NodeID      string    `json:"node_id"`
	State       string    `json:"state"`
	CreatedAt   time.Time `json:"created_at"`
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

	Ping(ctx context.Context) error
	Close()
}
