package store

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Memory is an in-process Store for tests and local development. State is lost on exit.
type Memory struct {
	mu        sync.RWMutex
	nodes     map[string]Node
	workloads map[string]Workload
}

var _ Store = (*Memory)(nil)

func NewMemory() *Memory {
	return &Memory{nodes: map[string]Node{}, workloads: map[string]Workload{}}
}

func cloneLabels(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneNode(n Node) Node {
	n.Labels = cloneLabels(n.Labels)
	return n
}

func (m *Memory) UpsertNode(_ context.Context, n Node) (Node, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if old, ok := m.nodes[n.ID]; ok {
		n.RegisteredAt = old.RegisteredAt
	}
	n = cloneNode(n)
	m.nodes[n.ID] = n
	return cloneNode(n), nil
}

func (m *Memory) Heartbeat(_ context.Context, id string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.nodes[id]
	if !ok {
		return ErrNotFound
	}
	n.LastHeartbeat = at
	m.nodes[id] = n
	return nil
}

func (m *Memory) GetNode(_ context.Context, id string) (Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n, ok := m.nodes[id]
	if !ok {
		return Node{}, ErrNotFound
	}
	return cloneNode(n), nil
}

func (m *Memory) ListNodes(_ context.Context) ([]Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Node, 0, len(m.nodes))
	for _, n := range m.nodes {
		out = append(out, cloneNode(n))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (m *Memory) CreateWorkload(_ context.Context, w Workload) (Workload, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.nodes[w.NodeID]; !ok {
		return Workload{}, ErrNotFound
	}
	for _, existing := range m.workloads {
		if existing.ID == w.ID || existing.Name == w.Name {
			return Workload{}, ErrConflict
		}
	}
	m.workloads[w.ID] = w
	return w, nil
}

func (m *Memory) GetWorkload(_ context.Context, id string) (Workload, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	w, ok := m.workloads[id]
	if !ok {
		return Workload{}, ErrNotFound
	}
	return w, nil
}

func (m *Memory) ListWorkloads(_ context.Context) ([]Workload, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Workload, 0, len(m.workloads))
	for _, w := range m.workloads {
		out = append(out, w)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (m *Memory) Ping(context.Context) error { return nil }

func (m *Memory) Close() {}
