package store

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Memory is an in-process Store for tests and local development. State is
// lost on exit. It follows the same rules as Postgres (uniqueness, foreign
// keys, optimistic locking, transactional all-or-nothing), which the shared
// conformance suite checks.
type Memory struct {
	mu sync.RWMutex
	s  memState
}

type memState struct {
	nodes        map[string]Node
	enrollments  map[string]Enrollment
	workloads    map[string]Workload // desired state only; Observed is always nil here
	observations map[string]Observation
	services     map[string]Service
	revisions    map[string][]Revision // per service; element i is revision i+1
	events       []Event
	nextEventID  int64
	leases       map[string]Lease

	unschedulable map[slotKey]Unschedulable
}

// slotKey identifies one replica slot of a service revision.
type slotKey struct {
	serviceID string
	revision  int
	index     int
}

var _ Store = (*Memory)(nil)

func newMemState() memState {
	return memState{
		nodes:        map[string]Node{},
		enrollments:  map[string]Enrollment{},
		workloads:    map[string]Workload{},
		observations: map[string]Observation{},
		services:     map[string]Service{},
		revisions:    map[string][]Revision{},
		nextEventID:  1,
		leases:       map[string]Lease{},

		unschedulable: map[slotKey]Unschedulable{},
	}
}

func NewMemory() *Memory {
	return &Memory{s: newMemState()}
}

func cloneLabels(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	v := t.UTC()
	return &v
}

func cloneNode(n Node) Node {
	n.Labels = cloneLabels(n.Labels)
	return n
}

func cloneEnrollment(e Enrollment) Enrollment {
	e.RevokedAt = clonePtr(e.RevokedAt)
	return e
}

func cloneObservation(o Observation) Observation {
	o.ExitCode = clonePtr(o.ExitCode)
	o.StartedAt = clonePtr(o.StartedAt)
	o.FinishedAt = clonePtr(o.FinishedAt)
	return o
}

func cloneSpec(s ServiceSpec) ServiceSpec {
	s.Command = append([]string(nil), s.Command...)
	if s.Env != nil {
		s.Env = cloneLabels(s.Env)
	}
	if s.NodeSelector != nil {
		s.NodeSelector = cloneLabels(s.NodeSelector)
	}
	if s.Ports != nil {
		s.Ports = append([]ServicePort(nil), s.Ports...)
	}
	if s.HealthCheck != nil {
		hc := *s.HealthCheck
		hc.Command = append([]string(nil), hc.Command...)
		s.HealthCheck = &hc
	}
	s.RestartPolicy = clonePtr(s.RestartPolicy)
	return s
}

func cloneService(s Service) Service {
	s.DeletedAt = clonePtr(s.DeletedAt)
	return s
}

func cloneRevision(r Revision) Revision {
	r.Spec = cloneSpec(r.Spec)
	return r
}

func cloneEvent(e Event) Event {
	if e.Details != nil {
		e.Details = cloneLabels(e.Details)
	}
	return e
}

func (s memState) clone() memState {
	c := newMemState()
	for k, v := range s.nodes {
		c.nodes[k] = cloneNode(v)
	}
	for k, v := range s.enrollments {
		c.enrollments[k] = cloneEnrollment(v)
	}
	for k, v := range s.workloads {
		c.workloads[k] = v
	}
	for k, v := range s.observations {
		c.observations[k] = cloneObservation(v)
	}
	for k, v := range s.services {
		c.services[k] = cloneService(v)
	}
	for k, revs := range s.revisions {
		for _, r := range revs {
			c.revisions[k] = append(c.revisions[k], cloneRevision(r))
		}
	}
	for _, e := range s.events {
		c.events = append(c.events, cloneEvent(e))
	}
	c.nextEventID = s.nextEventID
	for k, v := range s.leases {
		c.leases[k] = v
	}
	for k, v := range s.unschedulable {
		c.unschedulable[k] = cloneUnschedulable(v)
	}
	return c
}

func cloneUnschedulable(u Unschedulable) Unschedulable {
	u.Nodes = append([]NodeReason{}, u.Nodes...)
	return u
}

// memTx is a transaction: a private copy of the state that replaces the
// original when the transaction function returns nil.
type memTx struct{ *Memory }

func (memTx) LockPlacement(context.Context) error { return nil }

// InTx holds the store's write lock for the whole transaction, so it is
// serializable; calling the Store from inside fn would deadlock.
func (m *Memory) InTx(_ context.Context, fn func(tx Tx) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	work := &Memory{s: m.s.clone()}
	if err := fn(memTx{work}); err != nil {
		return err
	}
	m.s = work.s
	return nil
}

func (m *Memory) Ping(context.Context) error { return nil }

func (m *Memory) Close() {}

// ---- nodes ----

func (m *Memory) UpsertNode(_ context.Context, n Node) (Node, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if old, ok := m.s.nodes[n.ID]; ok {
		n.RegisteredAt = old.RegisteredAt
	}
	n = cloneNode(n)
	m.s.nodes[n.ID] = n
	return cloneNode(n), nil
}

func (m *Memory) Heartbeat(_ context.Context, id string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.s.nodes[id]
	if !ok {
		return ErrNotFound
	}
	n.LastHeartbeat = at
	m.s.nodes[id] = n
	return nil
}

func (m *Memory) GetNode(_ context.Context, id string) (Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n, ok := m.s.nodes[id]
	if !ok {
		return Node{}, ErrNotFound
	}
	return cloneNode(n), nil
}

func (m *Memory) ListNodes(_ context.Context) ([]Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Node, 0, len(m.s.nodes))
	for _, n := range m.s.nodes {
		out = append(out, cloneNode(n))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// ---- enrollments ----

func (m *Memory) CreateEnrollment(_ context.Context, e Enrollment) (Enrollment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.s.enrollments[e.NodeID]; ok && existing.Active() {
		return Enrollment{}, ErrConflict
	}
	e.RevokedAt = nil
	m.s.enrollments[e.NodeID] = e
	return cloneEnrollment(e), nil
}

func (m *Memory) GetEnrollment(_ context.Context, nodeID string) (Enrollment, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.s.enrollments[nodeID]
	if !ok {
		return Enrollment{}, ErrNotFound
	}
	return cloneEnrollment(e), nil
}

func (m *Memory) ListEnrollments(_ context.Context) ([]Enrollment, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Enrollment, 0, len(m.s.enrollments))
	for _, e := range m.s.enrollments {
		out = append(out, cloneEnrollment(e))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out, nil
}

func (m *Memory) RevokeEnrollment(_ context.Context, nodeID string, at time.Time) (Enrollment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.s.enrollments[nodeID]
	if !ok {
		return Enrollment{}, ErrNotFound
	}
	if e.RevokedAt == nil {
		e.RevokedAt = &at
		m.s.enrollments[nodeID] = e
	}
	return cloneEnrollment(e), nil
}

// ---- workloads and observations ----

// view returns the workload with its observation attached.
func (m *Memory) view(w Workload) Workload {
	w.Ports = append([]ServicePort(nil), w.Ports...)
	w.Command = append([]string(nil), w.Command...)
	if w.Env != nil {
		w.Env = cloneLabels(w.Env)
	}
	w.RestartPolicy = clonePtr(w.RestartPolicy)
	if o, ok := m.s.observations[w.ID]; ok {
		c := cloneObservation(o)
		w.Observed = &c
	}
	return w
}

func (m *Memory) CreateWorkload(_ context.Context, w Workload) (Workload, error) {
	w, err := w.prepareForCreate()
	if err != nil {
		return Workload{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.s.nodes[w.NodeID]; !ok {
		return Workload{}, fmt.Errorf("%w: node %s", ErrNotFound, w.NodeID)
	}
	if w.ServiceID != "" && w.ServiceRevision > len(m.s.revisions[w.ServiceID]) {
		return Workload{}, fmt.Errorf("%w: revision %d of service %s", ErrNotFound, w.ServiceRevision, w.ServiceID)
	}
	for _, existing := range m.s.workloads {
		switch {
		case existing.ID == w.ID || existing.Name == w.Name:
			return Workload{}, ErrConflict
		case w.ServiceID != "" && existing.ServiceID == w.ServiceID && existing.ServiceRevision == w.ServiceRevision && existing.ReplicaIndex == w.ReplicaIndex:
			return Workload{}, ErrConflict
		}
	}
	m.s.workloads[w.ID] = w
	return m.view(w), nil
}

func (m *Memory) GetWorkload(_ context.Context, id string) (Workload, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	w, ok := m.s.workloads[id]
	if !ok {
		return Workload{}, ErrNotFound
	}
	return m.view(w), nil
}

func (m *Memory) listWorkloads(keep func(Workload) bool, less func(a, b Workload) bool) []Workload {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []Workload{}
	for _, w := range m.s.workloads {
		if keep(w) {
			out = append(out, m.view(w))
		}
	}
	sort.Slice(out, func(i, j int) bool { return less(out[i], out[j]) })
	return out
}

func byName(a, b Workload) bool { return a.Name < b.Name }

func (m *Memory) ListWorkloads(_ context.Context) ([]Workload, error) {
	return m.listWorkloads(func(Workload) bool { return true }, byName), nil
}

func (m *Memory) ListWorkloadsForNode(_ context.Context, nodeID string) ([]Workload, error) {
	return m.listWorkloads(func(w Workload) bool { return w.NodeID == nodeID }, byName), nil
}

func (m *Memory) ListWorkloadsForService(_ context.Context, serviceID string) ([]Workload, error) {
	return m.listWorkloads(
		func(w Workload) bool { return w.ServiceID == serviceID },
		func(a, b Workload) bool {
			if a.ServiceRevision != b.ServiceRevision {
				return a.ServiceRevision < b.ServiceRevision
			}
			return a.ReplicaIndex < b.ReplicaIndex
		}), nil
}

func (m *Memory) SetWorkloadDesiredState(_ context.Context, id, desired string, at time.Time) (Workload, error) {
	if !ValidDesiredState(desired) {
		return Workload{}, fmt.Errorf("%w: desired state %q must be running or stopped", ErrInvalid, desired)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	w, ok := m.s.workloads[id]
	if !ok {
		return Workload{}, ErrNotFound
	}
	if w.DesiredState == desired {
		return m.view(w), ErrUnchanged
	}
	w.DesiredState = desired
	w.Generation++
	w.UpdatedAt = at.UTC()
	m.s.workloads[id] = w
	return m.view(w), nil
}

func (m *Memory) DeleteWorkload(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	w, ok := m.s.workloads[id]
	if !ok {
		return ErrNotFound
	}
	if w.DesiredState != DesiredStopped {
		return fmt.Errorf("%w: workload %s is still meant to run; stop it first", ErrInvalid, id)
	}
	delete(m.s.workloads, id)
	delete(m.s.observations, id)
	return nil
}

func (m *Memory) RecordObservations(_ context.Context, nodeID string, obs []Observation) error {
	for _, o := range obs {
		if !ValidObservedState(o.State) {
			return fmt.Errorf("%w: observed state %q of workload %s", ErrInvalid, o.State, o.WorkloadID)
		}
		if o.ObservedAt.IsZero() {
			return fmt.Errorf("%w: observation of workload %s has no time", ErrInvalid, o.WorkloadID)
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, o := range obs {
		w, ok := m.s.workloads[o.WorkloadID]
		if !ok || w.NodeID != nodeID {
			continue
		}
		if stored, ok := m.s.observations[o.WorkloadID]; ok && stored.ObservedAt.After(o.ObservedAt) {
			continue
		}
		o = cloneObservation(o)
		o.NodeID = nodeID
		o.ObservedAt = o.ObservedAt.UTC()
		o.StartedAt, o.FinishedAt = utcPtr(o.StartedAt), utcPtr(o.FinishedAt)
		m.s.observations[o.WorkloadID] = o
	}
	return nil
}

// ---- services ----

func (m *Memory) CreateService(_ context.Context, n NewService) (Service, error) {
	if err := n.validate(); err != nil {
		return Service{}, err
	}
	at := n.At.UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, existing := range m.s.services {
		if existing.ID == n.ID || (existing.Name == n.Name && existing.DeletedAt == nil) {
			return Service{}, ErrConflict
		}
	}
	svc := Service{ID: n.ID, Name: n.Name, Replicas: n.Replicas, CurrentRevision: 1, Version: 1, CreatedAt: at, UpdatedAt: at}
	m.s.services[svc.ID] = svc
	m.s.revisions[svc.ID] = []Revision{{
		ServiceID: svc.ID, Revision: 1, Spec: cloneSpec(n.Spec), SpecHash: n.Spec.Hash(),
		Message: n.Message, CreatedBy: n.CreatedBy, CreatedAt: at,
	}}
	return cloneService(svc), nil
}

func (m *Memory) GetService(_ context.Context, id string) (Service, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	svc, ok := m.s.services[id]
	if !ok {
		return Service{}, ErrNotFound
	}
	return cloneService(svc), nil
}

func (m *Memory) GetServiceByName(_ context.Context, name string) (Service, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, svc := range m.s.services {
		if svc.Name == name && svc.DeletedAt == nil {
			return cloneService(svc), nil
		}
	}
	return Service{}, ErrNotFound
}

func (m *Memory) ListServices(_ context.Context, includeDeleted bool) ([]Service, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []Service{}
	for _, svc := range m.s.services {
		if includeDeleted || svc.DeletedAt == nil {
			out = append(out, cloneService(svc))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

func (m *Memory) ListDefinitions(ctx context.Context, includeDeleted bool) ([]Definition, error) {
	services, err := m.ListServices(ctx, includeDeleted)
	if err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Definition, 0, len(services))
	for _, svc := range services {
		out = append(out, Definition{Service: svc, Revision: cloneRevision(m.s.revisions[svc.ID][svc.CurrentRevision-1])})
	}
	return out, nil
}

// mutable returns a live service for the caller to change under m.mu, or the
// error a failed optimistic update reports.
func (m *Memory) mutable(id string, expectedVersion int64) (Service, error) {
	svc, ok := m.s.services[id]
	if !ok || svc.DeletedAt != nil {
		return Service{}, ErrNotFound
	}
	if expectedVersion != 0 && svc.Version != expectedVersion {
		return Service{}, ErrVersionConflict
	}
	return svc, nil
}

func (m *Memory) ScaleService(_ context.Context, id string, replicas int, expectedVersion int64, at time.Time) (Service, error) {
	if replicas < 0 {
		return Service{}, fmt.Errorf("%w: replicas must not be negative", ErrInvalid)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	svc, err := m.mutable(id, expectedVersion)
	if err != nil {
		return Service{}, err
	}
	if svc.Replicas == replicas {
		return cloneService(svc), ErrUnchanged
	}
	svc.Replicas = replicas
	svc.Version++
	svc.UpdatedAt = at.UTC()
	m.s.services[id] = svc
	return cloneService(svc), nil
}

func (m *Memory) DeployService(_ context.Context, id string, d Deployment) (Service, Revision, error) {
	if err := d.Spec.validate(); err != nil {
		return Service{}, Revision{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	svc, err := m.mutable(id, d.ExpectedVersion)
	if err != nil {
		return Service{}, Revision{}, err
	}
	revs := m.s.revisions[id]
	current := revs[svc.CurrentRevision-1]
	if current.SpecHash == d.Spec.Hash() {
		return cloneService(svc), cloneRevision(current), ErrUnchanged
	}
	at := d.At.UTC()
	rev := Revision{
		ServiceID: id, Revision: len(revs) + 1, Spec: cloneSpec(d.Spec), SpecHash: d.Spec.Hash(),
		Message: d.Message, CreatedBy: d.CreatedBy, CreatedAt: at,
	}
	m.s.revisions[id] = append(revs, rev)
	svc.CurrentRevision = rev.Revision
	svc.Version++
	svc.UpdatedAt = at
	m.s.services[id] = svc
	return cloneService(svc), cloneRevision(rev), nil
}

func (m *Memory) DeleteService(_ context.Context, id string, expectedVersion int64, at time.Time) (Service, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	svc, err := m.mutable(id, expectedVersion)
	if err != nil {
		return Service{}, err
	}
	deleted := at.UTC()
	svc.DeletedAt = &deleted
	svc.Replicas = 0
	svc.Version++
	svc.UpdatedAt = deleted
	m.s.services[id] = svc
	return cloneService(svc), nil
}

func (m *Memory) GetRevision(_ context.Context, serviceID string, revision int) (Revision, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	revs := m.s.revisions[serviceID]
	if revision < 1 || revision > len(revs) {
		return Revision{}, ErrNotFound
	}
	return cloneRevision(revs[revision-1]), nil
}

func (m *Memory) ListRevisions(_ context.Context, serviceID string) ([]Revision, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Revision, 0, len(m.s.revisions[serviceID]))
	for _, r := range m.s.revisions[serviceID] {
		out = append(out, cloneRevision(r))
	}
	return out, nil
}

// ---- scheduling ----

func (u Unschedulable) key() slotKey {
	return slotKey{u.ServiceID, u.Revision, u.ReplicaIndex}
}

// validate checks the fields a record needs, whatever the store.
func (u Unschedulable) validate() error {
	if u.ServiceID == "" || u.Revision < 1 || u.ReplicaIndex < 1 || u.Code == "" {
		return fmt.Errorf("%w: an unschedulable record needs a service, a revision and replica index of at least 1, and a code", ErrInvalid)
	}
	return nil
}

func sortUnschedulable(list []Unschedulable) {
	sort.Slice(list, func(i, j int) bool {
		a, b := list[i], list[j]
		if a.ServiceID != b.ServiceID {
			return a.ServiceID < b.ServiceID
		}
		if a.Revision != b.Revision {
			return a.Revision < b.Revision
		}
		return a.ReplicaIndex < b.ReplicaIndex
	})
}

func (m *Memory) ReplaceUnschedulable(_ context.Context, items []Unschedulable, at time.Time) ([]Unschedulable, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	next := make(map[slotKey]Unschedulable, len(items))
	var changed []Unschedulable
	for _, item := range items {
		if err := item.validate(); err != nil {
			return nil, err
		}
		key := item.key()
		if _, dup := next[key]; dup {
			return nil, fmt.Errorf("%w: replica %d of revision %d of service %s is listed twice", ErrInvalid, key.index, key.revision, key.serviceID)
		}
		if item.Revision > len(m.s.revisions[item.ServiceID]) {
			return nil, fmt.Errorf("%w: revision %d of service %s", ErrNotFound, item.Revision, item.ServiceID)
		}
		item = cloneUnschedulable(item)
		if old, ok := m.s.unschedulable[key]; ok && old.SameReason(item) {
			next[key] = old
			continue
		}
		item.Since = at.UTC()
		next[key] = item
		changed = append(changed, cloneUnschedulable(item))
	}
	m.s.unschedulable = next
	sortUnschedulable(changed)
	return changed, nil
}

func (m *Memory) ListUnschedulable(_ context.Context, serviceID string) ([]Unschedulable, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []Unschedulable{}
	for key, u := range m.s.unschedulable {
		if serviceID == "" || key.serviceID == serviceID {
			out = append(out, cloneUnschedulable(u))
		}
	}
	sortUnschedulable(out)
	return out, nil
}

// ---- events ----

func (m *Memory) AppendEvent(_ context.Context, e Event) (Event, error) {
	if e.Action == "" || e.ResourceType == "" || e.Actor == "" {
		return Event{}, fmt.Errorf("%w: an event needs an actor, action and resource type", ErrInvalid)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	e = cloneEvent(e)
	if e.Details == nil {
		e.Details = map[string]string{}
	}
	e.ID = m.s.nextEventID
	m.s.nextEventID++
	e.At = e.At.UTC()
	m.s.events = append(m.s.events, e)
	return cloneEvent(e), nil
}

func (m *Memory) ListEvents(_ context.Context, f EventFilter) ([]Event, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []Event{}
	for i := len(m.s.events) - 1; i >= 0 && len(out) < f.limit(); i-- {
		e := m.s.events[i]
		switch {
		case f.ResourceType != "" && e.ResourceType != f.ResourceType:
		case f.ResourceID != "" && e.ResourceID != f.ResourceID:
		case f.BeforeID != 0 && e.ID >= f.BeforeID:
		default:
			out = append(out, cloneEvent(e))
		}
	}
	return out, nil
}

// ---- leases ----

func (m *Memory) AcquireLease(_ context.Context, name, holder string, ttl time.Duration, now time.Time) (Lease, error) {
	if !ValidName(name) || !ValidName(holder) || ttl <= 0 {
		return Lease{}, fmt.Errorf("%w: a lease needs a valid name, holder and a positive ttl", ErrInvalid)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	lease, held := m.s.leases[name]
	live := held && lease.ExpiresAt.After(now)
	switch {
	case live && lease.Holder != holder:
		return Lease{}, ErrLeaseHeld
	case live:
		// Same holder renewing: the token stays.
	case held:
		lease.Token++
	default:
		lease.Token = 1
	}
	lease.Name, lease.Holder, lease.ExpiresAt = name, holder, now.Add(ttl).UTC()
	m.s.leases[name] = lease
	return lease, nil
}

func (m *Memory) ReleaseLease(_ context.Context, name, holder string, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	lease, ok := m.s.leases[name]
	if !ok || lease.Holder != holder {
		return ErrNotFound
	}
	lease.ExpiresAt = now.UTC()
	m.s.leases[name] = lease
	return nil
}

func (m *Memory) GetLease(_ context.Context, name string) (Lease, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	lease, ok := m.s.leases[name]
	if !ok {
		return Lease{}, ErrNotFound
	}
	return lease, nil
}
