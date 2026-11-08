package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// ---- workloads (desired) joined with observations (observed) ----

const workloadSelect = `SELECT w.id, w.name, w.image, w.cpu_millis, w.memory_bytes, w.node_id, w.desired_state,
	w.service_id, w.service_revision, w.replica_index, w.created_at, w.updated_at,
	o.workload_id, o.node_id, o.state, o.message, o.container_id, o.exit_code, o.oom_killed, o.restart_count,
	o.started_at, o.finished_at, o.observed_at
	FROM workloads w LEFT JOIN workload_observations o ON o.workload_id = w.id`

func scanWorkload(row pgx.Row) (Workload, error) {
	var w Workload
	var (
		serviceID, obsWorkload, obsNode, obsState, obsMessage, obsContainer *string
		revision, replica, obsExit, obsRestarts                             *int
		obsOOM                                                              *bool
		obsStarted, obsFinished, obsAt                                      *time.Time
	)
	err := row.Scan(&w.ID, &w.Name, &w.Image, &w.CPUMillis, &w.MemoryBytes, &w.NodeID, &w.DesiredState,
		&serviceID, &revision, &replica, &w.CreatedAt, &w.UpdatedAt,
		&obsWorkload, &obsNode, &obsState, &obsMessage, &obsContainer, &obsExit, &obsOOM, &obsRestarts,
		&obsStarted, &obsFinished, &obsAt)
	if err != nil {
		return Workload{}, mapError(err)
	}
	if serviceID != nil {
		w.ServiceID, w.ServiceRevision, w.ReplicaIndex = *serviceID, *revision, *replica
	}
	w.CreatedAt, w.UpdatedAt = w.CreatedAt.UTC(), w.UpdatedAt.UTC()
	if obsWorkload != nil {
		w.Observed = &Observation{
			WorkloadID:   *obsWorkload,
			NodeID:       *obsNode,
			State:        *obsState,
			Message:      *obsMessage,
			ContainerID:  *obsContainer,
			ExitCode:     obsExit,
			OOMKilled:    *obsOOM,
			RestartCount: *obsRestarts,
			StartedAt:    utcPtr(obsStarted),
			FinishedAt:   utcPtr(obsFinished),
			ObservedAt:   obsAt.UTC(),
		}
	}
	return w, nil
}

func (q *queries) queryWorkloads(ctx context.Context, suffix string, args ...any) ([]Workload, error) {
	rows, err := q.db.Query(ctx, workloadSelect+suffix, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Workload{}
	for rows.Next() {
		w, err := scanWorkload(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

func (q *queries) CreateWorkload(ctx context.Context, w Workload) (Workload, error) {
	w, err := w.prepareForCreate()
	if err != nil {
		return Workload{}, err
	}
	_, err = q.db.Exec(ctx,
		`INSERT INTO workloads (id, name, image, cpu_millis, memory_bytes, node_id, desired_state,
		                        service_id, service_revision, replica_index, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		w.ID, w.Name, w.Image, w.CPUMillis, w.MemoryBytes, w.NodeID, w.DesiredState,
		nilIfZero(w.ServiceID), nilIfZero(w.ServiceRevision), nilIfZero(w.ReplicaIndex), w.CreatedAt, w.UpdatedAt)
	if err != nil {
		return Workload{}, mapError(err)
	}
	return w, nil
}

func (q *queries) GetWorkload(ctx context.Context, id string) (Workload, error) {
	return scanWorkload(q.db.QueryRow(ctx, workloadSelect+` WHERE w.id = $1`, id))
}

func (q *queries) ListWorkloads(ctx context.Context) ([]Workload, error) {
	return q.queryWorkloads(ctx, ` ORDER BY w.name`)
}

func (q *queries) ListWorkloadsForNode(ctx context.Context, nodeID string) ([]Workload, error) {
	return q.queryWorkloads(ctx, ` WHERE w.node_id = $1 ORDER BY w.name`, nodeID)
}

func (q *queries) ListWorkloadsForService(ctx context.Context, serviceID string) ([]Workload, error) {
	return q.queryWorkloads(ctx, ` WHERE w.service_id = $1 ORDER BY w.service_revision, w.replica_index`, serviceID)
}

func (q *queries) SetWorkloadDesiredState(ctx context.Context, id, desired string, at time.Time) (Workload, error) {
	if !ValidDesiredState(desired) {
		return Workload{}, fmt.Errorf("%w: desired state %q must be running or stopped", ErrInvalid, desired)
	}
	tag, err := q.db.Exec(ctx, `UPDATE workloads SET desired_state = $2, updated_at = $3 WHERE id = $1 AND desired_state <> $2`, id, desired, at.UTC())
	if err != nil {
		return Workload{}, mapError(err)
	}
	w, err := q.GetWorkload(ctx, id)
	if err != nil {
		return Workload{}, err
	}
	if tag.RowsAffected() == 0 {
		return w, ErrUnchanged
	}
	return w, nil
}

func (q *queries) RecordObservations(ctx context.Context, nodeID string, obs []Observation) error {
	for _, o := range obs {
		if !ValidObservedState(o.State) {
			return fmt.Errorf("%w: observed state %q of workload %s", ErrInvalid, o.State, o.WorkloadID)
		}
		if o.ObservedAt.IsZero() {
			return fmt.Errorf("%w: observation of workload %s has no time", ErrInvalid, o.WorkloadID)
		}
	}
	if len(obs) == 0 {
		return nil
	}
	return q.atomic(ctx, func(q *queries) error {
		for _, o := range obs {
			// Only a workload placed on the reporting node takes the report, and
			// only a report not older than the stored one replaces it.
			_, err := q.db.Exec(ctx,
				`INSERT INTO workload_observations
				     (workload_id, node_id, state, message, container_id, exit_code, oom_killed, restart_count,
				      started_at, finished_at, observed_at)
				 SELECT w.id, w.node_id, $3::text, $4::text, $5::text, $6::integer, $7::boolean, $8::integer,
				        $9::timestamptz, $10::timestamptz, $11::timestamptz
				 FROM workloads w
				 WHERE w.id = $1 AND w.node_id = $2
				 ON CONFLICT (workload_id) DO UPDATE SET
				     node_id = EXCLUDED.node_id,
				     state = EXCLUDED.state,
				     message = EXCLUDED.message,
				     container_id = EXCLUDED.container_id,
				     exit_code = EXCLUDED.exit_code,
				     oom_killed = EXCLUDED.oom_killed,
				     restart_count = EXCLUDED.restart_count,
				     started_at = EXCLUDED.started_at,
				     finished_at = EXCLUDED.finished_at,
				     observed_at = EXCLUDED.observed_at
				 WHERE workload_observations.observed_at <= EXCLUDED.observed_at`,
				o.WorkloadID, nodeID, o.State, o.Message, o.ContainerID, o.ExitCode, o.OOMKilled, o.RestartCount,
				utcPtr(o.StartedAt), utcPtr(o.FinishedAt), o.ObservedAt.UTC())
			if err != nil {
				return mapError(err)
			}
		}
		return nil
	})
}

// ---- services and revisions (desired) ----

const serviceColumns = `id, name, replicas, current_revision, version, created_at, updated_at, deleted_at`

func scanService(row pgx.Row) (Service, error) {
	var s Service
	if err := row.Scan(&s.ID, &s.Name, &s.Replicas, &s.CurrentRevision, &s.Version, &s.CreatedAt, &s.UpdatedAt, &s.DeletedAt); err != nil {
		return Service{}, mapError(err)
	}
	s.CreatedAt, s.UpdatedAt, s.DeletedAt = s.CreatedAt.UTC(), s.UpdatedAt.UTC(), utcPtr(s.DeletedAt)
	return s, nil
}

const revisionColumns = `service_id, revision, spec, spec_hash, message, created_by, created_at`

func scanRevision(row pgx.Row) (Revision, error) {
	var r Revision
	var spec []byte
	if err := row.Scan(&r.ServiceID, &r.Revision, &spec, &r.SpecHash, &r.Message, &r.CreatedBy, &r.CreatedAt); err != nil {
		return Revision{}, mapError(err)
	}
	if err := json.Unmarshal(spec, &r.Spec); err != nil {
		return Revision{}, fmt.Errorf("decode revision %d of service %s: %w", r.Revision, r.ServiceID, err)
	}
	r.CreatedAt = r.CreatedAt.UTC()
	return r, nil
}

func (q *queries) CreateService(ctx context.Context, n NewService) (Service, error) {
	if err := n.validate(); err != nil {
		return Service{}, err
	}
	spec, err := marshalJSON(n.Spec)
	if err != nil {
		return Service{}, err
	}
	at := n.At.UTC()
	var svc Service
	err = q.atomic(ctx, func(q *queries) error {
		var err error
		svc, err = scanService(q.db.QueryRow(ctx,
			`INSERT INTO services (id, name, replicas, current_revision, version, created_at, updated_at)
			 VALUES ($1, $2, $3, 1, 1, $4, $4)
			 RETURNING `+serviceColumns, n.ID, n.Name, n.Replicas, at))
		if err != nil {
			return err
		}
		_, err = q.db.Exec(ctx,
			`INSERT INTO service_revisions (service_id, revision, spec, spec_hash, message, created_by, created_at)
			 VALUES ($1, 1, $2, $3, $4, $5, $6)`,
			n.ID, spec, n.Spec.Hash(), n.Message, n.CreatedBy, at)
		return mapError(err)
	})
	if err != nil {
		return Service{}, err
	}
	return svc, nil
}

func (q *queries) GetService(ctx context.Context, id string) (Service, error) {
	return scanService(q.db.QueryRow(ctx, `SELECT `+serviceColumns+` FROM services WHERE id = $1`, id))
}

func (q *queries) GetServiceByName(ctx context.Context, name string) (Service, error) {
	return scanService(q.db.QueryRow(ctx, `SELECT `+serviceColumns+` FROM services WHERE name = $1 AND deleted_at IS NULL`, name))
}

func (q *queries) ListServices(ctx context.Context, includeDeleted bool) ([]Service, error) {
	query := `SELECT ` + serviceColumns + ` FROM services`
	if !includeDeleted {
		query += ` WHERE deleted_at IS NULL`
	}
	rows, err := q.db.Query(ctx, query+` ORDER BY name, created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Service{}
	for rows.Next() {
		s, err := scanService(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (q *queries) ListDefinitions(ctx context.Context, includeDeleted bool) ([]Definition, error) {
	// The join follows services.current_revision to the revision's primary key.
	query := `SELECT s.id, s.name, s.replicas, s.current_revision, s.version, s.created_at, s.updated_at, s.deleted_at,
		r.service_id, r.revision, r.spec, r.spec_hash, r.message, r.created_by, r.created_at
		FROM services s JOIN service_revisions r ON r.service_id = s.id AND r.revision = s.current_revision`
	if !includeDeleted {
		query += ` WHERE s.deleted_at IS NULL`
	}
	rows, err := q.db.Query(ctx, query+` ORDER BY s.name, s.created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Definition{}
	for rows.Next() {
		var d Definition
		var spec []byte
		s, r := &d.Service, &d.Revision
		if err := rows.Scan(&s.ID, &s.Name, &s.Replicas, &s.CurrentRevision, &s.Version, &s.CreatedAt, &s.UpdatedAt, &s.DeletedAt,
			&r.ServiceID, &r.Revision, &spec, &r.SpecHash, &r.Message, &r.CreatedBy, &r.CreatedAt); err != nil {
			return nil, mapError(err)
		}
		if err := json.Unmarshal(spec, &r.Spec); err != nil {
			return nil, fmt.Errorf("decode revision %d of service %s: %w", r.Revision, r.ServiceID, err)
		}
		s.CreatedAt, s.UpdatedAt, s.DeletedAt = s.CreatedAt.UTC(), s.UpdatedAt.UTC(), utcPtr(s.DeletedAt)
		r.CreatedAt = r.CreatedAt.UTC()
		out = append(out, d)
	}
	return out, rows.Err()
}

// explainNoUpdate says why a conditional UPDATE changed no row.
func (q *queries) explainNoUpdate(ctx context.Context, id string, expectedVersion int64) (Service, error) {
	svc, err := q.GetService(ctx, id)
	switch {
	case err != nil:
		return Service{}, err
	case svc.DeletedAt != nil:
		return Service{}, ErrNotFound
	case expectedVersion != 0 && svc.Version != expectedVersion:
		return Service{}, ErrVersionConflict
	}
	return svc, ErrUnchanged
}

func (q *queries) ScaleService(ctx context.Context, id string, replicas int, expectedVersion int64, at time.Time) (Service, error) {
	if replicas < 0 {
		return Service{}, fmt.Errorf("%w: replicas must not be negative", ErrInvalid)
	}
	svc, err := scanService(q.db.QueryRow(ctx,
		`UPDATE services SET replicas = $2, version = version + 1, updated_at = $4
		 WHERE id = $1 AND deleted_at IS NULL AND replicas <> $2 AND ($3::bigint = 0 OR version = $3::bigint)
		 RETURNING `+serviceColumns, id, replicas, expectedVersion, at.UTC()))
	if errors.Is(err, ErrNotFound) {
		return q.explainNoUpdate(ctx, id, expectedVersion)
	}
	return svc, err
}

func (q *queries) DeleteService(ctx context.Context, id string, expectedVersion int64, at time.Time) (Service, error) {
	svc, err := scanService(q.db.QueryRow(ctx,
		`UPDATE services SET deleted_at = $3, replicas = 0, version = version + 1, updated_at = $3
		 WHERE id = $1 AND deleted_at IS NULL AND ($2::bigint = 0 OR version = $2::bigint)
		 RETURNING `+serviceColumns, id, expectedVersion, at.UTC()))
	if errors.Is(err, ErrNotFound) {
		svc, err = q.explainNoUpdate(ctx, id, expectedVersion)
		if errors.Is(err, ErrUnchanged) {
			// Not deleted and the version matches, yet nothing changed: a concurrent change slipped in.
			return Service{}, ErrVersionConflict
		}
	}
	return svc, err
}

func (q *queries) DeployService(ctx context.Context, id string, d Deployment) (Service, Revision, error) {
	if err := d.Spec.validate(); err != nil {
		return Service{}, Revision{}, err
	}
	spec, err := marshalJSON(d.Spec)
	if err != nil {
		return Service{}, Revision{}, err
	}
	at := d.At.UTC()
	var svc Service
	var rev Revision
	err = q.atomic(ctx, func(q *queries) error {
		// The row lock serializes concurrent deployments, so revision
		// numbers are assigned one after the other.
		cur, err := scanService(q.db.QueryRow(ctx, `SELECT `+serviceColumns+` FROM services WHERE id = $1 FOR UPDATE`, id))
		if err != nil {
			return err
		}
		switch {
		case cur.DeletedAt != nil:
			return ErrNotFound
		case d.ExpectedVersion != 0 && cur.Version != d.ExpectedVersion:
			return ErrVersionConflict
		}
		current, err := scanRevision(q.db.QueryRow(ctx, `SELECT `+revisionColumns+` FROM service_revisions WHERE service_id = $1 AND revision = $2`, id, cur.CurrentRevision))
		if err != nil {
			return err
		}
		if current.SpecHash == d.Spec.Hash() {
			svc, rev = cur, current
			return ErrUnchanged
		}
		next := cur.CurrentRevision + 1
		if _, err := q.db.Exec(ctx,
			`INSERT INTO service_revisions (service_id, revision, spec, spec_hash, message, created_by, created_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			id, next, spec, d.Spec.Hash(), d.Message, d.CreatedBy, at); err != nil {
			return mapError(err)
		}
		rev = Revision{ServiceID: id, Revision: next, Spec: d.Spec, SpecHash: d.Spec.Hash(), Message: d.Message, CreatedBy: d.CreatedBy, CreatedAt: at}
		svc, err = scanService(q.db.QueryRow(ctx,
			`UPDATE services SET current_revision = $2, version = version + 1, updated_at = $3
			 WHERE id = $1 RETURNING `+serviceColumns, id, next, at))
		return err
	})
	if err != nil {
		if errors.Is(err, ErrUnchanged) {
			return svc, rev, ErrUnchanged
		}
		return Service{}, Revision{}, err
	}
	return svc, rev, nil
}

func (q *queries) GetRevision(ctx context.Context, serviceID string, revision int) (Revision, error) {
	return scanRevision(q.db.QueryRow(ctx, `SELECT `+revisionColumns+` FROM service_revisions WHERE service_id = $1 AND revision = $2`, serviceID, revision))
}

func (q *queries) ListRevisions(ctx context.Context, serviceID string) ([]Revision, error) {
	rows, err := q.db.Query(ctx, `SELECT `+revisionColumns+` FROM service_revisions WHERE service_id = $1 ORDER BY revision`, serviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Revision{}
	for rows.Next() {
		r, err := scanRevision(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ---- events ----

const eventColumns = `id, at, instance_id, actor, action, resource_type, resource_id, message, details`

func (q *queries) AppendEvent(ctx context.Context, e Event) (Event, error) {
	if e.Action == "" || e.ResourceType == "" || e.Actor == "" {
		return Event{}, fmt.Errorf("%w: an event needs an actor, action and resource type", ErrInvalid)
	}
	details, err := marshalJSON(cloneLabels(e.Details))
	if err != nil {
		return Event{}, err
	}
	e.At = e.At.UTC()
	err = q.db.QueryRow(ctx,
		`INSERT INTO events (at, instance_id, actor, action, resource_type, resource_id, message, details)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`,
		e.At, e.InstanceID, e.Actor, e.Action, e.ResourceType, e.ResourceID, e.Message, details).Scan(&e.ID)
	if err != nil {
		return Event{}, mapError(err)
	}
	e.Details = cloneLabels(e.Details)
	return e, nil
}

func (q *queries) ListEvents(ctx context.Context, f EventFilter) ([]Event, error) {
	// Only the filters that are set become conditions, so the planner can use
	// events_resource_idx for resource queries and the primary key otherwise.
	var conds []string
	var args []any
	add := func(cond string, v any) {
		args = append(args, v)
		conds = append(conds, strings.Replace(cond, "?", "$"+strconv.Itoa(len(args)), 1))
	}
	if f.ResourceType != "" {
		add("resource_type = ?", f.ResourceType)
	}
	if f.ResourceID != "" {
		add("resource_id = ?", f.ResourceID)
	}
	if f.BeforeID != 0 {
		add("id < ?", f.BeforeID)
	}
	query := `SELECT ` + eventColumns + ` FROM events`
	if len(conds) > 0 {
		query += ` WHERE ` + strings.Join(conds, ` AND `)
	}
	query += ` ORDER BY id DESC LIMIT ` + strconv.Itoa(f.limit())

	rows, err := q.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Event{}
	for rows.Next() {
		var e Event
		var details []byte
		if err := rows.Scan(&e.ID, &e.At, &e.InstanceID, &e.Actor, &e.Action, &e.ResourceType, &e.ResourceID, &e.Message, &details); err != nil {
			return nil, mapError(err)
		}
		if err := json.Unmarshal(details, &e.Details); err != nil {
			return nil, fmt.Errorf("decode event details: %w", err)
		}
		e.At = e.At.UTC()
		out = append(out, e)
	}
	return out, rows.Err()
}

// ---- leases ----

const leaseColumns = `name, holder, token, expires_at`

func scanLease(row pgx.Row) (Lease, error) {
	var l Lease
	if err := row.Scan(&l.Name, &l.Holder, &l.Token, &l.ExpiresAt); err != nil {
		return Lease{}, mapError(err)
	}
	l.ExpiresAt = l.ExpiresAt.UTC()
	return l, nil
}

func (q *queries) AcquireLease(ctx context.Context, name, holder string, ttl time.Duration, now time.Time) (Lease, error) {
	if !ValidName(name) || !ValidName(holder) || ttl <= 0 {
		return Lease{}, fmt.Errorf("%w: a lease needs a valid name, holder and a positive ttl", ErrInvalid)
	}
	// One statement decides: a free or expired lease is taken (token + 1), the
	// holder's own live lease is renewed (token kept), another holder's live
	// lease matches the WHERE clause not at all and returns no row.
	lease, err := scanLease(q.db.QueryRow(ctx,
		`INSERT INTO leases (name, holder, token, expires_at)
		 VALUES ($1, $2, 1, $4)
		 ON CONFLICT (name) DO UPDATE SET
		     token = CASE WHEN leases.holder = EXCLUDED.holder AND leases.expires_at > $3 THEN leases.token ELSE leases.token + 1 END,
		     holder = EXCLUDED.holder,
		     expires_at = EXCLUDED.expires_at
		 WHERE leases.holder = EXCLUDED.holder OR leases.expires_at <= $3
		 RETURNING `+leaseColumns,
		name, holder, now.UTC(), now.Add(ttl).UTC()))
	if errors.Is(err, ErrNotFound) {
		return Lease{}, ErrLeaseHeld
	}
	return lease, err
}

func (q *queries) ReleaseLease(ctx context.Context, name, holder string, now time.Time) error {
	tag, err := q.db.Exec(ctx, `UPDATE leases SET expires_at = $3 WHERE name = $1 AND holder = $2`, name, holder, now.UTC())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (q *queries) GetLease(ctx context.Context, name string) (Lease, error) {
	return scanLease(q.db.QueryRow(ctx, `SELECT `+leaseColumns+` FROM leases WHERE name = $1`, name))
}
