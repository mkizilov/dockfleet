package store

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// migrationLockID serializes concurrent managers applying migrations.
const migrationLockID int64 = 0x646f636b666c74

const (
	nodeColumns = `id, name, cpu_millis, memory_bytes, labels, agent_version, registered_at, last_heartbeat`
	// workloadInsertColumns are the columns a new workload sets; the status
	// columns are written later by agent reports.
	workloadInsertColumns = `id, name, image, cpu_millis, memory_bytes, node_id, state, created_at`
	workloadColumns       = `id, name, image, cpu_millis, memory_bytes, node_id, state, created_at, status_message, status_updated_at`
)

// Postgres is the PostgreSQL-backed Store.
type Postgres struct {
	pool *pgxpool.Pool
}

var _ Store = (*Postgres)(nil)

// OpenPostgres connects to PostgreSQL and verifies the connection.
func OpenPostgres(ctx context.Context, url string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("open postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

// Migrate applies embedded migrations that are not yet recorded in schema_migrations.
func (p *Postgres) Migrate(ctx context.Context) error {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("lock migrations: %w", err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    text PRIMARY KEY,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	for _, e := range entries {
		var applied bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, e.Name()).Scan(&applied); err != nil {
			return fmt.Errorf("check migration %s: %w", e.Name(), err)
		}
		if applied {
			continue
		}
		body, err := migrationFS.ReadFile("migrations/" + e.Name())
		if err != nil {
			return fmt.Errorf("read migration %s: %w", e.Name(), err)
		}
		if _, err := tx.Exec(ctx, string(body)); err != nil {
			return fmt.Errorf("apply migration %s: %w", e.Name(), err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, e.Name()); err != nil {
			return fmt.Errorf("record migration %s: %w", e.Name(), err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}
	return nil
}

func mapError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return ErrConflict
		case "23503":
			return ErrNotFound
		}
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

func scanNode(row pgx.Row) (Node, error) {
	var n Node
	var labels []byte
	err := row.Scan(&n.ID, &n.Name, &n.CPUMillis, &n.MemoryBytes, &labels, &n.AgentVersion, &n.RegisteredAt, &n.LastHeartbeat)
	if err != nil {
		return Node{}, mapError(err)
	}
	if err := json.Unmarshal(labels, &n.Labels); err != nil {
		return Node{}, fmt.Errorf("decode node labels: %w", err)
	}
	if n.Labels == nil {
		n.Labels = map[string]string{}
	}
	n.RegisteredAt = n.RegisteredAt.UTC()
	n.LastHeartbeat = n.LastHeartbeat.UTC()
	return n, nil
}

func scanWorkload(row pgx.Row) (Workload, error) {
	var w Workload
	err := row.Scan(&w.ID, &w.Name, &w.Image, &w.CPUMillis, &w.MemoryBytes, &w.NodeID, &w.State, &w.CreatedAt, &w.StatusMessage, &w.StatusUpdatedAt)
	if err != nil {
		return Workload{}, mapError(err)
	}
	w.CreatedAt = w.CreatedAt.UTC()
	if w.StatusUpdatedAt != nil {
		utc := w.StatusUpdatedAt.UTC()
		w.StatusUpdatedAt = &utc
	}
	return w, nil
}

func (p *Postgres) UpsertNode(ctx context.Context, n Node) (Node, error) {
	labels, err := json.Marshal(cloneLabels(n.Labels))
	if err != nil {
		return Node{}, fmt.Errorf("encode node labels: %w", err)
	}
	return scanNode(p.pool.QueryRow(ctx,
		`INSERT INTO nodes (`+nodeColumns+`)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (id) DO UPDATE SET
		   name = EXCLUDED.name,
		   cpu_millis = EXCLUDED.cpu_millis,
		   memory_bytes = EXCLUDED.memory_bytes,
		   labels = EXCLUDED.labels,
		   agent_version = EXCLUDED.agent_version,
		   last_heartbeat = EXCLUDED.last_heartbeat
		 RETURNING `+nodeColumns,
		n.ID, n.Name, n.CPUMillis, n.MemoryBytes, labels, n.AgentVersion, n.RegisteredAt, n.LastHeartbeat))
}

func (p *Postgres) Heartbeat(ctx context.Context, id string, at time.Time) error {
	tag, err := p.pool.Exec(ctx, `UPDATE nodes SET last_heartbeat = $2 WHERE id = $1`, id, at)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (p *Postgres) GetNode(ctx context.Context, id string) (Node, error) {
	return scanNode(p.pool.QueryRow(ctx, `SELECT `+nodeColumns+` FROM nodes WHERE id = $1`, id))
}

func (p *Postgres) ListNodes(ctx context.Context) ([]Node, error) {
	rows, err := p.pool.Query(ctx, `SELECT `+nodeColumns+` FROM nodes ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Node{}
	for rows.Next() {
		n, err := scanNode(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func (p *Postgres) CreateWorkload(ctx context.Context, w Workload) (Workload, error) {
	return scanWorkload(p.pool.QueryRow(ctx,
		`INSERT INTO workloads (`+workloadInsertColumns+`)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 RETURNING `+workloadColumns,
		w.ID, w.Name, w.Image, w.CPUMillis, w.MemoryBytes, w.NodeID, w.State, w.CreatedAt))
}

func (p *Postgres) GetWorkload(ctx context.Context, id string) (Workload, error) {
	return scanWorkload(p.pool.QueryRow(ctx, `SELECT `+workloadColumns+` FROM workloads WHERE id = $1`, id))
}

func (p *Postgres) ListWorkloads(ctx context.Context) ([]Workload, error) {
	rows, err := p.pool.Query(ctx, `SELECT `+workloadColumns+` FROM workloads ORDER BY name`)
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

func (p *Postgres) ListWorkloadsForNode(ctx context.Context, nodeID string) ([]Workload, error) {
	rows, err := p.pool.Query(ctx, `SELECT `+workloadColumns+` FROM workloads WHERE node_id = $1 ORDER BY name`, nodeID)
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

func (p *Postgres) UpdateWorkloadStatus(ctx context.Context, id, nodeID, state, message string, at time.Time) error {
	tag, err := p.pool.Exec(ctx,
		`UPDATE workloads SET state = $3, status_message = $4, status_updated_at = $5 WHERE id = $1 AND node_id = $2`,
		id, nodeID, state, message, at)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (p *Postgres) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

func (p *Postgres) Close() {
	p.pool.Close()
}
