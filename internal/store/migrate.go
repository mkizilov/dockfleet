package store

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// migrationLockID serializes managers that start at the same time and all try
// to migrate. The lock is released when the migration transaction ends.
const migrationLockID int64 = 0x646f636b666c74

var migrationFileName = regexp.MustCompile(`^(\d{3})_([a-z0-9_]+)\.sql$`)

// migration is one embedded schema change. Migrations are numbered 001, 002,
// ... without gaps, applied in order, and never edited once released: a
// change to the schema is always a new file.
type migration struct {
	number   int
	name     string // file name, also the version recorded in schema_migrations
	sql      string
	checksum string
}

// loadMigrations reads and validates the migration files in dir.
func loadMigrations(fsys fs.FS, dir string) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}
	var out []migration
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		parts := migrationFileName.FindStringSubmatch(e.Name())
		if parts == nil {
			return nil, fmt.Errorf("migration file %q must be named NNN_description.sql", e.Name())
		}
		number, _ := strconv.Atoi(parts[1])
		body, err := fs.ReadFile(fsys, path.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", e.Name(), err)
		}
		sum := sha256.Sum256(body)
		out = append(out, migration{number: number, name: e.Name(), sql: string(body), checksum: hex.EncodeToString(sum[:])})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].number < out[j].number })
	for i, m := range out {
		if m.number != i+1 {
			return nil, fmt.Errorf("migrations must be numbered 001, 002, ... without gaps or duplicates: %s is at position %d", m.name, i+1)
		}
	}
	return out, nil
}

// beginner is what migrating needs from a connection pool.
type beginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// applyMigrations brings the database to the latest schema. It is safe to call
// from several managers at once and idempotent.
//
// It refuses to run when the database is ahead of this binary (it knows a
// migration this binary does not: upgrade the manager), when a recorded
// migration's file has changed since it was applied, and when a migration
// older than an applied one is still pending (out of order).
func applyMigrations(ctx context.Context, db beginner, migrations []migration) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("lock migrations: %w", err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    text PRIMARY KEY,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	// Databases created before checksums were recorded get them on first sight.
	if _, err := tx.Exec(ctx, `ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum text`); err != nil {
		return fmt.Errorf("add checksum column: %w", err)
	}

	rows, err := tx.Query(ctx, `SELECT version, checksum FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}
	applied := map[string]*string{}
	for rows.Next() {
		var version string
		var checksum *string
		if err := rows.Scan(&version, &checksum); err != nil {
			rows.Close()
			return fmt.Errorf("read schema_migrations: %w", err)
		}
		applied[version] = checksum
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}

	known := make(map[string]migration, len(migrations))
	for _, m := range migrations {
		known[m.name] = m
	}
	highestApplied := 0
	for version, checksum := range applied {
		m, ok := known[version]
		if !ok {
			return fmt.Errorf("the database has migration %s, which this version of DockFleet does not know: upgrade the manager", version)
		}
		if checksum != nil && *checksum != m.checksum {
			return fmt.Errorf("migration %s was modified after it was applied (checksum mismatch); schema changes must be new migrations", version)
		}
		if checksum == nil {
			if _, err := tx.Exec(ctx, `UPDATE schema_migrations SET checksum = $2 WHERE version = $1`, version, m.checksum); err != nil {
				return fmt.Errorf("record checksum of %s: %w", version, err)
			}
		}
		if m.number > highestApplied {
			highestApplied = m.number
		}
	}

	for _, m := range migrations {
		if _, done := applied[m.name]; done {
			continue
		}
		if m.number < highestApplied {
			return fmt.Errorf("migration %s is pending but a later migration is already applied; refusing to apply it out of order", m.name)
		}
		if _, err := tx.Exec(ctx, m.sql); err != nil {
			return fmt.Errorf("apply migration %s: %w", m.name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, checksum) VALUES ($1, $2)`, m.name, m.checksum); err != nil {
			return fmt.Errorf("record migration %s: %w", m.name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}
	return nil
}
