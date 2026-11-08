//go:build integration

package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dockfleet/dockfleet/internal/pgtest"
)

// These tests need a PostgreSQL server (version 13 or newer) and are skipped
// unless DOCKFLEET_TEST_DATABASE_URL names one. Every test creates its own
// database on that server, migrates it, and drops it when done (see pgtest), so
// tests never share state with each other or with a developer's data.

// newIsolatedDatabase creates an empty database and returns its URL.
func newIsolatedDatabase(t *testing.T) string {
	t.Helper()
	return pgtest.NewDatabase(t)
}

func openStore(t *testing.T, dbURL string) *Postgres {
	t.Helper()
	p, err := OpenPostgres(context.Background(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func migratedStore(t *testing.T) (*Postgres, string) {
	t.Helper()
	dbURL := newIsolatedDatabase(t)
	p := openStore(t, dbURL)
	noErr(t, p.Migrate(context.Background()))
	return p, dbURL
}

const allTables = `nodes, enrollments, workloads, workload_observations, services, service_revisions, events, leases`

func TestPostgresConformance(t *testing.T) {
	p, _ := migratedStore(t)
	runStoreConformance(t, func(t *testing.T) Store {
		if _, err := p.pool.Exec(context.Background(), `TRUNCATE `+allTables+` RESTART IDENTITY CASCADE`); err != nil {
			t.Fatal(err)
		}
		return p
	})
}

func TestPostgresMigrationsAreIdempotentAndRecorded(t *testing.T) {
	p, _ := migratedStore(t)
	ctx := context.Background()
	noErr(t, p.Migrate(ctx))
	noErr(t, p.Migrate(ctx))

	migrations, err := loadMigrations(migrationFS, "migrations")
	noErr(t, err)
	rows, err := p.pool.Query(ctx, `SELECT version, checksum FROM schema_migrations ORDER BY version`)
	noErr(t, err)
	defer rows.Close()
	i := 0
	for rows.Next() {
		var version string
		var checksum *string
		noErr(t, rows.Scan(&version, &checksum))
		if i >= len(migrations) || version != migrations[i].name || checksum == nil || *checksum != migrations[i].checksum {
			t.Fatalf("schema_migrations row %d = %q %v, want %s with its checksum", i, version, checksum, migrations[i].name)
		}
		i++
	}
	noErr(t, rows.Err())
	if i != len(migrations) {
		t.Fatalf("%d migrations recorded, want %d", i, len(migrations))
	}
}

func TestPostgresMigrationSafetyChecks(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name    string
		tamper  string
		wantErr string
	}{
		{"modified migration", `UPDATE schema_migrations SET checksum = 'tampered' WHERE version = '001_init.sql'`, "modified after it was applied"},
		{"database ahead of the binary", `INSERT INTO schema_migrations (version, checksum) VALUES ('999_from_the_future.sql', 'x')`, "does not know"},
		{"out of order", `DELETE FROM schema_migrations WHERE version = '003_enrollments.sql'`, "out of order"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, _ := migratedStore(t)
			if _, err := p.pool.Exec(ctx, tt.tamper); err != nil {
				t.Fatal(err)
			}
			err := p.Migrate(ctx)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Migrate error = %v, want one mentioning %q", err, tt.wantErr)
			}
		})
	}
}

func TestPostgresUpgradeKeepsExistingData(t *testing.T) {
	dbURL := newIsolatedDatabase(t)
	p := openStore(t, dbURL)
	ctx := context.Background()
	all, err := loadMigrations(migrationFS, "migrations")
	noErr(t, err)

	// A database as an earlier version of DockFleet left it: schema 001-003 with
	// workloads that mix desired and observed state in one table.
	noErr(t, applyMigrations(ctx, p.pool, all[:3]))
	for _, stmt := range []string{
		`INSERT INTO nodes (id, name, cpu_millis, memory_bytes, registered_at, last_heartbeat) VALUES ('n1', 'n1', 1000, 1000, now(), now())`,
		`INSERT INTO workloads (id, name, image, cpu_millis, memory_bytes, node_id, state, created_at)
		 VALUES ('fresh', 'fresh', 'img', 1, 1, 'n1', 'scheduled', '2026-01-01T00:00:00Z')`,
		`INSERT INTO workloads (id, name, image, cpu_millis, memory_bytes, node_id, state, created_at, status_message, status_updated_at)
		 VALUES ('reported', 'reported', 'img', 1, 1, 'n1', 'running', '2026-01-01T00:00:00Z', 'up', '2026-01-02T00:00:00Z')`,
	} {
		if _, err := p.pool.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}

	noErr(t, p.Migrate(ctx))

	fresh, err := p.GetWorkload(ctx, "fresh")
	noErr(t, err)
	if fresh.DesiredState != DesiredRunning || fresh.Observed != nil || fresh.Phase() != WorkloadScheduled || !fresh.UpdatedAt.Equal(fresh.CreatedAt) {
		t.Fatalf("a workload nobody had reported on: %+v", fresh)
	}
	reported, err := p.GetWorkload(ctx, "reported")
	noErr(t, err)
	if reported.DesiredState != DesiredRunning || reported.Observed == nil || reported.Observed.State != WorkloadRunning ||
		reported.Observed.Message != "up" || reported.Observed.NodeID != "n1" || !reported.Observed.ObservedAt.Equal(time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("the old status must become an observation: %+v", reported)
	}
}

func TestPostgresRestartPreservesState(t *testing.T) {
	dbURL := newIsolatedDatabase(t)
	ctx := context.Background()
	first := openStore(t, dbURL)
	noErr(t, first.Migrate(ctx))

	// Everything the manager keeps: nodes, enrollments, services with several
	// revisions, replicas, placements, observations, events and a lease.
	addNode(t, first, "node-a")
	_, err := first.CreateEnrollment(ctx, Enrollment{NodeID: "node-a", Description: "rack 1", CreatedAt: testTime})
	noErr(t, err)
	svc := newService(t, first, "web", 2)
	v2 := webSpec()
	v2.Image = "nginx:1.28"
	_, _, err = first.DeployService(ctx, svc.ID, Deployment{Spec: v2, Message: "upgrade", At: testTime.Add(time.Hour)})
	noErr(t, err)
	_, err = first.ScaleService(ctx, svc.ID, 4, 0, testTime.Add(2*time.Hour))
	noErr(t, err)
	w := standalone("w1", "web-r2-1", "node-a")
	w.ServiceID, w.ServiceRevision, w.ReplicaIndex = svc.ID, 2, 1
	_, err = first.CreateWorkload(ctx, w)
	noErr(t, err)
	_, err = first.SetWorkloadDesiredState(ctx, "w1", DesiredStopped, testTime.Add(3*time.Hour))
	noErr(t, err)
	noErr(t, first.RecordObservations(ctx, "node-a", []Observation{{
		WorkloadID: "w1", State: WorkloadRunning, Message: "up", ContainerID: "abc", RestartCount: 1, ObservedAt: testTime.Add(4 * time.Hour),
	}}))
	_, err = first.AppendEvent(ctx, Event{At: testTime, InstanceID: "manager-1", Actor: "admin", Action: "service.created", ResourceType: "service", ResourceID: svc.ID})
	noErr(t, err)
	_, err = first.AcquireLease(ctx, "reconciler", "manager-1", time.Hour, testTime)
	noErr(t, err)

	// The manager process goes away; a new one starts against the same database.
	first.Close()
	second := openStore(t, dbURL)
	noErr(t, second.Migrate(ctx))

	node, err := second.GetNode(ctx, "node-a")
	if err != nil || node.CPUMillis != 4000 {
		t.Fatalf("node after restart = %+v, %v", node, err)
	}
	if e, err := second.GetEnrollment(ctx, "node-a"); err != nil || !e.Active() || e.Description != "rack 1" {
		t.Fatalf("enrollment after restart = %+v, %v", e, err)
	}
	got, err := second.GetServiceByName(ctx, "web")
	if err != nil || got.Replicas != 4 || got.CurrentRevision != 2 || got.Version != 3 {
		t.Fatalf("service after restart = %+v, %v", got, err)
	}
	revs, err := second.ListRevisions(ctx, svc.ID)
	if err != nil || len(revs) != 2 || revs[0].Spec.Image != "nginx:1.27" || revs[1].Spec.Image != "nginx:1.28" || revs[1].Message != "upgrade" {
		t.Fatalf("revisions after restart = %+v, %v", revs, err)
	}
	workload, err := second.GetWorkload(ctx, "w1")
	if err != nil || workload.DesiredState != DesiredStopped || workload.ServiceID != svc.ID || workload.ServiceRevision != 2 || workload.NodeID != "node-a" {
		t.Fatalf("workload after restart = %+v, %v", workload, err)
	}
	if workload.Observed == nil || workload.Observed.State != WorkloadRunning || workload.Observed.ContainerID != "abc" || workload.Observed.RestartCount != 1 {
		t.Fatalf("observation after restart = %+v", workload.Observed)
	}
	if events, err := second.ListEvents(ctx, EventFilter{}); err != nil || len(events) != 1 || events[0].InstanceID != "manager-1" {
		t.Fatalf("events after restart = %+v, %v", events, err)
	}
	if lease, err := second.GetLease(ctx, "reconciler"); err != nil || lease.Holder != "manager-1" || lease.Token != 1 {
		t.Fatalf("lease after restart = %+v, %v", lease, err)
	}
}

// Several managers share one database. These tests run two stores (two
// connection pools, as two processes would have) against it concurrently.
func TestPostgresManagersSharingADatabase(t *testing.T) {
	dbURL := newIsolatedDatabase(t)
	ctx := context.Background()
	a, b := openStore(t, dbURL), openStore(t, dbURL)
	stores := []*Postgres{a, b}

	// Both migrate at once, as two managers starting together would.
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, s := range stores {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- s.Migrate(ctx)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		noErr(t, err)
	}

	runAll := func(n int, fn func(i int, s *Postgres) error) []error {
		results := make([]error, n)
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results[i] = fn(i, stores[i%2])
			}()
		}
		wg.Wait()
		return results
	}
	count := func(results []error, want error) int {
		n := 0
		for _, err := range results {
			if (want == nil && err == nil) || (want != nil && errors.Is(err, want)) {
				n++
			}
		}
		return n
	}

	t.Run("one service per name", func(t *testing.T) {
		results := runAll(8, func(i int, s *Postgres) error {
			_, err := s.CreateService(ctx, NewService{ID: NewID(), Name: "contested", Replicas: 1, Spec: webSpec(), At: testTime})
			return err
		})
		if count(results, nil) != 1 || count(results, ErrConflict) != 7 {
			t.Fatalf("results = %v, want one success and seven ErrConflict", results)
		}
		if revs, _ := a.ListServices(ctx, false); len(revs) != 1 {
			t.Fatalf("services = %+v", revs)
		}
	})

	t.Run("optimistic locking has one winner", func(t *testing.T) {
		svc := newService(t, a, "scaled", 1)
		results := runAll(8, func(i int, s *Postgres) error {
			_, err := s.ScaleService(ctx, svc.ID, 10+i, svc.Version, testTime)
			return err
		})
		if count(results, nil) != 1 || count(results, ErrVersionConflict) != 7 {
			t.Fatalf("results = %v, want one success and seven ErrVersionConflict", results)
		}
		if cur, _ := a.GetService(ctx, svc.ID); cur.Version != 2 {
			t.Fatalf("service = %+v, want exactly one change", cur)
		}
	})

	t.Run("concurrent deployments get consecutive revisions", func(t *testing.T) {
		svc := newService(t, a, "deployed", 1)
		results := runAll(8, func(i int, s *Postgres) error {
			spec := webSpec()
			spec.Image = "app:" + string(rune('a'+i))
			_, _, err := s.DeployService(ctx, svc.ID, Deployment{Spec: spec, At: testTime})
			return err
		})
		if count(results, nil) != 8 {
			t.Fatalf("results = %v, want eight successes (without expected versions deployments queue up)", results)
		}
		revs, err := b.ListRevisions(ctx, svc.ID)
		noErr(t, err)
		if len(revs) != 9 {
			t.Fatalf("revisions = %d, want 9", len(revs))
		}
		for i, r := range revs {
			if r.Revision != i+1 {
				t.Fatalf("revision %d at position %d", r.Revision, i)
			}
		}
		if cur, _ := a.GetService(ctx, svc.ID); cur.CurrentRevision != 9 || cur.Version != 9 {
			t.Fatalf("service = %+v, want revision 9 at version 9", cur)
		}
	})

	t.Run("the placement lock stops two managers overfilling a node", func(t *testing.T) {
		addNode(t, a, "node-a")
		const room = 5
		errFull := errors.New("node is full")
		results := runAll(12, func(i int, s *Postgres) error {
			return s.InTx(ctx, func(tx Tx) error {
				if err := tx.LockPlacement(ctx); err != nil {
					return err
				}
				existing, err := tx.ListWorkloadsForNode(ctx, "node-a")
				if err != nil {
					return err
				}
				if len(existing) >= room {
					return errFull
				}
				id := NewID()
				_, err = tx.CreateWorkload(ctx, standalone(id, "p-"+id, "node-a"))
				return err
			})
		})
		if count(results, nil) != room || count(results, errFull) != 12-room {
			t.Fatalf("results = %v, want exactly %d placements", results, room)
		}
		if all, _ := a.ListWorkloadsForNode(ctx, "node-a"); len(all) != room {
			t.Fatalf("workloads = %d, want %d", len(all), room)
		}
	})

	t.Run("a lease has one holder", func(t *testing.T) {
		results := runAll(6, func(i int, s *Postgres) error {
			_, err := s.AcquireLease(ctx, "reconciler", "manager-"+string(rune('a'+i)), time.Hour, time.Now())
			return err
		})
		if count(results, nil) != 1 || count(results, ErrLeaseHeld) != 5 {
			t.Fatalf("results = %v, want one holder", results)
		}
		if lease, _ := a.GetLease(ctx, "reconciler"); lease.Token != 1 {
			t.Fatalf("lease = %+v", lease)
		}
	})

	t.Run("one manager sees what the other wrote", func(t *testing.T) {
		_, err := a.CreateEnrollment(ctx, Enrollment{NodeID: "shared", CreatedAt: testTime})
		noErr(t, err)
		if e, err := b.GetEnrollment(ctx, "shared"); err != nil || !e.Active() {
			t.Fatalf("enrollment seen by the other manager = %+v, %v", e, err)
		}
	})
}

// The schema enforces its own rules, whatever the Go code does.
func TestPostgresSchemaConstraints(t *testing.T) {
	p, _ := migratedStore(t)
	ctx := context.Background()
	addNode(t, p, "node-a")
	svc := newService(t, p, "web", 1)
	_, err := p.CreateWorkload(ctx, standalone("w1", "w1", "node-a"))
	noErr(t, err)

	exec := func(sql string, args ...any) error {
		_, err := p.pool.Exec(ctx, sql, args...)
		return err
	}
	svcID := `'` + svc.ID + `'`
	rejected := []struct{ name, sql string }{
		{"desired state outside the allowed values", `UPDATE workloads SET desired_state = 'paused' WHERE id = 'w1'`},
		{"observation phase outside the allowed values", `INSERT INTO workload_observations (workload_id, node_id, state, observed_at) VALUES ('w1', 'node-a', 'scheduled', now())`},
		{"observation of a workload that does not exist", `INSERT INTO workload_observations (workload_id, node_id, state, observed_at) VALUES ('ghost', 'node-a', 'running', now())`},
		{"observation reported by an unknown node", `INSERT INTO workload_observations (workload_id, node_id, state, observed_at) VALUES ('w1', 'ghost', 'running', now())`},
		{"service columns set only partly", `UPDATE workloads SET service_id = ` + svcID + ` WHERE id = 'w1'`},
		{"replica of a revision that does not exist", `UPDATE workloads SET service_id = ` + svcID + `, service_revision = 9, replica_index = 1 WHERE id = 'w1'`},
		{"replica index below one", `UPDATE workloads SET service_id = ` + svcID + `, service_revision = 1, replica_index = 0 WHERE id = 'w1'`},
		{"negative replica count", `UPDATE services SET replicas = -1`},
		{"service version below one", `UPDATE services SET version = 0`},
		{"deleting a node that has workloads", `DELETE FROM nodes WHERE id = 'node-a'`},
		{"deleting the current revision of a service", `DELETE FROM service_revisions WHERE service_id = ` + svcID},
		{"revision number zero", `INSERT INTO service_revisions (service_id, revision, spec, spec_hash, created_at) VALUES (` + svcID + `, 0, '{}', 'h', now())`},
		{"lease token below one", `INSERT INTO leases (name, holder, token, expires_at) VALUES ('l', 'h', 0, now())`},
	}
	for _, tt := range rejected {
		if err := exec(tt.sql); err == nil {
			t.Errorf("%s: the database accepted it", tt.name)
		}
	}

	// A service must point at a revision that exists, checked when the transaction commits.
	err = p.InTx(ctx, func(tx Tx) error {
		_, err := tx.(*pgTx).db.Exec(ctx,
			`INSERT INTO services (id, name, replicas, current_revision, created_at, updated_at) VALUES ('orphan', 'orphan', 1, 1, now(), now())`)
		return err
	})
	if err == nil {
		t.Error("a service without its first revision was committed")
	}

	// Deleting a workload takes its observation with it.
	noErr(t, p.RecordObservations(ctx, "node-a", []Observation{observation("w1", WorkloadRunning, testTime)}))
	noErr(t, exec(`DELETE FROM workloads WHERE id = 'w1'`))
	var n int
	noErr(t, p.pool.QueryRow(ctx, `SELECT count(*) FROM workload_observations`).Scan(&n))
	if n != 0 {
		t.Errorf("%d observations left after their workload was deleted", n)
	}
}

// explain returns the plan of a query with sequential scans disabled, so the
// plan names an index whenever one can serve the query.
func explain(t *testing.T, p *Postgres, query string) string {
	t.Helper()
	ctx := context.Background()
	tx, err := p.pool.Begin(ctx)
	noErr(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `SET LOCAL enable_seqscan = off`)
	noErr(t, err)
	rows, err := tx.Query(ctx, `EXPLAIN `+query)
	noErr(t, err)
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var line string
		noErr(t, rows.Scan(&line))
		plan = append(plan, line)
	}
	noErr(t, rows.Err())
	return strings.Join(plan, "\n")
}

func TestPostgresQueriesUseIndexes(t *testing.T) {
	p, _ := migratedStore(t)
	tests := []struct {
		name  string
		query string
		index string
	}{
		// Lookups only: ordering clauses are left out because with empty tables the
		// planner is free to satisfy ORDER BY from another index instead.
		{"workloads of a node", workloadSelect + ` WHERE w.node_id = 'n'`, "workloads_node_id_idx"},
		{"replicas of a service", workloadSelect + ` WHERE w.service_id = 's'`, "workloads_replica_key"},
		{"observations of a node", `SELECT * FROM workload_observations WHERE node_id = 'n'`, "workload_observations_node_idx"},
		{"observation of a workload", `SELECT * FROM workload_observations WHERE workload_id = 'w'`, "workload_observations_pkey"},
		{"service by active name", `SELECT ` + serviceColumns + ` FROM services WHERE name = 'web' AND deleted_at IS NULL`, "services_active_name_key"},
		{"revisions of a service", `SELECT ` + revisionColumns + ` FROM service_revisions WHERE service_id = 's' ORDER BY revision`, "service_revisions_pkey"},
		{"events of a resource", `SELECT ` + eventColumns + ` FROM events WHERE resource_type = 'service' AND resource_id = 's'`, "events_resource_idx"},
		{"latest events", `SELECT ` + eventColumns + ` FROM events ORDER BY id DESC LIMIT 10`, "events_pkey"},
		{"enrollment of a node", `SELECT ` + enrollmentColumns + ` FROM enrollments WHERE node_id = 'n'`, "enrollments_pkey"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if plan := explain(t, p, tt.query); !strings.Contains(plan, tt.index) {
				t.Fatalf("the plan does not use %s:\n%s", tt.index, plan)
			}
		})
	}
}
