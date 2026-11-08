# Control-plane state

The manager keeps all of its state in PostgreSQL (`internal/store`). A restarted manager picks up exactly where the previous one stopped: nodes, enrollments, services and their revisions, workload placements, what agents last reported, and the audit trail are all in the database. The only thing a restart loses is volatile **sessions** (which agent is connected to this process), and agents re-register on their own when that happens.

`DOCKFLEET_STORE=memory` runs the same code against an in-process store for development and tests. State is then lost on exit. Both implementations pass one shared conformance suite.

## Desired state and observed state

Every piece of data is one or the other. They live in different tables and have different writers, so a late or lost report can never overwrite intent, and intent never pretends to be a fact.

| | Desired state | Observed state |
| --- | --- | --- |
| Meaning | What an administrator asked for | What an agent reported |
| Written by | The manager, for an administrator (later also a reconciler acting on their intent) | The manager, for an agent (registration, heartbeats) |
| Tables | `services`, `service_revisions`, `workloads`, `enrollments` | `nodes`, `workload_observations` |
| Examples | replica count, which revision to run, which node a workload is placed on, `desired_state` (`running`/`stopped`), who may join | declared capacity, last heartbeat, a container's phase, exit code, OOM kill, restart count |

A `store.Workload` carries both, in separate places: its own fields are intent, and `Observed` (nil until the agent has reported) is the observation. `Workload.Phase()` is the observed phase, or `scheduled` while nothing has been observed. Consequences, all covered by tests:

- `CreateWorkload` ignores any `Observed` it is given; only `RecordObservations` writes observations.
- A heartbeat only ever writes observations. An agent reporting `stopped` for a workload that is meant to be `running` changes nothing about what is wanted.
- `SetWorkloadDesiredState` never touches the observation.
- Placement counts the capacity of workloads that are *desired* to run, regardless of what they currently do. A failed container still occupies its slot until someone asks for it to be stopped.
- An observation names the node that reported it (`node_id`). If a workload is later moved, a stale observation is recognizable by that.
- An agent is only told about workloads whose desired state is `running`.

## Schema

Migrations are embedded in the binary (`internal/store/migrations/NNN_description.sql`) and applied by `Migrate` when the manager starts.

| Migration | Adds |
| --- | --- |
| 001, 002 | `nodes`, `workloads` (originally with state columns), status columns |
| 003 | `enrollments` |
| 004 | `services`, `service_revisions` |
| 005 | Splits workloads into desired and observed: `desired_state`, `service_id`/`service_revision`/`replica_index`, `updated_at` on `workloads`; new `workload_observations`; existing reported statuses are moved into it |
| 006 | `events` |
| 007 | `leases` |

Tables and the indexes that serve the queries:

- `nodes (id)`: registered nodes, declared CPU/memory, labels, agent version, `registered_at`, `last_heartbeat`.
- `enrollments (node_id)`: permission to join; `revoked_at` set on revocation.
- `services (id)`: `name` (unique among live services through a partial index, so a deleted service's name can be reused), `replicas` (desired count), `current_revision`, `version` (optimistic lock), `deleted_at`.
- `service_revisions (service_id, revision)`: immutable `spec` (jsonb: image, command, env, CPU, memory, node selector), its `spec_hash`, message, author. `services.current_revision` must exist (deferred foreign key, so a service and its first revision are inserted in one transaction).
- `workloads (id)`: placement and intent. `name` is unique; `(service_id, service_revision, replica_index)` is unique and is the index behind "replicas of a service"; `node_id` has an index for "workloads of a node". A workload either belongs to no service or has all three service columns set (check constraint, foreign key to the revision).
- `workload_observations (workload_id)`: one row per workload at most, deleted with it. Index on `node_id`.
- `events (id)`: append-only audit trail; indexes on `(resource_type, resource_id, id DESC)` and `at`.
- `leases (name)`: see below.

`TestPostgresQueriesUseIndexes` asserts with `EXPLAIN` that the hot queries use these indexes, and `TestPostgresSchemaConstraints` that the database itself refuses states the Go code never produces (bad phases, half-set service columns, dangling references, ...).

### Migration rules

- A migration is never edited after it is released; a schema change is a new file. Each applied migration's SHA-256 is recorded in `schema_migrations`, and `Migrate` refuses to continue if a recorded checksum no longer matches (`TestReleasedMigrationsAreUnchanged` pins the released ones).
- Migrations are numbered `001`, `002`, ... without gaps and applied in order, each file in a transaction together with its bookkeeping row: a failing migration leaves nothing behind.
- `Migrate` takes an advisory lock, so several managers starting at once apply each migration exactly once, and is idempotent.
- It refuses to run when the database is ahead of the binary (a newer manager has migrated it: upgrade this one) and when an older migration is pending behind a newer applied one.
- Databases created before checksums existed get theirs recorded the first time they are seen.

Because all managers must understand the schema they share, upgrade managers before (or while) they run against a migrated database; a manager older than the schema refuses to start instead of guessing.

## Repository interfaces

`store.Store` is the union of small repositories, each documented in `internal/store/store.go`:

| Interface | Holds |
| --- | --- |
| `NodeRepository` | nodes (observed) |
| `EnrollmentRepository` | enrollments (desired) |
| `WorkloadRepository` | workload placements (desired) and `RecordObservations` (observed) |
| `ServiceRepository` | services, replica counts, deployment revisions (desired) |
| `EventRepository` | audit events |
| `LeaseRepository` | named leases |

Packages depend on the narrowest interface they use (`registry.Store`, for example), which keeps them testable with fakes.

Conventions: `ErrNotFound`, `ErrConflict` (uniqueness), `ErrInvalid`, `ErrVersionConflict` (optimistic lock lost), `ErrUnchanged` (the request equals the current state, so nothing was written: no new version, no new revision, no event) and `ErrLeaseHeld`. IDs are generated by the caller (`store.NewID`), never by a database sequence.

### Transactions

`Store.InTx(ctx, fn)` runs `fn` with a `Tx` that has every repository. If `fn` returns an error or panics, all of its writes are rolled back; otherwise they commit together. Examples in the code: registering a node saves it and records `node.registered` atomically; creating a workload reads the cluster, places it, saves it and records `workload.created` as one unit; enrollment changes and their events likewise. Multi-statement operations inside the repositories (`CreateService`, `DeployService`, `RecordObservations`) are atomic by themselves and nest inside a caller's transaction (as savepoints).

The in-memory store gives the same guarantees (a transaction works on a copy and replaces the state on success).

## Deployment revisions

A service holds the desired replica count and a pointer to its current revision. Revisions are immutable snapshots of the spec; numbers start at 1 and only grow. `DeployService` with a spec identical to the current revision's is a no-op (`ErrUnchanged`). A rollback is a new revision carrying an old spec, so the history stays linear and every revision is a faithful record of what was once desired. Replica workloads record which revision they belong to, which is what a future reconciler needs to roll out a change one replica at a time. Deleting a service marks it deleted and sets the desired replicas to zero; revisions, workloads and events are kept.

Nothing reconciles services into workloads yet: this change provides the durable desired state, not the controller. There are no HTTP endpoints for services yet either; the repository interfaces are the contract.

## Several managers on one database

Nothing in the store keeps state in process, and the schema and queries are written for managers sharing a database:

- **Identity.** Each manager has an instance ID (`DOCKFLEET_INSTANCE_ID`, random per process if unset) that audit events record, and that leases use as holder name.
- **No coordination to create things.** IDs are generated by the caller; uniqueness is enforced by constraints (one live service per name, one replica per slot), and a duplicate surfaces as `ErrConflict`.
- **Optimistic concurrency.** `Service.Version` increases on every change. `ScaleService`, `DeployService` and `DeleteService` take the version the caller read and fail with `ErrVersionConflict` if it changed meanwhile (zero skips the check). Deployments additionally lock the service row, so concurrent deployments get consecutive revision numbers.
- **Placement.** Placement reads the whole cluster and writes one workload, so two placements must not interleave. `Tx.LockPlacement` takes a cluster-wide advisory lock (released with the transaction), replacing the process-local mutex the API used before.
- **Leases.** `AcquireLease(name, holder, ttl, now)` gives exactly one manager at a time a duty such as reconciling. Renewing keeps the token; every change of hands increments it, so a manager that stalled and lost its lease can be told apart from the new holder by the fencing token. Managers need roughly synchronized clocks (NTP) because they pass their own time.
- **Agent sessions** are per manager process: an agent talks to one manager at a time and registers again if that manager forgets it. Which manager an agent reaches is up to the load balancer or its configured address.
- **Migrations** are serialized by an advisory lock (above).

What is *not* done yet: no leader election is running (the lease primitive exists and is tested), and `Heartbeat` and observation writes from different managers for the same node are last-writer-wins by `observed_at` (a report older than the stored one is ignored).

## Administrative events

`events` is an append-only audit trail written in the same transaction as the change it describes. Each event has the time, the manager instance, an actor (`admin` for the shared admin token, `node:<id>` for agents), an action, a resource and free-form details. Recorded today: `enrollment.created`, `enrollment.revoked`, `workload.created`, `node.registered`, `node.deregistered`, `node.session_ended`. Requests that fail or change nothing record nothing. `GET /v1/events` lists them newest first (`resource_type`, `resource_id`, `before` for paging, `limit`).

## Tests

- `internal/store`: `runStoreConformance` runs against `Memory` (always) and `Postgres` (tag `integration`): nodes, enrollments, workloads, observations, services, revisions, replicas, deletion, events, leases, transactions and rollbacks (including panics). Plus migration-file checks that need no database.
- Postgres-only (`postgres_integration_test.go`): migration idempotency, checksums, out-of-order and ahead-of-binary refusals; **upgrading** a database created by the previous schema without losing data; **restart** (close the store, open a new one on the same database, everything is still there); two managers on one database (simultaneous migration, one service per name, one winner for optimistic updates, consecutive revisions under concurrent deploys, the placement lock preventing overfilling a node, one lease holder); schema constraints; index use.

Run the database tests against the Compose database:

```sh
make compose-up
make test-integration     # uses DOCKFLEET_DATABASE_URL from config/dev.env
```

**Isolation.** `DOCKFLEET_TEST_DATABASE_URL` names a PostgreSQL *server* (13 or newer, for `DROP DATABASE ... WITH (FORCE)`) and a role that may `CREATE DATABASE`. Each test creates `dockfleet_test_<random>`, migrates it, runs, and drops it. The database named in the URL is only used to connect and is never written to, so development data is never touched and tests never share state.
