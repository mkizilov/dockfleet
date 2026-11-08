# DockFleet architecture

This document describes what exists today and where the planned orchestration work plugs in. Anything not listed under "Implemented" is not implemented.

## Overview

```
                    admin (curl, future CLI/UI)
                              |
                      HTTP  Bearer API token
                              v
   +----------------------------------------------------+
   | manager                                            |
   |   internal/api ---+                                |
   |                   v                                |
   |   internal/placement --> internal/scheduler (pure) |
   |   internal/reconcile (retires, deletes)            |
   |        |                                           |
   |        v                                           |
   |   internal/store  <--  internal/registry           |
   |                              ^                     |
   |                        internal/protocol (gRPC)    |
   +--------|-------------------------^----------------+
            v                         |
       PostgreSQL        gRPC  Bearer agent token (+TLS)
                                      |
                  +-------------------+-------------------+
                  | agent (one per worker node)           |
                  |   internal/agent runner               |
                  |        --> internal/docker            |
                  +---------------------------------------+
                                      |
                              Docker Engine (local)
```

Agents dial the manager; the manager never connects to agents. This keeps nodes behind NAT or firewalls reachable and gives the manager no outbound credentials.

## Components

### Implemented

- **`cmd/manager`**: loads configuration, opens the store (running migrations for PostgreSQL), binds both listeners, delegates to `internal/manager`. Exits non-zero on startup failure.
- **`cmd/agent`**: loads configuration, builds the Docker and gRPC clients, runs `internal/agent`.
- **`internal/manager`**: runs the HTTP and gRPC servers side by side and owns shutdown (below).
- **`internal/api`**: Go 1.22 `ServeMux` handlers. Request bodies are capped at 1 MiB and decoded strictly (unknown fields rejected). Bearer tokens are compared in constant time. Workload creation reads the cluster, asks the scheduler for a node, saves the workload and records its audit event in one transaction under a cluster-wide placement lock, so no manager sharing the database can overcommit a node; a refusal is a `409` that carries the scheduler's reason for the request and for every node. Administrative changes record events in the same transaction (`GET /v1/events`). Service definitions (`/v1/services`) are validated against an explicit allow-list of fields (unsafe Docker options get a specific refusal), normalized so equivalent definitions are not changes, and stored as revisions with optimistic `If-Match` locking; the handlers never write workloads or schedule, and only read them to show a service's `status` (where its replicas were assigned, and why any has no node). Nodes show their allocatable and allocated resources. See [api.md](api.md).
- **`internal/scheduler`**: the decision, as pure functions of a snapshot (nodes, existing assignments, replicas to place, overcommit). No I/O, no clock, no Docker (a test pins its imports). Deterministic placement that never oversubscribes unless configured, with a reason for every replica it cannot place and for every node it rules out. See [scheduler.md](scheduler.md).
- **`internal/rollout`**: the rules of a rolling deployment, pure like the scheduler: when a replica is ready (running, acknowledged, healthy if it has a check), how many replicas of a new spec may be created now (the wanted count plus `max_surge`, never more), how many old ones may be retired (never fewer ready than the wanted count minus `max_unavailable`), what a rollout is waiting for in words, and the state machine of a rollout's record (progress, deadline, failure, recovery, completion). The scheduler pass and the reconciler both call it, so they agree. See [rollouts.md](rollouts.md).
- **`internal/placement`**: carries decisions out against the store. Each pass runs in one transaction under the placement lock: it reads nodes, enrollments, workloads and service definitions, asks the scheduler where every waiting replica goes, saves the assignments as workloads, replaces the stored explanations for replicas without a node, and records audit events. The manager runs it every `DOCKFLEET_SCHEDULER_INTERVAL`. Running the containers stays with the agents.
- **`internal/reconcile`**: compares service definitions with what agents report and moves workloads one way: running, stopped, deleted. `Plan` is a pure function (scaled-down and deleted services, where the replicas to stop are chosen by how little good they do and by node balance rather than by number, rollouts that retire old replicas only as far as the service's `max_unavailable` allows, replicas that already run the current spec kept (a rollback), the workloads of a node that was given up on, deletion once a node confirms the container is gone); it also records each node's health (`healthy`, `suspect`, `unavailable`) and the audit events for its changes ([failure-handling.md](failure-handling.md)); `Reconciler.RunOnce` applies it in one transaction under the placement lock with audit events; the manager runs it every `DOCKFLEET_RECONCILE_INTERVAL`. See [reconciliation.md](reconciliation.md).
- **`internal/store`**: small repository interfaces (nodes, enrollments, workloads and their observations, services and revisions, scheduling explanations, events, leases) combined in `Store`, with `InTx` for transactions. PostgreSQL (`pgx` pool, embedded checksummed migrations guarded by an advisory lock) and in-memory implementations pass one shared conformance suite. Desired and observed state are separate tables with separate writers. See [state.md](state.md).
- **`internal/protocol`**: `NodeService` (`RegisterNode`, `Heartbeat`, `DeregisterNode`) from `proto/dockfleet/v1/fleet.proto`; protocol version rules; `ProtocolError` details surfaced as `protocol.Error`; a unary interceptor enforcing the agent token and another applying a per-call deadline; a client that attaches the token, refuses plaintext to non-loopback addresses and reconnects with backoff. Optional TLS 1.3. Details in [protocol.md](protocol.md).
- **`internal/registry`**: the manager-side node registry. `Register` persists the node and starts a session (replacing an older one); `Heartbeat` validates the session, refreshes liveness, applies status reports (writing only changes, keeping the highest acknowledged generation, ignoring workloads not assigned to the reporting node) and returns the node's complete assignment set, running and stopped workloads alike; `Deregister` ends the session and backdates the heartbeat. `Register` and every `Heartbeat` also require an active enrollment in the store, so revocation ends a session at its next heartbeat. Sessions are in memory; everything else is in the store.
- **`internal/netutil`**: `IsLoopbackAddr` and `RequireLoopback`, shared by config validation, the manager and the client.
- **`internal/pki`**: development CA, manager and node certificates (node identity is the URI SAN `spiffe://dockfleet/node/<id>`), CSR signing, validity checks, expiry reporting, and TLS 1.3 configurations whose certificate files and CA bundles are re-read when they change (a failed reload keeps the previous version). See [mtls.md](mtls.md).
- **`cmd/pki`** (`dockfleet-pki`): the CLI over `internal/pki`; writes keys owner-only into a git-ignored directory and never prints them.
- **`internal/agent`**: the connection loop and the runner. The loop registers with exponential backoff (1s to 30s), heartbeats at the interval the manager returns with a deadline on every call, hands every assignment set to the runner and sends the runner's statuses, registers again when the manager reports `NOT_REGISTERED` or `SESSION_SUPERSEDED`, retries transient failures forever, exits with an error on permanent ones (incompatible version, invalid registration, rejected credentials), and deregisters on clean shutdown. The runner works beside it: from a fresh listing of the node's managed containers it creates (pulling images), starts, restarts per policy (with a growing wait, a limit and a restart when the health check fails), stops and removes containers until they match the assignments, never touches a container DockFleet does not manage, and reports phase, container, exit code, health and the acknowledged generation. It counts the manager's lease from each heartbeat it sends and, when the lease runs out, stops its containers (fencing) so that a replica started elsewhere does not also run here. Containers outlive the agent process.
- **`internal/integration`**: tests only; real agent loops against a real manager over an in-memory gRPC transport.
- **`internal/docker`**: the agent-only Docker Engine adapter built on the Docker Go SDK (confined to `sdk.go`). It lists, creates, starts, stops, removes, inspects, logs and samples DockFleet's containers, identified by stable `io.dockfleet.*` labels and addressed by workload ID, never touching unrelated containers; creation passes a policy that rejects privileged mode, Docker socket and system-path mounts, added capabilities, host namespaces and public ports by default. The manager never imports it (enforced by a test). See [docker.md](docker.md).
- **`internal/dockerfake`**: an in-memory `docker.Runtime` for tests, with the adapter's ownership, conflict and policy behavior.
- **`internal/config`, `internal/logging`**: environment configuration with validation; `log/slog` JSON or text.

### Not implemented

Rescheduling a replica whose container failed beyond its restart limit, deleting a node that is gone for good (its stopped workloads stay on record), fencing a crashed agent's containers, per-service fencing, node removal, per-node identity, running leader election (the lease primitive exists; scheduling and reconciliation passes serialize on the placement lock instead), metrics. Agent sessions live in one process's memory, so an agent talks to one manager at a time.

## Data model

| Table | Key | Kind | Notes |
| ----- | --- | ---- | ----- |
| `nodes` | `id` | observed | Declared CPU (millicores) and memory, labels (`jsonb`), heartbeat time |
| `workload_observations` | `workload_id` | observed | An agent's latest report: phase, message, container ID, exit code, OOM kill, restart count, start/finish times, acknowledged generation |
| `enrollments` | `node_id` | desired | Administrator's permission for a node ID to join; `revoked_at` set on revocation |
| `services` | `id` | desired | Name (unique among live services), desired `replicas`, `current_revision`, optimistic-lock `version`, `deleted_at` |
| `service_revisions` | `(service_id, revision)` | desired | Immutable spec snapshots (deployment history) |
| `workloads` | `id` | desired | One container's lifetime: placement (`node_id`), unique `name`, resources, published `ports`, how to run it (`runtime`), `desired_state` and its `generation`, optional service/revision/replica slot (unique) |
| `unschedulable_replicas` | `(service_id, revision, replica_index)` | scheduler output | Why a replica has no node yet: code, message and one reason per node; removed when it is placed or no longer wanted |
| `events` | `id` | audit | Append-only administrative events |
| `leases` | `name` | coordination | Named leases with fencing tokens for multi-manager duties |

Migrations (001-011) live in `internal/store/migrations`, are recorded with their checksums in `schema_migrations` and are never edited after release. A workload has no observation until its agent reports (`Workload.Phase()` is then `scheduled`); reports set the phase (`pending`, `running`, `succeeded`, `failed`, `stopped`) in `workload_observations` and never touch `workloads`. Details, multi-manager design and the restart guarantee are in [state.md](state.md).

## Node liveness

A node is `ready` when `now - last_heartbeat <= DOCKFLEET_NODE_TTL`. The manager tells each agent its heartbeat interval at registration. Readiness is computed on read; nothing is written when a node goes quiet. A clean agent shutdown deregisters, which backdates the heartbeat so the node leaves scheduling immediately. The scheduler also skips a node whose enrollment was revoked, even while it still heartbeats, and says so.

## Lifecycle and cancellation

Both binaries create a root context with `signal.NotifyContext` for `SIGINT` and `SIGTERM`. Every blocking call (database, gRPC, Docker, backoff timers) takes that context or a child of it.

Manager shutdown order, bounded by `DOCKFLEET_SHUTDOWN_TIMEOUT`:

1. Stop accepting HTTP connections and wait for in-flight requests (`http.Server.Shutdown`); force-close if the budget runs out.
2. `grpc.Server.GracefulStop`; force `Stop` if the budget runs out.
3. Wait for both serve goroutines, then the store is closed by `main`.

If either server fails on its own, the other is shut down the same way and the error is returned (exit status 1). A clean, requested shutdown returns nil (exit status 0). The agent stops its loop when the context ends and then deregisters using a fresh, short deadline so shutdown cannot hang on an unreachable manager. gRPC `GracefulStop` waits for in-flight calls, each of which has a deadline.

## Security model

- **Secure defaults**: listeners bind to loopback; a non-loopback address requires TLS (manager) or a CA file (agent). Tokens are mandatory, at least 16 characters, and the admin and agent tokens must differ. Configuration errors never echo secret values.
- **Transport**: TLS 1.3 when configured, and TLS always means mutual TLS on the agent listener (`protocol.NewGRPCServer` refuses a TLS configuration that does not require and verify client certificates). Plaintext is loopback-only and enforced in three places: configuration validation, `Manager.Serve` (which refuses non-loopback listeners and closes them) and `protocol.Dial` (`ErrInsecureRemote`). A failed TLS handshake is a failed call; nothing retries in plaintext.
- **Authentication and identity**: under mutual TLS the client certificate is the credential. `protocol.UnaryIdentityInterceptor` requires a verified chain, re-checks the validity window on every call and extracts the node ID; the handlers require every request's `node_id` to equal it. The agent bearer token exists only for plaintext loopback development.
- **Authorization**: only enrolled node IDs can register, checked at registration and on every heartbeat (`registry`). Admin enrollment endpoints sit behind the admin bearer token, over HTTPS when TLS is configured.
- **Expiry and rotation**: certificate and CA files are reloaded when they change; the manager ends agent connections after at most 10 minutes so renewals reach quiet connections; both processes log expiry warnings and errors hourly.
- **Input limits**: node IDs and workload names match `^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`; gRPC messages are capped at 1 MiB and status reports at 1000 per heartbeat; HTTP bodies at 1 MiB; label counts are bounded.
- **Docker access**: only the agent talks to Docker, and only to its local Engine (`unix://` or loopback `tcp://`; the Engine API is unauthenticated and equivalent to root). The manager has no Docker settings and no Docker imports, so it never needs a socket. Containers are always created with the fixed hardening described in [docker.md](docker.md), and never from specs the policy rejects.
- **Development credentials** live in `config/dev.env`, are labeled as throwaway, and are not accepted by any production-oriented default.

Known gaps: the admin API uses one shared bearer token, there is no CRL/OCSP (revoke by enrollment), the development CA is not a production PKI, no rate limiting, no audit log beyond structured logs of enrollment changes and rejections.

## Decisions

- **Agents dial the manager** (see Overview) rather than the manager dialing agents.
- **Standard library first**: `log/slog`, `net/http` routing, `crypto/*`. The only third-party modules are gRPC, protobuf and `pgx`.
- **Generated code is not committed.** `buf generate` output goes to `internal/protocol/dockfleet/` and is git-ignored; CI and `make proto` regenerate it from pinned tool versions. This avoids drift at the cost of a required step after cloning.
- **In-memory store** exists for fast tests and database-free development; it is not a production option and logs a warning.
- **Placement is recorded; execution is the agent's.** The manager decides and writes intent, so the admin API, scheduler and reconciler are tested without a runtime, and an agent that is away does not block them.
- **Instructions are states, not commands.** The manager sends each agent the complete set of its workloads with their desired state, and each agent reports the complete set it knows. Neither side depends on a message arriving once, in order, or at all, which is what makes retries and restarts safe.
- **A workload is one container's lifetime** (running, stopped, deleted), so its random ID names at most one container ever.
- **Deciding is separate from saving, and saving from running.** `internal/scheduler` answers "where?" from a snapshot and nothing else; `internal/placement` saves the answer as desired state; the agent and `internal/docker` are the only code that touches containers. Each can be tested without the others.

## Extension points for the next stages

- Assignment delivery and status reporting already ride on `Heartbeat`; if latency matters, add a server-streaming `Watch` RPC next to it and bump the protocol minor version.
- Metrics and usage reporting: the adapter can already sample `Stats`; the runner does not report it yet.
- Rescheduling a replica that keeps failing on one node onto another, and a way to retire a node that is gone for good.
- Per-user admin authentication (the admin API still uses one shared token), certificate revocation lists, and integration with an external CA.
