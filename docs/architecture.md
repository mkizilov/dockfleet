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
   |   internal/api  --->  internal/scheduler           |
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
                  |   internal/agent --> internal/docker  |
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
- **`internal/api`**: Go 1.22 `ServeMux` handlers. Request bodies are capped at 1 MiB and decoded strictly (unknown fields rejected). Bearer tokens are compared in constant time. Workload creation lists nodes and workloads, asks the scheduler for a node and stores the result while holding a mutex, so a single manager cannot overcommit a node.
- **`internal/scheduler`**: pure functions. `Candidates` drops nodes without a recent heartbeat and totals reserved CPU and memory per node; `Place` filters by label selector and free capacity and picks the node with the most free CPU (ties: free memory, then node ID).
- **`internal/store`**: `Store` interface with PostgreSQL (`pgx` pool, embedded SQL migrations guarded by an advisory lock) and in-memory implementations. Both pass one shared conformance test.
- **`internal/protocol`**: `NodeService` (`RegisterNode`, `Heartbeat`, `DeregisterNode`) from `proto/dockfleet/v1/fleet.proto`; protocol version rules; `ProtocolError` details surfaced as `protocol.Error`; a unary interceptor enforcing the agent token and another applying a per-call deadline; a client that attaches the token, refuses plaintext to non-loopback addresses and reconnects with backoff. Optional TLS 1.3. Details in [protocol.md](protocol.md).
- **`internal/registry`**: the manager-side node registry. `Register` persists the node and starts a session (replacing an older one); `Heartbeat` validates the session, refreshes liveness, applies status reports (writing only changes, ignoring workloads not assigned to the reporting node) and returns the node's complete assignment set; `Deregister` ends the session and backdates the heartbeat. `Register` and every `Heartbeat` also require an active enrollment in the store, so revocation ends a session at its next heartbeat. Sessions are in memory; everything else is in the store.
- **`internal/netutil`**: `IsLoopbackAddr` and `RequireLoopback`, shared by config validation, the manager and the client.
- **`internal/pki`**: development CA, manager and node certificates (node identity is the URI SAN `spiffe://dockfleet/node/<id>`), CSR signing, validity checks, expiry reporting, and TLS 1.3 configurations whose certificate files and CA bundles are re-read when they change (a failed reload keeps the previous version). See [mtls.md](mtls.md).
- **`cmd/pki`** (`dockfleet-pki`): the CLI over `internal/pki`; writes keys owner-only into a git-ignored directory and never prints them.
- **`internal/agent`**: the connection loop. It registers with exponential backoff (1s to 30s), heartbeats at the interval the manager returns with a deadline on every call, mirrors assignments into status reports, registers again when the manager reports `NOT_REGISTERED` or `SESSION_SUPERSEDED`, retries transient failures forever, exits with an error on permanent ones (incompatible version, invalid registration, rejected credentials), and deregisters on clean shutdown.
- **`internal/integration`**: tests only; real agent loops against a real manager over an in-memory gRPC transport.
- **`internal/docker`**: the agent-only Docker Engine adapter built on the Docker Go SDK (confined to `sdk.go`). It lists, creates, starts, stops, removes, inspects, logs and samples DockFleet's containers, identified by stable `io.dockfleet.*` labels and addressed by workload ID, never touching unrelated containers; creation passes a policy that rejects privileged mode, Docker socket and system-path mounts, added capabilities, host namespaces and public ports by default. The manager never imports it (enforced by a test). See [docker.md](docker.md).
- **`internal/config`, `internal/logging`**: environment configuration with validation; `log/slog` JSON or text.

### Not implemented

Driving the Docker adapter from assignments (the agent does not start workload containers yet and reports them as `pending`), reconciliation of desired versus actual state, workload deletion or rescheduling when a node dies, node removal, per-node identity, leader election or multiple managers (sessions live in one process's memory), metrics.

## Data model

| Table       | Key        | Notes                                                                 |
| ----------- | ---------- | --------------------------------------------------------------------- |
| `nodes`     | `id`       | Declared CPU (millicores) and memory, labels (`jsonb`), heartbeat time |
| `workloads` | `id`       | Unique `name`, resources, `node_id` foreign key, `state`              |
| `enrollments` | `node_id` | Administrator's permission for a node ID to join; `revoked_at` set on revocation (migration 003) |

Migrations live in `internal/store/migrations` and are recorded in `schema_migrations`. A workload starts as `scheduled`; agent reports then set `state` (`pending`, `running`, `succeeded`, `failed`, `stopped`) together with `status_message` and `status_updated_at` (migration 002).

## Node liveness

A node is `ready` when `now - last_heartbeat <= DOCKFLEET_NODE_TTL`. The manager tells each agent its heartbeat interval at registration. Readiness is computed on read; nothing is written when a node goes quiet. A clean agent shutdown deregisters, which backdates the heartbeat so the node leaves scheduling immediately.

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
- **Placement is recorded, not executed**, so the admin API and scheduler can be built and tested before the runtime exists.

## Extension points for the next stages

- Assignment delivery and status reporting already ride on `Heartbeat`; if latency matters, add a server-streaming `Watch` RPC next to it and bump the protocol minor version.
- Have the agent depend on `docker.Runtime` and drive it from assignments (pull, create, start, stop, remove, report real phases and usage); the adapter and its fakes already exist.
- Add a reconciler in the agent that drives actual state toward the manager's desired state.
- Per-user admin authentication (the admin API still uses one shared token), certificate revocation lists, and integration with an external CA.
