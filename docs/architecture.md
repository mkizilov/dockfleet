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
   |   internal/store  <---  internal/protocol (gRPC)   |
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
- **`internal/protocol`**: `NodeService` (`RegisterNode`, `Heartbeat`) from `proto/dockfleet/v1/fleet.proto`, a unary interceptor enforcing the agent token, and a client that attaches it. Optional TLS 1.3.
- **`internal/agent`**: registers with exponential backoff (1s to 30s), heartbeats at the interval the manager returns, re-registers when the manager answers `NOT_FOUND`, and survives transient heartbeat failures.
- **`internal/docker`**: Docker Engine `GET /_ping` over a unix socket or loopback TCP.
- **`internal/config`, `internal/logging`**: environment configuration with validation; `log/slog` JSON or text.

### Not implemented

Container create/start/stop, image pulls, reconciliation of desired versus actual state, workload deletion or rescheduling when a node dies, node removal, per-node identity, leader election or multiple managers, metrics.

## Data model

| Table       | Key        | Notes                                                                 |
| ----------- | ---------- | --------------------------------------------------------------------- |
| `nodes`     | `id`       | Declared CPU (millicores) and memory, labels (`jsonb`), heartbeat time |
| `workloads` | `id`       | Unique `name`, resources, `node_id` foreign key, `state`              |

Migrations live in `internal/store/migrations` and are recorded in `schema_migrations`. A workload's `state` is only ever `scheduled` today.

## Node liveness

A node is `ready` when `now - last_heartbeat <= DOCKFLEET_NODE_TTL`. The manager tells each agent its heartbeat interval at registration. Readiness is computed on read; nothing is written when a node goes quiet.

## Lifecycle and cancellation

Both binaries create a root context with `signal.NotifyContext` for `SIGINT` and `SIGTERM`. Every blocking call (database, gRPC, Docker, backoff timers) takes that context or a child of it.

Manager shutdown order, bounded by `DOCKFLEET_SHUTDOWN_TIMEOUT`:

1. Stop accepting HTTP connections and wait for in-flight requests (`http.Server.Shutdown`); force-close if the budget runs out.
2. `grpc.Server.GracefulStop`; force `Stop` if the budget runs out.
3. Wait for both serve goroutines, then the store is closed by `main`.

If either server fails on its own, the other is shut down the same way and the error is returned (exit status 1). A clean, requested shutdown returns nil (exit status 0). The agent stops its loop immediately when the context ends.

## Security model

- **Secure defaults**: listeners bind to loopback; a non-loopback address requires TLS (manager) or a CA file (agent). Tokens are mandatory, at least 16 characters, and the admin and agent tokens must differ. Configuration errors never echo secret values.
- **Transport**: TLS 1.3 when configured. The gRPC client refuses to send the bearer token over plaintext unless the connection is loopback-only by configuration.
- **Input limits**: node IDs and workload names match `^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`; gRPC messages are capped at 64 KiB; HTTP bodies at 1 MiB; label counts are bounded.
- **Docker access**: the agent only accepts `unix://` or loopback `tcp://` Docker hosts because the Engine API is unauthenticated. Anyone who can reach it controls the host, so agent-host hardening belongs to the future container work.
- **Development credentials** live in `config/dev.env`, are labeled as throwaway, and are not accepted by any production-oriented default.

Known gaps: a shared token per role (a leaked agent token lets any process register nodes), no rate limiting, no audit log.

## Decisions

- **Agents dial the manager** (see Overview) rather than the manager dialing agents.
- **Standard library first**: `log/slog`, `net/http` routing, `crypto/*`. The only third-party modules are gRPC, protobuf and `pgx`.
- **Generated code is not committed.** `buf generate` output goes to `internal/protocol/dockfleet/` and is git-ignored; CI and `make proto` regenerate it from pinned tool versions. This avoids drift at the cost of a required step after cloning.
- **In-memory store** exists for fast tests and database-free development; it is not a production option and logs a warning.
- **Placement is recorded, not executed**, so the admin API and scheduler can be built and tested before the runtime exists.

## Extension points for the next stages

- Add RPCs for agents to receive and report workload state (a server-streaming `Watch` or polling `ListAssignedWorkloads`); implement them in `internal/protocol` against new `Store` methods.
- Extend `docker.Client` with image and container operations; keep `internal/agent` depending on the interface so it stays testable with fakes.
- Add a reconciler in the agent that drives actual state toward the manager's desired state.
- Replace the shared agent token with per-node credentials or mTLS.
