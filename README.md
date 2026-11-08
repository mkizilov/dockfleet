# DockFleet

DockFleet is a distributed container orchestration system written in Go. A central **manager** keeps cluster state in PostgreSQL and places workloads; **agents** on worker nodes register with the manager and report liveness.

> **Status: project skeleton.** Both processes start, authenticate, talk gRPC and shut down gracefully. The manager records placement decisions, but **no containers are created, started or stopped yet**. See [Limitations](#limitations).

## Layout

| Path                 | Purpose                                                                 |
| -------------------- | ----------------------------------------------------------------------- |
| `cmd/manager`        | Control-plane entry point                                               |
| `cmd/agent`          | Worker-node entry point                                                 |
| `internal/api`       | Administrative HTTP API (nodes, workloads, health)                      |
| `internal/scheduler` | Workload placement (pure functions, no I/O)                             |
| `internal/agent`     | Node logic: register, heartbeat, retry, re-register                     |
| `internal/store`     | Persistence: `Store` interface, PostgreSQL and in-memory implementations |
| `internal/protocol`  | gRPC service, client, bearer-token auth, TLS helpers                    |
| `internal/docker`    | Docker Engine client (reachability check only for now)                  |
| `internal/manager`   | Wires the API, gRPC server and store; owns graceful shutdown            |
| `internal/config`    | Environment-based configuration with secure-default validation          |
| `internal/logging`   | Structured logging (`log/slog`, JSON or text)                           |
| `proto/`             | Protobuf definitions (`buf` module)                                     |
| `deploy/`            | Docker Compose file for development dependencies                        |
| `config/dev.env`     | Development-only configuration                                          |
| `docs/`              | [Architecture](docs/architecture.md)                                    |

## Prerequisites

- Go 1.23 or newer
- Docker with the Compose plugin (only for the PostgreSQL development database and agent reachability checks)
- Network access once, to download modules and the pinned code generators

## Quick start

```sh
make tools          # install pinned buf + protoc plugins into .tools/bin
make proto          # generate gRPC code (internal/protocol/dockfleet/, not committed)
make tidy           # resolve modules and write go.sum
make check          # gofmt check, go vet, unit tests with -race
make build          # bin/dockfleet-manager and bin/dockfleet-agent
```

Run a local cluster:

```sh
make compose-up     # PostgreSQL on 127.0.0.1:21232
make run-manager    # API on 127.0.0.1:21280, gRPC on 127.0.0.1:21281
make run-agent      # in another terminal
```

No Docker? `make run-manager-memory` uses the in-memory store instead of PostgreSQL.

Try the API (development token from `config/dev.env`):

```sh
curl -s -H 'Authorization: Bearer dev-api-token-0123456789' http://127.0.0.1:21280/v1/nodes

curl -s -X POST -H 'Authorization: Bearer dev-api-token-0123456789' \
  -d '{"name":"web","image":"nginx:1.27","cpu_millis":500,"memory_bytes":134217728}' \
  http://127.0.0.1:21280/v1/workloads
```

Stop either process with Ctrl-C or `SIGTERM`; both drain and exit with status 0. `make compose-down` stops the database.

## Administrative API

| Method and path           | Auth | Description                                                         |
| ------------------------- | ---- | ------------------------------------------------------------------- |
| `GET /healthz`            | no   | Liveness                                                            |
| `GET /readyz`             | no   | Readiness (store reachable)                                         |
| `GET /v1/nodes`           | yes  | Registered nodes with `ready` / `not_ready` status                  |
| `GET /v1/nodes/{id}`      | yes  | One node                                                            |
| `POST /v1/workloads`      | yes  | Choose a node for a workload and record it as `scheduled`          |
| `GET /v1/workloads`       | yes  | All workloads                                                       |
| `GET /v1/workloads/{id}`  | yes  | One workload                                                        |

Errors are `{"error": "..."}`. A workload request that fits on no ready node returns `409`.

## Configuration

All settings are environment variables. The manager and agent refuse to start on invalid configuration, and error messages never contain secrets.

| Variable                         | Used by | Default                      | Notes                                                      |
| -------------------------------- | ------- | ---------------------------- | ---------------------------------------------------------- |
| `DOCKFLEET_LOG_LEVEL`            | both    | `info`                       | `debug`, `info`, `warn`, `error`                           |
| `DOCKFLEET_LOG_FORMAT`           | both    | `json`                       | `json` or `text`                                           |
| `DOCKFLEET_API_ADDR`             | manager | `127.0.0.1:21280`            | Non-loopback requires TLS                                  |
| `DOCKFLEET_GRPC_ADDR`            | manager | `127.0.0.1:21281`            | Non-loopback requires TLS                                  |
| `DOCKFLEET_API_TOKEN`            | manager | **required**                 | Admin bearer token, at least 16 characters                 |
| `DOCKFLEET_AGENT_TOKEN`          | both    | **required**                 | Agent bearer token, at least 16 characters, differs from the API token |
| `DOCKFLEET_STORE`                | manager | `postgres`                   | `postgres` or `memory` (state lost on exit)                |
| `DOCKFLEET_DATABASE_URL`         | manager | required for `postgres`      | Migrations run at startup                                  |
| `DOCKFLEET_TLS_CERT_FILE` / `_KEY_FILE` | manager | unset               | Enables TLS 1.3 on both listeners                          |
| `DOCKFLEET_HEARTBEAT_INTERVAL`   | manager | `10s`                        | Sent to agents on registration                             |
| `DOCKFLEET_NODE_TTL`             | manager | `30s`                        | Node is `not_ready` after this long without a heartbeat; at least 2x the interval |
| `DOCKFLEET_SHUTDOWN_TIMEOUT`     | manager | `15s`                        | Drain budget before connections are forced closed          |
| `DOCKFLEET_MANAGER_ADDR`         | agent   | `127.0.0.1:21281`            | Non-loopback requires `DOCKFLEET_TLS_CA_FILE`              |
| `DOCKFLEET_TLS_CA_FILE`          | agent   | unset                        | CA bundle that signed the manager certificate              |
| `DOCKFLEET_NODE_ID`              | agent   | hostname                     | `[A-Za-z0-9._-]`, at most 63 characters                    |
| `DOCKFLEET_NODE_NAME`            | agent   | node ID                      |                                                            |
| `DOCKFLEET_NODE_CPU_MILLIS`      | agent   | CPU count x 1000             | Declared capacity                                          |
| `DOCKFLEET_NODE_MEMORY_MB`       | agent   | `1024`                       | Declared capacity (placeholder until memory detection exists) |
| `DOCKFLEET_NODE_LABELS`          | agent   | none                         | `key=value,key=value`, used by `node_selector`             |
| `DOCKFLEET_DOCKER_HOST`          | agent   | `unix:///var/run/docker.sock` | `unix://` or loopback `tcp://` only                       |

## Testing

```sh
make test               # unit tests, -race, no external services
make cover              # per-package coverage
make test-integration   # PostgreSQL store tests; needs `make compose-up`
```

Unit tests need no Docker, database or network. Network-style tests use in-memory transports (`bufconn`, fake HTTP round-trippers) or loopback ports 21290-21291 (`internal/manager`). PostgreSQL tests are behind the `integration` build tag and read `DOCKFLEET_TEST_DATABASE_URL`.

Generated protobuf code is not committed, so run `make proto` after a fresh clone and after editing `proto/`.

## Limitations

- No container lifecycle: `scheduled` workloads are never started, and nothing reconciles them.
- Scheduling is a single-manager spread strategy; a lock serializes placement in one process only.
- Node capacity is declared by the agent, not measured.
- Registered nodes are never deleted; dead nodes stay listed as `not_ready`.
- Authentication is a shared bearer token per role. Per-node identity and mTLS are not implemented.
- `go.sum` is generated by `make tidy`, which needs network access.

## License

No license has been chosen yet.
