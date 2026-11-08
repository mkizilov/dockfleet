# DockFleet

DockFleet is a distributed container orchestration system written in Go. A central **manager** keeps cluster state in PostgreSQL and places workloads; **agents** on worker nodes register with the manager, heartbeat, receive their assigned workloads and report status.

> **Status: early.** The manager-agent gRPC protocol (versioned registration, heartbeats, assignments, status reports) works end to end over mutual TLS with explicit node enrollment, and both processes shut down gracefully. The manager records placement decisions, but **no containers are created, started or stopped yet**: agents acknowledge assignments as `pending`. See [Limitations](#limitations).

## Layout

| Path                 | Purpose                                                                 |
| -------------------- | ----------------------------------------------------------------------- |
| `cmd/manager`        | Control-plane entry point                                               |
| `cmd/agent`          | Worker-node entry point                                                 |
| `cmd/pki`            | `dockfleet-pki`: development CA and certificate issuing                 |
| `internal/api`       | Administrative HTTP API (nodes, workloads, health)                      |
| `internal/scheduler` | Workload placement (pure functions, no I/O)                             |
| `internal/agent`     | Node logic: register, heartbeat, retry, re-register                     |
| `internal/store`     | Persistence: `Store` interface, PostgreSQL and in-memory implementations |
| `internal/protocol`  | gRPC service, client, versioning, errors, bearer-token auth, TLS helpers |
| `internal/registry`  | Manager-side node registry: sessions, heartbeats, status reports        |
| `internal/pki`       | CA, certificates, node identity, validation, reloading TLS configs      |
| `internal/netutil`   | Loopback checks behind the plaintext-only-on-loopback rule              |
| `internal/integration` | Tests running simulated agents against a manager over gRPC           |
| `internal/docker`    | Agent-only Docker Engine adapter: managed containers, hardening policy (see [docs/docker.md](docs/docker.md)) |
| `internal/manager`   | Wires the API, gRPC server and store; owns graceful shutdown            |
| `internal/config`    | Environment-based configuration with secure-default validation          |
| `internal/logging`   | Structured logging (`log/slog`, JSON or text)                           |
| `proto/`             | Protobuf definitions (`buf` module)                                     |
| `deploy/`            | Docker Compose file for development dependencies                        |
| `config/dev.env`     | Development-only configuration                                          |
| `docs/`              | [Architecture](docs/architecture.md), [Protocol](docs/protocol.md), [Mutual TLS and enrollment](docs/mtls.md), [Docker adapter](docs/docker.md) |

## Prerequisites

- Go 1.23 or newer
- Docker with the Compose plugin (the PostgreSQL development database), and a local Docker Engine on every worker running an agent. Only agents use Docker; the manager never does
- Network access once, to download modules and the pinned code generators

## Quick start

```sh
make generate       # pinned buf + protoc plugins into .tools/bin, lint, generate gRPC code
                    # (output: internal/protocol/dockfleet/, not committed)
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

The commands above run in plaintext development mode on loopback, authenticated by the shared tokens in `config/dev.env`. Nodes must be enrolled before they can register: run `make dev-enroll` once the manager is up (`dev-node-1`). To run with mutual TLS instead:

```sh
make dev-pki              # CA + manager + dev-node-1 certificates in .dev-pki/ (git-ignored)
make run-manager-mtls     # HTTPS admin API; agent listener requires client certificates
make dev-enroll SCHEME=https
make run-agent-mtls
```

Administrators distributing real credentials should read [docs/mtls.md](docs/mtls.md).

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
| `POST /v1/enrollments`    | yes  | Enroll a node ID (`{"node_id":"...","description":"..."}`); `409` if already enrolled |
| `GET /v1/enrollments`     | yes  | All enrollments                                                     |
| `GET /v1/enrollments/{id}` | yes | One enrollment                                                      |
| `DELETE /v1/enrollments/{id}` | yes | Revoke an enrollment; the node's session ends at its next heartbeat |

Errors are `{"error": "..."}`. A workload request that fits on no ready node returns `409`. Once the target node's agent reports on a workload, its `state` changes from `scheduled` to the reported state (`pending`, `running`, `succeeded`, `failed`, `stopped`) and `status_message` / `status_updated_at` are filled in.

## Manager-agent protocol

Agents dial the manager over gRPC, send a protocol version, node identity and CPU/memory capacity, heartbeat at the interval the manager returns, receive their assigned workloads and report status. Versions, errors and the session lifecycle are documented in [docs/protocol.md](docs/protocol.md).

## Security model

- **Mutual TLS.** Outside loopback development, the agent listener requires a client certificate signed by your CA (TLS 1.3), and agents verify the manager certificate. Remote agent traffic never falls back to plaintext: non-loopback plaintext listeners and plaintext dials are refused, and TLS without mandatory client certificates does not exist.
- **Identity binding.** A node certificate carries the node ID; every request must name that ID.
- **Explicit enrollment.** Only node IDs an administrator enrolled can register; revocation applies at the next heartbeat.
- **Expiration.** Expired or not-yet-valid certificates are rejected, expiry is re-checked per call, startup and periodic warnings are logged, and renewed certificates are picked up without a restart.
- **No committed keys.** `dockfleet-pki` writes keys owner-only into the git-ignored `.dev-pki/`; `make check-secrets` guards the repository.

## Configuration

All settings are environment variables. The manager and agent refuse to start on invalid configuration, and error messages never contain secrets.

| Variable                         | Used by | Default                      | Notes                                                      |
| -------------------------------- | ------- | ---------------------------- | ---------------------------------------------------------- |
| `DOCKFLEET_LOG_LEVEL`            | both    | `info`                       | `debug`, `info`, `warn`, `error`                           |
| `DOCKFLEET_LOG_FORMAT`           | both    | `json`                       | `json` or `text`                                           |
| `DOCKFLEET_API_ADDR`             | manager | `127.0.0.1:21280`            | Non-loopback requires TLS                                  |
| `DOCKFLEET_GRPC_ADDR`            | manager | `127.0.0.1:21281`            | Non-loopback requires TLS                                  |
| `DOCKFLEET_API_TOKEN`            | manager | **required**                 | Admin bearer token, at least 16 characters                 |
| `DOCKFLEET_AGENT_TOKEN`          | both    | required without TLS         | Plaintext loopback development only; at least 16 characters, differs from the API token |
| `DOCKFLEET_STORE`                | manager | `postgres`                   | `postgres` or `memory` (state lost on exit)                |
| `DOCKFLEET_DATABASE_URL`         | manager | required for `postgres`      | Migrations run at startup                                  |
| `DOCKFLEET_TLS_CERT_FILE`, `DOCKFLEET_TLS_KEY_FILE`, `DOCKFLEET_TLS_CLIENT_CA_FILE` | manager | unset | Set all three to enable TLS 1.3: HTTPS admin API and mutual TLS for agents (`DOCKFLEET_AGENT_TOKEN` is then ignored) |
| `DOCKFLEET_HEARTBEAT_INTERVAL`   | manager | `10s`                        | Sent to agents on registration                             |
| `DOCKFLEET_NODE_TTL`             | manager | `30s`                        | Node is `not_ready` after this long without a heartbeat; at least 2x the interval |
| `DOCKFLEET_REQUEST_TIMEOUT`      | manager | `10s`                        | Deadline for each gRPC call                                |
| `DOCKFLEET_SHUTDOWN_TIMEOUT`     | manager | `15s`                        | Drain budget before connections are forced closed          |
| `DOCKFLEET_MANAGER_ADDR`         | agent   | `127.0.0.1:21281`            | Non-loopback requires `DOCKFLEET_TLS_CA_FILE`              |
| `DOCKFLEET_TLS_CA_FILE`, `DOCKFLEET_TLS_CERT_FILE`, `DOCKFLEET_TLS_KEY_FILE` | agent | unset | Set all three for mutual TLS (required for a non-loopback manager): CA that signed the manager certificate, and the agent's own certificate and key |
| `DOCKFLEET_TLS_SERVER_NAME`      | agent   | host of the manager address  | Name the manager certificate must carry                    |
| `DOCKFLEET_RPC_TIMEOUT`          | agent   | `5s`                         | Deadline for each call to the manager                      |
| `DOCKFLEET_NODE_ID`              | agent   | hostname                     | `[A-Za-z0-9._-]`, at most 63 characters                    |
| `DOCKFLEET_NODE_NAME`            | agent   | node ID                      |                                                            |
| `DOCKFLEET_NODE_CPU_MILLIS`      | agent   | CPU count x 1000             | Declared capacity                                          |
| `DOCKFLEET_NODE_MEMORY_MB`       | agent   | `1024`                       | Declared capacity (placeholder until memory detection exists) |
| `DOCKFLEET_NODE_LABELS`          | agent   | none                         | `key=value,key=value`, used by `node_selector`             |
| `DOCKFLEET_DOCKER_HOST`          | agent   | `unix:///var/run/docker.sock` | `unix://` or loopback `tcp://` only; only the agent talks to Docker |
| `DOCKFLEET_ALLOWED_BIND_ROOTS`   | agent   | none                         | Host directories containers may bind-mount from (never the Docker socket or system paths) |
| `DOCKFLEET_ALLOWED_CAPABILITIES` | agent   | none                         | Capabilities containers may add (dangerous ones are always refused) |

## Testing

```sh
make test               # unit tests, -race, no external services
make cover              # per-package coverage
make test-integration   # PostgreSQL store tests; needs `make compose-up`
make test-docker        # Docker Engine tests; needs DOCKFLEET_TEST_DOCKER_HOST
```

Unit tests need no Docker, database or network. `internal/integration` runs real agent loops against a real manager over gRPC on an in-memory transport (registration, heartbeats, assignments and status reports, manager restart and reconnection, long outages, graceful deregistration, superseded sessions, incompatible protocol versions, bad credentials) and over real TLS handshakes: trusted nodes, untrusted client and server certificates, wrong server names, wrong node identity, expired and not-yet-valid certificates, certificates expiring on an open connection, missing client certificates, unenrolled and revoked nodes, certificate renewal, and the absence of any plaintext fallback. Other network-style tests use `bufconn`, fake HTTP round-trippers, or loopback ports 21290-21295 (`internal/manager`). PostgreSQL tests are behind the `integration` build tag and read `DOCKFLEET_TEST_DATABASE_URL`.

Generated protobuf code is not committed, so run `make generate` after a fresh clone and `make proto` after editing `proto/`.

## Limitations

- The Docker adapter (create, start, stop, remove, inspect, logs, stats) is implemented and tested, but the agent does not use it to run assigned workloads yet, so agents still report them `pending` and nothing reconciles failures. Workload assignments carry no ports, mounts or environment yet.
- Sessions are held in manager memory (one manager only); two agents configured with the same node ID will keep superseding each other.
- Scheduling is a single-manager spread strategy; a lock serializes placement in one process only.
- Node capacity is declared by the agent, not measured.
- Registered nodes are never deleted; dead nodes stay listed as `not_ready`.
- The admin API is authenticated by one shared bearer token (over HTTPS when TLS is configured). Certificate revocation is by enrollment only (no CRL/OCSP), and the development CA is not a production PKI.
- `go.sum` is generated by `make tidy`, which needs network access.

## License

No license has been chosen yet.
