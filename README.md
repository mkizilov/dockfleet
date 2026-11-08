# DockFleet

DockFleet is a distributed container orchestration system written in Go. A central **manager** keeps cluster state in PostgreSQL (desired and observed state kept apart) and schedules workloads onto nodes by their free CPU and memory; **agents** on worker nodes register with the manager, heartbeat, receive their assigned workloads, run them as Docker containers and report status.

> **Status: early.** The manager-agent gRPC protocol (versioned registration, heartbeats, assignments, status reports) works end to end over mutual TLS with explicit node enrollment, and both processes shut down gracefully. A service definition now becomes running containers: the scheduler assigns its replicas to nodes, the reconciler retires the ones that should go (scale down, deletion, rollouts), and agents create, start, stop and remove the containers they are told about, restarting them per policy and reporting back. A change of spec is a rolling deployment ([docs/rollouts.md](docs/rollouts.md)): replicas of the new revision are created a few at a time within the service's `rollout` policy (`max_surge` extra replicas, `max_unavailable` missing ones), a replica counts as ready only when it runs and its health check passes, a rollout that has no room or no ready replacement waits and says why instead of breaking its policy, one that makes no progress for its deadline is reported failed, and it can be paused, resumed and rolled back. Replica counts are changed through the API: new replicas are created where there is room, a scale-down stops the replicas doing the least good (failed, unhealthy, on a quiet node) and keeps the healthy ones untouched, and `GET /v1/services/{name}/scale` reports the progress. Failures are handled too: containers are restarted with a growing wait and a limit, and when their health check fails; a node that goes quiet is first doubted (`suspect`) and then given up on (`unavailable`), its replicas are started on other nodes, and a lease and workload generations keep the old and the new copy from both being in charge; every step is visible in the API. The whole loop is tested against in-memory Docker engines; the tests against a real Docker daemon exist but need one ([docs/docker.md](docs/docker.md#tests)). See [docs/reconciliation.md](docs/reconciliation.md), [docs/failure-handling.md](docs/failure-handling.md) and [Limitations](#limitations).

## Layout

| Path                 | Purpose                                                                 |
| -------------------- | ----------------------------------------------------------------------- |
| `cmd/manager`        | Control-plane entry point                                               |
| `cmd/agent`          | Worker-node entry point                                                 |
| `cmd/pki`            | `dockfleet-pki`: development CA and certificate issuing                 |
| `internal/api`       | Administrative HTTP API (services, nodes, workloads, enrollments, events, health) |
| `internal/scheduler` | Placement decisions and their explanations (pure functions, no I/O) ([docs/scheduler.md](docs/scheduler.md)) |
| `internal/placement` | Carries decisions out: reads the cluster, runs the scheduler, saves assignments and explanations |
| `internal/reconcile` | Compares services with what agents report; stops replicas that should go, deletes them once their node confirms, records node health and gives up on lost nodes ([docs/reconciliation.md](docs/reconciliation.md), [docs/failure-handling.md](docs/failure-handling.md)) |
| `internal/agent`     | Node logic: register, heartbeat, retry, re-register; the runner that keeps the node's containers in line with its assignments |
| `internal/store`     | Persistence: repository interfaces, transactions, versioned migrations; PostgreSQL and in-memory implementations ([docs/state.md](docs/state.md)) |
| `internal/protocol`  | gRPC service, client, versioning, errors, bearer-token auth, TLS helpers |
| `internal/registry`  | Manager-side node registry: sessions, heartbeats, status reports        |
| `internal/pki`       | CA, certificates, node identity, validation, reloading TLS configs      |
| `internal/netutil`   | Loopback checks behind the plaintext-only-on-loopback rule              |
| `internal/integration` | Tests running simulated agents against a manager over gRPC           |
| `internal/docker`    | Agent-only Docker Engine adapter: managed containers, hardening policy (see [docs/docker.md](docs/docker.md)) |
| `internal/dockerfake` | In-memory `docker.Runtime` used by tests                               |
| `internal/manager`   | Wires the API, gRPC server and store; owns graceful shutdown            |
| `internal/config`    | Environment-based configuration with secure-default validation          |
| `internal/logging`   | Structured logging (`log/slog`, JSON or text)                           |
| `proto/`             | Protobuf definitions (`buf` module)                                     |
| `deploy/`            | Docker Compose files: development PostgreSQL, and a disposable Docker daemon for the Docker tests |
| `config/dev.env`     | Development-only configuration                                          |
| `docs/`              | [Architecture](docs/architecture.md), [Protocol](docs/protocol.md), [Mutual TLS and enrollment](docs/mtls.md), [Docker adapter](docs/docker.md), [Control-plane state](docs/state.md), [Services API](docs/api.md), [Scheduler](docs/scheduler.md), [Reconciliation](docs/reconciliation.md), [Failure handling](docs/failure-handling.md), [Rolling deployments](docs/rollouts.md) |

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

# Describe a service (desired state). Within a few seconds the scheduler assigns its replicas
# to nodes, the agents start the containers, and `status` shows where each replica is, whether
# its node acknowledged the instruction, what it reports, or why a replica could not be placed
curl -s -X POST -H 'Authorization: Bearer dev-api-token-0123456789' \
  -d '{"name":"site","replicas":3,"spec":{"image":"nginx:1.27","cpu_millis":500,"memory_bytes":134217728,"ports":[{"container_port":80}]}}' \
  http://127.0.0.1:21280/v1/services
curl -s -H 'Authorization: Bearer dev-api-token-0123456789' http://127.0.0.1:21280/v1/services/site
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
| `GET /v1/nodes`           | yes  | Registered nodes with `ready` / `not_ready` status (and why), allocatable and allocated resources |
| `GET /v1/nodes/{id}`      | yes  | One node                                                            |
| `POST /v1/workloads`      | yes  | Choose a node for a workload (desired state `running`); nothing is observed until its agent reports |
| `GET /v1/workloads`       | yes  | All workloads (including the scheduler's assignments of service replicas), each with its node, desired state and its `observed` report |
| `GET /v1/workloads/{id}`  | yes  | One workload                                                        |
| `POST /v1/services`       | yes  | Create a service definition: image, replicas, CPU and memory limits, environment, ports, health check, restart policy ([details](docs/api.md)) |
| `GET /v1/services`        | yes  | List service definitions (`include_deleted=true` adds deleted ones)  |
| `GET /v1/services/{name}` | yes  | One service definition, with its `ETag` and its scheduling `status`: where each replica was assigned, and why any has no node |
| `PUT /v1/services/{name}` | yes  | Replace a definition: a changed spec is a new revision, a changed replica count a scaling; `If-Match` guards against lost updates |
| `DELETE /v1/services/{name}` | yes | Delete a service (history is kept)                                |
| `GET /v1/services/{name}/revisions` | yes | Deployment history                                         |
| `GET /v1/services/{name}/scale` | yes | The wanted replica count and the progress towards it (`scaling_up`, `scaling_down`, `blocked`, `stable`, ...) |
| `GET /v1/services/{name}/rollout` | yes | The rollout policy, the progress of the current deployment (`progressing`, `paused`, `complete`, `failed`), and how earlier ones ended; see [docs/rollouts.md](docs/rollouts.md) |
| `POST /v1/services/{name}/pause`, `.../resume` | yes | Hold rollouts where they are, or let them go on (idempotent) |
| `POST /v1/services/{name}/rollback` | yes | Deploy an earlier revision's spec again as a new revision; the replicas that still run it are kept |
| `PUT /v1/services/{name}/scale` | yes | Set the replica count only (`{"replicas": 5}`); idempotent, so repeating a request changes nothing; see [docs/api.md](docs/api.md#scaling) |
| `GET /v1/events`          | yes  | Audit trail, newest first (`resource_type`, `resource_id`, `before`, `limit`) |
| `POST /v1/enrollments`    | yes  | Enroll a node ID (`{"node_id":"...","description":"..."}`); `409` if already enrolled |
| `GET /v1/enrollments`     | yes  | All enrollments                                                     |
| `GET /v1/enrollments/{id}` | yes | One enrollment                                                      |
| `DELETE /v1/enrollments/{id}` | yes | Revoke an enrollment; the node's session ends at its next heartbeat |

Errors are `{"error": "..."}`. A workload request that fits on no ready node returns `409` with the scheduler's reason for the request and for each node (`code`, `nodes`; [docs/api.md](docs/api.md#nodes-and-standalone-workloads)). A workload shows what was asked for (`desired_state`, `node_id`, ...) and, separately, what its agent reported (`observed`, `null` until the first report: `state` is one of `pending`, `running`, `succeeded`, `failed`, `stopped`, plus message, container ID, exit code, OOM kill, restart count and times). The two are never mixed: a report never changes what is wanted. See [docs/state.md](docs/state.md).

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
| `DOCKFLEET_DATABASE_URL`         | manager | required for `postgres`      | Migrations run at startup (safe with several managers starting at once) |
| `DOCKFLEET_INSTANCE_ID`          | manager | `manager-<random>`           | Names this manager in audit events and leases; set it for a stable identity |
| `DOCKFLEET_TLS_CERT_FILE`, `DOCKFLEET_TLS_KEY_FILE`, `DOCKFLEET_TLS_CLIENT_CA_FILE` | manager | unset | Set all three to enable TLS 1.3: HTTPS admin API and mutual TLS for agents (`DOCKFLEET_AGENT_TOKEN` is then ignored) |
| `DOCKFLEET_HEARTBEAT_INTERVAL`   | manager | `10s`                        | Sent to agents on registration                             |
| `DOCKFLEET_NODE_TTL`             | manager | `30s`                        | Node is `not_ready` and `suspect` after this long without a heartbeat; at least 2x the interval |
| `DOCKFLEET_NODE_LEASE`           | manager | `60s`                        | Lease each heartbeat renews; an agent that fences itself stops its containers when it runs out; at least 2x the interval |
| `DOCKFLEET_NODE_UNAVAILABLE_AFTER` | manager | `2m`                       | Silence after which a node is `unavailable` and its replicas are started elsewhere; at least the lease plus one interval, and longer than the node TTL. See [docs/failure-handling.md](docs/failure-handling.md) |
| `DOCKFLEET_SCHEDULER_INTERVAL`   | manager | `5s`                         | Time between scheduling passes that assign waiting replicas to nodes |
| `DOCKFLEET_RECONCILE_INTERVAL`   | manager | `5s`                         | Time between reconciliation passes that stop replicas that should go (scale down, deletion, rollout) and delete them once their node confirms; see [docs/reconciliation.md](docs/reconciliation.md) |
| `DOCKFLEET_SCHEDULER_CPU_OVERCOMMIT`, `DOCKFLEET_SCHEDULER_MEMORY_OVERCOMMIT` | manager | `1` | How many times a node's CPU or memory may be promised to workloads, from `1` (never more than the node has) to `8`. Raising it is a deliberate choice; see [docs/scheduler.md](docs/scheduler.md) |
| `DOCKFLEET_REQUEST_TIMEOUT`      | manager | `10s`                        | Deadline for each gRPC call                                |
| `DOCKFLEET_SHUTDOWN_TIMEOUT`     | manager | `15s`                        | Drain budget before connections are forced closed          |
| `DOCKFLEET_MANAGER_ADDR`         | agent   | `127.0.0.1:21281`            | Non-loopback requires `DOCKFLEET_TLS_CA_FILE`              |
| `DOCKFLEET_TLS_CA_FILE`, `DOCKFLEET_TLS_CERT_FILE`, `DOCKFLEET_TLS_KEY_FILE` | agent | unset | Set all three for mutual TLS (required for a non-loopback manager): CA that signed the manager certificate, and the agent's own certificate and key |
| `DOCKFLEET_TLS_SERVER_NAME`      | agent   | host of the manager address  | Name the manager certificate must carry                    |
| `DOCKFLEET_RPC_TIMEOUT`          | agent   | `5s`                         | Deadline for each call to the manager                      |
| `DOCKFLEET_FENCE_ON_LEASE_LOSS`  | agent   | `true`                       | Stop the node's containers when the manager's lease runs out, so a replica started elsewhere does not also run here; `false` keeps containers through any manager outage |
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
make test-integration   # PostgreSQL store, API, scheduling and reconciliation tests; needs `make compose-up`
make test-docker        # Docker Engine tests: the adapter and a service deployment; needs DOCKFLEET_TEST_DOCKER_HOST (`make docker-test-up` starts a disposable daemon on 127.0.0.1:21297)
```

Unit tests need no Docker, database or network. `internal/scheduler` has table-driven tests for resource exhaustion, offline nodes, deterministic placement (shuffled inputs give identical decisions), multiple replicas and conflicting constraints, all without a store or a clock; `internal/placement` runs the scheduling passes against the in-memory store (and PostgreSQL under the `integration` tag, including two managers scheduling at once). `internal/reconcile` tests the reconciliation plan as a table (including which replicas a scale-down chooses) and the whole control loop (deploy, repeated passes, scale down and up, scaling back and forth with exact counts, scaling while a node is unavailable or more than fits, rolling updates under every kind of policy, rollouts that fail or have no room, pause and rollback, deletion, a fixed host port needing `max_unavailable`) against simulated agents; `internal/agent` tests the runner against an in-memory Docker engine (creation, adoption after a restart, stopping and removal, bystander containers, orphans, image pulls, backoff, restart policies). `internal/integration` runs real agent loops against a real manager over gRPC on an in-memory transport (registration, heartbeats, assignments and status reports, a service deployed end to end with exact counts of created and removed containers, manager restart and reconnection, an agent restarting beside a crashed container, a removal waiting for an absent node, long outages, graceful deregistration, superseded sessions, incompatible protocol versions, bad credentials) and over real TLS handshakes: trusted nodes, untrusted client and server certificates, wrong server names, wrong node identity, expired and not-yet-valid certificates, certificates expiring on an open connection, missing client certificates, unenrolled and revoked nodes, certificate renewal, and the absence of any plaintext fallback. Failure handling is tested with crashed agents, cut networks, lost heartbeats, failing and unhealthy containers and returning nodes: node health as a table (`internal/scheduler`), failover and recovery in the control loop (`internal/reconcile`), restart waits, limits, health restarts and fencing (`internal/agent`), and, over gRPC with a journal that orders the actions of two nodes, a partitioned agent that stops its container before the replacement is created, a crashed agent cleaned up when it returns, a manager outage longer than the lease, and a container restarted to its limit (`internal/integration/failure_test.go`). Rolling deployments are tested the same way (`internal/integration/rollout_test.go`), with a sampler reading the store during the rollout: a successful rollout under three policies, replacements that crash on start (reported failed, old replicas untouched, then rolled back without restarting them), a rollout without room, pause and resume, and a rollback after a finished rollout. Replica scaling is tested through the REST API with real agents (`internal/integration/scale_test.go`): scale-up creating only what is missing, scale-down removing a failed replica first and leaving the rest alone, more replicas than a node holds (reported as blocked, completed when a node joins), repeated requests, and a scaling while a node is cut off. The same deployment flow against a real Docker daemon is behind the `dockerintegration` tag. Other network-style tests use `bufconn`, fake HTTP round-trippers, or loopback ports 21290-21295 (`internal/manager`). PostgreSQL tests are behind the `integration` build tag and read `DOCKFLEET_TEST_DATABASE_URL`: they create and drop a throwaway database per test on that server, so no data is shared or touched (migrations, upgrade from the previous schema, restart persistence, two managers on one database, constraints and index use; see [docs/state.md](docs/state.md)).

Generated protobuf code is not committed, so run `make generate` after a fresh clone and `make proto` after editing `proto/`.

## Limitations

- Agents create, start, stop and remove containers, restart them per policy (waits, limits, health checks) and report back, publish ports only on loopback, and offer no mounts or networks beyond the container policy's defaults. Stopping a workload removes its container and its logs. See [docs/reconciliation.md](docs/reconciliation.md#limitations).
- Failover has limits ([docs/failure-handling.md](docs/failure-handling.md#limitations)): a crashed agent process cannot stop its own containers, so a replica started elsewhere runs beside them until it returns; with fencing on (the default) a manager outage longer than the lease stops every container in the fleet until the manager is back; a replica whose container failed beyond its restart limit is not moved to another node; standalone workloads are not moved.
- Sessions are held in manager memory: an agent talks to one manager at a time and registers again if that manager forgets it. Two agents configured with the same node ID will keep superseding each other.
- The database is ready for several managers (placement lock, optimistic versions, leases, migration lock), but no leader election runs yet; scheduling and reconciliation passes on several managers queue on the placement lock instead. Service definitions have a REST API ([docs/api.md](docs/api.md)); the scheduler assigns their replicas ([docs/scheduler.md](docs/scheduler.md)), the reconciler retires the ones that should go, and a rollout replaces replicas within its policy ([docs/rollouts.md](docs/rollouts.md)). A node that is silent for `DOCKFLEET_NODE_UNAVAILABLE_AFTER` loses its replicas to other nodes, and its stopped workloads stay on record until it returns.
- Scheduling reserves each workload's declared CPU and memory limits (it does not measure usage), has no priorities, preemption or affinities, and checks only fixed host-port conflicts.
- Node capacity is declared by the agent, not measured.
- Registered nodes are never deleted; dead nodes stay listed as `not_ready`.
- The admin API is authenticated by one shared bearer token (over HTTPS when TLS is configured). Certificate revocation is by enrollment only (no CRL/OCSP), and the development CA is not a production PKI.
- `go.sum` is generated by `make tidy`, which needs network access.

## License

No license has been chosen yet.
