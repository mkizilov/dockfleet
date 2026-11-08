# Docker Engine adapter

`internal/docker` is the agent's adapter for its **local** Docker Engine. It can list, create, start, stop and remove DockFleet's containers, read their state and logs, and report resource usage. The agent's runner (`internal/agent`) drives it from the manager's assignments: it creates, starts, stops and removes the containers of the workloads assigned to the node and nothing else ([reconciliation.md](reconciliation.md)). The runner depends on `docker.Runtime` only, so it is tested against `internal/dockerfake`, an in-memory engine that behaves like this adapter.

## Who talks to Docker

Only the agent. The manager never connects to Docker and never needs a Docker socket:

- `internal/manager`, `internal/api`, `internal/registry`, `internal/store`, `internal/scheduler`, `internal/placement`, `internal/reconcile`, `internal/protocol` and `cmd/manager` do not import `internal/docker` or any `github.com/docker/...` package. `TestManagerSideNeverImportsDocker` parses their imports (transitively) and fails the build if that changes.
- The manager's configuration has no Docker settings, and `deploy/` files must not mention `docker.sock` (also tested).
- Agents reach the manager only over gRPC (mutual TLS). The adapter accepts a `unix://` socket or a loopback `tcp://` address; any other TCP host is refused because the Engine API is unauthenticated and equivalent to root on the host (`DOCKFLEET_DOCKER_HOST`, default `unix:///var/run/docker.sock`).
- The agent process needs access to the socket, which is root-equivalent on that machine. Run it as a dedicated user with access to the socket and nothing else, and treat the worker host accordingly.

## Interface

`docker.Runtime` is what the rest of the agent should depend on; `*docker.Adapter` implements it, and tests substitute fakes. `docker.Client` (`Ping`, `Close`) is the part the agent loop uses today.

| Method | Behavior |
| ------ | -------- |
| `List` | Managed containers of this node (any state), ordered by workload ID |
| `PullImage` | Pulls an image; stream errors are reported |
| `Create` | Validates the spec against the policy, then creates (does not start) `dockfleet-<workload-id>` |
| `Start`, `Stop`, `Remove` | By workload ID; `Stop` takes a timeout, `Remove` a `force` flag and also deletes the container's anonymous volumes |
| `Inspect` | Running/exit state, OOM kill, restart count, health, start/finish times |
| `Logs` | Demultiplexed stdout/stderr chunks in order, bounded: at most 10000 lines (default 100) and 8 MiB (default 1 MiB), `Truncated` set when output was dropped |
| `Stats` | One sample: CPU time and percentage (when the daemon supplies a previous sample), working-set memory and limit, PIDs, network bytes. It takes about a second because the daemon samples twice |

Errors are sentinels to test with `errors.Is`: `ErrNotFound`, `ErrConflict`, `ErrStateConflict`, `ErrImageNotFound`, `ErrAmbiguous`, `ErrInvalid`, and `ErrRejected` for policy violations.

## Identifying DockFleet's containers

Every container the adapter creates carries these labels. They are a stable contract: never rename them, only add new ones.

| Label | Value |
| ----- | ----- |
| `io.dockfleet.managed` | `true` |
| `io.dockfleet.schema` | `1` (bumped if a label's meaning changes) |
| `io.dockfleet.node-id` | the creating node's ID |
| `io.dockfleet.workload-id` | the manager's workload ID |
| `io.dockfleet.workload-name` | the workload name (informational) |

Containers are addressed **only by workload ID**, never by Docker ID or name. Each operation first lists containers filtered by `managed=true`, this `node-id` and the `workload-id`, and then acts on the Docker ID of the one container it found. The adapter also re-checks the labels on everything the daemon returns, so a container that lacks them is never acted on even if a filter were ignored; `Inspect` checks them again. Consequences:

- Unrelated containers (no labels, other labels, `managed=false`, another node's) are invisible to `List` and unreachable by any operation (`ErrNotFound`). Passing an unrelated container's ID or name as a workload ID cannot reach it.
- If a container named `dockfleet-<id>` exists but is not managed, `Create` fails with `ErrConflict` and leaves it alone.
- Two managed containers for one workload ID is an anomaly (`ErrAmbiguous`) and nothing is touched.

## Rejected by default

`Create` runs the spec through a `Policy` first. A rejected spec never reaches Docker, and the error lists every violation (`errors.Is(err, docker.ErrRejected)`). The default policy (`docker.DefaultPolicy()`) rejects:

- privileged mode (always, there is no override);
- added capabilities; every container also starts with all capabilities dropped and `no-new-privileges`;
- host devices, and any namespace sharing (`PidMode`, `IpcMode`, `UTSMode`, `UsernsMode`), `SecurityOpt` overrides such as `seccomp=unconfined`;
- network modes other than `bridge` (default) and `none`: `host` and `container:*` are refused;
- **bind mounts**: all of them, unless the agent configures allowed roots. Even then, never the Docker socket (any path ending in `.sock`), `/`, `/etc`, `/proc`, `/sys`, `/dev`, `/boot`, `/root`, `/run`, `/var/run`, `/var/lib/docker`, `/var/lib/containerd`, `/var/lib/kubelet`, anything inside them, or any directory that contains one of them (mounting `/var` would expose `/var/run/docker.sock`). Symlinks are resolved and the real target is checked too. Policy construction refuses roots that are, contain or sit inside those paths;
- named volumes not called `dockfleet-<name>`, so unrelated volumes stay untouched;
- published ports on non-loopback addresses (ports default to `127.0.0.1`);
- missing CPU or memory limits (every container runs limited, swap disabled, with a PID limit of 512 unless lowered or raised up to 32768);
- a health check that is not bounded: the command is the exec form (a leading `CMD`, `CMD-SHELL` or `NONE` is refused, so there is never a shell string), 1 to 32 arguments of at most 1024 bytes, an interval of 1 s to 1 h, a timeout of 1 s to 5 min, a start period up to 1 h and 1 to 10 retries. Docker runs it inside the container, with the container's own privileges, and the container's state then carries `starting`, `healthy` or `unhealthy`, which `Inspect` reports and the agent acts on ([failure-handling.md](failure-handling.md));
- malformed workload IDs, image references, environment names, users, mount targets, and oversized lists.

The Docker layer (`sdk.go`) applies the same hardening independently of the spec: privileged off, `CapDrop: ALL`, `no-new-privileges`, no restart policy, bounded `json-file` logs, no host namespaces, no binds outside `Mounts` with `rprivate` propagation, and `bridge`/`none` networking only. `TestBuildConfigsIgnoresUnsafeFieldsEvenIfPolicyWereBypassed` checks that unsafe fields cannot leak through.

### Relaxing the policy

The agent reads two optional settings and passes them to `docker.NewPolicy`; the always-denied sets above still apply and invalid values stop the agent at startup.

| Variable | Meaning |
| -------- | ------- |
| `DOCKFLEET_ALLOWED_BIND_ROOTS` | Comma-separated absolute directories bind mounts may live under (default: none) |
| `DOCKFLEET_ALLOWED_CAPABILITIES` | Comma-separated capabilities containers may add (default: none). Never allowed: `ALL`, `SYS_ADMIN`, `SYS_PTRACE`, `SYS_MODULE`, `SYS_RAWIO`, `SYS_BOOT`, `SYS_TIME`, `SYS_RESOURCE`, `DAC_READ_SEARCH`, `NET_ADMIN`, `NET_RAW`, `MKNOD`, `SETFCAP`, `MAC_ADMIN`, `MAC_OVERRIDE`, `AUDIT_CONTROL`, `BPF`, `PERFMON`, `SYSLOG`, `LINUX_IMMUTABLE`, `CHECKPOINT_RESTORE`, `WAKE_ALARM`, `BLOCK_SUSPEND` |

`PolicyConfig.AllowPublicPorts` exists for programmatic use but is not exposed as a setting yet. Assignments carry the service's ports, which the agent publishes on loopback (`127.0.0.1`) only; a spec that asks for more is refused by the policy and reported `FAILED`.

## Docker SDK

The adapter uses `github.com/docker/docker` (pinned in `go.mod`, with `github.com/docker/go-connections`), the Docker Engine SDK for Go. All SDK use is in `sdk.go`; the rest of the package sees a small internal interface (`backend`) with plain Go types. If the SDK moves to another module path such as `github.com/moby/moby/client`, only `sdk.go` and its test need to change. The client ignores `DOCKER_HOST` and the other `DOCKER_*` environment variables, so the daemon is always the configured one.

## Tests

- `adapter_test.go`: the adapter against an in-memory fake backend that deliberately ignores label filters: ownership, unrelated containers, ambiguity, conflicts, validation, defaults.
- `policy_test.go`: every rejection, socket and symlink tricks, allowed roots and capabilities.
- `logs_stats_test.go`: log demultiplexing and limits, stats parsing (cgroup v1 and v2).
- `sdk_test.go`: the real SDK code against a fake Docker Engine HTTP transport (no daemon, socket or port): request shapes, label filters, the hardened create body, error mapping.
- `boundary_test.go`: the manager side never imports Docker.
- `docker_integration_test.go` (build tag `dockerintegration`): the full lifecycle against a real daemon, including a bystander container that must survive untouched. `internal/integration/docker_test.go` (same tag) deploys a service through the whole control loop on a real daemon. Neither is part of `make test`:

```sh
make docker-test-up                         # a disposable daemon on 127.0.0.1:21297 (Docker in Docker)
export DOCKFLEET_TEST_DOCKER_HOST=tcp://127.0.0.1:21297
make test-docker
make docker-test-down                       # deletes the daemon, its images and containers
```

They pull `alpine:3.20` (override with `DOCKFLEET_TEST_DOCKER_IMAGE`), create containers whose names start with `dockfleet-` for node IDs starting with `itest-` or `bystander-`, and remove everything they create. The disposable daemon keeps the tests off the host's Docker; `DOCKFLEET_TEST_DOCKER_HOST` may name any other local daemon, but never run them against one that holds containers you care about. CI runs them in a job with the runner's Docker.
- `internal/dockerfake`: the in-memory engine the agent and integration tests use. It applies the container policy, addresses containers by workload ID, refuses duplicates and name conflicts, requires images to be pulled, and holds bystander containers that no operation can reach.
