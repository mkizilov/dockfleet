# Manager-agent protocol

The wire contract is `proto/dockfleet/v1/fleet.proto` (package `dockfleet.v1`, service `NodeService`). This document explains the behavior around it. The `.proto` file is the authority on field names and numbers.

## Generating the Go bindings

Generated code is not committed. It is produced from pinned tool versions so every machine and CI get the same output:

```sh
make generate      # = make tools proto-lint proto
```

- `make tools` installs `buf`, `protoc-gen-go` and `protoc-gen-go-grpc` at the versions pinned in the `Makefile` into `.tools/bin` (needs network once).
- `make proto-lint` runs `buf lint` (STANDARD rules) on `proto/`.
- `make proto` runs `buf generate proto` using `buf.gen.yaml` and writes `internal/protocol/dockfleet/v1/*.pb.go` (git-ignored). No system `protoc` is needed.

Changing the protocol means editing the `.proto`, bumping the version if required (below), updating this document, and running `make generate`.

## Connection model

Agents dial the manager (gRPC: unary calls, and one long-lived stream for container logs, below). The manager never connects to agents. Every unary call has a server-side deadline (`DOCKFLEET_REQUEST_TIMEOUT`, default 10s) and the agent applies its own per-call deadline (`DOCKFLEET_RPC_TIMEOUT`, default 5s).

### Transport security and authentication

Full details, including credential distribution, are in [mtls.md](mtls.md).

- **Mutual TLS (any non-loopback deployment).** Both sides use TLS 1.3. The manager requires and verifies a client certificate; the agent verifies the manager certificate. The caller's identity is the node ID in the client certificate (URI SAN `spiffe://dockfleet/node/<id>`), and every `node_id` in a request must equal it (`PERMISSION_DENIED` / `IDENTITY_MISMATCH`). The validity window is re-checked on every call. The bearer token is not used.
- **Plaintext development mode (loopback only).** Calls carry `authorization: Bearer <agent token>`; calls without it fail with `UNAUTHENTICATED`. There is no certificate, so a caller may claim any enrolled node ID.
- **No plaintext fallback.** The manager refuses plaintext listeners on non-loopback addresses (configuration validation and again in `Manager.Serve`), a TLS server configuration without mandatory client certificates (`protocol.ErrClientAuthRequired`), and an agent refuses to dial a non-loopback manager without TLS (`protocol.ErrInsecureRemote`). A TLS handshake failure is a failed call, never a retry without TLS.
- **Enrollment.** Whatever the transport, only node IDs an administrator enrolled may register, and enrollment is re-checked on every heartbeat (`PERMISSION_DENIED` / `NOT_ENROLLED`).

## Session lifecycle

```
agent                                          manager
  |  RegisterNode(version, identity, capacity)   |
  |--------------------------------------------->|  check version, validate, persist node,
  |  session_id, interval, assignments, version  |  create session (replaces any older one)
  |<---------------------------------------------|
  |                                              |
  |  Heartbeat(node_id, session_id, statuses)    |  every heartbeat_interval_ms
  |--------------------------------------------->|  refresh liveness, record statuses
  |  assignments (complete desired set)          |
  |<---------------------------------------------|
  |  ...                                         |
  |  DeregisterNode(node_id, session_id)         |  clean shutdown only
  |--------------------------------------------->|  drop session, mark node unavailable
```

- **Identity.** `node_id` is chosen by the operator, must match `^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$` and must be unique and stable. A manager-issued `session_id` identifies one registration. If two agents use the same `node_id`, the newer registration wins and the other keeps being told to register again, so duplicates fight visibly in the logs; fix the configuration.
- **Capacity.** `NodeCapacity` is what the agent declares (CPU in millicores, memory in bytes). It is not measured.
- **Liveness.** A node is `ready` while its last heartbeat is newer than `DOCKFLEET_NODE_TTL`. Deregistration clears the heartbeat (and records when the agent left) so the node stops being schedulable immediately. A node that stays silent is `suspect`, then `unavailable`; see [failure-handling.md](failure-handling.md).
- **Lease.** `RegisterNodeResponse.lease_ms` is how long each successful heartbeat keeps the session's lease, counted from when the agent *sent* the request (0: none granted). An agent that fences itself stops its containers when its lease runs out. The manager gives up on a node only after the lease and one more heartbeat interval, so a live agent has stopped its containers before the manager starts the replicas elsewhere.
- **Assignments.** `RegisterNodeResponse.assignments` and `HeartbeatResponse.assignments` are the complete set of workloads placed on the node, each an instruction: `desired_state` `RUNNING` (create the container if there is none, start it, keep it running by its restart policy) or `STOPPED` (stop it and remove it). An assignment carries everything needed to run the container (`image`, `command`, `env`, `ports`, CPU and memory limits, `restart_policy` with its limit and backoff, `health_check`) and a `generation`: 1 when the workload is created, increased whenever its desired state changes. A workload leaves the set only when the manager deletes it, after the node confirmed that the container is gone. The set is a statement of state, not a command stream, so an instruction can be received twice, late or after a restart and the result is the same ([reconciliation.md](reconciliation.md)).
- **Workload IDs** are random, unique in the fleet and never reused; each names exactly one container's lifetime on one node, and the agent addresses containers by nothing else.
- **Status reports.** `HeartbeatRequest.statuses` is the complete set of workloads the agent knows about, each with a `WorkloadPhase`, a message, the container ID (empty when the node has none), the exit code of a container that exited, whether it was OOM-killed, the restart count, `health` (the container's health check result: empty, `starting`, `healthy` or `unhealthy`) and `acknowledged_generation`. The manager stores each report as the workload's *observation* and writes only when something changed; an observation never alters desired state (see [state.md](state.md)). Reports for workloads not placed on the reporting node are ignored.
- **Acknowledgments.** `acknowledged_generation` is the highest `generation` of the workload's assignment the agent has received. The manager keeps the highest value it has seen, ignores a value above the workload's own generation, and treats a report as describing an instruction only once that instruction is acknowledged. An agent that does not acknowledge (protocol 1.0 and 1.1) sends 0.
- **Both sets are complete**, so a lost heartbeat or response is repaired by the next one.
- **Manager restart.** Sessions are in memory. After a restart the manager answers `NOT_REGISTERED`; the agent registers again and receives its assignments. Node and workload records are in the store and survive, and the agent's containers were never touched.
- **Agent restart.** The agent registers again, receives the assignments, lists its containers and adopts those that match. Stopping the agent does not stop containers.

## Versioning

`ProtocolVersion {major, minor}` is sent in `RegisterNodeRequest` and returned in `RegisterNodeResponse`. This build speaks `1.4` (`protocol.VersionMajor` / `protocol.VersionMinor`). 1.1 added the `NOT_ENROLLED`, `IDENTITY_MISMATCH` and `CERTIFICATE_INVALID` error codes. 1.2 made assignments instructions: `WorkloadAssignment` gained `generation`, `desired_state`, `command`, `env`, `ports` and `restart_policy`, and `WorkloadStatus` gained `acknowledged_generation`, `container_id`, `exit_code`, `oom_killed` and `restart_count`. 1.3 added failure handling: `RegisterNodeResponse.lease_ms`, `RestartPolicy.backoff_seconds` and `max_backoff_seconds`, the `HealthCheck` message in `WorkloadAssignment.health_check`, and `WorkloadStatus.health`. 1.4 added container logs: the `Logs` RPC and its messages (`LogsRequest`, `LogsResponse` and what they carry). All are optional fields or new calls, so older peers ignore them; an unspecified `desired_state` means `RUNNING`, and an agent that is not sent a lease never fences itself.

| Rule | Who checks |
| ---- | ---------- |
| Agent and manager must have the same major version | both |
| Agent minor must not exceed the manager minor | manager (a newer agent may use features the manager lacks) |
| A newer manager minor is accepted by an older agent | agent |

Bump the minor for backward-compatible additions (new optional fields or RPCs) and the major for breaking changes; upgrade managers before agents. A missing version counts as incompatible.

## Container logs

`Logs` is a bidirectional stream the agent opens after it registered and keeps open for the session ([logs.md](logs.md) describes what it is for and the limits that bound it). The manager never connects to agents, so the requests for logs travel down the stream the agent opened.

| Direction | Message | Meaning |
| --------- | ------- | ------- |
| agent → manager | `LogsHello` | First, and only first: the `node_id` and `session_id` the stream speaks for. |
| agent → manager | `LogBatch` | Lines of one request (`request_id`), oldest first, each with its time, stream and text; `history_complete` marks the last batch of the history. |
| agent → manager | `LogEnd` | The request is over: `COMPLETE`, `NOT_FOUND`, `CANCELED`, `BUSY` or `ERROR`. |
| manager → agent | `LogOpen` | Read the logs of `workload_id`: `tail` lines (negative: all) after `after_unix_nano`, `follow`, at most `max_line_bytes` per line, with `credit_bytes` of credit. |
| manager → agent | `LogCredit` | The agent may send `bytes` more of a request. A line costs its text plus 64 bytes. |
| manager → agent | `LogCancel` | End a request. |

- **Authentication.** The stream is authenticated like every call: the bearer token without TLS, the verified client certificate with mutual TLS (`UNAUTHENTICATED` otherwise). The hello's `node_id` must equal the certificate's identity (`PERMISSION_DENIED` / `IDENTITY_MISMATCH`), the node must be enrolled and `session_id` must be its current session (`NOT_ENROLLED`, `NOT_REGISTERED`, `SESSION_SUPERSEDED`). A stream that sends no hello within 10 seconds, or starts with anything else, is ended with `INVALID_REQUEST`.
- **Bound to the session.** The manager checks the session and the enrollment every 5 seconds and ends the stream with the matching error when they no longer hold. The agent opens a new stream after it registers again; an error that says the session is over (or `UNIMPLEMENTED`: an older manager, or one with logs off) is not retried within the session.
- **Flow control.** The agent sends a request's lines only while it has credit for them; the manager returns credit as the reader takes lines out. Lines beyond the credit end the request. Streams have no deadline, and the manager's periodic recycling of connections ends them like any other: the agent opens the stream again and the manager resumes the requests that follow.
- **One stream per node.** A newer stream of the same node replaces the older one. The manager only accepts lines and ends for requests it opened on that very stream.

## Errors

Failures an agent can act on carry a `ProtocolError` in the gRPC status details (`protocol.Error` in Go; `protocol.ErrorCode(err)` reads it). The `supported_version` field holds the sender's own version.

| `ProtocolErrorCode` | gRPC code | Meaning | Agent behavior |
| ------------------- | --------- | ------- | -------------- |
| `INCOMPATIBLE_VERSION` | `FAILED_PRECONDITION` | Version rules above violated | Fatal: `Run` returns the error |
| `INVALID_REQUEST` | `INVALID_ARGUMENT` | Malformed request or limit exceeded | Fatal on registration |
| `NOT_REGISTERED` | `FAILED_PRECONDITION` | No session for this node (e.g. manager restarted) | Register again |
| `SESSION_SUPERSEDED` | `FAILED_PRECONDITION` | A newer registration replaced this session | Register again |
| `NOT_ENROLLED` | `PERMISSION_DENIED` | The node ID has no active enrollment (never enrolled, or revoked) | Keep retrying with backoff and log an error; an administrator enrolls the node |
| `IDENTITY_MISMATCH` | `PERMISSION_DENIED` | `node_id` differs from the client certificate identity | Fatal |
| `CERTIFICATE_INVALID` | `UNAUTHENTICATED` | Client certificate expired, not yet valid, or without a node identity (found on an open connection) | Keep running; a renewed certificate is used on reconnect |
| (none) | `UNAUTHENTICATED` | Bad or missing token (plaintext mode) | Fatal |
| (none) | `UNAVAILABLE` | Includes TLS handshake failures: untrusted, expired or wrongly named certificates | Retry; the cause is in the error text |
| (none) | `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `INTERNAL` | Transient | Retry: backoff (1s to 30s) while registering, next tick while heartbeating |

An agent that exits on a fatal error leaves restarting it to its supervisor, which makes version skew and bad credentials loud instead of an endless retry loop.

## Limits

On the log stream a batch carries at most 256 lines and 256 KiB (a line's text at most 64 KiB), identifiers are 1-64 characters, and a request starts with at most the window the manager grants (256 KiB by default).

gRPC messages are capped at 1 MiB on the server (requests). Per request: at most 32 labels (keys and values up to 128 bytes), 1000 workload statuses, status messages up to 512 bytes, workload IDs up to 64 bytes, container IDs up to 128 bytes, node names up to 255 bytes. An agent accepts a response of up to 32 MiB, which bounds the assignments of one node, environments included.
