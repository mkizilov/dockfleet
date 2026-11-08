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

Agents dial the manager (gRPC, unary calls only). The manager never connects to agents. Every call has a server-side deadline (`DOCKFLEET_REQUEST_TIMEOUT`, default 10s) and the agent applies its own per-call deadline (`DOCKFLEET_RPC_TIMEOUT`, default 5s).

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
- **Liveness.** A node is `ready` while its last heartbeat is newer than `DOCKFLEET_NODE_TTL`. Deregistration backdates the heartbeat so the node stops being schedulable immediately.
- **Assignments.** `RegisterNodeResponse.assignments` and `HeartbeatResponse.assignments` are the complete set of workloads assigned to the node. A workload that disappears from the set is no longer assigned.
- **Status reports.** `HeartbeatRequest.statuses` is the complete set of workloads the agent knows about, each with a `WorkloadPhase` and a message. The manager stores `state`, `status_message` and `status_updated_at` on the workload and writes only when something changed. Reports for workloads not assigned to the reporting node are ignored. Because both sets are complete, a lost heartbeat is repaired by the next one.
- **Manager restart.** Sessions are in memory. After a restart the manager answers `NOT_REGISTERED`; the agent registers again and receives its assignments. Node and workload records are in the store and survive.

Until a container runtime exists, an agent reports every assigned workload as `WORKLOAD_PHASE_PENDING` with the message "accepted; container runtime is not implemented yet".

## Versioning

`ProtocolVersion {major, minor}` is sent in `RegisterNodeRequest` and returned in `RegisterNodeResponse`. This build speaks `1.1` (`protocol.VersionMajor` / `protocol.VersionMinor`); 1.1 added the `NOT_ENROLLED`, `IDENTITY_MISMATCH` and `CERTIFICATE_INVALID` error codes.

| Rule | Who checks |
| ---- | ---------- |
| Agent and manager must have the same major version | both |
| Agent minor must not exceed the manager minor | manager (a newer agent may use features the manager lacks) |
| A newer manager minor is accepted by an older agent | agent |

Bump the minor for backward-compatible additions (new optional fields or RPCs) and the major for breaking changes; upgrade managers before agents. A missing version counts as incompatible.

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

gRPC messages are capped at 1 MiB on the server. Per request: at most 32 labels (keys and values up to 128 bytes), 1000 workload statuses, status messages up to 512 bytes, workload IDs up to 64 bytes, node names up to 255 bytes.
