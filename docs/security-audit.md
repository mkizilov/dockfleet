# Security and robustness audit

An audit of six areas, done as part of preparing DockFleet for self-hosted deployment. For each: what was examined and how, what was found, what was changed, and what remains. It is an internal review, backed by tests and tools, and not an independent assessment: have one done before you expose a fleet that matters.

How to read the status: **fixed** = changed in the commit that added this document, with a test that fails without the change (each was checked by disabling the fix); **ok** = examined, nothing to change, pinned by a test; **limitation** = a real gap that is documented rather than closed, with the reason.

| # | Area | Result |
| --- | --- | --- |
| 1 | [Container isolation defaults](#container-isolation) | ok, with five limitations |
| 2 | [API authorization](#api-authorization) | ok; limitations: one shared token, no throttling of the API token |
| 3 | [TLS validation](#tls-validation) | ok; no revocation lists |
| 4 | [Stale assignments](#stale-assignments) | one defect **fixed** (older generation obeyed), the rest ok |
| 5 | [Database migrations](#database-migrations) | ok; three audit tests added; no down-migrations |
| 6 | [Shutdown, restart and outage behavior](#shutdown-restart-and-outage-behavior) | one serious defect **fixed** (fleet failed over after a database outage) |
| 7 | [Dependencies and toolchain](#dependencies-and-toolchain) | 53 reachable known vulnerabilities **fixed** by upgrading; one accepted |

## Container isolation

**Examined:** `internal/docker/sdk.go` (`buildConfigs`, the one place a create request is made), `policy.go` (what a spec may ask for), `adapter.go` (which containers the agent may touch), the service validation in `internal/api/services_validate.go` (the manager-side limit at least as strict as the agent's), and the tests that pin each.

**What every managed container gets, whatever the spec says** (`TestBuildConfigs…`, and a test that decodes the request on the wire):

| Default | Value |
| --- | --- |
| privileged | never; refused by the policy and set false by the builder |
| capabilities | `--cap-drop ALL`; only the ones the operator allowed in `DOCKFLEET_ALLOWED_CAPABILITIES` are added, and 23 are refused whatever the operator allows (`SYS_ADMIN`, `NET_ADMIN`, `NET_RAW`, `SYS_PTRACE`, `SYS_MODULE`, `MKNOD`, `BPF`, …) |
| privilege escalation | `no-new-privileges:true`; `SecurityOpt` from a spec is refused, so the daemon's seccomp and AppArmor profiles cannot be switched off |
| resources | a CPU limit and a memory limit are mandatory (at least 6 MiB), swap equals memory (no swap), a PID limit (default 512, at most 32768) |
| namespaces | no host PID, IPC, UTS or user namespace; network `bridge` or `none`, never `host` or another container's |
| devices | none |
| mounts | none unless the operator allowed a host directory root; `/`, `/etc`, `/proc`, `/sys`, `/dev`, `/boot`, `/root`, `/run`, `/var/run`, `/var/lib/docker`, anything named `*.sock` or `docker.sock`, and any directory that contains one of these can never be allowed; symlinks are resolved before the check |
| published ports | on `127.0.0.1` only; the policy has a switch for other addresses but **no configuration reaches it** |
| restart | `no` at the engine; DockFleet's agent restarts with its own backoff and limit |
| logs | `json-file`, rotated (10 MiB × 3) |
| scope | the agent acts only on containers that carry its own node's DockFleet labels, addressed by workload ID; a container it did not start is never stopped, removed or adopted |

**Limitations (not closed):**

1. *Containers run as the image's user, which is often root inside the container.* Setting `user` in the service spec is validated and passed through, but nothing forces it. With all capabilities dropped and no-new-privileges, root in the container is much weaker than root on the host; it is still not non-root, and the user namespace is not remapped. **Mitigation:** require `user` in your own review of service definitions; run Docker with user-namespace remapping or rootless mode. A default that forced non-root would break images that expect root, so it is left to the operator.
2. *The agent is as powerful as the Docker Engine socket it holds* — that is root on the node. The policy protects the node from the **containers DockFleet starts**, not from the agent or from whoever controls the manager and the agents' certificates. Run the agent as its own unprivileged user in the `docker` group (the systemd unit does), prefer rootless Docker, and treat the manager's admin token and the CA key as keys to every worker.
3. *`read_only_root_fs` is opt-in* (`spec.read_only_root_fs`).
4. *Environment values are stored and returned in plain text* ([api.md](api.md)). There is no secret store: do not put credentials in a service spec. They are never written to events or logs.
5. *Images are not restricted.* No allow-list of registries, no requirement of a digest, no signature check; the worker's Docker Engine pulls whatever the spec names, with the credentials in the worker's own Docker configuration. Use digests in your specs and a private registry.

## API authorization

**Examined:** every route registered on the admin API; how the token is checked (`subtle.ConstantTimeCompare`, `Authorization: Bearer` only); the dashboard's separate session; what the health routes reveal.

**Added:** `TestEveryRouteExceptHealthRequiresTheAdminToken`. It reads the API's source with the Go parser, finds every `mux.Handle`/`HandleFunc` in the package, requires each to be wrapped in the authentication check except exactly `GET /healthz` and `GET /readyz`, and then sends eight kinds of wrong credential (none, malformed, wrong, different case of the scheme, Basic, the bare token, the token with a suffix, a double space) plus the token in the query string to **each route found** and expects `401`. A route added later without the check fails the test; it was verified by registering one route without it. The same is checked end to end over TLS against the running binary (`TestAdminAPIRefusesWhatIsNotAuthenticatedTLS`), including `/metrics`, the logs endpoint and the dashboard.

**Findings:**

- **ok** — no route other than the two health checks is open; the health checks say only `ok`, `ready` or `store unavailable`.
- **ok** — the token is compared in constant time and read only from the header; request bodies are bounded (1 MiB) and never logged; responses carry `Cache-Control: no-store`.
- **ok** — every request that can change something leaves an `audit` log record with its outcome (accepted, rejected, denied, failed), including attempts that were refused, with the request ID and the remote address of the connection (never a forwarding header).
- **limitation** — *there is one admin token.* No users, roles or read-only access: whoever holds it can do everything, and the audit actor is always `admin`. Give it to few tools and rotate it ([credentials.md](credentials.md)).
- **limitation** — *failed attempts with the API token are not throttled* (the dashboard's login is). The token is at least 16 characters; generate 32 random bytes and put the API behind your network's access control. `subtle.ConstantTimeCompare` returns early for a different length, so the token's length is observable; it is not secret.
- **limitation** — dashboard sessions are signed cookies that cannot be revoked; they end when they expire or the token is rotated.

## TLS validation

**Examined:** `internal/pki/tls.go` (server and client configurations), `protocol/server.go` (what a gRPC server may be built with), `etcd` client TLS, the health probe, and every use of `tls.Config` and `InsecureSkipVerify` in the module.

**Findings:**

- **ok** — TLS 1.3 is the minimum for the admin API, the agents' port, the agents' client and the health probe; there is no `InsecureSkipVerify` in any non-test code (searched).
- **ok** — the agents' port requires and verifies a client certificate (`RequireAndVerifyClientCert`); `NewGRPCServer` *refuses* a TLS configuration that does not (a test), so a future change cannot quietly weaken it. There is no TLS mode without client certificates.
- **ok** — the client verifies the manager against the configured CA and the expected name; the identity of a node is the URI SAN of its certificate, checked on every call, not what the request says; a certificate expiring on an open connection is rejected at the next call.
- **ok** — plaintext exists for loopback development only: a non-loopback listener without TLS, an agent dialing a remote manager without TLS, and an etcd endpoint over plain HTTP that is not loopback are all refused at configuration time. A failed TLS handshake is never retried in plaintext.
- **ok** — the new `healthcheck` probe trusts exactly one certificate, the one the process serves, follows no redirect and sends no credential.
- **Tested end to end against the binaries** (`TestAgentPortRequiresMutualTLS`, `TestAgentAdmissionAndRefusals`): a client with no certificate, one from another CA, a TLS 1.2 client, a client expecting another server name, plaintext bytes on the TLS port, an agent that does not trust the manager's CA, an agent with another node's certificate, a world-readable private key, a node that is not enrolled. Expired and not-yet-valid certificates, certificates expiring on an open connection, and renewal are covered in `internal/integration/mtls_test.go` (real handshakes in process).
- **limitation** — *no certificate revocation list or OCSP.* Revoke a node by revoking its enrollment (immediate, at its next heartbeat) and keep certificate lifetimes short (the default is 90 days).

## Stale assignments

A *stale assignment* is an instruction from a manager or a leader that no longer decides: an older generation of a workload, an old leader's answer, a lost node's replica that comes back.

**Examined:** how generations are made and acknowledged (`runner.Assign`, the reconciler), the epochs of high availability, leases and fencing on the agent, and what a returning node is told.

**Found and fixed:** *the agent obeyed any assignment, whatever its generation.* It never let its **acknowledgment** go back, but it took the instruction itself as given. A manager that restarted from an older backup of the database, or any source of a delayed answer, could therefore tell an agent to run a workload it had been told to remove, and the agent would. `runner.Assign` now keeps the assignment in force when the new set carries a lower generation for the same workload, and logs it (`TestAnAssignmentOlderThanTheOneInForceIsIgnored`; with the guard disabled the test fails: `create w1 start w1 stop w1 remove w1 create w1 start w1`). The same generation and higher ones are obeyed as before; a workload the stale set mentions for the first time is obeyed.

**ok** (each pinned by tests):

- Workload IDs are random and never reused; a workload is one container's lifetime (running, stopped, deleted), and is deleted only after its node confirmed the stop and reported no container.
- A replica moved off a lost node is stopped there with its generation raised and started elsewhere under a new ID; the lost node, when it returns, is told to remove what it no longer owns.
- An agent stops its own containers when its lease runs out, before the manager moves their replicas: the configuration refuses a give-up time shorter than lease plus heartbeat, and the end-to-end partition test checks the order on the wall clock (over several runs the agent fenced itself 0.58 s after the cut, with a 0.6 s test lease, and the first replacement was scheduled 2.2 to 2.4 s after, with a 2 s give-up time).
- With several managers, an answer to an agent carries the epoch of the leader's term; an agent refuses a lower epoch than it has heard, and a manager that stops leading while it prepares an answer sends `NOT_LEADER` instead ([high-availability.md](high-availability.md)). A test offers an agent instructions from a replaced control plane repeatedly; it takes none.
- The agent does nothing before the manager has answered once, and removes unassigned managed containers only after a grace period.

**limitation** — the highest epoch an agent has heard is kept in its memory; a restarted agent that reaches a replaced leader first can take one answer from it ([high-availability.md](high-availability.md#remaining-limitations-partitions-included)). A crashed agent process cannot stop its own containers, so a replica started elsewhere runs beside them until it returns ([failure-handling.md](failure-handling.md#limitations)).

## Database migrations

**Examined:** `internal/store/migrate.go` and the twelve migrations; the existing tests (checksums pinned, tampering refused, a database ahead of the binary, out-of-order, upgrade from the first three with data).

**Added** (`internal/store/postgres_integration_test.go`, run against PostgreSQL 16.4):

- `TestPostgresEveryUpgradePathEndsInTheSameSchema` — for each of the 11 earlier schema states, a database that starts there and is migrated ends with exactly the schema of a fresh database (columns, defaults, indexes, constraints compared). A migration that only works on an empty database, or only on top of the one before it, fails.
- `TestPostgresManagersStartingTogetherMigrateOnce` — eight managers call `Migrate` at the same instant on a new database: no error, each of the 12 migrations recorded once, one `control_plane` row.
- `TestPostgresOlderManagerRefusesANewerSchema` — a manager that does not know the schema refuses with a message that says to upgrade it, and changes nothing.

**ok** — a migration is a numbered file applied in one transaction with its bookkeeping row; its SHA-256 is recorded and a changed released migration stops the manager; `TestReleasedMigrationsAreUnchanged` pins the released ones; an advisory lock serializes concurrent starts.

**limitation** — *migrations only go forward.* There is no down-migration: to go back, restore a backup. And nothing stops an older manager that is **already running** from continuing on a migrated database (the check is at startup): stop all old managers before the first new one starts ([deployment.md](deployment.md#upgrades-backups-restores)). Rolling a cluster across a schema change is untested.

## Shutdown, restart and outage behavior

**Examined:** `Manager.Serve` and `Agent.Run` (what stops in which order, with what deadline), the election's shutdown path, and then **ran** them as real processes: `SIGTERM` to a manager and an agent, `SIGKILL` of each, the database crashed and restarted, etcd stopped and restarted, an agent frozen and woken, the network cut.

**ok, measured** (`internal/e2e`):

- `SIGTERM` makes the manager finish in-flight requests, end its log streams and exit **0** within `DOCKFLEET_SHUTDOWN_TIMEOUT` (it was well inside the limit); a leader gives up its lease first, so the other manager takes over in about 1.7 s instead of waiting for the lease to run out; the agent deregisters, exits 0 and leaves its containers running.
- A killed manager's restart finds the fleet as it was: no replica created twice or stopped, no node mistaken for dead, agents reconnect by themselves.
- A killed leader is replaced in about 6 s with a 5 s election TTL, and nothing that runs is touched. An outage of etcd stops the leader from deciding anything and, when etcd is back, a leader is elected again.
- A frozen agent is treated as lost, and when it wakes it stops what it had before anything else (its lease ran out) and takes its new orders.

**Found and fixed — a database outage failed the whole fleet over when the database came back.** While the database is down, agents' heartbeats cannot be recorded, so every node looks silent for the length of the outage. The first reconciliation pass after the database returned saw every node silent for longer than `DOCKFLEET_NODE_UNAVAILABLE_AFTER`, gave up on all of them and stopped and replaced every replica of every service, although nothing was wrong with any node. The end-to-end test crashed PostgreSQL for 6 s with a 4 s limit: both nodes were declared unavailable and all three replicas were replaced. This follows from the model for any outage longer than the give-up time (2 minutes by default), and from the default timings (a 5 s loop against 10 s heartbeats) even sooner.

*Fix:* the manager remembers when its store last failed (`internal/outage`; every failed scheduling or reconciliation pass and every failed readiness check marks it) and, like the time before it started, does not count silence from before that moment (`scheduler.Timing.Outages`). A node that really died during the outage is still found out, after the manager has watched it be silent for the full limit. Tests: a unit table for the timing, a reconciliation test whose control run (without the record) shows the mass failover, and the end-to-end outage test, which fails without the fix and passes with it.

**limitation** — for an outage of etcd longer than the node lease, agents stop their containers (that is the fencing doing its job; [high-availability.md](high-availability.md)). While the database is down the manager cannot schedule or reconcile; running containers are unaffected.

## Dependencies and toolchain

`govulncheck` (v1.1.4, which reports only vulnerabilities in code the binaries actually call) was run on the module as it stood. It reported **53 reachable vulnerabilities**, in the Go 1.23.12 standard library (`net/http`, `crypto/tls`, `crypto/x509`, `html/template`, `net/url`, `encoding/asn1`, `encoding/xml`, `net/textproto`, `encoding/pem`), `golang.org/x/net` v0.29.0, `golang.org/x/text` v0.18.0, `google.golang.org/grpc` v1.68.0 and `github.com/jackc/pgx/v5` v5.7.1. They had been left in place since the toolchain was first pinned to what the module graph then allowed.

**Fixed:** Go 1.26.10 (its published SHA-256 was checked) and `grpc` 1.83.2, `x/net` 0.60.0, `x/text` 0.42.0, `pgx` 5.9.2 (and what they pull in); `go.mod` now says `go 1.26.10`, the Dockerfile builds with it, and CI's `vulnerabilities` job runs `make vuln` on every change. The whole test suite, the end-to-end tests and the PostgreSQL tests were run again on the new toolchain.

A second run after the upgrade showed one more (GO-2026-6443, a server panic on a request with no authority header, fixed in grpc 1.83.2), which is why that is the version pinned. `make vuln` now passes with only the two accepted reports below.

**Accepted:** two reports against `github.com/docker/docker` v27.5.1 with no fixed version (GO-2026-4887, GO-2026-4883: an authorization-plugin bypass and a plugin-privilege validation error). Both are bugs in the Docker **daemon's** code; DockFleet imports only the client packages and never runs daemon code, which is why `govulncheck` can only point at package initialization. Run a patched Docker Engine on the workers; that is where those bugs are fixed.

## What the audit did not cover

Fuzzing of the gRPC and HTTP inputs beyond the table-driven bad-input tests; a review of the Docker Engine's own configuration on workers; the supply chain of the base image (pin it by digest); a penetration test; a performance or denial-of-service assessment beyond the bounds already in the code (message sizes, stream credits, series limits); anything on Windows.
