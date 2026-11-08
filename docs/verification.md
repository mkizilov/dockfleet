# Verification status

This is the one place that says what DockFleet does, how sure we are, and why. It was written against commit `a2bf862` plus the changes of the commit that added this file, and every "verified" below names a command that was run and passed in the environment described here. Anything not run is marked **unverified**, even where the code is written and the test exists. [ROADMAP.md](../ROADMAP.md) says what to do about the gaps, in order.

## Words used

| Word | Meaning |
| --- | --- |
| **Implemented** | The code exists and is reachable from the shipped binaries. |
| **Verified** | A test or command listed in this document ran here and passed, and exercises the behavior itself rather than a stand-in for it. |
| **Partial** | Works as described, with gaps that are named in the same row. |
| **Unverified** | Written, and often tested statically or against a stand-in, but never run for real. Do not rely on it until it has been. |

A row can be implemented and still partial or unverified. "Verified against an in-memory engine" is stated as such: it is evidence about DockFleet's logic, not about Docker.

## The environment the checks ran in

| | |
| --- | --- |
| Machine | macOS on Apple silicon (darwin/arm64), no Linux host, no systemd |
| Go | 1.26.10 (go.dev release, checksum verified), project-local under `.tools/go` |
| PostgreSQL | 16.4, a relocatable build under `.tools/postgres`, loopback only, one server |
| etcd | 3.5.35, one member, under `.tools/etcd`, loopback only |
| Docker | **none**: no daemon was reachable and the host's was not used |
| Network | available for module and tool downloads only |

## Checks that ran

All commands run from the repository root with `GOTOOLCHAIN=local GOFLAGS=-p=2` and the project's `.tools/go/bin` and `.tools/bin` on `PATH`.

| Command | What it covers | Result |
| --- | --- | --- |
| `make check` | `gofmt` (including `deploy/`), `go vet` for the default build and with every test tag (`integration`, `etcdintegration`, `dockerintegration`, `e2e`), the secret scan, and `go test -race ./...` | passes: in a counted run of the same suite (`go test -race -count=1 -v ./...`), 546 top-level tests, 849 subtests, 0 failed, 0 skipped, 27 packages |
| `go test -race -tags integration ./internal/store/... ./internal/api/... ./internal/placement/... ./internal/reconcile/... ./internal/integration/...` against PostgreSQL 16.4 (`DOCKFLEET_TEST_DATABASE_URL`) | the store conformance suite on both stores, migrations, the API, scheduling and reconciliation on PostgreSQL, two managers on one database, multi-manager scenarios on PostgreSQL | passes: 140 top-level tests, 810 subtests, 0 failed, 0 skipped, 5 packages |
| `make test-etcd` against etcd 3.5.35 (`DOCKFLEET_TEST_ETCD_ENDPOINTS`) | the election client's contract against a real etcd, and the multi-manager scenarios with a real etcd | passes: 5 top-level tests, 6 subtests passed, 1 skipped (it must move a clock, which a real etcd does not allow; it runs against the fake) |
| `make test-e2e` | the real `dockfleet-manager` and `dockfleet-agent` binaries as processes over mutual TLS from `dockfleet-pki`, credentials as files, PostgreSQL and etcd started by the test (see below) | passes: 12 scenarios, 6 subtests, 0 skipped |
| `make cross-build` | the three binaries compile statically for linux/amd64, linux/arm64 and darwin/arm64 | passes (compiled, not run, except darwin/arm64) |
| `make tidy-check` | `go.mod` and `go.sum` are what `go mod tidy` writes | passes |
| `make vuln` (`govulncheck` v1.1.4) | known vulnerabilities in code the binaries call | passes with two accepted reports, both daemon-side bugs in `github.com/docker/docker` v27.5.1 that have no fixed version ([security-audit.md](security-audit.md#dependencies-and-toolchain)) |

Repeated runs, for flakes, on the final tree: the whole `internal/integration` package eight times under `-race` (passed), and `TestCleanShutdownDeregistersNode` 300 times (passed; before it was fixed it failed in 2 of 60 runs and in 1 of 300, see below).

Mutation checks, in which the protection under test was disabled, the test was seen to fail, and the protection was restored (done when each protection was written, on code that has not changed since): the agent's refusal of a lower epoch, the agent's refusal of a lower generation, the manager's grace for a store outage (end-to-end and unit), `FOR SHARE` in the epoch check on PostgreSQL, a route registered without authentication, and the Docker socket added to a Compose file.

### What the end-to-end suite is, and is not

`internal/e2e` builds the manager and the agent from `./cmd/...`, starts them as separate processes with configuration only from the environment and credential files, and drives them through the REST API. The agent is built with `-tags e2efake`, which keeps its containers in memory, because there is no Docker daemon here; a test pins that no build without the tag contains it. So the suite verifies everything *except* the container engine: configuration, certificates, enrollment, the control loops, failure handling, shutdown, the health endpoints, leadership. Its scenarios are: deployment, scaling (a repeated request, past capacity), a rolling update sampled against its surge and unavailability limits, rollback, a crashed agent and its recovery, a network partition (the agent stops its containers before the manager moves their replicas: 0.58 s after the cut against 2.2 to 2.4 s for the first replacement, with test-sized settings), a frozen agent, a clean agent restart, a manager crash and a graceful `SIGTERM` stop, a database crash, a leader crash (4.5 to 6.2 s to a new leader with a 5 s election TTL on one machine) and a clean handoff (1.6 to 1.7 s), an etcd outage, and the refusals. These timings are what the tests observed, not guarantees.

## Status by area

| Area | Status | Evidence | What is missing |
| --- | --- | --- | --- |
| Manager–agent protocol: versions, registration, heartbeats, assignments, reports | **Verified** | `internal/protocol`, `internal/integration` (in-memory transport, real handshakes), `internal/e2e` (real sockets) | compatibility is tested between this build and itself only; no earlier release exists to test against |
| Mutual TLS, identity binding, enrollment | **Verified** | `internal/pki`, `internal/protocol`, `internal/integration/mtls_test.go`, and `internal/e2e/security_test.go` against the binaries: no certificate, a foreign CA, TLS 1.2, plaintext, another node's certificate, a world-readable key, an unenrolled node | no revocation list (by design; revoke by enrollment); the CA is a development CA |
| State: in-memory and PostgreSQL stores, migrations | **Verified** | the conformance suite on both stores, every upgrade path ends in the same schema, managers migrating together, an older manager refusing a newer schema | only PostgreSQL 16.4 and one server; the documents say 13 or newer, which is not tested |
| Scheduler | **Verified** | table-driven tests including shuffled inputs, `internal/placement` on both stores | nothing above three nodes; priorities, affinities and preemption do not exist |
| Reconciliation, scaling, rolling updates, rollback, pause and resume | **Verified** against an in-memory engine; **unverified** against Docker | unit tables, control-loop tests on both stores with exact create and stop counts, `internal/e2e` with the real binaries | a real daemon: the deployment test for it is written and has not run |
| Failure handling: suspect and unavailable nodes, leases and fencing, restart limits and backoff, health-driven restarts | **Verified** against an in-memory engine | `internal/reconcile`, `internal/agent`, `internal/e2e` (crash, partition, freeze) | the real daemon runs the health checks and reports exit codes; a replica that failed beyond its limit is not moved; restart counts are lost when the agent restarts ([ROADMAP.md](../ROADMAP.md) steps 3 and 5) |
| Docker adapter and container hardening | **Partial** | request construction is read off the wire in tests against a stubbed Docker HTTP API; the policy has tables for every refusal | the hardening is not read back from a real container (`docker inspect`), and the lifecycle test against a real daemon is written and has not run: **unverified** |
| Container logs: streaming, history, following, filters, cursors, bounded memory, permissions | **Verified** against an in-memory engine and a stubbed SDK | `internal/logstream`, `internal/agent`, `internal/api`, `internal/integration/logs_test.go` | logs of a real container; on a follower of a high-availability cluster a query reports every node unavailable |
| Metrics, dashboard, audit trail | **Partial** | `internal/metrics`, `internal/api`, `internal/dashboard`, `internal/manager` over real sockets; the fleet gauges are compared with what the API says | utilization is the reservation made by declared limits, not measured use; the dashboard has not been looked at in a browser and the metrics have not been scraped by a Prometheus |
| High availability: leader election, fencing epochs, followers, leadership events | **Verified** with one etcd member | `internal/election`, `internal/etcd` against the fake and a real etcd, the store's epoch checks on PostgreSQL, `internal/integration` and `internal/e2e` (crash, handoff, etcd outage, a replaced control plane ignored by agents) | a multi-member etcd, delay and loss between components, clock skew, TLS to a non-loopback etcd: **unverified** |
| Credentials as files, health endpoints, the `healthcheck` subcommands | **Verified** | `internal/config`, `internal/healthcheck`, `internal/agent`, `cmd/agent`, and the binaries in `internal/e2e` | |
| Container images (`Dockerfile`, `.dockerignore`) | **Unverified** | static tests only: the pins agree with the `Makefile` and `go.mod`, the stages run as a numeric non-root user and name the health check, no credential in `ENV` or `ARG` | the images have never been built or run |
| Compose file for the manager | **Unverified** | it parses as YAML and a static test keeps its hardening | never run |
| systemd units | **Unverified** | a static test keeps their sandbox settings | never loaded, never checked with `systemd-analyze` |
| CI workflow | **Unverified** | it parses as YAML and a test checks which jobs and steps it names and that actions are pinned | it has never run on GitHub Actions; the `docker`, `images`, `e2e` and `vulnerabilities` jobs have not run anywhere |
| Binaries on Linux | **Partial** | `make cross-build` compiles them for linux/amd64 and linux/arm64 | only darwin/arm64 binaries have been run |
| Dependencies | **Verified** with exceptions | `make vuln` | two accepted reports (above) |
| Scale and performance | **Unverified** | none | nothing has been measured above three nodes; there is no benchmark, load test or soak test, and no figure in these documents is a measurement |
| The documents | **Partial** | `deploy/docs_test.go` checks that every relative link and anchor resolves; `deploy/roadmap_test.go` checks the roadmap's numbering, order and arithmetic; the example configuration is loaded by the real configuration code | statements in prose are reviewed by hand, not tested |

## Known flakes and skips

- `TestCleanShutdownDeregistersNode` waited for the manager to have a session, then stopped the agent at once. If the agent was stopped before the registration answer reached it, it did not know its session ID and could not deregister, and the test failed (2 of 60 runs, then 1 of 300). The test now waits for the agent itself to hold the session; the behavior of the product in that millisecond (the node looks alive until its TTL, then suspect) is harmless and listed in [failure-handling.md](failure-handling.md#limitations) and in the roadmap.
- One etcd contract subtest is skipped against a real etcd by design (above).
- No other test is skipped in any suite listed here. The PostgreSQL, etcd, Docker and end-to-end tests are behind build tags (`integration`, `etcdintegration`, `dockerintegration`, `e2e`), so a plain `go test ./...` does not compile them; the counts above for each of those suites are for runs with the tag and the server it needs. Without a server, a tagged test skips itself and says why.

## Reproducing the checks

```sh
export PATH="$PWD/.tools/go/bin:$PWD/.tools/bin:$PATH" GOTOOLCHAIN=local GOFLAGS=-p=2
make generate                      # once: protobuf code (not committed) and the pinned generators
make check                         # formatting, vet with every tag, secret scan, unit tests with -race
make test-integration              # PostgreSQL suites (needs `make compose-up`, or DOCKFLEET_TEST_DATABASE_URL)
make test-etcd                     # real etcd (needs `make etcd-test-up`, or DOCKFLEET_TEST_ETCD_ENDPOINTS)
make test-e2e                      # real binaries; starts PostgreSQL and etcd from .tools/ unless told otherwise
make cross-build tidy-check vuln
# Not run here, and needing a Docker daemon (ROADMAP.md step 1):
make docker-test-up && DOCKFLEET_TEST_DOCKER_HOST=tcp://127.0.0.1:21297 make test-docker
make docker-build
```

The PostgreSQL and etcd binaries that `make test-e2e` finds under `.tools/` are not part of the repository; `DOCKFLEET_E2E_POSTGRES_BIN` and `DOCKFLEET_E2E_ETCD_BIN` name others. Without any, the end-to-end tests that need them skip themselves and say so.
