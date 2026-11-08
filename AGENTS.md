# DockFleet: project boundaries and working agreement

- Work only in `/Users/mk-rune/test_projects/03-dockfleet`. This is one of fourteen independent repositories.
- Do not read, modify, clean up, or run commands in sibling repositories or the user's configuration.
- Work autonomously: choose sensible defaults and document assumptions; do not ask questions or delegate.
- Never install anything globally or use sudo, Homebrew, launchd, host credentials, or host Docker sockets.
- Dependencies, toolchains, caches, browser binaries, SDKs, temporary files and test data stay in this repository. Preserve the supplied project-local environment. Python packages go in `.venv`.
- `.agent-local/` is ignored, project-local build/runtime state. `.opencode/` is managed orchestration configuration; do not change or remove it or bypass the sandbox.
- Use existing system tools read-only, or install pinned tools under this repository. Do not change shell profiles, user settings, Keychain, or global Git settings.
- Bind test servers to loopback; use ports 21200–21299. Limit build/test workers to two unless a required benchmark explicitly needs more.
- Docker-dependent integration tests must remain reproducible but be reported as unverified here if they require access outside this repository. Do not bypass this boundary.
- Do not touch attached hardware, enroll services, flash firmware, install agents, or alter OS settings. Supply manual checklists for such steps.
- Implement only the current numbered prompt, preserving prior behavior. Run appropriate checks, fix failures, and report genuine limitations without fabricating test or benchmark results.
- Before finishing every numbered turn: stop its servers/watchers, remove scratch outputs, update relevant documentation, and make a local conventional-commit Git commit. Do not push.
- End each turn with a short summary of changes, checks actually run, remaining limitations, and commit hash. The orchestrator will provide the next prompt only after this turn ends.

Extend this file with the project's language-specific architecture and verification commands while retaining these boundaries.

## Project: DockFleet (Go)

DockFleet is a distributed container orchestration system: a manager (control plane) and agents (worker nodes) communicating over gRPC, with state in PostgreSQL. See `README.md` for usage and `docs/architecture.md` for design. Container orchestration itself is not implemented yet; update both documents when that changes.

### Layout and dependency direction

- `cmd/manager`, `cmd/agent`: entry points only. Parse config, wire dependencies, handle signals. No business logic.
- `internal/manager` wires `api`, `protocol`, `registry` and `store`; `internal/agent` wires `protocol` and `docker`.
- `internal/api` (admin HTTP), `internal/protocol` (gRPC service, client, versioning), `internal/registry` (manager-side node sessions), `internal/scheduler` (pure placement), `internal/store` (persistence), `internal/docker` (agent-only Docker Engine adapter), `internal/netutil`, `internal/pki` (certificates, node identity, TLS configs; imports nothing from the other internal packages), `internal/config`, `internal/logging`. `cmd/pki` is the certificate CLI. `internal/integration` is tests only; `internal/pgtest` is a test helper that makes throwaway PostgreSQL databases and depends on nothing else here.
- The services REST API (`/v1/services`, `docs/api.md`) only records desired state: its handlers must not touch nodes, workloads or the scheduler. A new definition field needs an entry in the allow-lists in `internal/api/services_validate.go`, validation with a limit at least as strict as the agent's policy in `internal/docker/policy.go` (the manager cannot import it), canonicalization so equivalent definitions hash alike, and tests in the `internal/api/services_*_test.go` suite, which must keep passing against both the in-memory store and PostgreSQL.
- Lower layers (`store`, `scheduler`, `registry`, `config`, `logging`, `netutil`, `pki`) must not import `api`, `protocol`, `manager` or `agent`. Depend on small interfaces (`store.Store`, `registry.Store`, `protocol.Registry`, `agent.Manager`, `docker.Client`) so each package is testable with fakes.
- Docker is agent-only: nothing the manager is built from (`cmd/manager`, `manager`, `api`, `registry`, `store`, `scheduler`, `protocol`, and therefore `config`) may import `internal/docker` or a `github.com/docker/...` package (`TestManagerSideNeverImportsDocker` enforces it), and the Docker SDK stays inside `internal/docker/sdk.go`. The adapter only acts on containers carrying this node's DockFleet labels, addressed by workload ID, and rejects privileged mode, Docker-socket and system-path mounts and other dangerous settings by default (`docs/docker.md`); relaxing the policy needs an explicit setting and must keep the always-denied sets.
- Authentication changes (mutual TLS, identity, enrollment) update `docs/mtls.md` and `docs/protocol.md` and get tests in `internal/pki`, `internal/protocol` and `internal/integration` for the failure cases (untrusted, wrong identity, expired, not enrolled, no plaintext fallback), not only the happy path.
- The wire protocol is `proto/dockfleet/v1/fleet.proto` and `docs/protocol.md`. Any protocol change updates both, follows the versioning rules there, and is exercised in `internal/integration`.
- Generated protobuf code lives in `internal/protocol/dockfleet/` and is git-ignored. Edit `proto/`, then run `make proto`. Never hand-edit generated files.

### Scoped changes

- Implement only what the current task asks for. No drive-by refactors, renames, formatting sweeps or dependency upgrades.
- Every changed line should trace to the request. Keep commits to one logical change using conventional commit messages (`feat:`, `fix:`, `test:`, `docs:`, `chore:`, `ci:`, `refactor:`).
- New third-party dependencies need a reason in the commit message; prefer the standard library. Pin tool versions in the `Makefile`.
- Database changes are new numbered files in `internal/store/migrations/` (`NNN_description.sql`, no gaps); never edit a released migration (checksums are recorded and `TestReleasedMigrationsAreUnchanged` pins them). Add new migrations to the list in `docs/state.md`.
- Keep desired state (what an administrator asked for) and observed state (what agents reported) apart: separate tables, separate writers, separate fields. Observations are written only through `RecordObservations`; intent is never derived from a report. Anything that must be consistent across several writes goes in one `InTx`; placement takes `Tx.LockPlacement`; concurrent edits of a service use its version (`ErrVersionConflict`). Nothing may keep cluster state only in process memory, because several managers may share the database.
- Public behavior changes (API routes, env vars, RPCs, schema) update `README.md` and `docs/architecture.md` in the same change.

### Test coverage

- Every new package or behavior ships with tests in the same change; bug fixes include a regression test that fails without the fix.
- Test through interfaces with fakes. Use `bufconn` for gRPC and `httptest` recorders or fake `http.RoundTripper`s for HTTP and Docker; open a real socket only when the test is about the socket, and then only on `127.0.0.1` in ports 21200–21299 (currently 21290–21295 in `internal/manager`; `internal/integration` uses an in-memory transport and no ports). Use `t.TempDir()` for files.
- Tests must not need Docker, a database or the internet. PostgreSQL tests are tagged `integration` and read `DOCKFLEET_TEST_DATABASE_URL` (a server whose role may `CREATE DATABASE`; every test makes and drops its own database and never writes to the one in the URL); Docker daemon tests are tagged `dockerintegration` and read `DOCKFLEET_TEST_DOCKER_HOST` (never run against the host's Docker from this development sandbox); both stores must keep passing `runStoreConformance`.
- Tests must be deterministic: no fixed sleeps as synchronization (poll with a deadline), injected clocks where time matters, and `-race` clean.
- Cover failure paths (bad input, auth failure, dependency errors, cancellation), not only success.

### Clean process management

- Every goroutine has an owner and a stop condition tied to a `context.Context`, a closed channel or a `WaitGroup`. No goroutine may outlive the function that started it unless it is documented.
- Pass `context.Context` as the first parameter of anything that blocks or does I/O; never store contexts in structs.
- Shutdown must be graceful and bounded by `DOCKFLEET_SHUTDOWN_TIMEOUT`, then forced. A requested shutdown exits 0.
- Before finishing a task, stop every server, watcher and container you started (`make compose-down`), and delete scratch files, logs and build output (`make clean`). Report anything intentionally left running.

### Secure defaults

- Bind to loopback by default; a non-loopback bind requires TLS, and TLS on the agent listener means mutual TLS with a verified, identity-bound, enrolled client. Never add a plaintext fallback or a TLS mode without client certificates. Tokens are never defaulted, logged, echoed in errors or committed. The only credentials in the repository are the labeled throwaway tokens in `config/dev.env`.
- Private keys and certificates for development live in the git-ignored `.dev-pki/` (`make dev-pki`), are written owner-only and are never printed, logged or committed; `make check-secrets` (part of `make check`) must pass. Test keys are generated in memory or `t.TempDir()`.
- Validate all external input (HTTP, gRPC, env, files) at the boundary and bound its size. Compare secrets in constant time. Use parameterized SQL only.
- Never expose the Docker Engine API over non-loopback TCP. Do not mount or use the host Docker socket from this development environment; Docker-dependent steps are written but reported as unverified here.
- Do not log secrets, tokens, database URLs or request bodies.

### Verification commands

Run from the repository root with a Go 1.23+ toolchain, installed under this repository (for example in `.tools/` or `.agent-local/`) when working in the sandbox.

```sh
make generate           # pinned buf + protoc plugins into .tools/bin, lint, generate code (needs network once)
make proto-lint proto   # lint and generate protobuf code with the tools already installed
make tidy               # go mod tidy; writes go.sum (needs network)
make check              # gofmt check, go vet, secret scan, go test -race ./...
make build              # bin/dockfleet-manager, bin/dockfleet-agent
make test-integration   # PostgreSQL store and API tests (each test gets its own throwaway database); needs `make compose-up`
make test-docker        # Docker Engine tests (build tag dockerintegration); needs DOCKFLEET_TEST_DOCKER_HOST
```

At most two build/test workers (`GOFLAGS=-p=2`).

### Verification reporting

- Report only checks you actually ran, with the command and its real result (pass/fail, counts). Never paste or invent output you did not see.
- State what could not be run and why (no toolchain, no network, Docker not reachable from the sandbox) and mark those items **unverified**. A change is not "tested" if its tests did not execute.
- Report failures you could not fix, flaky tests, skipped tests and known gaps instead of weakening tests to pass.
- End every task with: summary of changes, checks run and results, unverified items and limitations, commit hash.
