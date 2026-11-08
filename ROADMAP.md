# DockFleet roadmap

Written on 2026-11-08 against commit `a2bf862` and refreshed the same day after the publication pass (the MIT license, third-party notices, file linters and a secret scan of every commit), after a review of the whole implementation: the 167 Go files (about 22,000 lines of code and 36,000 lines of tests), the documents, the audit in [docs/security-audit.md](docs/security-audit.md) and the status matrix in [docs/verification.md](docs/verification.md). That matrix is the source for what is true today; this file says what to do about the parts that are partial, unverified or missing.

**How the steps are ordered.** Unfinished original requirements and gaps in verification, correctness and security come first. New features come last, because each of them would be built on parts that have not yet been run against a real Docker daemon, on Linux, or at more than three nodes. The order is also a dependency order: a step only depends on steps with a lower number. Each step states its category (`incomplete requirement`, `verification gap`, `correctness`, `security` or `new feature`), scope, rationale, dependencies, acceptance criteria and estimated effort in hours. A test (`deploy/roadmap_test.go`) checks that the numbering, the ordering, the dependency direction and the total below stay consistent.

**What is not on the roadmap, on purpose.** Pods or multi-container replicas, an overlay network or built-in load balancer, runtimes other than the Docker Engine, Windows nodes, autoscaling, federation of several clusters, and a replacement of PostgreSQL as the store of record. [docs/limitations.md](docs/limitations.md) says why DockFleet is deliberately smaller than Kubernetes.

**Done since the first version of this roadmap, and so no longer steps:** the MIT license and generated third-party notices (`LICENSE`, `THIRD_PARTY_NOTICES.md`, carried in both images); linters for the CI workflow, the shell script, the Dockerfile and the Compose files (`make lint-files`), which now pass; a secret scan of all history (`make scan-secrets`) with the known false positives listed by fingerprint; a time limit on every `go test` the Makefile runs. The CI workflow and the images are still unrun (step 1). What each of these checked is in [docs/verification.md](docs/verification.md).

## 1. Run the whole system on Linux against a real Docker daemon

**Category:** verification gap

**Scope:** Execute, for the first time, everything that is written but has not run: `make test-docker` (the adapter lifecycle including a bystander container, `StreamLogs`, and a service deployed through the whole control loop) against the disposable daemon from `make docker-test-up`; the CI jobs `docker`, `images`, `e2e`, `integration`, `etcd` and `files` on an Ubuntu runner (the workflow already passes `actionlint` and the GitHub workflow schema, the Dockerfile passes `hadolint`, and the Compose files pass the Compose schema, so what is left is what only a run shows); the Compose file for the manager; both systemd units under `systemd-analyze verify` and `systemd-analyze security` on a host with systemd 250 or newer; the end-to-end suite with the Linux binaries. Fix what breaks, and move each row of the matrix in `docs/verification.md` from unverified to verified, with the command and the result.

**Rationale:** The headline claim, that a service definition becomes running containers, has been verified against an in-memory engine and against stubbed Docker HTTP traffic, never against a daemon. Everything has run on macOS (darwin/arm64) only, while the images and units target Linux, and cgroup, `json-file` log and port-binding behavior differ there. The first real run is likely to find defects, which is why it is first.

**Dependencies:** none; needs a Linux host or CI runners with Docker.

**Acceptance criteria:**
- `make test-docker` passes against a real daemon in CI, with the counts recorded in `docs/verification.md`.
- The `e2e`, `integration` and `etcd` jobs pass on `ubuntu-latest`, and the `images` job builds both images, sees the manager reach `healthy` under `--read-only --cap-drop ALL`, and sees it exit 0 on `SIGTERM`.
- `systemd-analyze verify` reports nothing for both units, and the `security` exposure scores are recorded with the units' hardening settings.
- A real container's CPU and memory limits, dropped capabilities, no-new-privileges, read-only root filesystem and loopback-only port binding are read back from `docker inspect` in a test (today they are checked on the request that is sent, and only the memory limit is read back from a daemon).
- Every defect found has a regression test, and no row in the matrix claims more than a command that ran supports.

**Estimated effort:** 18 hours

## 2. Measure resource usage and detect node capacity

**Category:** incomplete requirement

**Scope:** The agent samples the running containers with the `Stats` call the adapter already has, at a bounded rate and never on the heartbeat path, and reports the latest sample in the status of each workload (optional protocol fields, minor version bump). The store keeps the latest sample with the observation (migration 013); the API shows `usage` next to `allocated` on replicas and nodes; the metrics export used CPU and memory per node and per service; the dashboard shows used against reserved. At startup the agent detects the host's CPU and memory (Linux `/proc` and cgroup limits, macOS `sysctl`) with an override and a reserve for the system.

**Rationale:** The observability requirement asked for node resource capacity and utilization. Today "utilization" is the reservation made by declared limits ([docs/observability.md](docs/observability.md#limitations)), and a node that does not set `DOCKFLEET_NODE_MEMORY_MB` declares a fixed 1 GiB whatever the host has, which silently misleads the scheduler.

**Dependencies:** step 1, because the parsing of real stats (cgroup v1 and v2) can only be confirmed against a real daemon.

**Acceptance criteria:**
- A heartbeat carries a usage sample for each running workload no older than the sampling interval, and an engine that is slow or failing never delays a heartbeat (a test with an engine that blocks).
- `GET /v1/nodes` shows `used` beside `allocated`, and a test pins that the metrics and the API give the same figures, as the existing gauge tests do for replicas.
- With `DOCKFLEET_NODE_MEMORY_MB` unset, the agent declares the detected memory minus the reserve, on Linux and macOS (unit tests with injected readers); an explicit value always wins.
- An older agent with a newer manager, and the reverse, still register and heartbeat (protocol compatibility tests), and the documents say which figures are measured and which are declared.

**Estimated effort:** 26 hours

## 3. Replace replicas that keep failing, and notice nodes that fail everything

**Category:** incomplete requirement

**Scope:** When a replica's container has failed beyond its restart limit, the reconciler retires that workload and the scheduler places a replacement on a different node, with the failing node excluded for a cooldown and with a per-service budget and backoff so that a bad image cannot cause an endless chain of replacements. A node on which several different workloads fail within a window is marked `degraded`: it gets no new replicas, and the API, an event and a metric say so; it recovers by itself when its workloads run again. Standalone workloads are unchanged.

**Rationale:** The requirement was safe reassignment after node failure. A lost node is handled, but a replica that failed beyond its limit stays failed on its node, and a node that makes every container fail is not detected ([docs/failure-handling.md](docs/failure-handling.md#limitations)).

**Dependencies:** none.

**Acceptance criteria:**
- A table of plan cases and a control-loop test show a replica that exhausted its restarts moving to another node exactly once, with exact create and stop counts.
- When every node fails the same image, replacement stops at the budget and the service reports `blocked` or `degraded` with the reason, instead of thrashing (a test counts creations over time with the injected clock).
- A `degraded` node receives no placement, shows its reason in `GET /v1/nodes`, and returns to `healthy` after a quiet period; the transition is in the audit trail.
- The behavior is covered on the memory store and on PostgreSQL, and by an end-to-end scenario with a crash-looping container in the in-memory engine.

**Estimated effort:** 20 hours

## 4. Retire nodes and bound the history the manager keeps

**Category:** incomplete requirement

**Scope:** `DELETE /v1/nodes/{id}` removes a node that is unenrolled and unavailable, together with its stopped workloads and observations, and refuses (`409`, with the reason) while it holds a workload meant to run. A retention setting (`DOCKFLEET_EVENT_RETENTION`) lets the leader prune old audit events in bounded batches; leadership and rollout events can be kept longer. Every deletion is itself audited.

**Rationale:** Registered nodes are never deleted, a node that is gone for good keeps its stopped workloads on record, and the `events` table only ever grows (no code deletes from it). Both are fine for a demonstration and wrong for a fleet that runs for years.

**Dependencies:** none.

**Acceptance criteria:**
- API tests on both stores: deleting a live or enrolled node is refused; deleting a lost, unenrolled node removes it and its records and leaves other nodes' data alone; an agent with that node ID that returns is refused as not enrolled.
- A retention test with an injected clock prunes only events older than the setting, in batches that never hold a long transaction (the batch size is asserted), and never deletes the records of a rollout still in progress.
- `docs/state.md`, `docs/api.md` and the README describe the new route and setting, and the audit trail shows `node.deleted` and `events.pruned`.

**Estimated effort:** 14 hours

## 5. Keep the agent's local state across restarts

**Category:** security

**Scope:** An optional state directory for the agent (`DOCKFLEET_STATE_DIR`, `StateDirectory=` in the systemd unit) that holds the highest control-plane epoch the agent has heard and the restart counts and backoff of each workload, written atomically with mode `0600`. A missing, corrupt or unwritable directory is reported and ignored, never a reason not to start. The manager also drops a session that never heartbeats within the node TTL, so that an agent stopped between the manager creating its session and the answer reaching it (which cannot deregister, because it was never told its session ID) leaves nothing behind.

**Rationale:** Two documented limits come from agent state being in memory. A restarted agent that reaches a replaced leader first can take one answer from it ([docs/high-availability.md](docs/high-availability.md#remaining-limitations-partitions-included)), and restart counts reset when the agent restarts, so a crash-looping container's limit is circumvented by restarting the agent. The second case was found while preparing this roadmap, when `TestCleanShutdownDeregistersNode` failed in 2 of 60 runs; the test was fixed to wait for the agent itself, and the behavior is now listed as a limitation.

**Dependencies:** none.

**Acceptance criteria:**
- A restart test shows an agent that has heard epoch 7 refusing the first answer of a control plane at epoch 6 after the restart, and the restart count of a workload surviving it.
- Corrupt, unreadable and read-only state directories each leave the agent running with a warning (three tests); the file mode is pinned.
- A registry test shows a session that never heartbeats being dropped within the TTL.
- The systemd unit allows writing only to its state directory, an artifact test keeps it that way, and `docs/failure-handling.md` and `docs/high-availability.md` no longer list these limits.

**Estimated effort:** 12 hours

## 6. Give the administrative API more than one token and some access control

**Category:** security

**Scope:** Several named tokens, each with a role (admin, operator that may deploy and scale but not enroll or revoke, read-only), read from files and re-read on `SIGHUP` so that two tokens are valid during a rotation. Failed attempts are throttled per remote address in bounded memory, answered `429` with `Retry-After`. The audit actor becomes the token's name, never its value. Dashboard sessions are tied to the token that created them and end when it is removed.

**Rationale:** The audit lists one shared token for everything, no throttling of failed attempts with it, and dashboard sessions that cannot be revoked ([docs/security-audit.md](docs/security-audit.md#api-authorization)). Rotation today means a restart and a window in which every tool fails.

**Dependencies:** none.

**Acceptance criteria:**
- A role matrix is generated from the route table, in the way `TestEveryRouteExceptHealthRequiresTheAdminToken` reads the source, so that a route added without a declared role fails the build; a read-only token gets `403` on every mutating route and `200` on every read.
- A rotation test shows the old and the new token working together and the old one failing after it is removed, without a restart.
- The throttle returns `429`, is bounded in memory under a flood of distinct addresses (asserted), and does not lock out a valid token from another address.
- Events carry the token name as actor, and a test searches events and logs for the token values and finds none; the end-to-end refusals cover the new cases.

**Estimated effort:** 22 hours

## 7. A secret store for services

**Category:** security

**Scope:** Write-only secret objects (`/v1/secrets`), referenced by name from a service's environment or as files, encrypted at rest in PostgreSQL with AES-256-GCM under a key read from a file (with a key identifier, so the key can be rotated). The API never returns a value and events, logs and the revision hash never contain one; a revision records the secret names and versions it uses, so a rollback restores the right ones. The manager sends a value only to the agent that holds an assigned workload using it, over the existing mutual TLS, and the agent hands it to the container through a tmpfs file or the environment.

**Rationale:** Environment values in a service spec are stored and returned in plain text, and the documents tell operators not to put credentials there ([docs/api.md](docs/api.md)). Real services need credentials, so today they must be injected outside DockFleet.

**Dependencies:** step 6, so that managing secrets is a separate permission from deploying services.

**Acceptance criteria:**
- A test dumps every table of a PostgreSQL database after a secret is stored and finds the value in none of them.
- No API response, event, log line or metric contains a value (a canary test in the style of the end-to-end credential search), and a read-only token cannot list even the names if the operator forbids it.
- Updating a secret rolls out the services that use it in the way a spec change does, within their rollout policy, and a rollback restores the previous version.
- The key can be rotated by re-encrypting in the background while reads continue; a wrong or missing key stops the manager with a clear error rather than serving garbage.
- The protocol change is a minor version with a compatibility test, and the agent policy refuses a secret mount outside tmpfs.

**Estimated effort:** 38 hours

## 8. Enforce a policy on images and users

**Category:** security

**Scope:** An operator policy, applied by the manager when a definition is validated and again by the agent before it creates a container: containers must name a non-root `user` (on by default), images must come from an allow-list of registries or prefixes, and optionally must be referenced by digest. Replicas that already run keep running when the policy tightens; new revisions that violate it are refused with the field and the reason.

**Rationale:** The audit lists containers that run as the image's user (often root inside the container) and images that are not restricted at all ([docs/security-audit.md](docs/security-audit.md#container-isolation)). Both are policy decisions that belong to the operator and are cheap to enforce at both layers.

**Dependencies:** none.

**Acceptance criteria:**
- Validation tables on the manager and in the agent policy agree case by case (the manager's limit is never looser than the agent's, as `AGENTS.md` already requires), for users, registries, tags and digests.
- Tightening the policy does not stop or restart a running replica (an end-to-end check with exact container counts); a new revision that violates it is refused with `400` and the field.
- The defaults are documented with their effect on common images, and the end-to-end suite has refusal cases for each rule.

**Estimated effort:** 16 hours

## 9. Make manager upgrades safe across schema changes

**Category:** correctness

**Scope:** A running manager checks the schema version at an interval and the leader stops acting, and exits with a clear message, when the database is ahead of the binary. Migrations follow an expand-then-contract rule that is written down and enforced by a test over the migration files. A CI job runs the previous release's manager against the current schema, and an end-to-end test performs a rolling upgrade of two managers.

**Rationale:** The check that refuses a newer schema happens at startup only, so an old manager that is already running keeps going on a migrated database, and rolling a cluster across a schema change has not been tested ([docs/deployment.md](docs/deployment.md#upgrades-backups-restores)).

**Dependencies:** step 1, for the Linux CI; and a tagged release to compare against (until one exists the test builds the previous commit).

**Acceptance criteria:**
- An end-to-end test starts an old manager, migrates the database with a new one, and sees the old manager stop leading and exit within the check interval, without any workload being moved.
- A rolling-upgrade scenario with two managers and two agents finishes with all agents continuously ready and no replica created or stopped.
- A test over `internal/store/migrations` rejects a migration that drops or renames a column, or tightens a constraint, without the documented two-step form.
- `docs/deployment.md` states the supported upgrade order and the previous version each release can run beside.

**Estimated effort:** 16 hours

## 10. Serve log queries on followers of a high-availability cluster

**Category:** correctness

**Scope:** A follower that receives a log request forwards it to the leader (an advertised API address in the configuration, TLS and the caller's own credential preserved, a hop header to make loops impossible) and streams the answer back, or answers a redirect where forwarding is not possible. Cancellation, a slow client and a leader change during a stream behave as they do on the leader.

**Rationale:** Agents are connected only to the leader, so a log query that reaches a follower reports every node as unavailable ([docs/logs.md](docs/logs.md#limitations)). In a cluster behind a load balancer that is a common experience, and it looks like a fault.

**Dependencies:** none (if step 6 is done first the forwarded credential is the caller's own token).

**Acceptance criteria:**
- An end-to-end HA test returns the same lines for the same query through a follower and through the leader.
- A leader failover during a following query resumes from the cursor without repeats or gaps, as a single-manager reconnect does today.
- A request that would loop is refused, a bad credential is never forwarded, and cancelling the client cancels the request at the leader (counted on the agent's side).
- The memory bounds in `docs/logs.md` still hold with a follower in the path (stated and tested).

**Estimated effort:** 12 hours

## 11. Measure scale, run a soak test and paginate the lists

**Category:** verification gap

**Scope:** A load harness with simulated agents (in-process, in-memory engines) against PostgreSQL, for example 200 nodes and 2,000 replicas, that records scheduling and reconciliation pass durations, heartbeat and API latency (p50 and p99), query counts per pass, heap, goroutines and open connections. Fix the hotspots it shows (batched reads, indexes, reading less per pass). Add cursor pagination to `GET /v1/services`, `/v1/workloads` and `/v1/nodes`. A 24-hour soak with churn (scale, roll out, kill nodes) that checks for growth. Publish only measured limits.

**Rationale:** Nothing has been measured above three nodes. Each scheduling and reconciliation pass reads the whole cluster, the fleet gauges are recomputed on every scrape, and three of the lists have no pagination ([docs/api.md](docs/api.md)). The documents say "not benchmarked" and that is where it must change into numbers.

**Dependencies:** step 1 (representative hardware and Linux) and step 4 (bounded tables).

**Acceptance criteria:**
- `make bench-scale` writes a report (machine-readable and readable) that names the environment, the sizes and the results, and `docs/limitations.md` quotes only figures from such a report.
- At the tested size a pass finishes in less than its interval, or the maximum size at which it does is documented as the supported limit.
- Pagination tests on both stores cover cursors, stable order under concurrent changes and the maximum page size.
- The soak shows no monotonic growth in goroutines, heap or connections over 24 hours (the data is attached), and a benchmark with generous bounds guards against order-of-magnitude regressions in CI.

**Estimated effort:** 36 hours

## 12. Verify high availability with a multi-member etcd and with network faults

**Category:** verification gap

**Scope:** End-to-end scenarios with a three-member etcd: losing the member that leads, a minority, a majority and a partition between members. Extend the existing TCP proxy to add delay, loss and half-open connections between agents and managers and between managers and etcd. Test the election's trust window against clock skew with injected clocks and with a subprocess under a skewed clock. A seeded, replayable generator of fault schedules that checks, after each, that there was never more than one leader per epoch and no replica ran twice.

**Rationale:** High availability was verified against one etcd member and against clean cuts. Its safety argument assumes a bounded drift of clock rates, and its documented failover times were measured on one machine ([docs/high-availability.md](docs/high-availability.md)).

**Dependencies:** step 1.

**Acceptance criteria:**
- The three-member scenarios pass three runs in a row in a nightly CI job, within the port range the project uses.
- At least 50 seeded fault schedules run clean, any failing seed is printed and replays identically, and a deliberately broken election (a test-only switch) is caught by the same checker.
- The failover times measured under delay and loss are recorded in `docs/high-availability.md` with the environment, and the limits section is updated with what was learned.
- Each defect found has a regression test.

**Estimated effort:** 20 hours

## 13. Fuzz the inputs and keep the corpus in the build

**Category:** security

**Scope:** Go fuzz targets for service definition validation and canonicalization, configuration parsing (labels, durations, `_FILE` handling), the log line reader, the protocol message validators, the etcd JSON replies and the event filters. Seed corpora are checked in and run as ordinary tests in `make check`; CI runs each target briefly on pull requests and longer overnight.

**Rationale:** The audit says plainly that fuzzing was not done. These are the places where hostile input enters (an administrator's JSON, an agent's messages, a container's output, an etcd reply), each already has table-driven bad-input tests, and fuzzing is the cheap way to find what the tables did not think of.

**Dependencies:** none.

**Acceptance criteria:**
- At least eight fuzz targets exist and their seed corpora run in `make check`.
- Each target runs ten minutes in the nightly job without a crash, and the first overnight run's findings are fixed.
- Every crash found has a minimized input in the corpus and a regression test that fails without the fix.
- `docs/security-audit.md` lists the targets and what each covers, and what remains unfuzzed.

**Estimated effort:** 14 hours

## 14. Release engineering and supply chain

**Category:** security

**Scope:** A release workflow for tagged versions that builds the binaries and images, with checksums, an SBOM and a keyless signature, and documents how to verify them. Pin the base images by digest with a script that updates the pins, and add a scheduled dependency update workflow. Align the module path (`github.com/dockfleet/dockfleet`, a placeholder organization) and the image `source` labels and `Documentation=` links with the published repository, with one mechanical rename that `go build` and the tests check. Move the Docker client from `github.com/docker/docker` v27 to the split client modules, if the API allows, which would clear the two accepted advisories in `scripts/vuln-check.sh`. Write a versioning and compatibility policy for the REST API, the protocol and the schema, and keep a changelog.

**Rationale:** There are no tags and no releases, the version string is `dev`, the images are pinned by tag only, and two vulnerability reports are accepted because the module has no fixed version. A fleet manager is software people run with the Docker socket in reach, so how it is built and signed matters.

**Dependencies:** step 1 (the images must be shown to build and run first), and step 9 (the compatibility checks that the policy below relies on). The `LICENSE` file and the third-party notices already exist.

**Acceptance criteria:**
- Pushing a tag builds checksummed binaries for the released platforms and both images, attaches an SBOM and a signature, and the documented verification commands succeed on the output.
- `make vuln` passes with an empty accepted list, or each remaining entry names the advisory, the reason and the date to revisit.
- Base images carry digests, and a test fails if a `Dockerfile` or Compose file references an image by tag only.
- The compatibility policy states what a minor and a major version may change, and the migration and protocol checks from step 9 enforce it.

**Estimated effort:** 16 hours

## 15. Automatic rollback and finer rollout limits

**Category:** new feature

**Scope:** An opt-in `auto_rollback` on the rollout policy: when a rollout is reported failed (its progress deadline passed), the reconciler deploys the last revision that completed, as a new revision, with the reason in the audit trail. `min_ready_seconds` so that a replica counts as ready only after it has stayed healthy that long, and percentage forms of `max_surge` and `max_unavailable`.

**Rationale:** A failed rollout is reported and carried on, and the administrator decides ([docs/rollouts.md](docs/rollouts.md#limitations)). Many operators want the decision made for them, and "ready the moment it runs" is a weak gate for services that crash a few seconds in.

**Dependencies:** step 3, since replacing failing replicas and rolling back must not fight each other.

**Acceptance criteria:**
- With `auto_rollback` on, a failing image ends with the service back on the last good revision, with exact create and remove counts in a control-loop test and an end-to-end scenario; with it off, behavior is unchanged.
- A replica that is healthy for less than `min_ready_seconds` does not count toward the rollout's budget, with the clock injected; percentages round in the documented direction and respect the minimum of one.
- Rollback itself cannot loop: a failure of the rolled-back revision is reported and stops, and a test shows no more than one automatic rollback per failed rollout.

**Estimated effort:** 18 hours

## 16. Let services be reached: published addresses and an endpoints API

**Category:** new feature

**Scope:** An agent setting (`DOCKFLEET_PUBLISH_ADDRESS`) that lets the operator's policy publish container ports on a chosen address of the node instead of loopback only, using the switch the policy already has but no configuration reaches. Nodes report an advertised address, and `GET /v1/services/{name}/endpoints` lists address and port for each ready replica. Example configurations for a reverse proxy driven by that list. No built-in load balancer.

**Rationale:** Today ports bind to the node's loopback and nothing says where replicas are except a status read; every deployment needs its own glue to reach its services. A small, honest piece of glue closes most of the gap without turning DockFleet into a network layer.

**Dependencies:** step 8, because publishing on a non-loopback address must go with the image and user policy; and step 6, so that endpoint reads can be given to a proxy with a read-only token.

**Acceptance criteria:**
- The default is unchanged (loopback only), and publishing elsewhere needs the setting and is refused for addresses the node does not own; both are tested at the policy and at the adapter.
- The endpoints API lists only ready replicas of the current revision, changes as replicas come and go, and is paginated like the other lists.
- The example proxy configuration is exercised in an end-to-end test that routes a request to a replica and keeps working through a rollout.
- The documents say plainly what is still the operator's job (TLS termination, balancing, health-aware routing).

**Estimated effort:** 28 hours

## 17. Richer scheduling constraints

**Category:** new feature

**Scope:** Affinity and anti-affinity between services and to node labels, a spread constraint over a label such as a zone, service priorities with bounded preemption of lower priorities when a node is full, and placement of a replica next to the named volume it needs. All of it stays deterministic, and every refusal keeps its reason per node.

**Rationale:** The scheduler places by declared CPU and memory, a label selector and host ports, and spreads a service's replicas over nodes ([docs/scheduler.md](docs/scheduler.md#limitations)). Real fleets need replicas kept apart or together, kept across zones, and some services to win over others when there is not room.

**Dependencies:** step 2, so that constraints and priorities can be reasoned about with measured usage; and step 3, so that a replica moved because it failed respects them.

**Acceptance criteria:**
- Table-driven tests in `internal/scheduler` cover each constraint, including conflicting ones, with a reason for every node; shuffled input still gives identical decisions, as the existing determinism test requires.
- Preemption stops the minimum number of lower-priority replicas, never a replica of a service at or above the incoming priority, and never twice in one pass for the same room (a control-loop test with exact counts).
- Placement next to a volume never moves a replica that already has data, and says so when it cannot place one.
- `docs/scheduler.md` describes each rule with an example, and the end-to-end suite has a scenario for spread and one for preemption.

**Estimated effort:** 30 hours

## Total estimate

**Total estimate:** 356 hours

By category: incomplete requirements (steps 2 to 4) 60 hours; verification gaps (steps 1, 11 and 12) 74 hours; correctness (steps 9 and 10) 28 hours; security (steps 5 to 8, 13 and 14) 118 hours; new features (steps 15 to 17) 76 hours. Steps 1 to 14, which are everything before the new features, come to 280 hours.

A reasonable path with two engineers is step 1 first, then steps 2, 3 and 4 in parallel with 5 and 6, then steps 7 to 14 in the order given while step 1's findings are fixed. The longest chain of dependencies is steps 1, 2 and 17 (74 hours), then steps 6 and 7 (60 hours); everything else can run beside them.

## Assumptions

- One engineer who knows the code base, working about six productive hours a day; the hours are effort, not elapsed time, and exclude waiting for review or for CI.
- Each estimate includes the tests the repository requires (a unit test, a PostgreSQL test where the store is involved, and an end-to-end scenario where behavior crosses processes) and the documents that `AGENTS.md` requires to change with the code.
- A Linux host or CI runners with Docker are available from step 1 on, as are several machines for steps 11 and 12.
- Step 1 is the largest uncertainty: nobody has yet run the Docker-dependent tests, so the 18 hours cover running them and fixing what is easy to fix; a serious defect there would move the later steps.
- The estimates are good to about plus or minus 30 percent, except step 11 (the size of the hotspots it finds is unknown) and step 7 (the semantics of updating a secret that is in use are a design decision that may need review).
- Dependencies and the Go release stay as they are; upgrades forced by new advisories are outside the estimates.
- Protocol and schema changes keep the compatibility rules in `docs/protocol.md` and `AGENTS.md` (optional fields, minor version, numbered migrations without gaps).
