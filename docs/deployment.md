# Deploying DockFleet on several machines

This guide takes you from nothing to a fleet of two or more worker machines under one or more managers, with mutual TLS, credentials kept out of the environment, health checks, and (optionally) a high-availability control plane. Read [credentials.md](credentials.md) for how the secrets in it are made and moved, and [limitations.md](limitations.md) before you depend on it.

What was and was not verified is in [the last section](#what-has-and-has-not-been-verified).

## What you deploy

```
   administrators (curl, scripts, the dashboard)
          │ HTTPS, bearer token
          ▼
 ┌──────────────────────┐   ┌──────────────────────┐       ┌─────────────┐
 │ manager-1  (leader)  │   │ manager-2 (follower) │──────▶│ PostgreSQL  │  state of the fleet
 │ :21280 API  :21281 ◀─┼───┼─ gRPC, mutual TLS    │       └─────────────┘
 └──────────┬───────────┘   └──────────┬───────────┘       ┌─────────────┐
            └── election lease ────────┴──────────────────▶│ etcd (3)    │  only who leads
                                                            └─────────────┘
   workers (each: dockfleet-agent + its Docker Engine)
   worker-1   worker-2   worker-3      agents DIAL the managers; nothing dials a worker
```

| Part | Count | Needed for | Holds |
| --- | --- | --- | --- |
| `dockfleet-manager` | 1, or 2–3 for high availability | the control plane | nothing: it is stateless over the database |
| PostgreSQL 13 or newer | 1 (you make it highly available however you do that) | the state of the fleet | services, revisions, nodes, workloads, observations, the audit trail, the fencing epoch |
| etcd 3.4 or newer | 3 or 5, **only for several managers** | choosing the leader | one leadership key; nothing durable |
| `dockfleet-agent` + Docker Engine | one per worker | running containers | the containers; their state is the engine's |
| `dockfleet-pki`, on the CA host | 1 | making certificates | `ca.key` (offline) |

A single manager needs no etcd and no second manager: skip every section marked *(high availability)*.

## Network

| Port | Where | Who connects | Protection |
| --- | --- | --- | --- |
| 21280/tcp | manager | administrators, Prometheus, a browser (dashboard) | TLS 1.3; bearer token on every route except `/healthz` and `/readyz` |
| 21281/tcp | manager | agents | **mutual** TLS 1.3, a client certificate bound to the node ID, and an enrollment |
| 21282/tcp | agent, **loopback only** | the machine's own supervisor and health check | none needed: it cannot be bound to another address |
| 5432/tcp | PostgreSQL | managers | your database's TLS and authentication; use `sslmode=verify-full` in the URL |
| 2379/tcp | etcd | managers *(high availability)* | mutual TLS (plaintext is refused except on loopback) |
| the Docker socket | each worker | the agent, locally | file permissions; see [the audit](security-audit.md#container-isolation) |

Workers need to reach managers on 21281 and nothing else of DockFleet. Managers never connect to workers, so workers can sit behind NAT. Containers DockFleet starts publish ports on the worker's **loopback** only (the policy has no switch for anything else): put the reverse proxy or load balancer of your choice on the worker to expose a service.

## 1. PostgreSQL

Create a database and a role that owns it (the first manager to start applies the schema, so the role needs to create tables):

```sql
CREATE ROLE dockfleet LOGIN PASSWORD '...';   -- generate the password; do not type one
CREATE DATABASE dockfleet OWNER dockfleet;
```

The database URL is a credential: `postgres://dockfleet:<password>@db.internal:5432/dockfleet?sslmode=verify-full`. It goes in a file (next step), never in an environment variable or a file you commit. Back the database up like the state it is: it is **all** the state there is ([state.md](state.md)).

Migrations run when a manager starts, under an advisory lock, so managers starting together apply each migration once. A manager older than the schema refuses to start and says to upgrade it; migrations are never edited after release and each one's checksum is recorded ([state.md](state.md#migration-rules)).

## 2. Certificates

On the CA host (offline, or at least not a manager or a worker):

```sh
dockfleet-pki init-ca -dir pki
dockfleet-pki issue-manager -dir pki -dns manager-1.internal,manager-2.internal -ip 10.0.0.11,10.0.0.12
```

Every name and address an agent or administrator will use to reach a manager must be in the certificate; they verify it. For each worker, use the signing-request flow so the worker's private key never leaves the worker ([credentials.md](credentials.md#a-worker-certificate-the-key-stays-on-the-worker)).

## 3. Install the binaries

Either build them (`make build` gives `bin/dockfleet-manager`, `bin/dockfleet-agent`, `bin/dockfleet-pki`; they are static, no shared libraries, for linux/amd64, linux/arm64 and darwin/arm64 — `make cross-build`), or build the images (`make docker-build`, which uses the `Dockerfile` at the root: two targets, `manager` and `agent`, each a single static binary on a distroless non-root base with a built-in `HEALTHCHECK`; the manager is meant to run as a container, the agent as a host service).

Pin what you run: a version tag or digest, not `latest`. Pin the base image by digest in your own build (`--build-arg RUNTIME_IMAGE=gcr.io/distroless/static-debian12:nonroot@sha256:...`) after reading it with `docker buildx imagetools inspect`.

## 4. The manager

Copy [`deploy/examples/manager.env.example`](../deploy/examples/manager.env.example) to `/etc/dockfleet/manager.env` and set at least:

```
DOCKFLEET_INSTANCE_ID=manager-1
DOCKFLEET_API_ADDR=0.0.0.0:21280
DOCKFLEET_GRPC_ADDR=0.0.0.0:21281
DOCKFLEET_TLS_CERT_FILE=/etc/dockfleet/tls/manager.pem
DOCKFLEET_TLS_KEY_FILE=/etc/dockfleet/tls/manager.key
DOCKFLEET_TLS_CLIENT_CA_FILE=/etc/dockfleet/tls/ca.pem
DOCKFLEET_API_TOKEN_FILE=/run/secrets/dockfleet_api_token
DOCKFLEET_DATABASE_URL_FILE=/run/secrets/dockfleet_database_url
```

Choices to make, with the reasons:

- **Credentials as files.** `DOCKFLEET_API_TOKEN_FILE` and `DOCKFLEET_DATABASE_URL_FILE` (and `DOCKFLEET_AGENT_TOKEN_FILE`, for plaintext development only) read the value from a file once at startup. A value in the environment shows in `docker inspect`, `/proc/<pid>/environ` and crash reports; a file mounted by your runtime does not. Setting both a variable and its `_FILE` is an error.
- **TLS is not optional on a public address.** A listener that is not loopback needs the three TLS settings, or the manager refuses to start. With TLS, agents authenticate by certificate and `DOCKFLEET_AGENT_TOKEN` is ignored.
- **Timing.** The defaults (heartbeat 10 s, a node is suspect after 30 s of silence, an agent stops its own containers after a 60 s lease, a node is given up on after 2 min) trade failover speed against false alarms. `DOCKFLEET_NODE_UNAVAILABLE_AFTER` must be at least `DOCKFLEET_NODE_LEASE` plus a heartbeat, so that a live agent has stopped its containers before their replicas start elsewhere; the manager checks this at startup. Keep it well above 30 s as well: after a manager restart an agent can take that long to register again (its retry wait doubles up to 30 s), and a node that has not come back by then is given up on. Details: [failure-handling.md](failure-handling.md).
- **Capacity.** `DOCKFLEET_SCHEDULER_CPU_OVERCOMMIT` and `..._MEMORY_OVERCOMMIT` default to 1: a node is never promised more than it declared.

Run it under systemd ([`dockfleet-manager.service`](../deploy/examples/systemd/dockfleet-manager.service): a sandbox with no capabilities, a read-only system, systemd credentials) or as a container ([`compose.manager.yml`](../deploy/examples/compose.manager.yml): read-only filesystem, all capabilities dropped, secrets as files, the image's health check). Give it `TimeoutStopSec`/`stop_grace_period` longer than `DOCKFLEET_SHUTDOWN_TIMEOUT`: on SIGTERM it finishes in-flight requests, ends its streams and exits 0 within that time, and is killed only after.

Check it:

```sh
curl --cacert pki/ca.pem https://manager-1.internal:21280/readyz           # {"status":"ready"}
curl --cacert pki/ca.pem -H "Authorization: Bearer $(cat api-token)" https://manager-1.internal:21280/v1/cluster
```

## 5. Workers

On each worker, as the user that will run the agent:

```sh
dockfleet-pki new-csr -node-id worker-1 -out /etc/dockfleet/tls     # agent.key (0600) and agent.csr
```

Copy `agent.csr` to the CA host, sign it (`dockfleet-pki sign-agent -dir pki -node-id worker-1 -csr agent.csr`), copy `agent.pem` and `ca.pem` back, and **enroll the node** from an administrator's machine:

```sh
curl --cacert pki/ca.pem -H "Authorization: Bearer $TOKEN" \
     -d '{"node_id":"worker-1","description":"rack 4"}' https://manager-1.internal:21280/v1/enrollments
```

Copy [`worker.env.example`](../deploy/examples/worker.env.example) to `/etc/dockfleet/worker.env` and set `DOCKFLEET_NODE_ID` (it must equal the certificate's node ID or the agent refuses to start), `DOCKFLEET_MANAGER_ADDRS` (every manager of the cluster), the three TLS files, the capacity the node should be scheduled for (`DOCKFLEET_NODE_CPU_MILLIS`, `DOCKFLEET_NODE_MEMORY_MB`: *declared*, not measured) and its labels.

Run it under systemd ([`dockfleet-agent.service`](../deploy/examples/systemd/dockfleet-agent.service)): its own user, in the `docker` group, with the rest of the sandbox closed. There is no Compose file for the agent on purpose: it needs the engine's socket, which is root on the node, and nothing under `deploy/` hands that socket to a container ([the examples README](../deploy/examples/README.md#why-there-is-no-compose-file-for-the-agent)). The image's `agent` target is there for those who package it their own way. The agent leaves its containers running when it stops, so restarting or upgrading the agent restarts no workload; the next agent adopts what it finds.

Check it from the worker and from the manager:

```sh
curl http://127.0.0.1:21282/readyz          # registered, a recent heartbeat accepted, the container engine answers
dockfleet-agent healthcheck                 # the same, as the image's HEALTHCHECK runs it; exit 0 or 1
curl --cacert pki/ca.pem -H "Authorization: Bearer $TOKEN" https://manager-1.internal:21280/v1/nodes
```

A node that is not enrolled stays unready and says so in its log; enroll it and it joins without a restart.

## 6. Deploy something

```sh
curl --cacert pki/ca.pem -H "Authorization: Bearer $TOKEN" -d '{
  "name": "web", "replicas": 3,
  "spec": {"image": "registry.internal/web:1.0", "cpu_millis": 500, "memory_bytes": 268435456,
           "ports": [{"container_port": 8080}], "restart_policy": {"name": "on-failure", "max_retries": 5}}
}' https://manager-1.internal:21280/v1/services

curl ... https://manager-1.internal:21280/v1/services/web          # status: where each replica is and why
curl ... -X PUT -d '{"replicas":5}' .../v1/services/web/scale      # scale
curl ... -X PUT -d '{... "image": "registry.internal/web:1.1" ...}' .../v1/services/web   # rolling update
curl ... -X POST .../v1/services/web/rollback                      # back to the previous spec
```

The full API is in [api.md](api.md); rolling updates in [rollouts.md](rollouts.md); where replicas go in [scheduler.md](scheduler.md). Images are pulled by the worker's Docker Engine, so it needs credentials for a private registry in its own Docker configuration; DockFleet has none.

## 7. High availability *(optional)*

Run etcd as 3 or 5 members with mutual TLS. Then, on **every** manager, add to `manager.env` (a unique `DOCKFLEET_INSTANCE_ID` each):

```
DOCKFLEET_ETCD_ENDPOINTS=https://etcd-1.internal:2379,https://etcd-2.internal:2379,https://etcd-3.internal:2379
DOCKFLEET_ETCD_CA_FILE=/etc/dockfleet/tls/etcd-ca.pem
DOCKFLEET_ETCD_CERT_FILE=/etc/dockfleet/tls/etcd-client.pem
DOCKFLEET_ETCD_KEY_FILE=/etc/dockfleet/tls/etcd-client.key
```

and on every worker set `DOCKFLEET_MANAGER_ADDRS` to all the managers. The node lease must fit a failover: at least `2 × DOCKFLEET_ELECTION_TTL + heartbeat` (the default 60 s lease fits the default 15 s TTL; the manager checks). Upgrade agents before turning this on: an older agent knows one manager and does not understand epochs. Then:

```sh
curl ... https://manager-1.internal:21280/v1/cluster    # role, leader, epoch; ask any manager
```

Expect a crash of the leader to be taken over in about `4/3 × DOCKFLEET_ELECTION_TTL` (6 s measured with a 5 s TTL on a single machine; more across a network), and a clean stop in a couple of seconds, with nothing that runs touched. [high-availability.md](high-availability.md) has the guarantees and the partitions it does not cover.

## Health checks and monitoring

| What | Endpoint | Meaning |
| --- | --- | --- |
| manager alive | `GET /healthz` (no token) | the process serves requests; independent of the database, so a supervisor does not restart a manager that is only waiting for it |
| manager ready | `GET /readyz` (no token) | the manager can reach its database (`503 store unavailable` otherwise) |
| agent alive / ready | `GET http://127.0.0.1:21282/healthz`, `/readyz` | ready: registered, a heartbeat accepted within three intervals, the Docker Engine answers; the `reason` says what is wrong |
| image health | `HEALTHCHECK` = `<binary> healthcheck` | the binary asks its own `/readyz` (the manager over TLS, trusting only its own certificate file; sends no credential) |
| fleet | `GET /metrics` (token) | [observability.md](observability.md): nodes, replicas, rollouts, latency, errors, leadership |
| who leads | `GET /v1/cluster` (token) | per manager |

Alert on: no manager with `dockfleet_manager_is_leader == 1` for longer than the election TTL; a node `unavailable`; `dockfleet_service_replicas{state!="running"}` above zero for longer than a rollout; `dockfleet_reconcile_errors_total` rising; an agent `/readyz` that stays 503; certificates within 30 days of expiry (the processes log warnings; `dockfleet-pki inspect`).

## Upgrades, backups, restores

- **Back up PostgreSQL.** It is the whole state. etcd holds nothing you need to recover.
- **Upgrading agents** is safe at any time: they are compatible within a protocol major version and leave containers running while they restart.
- **Upgrading managers.** The first new manager applies the migrations. Migrations are not reversible and an older manager that is *already running* against a migrated database is not stopped by anything: stop all old managers, start the new ones. With one manager that is a pause of seconds, during which workloads keep running (agents stop their containers only if the pause outlasts the lease, 60 s by default). Rolling a high-availability cluster one manager at a time across versions that change the schema is **not tested**.
- **Restoring an old backup** is safe for what is running: agents ignore an assignment whose generation is lower than the one they act on, and a manager that restarts from older state finds the workloads the agents still report and reconciles. Intent made after the backup is lost, of course.
- **A database outage** is survived: the manager reports not ready, does not hold the silence of nodes against them while it could not record heartbeats, and resumes. An outage longer than `DOCKFLEET_NODE_UNAVAILABLE_AFTER` does not fail the fleet over when the database returns (tested end to end).
- **Shutdown.** Managers and agents finish within the time given and exit 0 on SIGTERM. Agents deregister (the node is `suspect` at once and nothing is moved); a manager that is the leader gives up the lease first.

## Troubleshooting

| Symptom | Look at |
| --- | --- |
| manager exits at startup | the first line says which setting is wrong; configuration errors are listed together |
| agent not ready, log says `not enrolled` | enroll the node ID; no restart needed |
| agent exits: `must match` | the certificate is for another node ID than `DOCKFLEET_NODE_ID` |
| agent exits about a key | the private key is readable by others (`chmod 600`) or the pair does not match |
| agent not ready, TLS errors | the CA file, the manager's certificate names, expiry (`dockfleet-pki inspect`), the clock |
| node `suspect` / `unavailable` | `GET /v1/nodes/{id}` has `health_reason`; the agent's `/readyz` has the reason on its side |
| replicas `unschedulable` | `GET /v1/services/{name}` `status.unschedulable` has a reason per node |
| no leader | `GET /v1/cluster` on each manager; etcd reachable? `dockfleet_election_errors_total` |
| `503 not_leader` on `POST /v1/workloads` | send it to the leader named in the answer |

## What has and has not been verified

Run and passing in the development environment (macOS, no Docker daemon, no systemd): the manager and agent binaries as real processes with mutual TLS from `dockfleet-pki`, PostgreSQL 16.4 and etcd 3.5.35 — deployment, scaling (to the same count, past capacity), rolling update with a sampler checking the surge and unavailability limits, rollback, node crash and recovery, a network partition (the agent fences itself before its replicas are moved), a frozen agent, clean agent and manager restarts, a manager crash, a database crash, leader crash, leader handoff and an etcd outage (`make test-e2e`); the health endpoints and the `healthcheck` subcommands; the example environment files loading through the real configuration code; credentials as files; migrations from every earlier schema.

**Not verified here, and written to be checked where you can:** building the two images (`make docker-build`) and running them; the Compose files; the systemd units (`systemd-analyze security` is a good first look); the GitHub Actions workflow; anything that needs a Docker Engine (`make test-docker`); a multi-member etcd; TLS to a non-loopback etcd; more than a handful of nodes; running on Linux at all (the binaries cross-compile for it). The CI workflow runs all of these, and is the first place to see a regression in them.
