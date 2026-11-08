# Administrative REST API: services

The manager's HTTP API (`internal/api`) is how administrators describe what should run. This document covers **service definitions**. Nodes, workloads, enrollments and events are listed in the [README](../README.md#administrative-api).

A service definition is **desired state**: it says what an administrator wants, and it is stored (PostgreSQL, or memory for development) as such. Defining, changing or deleting a service through this API does **not** place, schedule, start or stop anything itself: the handlers only record intent (and read the results back, see [Scheduling status](#scheduling-status)). Separate loops in the manager act on it: the scheduler ([scheduler.md](scheduler.md)) assigns waiting replicas to nodes and records why any has no node, and the reconciler ([reconciliation.md](reconciliation.md)) stops the replicas that should go; the agents create, start, stop and remove the containers. So a service shows `status` of an empty set for up to `DOCKFLEET_SCHEDULER_INTERVAL` after it is created, and its replicas run a few seconds later. See [state.md](state.md).

## Access

- **Authentication.** Every `/v1/` endpoint needs `Authorization: Bearer <DOCKFLEET_API_TOKEN>`; without it the answer is `401` with `WWW-Authenticate: Bearer` and no data. The token is compared in constant time, must be at least 16 characters, and is never defaulted. Rejected requests are logged (method, path, remote address; never the credentials). Only `/healthz` and `/readyz` are open.
- **Network.** The API listens on `127.0.0.1:21280` by default. A non-loopback address is refused at startup unless TLS is configured (`DOCKFLEET_TLS_CERT_FILE`, `_KEY_FILE`, `_CLIENT_CA_FILE`), in which case the API serves HTTPS. See [mtls.md](mtls.md).
- **Responses** carry `Cache-Control: no-store` and `X-Content-Type-Options: nosniff`, because they can contain environment variables. Request bodies are limited to 1 MiB (`413` beyond that). Request bodies are never logged.

## Endpoints

| Method and path | Purpose | Success |
| --- | --- | --- |
| `POST /v1/services` | Create a service | `201`, `Location`, `ETag` |
| `GET /v1/services` | List services (`?include_deleted=true` adds deleted ones) | `200` |
| `GET /v1/services/{name}` | Inspect one | `200`, `ETag` |
| `PUT /v1/services/{name}` | Replace the definition | `200`, `ETag` |
| `DELETE /v1/services/{name}` | Delete | `200` with the deleted service |
| `GET /v1/services/{name}/revisions` | Deployment history, oldest first | `200` |
| `GET /v1/services/{name}/scale` | The wanted replica count and the progress towards it | `200`, `ETag` |
| `PUT /v1/services/{name}/scale` | Set the wanted replica count (see [Scaling](#scaling)) | `200`, `ETag` |
| `GET /v1/services/{name}/rollout` | The rollout policy, whether rollouts are paused, the progress of the current deployment and how earlier ones ended | `200`, `ETag` |
| `POST /v1/services/{name}/pause` | Hold rollouts where they are | `200`, `ETag` |
| `POST /v1/services/{name}/resume` | Let them go on | `200`, `ETag` |
| `POST /v1/services/{name}/rollback` | Return to an earlier spec, as a new revision | `200`, `ETag` |

Services are addressed by **name**, which is unique among live services. A deleted service's name can be used again by a new service (with a new `id`), so a deleted service is no longer addressable by name; `GET /v1/services?include_deleted=true` still lists it.

Errors are `{"error": "..."}`. Validation failures are `400` with every problem found:

```json
{
  "error": "invalid service definition",
  "details": [
    {"field": "spec.cpu_millis", "message": "must be between 1 and 1000000 (1000 is one CPU)"},
    {"field": "spec.privileged", "message": "is not allowed: DockFleet never lets a service weaken the isolation between its container and the host"}
  ]
}
```

Other statuses: `404` unknown service, `409` name already in use (or a concurrent change without `If-Match`), `412` `If-Match` no longer matches, `413` body too large, `405` wrong method.

## The definition

```json
{
  "name": "web",
  "replicas": 3,
  "spec": {
    "image": "nginx:1.27",
    "command": ["nginx", "-g", "daemon off;"],
    "env": {"MODE": "production"},
    "cpu_millis": 500,
    "memory_bytes": 134217728,
    "ports": [{"container_port": 80, "protocol": "tcp", "host_port": 18080}],
    "health_check": {
      "command": ["curl", "-f", "http://localhost/healthz"],
      "interval_seconds": 30, "timeout_seconds": 5, "start_period_seconds": 0, "retries": 3
    },
    "restart_policy": {"condition": "on-failure", "max_retries": 5, "backoff_seconds": 1, "max_backoff_seconds": 60},
    "node_selector": {"zone": "a"}
  },
  "rollout": {"max_unavailable": 0, "max_surge": 1, "progress_deadline_seconds": 600}
}
```

| Field | Required | Rules |
| --- | --- | --- |
| `name` | create | 1-63 characters of `[A-Za-z0-9._-]`, starting with a letter or digit. Cannot change. |
| `replicas` | yes | Desired count, 0-1000. |
| `spec.image` | yes | A Docker image reference: `[host[:port]/]repository[:tag][@sha256:digest]`, lower-case repository, at most 255 characters. A tag or digest is recommended; a digest makes the deployment exact. |
| `spec.command` | no | The program and its arguments (exec form), at most 128 arguments of 4096 bytes; replaces the image's default command. |
| `spec.env` | no | Up to 128 variables; names `[A-Za-z_][A-Za-z0-9_]*`, values up to 32 KiB. **Values are stored and returned in plain text**; there is no secret store yet, so do not put credentials here. |
| `spec.cpu_millis` | yes | CPU limit, 1-1000000; 1000 is one CPU. Every service has one. |
| `spec.memory_bytes` | yes | Memory limit, 6 MiB to 1 TiB. Every service has one. |
| `spec.ports` | no | Up to 32. `container_port` 1-65535 (required); `protocol` `tcp` (default) or `udp`; `host_port` 0 or omitted to let the node choose, otherwise 1024-65535 (lower ports are privileged). Ports and host ports must not repeat. Where a port is published is the agent's decision (loopback by default); a fixed `host_port` allows one replica per node. |
| `spec.health_check` | no | `command`: program and arguments run inside the container, exit status 0 is healthy; at most 32 arguments. Exec form only: a leading `CMD`, `CMD-SHELL` or `NONE` is refused, so there is no shell string. `interval_seconds` (default 30, max 3600), `timeout_seconds` (5, max 300), `start_period_seconds` (0, max 3600), `retries` (3, max 10); zero or omitted means the default. |
| `spec.restart_policy` | no | `condition` is `no`, `on-failure` or `always`. `max_retries` (1-100) is how many restarts in a row are allowed: default 5 for `on-failure`; for `always` it is optional and means no limit when omitted. `backoff_seconds` (1-3600, default 1) is the wait before the second restart in a row, doubling with each further one up to `max_backoff_seconds` (1-3600, default 60, at least `backoff_seconds`). The first restart is immediate. A container that has run for 30 seconds starts over. `no` takes none of the three. Omitted means `on-failure` with 5 retries. A container that turns unhealthy is stopped and restarted under the same policy ([failure-handling.md](failure-handling.md)). |
| `spec.node_selector` | no | Up to 16 labels a node must carry. The scheduler only places a replica on a node whose labels include all of them. |
| `rollout` | no | How a change of spec replaces the running replicas ([rollouts.md](rollouts.md)): `max_surge` (0-1000, default 1) is how many replicas may run above `replicas` while it does, `max_unavailable` (0-1000, default 0) how many below it may be not ready, `progress_deadline_seconds` (1-86400, default 600) how long it may go without progress before it is reported failed. They cannot both be 0: a rollout could never replace a replica. A field left out keeps its value (the service's current one on an update, the default on creation). It belongs to the service, not to a revision: changing it starts no deployment. |

Defaults are written into the stored definition, and the definition is put in a canonical form (ports sorted, empty values dropped, the default restart waits stored as absent, so writing `"backoff_seconds": 1` and leaving it out are one definition). So `GET` shows what is stored, the same definition written differently is not a change, and **what `GET` returns is a valid `PUT` body** apart from the read-only fields below.

### Rejected options

Only the fields above exist. Anything else is refused rather than ignored, so a typo or an unimplemented option can never be silently dropped. Docker options that would weaken the isolation between a container and its host get a specific message: `privileged`, `cap_add`, `devices`, `device_cgroup_rules`, `volumes`, `volumes_from`, `mounts`, `binds` (so no Docker socket or host path can be mounted), `network_mode`, `pid`/`pid_mode`, `ipc`/`ipc_mode`, `uts`/`uts_mode`, `userns_mode`, `security_opt`, `sysctls`, `cgroup_parent`, `group_add`, `host_ip`, `extra_hosts`. `id`, `version`, `revision`, `status`, `paused`, `created_at`, `updated_at` and `deleted_at` are read-only (pausing is `POST /v1/services/{name}/pause`).

The limits are kept at least as strict as the agent's container policy ([docker.md](docker.md)): the agent re-checks every container it creates and refuses privileged mode, Docker socket mounts and the rest again, so a definition the API accepts is never one an agent would reject for its content.

## Reading a service

```json
{
  "id": "5f0c...",
  "name": "web",
  "replicas": 3,
  "revision": 2,
  "version": 5,
  "spec": { "...": "as above" },
  "rollout": {"max_unavailable": 0, "max_surge": 1, "progress_deadline_seconds": 600},
  "status": { "...": "see Scheduling status" },
  "created_at": "2026-01-01T12:00:00Z",
  "updated_at": "2026-01-02T08:30:00Z"
}
```

`revision` is the deployment revision currently wanted and `spec` its content. `version` increases with every change and is also the `ETag` (`"5"`). A deleted service has `deleted_at` and `replicas` 0. `status` is the scheduler's, not part of the definition: it is read-only, and a `PUT` body that includes it is refused like any other read-only field (leave it out when writing a `GET` back).

## Updating and deleting

`PUT` replaces the definition. It must carry `replicas` and a complete `spec` (`name` is optional and must match the URL). Then:

- a changed `spec` becomes a **new deployment revision** and a **rolling deployment** ([rollouts.md](rollouts.md)): revision numbers only grow; rolling back is a `PUT` of an old spec, or `POST /v1/services/{name}/rollback`, which becomes a new revision with the old content;
- a changed `rollout` policy is recorded (`service.rollout_configured`) and starts no deployment;
- a changed `replicas` is a **scaling** and adds no revision (`PUT /v1/services/{name}/scale` changes only the count: see [Scaling](#scaling));
- an identical definition changes nothing: same version, no revision, no event.

`DELETE` marks the service deleted and its desired replicas zero. Its revisions, workloads and events are kept.

**Lost updates.** Send `If-Match: "<version>"` (the `ETag` you read) with `PUT` or `DELETE` and the change only applies to that version; otherwise `412` and nothing changes, even if the change would have been a no-op. `If-Match: *` or no header applies to whatever is current. Several managers can share the database; versions are how their writers (and administrators) avoid overwriting each other.

```sh
TOKEN=...  # DOCKFLEET_API_TOKEN
API=http://127.0.0.1:21280

curl -s -H "Authorization: Bearer $TOKEN" -d '{"name":"web","replicas":2,"spec":{"image":"nginx:1.27","cpu_millis":500,"memory_bytes":134217728}}' $API/v1/services
curl -s -H "Authorization: Bearer $TOKEN" $API/v1/services/web -D -          # note the ETag
curl -s -X PUT -H "Authorization: Bearer $TOKEN" -H 'If-Match: "1"' \
     -d '{"replicas":4,"spec":{"image":"nginx:1.27","cpu_millis":500,"memory_bytes":134217728}}' $API/v1/services/web
curl -s -H "Authorization: Bearer $TOKEN" $API/v1/services/web/revisions
curl -s -X DELETE -H "Authorization: Bearer $TOKEN" $API/v1/services/web
```

## Scaling

`PUT /v1/services/{name}/scale` sets how many replicas a service wants, and nothing else: the image, the limits and the revision stay as they are, so no replica is replaced. The body is `{"replicas": 5}` (0 to 1000; anything else in it is refused, and so is a missing, negative, fractional or non-numeric count, with `400` and every problem found). `GET` on the same path reads the count and the progress. Both answer with the service's `name`, `revision`, `version` (also the `ETag`), `replicas` and `scaling`; the answer to a `PUT` adds `previous_replicas` and `changed`:

```json
{
  "name": "web", "revision": 2, "version": 7, "replicas": 5,
  "previous_replicas": 3, "changed": true,
  "scaling": {
    "state": "scaling_up", "desired": 5, "ready": 3, "starting": 1, "waiting": 0, "surplus": 0, "stopping": 0, "outdated": 0,
    "message": "3 ready, 5 wanted; 1 to create, 1 starting"
  }
}
```

Like every call of this API it only records intent. The scheduler creates the replicas that are missing, the reconciler stops the ones in surplus, and the agents start and stop the containers ([reconciliation.md](reconciliation.md#scaling)); the answer is how far that had come when it was given, and `GET` shows the rest as it happens.

**Idempotent.** The request says what the count should be, not by how much to change it. Asking for the count a service already has changes nothing: the same version, no event, no container, `"changed": false`, and `200`, even if the request carries an `If-Match` that is out of date (the service already is as the client wants it, so a request whose answer was lost can be sent again). A request that does change the count follows the rules of `PUT /v1/services/{name}`: with `If-Match` it applies only to that version (`412` otherwise); without, to whatever is current. Each change is one `service.scaled` event with the old and the new count.

**`scaling.state`** says where the service is between the count it has and the one it was asked for:

| State | Meaning |
| --- | --- |
| `stable` | As many replicas are ready as are wanted, and nothing is on its way out. A service scaled to 0 is `stable` once its last container is gone (`none wanted, none left`). |
| `scaling_up` | Replicas are still to be created by the scheduler (`message` counts them) or are starting (`starting`). |
| `scaling_down` | Replicas above the wanted count are still to be stopped (`surplus`) or are being stopped (`stopping`, until their nodes confirm that the containers are gone). A node that is `unavailable` cannot confirm, so a replica on one does not hold the state back. |
| `updating` | Replicas of an older revision still run (`outdated`): a deployment is rolling out. |
| `blocked` | Some replicas (`waiting`) cannot be placed. The message carries the scheduler's reason; the full explanation, with a reason for every node, is in `status.unschedulable` of the service. The replicas that fit run regardless, and the service completes by itself when room appears (a node joins, resources are freed) or the count is lowered. |
| `degraded` | Fewer replicas are ready than wanted and nothing is on its way to fix it: some failed beyond their restart limit, fail their health check, or sit on an unavailable node. See [failure-handling.md](failure-handling.md). |

`ready` counts the replicas of the current revision that run (or finished successfully) on a node that is not unavailable and, if they have a health check, pass it. While a service is scaled down it can be more than `desired`.

**Which replicas go.** A scale-down does not stop the highest numbered replicas but the ones doing the least good: failed ones first, then unhealthy ones, then those on a node that has gone quiet (`suspect`), then those that are not running yet. Among the rest, the node that runs most replicas of the service gives one up, so the replicas stay spread; then the one that was restarted most, then the newest. A healthy replica that has run for a while is the last to go, and a replica that stays is not touched: it keeps its container, its workload and its generation. The choice is recorded in the `replica.stopped` event (`why`: `failed`, `unhealthy`, `suspect_node`, `not_running`, `spread`, `restarts` or `newest`). A scale-up fills the lowest free replica numbers, so after a scale-down the numbers in use need not be 1 to N.

**Capacity and unavailable nodes.** A scale-up places replicas only where there is room (CPU, memory, host ports, node selector) on a node that answers; nothing is oversubscribed unless `DOCKFLEET_SCHEDULER_*_OVERCOMMIT` allows it ([scheduler.md](scheduler.md)). The count can be changed while nodes are away: the others are used, replicas that do not fit wait with a reason that names the unavailable nodes, and when a node returns it removes what it was told to stop and takes what is waiting.

```sh
curl -s -X PUT -H "Authorization: Bearer $TOKEN" -d '{"replicas":5}' $API/v1/services/web/scale
curl -s -H "Authorization: Bearer $TOKEN" $API/v1/services/web/scale   # progress
```

## Rolling deployments

Changing the `spec` deploys a new revision, and the running replicas are replaced a few at a time, within the service's `rollout` policy: at most `max_surge` replicas above the wanted count, at least `replicas` minus `max_unavailable` ready. [rollouts.md](rollouts.md) describes the rules in full; in short:

- `GET /v1/services/{name}/rollout` reports the policy, whether the service is paused, the progress of the current deployment (`state` `progressing`, `paused`, `complete` or `failed`; replicas of the new spec created and ready, old ones left; the `message` that says what it is waiting for; when it started, last progressed and will be reported failed) and `history`, how each earlier revision's rollout ended. The same progress is `status.rollout` of every service view.
- A rollout that has no room for its extra replica, or no new replica that is ready, **waits and says so** instead of giving up capacity the policy forbids; after `progress_deadline_seconds` without progress it is reported `failed` (`reason` `progress_deadline_exceeded`) with the cause in `message`, and the `rollout.failed` event. It is still carried on, and goes back to `progressing` if it moves again.
- `POST /v1/services/{name}/pause` and `/resume` hold and release rollouts. Both are idempotent: asking for the state the service is in changes nothing, records nothing, and succeeds, even with an out-of-date `If-Match`. While paused no replica of the new spec is added and no old one retired; the progress deadline does not run.
- `POST /v1/services/{name}/rollback` deploys an earlier revision's spec again as a new revision. Without a body it is the latest earlier revision whose spec differs from the current one; `{"revision": N}` names it, and then the request is idempotent (a service that already runs that spec answers `"changed": false`). Replicas that still run that spec are kept. A rollback clears a pause. `409` when there is nothing to go back to, `404` for a revision that does not exist.

The answers of the POSTs carry `changed`, and `rolled_back_to` for a rollback.

## Scheduling status

Every service view (`GET /v1/services`, `GET /v1/services/{name}` and the responses of `POST`, `PUT` and `DELETE`) carries what the scheduler has decided:

```json
"status": {
  "desired_replicas": 3,
  "scheduled_replicas": 2,
  "unschedulable_replicas": 1,
  "running_replicas": 1,
  "failed_replicas": 0,
  "unhealthy_replicas": 0,
  "lost_replicas": 0,
  "scaling": {"state": "scaling_up", "desired": 3, "ready": 1, "starting": 1, "waiting": 1, "surplus": 0, "stopping": 0, "outdated": 0, "message": "..."},
  "rollout": {"revision": 2, "state": "progressing", "message": "...", "paused": false, "desired": 3, "updated": 1, "ready": 0, "outdated": 3, "surge": 1, "unavailable": 0, "started_at": "...", "progress_at": "...", "deadline_at": "..."},
  "replicas": [
    {"revision": 2, "index": 1, "workload_id": "9c1e...", "workload_name": "web-r2-1-3f9a2c",
     "node_id": "worker-1", "desired_state": "running", "generation": 1, "acknowledged": false, "phase": "scheduled",
     "restart_count": 0, "node_health": "healthy"},
    {"revision": 2, "index": 2, "workload_id": "b07d...", "workload_name": "web-r2-2-81c0de",
     "node_id": "worker-2", "desired_state": "running", "generation": 1, "acknowledged": true, "phase": "running",
     "message": "running", "container_id": "4be1...", "health": "healthy", "restart_count": 1, "node_health": "healthy"}
  ],
  "unschedulable": [
    {"service_id": "5f0c...", "revision": 2, "replica_index": 3,
     "code": "insufficient_resources",
     "message": "0/2 nodes can run web#3: 2 node(s) lack CPU (needs 500m)",
     "nodes": [
       {"node_id": "worker-1", "code": "insufficient_resources", "message": "insufficient resources (CPU: needs 500m, 100m free)"},
       {"node_id": "worker-2", "code": "insufficient_resources", "message": "insufficient resources (CPU: needs 500m, 0m free)"}
     ],
     "since": "2026-01-02T08:31:05Z"}
  ]
}
```

- `scaling` is the progress towards the wanted count, as described under [Scaling](#scaling). `rollout` is the progress of the current revision's deployment, as described under [Rolling deployments](#rolling-deployments); `outdated` counts replicas that do not run the current spec, and a replica of an older revision that runs the same spec (after a rollback) is not one.
- `scheduled_replicas` counts the replicas of the current revision that have a workload meant to run, up to the wanted count; `unschedulable_replicas` those that have none because no node qualifies.
- `running_replicas` counts the wanted replicas of the current revision whose agent reports the container running and healthy on a node that is not unavailable. The others are counted where they are: `failed_replicas` (the container failed and is not being restarted, or not yet), `unhealthy_replicas` (it runs but fails its health check) and `lost_replicas` (its node is unavailable, so the manager is moving it). A replica that has not reported yet is only `scheduled`. How failures are handled is in [failure-handling.md](failure-handling.md).
- `replicas` lists the service's workloads, by revision and index, including those of older revisions: they stay until the reconciler has replaced them (an older revision's replica is stopped for each replica of the current revision that runs) and their node has confirmed that the container is gone. `phase` is what the node's agent last reported, or `scheduled` before it has reported anything; `message` and `container_id` are the agent's words and the container it reported; `health` is its health check result (absent without a check), `restart_count` how often the agent restarted it, `exit_code` how it last ended, and `node_health` the state of its node (`healthy`, `suspect`, `unavailable`). The replica that was moved off a lost node stays listed as `stopped` until its node confirms. The decision (`node_id`, `desired_state`, `generation`) and the observation (`phase`, `acknowledged`) are separate facts: `generation` counts the instructions given for the workload, and `acknowledged` says its node has received the latest. See [reconciliation.md](reconciliation.md).
- `unschedulable` explains each waiting replica: a `code` (`no_nodes`, `no_available_nodes`, `selector_unsatisfied`, `host_port_conflict`, `insufficient_resources`, `exceeds_capacity`), a `message`, one reason per node, and `since`, when this explanation first applied (it does not move while nothing changes). The codes are described in [scheduler.md](scheduler.md#why-a-replica-cannot-be-placed). Scheduling runs every `DOCKFLEET_SCHEDULER_INTERVAL`, so a new or changed service shows an empty status for up to that long.

The assignments themselves are ordinary workloads: `GET /v1/workloads` lists them with `node_id`, `desired_state`, `service_id`, `service_revision`, `replica_index` and `ports`, and each carries the agent's `observed` report beside the decision. The scheduler's audit events, with the actor `scheduler`, are in `GET /v1/events`: `replica.scheduled` (resource: the workload; details: service, revision, replica, node) and `replica.unschedulable` (resource: the service), the latter only when an explanation is new or changed.

Failures are on the same trail: the reconciler records `node.suspect`, `node.unavailable` and `node.recovered` (resource: the node) and `replica.stopped` with the reason `node_unavailable` when it moves a replica, and agents' reports produce `workload.failed`, `workload.unhealthy`, `workload.restarted` and `workload.recovered` (resource: the workload), each once per change.

### Nodes and standalone workloads

The same scheduler serves the other state endpoints:

- `GET /v1/nodes` and `GET /v1/nodes/{id}` show, beside `status` (`ready` or `not_ready`), a `status_reason` when it is not ready (for example `no heartbeat within the last 30s`), the `allocatable` CPU and memory (`cpu_millis`, `memory_bytes`: capacity times the overcommit factor) and what is `allocated` to workloads that are meant to run (`cpu_millis`, `memory_bytes`, `workloads`).
- The same views carry the node's **health**, from how long it has been silent: `health` is `healthy`, `suspect` (silent longer than `DOCKFLEET_NODE_TTL`, or its agent has left: it gets no new replicas but keeps its own) or `unavailable` (silent longer than `DOCKFLEET_NODE_UNAVAILABLE_AFTER`: its replicas are being started on other nodes), with a `health_reason`, `health_since` (when the manager recorded the state; absent until it has) and `lease_expires_at` (last heartbeat plus the lease; an agent that fences itself stops its containers then). It is computed when read. See [failure-handling.md](failure-handling.md).
- `POST /v1/workloads` places the workload with the same rules. When nothing fits the answer is `409` with the reason for the request and for every node: `{"error": "0/1 nodes can run ...", "code": "insufficient_resources", "nodes": [{"node_id": "...", "code": "...", "message": "..."}]}`. A node whose enrollment was revoked gets nothing. (A duplicate name is also `409`, with only `error`.)

## Audit events

Each successful change is recorded in `GET /v1/events` in the same transaction as the change: `service.created`, `service.deployed` (from and to revision, image), `service.scaled` (from and to replicas) and `service.deleted`, with the actor `admin`. Events never contain environment values. Requests that fail validation or change nothing record nothing.

## Not yet

No pagination of the service list, no partial (`PATCH`) updates, no secret values, no image allow-list, no per-user authentication (one shared admin token), no rescheduling of a replica whose container failed beyond its restart limit (see [failure-handling.md](failure-handling.md#limitations)), and no automatic rollback of a failed rollout ([rollouts.md](rollouts.md#limitations)).

## Tests

`internal/api/services_*_test.go` is one suite run against the in-memory store always and against PostgreSQL under the `integration` build tag (`make test-integration`; every test gets a throwaway database). It covers authentication (missing, malformed and wrong credentials on every route), CRUD, equivalent definitions, optimistic locking, deletion and name reuse, revisions, events, a validation table of over a hundred bad inputs, every unsafe option at every nesting level, request limits, that the handlers never create a workload, and the status after a scheduling pass (assigned, unschedulable with a reason for every node, across revisions, scaling and deletion), the node view and the standalone workload refusals. The scale endpoint has its own tests in the same suite: reading and setting the count with the progress at each step, repeated and stale requests, fifteen kinds of bad request, a scale-up and scale-down through the scheduler, the reconciler and simulated agents (the replicas that stay are the very same workloads), more replicas than fit, and a scaling while a node is unavailable. The rollout part has its own: the policy in the definition and its validation, pausing and resuming, rolling back (the previous revision, a named one, repeated and refused requests), and the progress of a rollout from its first step to a reported failure and a rollback that keeps the replicas it returns to. PostgreSQL-only tests restart the store on the same database and run two managers against one.
