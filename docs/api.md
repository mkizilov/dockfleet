# Administrative REST API: services

The manager's HTTP API (`internal/api`) is how administrators describe what should run. This document covers **service definitions**. Nodes, workloads, enrollments and events are listed in the [README](../README.md#administrative-api).

A service definition is **desired state**: it says what an administrator wants, and it is stored (PostgreSQL, or memory for development) as such. Defining, changing or deleting a service does **not** place, schedule, start or stop anything. Nothing turns definitions into containers yet; the definitions, their revision history and their replica counts are what a later controller will act on. See [state.md](state.md).

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
    "restart_policy": {"condition": "on-failure", "max_retries": 5},
    "node_selector": {"zone": "a"}
  }
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
| `spec.restart_policy` | no | `condition` is `no`, `on-failure` or `always`. `max_retries` (1-100, default 5) applies to `on-failure` only. Omitted means `on-failure` with 5 retries. |
| `spec.node_selector` | no | Up to 16 labels a node must carry. Used when placement exists. |

Defaults are written into the stored definition, and the definition is put in a canonical form (ports sorted, empty values dropped). So `GET` shows what is stored, the same definition written differently is not a change, and **what `GET` returns is a valid `PUT` body** apart from the read-only fields below.

### Rejected options

Only the fields above exist. Anything else is refused rather than ignored, so a typo or an unimplemented option can never be silently dropped. Docker options that would weaken the isolation between a container and its host get a specific message: `privileged`, `cap_add`, `devices`, `device_cgroup_rules`, `volumes`, `volumes_from`, `mounts`, `binds` (so no Docker socket or host path can be mounted), `network_mode`, `pid`/`pid_mode`, `ipc`/`ipc_mode`, `uts`/`uts_mode`, `userns_mode`, `security_opt`, `sysctls`, `cgroup_parent`, `group_add`, `host_ip`, `extra_hosts`. `id`, `version`, `revision`, `created_at`, `updated_at` and `deleted_at` are read-only.

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
  "created_at": "2026-01-01T12:00:00Z",
  "updated_at": "2026-01-02T08:30:00Z"
}
```

`revision` is the deployment revision currently wanted and `spec` its content. `version` increases with every change and is also the `ETag` (`"5"`). A deleted service has `deleted_at` and `replicas` 0.

## Updating and deleting

`PUT` replaces the definition. It must carry `replicas` and a complete `spec` (`name` is optional and must match the URL). Then:

- a changed `spec` becomes a **new deployment revision** (revision numbers only grow; rolling back is a `PUT` of an old spec, which becomes a new revision with the old content);
- a changed `replicas` is a **scaling** and adds no revision;
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

## Audit events

Each successful change is recorded in `GET /v1/events` in the same transaction as the change: `service.created`, `service.deployed` (from and to revision, image), `service.scaled` (from and to replicas) and `service.deleted`, with the actor `admin`. Events never contain environment values. Requests that fail validation or change nothing record nothing.

## Not yet

No pagination of the service list, no partial (`PATCH`) updates, no secret values, no image allow-list, no per-user authentication (one shared admin token), and, by design for now, no placement or scheduling.

## Tests

`internal/api/services_*_test.go` is one suite run against the in-memory store always and against PostgreSQL under the `integration` build tag (`make test-integration`; every test gets a throwaway database). It covers authentication (missing, malformed and wrong credentials on every route), CRUD, equivalent definitions, optimistic locking, deletion and name reuse, revisions, events, a validation table of over a hundred bad inputs, every unsafe option at every nesting level, request limits, and that no workload is ever created. PostgreSQL-only tests restart the store on the same database and run two managers against one.
