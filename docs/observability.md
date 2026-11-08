# Observability: metrics, the dashboard and the audit trail

The manager reports on itself and on the fleet in three ways. All three are read-only, all three need the administrator's token, and none of them holds a second copy of the cluster: what they show is computed from the store (or from what the manager just did) at the moment they are asked.

| | Where | What it is for |
| --- | --- | --- |
| Metrics | `GET /metrics` | Prometheus text format: nodes, replicas, errors and latency, for alerting and graphs. |
| Dashboard | `/dashboard` | Server-rendered HTML: nodes, services, replicas, health, recent events, for a person. |
| Audit trail | `GET /v1/events` and the manager's log | Who changed what, when and from where. |

`DOCKFLEET_METRICS_ENABLED` and `DOCKFLEET_DASHBOARD_ENABLED` (both default `true`) switch the first two off; off, the paths do not exist (`404`).

## Metrics

`GET /metrics` answers in the [Prometheus text exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/#text-based-format) (version 0.0.4). It needs `Authorization: Bearer <DOCKFLEET_API_TOKEN>`; Prometheus sends it with `authorization.credentials` (or `credentials_file`) in the scrape configuration:

```yaml
scrape_configs:
  - job_name: dockfleet
    metrics_path: /metrics
    authorization:
      credentials_file: /etc/prometheus/dockfleet-token   # the admin token, owner-readable only
    static_configs:
      - targets: ["127.0.0.1:21280"]
```

The registry is small and lives in `internal/metrics` (standard library only: no client library was added). There are two kinds of series. **The state of the fleet** is read from the store on every scrape, through the same code that answers `GET /v1/nodes` and `GET /v1/services`, so a metric cannot disagree with the API. **What the manager does** (requests, passes of its loops, failures) is counted by the code that does it.

### The fleet (gauges, read on every scrape)

| Metric | Labels | Meaning |
| --- | --- | --- |
| `dockfleet_nodes` | `health` = `healthy`, `suspect`, `unavailable` | Registered nodes by health ([failure-handling.md](failure-handling.md)); a state with no node is `0`. |
| `dockfleet_node_healthy` | `node` | `1` if the node is healthy, `0` if it is suspect or unavailable. |
| `dockfleet_node_cpu_millicores` | `node`, `kind` = `capacity`, `allocatable`, `allocated` | CPU as the agent declared it; as the scheduler may promise it (capacity times the overcommit factor); and as it is promised to the workloads meant to run there. |
| `dockfleet_node_memory_bytes` | `node`, `kind` | The same for memory. |
| `dockfleet_node_allocation_ratio` | `node`, `resource` = `cpu`, `memory` | `allocated / allocatable`. |
| `dockfleet_node_workloads` | `node` | Workloads meant to run on the node. |
| `dockfleet_services` | | Services that exist (deleted ones are not counted). |
| `dockfleet_service_replicas_desired` | `service` | Replicas a service is meant to have. |
| `dockfleet_service_replicas` | `service`, `state` = `running`, `starting`, `failed`, `unhealthy`, `lost`, `unschedulable`, `outdated`, `stopping` | Replicas by state, counted exactly as the service's `status` counts them (`running` is `status.scaling.ready`; `lost` is on an unavailable node; `unschedulable` has no node that can take it; `outdated` is of an older revision and still running). |
| `dockfleet_rollouts` | `state` = `progressing`, `paused`, `complete`, `failed` | Services by the state of the rollout of their current revision. |
| `dockfleet_service_rollout_failed` | `service` | `1` if the current revision's rollout made no progress within its deadline. |

**Utilization is what is promised, not what is used.** DockFleet schedules by the CPU and memory limits a service declares and does not measure what containers actually consume ([scheduler.md](scheduler.md)), so `allocated` and `allocation_ratio` are reservations. A node at ratio `1` is full for the scheduler even if its containers are idle. Measured usage is not exported.

### What the manager does (counters and histograms)

| Metric | Labels | Meaning |
| --- | --- | --- |
| `dockfleet_http_request_duration_seconds` | `method`, `route`, `code` | Latency of administrative API requests (histogram). |
| `dockfleet_grpc_request_duration_seconds` | `method`, `code` | Latency of the agents' unary calls (`RegisterNode`, `Heartbeat`, `DeregisterNode`), refusals included (histogram). |
| `dockfleet_scheduling_duration_seconds` | | Duration of scheduling passes (histogram). |
| `dockfleet_scheduling_passes_total` | `result` = `ok`, `error` | Scheduling passes. |
| `dockfleet_scheduling_errors_total` | | Scheduling passes that failed; nothing was saved by them. |
| `dockfleet_scheduling_decisions_total` | `result` = `placed`, `unschedulable` | Replicas placed on a node, and replicas found to have no valid node (counted when the reason is new or changed, as in the audit trail). A failed pass counts none. |
| `dockfleet_reconcile_duration_seconds` | | Duration of reconciliation passes (histogram). |
| `dockfleet_reconcile_passes_total` | `result` | Reconciliation passes. |
| `dockfleet_reconcile_errors_total` | | Reconciliation passes that failed; nothing was saved by them. |
| `dockfleet_deployment_failures_total` | `service` | Rollouts this manager reported failed (once per failure, when the report is saved). |
| `dockfleet_build_info` | `version` | `1`; the version is the label. |
| `dockfleet_start_time_seconds` | | When this manager started. |
| `dockfleet_metrics_dropped_observations_total` | | Observations the registry refused (below). |
| `dockfleet_metrics_collect_errors_total` | `collector` | Scrapes in which a collector failed. |

Notes on how they are kept honest:

- **Request and scheduling latency.** HTTP latency is measured around the whole handler, including authentication, and is labeled with the **route pattern** the request matched (`/v1/services/{name}/scale`), never the raw path. A request that matched nothing is `unmatched`, and a method other than the standard ones is `OTHER`: a client cannot create label values. Scheduling and reconciliation latency are the duration of a whole pass, which is one transaction under the placement lock, so it includes waiting for that lock when several managers share the database.
- **Counters start at zero.** Series that always exist (`ok` and `error` passes, `placed` and `unschedulable`, the error counters) are exported from the first scrape, so `rate()` and `increase()` have a start.
- **Failures are counted where they happen.** A loop pass that fails because the manager is shutting down is not an error. A rollout failure is counted when the reconciler's transaction commits; a pass that is rolled back counts none, and the failure is counted by the pass that succeeds.
- **Several managers.** Counters and histograms are per process (each manager reports its own passes and requests; add them up in Prometheus). The gauges describe the shared database, so every manager reports the same values; query one of them, or take `max`/`avg`.
- **Bounded memory.** An instrument keeps at most 1000 label combinations. Past that, an observation is dropped and counted in `dockfleet_metrics_dropped_observations_total`; a wrong number of label values or a negative counter increment is dropped the same way. Node and service names come from the store, and `dockfleet_service_*` and `dockfleet_node_*` are as many series as there are nodes and services; a fleet with thousands of them should scrape less often or turn metrics off.
- **A failing scrape is partial, not wrong.** If the store cannot be read, the fleet gauges are left out of that scrape, `dockfleet_metrics_collect_errors_total{collector="fleet"}` goes up, and the counters and histograms are still served. A scrape is limited to 10 seconds.
- Label values (node IDs, service names) are escaped as the format requires, and invalid UTF-8 is replaced.

Suggested alerts:

```yaml
- alert: DockFleetNodeNotHealthy
  expr: dockfleet_nodes{health!="healthy"} > 0
  for: 2m
- alert: DockFleetReplicasMissing
  expr: dockfleet_service_replicas{state="running"} < on(service) dockfleet_service_replicas_desired
  for: 5m
- alert: DockFleetRolloutFailed
  expr: dockfleet_service_rollout_failed == 1
- alert: DockFleetReconcileErrors
  expr: increase(dockfleet_reconcile_errors_total[10m]) > 0
- alert: DockFleetSchedulingErrors
  expr: increase(dockfleet_scheduling_errors_total[10m]) > 0
- alert: DockFleetSlowRequests
  expr: histogram_quantile(0.95, sum by (le) (rate(dockfleet_http_request_duration_seconds_bucket[5m]))) > 1
  for: 10m
```

## The dashboard

`/dashboard` is a small server-rendered Go dashboard (`internal/dashboard`, `html/template`, embedded in the binary). There is no JavaScript, no framework and no build step. Pages reload themselves every 10 seconds with a `<meta http-equiv="refresh">` tag.

| Page | Shows |
| --- | --- |
| `/dashboard` | A summary (nodes by health, replicas running of desired and how many have problems, services and failed rollouts, CPU and memory promised against allocatable), the nodes (those that need attention first), the services and the 15 latest events. |
| `/dashboard/services/{name}` | One service: image and revision, scaling and rollout state with the progress deadline, every replica (revision, node and the node's health, wanted and reported state, health check result, restarts, exit code, message), why any replica waits for a node (node by node), and the service's events. |
| `/dashboard/nodes/{id}` | One node: health and why, last heartbeat, lease, labels, CPU and memory promised against allocatable, the workloads placed on it with what their agent reported, and the node's events. |
| `/dashboard/events` | The audit trail, 50 at a time, newest first, filtered by `resource_type` and `resource_id` and paged with `before`. |

**It reuses the API.** The dashboard does not read the store. Each page calls the administrative API's own handler in process (`GET /v1/nodes`, `/v1/services`, `/v1/workloads`, `/v1/events`) with the admin token, and renders the JSON that comes back. What it shows is therefore what an API client sees, with the same node health, the same replica states, and the same limits, and a change to what the API reports is a change to what the dashboard can show. Each call carries the browser's address and its own request ID (`dash-…`), so the API's logs and audit records name the browser that asked. A page whose API call fails is a `502` page that says the manager's API could not be read; the details (database addresses, errors) go to the log and never to the page. A `404` from the API (unknown service or node) is shown as a `404` page with the API's message.

**It changes nothing.** Every page is a `GET`; any other method is `405`. There is no button that scales, pauses, rolls back or deletes: those are the API's. The only `POST`s are logging in and out.

### Access control

- **Logging in.** Browsers cannot send a bearer header on a link, so `/dashboard/login` takes the admin token (`DOCKFLEET_API_TOKEN`) once, compares it in constant time and sets a session cookie. Without a valid session every page redirects (`303`) to the login page and no data is read from the API. A request with a correct `Authorization: Bearer` header is accepted as well (scripts, or a proxy that adds it); a **wrong** bearer header is `401` with a challenge, not a redirect, even beside a valid cookie.
- **The cookie holds no secret and no state.** It is `v1.<expiry>.<HMAC-SHA-256 of the expiry>`, signed with a key derived from the admin token. It expires after 8 hours, is `HttpOnly`, `SameSite=Strict`, scoped to `/dashboard`, and `Secure` whenever the request came over TLS. It is valid on every manager that shares the token and stops being valid everywhere when the token is rotated. Logging out clears it; because there is no server-side session, a copied cookie stays valid until it expires or the token changes (a limitation: rotate the token to revoke).
- **The token is never in a URL, a page or a cookie.** Only the form body counts (`?token=` does nothing). Pages never contain the token, and the API calls the dashboard makes are in process, not over the network.
- **Guessing is slowed down.** Ten wrong tokens from one address within a minute earn `429` (with `Retry-After`) for the rest of the minute, even for the right token; the table of addresses is bounded.
- **Other sites cannot drive it.** Login and logout refuse a request that a browser marks cross-site (`Sec-Fetch-Site`) or whose `Origin` is not the dashboard's own host. After logging in, only a path under `/dashboard` can be returned to: the `next` parameter cannot send an administrator elsewhere.
- **Pages are inert.** Every response carries `Content-Security-Policy: default-src 'none'; style-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'` (no script can run, and styles come only from the dashboard's own stylesheet), `X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer` and `Cache-Control: no-store`. Everything the cluster reports (a node's name and labels, a container's exit message, an event's text) goes through `html/template`'s contextual escaping; a test feeds markup through all of them.
- Behind the TLS the API already uses, nothing changes: the dashboard is served by the same listener as the API, so the same address, certificate and `DOCKFLEET_API_ADDR` rules apply, and a non-loopback bind still requires TLS.

Logging in and out are administrative acts and are written to the audit trail (`dashboard.login`, `dashboard.logout`; resource type `dashboard`, details: the address). A refused login is logged as a warning, not recorded as a change.

## The audit trail

Every change an administrator makes is an **event** in the store, written in the same transaction as the change (`GET /v1/events`; the actions are listed in [state.md](state.md#administrative-events) and [api.md](api.md#audit-events)), and every request that could change something also leaves one structured **audit log record**, whatever came of it. The two share a request ID.

**Events** now say where a change came from. Besides the details of the action, an event recorded for an API request carries:

| Detail | Meaning |
| --- | --- |
| `request_id` | The ID of the request that made the change; the response carries it in `X-Request-ID`. |
| `remote` | The address the request came from: the peer of the connection, never a forwarding header, which anyone can write. |

The actor is `admin` for the shared token. (There is one token; per-user identities would replace the actor, and `request_id` and `remote` would stay.) Events never contain environment values or request bodies.

**Log records.** For each `POST`, `PUT`, `PATCH` or `DELETE` the manager logs one record with the message `audit`, as JSON when `DOCKFLEET_LOG_FORMAT=json`:

```json
{"time":"2026-03-01T09:00:00Z","level":"INFO","msg":"audit","request_id":"3f2a9c1b7d4e8a10","actor":"admin","action":"PUT /v1/services/{name}/scale","path":"/v1/services/web/scale","status":200,"outcome":"accepted","remote":"203.0.113.9","instance_id":"manager-1"}
```

| Field | Meaning |
| --- | --- |
| `outcome` | `accepted` (2xx and 3xx), `rejected` (4xx: invalid, out of date, not found, conflicting; logged at info), `denied` (`401`/`403`: no valid credentials; logged at warning, `actor` is `anonymous`) or `failed` (5xx; logged at error). |
| `action` | The method and the **route pattern**, not the path, so it names the kind of change. `path` is the path, without the query. |
| `request_id` | Sent by the client in `X-Request-ID` if it is 1-64 characters of `[A-Za-z0-9._-]`; otherwise made up. It is also on the `http request` record and in `X-Request-ID` of the response. |

So a change in the trail can be found from a log line (`request_id` → the event whose details have the same `request_id`) and the other way round, and a **refused** attempt, which changes nothing and records no event, is still in the log with who tried it and from where. Reads are not audited. Query strings, request and response bodies, credentials and environment values are never logged (a test checks it).

Not everything is an administrator's act: the manager's own decisions are events too (`replica.scheduled`, `replica.stopped`, `node.unavailable`, `rollout.failed`, ...), with actors `scheduler`, `reconciler` and `node:<id>`, and appear in the dashboard beside the administrators'.

## Code

- `internal/metrics`: the registry, the text format, the instruments the manager uses. Standard library only (`TestMetricsImportsOnlyTheStandardLibrary`).
- `internal/api` (`metrics.go`): `GET /metrics` and the collector that reads the fleet; (`audit.go`): request IDs, the audit record, the context events are built from; (`api.go`): the middleware that times requests.
- `internal/placement`, `internal/reconcile`: an `Observer` each loop is given and calls after every pass; the manager turns the calls into metrics. The loops know nothing about Prometheus.
- `internal/protocol`: `ServerOptions.Observe`, a unary interceptor that sees every agent call, refusals included.
- `internal/dashboard`: the pages, the login and the templates.
- `internal/manager`: puts them together: the dashboard in front of the API, the observers, the dashboard's audit events.

## Tests

- `internal/metrics`: the exposition format byte for byte; cumulative, inclusive histogram buckets; escaping; counters that only go up; the bound on series and the drops it counts; a failing or clashing collector; registration mistakes; concurrent use; each instrument the manager uses (invented methods become `OTHER`, a failed pass counts no decisions, zero series from the first scrape).
- `internal/api` (on the in-memory store and PostgreSQL): `/metrics` needs the admin token and is off without metrics; the gauges for a fleet with nodes in all three health states, checked against the numbers of `GET /v1/nodes` and `GET /v1/services`; the fleet as it changes (replicas that do not fit, containers that fail, a deleted service); latency labeled by route and not by path; every kind of administrative change recording `request_id` and `remote` in its event and one audit record; refusals logged as denied or rejected; secrets, bodies and queries absent from the log.
- `internal/reconcile`, `internal/placement`: the observers see every pass and its error, a rollout failure once and only once it is saved, nothing for a pass cut short by shutdown.
- `internal/protocol`: the interceptor sees granted and refused calls with their codes.
- `internal/dashboard`: access control (redirects, bearer, wrong credentials, sessions that are tampered with, expired or signed with another key, open redirects, throttling, cross-site login and logout, no data read for a request that is not logged in); every response's security headers; operational state rendering for a cluster with healthy, suspect and unavailable nodes, a failed replica and a replica no node can take; escaping of hostile node names, labels, messages and events; paging and filtering of events; an API failure shown without details; that the dashboard only reads, on behalf of the browser, with the admin token and a request ID per call.
- `internal/manager`: a running manager over real sockets exports what an agent and the loops did and shows the fleet in the dashboard, with the login audited; both can be switched off.

## Limitations

- Utilization is the reservation made by declared limits, not measured CPU and memory use.
- Metrics of the loops and of requests are per manager process and reset when it restarts; the gauges are read from the shared database.
- A dashboard session cannot be revoked before it expires except by rotating the admin token.
- One shared admin token means one actor, `admin`, and no roles: whoever can log in sees everything the dashboard shows (nodes, services, replicas, events). It shows no container logs and no environment values.
- The dashboard is for looking; every change is made through the API.
