# Centralized container logs

An administrator reads the output of DockFleet's containers from the manager, whichever node they run on: the recent history, the lines written from now on, one replica or a whole service, with the time each line was written. The agents stream the logs to the manager over their authenticated gRPC connection, and the manager streams them on over the administrative API. Memory is bounded on both sides, so a slow reader slows the stream down instead of filling anyone's memory.

```
administrator ──GET /v1/services/web/logs──▶ manager ◀══ Logs stream ══ agent ── Docker daemon
 (HTTP, admin token)      ndjson / text / sse   (hub)     (gRPC, mTLS)     (reads the container's log files)
```

## The path of a line

1. **The agent opens a log stream.** After it registers, the agent opens one long-lived bidirectional `Logs` stream to the manager and says which node and session it speaks for (the *hello*). The manager never connects to agents, so the requests for logs travel down the stream the agent opened. The stream lives as long as the session; when it drops, the agent opens it again.
2. **The administrator asks.** `GET /v1/services/{name}/logs` or `GET /v1/workloads/{id}/logs` names the containers (see below). The API looks up where they run in the store and opens a *query* on the manager's hub.
3. **The hub sends a request** (`LogOpen`) for each container down the stream of the node it runs on: how many lines of history, after which time, whether to follow, how long a line may be, and a window of credit.
4. **The agent reads the container's output from Docker** (the container's `json-file` log, through the Docker Engine API, with timestamps), cuts it into lines and sends batches of them (`LogBatch`), oldest first. First the history, ending with a batch marked `history_complete`; then, if the request follows, the lines the container writes afterwards, until it stops.
5. **The hub merges and delivers.** The history of all the containers of a query is merged by time, so the replicas of a service read as one log; after the history, new lines come in the order they arrive. The API writes them to the response. Merging needs the first line of every container's history, so the hub waits for the slowest agent, but for at most 10 seconds (`HubConfig.HistoryWait`): a container whose agent has not answered by then no longer holds the others back, and its lines are delivered as they arrive.

Only the agent talks to Docker, and only about containers that carry its own node's DockFleet labels, addressed by workload ID (`internal/docker`): the logs of a container DockFleet does not manage cannot be read through DockFleet.

## Asking for logs

`GET /v1/services/{name}/logs` streams the logs of the service's replicas that are meant to run, of every revision. `GET /v1/workloads/{id}/logs` streams one workload's, a replica or a standalone workload. Both need the admin token ([api.md](api.md)), only read, and answer with a stream that stays open while it has something to follow.

| Parameter | Meaning |
| --- | --- |
| `tail` | How many of the last lines of each container to start with: `0` to `5000`, or `all` (which is 5000). Default 100. |
| `follow` | `true` keeps the response open and sends lines as the containers write them, until they stop or the client leaves. Default `false`: the history, then the response ends. |
| `since` | Only lines after a time: a duration such as `10m` (up to 30 days) or an RFC 3339 time. |
| `after` | A *cursor*: only lines after the ones it names, per container (below). |
| `timestamps` | `false` leaves the time out of the output. Default `true`. |
| `format` | `ndjson` (default), `text` or `sse`. A client that sends `Accept: text/event-stream` gets `sse` unless `format` says otherwise. |
| `replica`, `revision`, `node`, `workload` | Service endpoint only. Pick replicas by number (several, separated by commas or repeated), by revision, by node, or one workload by ID. |

A parameter that is not supported, given twice, or out of range is refused with a `400` that lists every problem, rather than ignored, so a typo cannot silently widen a filter. At most 32 containers can be read at once (`400` if more match: narrow them). `404` for an unknown service or workload and for a filter that matches no replica; `429` (with `Retry-After`) when the manager serves as many streams as it allows; `503` when the manager does not serve logs or is shutting down.

### Output

`ndjson` is one JSON object per line:

```json
{"type":"line","time":"2026-03-01T09:00:00.123456789Z","service":"web","replica":2,"revision":3,"workload":"5f0c…","name":"web-r3-2-8f3a1c","node":"node-b","stream":"stdout","text":"GET /healthz 200"}
{"type":"notice","kind":"interrupted","service":"web","replica":2,"workload":"5f0c…","node":"node-b","message":"the connection to node node-b was lost; waiting up to 30s for it to come back"}
{"type":"end","reason":"complete","cursor":"5f0c…=1772355600123456789,9a1b…=1772355600098765432"}
```

- `time` is when the container wrote the line, as the Docker daemon recorded it, with nanoseconds, in UTC. A line carries `"truncated":true` if it was longer than the limit (16 KiB) and the rest was dropped. `stream` is `stdout` or `stderr`.
- A **notice** says something about one container's part of the stream: `unavailable` (its node is not connected to this manager, has no container for it, or is busy), `interrupted` and `resumed` (the connection to the node dropped and came back), `ended` (a following container stopped), `error`. A stream is never silently shorter than it should be.
- The **end** record is the last one. `reason` is `complete` (everything there was has been sent) or `closed` (the manager is shutting down). It carries the `cursor`.

`text` is for people: `<time> <service>/<replica> <stream>: <text>` (a standalone workload is shown by its name; `--` lines are notices and the end). Control characters in a container's output are replaced with U+FFFD so that a container cannot move the cursor of an administrator's terminal or rewrite what is on the screen. The JSON formats escape them.

`sse` sends each record as a [server-sent event](https://html.spec.whatwg.org/multipage/server-sent-events.html) (`event: line`, `notice` or `end`, `data:` the JSON record). Every line event carries an `id:` with the cursor, so a browser's `EventSource` that reconnects sends it back as `Last-Event-ID`, which the endpoint reads as `after`. An idle following stream gets a comment line every 15 seconds so that proxies keep it open.

### Resuming

A **cursor** is, for each container, the time of the newest line the client has been given: `workload=unixnano,workload=unixnano`. It is exclusive: `after` returns lines strictly later. The end record and every `sse` event carry the cursor of the stream at that point; a client that tracks `workload` and `time` of the lines it receives can build one itself (an RFC 3339 time works in place of nanoseconds). To resume after the response was interrupted, send the last cursor as `after`. Lines the client already has are not sent again, and none after them are skipped, provided the container's log still holds them (Docker rotates the log files: 3 files of 10 MiB per container).

## Interruptions

- **The connection between an agent and the manager drops** (a network failure, an agent restart, a manager restart behind a load balancer, the manager's periodic recycling of connections). The agent opens the stream again. A query that follows keeps its response open: the client sees an `interrupted` notice, and when the node's stream is back (within `DOCKFLEET_LOG_RECONNECT_WAIT`, default 30 s) the hub asks the agent again for the lines *after the last one it delivered* and a `resumed` notice follows. Nothing is repeated and nothing that is still in Docker's log is lost. If the node does not return in time, the container's part of the stream ends with an `error` notice. A query that does not follow ends its container's part with an `error` notice at once.
- **The manager restarts.** Its queries are gone: open responses end with an `end` record (`closed`) and the cursor to resume from. The agents register again, open their streams on the new manager, and the client resumes with `after`.
- **The client leaves** (it closes the connection, or its context ends). The handler returns, the query is closed, and the hub tells each agent to cancel its request; the agent closes its Docker reader at once. `OpenLogReaders` of the in-memory engine in the tests is how this is checked.
- **A container stops or is removed** while it is followed: the request ends and the client sees an `ended` notice. The follow does not carry on into the container's next run (a restart by the restart policy): ask again.
- **The agent restarts** while a query follows its containers: as above. The containers keep running and keep writing, and the lines written meanwhile are read from Docker when the agent is back.

## Limits: nobody's memory is exhausted by a slow reader

Every stage is bounded and passes pressure back to the one before it. Beyond the limits below, data stays where it is: in the Docker daemon's own log files.

| Where | Bound | What happens beyond it |
| --- | --- | --- |
| A line | 16 KiB (the request may lower it; 64 KiB is the most any side accepts) | The line is cut and marked `truncated`; the Docker reader keeps at most the limit of a line per output stream, however long the line really is. |
| An agent's batch | 256 lines and 256 KiB | The batch is sent and a new one started. |
| **Credit** (flow control) | Each request starts with a window of bytes (`DOCKFLEET_LOG_WINDOW_KIB`, 256 KiB for a query, shared among its containers, at least 32 KiB each). A line costs its text plus 64 bytes. | The agent sends **only as many bytes as it has credit for** and, out of credit, **stops reading the container's output** (it stays in Docker). The hub returns credit only as the reader takes lines out, a quarter window at a time. An agent that sends more than it was granted has its request ended, and what it sent beyond is not kept. |
| The manager, per query | One window, however slowly the client reads | A client that reads slowly makes the agents wait; it does not make the manager grow. |
| The HTTP response | A 16 KiB buffer, and a **write deadline** (`DOCKFLEET_LOG_WRITE_TIMEOUT`, 30 s) on every write | A client that takes no data for that long is dropped: the response ends, the query is closed, the agents are told to stop. |
| Queries open at once | `DOCKFLEET_LOG_MAX_STREAMS` (32) | `429`. |
| Containers per query | 32 | `400`. |
| Requests an agent serves at once | `DOCKFLEET_LOG_MAX_REQUESTS` (16, `0` turns serving logs off) | The excess is refused with a `busy` end, shown as an `unavailable` notice. |
| The manager's queue to one agent | 256 small messages (requests, cancels, credit) | An agent that does not read its stream is dropped. |

In the worst case the manager holds, per open query, one window per container (at least 32 KiB each, so at most about 1 MiB for 32 containers) and, with 32 queries, about 32 MiB; an agent holds, per request it serves, one batch (at most 256 KiB) and the line reader's buffers (a 32 KiB chunk, a 4 KiB read buffer and one partial line per output stream, so about 70 KiB with the default line limit and at most about 170 KiB with the largest), so at most about 7 MiB with 16 requests. These are bounds, not typical sizes.

## Permissions

- **The HTTP endpoints** need the admin bearer token like every other `/v1` route (`401` otherwise). The agents' token is a different secret and is refused. They only read: any other method is `405`.
- **The agents' stream** is authenticated like every other agent call. Under mutual TLS the client certificate is verified and its node identity must equal the `node_id` in the hello (`PERMISSION_DENIED` / `IDENTITY_MISMATCH` otherwise), so an agent cannot open the stream of another node and receive requests meant for it; without TLS (loopback development) the agent token authenticates. There is no plaintext fallback. The node must be enrolled and the session in the hello must be the node's current one (`NOT_ENROLLED`, `NOT_REGISTERED` or `SESSION_SUPERSEDED` otherwise).
- **The stream is bound to the session.** The manager checks the session and the enrollment again every few seconds; a stream whose session was replaced, ended or revoked is ended with the matching error, and the node's containers become unreadable at once, without waiting for the next heartbeat.
- **Everything an agent sends is checked** before the hub sees it: identifiers, line and batch sizes, stream names, times. A malformed message ends the stream (`INVALID_REQUEST`). The hub accepts lines only for requests it opened *on that agent's stream*; another agent that learns a request ID cannot feed it.
- **The agent reads only its own containers:** a request for a workload that is not an assigned DockFleet container of this node (an unknown ID, a container someone else started, one of another node) is answered `not_found`, and Docker is never asked about it.
- Logs can contain secrets the containers print. Responses are not cacheable (`Cache-Control: no-store`) and are served as `nosniff`; the manager never writes log contents to its own log.

## Protocol

The stream is the `Logs` RPC of `NodeService` (protocol 1.4, [protocol.md](protocol.md)): the agent sends `LogsHello`, then `LogBatch` and `LogEnd`; the manager sends `LogOpen`, `LogCancel` and `LogCredit`. Older managers answer `UNIMPLEMENTED` and an agent stops asking for the session; an older agent never opens the stream, and its containers show as `unavailable`.

## Code

- `internal/logstream`: the hub (the agents' streams, queries, merging, credit, resuming) and its types. It has no I/O and imports only the standard library; the transports plug in through small interfaces.
- `internal/protocol` (`logs.go`): the `Logs` handler and client, the conversion with its validation, the stream authentication.
- `internal/agent` (`logs.go`): serves the requests from Docker: history, follow without gaps or repeats, credit, batching, limits.
- `internal/docker`: `StreamLogs` and the line reader (`logs.go`); `internal/dockerfake` implements the same for tests.
- `internal/api` (`logs.go`): the endpoints, the query parameters, the formats and the write deadline.

## Tests

- `internal/logstream`: merging of the history by time; live lines taking turns; credit returned a quarter window at a time; a slow reader holding the agent to one window; an agent that ignores its window being ended; requests that are not the stream's own being ignored; unreachable nodes, ends and reconnection (resuming after the last line, giving up after the wait, a newer stream taking over); cancellation and context; the limit of queries, validation, the share of the window; cursors; a pin on its imports.
- `internal/docker`: the line reader over frames (partial lines per stream, timestamps, long lines cut without being buffered, a 40 MiB frame, bad UTF-8, errors, `Close` ending a blocked read); `StreamLogs` through the adapter and the SDK (query parameters, only this node's containers). A `dockerintegration` test reads a real container's logs.
- `internal/agent`: history, cursor, follow without gaps or repeats, credit (checked on every send), cancel releasing the reader, containers that are not ours, the request limit, removal, the end of the stream, long lines, tiny windows, Docker failures, batching limits, reconnection and the answers that end retrying.
- `internal/protocol`: message conversion and every refusal of an invalid message; stream authentication; the handler over a real connection (requests and lines, the session check, revocation, a superseded session, bad messages, no hello).
- `internal/api`: the endpoints with a real hub and simulated agents, on the in-memory store and on PostgreSQL: merging, filters, records, formats, control characters, following, reconnection, unreachable nodes, cursors, 400/404/429/503, authentication, and a client that does not read.
- `internal/integration/logs_test.go`: real agents, a real manager over gRPC and the real API handler: several containers on several nodes merged and followed, an agent restart, a manager restart resumed from a cursor, cancellation releasing every reader, authentication and revocation, mutual TLS identity binding.

## Limitations

- Only the output Docker keeps is available: `json-file` logs with rotation (3 × 10 MiB per container). Containers started with a TTY (not DockFleet's) are not supported.
- A following request ends when the container stops. It does not carry on into the next run of a restarted container.
- A long line that Docker itself split into several records (above 16 KiB) is cut at the line limit; if the request allows more than 16 KiB, a stray timestamp can appear inside such a line.
- With several managers, each knows the agents connected to it: a query sees the nodes whose streams ended up on the manager that serves it, and reports the others as `unavailable`.
- A cursor holds a time per container, so lines with the identical nanosecond of one container are treated as one.
- Log contents are not stored on the manager: a container that is gone is gone.
