# Failure handling

DockFleet keeps services running when nodes go quiet and when containers fail. This document says what each failure does, how the manager and the agents decide, and how to see it. The loop that carries instructions out is in [reconciliation.md](reconciliation.md); the placement rules are in [scheduler.md](scheduler.md).

Two kinds of failure are handled differently:

- **A container fails** (it crashes, is killed for memory, or fails its health check). The agent on its node restarts it under the service's restart policy, with a growing wait and a limit. The manager is told and shows it; it does not move a failed replica.
- **A node goes quiet** (the host or the agent is down, or the network is cut). The manager first doubts the node, then gives up on it and starts its replicas on other nodes. A lease and a generation keep the old and the new copy from both being in charge.

## Node states

The manager derives a node's state from how long it has been silent. Silence is measured from the node's last heartbeat; for an agent that left cleanly it is measured from when it left.

| State | Meaning | New replicas | Its replicas |
| ----- | ------- | ------------ | ------------ |
| `healthy` | A heartbeat within `DOCKFLEET_NODE_TTL` (30 s) | yes | stay |
| `suspect` | Silent for longer than that, or the agent has left | no | stay |
| `unavailable` | Silent for longer than `DOCKFLEET_NODE_UNAVAILABLE_AFTER` (2 min) | no | are stopped, and started on other nodes |

A suspect node is one a dropped packet or a restarting agent can explain, so nothing is taken from it. Only the longer silence moves replicas.

The state is computed from the heartbeat every time it is read, so the API is current at once. The reconciler also records it (`nodes.health`, `nodes.health_since`) on every pass, which is what turns a change into one audit event (`node.suspect`, `node.unavailable`, `node.recovered`) and tells the scheduler which nodes were given up on.

**A manager that was down does not count its own absence against the nodes.** Silence from before this manager process started is ignored (`scheduler.Timing.Since`), so a node is given up on no earlier than `DOCKFLEET_NODE_UNAVAILABLE_AFTER` after the manager began to watch. Without this, restarting a manager after an outage would look like every node dying at once. With several managers sharing a database, each applies this to its own start.

## Leases: who may run a replica

A heartbeat renews the node's **lease**. The manager grants it at registration (`lease_ms` in `RegisterNodeResponse`, `DOCKFLEET_NODE_LEASE`, default 60 s) and it applies to every heartbeat the manager answers.

```
   agent:    heartbeat sent ........ lease ends (agent fences itself)
                 |-------------- lease --------------|
   manager:        received |--------------- lease + one heartbeat interval ---------------|
                                                                          node unavailable; replicas move
```

- **The agent counts the lease from when it sent the heartbeat**, not from when the answer arrived. The manager counts from when the request reached it, which is later. So the agent's lease never outlasts the manager's.
- **When the lease runs out** (no heartbeat was answered for that long) and fencing is on, the agent **stops every managed container of the node**, immediately and without a grace period, and does not remove them. This is *fencing*. The runner wakes at the moment the lease would end rather than at the next resync.
- **The manager gives up on a node only after the lease and a heartbeat interval.** The configuration refuses `DOCKFLEET_NODE_UNAVAILABLE_AFTER` below `DOCKFLEET_NODE_LEASE` plus `DOCKFLEET_HEARTBEAT_INTERVAL`, and a lease shorter than two heartbeat intervals (one lost heartbeat must not end a lease). So by the time a replica is started elsewhere, a live agent on the old node has already stopped its copy.
- **When the lease is renewed** the agent starts the same containers again. This is a pause, not a failure: it creates and removes nothing and does not count as a restart.

Fencing is on by default (`DOCKFLEET_FENCE_ON_LEASE_LOSS=true` on the agent). **The price is that a manager outage longer than the lease stops every container in the fleet**, until the manager is back. A manager that restarts within seconds costs nothing. If you would rather keep containers running through a long manager outage and accept a duplicate replica after a failover, turn it off.

What fencing cannot do:

- **A crashed agent process cannot fence.** If only the agent dies, its containers keep running, and a replica started elsewhere runs beside them until the agent comes back (or the host is rebooted). The generation (below) removes the old copy the moment the agent returns.
- **An agent that restarts during a partition** has no lease yet and leaves the containers it finds alone until it hears from the manager.
- Lease times are compared on each side's own clock; only the *rate* of the two clocks matters, not that they agree. A margin of one heartbeat interval covers jitter.

## Generations: the old owner learns it is not

Every workload has a **generation** that counts the instructions given for it, and the agent acknowledges the highest one it has seen with each report. When the manager gives up on a node it does not edit the workload and does not move it:

1. The workload on the lost node is told to **stop**: generation 2, desired state `stopped`. Its record stays.
2. A **new workload with a new ID** is placed for the replica on another node. A stopped workload no longer owns its replica slot once its node is lost (migration 010: the database allows one workload *meant to run* per replica), so this does not wait for the lost node.
3. When the old node comes back, its first answer from the manager lists the old workload as `stopped` at generation 2. The agent removes the container, reports `stopped` with no container, and the manager deletes the record.

A workload ID names one container's lifetime on one node, so the two copies can never be confused, and the stale one is always told to go. A report from the old node about the old workload cannot reopen it: acknowledgments are clamped to the workload's own generation and never go back, and a workload is never started again after it was stopped.

If the node never returns, its stopped workloads stay on record (visible in `GET /v1/workloads` and in the service's `status`), which is the proof that the replicas were moved.

## What moves, and what does not

- **Replicas of services** on an unavailable node move, one new workload per replica, placed by the ordinary scheduler (spread, resources, selectors, ports). If nothing fits, the replica is unschedulable with the usual explanation, and is placed as soon as something does (a node returns, a node is added, resources free up).
- **Standalone workloads** (`POST /v1/workloads`) are pinned to the node an administrator chose and are not moved. They stay assigned, and the node view shows why they are not running.
- **Replicas of an older revision** on a lost node are stopped as well; they have already been replaced or will be by the rollout.
- **A failed container does not move.** A replica whose container failed and ran out of restarts stays failed on its node (see below). Deploying a new revision replaces it, and so does lowering the replica count by one and raising it again: a scale-down removes the failed replica before any healthy one ([reconciliation.md](reconciliation.md#scaling)), and the scale-up creates a fresh workload.
- **Nothing moves back.** A returning node takes new replicas like any healthy node; replicas that were moved stay where they are.
- **A replica is moved once.** The reconciler decides in one transaction under the placement lock, so two managers cannot both move the same replica, and a repeated pass changes nothing.

## Container failures

The agent restarts a container whose restart policy says so. The policy is part of the service (`spec.restart_policy`, [api.md](api.md)):

| Field | Meaning | Default |
| ----- | ------- | ------- |
| `condition` | `no`: never. `on-failure`: when it exited non-zero, was killed for memory, or turned unhealthy. `always`: also after a clean exit | `on-failure` |
| `max_retries` | Restarts in a row allowed (1 to 100). For `always`, 0 means no limit | 5 for `on-failure` |
| `backoff_seconds` | Wait before the second restart in a row; it doubles with each further one | 1 |
| `max_backoff_seconds` | The longest wait | 60 |

- **The first restart is immediate.** The wait before the *k*-th restart in a row (for *k* of 2 or more) is `backoff_seconds` × 2^(*k*-2), up to `max_backoff_seconds`: 1 s, 2 s, 4 s, 8 s … 60 s with the defaults.
- **The limit counts restarts in a row.** A container that has run steadily for 30 seconds is no longer in a crash loop: the count and the wait start over. An occasional failure never uses up the limit; a crash loop does.
- **At the limit the agent stops restarting.** The workload is reported `failed` with the reason (`exited with code 1; gave up after 5 restarts in a row`), the container is left in place for inspection, and the manager records `workload.failed`. It stays that way until the service is changed.
- **A restart reuses the container.** Create and start happen once; a restart only starts it again, so volumes and the container's identity are kept. The restart count is reported with every status.
- **The agent's memory is the count.** An agent that restarts begins its counts again; the containers themselves are not lost.

### Health checks

A service's `spec.health_check` is given to Docker, which runs the command inside the container at its interval and keeps the result. The agent reads it with every pass and reports `health` (`starting`, `healthy`, `unhealthy`, or empty without a check).

- A **running container that turns unhealthy** is treated like a crash: the agent stops it, and the restart policy decides what happens next (backoff and limit included). The exit is counted as a failure whatever the exit code. With a policy of `no` the container is left running and reported `unhealthy`.
- At the limit it stays stopped and is reported `failed` with `health check failed` in the message.
- Docker's `start_period` and `retries` decide when it becomes unhealthy, so a slow start is not a failure.

## Reconnection and reconciliation

Every message carries the complete state, so there is nothing to replay:

| What happens | What the agent and manager do |
| ------------ | ----------------------------- |
| **The agent loses the manager** (network, manager down) | Heartbeats fail and are retried every interval. Containers keep running until the lease ends; then they are fenced. The agent never gives up on a transient failure. |
| **The manager restarts** | The session is gone. The agent's next heartbeat gets `NOT_REGISTERED`; it registers again and receives its complete assignment set with a new lease. Fenced containers start again; nothing is created. The new manager does not count the time it was down against any node. |
| **A partition heals before the lease ends** | The next heartbeat is accepted and renews the lease. Nothing stopped, nothing moved. |
| **A partition heals after the lease ended but before the node was given up on** | The agent starts its containers again; the replicas never moved. |
| **A partition heals after the node was given up on** | The agent learns which of its workloads were stopped, removes those containers and keeps the rest. The records are deleted once it confirms. |
| **The agent restarts** | It registers, lists its containers and adopts those that match its assignments; containers of workloads it no longer has are removed after the orphan grace period. |
| **The node reboots** | The containers are gone with it. The agent finds assigned workloads without containers and creates them, as for a new assignment, unless the manager has moved them. |

## What is visible

Failures are in state, never only in logs.

- `GET /v1/nodes`, `GET /v1/nodes/{id}`: `health` (`healthy`, `suspect`, `unavailable`), `health_reason`, `health_since` (when the reconciler recorded it; absent until it has), `lease_expires_at`, and the existing `status`/`status_reason` for scheduling.
- `GET /v1/services/{name}`: `status.failed_replicas`, `unhealthy_replicas` and `lost_replicas` beside `running_replicas`, and for every replica `health`, `restart_count`, `exit_code`, `message` and `node_health`. A replica on an unavailable node counts as lost even if it last reported running.
- `GET /v1/workloads`: each workload's observation with `state`, `message`, `exit_code`, `oom_killed`, `restart_count`, `health` and `acknowledged_generation`; the stopped workload of a moved replica is listed with its node and its generation.
- `GET /v1/events`: `node.suspect`, `node.unavailable`, `node.recovered` (actor `reconciler`, resource: the node); `replica.stopped` with `reason: node_unavailable` and `replica.scheduled` for the replacement; `replica.removed` when the old node confirmed; `workload.failed`, `workload.unhealthy`, `workload.restarted` and `workload.recovered` (actor: the node, resource: the workload), each once per change.
- Agent logs: `the manager's lease on this node ran out; containers stopped so that no replica runs twice`, `workload status`, `heartbeat failed`.

## Settings

| Variable | Used by | Default | Meaning |
| -------- | ------- | ------- | ------- |
| `DOCKFLEET_NODE_TTL` | manager | `30s` | Silence after which a node is suspect |
| `DOCKFLEET_NODE_LEASE` | manager | `60s` | Lease granted with each heartbeat; at least twice the heartbeat interval |
| `DOCKFLEET_NODE_UNAVAILABLE_AFTER` | manager | `2m` | Silence after which a node is given up on; at least the lease plus one heartbeat interval, and longer than the node TTL |
| `DOCKFLEET_RECONCILE_INTERVAL` | manager | `5s` | How often failures are acted on |
| `DOCKFLEET_FENCE_ON_LEASE_LOSS` | agent | `true` | Stop containers when the lease runs out |

Restart policy and health check are per service.

## Tests

- `internal/scheduler`: node health as a table over silence, departures, the manager's start and the zero value.
- `internal/reconcile`: `Plan` for lost nodes (reasons, standalone workloads, rollouts), `PlanNodes`, and the whole control loop with simulated agents: failover and recovery, a manager restart, a suspect node that comes back, a departed agent, the only node lost, standalone workloads.
- `internal/placement`: a lost node frees its slots; a quiet one does not.
- `internal/agent`: restart backoff and limits from the policy, forgetting restarts after steady running, health restarts, fencing and its end, lease timing, and the whole agent with failing heartbeats.
- `internal/registry`, `internal/protocol`, `internal/store`, `internal/api`, `internal/docker`: failure events, the lease and health on the wire, node health in both stores, node and service views, health check policy and Docker config.
- `internal/integration/failure_test.go`: real agents and a real manager over gRPC with in-memory Docker engines. A crashed agent and the cleanup when it returns; a network partition in which the agent fences itself *before* the replacement is created (checked in one journal across both nodes); a brief partition that changes nothing; a long manager outage and the restart of the same containers; a container restarted up to its limit and then reported failed; an unhealthy container restarted.

## When the manager restarts

Silence from before a manager started is not counted, so a manager that was down or restarted does not mistake a node it could not hear for a dead one. After the restart the agents have to find it again: they retry registration with a wait that doubles from 1 s up to **30 s** (no jitter), so an agent can take up to half a minute to come back. `DOCKFLEET_NODE_UNAVAILABLE_AFTER` must therefore stay well above 30 s; the default of 2 minutes does. (Shorter values are only for tests.) A manager killed for longer than the give-up time and restarted is tested end to end with real agents: nothing is created, stopped or moved.

## When the manager's own store is down

A node is judged by how long it has been silent, and silence is the time since its last heartbeat was *recorded*. While the database is unreachable no heartbeat can be recorded, however many the agents send, so every node looks silent for the length of the outage. Without a remedy, the first pass after the database returned would have seen every node silent for longer than `DOCKFLEET_NODE_UNAVAILABLE_AFTER`, given up on all of them, and stopped and replaced every replica of the fleet.

So the manager remembers when its store last failed (`internal/outage`: every failed scheduling or reconciliation pass, and every failed readiness check, marks the time) and, exactly as it does for the time before it started, **does not count silence from before that moment** (`scheduler.Timing.Outages`; the API, the scheduling pass and the reconciler share one record, so they agree). Agents have until the limit again, counted from the end of the outage, to be heard. A node that really died during the outage is found out after that. Tested at four levels: a table for the timing, a reconciliation test with a control run that shows the mass failover without the record, and an end-to-end test that crashes PostgreSQL under the running binaries for longer than the limit (it fails without the fix and passes with it).

The manager's `/healthz` does not depend on the database, so a supervisor does not restart a manager that is only waiting for it; `/readyz` does, and says `store unavailable`.

## Limitations

- A node that is gone for good keeps its stopped workloads on record; there is no way to delete the node yet.
- A crashed agent with live containers cannot fence them (above); a duplicate runs until it returns.
- A failed replica is not rescheduled to another node. A node that makes every container fail is not detected as bad.
- Fencing stops all containers of a node, including ones whose workloads the manager would not have moved. Per-service fencing is not configurable.
- Lease and silence limits are the same for every node.
- Restart counts and backoff are kept in the agent's memory.
- Standalone workloads are not moved.
- The failure of a **manager** is the subject of [high-availability.md](high-availability.md): with several managers and etcd, another one takes over, node health is counted from the start of its term so that the failover is not mistaken for a failure of the nodes, and agents that cannot find a leader before their lease runs out fence themselves exactly as they do for any other manager outage. The lease must therefore be at least `2 × DOCKFLEET_ELECTION_TTL + DOCKFLEET_HEARTBEAT_INTERVAL`, which the manager checks at startup when high availability is on.
