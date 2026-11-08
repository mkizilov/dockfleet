# Rolling deployments and rollback

Changing a service's `spec` (its image, command, environment, limits, ports, health check, restart policy or node selector) deploys a new **revision** ([api.md](api.md)). The running replicas are then replaced by replicas of the new revision **a few at a time**, within a policy the service carries, and the API reports how far the deployment has come, whether it has stalled, and why. A rollout can be paused and resumed, and a service can be rolled back to an earlier spec.

Like everything the services API does, these requests only record intent. The scheduler ([scheduler.md](scheduler.md)) creates the replicas of the new spec, the reconciler ([reconciliation.md](reconciliation.md)) retires old ones and keeps the record of the rollout, and the agents ([docker.md](docker.md)) run the containers.

## The policy

```json
"rollout": {"max_unavailable": 0, "max_surge": 1, "progress_deadline_seconds": 600}
```

| Field | Default | Meaning |
| --- | --- | --- |
| `max_surge` | 1 | How many replicas may run **above** the wanted count while a rollout is going on. |
| `max_unavailable` | 0 | How many replicas **below** the wanted count may be not ready. |
| `progress_deadline_seconds` | 600 | How long a rollout may go without progress before it is reported failed. 1 to 86400. |

Both limits are counts of replicas (0 to 1000), not percentages. They cannot both be 0: a rollout could never replace a replica, so the API refuses it and the database does too. The policy belongs to the service, not to a revision: set it with `POST` or `PUT` (`"rollout"` in the body; a field left out keeps its value), and changing it starts no deployment.

The two limits are two ways to find room for the new replicas, and they combine:

| Policy | What a rollout of 4 replicas does |
| --- | --- |
| `max_surge 1`, `max_unavailable 0` (the default) | Creates one replica of the new spec, waits until it is ready, retires one old replica, and so on. 5 replicas at most, 4 ready at least. Needs room for one more replica. |
| `max_surge 0`, `max_unavailable 1` | Retires one old replica first and creates its successor in the room it leaves. 4 at most, 3 ready at least. Needs no spare capacity, and gives up a replica's worth of availability. |
| `max_surge 2`, `max_unavailable 2` | Creates two at once and retires two at once. Fast, with the most room used and the most availability given up. |
| `max_surge 4`, `max_unavailable 0` | Creates all four new replicas, then retires the four old ones: a blue-green switch. 8 at most. |

## How a rollout goes

Each pass of the scheduler and of the reconciler works out the same numbers from the same snapshot (`internal/rollout`, a pure package):

- **Create.** Replicas of the new spec are created only while the replicas that are meant to run, old and new together, number at most the wanted count plus `max_surge`, and only as many as are missing. A service with no old replicas is not rolling out and gets all it lacks at once (the first deployment, a scale-up).
- **Retire.** A ready old replica is retired only while the replicas that can be counted on stay at least the wanted count minus `max_unavailable`. Old replicas that provide nothing (not ready, or on a node that has gone quiet) are retired at once. Old replicas beyond the wanted count, or beyond the count plus the surge, are retired whatever their state: that is what a scale-down in the middle of a rollout leaves.
- **Ready.** A replica is ready when it is meant to run, its agent has acknowledged the latest instruction and reports the container running (or finished successfully), **and, if its spec has a health check, the check passes.** A replica whose check is `starting` or `unhealthy` is not ready, and the rollout does not retire anything for it. A replica on a node that is `suspect` or `unavailable` is not counted. Without a health check "ready" is only "running", so a container that starts and then crashes is ready until it does; use a health check on anything whose start is not the same as being up.
- **Which old replicas go** is chosen as for a scale-down ([reconciliation.md](reconciliation.md#scaling)): the ones doing the least good first, then the node that carries most, then the one restarted most, then the newest.

The numbers, and the policy, hold at every moment the manager decides something. A replica that later fails, or a node that goes away, is the failure handling's business ([failure-handling.md](failure-handling.md)); it can take the ready count below the limit, and the rollout then waits (it retires nothing more) until the count is back.

### Capacity is handled explicitly

A rollout never goes beyond either limit. When it cannot move without doing so, it **waits, and says so**:

- If the node room for the extra replica is missing (CPU, memory, a fixed host port, a node selector), the scheduler records why, as for any replica ([scheduler.md](scheduler.md)), and the rollout's `message` carries that reason. With `max_unavailable` 0 it adds what can be done: *"1 replica cannot be placed: 0/1 nodes can run web#5: 1 node(s) lack CPU (needs 500m) (the old replicas stay, since max_unavailable is 0 and max_surge is 1: free capacity, add nodes, or allow max_unavailable of 1 or more)"*.
- The old replicas are **not** retired to make room, however long that takes. Raising `max_unavailable` is the administrator's decision, and it takes effect on the next pass.
- A replica that publishes a fixed host port allows one replica per node, so a new replica cannot start beside the old one on the same node. On a single node that is the case above; with more nodes the new replica goes to a node that has no old one. (Earlier versions retired the old replica to free the port whatever the policy; that is now what `max_unavailable` is for.)
- If nothing progresses for `progress_deadline_seconds`, the rollout is reported **failed** (below), with the same reason.

## Progress and failure

`GET /v1/services/{name}/rollout` and `status.rollout` of every service view report the deployment of the current revision:

```json
"rollout": {
  "revision": 3, "state": "progressing", "paused": false,
  "message": "1 of 3 replicas of revision 3 are ready; 2 replicas of an older spec still run; 1 replica is not ready yet: web-r3-2-8f3a1c",
  "desired": 3, "updated": 2, "ready": 1, "outdated": 2, "surge": 1, "unavailable": 0,
  "started_at": "...", "progress_at": "...", "deadline_at": "..."
}
```

| State | Meaning |
| --- | --- |
| `progressing` | Replicas of the revision are being created, started and made ready, and old ones retired; it has moved within the deadline. `deadline_at` is when it will be reported failed if it does not move again. |
| `paused` | `progressing`, held by a pause. No deadline. |
| `complete` | Every wanted replica of the revision is ready and no replica of another spec is left. Final: later scaling or a replica that fails does not reopen it. |
| `failed` | It made no progress for longer than its deadline. `reason` is `progress_deadline_exceeded` and `message` the cause, as the rollout saw it then: the replicas that failed (with the container's own words), the ones failing their health check, the ones not ready yet, the ones that cannot be placed and why. |
| `deleted` | The service was deleted. |

`history` of the rollout view lists every revision's rollout with how it ended; `superseded` is a rollout that another revision overtook before it completed.

**Progress** is: more replicas of the new spec than ever before, more of them ready than ever before, or fewer old ones left than ever before. A replica that comes and goes does not count each time. **A failed rollout is still being carried out**: if it moves again (a node joins, the health check passes, `max_unavailable` is raised) it returns to `progressing` and the `rollout.recovered` event says so. Nothing is rolled back automatically: that is an administrator's decision, and `POST .../rollback` makes it a single call.

The manager keeps the record of each rollout (`rollouts`, [state.md](state.md)): when it started, when it last made progress, the best counts it reached. It is derived from the workloads and the agents' reports, written only by the reconciler in the same transaction as its other work, and the one thing decided from it is whether a rollout has stalled. A manager restart or a second manager changes nothing: the deadline is counted from the stored `progress_at`.

## Pausing and resuming

`POST /v1/services/{name}/pause` holds rollouts; `.../resume` releases them. While paused:

- no replica is added above the wanted count (the surge is treated as 0), and no old replica is retired for the rollout (the unavailable budget is not spent);
- the progress deadline does not run, and resuming starts it afresh from the resume;
- a revision deployed while paused is prepared and starts nothing; a service that has no replicas yet still gets its first ones;
- a scale-down, the failover of replicas from a lost node and the retirement of old replicas beyond the count still happen. A replica that must be replaced is replaced by a replica of the **new** spec: there is only one current spec, and a pause cannot create replicas of an old one.

Pausing is a change of the service (it bumps the version and records `service.paused` / `service.resumed`). Both calls are idempotent: asking for the state the service is in changes nothing, records nothing and succeeds, even with an out-of-date `If-Match`.

## Rolling back

`POST /v1/services/{name}/rollback` deploys an earlier revision's spec again **as a new revision** (history stays linear; every revision is a faithful record of what was once wanted). Without a body the target is the latest earlier revision whose spec differs from the current one; `{"revision": N}` names it. The new revision's `message` says `rollback to revision N`, and `service.rolled_back` is recorded.

**The replicas that still run the target spec are kept.** A replica counts as running the current spec if it was created for the current revision *or for any revision with the same spec*. Rolling back from a rollout that has gone wrong therefore leaves the old replicas it had not reached exactly as they are (same containers, no restart, no new generation) and retires only what ran the spec being left, the failed or unready ones at once and the rest within the policy. When the replicas of the target are gone, a rollback is a rollout like any other: gradual, within the policy.

A rollback clears a pause, since it is the way out of a bad rollout. Naming the revision makes the request idempotent: a service that already runs that spec answers `"changed": false` and records nothing. "The previous revision" is relative to where the service is now, so repeating that request would go back to the revision it came from; send `If-Match` with the version you read to make it safe to repeat.

## Audit events

| Event | By | When |
| --- | --- | --- |
| `service.deployed` | admin | a new spec was deployed (from and to revision, image) |
| `service.rollout_configured` | admin | the policy changed |
| `service.paused`, `service.resumed` | admin | rollouts were held or released |
| `service.rolled_back` | admin | a rollback (from revision, to the new revision, the target revision) |
| `replica.scheduled`, `replica.stopped`, `replica.removed` | scheduler, reconciler | each replica created, retired and removed; `reason` `superseded` (retired by the rollout) or `scaled_down`, and `why` for the choice |
| `rollout.completed`, `rollout.failed`, `rollout.recovered`, `rollout.superseded` | reconciler | the rollout of a revision changed state; each is recorded once |

## Tests

- `internal/rollout`: the budgets as a table over census, policy and pause; a walk through a rollout under every policy for 1 to 6 replicas checking the surge and the unavailable limit at every step; readiness with and without health checks; the descriptions; the record's state machine (what counts as progress, the deadline, the pause stopping the clock, failure, recovery, completion); and that the package stays pure.
- `internal/reconcile`: `Plan` over replica counts and policies; the control loop with simulated agents for every policy (the invariants checked after every round), replacements that fail, a rollout without room, pausing, health checks gating readiness, rolling back (with the old replicas kept, and after a finished rollout), a revision overtaking a rollout, and a fixed host port needing `max_unavailable`. On PostgreSQL too, under the `integration` tag.
- `internal/placement`: replicas created only as far as the surge allows, none while paused, replicas of a rollback's spec not created again.
- `internal/store`: the policy and the pause (validation, optimistic versions, persistence), which revisions share a spec, the rollout records, the schema's constraints.
- `internal/api`: the policy in the definition and its validation, pause and resume, rollback, and the progress of a rollout as the API reports it.
- `internal/integration/rollout_test.go`: real agents and a real manager over gRPC, driven through the REST API, with a sampler that reads the store while a rollout runs: a successful rollout under three policies (surge and availability limits held at every sample, every container created once and retired once), replacements that crash on start (reported failed, old replicas untouched, then rolled back without restarting them), a rollout without room (waits, says why, goes on once `max_unavailable` allows), pause and resume (nothing moves while paused, the deadline does not run), and a rollback after a finished rollout.

## Limitations

- No automatic rollback: a failed rollout is reported and carried on, and the administrator decides.
- Limits are counts of replicas, not percentages; there is no `min_ready_seconds`. Readiness without a health check is "running".
- The budget counts the replicas that are *meant to run*. A retired replica's container takes a moment to go, and the scheduler treats its resources as freed at once, so a node can briefly carry more than the policy says.
- One current spec: replicas replaced while paused, and replicas added by a scale-up during a rollout, run the new spec.
- Rollout state is per service; there is no ordering between services.
