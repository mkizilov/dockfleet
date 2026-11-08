# Scheduler

The scheduler decides which node runs each replica of a service. It decides and explains; it does not run anything.

| Package | Job | Touches |
| ------- | --- | ------- |
| `internal/scheduler` | Decide where replicas go, or why they cannot go anywhere | Nothing: a function of a snapshot |
| `internal/placement` | Read the cluster, ask the scheduler, save the answer | The store |
| `internal/agent`, `internal/docker` | Run the containers (not yet implemented for workloads) | The node's Docker Engine |

`TestSchedulerStaysPure` pins the scheduler's imports to a short list (standard library and the store's data types) and fails if it reads the clock. `TestManagerSideNeverImportsDocker` keeps Docker out of everything the manager is built from, `internal/placement` included.

## What a replica needs from a node

A replica may go to a node when all four hold, checked in this order:

1. **Available.** The node sent a heartbeat within `DOCKFLEET_NODE_TTL`, has not deregistered, and its enrollment has not been revoked.
2. **Selector.** Every label in the service's `node_selector` is set on the node to the same value.
3. **Host ports.** No workload already assigned to the node, and no replica placed earlier in the same pass, publishes the same fixed host port with the same protocol. A port with `host_port` 0 is chosen by the node and never conflicts; an empty protocol means `tcp`.
4. **Room.** The replica's `cpu_millis` and `memory_bytes` fit in what is left of the node: its capacity (times the overcommit factor, see below) minus the limits of every workload assigned to it whose desired state is `running`. Observed state does not matter: a container that crashed is still meant to run and keeps its reservation; a workload whose desired state is `stopped` reserves nothing.

Limits are what is reserved, not measured usage. The sum of the limits on a node never exceeds what it may be promised, which is what "no oversubscription" means here.

## Overcommit

Off by default. `DOCKFLEET_SCHEDULER_CPU_OVERCOMMIT` and `DOCKFLEET_SCHEDULER_MEMORY_OVERCOMMIT` are factors from 1 to 8 (`1` is the default). With a CPU factor of 2 a 4000m node may be promised 8000m of CPU limits; memory is unaffected unless its own factor is raised. A value below 1, a non-number, `NaN` or `Inf` is a configuration error rather than something quietly corrected, and the scheduler itself treats anything below 1 as 1.

## Which node, among those that qualify

For each replica, in this order:

1. The node with the fewest replicas of the same service (so replicas spread out before any node gets a second one, but spreading never beats feasibility: a node that does not qualify is never chosen).
2. The node that would stay emptiest: free CPU share plus free memory share after placing, compared in whole thousandths.
3. The node with the most free CPU, then the most free memory, after placing.
4. The lowest node ID.

## Determinism

The same snapshot always gives the same decisions, whatever order its parts arrive in. Nodes are taken in ID order, existing assignments in (node, name) order, and replicas in (service name, service ID, index) order; every tie ends at the node ID. Replicas are placed one after another and each sees the resources and ports taken by the ones before it, so one pass cannot promise a node more than it has. Consequently a service earlier in the alphabet is placed first when capacity is short; there are no priorities yet.

## Why a replica cannot be placed

Each unplaced replica gets a code, a message and one reason per node (in node ID order).

| `code` | Meaning |
| ------ | ------- |
| `no_nodes` | No node is registered |
| `no_available_nodes` | Every node is unavailable (silent, deregistered or revoked) |
| `selector_unsatisfied` | Of the available nodes, none matches the node selector |
| `host_port_conflict` | Of the nodes that match, all already use a requested host port |
| `insufficient_resources` | The nodes left are full right now; a node with more room would fit it |
| `exceeds_capacity` | The replica is larger than any remaining node could give even when idle; only a bigger node (or overcommit) helps |

The code names the stage at which the last candidate was ruled out. Per-node codes are `unavailable`, `selector_mismatch`, `host_port_in_use` and `insufficient_resources`, each with text such as:

```
0/4 nodes can run z-proxy#1: 1 node(s) not available; 1 node(s) do not match the node selector {zone=y}; 1 node(s) already use host port 15432/tcp; 1 node(s) lack CPU (needs 1000m)

a-stale       unavailable             no heartbeat within the last 30s
b-zone-x      selector_mismatch       does not match the node selector: label zone is "x" (want "y")
c-port-taken  host_port_in_use        host port 15432/tcp is already used by a-holder-r1-1-3f9a2c
d-full        insufficient_resources  insufficient resources (CPU: needs 1000m, 500m free)
```

The text never contains a node's age or a timestamp, so an explanation that has not really changed is byte-for-byte the same from one pass to the next.

## Placement passes

A replica slot is replica `i` (1 to `replicas`) of a service's current revision. A slot with no workload is waiting; a slot with a workload in any state is not. `placement.Scheduler.RunOnce` makes one pass:

1. Takes the cluster-wide placement lock (`Tx.LockPlacement`; a PostgreSQL advisory lock), so passes on several managers queue up and each sees the previous one's work.
2. Reads nodes, enrollments, workloads and live service definitions in the same transaction.
3. Asks `scheduler.Schedule` for every waiting slot at once.
4. Creates a workload for each placed replica (desired state `running`, the node, the revision's image, limits and ports, name `<service>-r<revision>-<index>-<6 hex>`) and appends a `replica.scheduled` event.
5. Replaces the stored explanations with the ones for the replicas still waiting (`unschedulable_replicas`). An explanation that has not changed keeps its `since` time; a `replica.unschedulable` event is appended only when one is new or changed, not on every pass.

If any step fails nothing is saved and the next pass starts over. A pass is idempotent: with nothing waiting it writes nothing. The manager runs one every `DOCKFLEET_SCHEDULER_INTERVAL` (default `5s`) until it shuts down.

A slot with a stopped workload on it is not waiting while the container may still be there, with one exception: when the workload's node has been given up on (silent for `DOCKFLEET_NODE_UNAVAILABLE_AFTER`, see [failure-handling.md](failure-handling.md)), the reconciler has stopped the workload and the node cannot be asked to remove the container, so the slot is waiting again and the replica is placed on another node at once. The database allows only one workload *meant to run* per slot, so the old and new workloads never both claim it.

Deliberately not done by a pass: removing workloads (scaling down, retiring the replicas of an older revision, stopping the workloads of a lost node), and starting containers. A deployment of a new revision therefore places the new revision's replicas beside the old ones, and needs room for both until the reconciler ([reconciliation.md](reconciliation.md)) retires the old ones, one replica at a time, as the new ones come up. The scheduler treats a workload as freed the moment it is told to stop, even though its container takes a moment to go.

Standalone workloads (`POST /v1/workloads`) use the same scheduler and the same rules, so what a service's replicas reserve counts against them and the other way round.

## Where decisions show up

- `GET /v1/services` and `GET /v1/services/{name}`: `status` with the desired, scheduled and unschedulable replica counts, each replica's node and phase, and every waiting replica's code, message and per-node reasons.
- `GET /v1/workloads[/{id}]`: the assignments themselves (`node_id`, `desired_state`, `service_id`, `service_revision`, `replica_index`, `ports`), beside the agent's observation.
- `GET /v1/nodes[/{id}]`: `status_reason`, `allocatable` and `allocated` resources.
- `POST /v1/workloads`: `409` with `code` and per-node `nodes` when nothing fits.
- `GET /v1/events`: `replica.scheduled` and `replica.unschedulable`, actor `scheduler`.
- Agents: an assigned workload reaches its node in the heartbeat response like any other.

See [api.md](api.md) for the shapes.

## Tests

- `internal/scheduler`: table-driven tests for one replica (placement, availability, resource exhaustion down to one byte, overcommit, selectors, host ports, spreading), for several replicas in one pass, for every message, and for determinism (a hundred shuffles of nodes, assignments and replicas give identical decisions). None needs a database or Docker.
- `internal/placement`: one suite against the in-memory store always and PostgreSQL under the `integration` tag: idempotence, scaling, explanations that are recorded once, unavailable and revoked nodes, host ports, existing reservations, revisions, rollback on failure, the loop. PostgreSQL adds two managers passing at once.
- `internal/store`: conformance for workload ports and the explanation records.
- `internal/api`: the service status, node view and workload refusal through HTTP.
- `internal/integration`: a scheduled replica reaches its agent, which has no Docker.

## Limitations

- Reservations are limits, not usage; there are no priorities, preemption, taints, affinities, or spreading across zones.
- Only host ports are checked for conflicts. Whether the node can bind an address is the agent's concern.
- A node's capacity is what its agent declared at registration.
- Nothing moves a workload after it is placed.
