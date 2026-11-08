# High availability: several managers, one leader

DockFleet runs as a single manager by default, and that stays supported: no etcd, nothing to configure, nothing in this document applies. High availability is optional. With it, **several managers share one PostgreSQL database and elect one leader through a lease in etcd**. Only the leader schedules, reconciles and answers agents. When it fails, another manager takes over from the database and the agents find it.

| | Single manager (default) | High availability |
| --- | --- | --- |
| Needs | PostgreSQL (or the in-memory store, for development) | PostgreSQL **and** etcd (3.4 or later) |
| Turned on by | nothing | `DOCKFLEET_ETCD_ENDPOINTS` |
| State | PostgreSQL | PostgreSQL (etcd holds only the leadership lease, never state) |
| A manager that fails | the control plane is down until it is back | another manager leads within about `4/3 × DOCKFLEET_ELECTION_TTL` |

## How it works

Three mechanisms, each with its own job:

1. **Election (etcd).** Each manager keeps a lease alive in etcd and tries to create one key, `<prefix>/leader` (`/dockfleet/leader` by default), attached to it. Creating the key is a single atomic transaction ("if the key does not exist"), so exactly one manager wins. The key disappears when the lease expires or is revoked. This decides *who should lead*.
2. **Fencing epoch (PostgreSQL).** Winning the key is not enough to act. The winner starts a *term* in the database: it increments the epoch in the one-row table `control_plane` and records `leader.elected`, in one transaction. The epoch only grows, by one per election, and is handed out by the database, so etcd cannot repeat or lose one. A leader acts *under* its epoch. This decides *who may write*, and it is what makes a leader that has been replaced harmless.
3. **Epochs on the wire (agents).** Every answer that carries instructions (`RegisterNode`, `Heartbeat`) carries the epoch of the leader's term. An agent keeps the highest epoch it has heard and refuses answers with a lower one. This decides *whom an agent obeys*.

```
              etcd                         PostgreSQL                          agents
   manager-1 ──lease/key──▶ leader      control_plane: epoch 7 (manager-1)
   manager-2 ──lease──────▶ follower          ▲  CheckEpoch(7) in every pass          ◀── epoch 7, instructions
   manager-3 ──lease──────▶ follower          │  (FOR SHARE: no new term meanwhile)     ◀── NOT_LEADER from followers
        manager-1 fails: lease runs out ─▶ manager-2 wins the key ─▶ BeginTerm → epoch 8 ─▶ agents register with it
```

### What the leader does and what followers do

| | Leader | Follower |
| --- | --- | --- |
| Scheduling and reconciliation loops | run | do not run |
| Agent calls (`RegisterNode`, `Heartbeat`, `DeregisterNode`, `Logs`) | answered, stamped with the epoch | refused with `NOT_LEADER` (the agent tries the next manager) |
| `POST /v1/workloads` (places a workload by request: scheduling) | works | `503` with `{"code": "not_leader", "leader": "<name>"}` and `Retry-After: 1` |
| Other API requests (services, scaling, rollouts, enrollments, events, nodes) | work | work: they only record desired state in the shared database, protected by versions as before |
| `GET /v1/cluster`, `GET /metrics`, `/dashboard` | work | work, and say who leads |
| Container logs | work | agents are not connected to a follower, so every node is reported unavailable |

### The term, step by step

1. A follower creates the key. It then calls `BeginTerm`: the database increments the epoch and records `leader.elected` (details: `epoch`, `previous_leader`, `previous_epoch`, `etcd_revision`). If that fails (the database is down) the manager gives the key up: it cannot lead without an epoch.
2. It renews the lease once more. This orders the term after every earlier one: a manager that held the key before cannot begin a term after this point.
3. It starts the duties: new scheduling and reconciliation loops, built for this term. Node health is counted from the start of the term (below).
4. It renews the lease every `DOCKFLEET_ELECTION_TTL / 3`. It trusts the lease for **80 % of what etcd says is left, counted from before the renewal was sent**. Past that point the manager answers `NOT_LEADER` and its passes refuse to start, **even if its renewal goroutine is stuck**, because `Epoch()` checks the clock itself.
5. The term ends when the lease is reported gone (`lease expired`), when it could not be renewed in time (`lease could not be renewed in time`), when a pass finds a newer epoch in the database (`fenced`), or on shutdown (`shutdown`). (The elector can also resign; the tests use it, and no API exposes it.) The duties are stopped **and have returned** before the manager is a follower again, so two terms of one manager never overlap. It records `leader.lost` with the reason. A manager that is fenced (or resigns) stays out of the next election for one TTL so that another one wins it.

On shutdown the election ends *first*: the leader gives up its lease, so another manager takes over at once and agents are not left with a leader that is about to go.

### Fencing: where the epoch is checked

| Where | How | What it prevents |
| --- | --- | --- |
| Every scheduling pass and every reconciliation pass | Inside the pass's transaction, after taking the placement lock, `CheckEpoch(epoch)`: a `SELECT … FOR SHARE` of the `control_plane` row; `BeginTerm` is an `UPDATE` of the same row | A leader that has been replaced saves nothing. And because `FOR SHARE` conflicts with the `UPDATE`, **a new term cannot begin while such a transaction is open**: what the old leader writes under its epoch commits *before* the new term, never after it. |
| `POST /v1/workloads` | The same check in the same transaction | A follower, or a replaced leader, placing a workload |
| Answers to agents | The epoch is read before the answer is prepared and again before it is sent; if the term changed in between, `NOT_LEADER` is sent instead | An answer prepared in one term and delivered in the next |
| The agent | `admit`: an answer with an epoch lower than one already heard is refused (the agent registers again); a higher one is taken over, whoever it comes from | A manager that was replaced and does not know it, answering an agent that has already heard the new one |

A pass that is refused because the manager does not lead (or no longer leads) is **not** counted as a scheduling or reconciliation error in the metrics, and nothing of it is saved.

### Node health after a failover

A node is suspect, then unavailable, by how long it has been silent. After a failover, agents have had no leader for a while and need time to find the new one, so silence from before the new term began is **not counted**: node health is measured from the start of the term (`control_plane.elected_at`, in the database, so every manager and the API agree). An agent therefore has the full `DOCKFLEET_NODE_UNAVAILABLE_AFTER` to find the new leader before its replicas are moved.

## Configuration

Managers (`docs/state.md` for the schema; all of these are validated at startup):

| Variable | Default | Meaning |
| --- | --- | --- |
| `DOCKFLEET_ETCD_ENDPOINTS` | *(unset: single manager)* | Comma-separated etcd client URLs. Setting it turns high availability on. |
| `DOCKFLEET_INSTANCE_ID` | *(required in this mode)* | The manager's name: unique per manager. It names the leader in the cluster, the audit trail and the metrics. |
| `DOCKFLEET_ELECTION_TTL` | `15s` | Lease TTL, 5 s to 5 min. A failover takes at most `4/3 ×` this. etcd counts whole seconds. |
| `DOCKFLEET_ETCD_PREFIX` | `/dockfleet` | Key prefix; the leader key is `<prefix>/leader`. Use one prefix per cluster. |
| `DOCKFLEET_ETCD_CA_FILE`, `_CERT_FILE`, `_KEY_FILE` | | Mutual TLS to etcd. **Required together for any `https` endpoint**; the certificate is re-read when it is renewed. Plaintext `http` is accepted for loopback only. |

Validation that exists because of failover:

- `DOCKFLEET_STORE` must be `postgres`: managers that elect a leader have to share their state.
- `DOCKFLEET_NODE_LEASE` must be at least `2 × DOCKFLEET_ELECTION_TTL + DOCKFLEET_HEARTBEAT_INTERVAL` (40 s with the defaults; the default lease is 60 s). A failover must fit inside the lease agents fence themselves by, or **every change of leader would stop the containers of the fleet** (see the first limitation below).
- Whoever can write the leadership key decides who leads, so etcd is reached with mutual TLS or on loopback; there is no plaintext fallback.

Agents:

| Variable | Default | Meaning |
| --- | --- | --- |
| `DOCKFLEET_MANAGER_ADDRS` | `DOCKFLEET_MANAGER_ADDR` | Comma-separated manager gRPC addresses, in the order to try. Only the leader answers, so the agent goes round them until one does (and starts with the one that answered last). Without mutual TLS every address must be loopback. |

Agents must be at least this version before the cluster is turned on: an older agent knows one manager address, ignores epochs and does not understand `NOT_LEADER`. Managers accept older agents (protocol 1.5 only added optional fields and one error code), so upgrade agents first.

## Observing leadership

- **Audit trail** (`GET /v1/events`): `leader.elected` (resource `cluster/control-plane`; epoch, predecessor, etcd revision; actor `election`) and `leader.lost` (epoch, reason). `leader.elected` is written in the term's own transaction, so epochs in the trail are consecutive and each names its predecessor. `leader.lost` is best effort: a manager that crashes cannot write it.
- **`GET /v1/cluster`** (any manager): `mode` (`single` or `high-availability`), `role` (`leader`, `follower`, `single`), this manager's `epoch` while it leads, the `leader` as this manager knows it, `since`, `valid_until`, the number of `transitions` and `errors`, and `control_plane`: the epoch and leader recorded in the database. A leader whose `epoch` is lower than `control_plane.epoch` has been replaced and has not noticed yet.
- **Metrics** (per manager): `dockfleet_manager_is_leader`, `dockfleet_manager_epoch`, `dockfleet_control_plane_epoch`, `dockfleet_cluster_leader{leader}`, `dockfleet_leader_transitions_total`, `dockfleet_election_errors_total`. Scrape every manager; the first two differ between them, the third is the same everywhere.
- **Logs**: `this manager is now the leader` and `this manager is no longer the leader` with the epoch and the reason, plus a warning for every failed election step.

Alerts worth having: no manager has `dockfleet_manager_is_leader == 1` for more than the election TTL; more than one has it for more than a few seconds (a fenced leader steps down within one renewal interval); `increase(dockfleet_leader_transitions_total[1h])` high (flapping: etcd or the network is unstable); `dockfleet_manager_epoch` of a leader below `dockfleet_control_plane_epoch`.

## Failover timeline

For a leader that crashes at *t = 0* just after renewing, with the default 15 s TTL:

- *t ≤ 12 s*: had the leader not crashed but only lost its connection to etcd, it would have stopped acting by its own clock by now (80 % of the lease).
- *t = 15 s*: etcd expires the lease and deletes the key.
- *t ≤ 20 s*: the next campaign of a follower (every 5 s) wins the key; `BeginTerm` makes epoch *n + 1*; the loops start.
- *t ≤ 20 s + a heartbeat*: agents have noticed that their manager is gone (a failed heartbeat, then `NOT_LEADER` from a follower), register with the new leader, and carry on. **Containers are not touched**: the new leader reads the same workloads from the database and the agents report the same containers.

A manager that shuts down cleanly gives up the lease, which shortens this to about one campaign interval.

## Consistency guarantees

These hold with the configuration validated above. Each is checked by tests (see below).

1. **One current epoch.** The database has exactly one current epoch; epochs are consecutive, and two managers never get the same one, however many begin a term at once.
2. **No write from a replaced leader.** A scheduling or reconciliation transaction commits only if its epoch is the current one when it checks, and no term begins between that check and its commit. Everything a leader decided under epoch *n* is committed before epoch *n + 1* exists, or is refused.
3. **A manager stops before another starts, normally.** A leader stops acting by its own clock at 80 % of the last lease etcd confirmed, counted from before the renewal was sent. A new leader can be elected only after etcd has expired that lease, which is at or after 100 %. This needs only that the clocks run at about the same *rate*, not that they agree. Guarantee 2 does not depend on it.
4. **Followers issue nothing.** A manager that does not lead answers no agent call with instructions and starts no loop. A manager that stops leading while it prepares an answer sends `NOT_LEADER` instead.
5. **An agent never goes back.** While it runs, an agent obeys no control plane whose epoch is lower than one it has heard; a higher one is obeyed whoever sends it.
6. **Nothing of the cluster's state is in a manager's memory.** Desired state, placement, observations, rollout records and the audit trail are in PostgreSQL. What a manager does hold (agent sessions, open log streams) is rebuilt after a failover: agents register again, log queries that follow resume from their cursor.
7. **A single manager is unchanged.** Without etcd nothing above is active: no epoch is checked, no election runs, and the behaviour is that of the previous releases.

## Remaining limitations, partitions included

- **etcd unavailable (no quorum, or unreachable from the managers) stops the control plane, and then, after the lease, the fleet.** No leader can renew, so after 80 % of the TTL there is none; agents get `NOT_LEADER` and their leases stop being renewed. With the default `DOCKFLEET_FENCE_ON_LEASE_LOSS=true`, **an etcd outage longer than the node lease stops every container** (the agents fence themselves). Size the lease for the outage you want to survive, run etcd as a cluster of three or five, or turn fencing off on agents and accept duplicate replicas after a failover.
- **Leadership does not depend on the database being reachable.** A leader that holds its etcd lease but cannot reach PostgreSQL keeps the leadership, and its passes fail; no other manager takes over. (Followers could not do better without the database: they cannot begin a term either.) Watch `dockfleet_scheduling_errors_total` and `dockfleet_reconcile_errors_total` on the leader. PostgreSQL's own availability is out of scope.
- **The agent forgets the epoch when it restarts.** The highest epoch is kept in the agent process. A restarted agent that reaches a replaced leader first can take one answer from it before it hears the new epoch. What such a leader says is what the database says, and it stops answering within its trusted window, so the exposure is one stale-but-consistent answer, not a divergence.
- **An agent that can reach only followers is in the same position as one that reaches no manager.** If the partition separates agents from the leader but not the leader from etcd, the leader stays the leader, the agents get no instructions and, once their lease runs out, stop their containers (with fencing on). The node is then marked unavailable and its replicas are started elsewhere, which is what the lease and the unavailable timeout were sized for.
- **Replicas on a node that is partitioned from every manager** are handled by the existing node-failure rules ([failure-handling.md](failure-handling.md)), not by anything new here: the node's agent fences itself at the end of its lease, and its replicas are moved after `DOCKFLEET_NODE_UNAVAILABLE_AFTER`.
- **Agent reports are not fenced.** Heartbeats and workload reports a replaced leader receives before it notices are recorded as what they are: true facts about the node. They are observed state, never intent. The audit event of a dashboard login is not fenced either.
- **Clocks.** Guarantee 3 assumes bounded drift of the clock *rate*. A process that is paused for longer than its trust window (a stopped VM, a long garbage collection) can wake up believing it still leads; the database check refuses its writes, and the pause itself gives it no way to hurt. Managers still compare wall-clock times written by each other (node heartbeats, lease expiry in the older `leases` table), so keep them on NTP, as before.
- **Two clusters on one database.** Managers that share a database must share an etcd prefix. If two groups with different prefixes share a database, both elect a leader, but each election raises the epoch and fences the other, so they take turns stepping down. Writes stay safe, the cluster does not work. Do not do that.
- **A single manager and a cluster on one database.** A manager without etcd does not check epochs. Do not run one beside managers that elect.
- **No automatic failback or stickiness.** Whoever wins the next election leads. A manager that is fenced sits out one TTL.
- **Container logs are read through the leader.** A follower has no agent streams.
- **No leader-aware load balancing.** Put a load balancer in front of the API if you want one address; any manager serves reads and desired-state writes, only `POST /v1/workloads` needs the leader (`503 not_leader` with the leader's name), and the dashboard shows whichever manager you reach.
- **Cluster membership is static.** Managers and agents are configured with the addresses they know; there is no discovery.

## Deploying

1. Run etcd (three or five members in production; `make etcd-test-up` is a disposable single member for tests only, loopback and plaintext).
2. Give each manager the same `DOCKFLEET_DATABASE_URL`, `DOCKFLEET_ETCD_ENDPOINTS`, `DOCKFLEET_ETCD_PREFIX` and tokens or certificates, and its own `DOCKFLEET_INSTANCE_ID` and listen addresses. Migrations are applied by whichever manager starts first (migration 012 adds `control_plane`).
3. Upgrade agents, then set `DOCKFLEET_MANAGER_ADDRS` on them to all manager gRPC addresses.
4. Check `GET /v1/cluster` on each manager and `GET /v1/events?resource_type=cluster`.

To go back to a single manager, stop the others, unset `DOCKFLEET_ETCD_ENDPOINTS` on the remaining one, and leave the epoch where it is; it is only read when high availability is on.

## Code and tests

- `internal/election`: the elector, with a `Backend` interface, the `Fence` contract the loops and the API use, and no dependency but the standard library. `internal/etcd`: the backend, a client of etcd's JSON gateway on `net/http` (no new dependency), with failover between members and mutual TLS. `internal/etcdfake`: an in-memory etcd for tests.
- `internal/store`: `ControlPlane`, `BeginTerm`, `CheckEpoch` (memory and PostgreSQL), migration `012_control_plane.sql`.
- `internal/placement`, `internal/reconcile`: `Config.Fence`. `internal/protocol`: `Fencing` on the server; the client's endpoint list and epoch check. `internal/api`: `GET /v1/cluster`, the leader-only rule and the cluster metrics. `internal/manager`: `WithElection`, the term, and the loops built for each term. `internal/agent`: the retry while no manager leads.
- Tests: `internal/election` (every way a term begins and ends, on a manual clock); `internal/etcd` (the backend contract, run against the fake always and against a real etcd under the `etcdintegration` tag, failover between members, TLS, hostile replies); `internal/store` conformance on both stores, including that a term waits for a transaction that checked the epoch (verified to fail without `FOR SHARE` on PostgreSQL); placement and reconciliation passes under a replaced epoch; `internal/protocol` (followers, epochs, stale answers, streams); `internal/api` (cluster view, leader-only, metrics, health from the term); `internal/integration/ha_test.go`: three real managers on loopback ports 21260 and up and real agents on an in-memory Docker, covering one leader acting alone, leadership transfer on shutdown with the old manager rejoining, failover after a crash that keeps its lease, a partitioned leader that stops before its successor starts, and `ha_stale_test.go`: an agent offered instructions by a control plane that was replaced. The multi-manager scenarios run against the fake etcd and the in-memory store, against PostgreSQL (tag `integration`) and against a real etcd (tag `etcdintegration`, `make test-etcd`).
