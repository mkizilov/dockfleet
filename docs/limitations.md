# Operational limitations, and where DockFleet differs from Kubernetes

DockFleet is a small orchestrator for a fleet of Docker hosts: you describe a **service** (an image, a replica count, resource limits, a restart policy, a rollout policy) and it keeps that many healthy replicas running on the nodes you have, replaces them when a node or container fails, updates them gradually and tells you what it did. It is not a Kubernetes replacement, and this document says where, so that nobody chooses it expecting something it is not.

Per-feature limitations are in the document of the feature; this page collects what an operator needs to know before depending on it, and ends with what has not been verified.

## Where it differs from Kubernetes

| | Kubernetes | DockFleet |
| --- | --- | --- |
| **Unit of deployment** | Pod: one or more containers sharing a network and volumes, sidecars, init containers | One container per replica. No pods, sidecars or init containers |
| **Workload kinds** | Deployment, StatefulSet, DaemonSet, Job, CronJob, and your own through CRDs | Services (replicated, rolling) and standalone workloads. No stateful sets, daemon sets, batch jobs or schedules; no extension mechanism |
| **Networking** | Pod IPs, Services, DNS, Ingress, NetworkPolicy, a CNI plugin | None. A replica's published ports bind to the **node's loopback** only; exposing a service, balancing between replicas and finding them is your reverse proxy's job. No overlay network, no service discovery, no network policy |
| **Storage** | PersistentVolumes, claims, CSI drivers, dynamic provisioning | Named Docker volumes (`dockfleet-` prefix) and bind mounts under directories the operator allowed on that node; nothing follows a replica to another node |
| **Configuration and secrets** | ConfigMaps, Secrets (and encryption at rest, external providers) | Environment variables in the service spec, **stored and returned in plain text**. No secret store: do not put credentials in a spec |
| **Scheduling** | Requests/limits, node and pod affinity, taints and tolerations, topology spread, priorities and preemption, custom schedulers | Declared CPU and memory (never overcommitted unless configured), a label node selector, host-port conflicts, and spreading a service's replicas over nodes. No affinities, taints, priorities or preemption. Deterministic and explained, one reason per node |
| **Resource accounting** | Measured usage through metrics-server; autoscaling (HPA/VPA, cluster autoscaler) | **Declared** capacity and limits only; no measured usage is collected or exported, and nothing scales by itself: you set the replica count |
| **Health** | Liveness, readiness and startup probes (exec, HTTP, TCP, gRPC), separate | The container's Docker health check (exec form) and its restart policy. A replica is *ready* when it runs and, if it has a check, the check passes |
| **Rolling updates** | `maxSurge`/`maxUnavailable` (also percentages), `minReadySeconds`, revision history limits, pause, rollback, progress deadline | `max_surge`/`max_unavailable` as replica counts, pause, resume, rollback as a new revision, progress deadline that reports failure. No percentages, no `minReadySeconds`, **no automatic rollback**, no disruption budgets |
| **Control plane state** | etcd *is* the store of record; API server serves watches | PostgreSQL is the store of record; etcd (optional) only holds the leadership key. REST + JSON, polling loops (5 s by default), **no watch API**, no kubectl |
| **Controllers** | Level-triggered control loops with watches, plus your own operators | Two loops (scheduler, reconciler) in the leader, level-triggered from the store, every 5 s |
| **Node agent** | kubelet, pulled over a connection the API server opens in places | The agent dials the manager and the manager never connects to a node, so workers can be behind NAT or a firewall |
| **Node failure** | Node lifecycle controller taints the node; pods are evicted after a timeout (5 minutes by default); pods may keep running on a partitioned node | Silence is graded `suspect` then `unavailable`; the agent **stops its own containers** when its lease runs out, strictly before the manager moves their replicas, so a replica does not run twice. The lease/silence limits are cluster-wide settings |
| **Multi-tenancy and access** | Namespaces, RBAC, admission control, audit policy, OIDC, quotas | **One shared admin token**; no users, roles, namespaces or quotas. Every change is audited, with the actor always `admin` |
| **Runtime** | Any CRI runtime (containerd, CRI-O), Windows nodes, many architectures | The Docker Engine only (through its API), Linux hosts. The binaries cross-compile for linux/amd64, linux/arm64 and darwin/arm64 |
| **Scale** | Thousands of nodes (documented limits) | **Not measured.** The tests use up to three nodes and a few dozen replicas. Do not assume tens of nodes work well without testing your own |
| **Ecosystem** | Helm, operators, service meshes, CI integrations, a very large community | None. A REST API and the dashboard |
| **Container security** | Pod security standards, seccomp/AppArmor profiles, admission webhooks | A fixed, strict policy applied by the agent to every container: no privileged mode, all capabilities dropped (a short allow-list can add some back), no-new-privileges, no host namespaces, no devices, mandatory CPU/memory/PID limits, no Docker socket or system paths, loopback-only ports. It cannot be loosened per service |

A way to think about it: DockFleet takes the part of Kubernetes that most small Docker deployments actually want — *declare N replicas, spread them, restart them, roll out and back, survive a node dying, see what happened* — and leaves out the platform around it.

## Operational limitations

**Networking and exposure**
- Nothing routes traffic to replicas. Put a reverse proxy on each node (or a load balancer in front) and point it at the loopback ports DockFleet publishes; you must keep it in step with where replicas are (`GET /v1/services/{name}` says).
- A replica cannot be reached from another node except through such a proxy.

**State and data**
- A replica that moves to another node starts with an empty filesystem and none of the old node's volumes. Do not run databases or anything that keeps state in the container as a service with more than one replica, or one that may move.
- Standalone workloads are never moved when their node fails.
- Everything is in PostgreSQL. Its availability, backup and restore are yours ([deployment.md](deployment.md#upgrades-backups-restores)); the manager cannot schedule or reconcile while it is down (running containers are not affected, and an outage does not make the manager give up on nodes when it ends).
- Migrations only go forward; there is no way back but a backup. Rolling a cluster of managers across a schema change is not tested.

**Failure handling** ([failure-handling.md](failure-handling.md), [high-availability.md](high-availability.md))
- A crashed agent *process* cannot stop its own containers, so a replica started elsewhere runs beside them until the agent returns (a hung or partitioned agent that is still alive does fence itself).
- A manager outage, or an etcd outage in a cluster, longer than `DOCKFLEET_NODE_LEASE` (60 s by default) makes every agent stop its containers until a manager answers. Turn `DOCKFLEET_FENCE_ON_LEASE_LOSS` off to keep containers running through such an outage, at the price of duplicate replicas after a failover.
- A replica whose container failed beyond its restart limit stays failed on its node; it is not moved.
- Restart counts and backoff are kept in the agent's memory and start again when the agent restarts.
- An agent keeps the highest control-plane epoch it has heard only while it runs.
- High availability needs etcd (3 or 5 members) as well as PostgreSQL; with it, leadership does not depend on the database being reachable, and an agent that can reach only followers is as cut off as one that reaches no manager.

**Security** ([security-audit.md](security-audit.md))
- One admin token for everything; failed attempts with it are not throttled; dashboard sessions cannot be revoked.
- No revocation list for certificates; revoke the node's enrollment instead.
- Containers run as the image's user unless the service says otherwise; user namespaces are not remapped.
- The agent holds the Docker socket, which is root on its node.
- No image allow-list or digest requirement.

**Observability and operations**
- Container logs are Docker's own `json-file` logs (rotated at 10 MiB × 3), read through the agent and never stored by the manager; a following query ends when the container stops; with several managers a query sees only the agents connected to the manager that serves it ([logs.md](logs.md)).
- Metrics of the loops and requests are per manager process and reset on restart; fleet gauges come from the database ([observability.md](observability.md)).
- Resource figures are what is *promised* to workloads, not what they use.
- Agent restarts do not restart containers, but an upgrade of the manager schema has to be done with the old managers stopped.

## What has and has not been verified

See [deployment.md](deployment.md#what-has-and-has-not-been-verified) for the list: what ran (the binaries end to end over mutual TLS with PostgreSQL and etcd, with failures injected; the unit, PostgreSQL and etcd suites) and what is written but was not run in the environment this was built in (the container images, Compose, systemd units, the CI workflow, the Docker-daemon tests, a multi-member etcd, more than a few nodes, Linux itself).
