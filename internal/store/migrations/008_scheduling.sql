-- Scheduling.
--
-- A placement is a workload row (desired state: which node, which resources).
-- Workloads now also record the ports they publish, so the scheduler can keep
-- two of them from claiming the same host port on one node.
--
-- unschedulable_replicas records why a replica of a service's current revision
-- could not be placed. A row exists only while the replica is waiting; the
-- scheduler removes it when the replica is placed or no longer wanted.

ALTER TABLE workloads ADD COLUMN ports jsonb NOT NULL DEFAULT '[]'::jsonb;

CREATE TABLE unschedulable_replicas (
    service_id    text    NOT NULL,
    revision      integer NOT NULL CHECK (revision >= 1),
    replica_index integer NOT NULL CHECK (replica_index >= 1),
    code          text    NOT NULL,
    message       text    NOT NULL,
    -- One entry per node: why that node was rejected.
    nodes         jsonb   NOT NULL DEFAULT '[]'::jsonb,
    -- When this code and message first applied; unchanged rows are not rewritten.
    since         timestamptz NOT NULL,
    PRIMARY KEY (service_id, revision, replica_index),
    FOREIGN KEY (service_id, revision) REFERENCES service_revisions (service_id, revision)
);
