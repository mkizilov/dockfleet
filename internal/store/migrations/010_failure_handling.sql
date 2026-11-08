-- Node failure handling.
--
-- health is the state the reconciler last recorded for a node (healthy,
-- suspect, unavailable) and health_since when it did, so that a change is
-- reported once. deregistered_at is when an agent left cleanly: the node has no
-- heartbeat then, and the time is where the clock for "unavailable" starts.
--
-- A workload that is meant to run owns its replica slot. One that is meant to
-- be stopped does not, even while its container is still being removed (or
-- sits on a node that is gone): the slot can be filled at once by a new
-- workload on another node. That is what moving a replica off a failed node
-- needs, so the whole-table uniqueness of (service, revision, replica) becomes
-- uniqueness among the workloads that are meant to run.
--
-- workload_observations.health is the container's health check result.

ALTER TABLE nodes
    ADD COLUMN health          text NOT NULL DEFAULT 'healthy'
        CONSTRAINT nodes_health_check CHECK (health IN ('healthy', 'suspect', 'unavailable')),
    ADD COLUMN health_since    timestamptz,
    ADD COLUMN deregistered_at timestamptz;

UPDATE nodes SET health_since = registered_at;

ALTER TABLE nodes ALTER COLUMN health_since SET NOT NULL;

ALTER TABLE workloads DROP CONSTRAINT workloads_replica_key;

CREATE UNIQUE INDEX workloads_running_replica_key
    ON workloads (service_id, service_revision, replica_index)
    WHERE desired_state = 'running' AND service_id IS NOT NULL;

-- The index behind "replicas of a service, by revision and index".
CREATE INDEX workloads_replica_idx ON workloads (service_id, service_revision, replica_index);

ALTER TABLE workload_observations ADD COLUMN health text NOT NULL DEFAULT '';
