-- Separates desired state from observed state for workloads.
--
-- workloads keeps only intent (placement, desired_state, which service
-- revision it belongs to). What the node's agent reports moves to
-- workload_observations, which only agents' reports write.

ALTER TABLE workloads
    ADD COLUMN desired_state    text NOT NULL DEFAULT 'running',
    ADD COLUMN service_id       text,
    ADD COLUMN service_revision integer,
    ADD COLUMN replica_index    integer,
    ADD COLUMN updated_at       timestamptz;

UPDATE workloads SET updated_at = created_at;

ALTER TABLE workloads
    ALTER COLUMN updated_at SET NOT NULL,
    ADD CONSTRAINT workloads_desired_state_check CHECK (desired_state IN ('running', 'stopped')),
    ADD CONSTRAINT workloads_replica_index_check CHECK (replica_index IS NULL OR replica_index >= 1),
    -- service_id, service_revision and replica_index are all set or all null.
    ADD CONSTRAINT workloads_service_columns_check CHECK (
        (service_id IS NULL) = (service_revision IS NULL) AND (service_id IS NULL) = (replica_index IS NULL)
    ),
    ADD CONSTRAINT workloads_service_revision_fkey
        FOREIGN KEY (service_id, service_revision) REFERENCES service_revisions (service_id, revision),
    -- Also the index behind "replicas of a service, by revision and index".
    ADD CONSTRAINT workloads_replica_key UNIQUE (service_id, service_revision, replica_index);

CREATE TABLE workload_observations (
    workload_id   text PRIMARY KEY REFERENCES workloads (id) ON DELETE CASCADE,
    -- The node that reported; differs from workloads.node_id if the workload
    -- was moved since, which marks the observation as stale.
    node_id       text NOT NULL REFERENCES nodes (id),
    state         text NOT NULL CHECK (state IN ('pending', 'running', 'succeeded', 'failed', 'stopped')),
    message       text NOT NULL DEFAULT '',
    container_id  text NOT NULL DEFAULT '',
    exit_code     integer,
    oom_killed    boolean NOT NULL DEFAULT false,
    restart_count integer NOT NULL DEFAULT 0,
    started_at    timestamptz,
    finished_at   timestamptz,
    observed_at   timestamptz NOT NULL
);

CREATE INDEX workload_observations_node_idx ON workload_observations (node_id);

-- Keep what agents had already reported.
INSERT INTO workload_observations (workload_id, node_id, state, message, observed_at)
SELECT id, node_id, state, status_message, status_updated_at
FROM workloads
WHERE status_updated_at IS NOT NULL AND state IN ('pending', 'running', 'succeeded', 'failed', 'stopped');

ALTER TABLE workloads
    DROP COLUMN state,
    DROP COLUMN status_message,
    DROP COLUMN status_updated_at;
