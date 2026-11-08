CREATE TABLE nodes (
    id             text PRIMARY KEY,
    name           text NOT NULL,
    cpu_millis     bigint NOT NULL CHECK (cpu_millis >= 0),
    memory_bytes   bigint NOT NULL CHECK (memory_bytes >= 0),
    labels         jsonb NOT NULL DEFAULT '{}'::jsonb,
    agent_version  text NOT NULL DEFAULT '',
    registered_at  timestamptz NOT NULL,
    last_heartbeat timestamptz NOT NULL
);

CREATE TABLE workloads (
    id           text PRIMARY KEY,
    name         text NOT NULL UNIQUE,
    image        text NOT NULL,
    cpu_millis   bigint NOT NULL CHECK (cpu_millis > 0),
    memory_bytes bigint NOT NULL CHECK (memory_bytes > 0),
    node_id      text NOT NULL REFERENCES nodes (id),
    state        text NOT NULL,
    created_at   timestamptz NOT NULL
);

CREATE INDEX workloads_node_id_idx ON workloads (node_id);
