CREATE TABLE enrollments (
    node_id     text PRIMARY KEY,
    description text NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL,
    revoked_at  timestamptz
);
