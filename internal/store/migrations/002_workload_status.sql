ALTER TABLE workloads
    ADD COLUMN status_message    text NOT NULL DEFAULT '',
    ADD COLUMN status_updated_at timestamptz;
