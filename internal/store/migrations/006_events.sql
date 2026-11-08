-- Administrative audit trail. Append-only; written in the same transaction as
-- the change it describes.

CREATE TABLE events (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    at            timestamptz NOT NULL,
    -- The manager instance that recorded the event.
    instance_id   text NOT NULL DEFAULT '',
    actor         text NOT NULL,
    action        text NOT NULL,
    resource_type text NOT NULL,
    resource_id   text NOT NULL,
    message       text NOT NULL DEFAULT '',
    details       jsonb NOT NULL DEFAULT '{}'::jsonb
);

-- "What happened to this resource", newest first.
CREATE INDEX events_resource_idx ON events (resource_type, resource_id, id DESC);
CREATE INDEX events_at_idx ON events (at);
