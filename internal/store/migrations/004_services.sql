-- Desired state: service definitions and their immutable revisions.

CREATE TABLE services (
    id               text PRIMARY KEY,
    name             text NOT NULL,
    replicas         integer NOT NULL CHECK (replicas >= 0),
    current_revision integer NOT NULL CHECK (current_revision >= 1),
    -- Optimistic-lock token: every change to the row increments it, and
    -- writers state the version they read, so concurrent managers cannot
    -- silently overwrite each other.
    version          bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at       timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL,
    deleted_at       timestamptz
);

-- A name is unique among live services, so a deleted service's name can be reused.
CREATE UNIQUE INDEX services_active_name_key ON services (name) WHERE deleted_at IS NULL;

CREATE TABLE service_revisions (
    service_id text NOT NULL REFERENCES services (id),
    revision   integer NOT NULL CHECK (revision >= 1),
    spec       jsonb NOT NULL,
    spec_hash  text NOT NULL,
    message    text NOT NULL DEFAULT '',
    created_by text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL,
    PRIMARY KEY (service_id, revision)
);

-- The current revision must exist. Deferred, because a service and its first
-- revision are inserted in one transaction, service first.
ALTER TABLE services
    ADD CONSTRAINT services_current_revision_fkey
    FOREIGN KEY (id, current_revision) REFERENCES service_revisions (service_id, revision)
    DEFERRABLE INITIALLY DEFERRED;
