-- Rolling deployments.
--
-- A service now says how a new revision replaces the old one: how many
-- replicas may run above the wanted count while it does (max_surge), how many
-- below it may be not ready (max_unavailable), and how long the rollout may go
-- without progress before it is reported failed (progress_deadline_seconds).
-- Both limits at zero would make a rollout unable to replace anything, so the
-- database refuses it. The defaults replace one replica at a time and never
-- give up availability, which is what rollouts did before they could be
-- configured. paused holds a rollout where it is.
--
-- rollouts records how far the deployment of each revision has come. It is
-- derived from workloads and agent reports by the reconciler, its only writer;
-- the only thing decided from it is whether a rollout has stalled, which needs
-- the time of its last progress. The counts are the best the rollout has
-- reached (replicas of the revision's spec that existed and that were ready,
-- and the fewest old replicas left), so a replica that comes and goes does not
-- count as progress each time.

ALTER TABLE services
    ADD COLUMN max_unavailable          integer NOT NULL DEFAULT 0 CHECK (max_unavailable >= 0),
    ADD COLUMN max_surge                integer NOT NULL DEFAULT 1 CHECK (max_surge >= 0),
    ADD COLUMN progress_deadline_seconds integer NOT NULL DEFAULT 600 CHECK (progress_deadline_seconds >= 1),
    ADD COLUMN paused                   boolean NOT NULL DEFAULT false,
    ADD CONSTRAINT services_rollout_can_progress CHECK (max_unavailable > 0 OR max_surge > 0);

CREATE TABLE rollouts (
    service_id  text    NOT NULL,
    revision    integer NOT NULL CHECK (revision >= 1),
    -- progressing, complete, failed or superseded.
    state       text    NOT NULL CHECK (state IN ('progressing', 'complete', 'failed', 'superseded')),
    reason      text    NOT NULL DEFAULT '',
    message     text    NOT NULL DEFAULT '',
    paused      boolean NOT NULL DEFAULT false,
    updated     integer NOT NULL DEFAULT 0 CHECK (updated >= 0),
    ready       integer NOT NULL DEFAULT 0 CHECK (ready >= 0),
    outdated    integer NOT NULL DEFAULT 0 CHECK (outdated >= 0),
    started_at  timestamptz NOT NULL,
    progress_at timestamptz NOT NULL,
    finished_at timestamptz,
    PRIMARY KEY (service_id, revision),
    FOREIGN KEY (service_id, revision) REFERENCES service_revisions (service_id, revision)
);
