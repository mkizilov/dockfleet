-- The fencing record of high-availability mode. Several managers may share this
-- database; a leader, elected elsewhere (etcd), begins an epoch here and acts
-- under it. Every scheduling and reconciliation transaction checks, with a row
-- lock, that its epoch is still the current one, so a manager that has been
-- replaced cannot write. Epoch only grows, and 0 means nobody was elected yet
-- (single-manager mode never changes it).

CREATE TABLE control_plane (
    id         boolean PRIMARY KEY DEFAULT true CHECK (id),
    epoch      bigint NOT NULL CHECK (epoch >= 0),
    leader     text NOT NULL DEFAULT '',
    elected_at timestamptz NOT NULL
);

INSERT INTO control_plane (id, epoch, leader, elected_at) VALUES (true, 0, '', to_timestamp(0));
