-- Reconciliation.
--
-- A workload is one container's lifetime: running while it is meant to run,
-- stopped (desired_state) when it is retired, deleted once its node has
-- confirmed that the container is gone. Nothing runs under an ID again after
-- it was stopped.
--
-- generation increases with every change of desired_state. The agent
-- acknowledges the generation it has seen (acknowledged_generation), which
-- separates an instruction that was received from one still on its way.
--
-- runtime holds what the container is started with (command, environment,
-- restart policy), copied from the service revision when the workload is
-- placed so that a workload never changes underneath its container.

ALTER TABLE workloads
    ADD COLUMN generation bigint NOT NULL DEFAULT 1 CHECK (generation >= 1),
    ADD COLUMN runtime    jsonb  NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE workload_observations
    ADD COLUMN acknowledged_generation bigint NOT NULL DEFAULT 0 CHECK (acknowledged_generation >= 0);
