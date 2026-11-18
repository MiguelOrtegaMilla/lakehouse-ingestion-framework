-- The control plane (ADR-0003): the runs, the batches they ingest, the state of each source and
-- the leases on it (ADR-0005, ADR-0006), and the privileges of the role that uses them
-- (ADR-0013 §3).
--
-- Applied by Flyway as ingest_migrator, which owns schema control. The schema and the roles
-- named below come from the cluster bootstrap (docker/postgres/init): creating them takes
-- privileges that no runtime role has. Every name is schema-qualified, so nothing depends on a
-- search_path.
--
-- The names of the layers, of the statuses and of the classes of failure are those of the model
-- (lakehouse.ingestion.core.control); a test holds the two to each other.

-- The version of this schema, which the engine checks before it touches a table. A table of
-- this schema, not the migration tool's history, so that the engine neither depends on that
-- tool nor reads its bookkeeping. A later migration updates the row.
CREATE TABLE control.schema_version (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    version   integer NOT NULL
);
INSERT INTO control.schema_version (version) VALUES (1);

-- The configuration that runs ran with (ADR-0007): the content, once for each hash. A row is
-- written and never changed: it is what a run was told, and the audit trail points at it.
CREATE TABLE control.config_snapshot (
    config_hash   text        PRIMARY KEY CHECK (config_hash ~ '^[0-9a-f]{64}$'),  -- SHA-256
    source_id     text        NOT NULL,
    content       jsonb       NOT NULL,  -- { schema_version, profile, source }: no secret in it
    first_seen_at timestamptz NOT NULL DEFAULT now()
);

-- One row for each run: one attempt to ingest from a source (ADR-0005 §1). It is the audit
-- trail: who ran, what, with which configuration, how it ended.
--
-- A run is born RUNNING, with the epoch of the lease it took, or SKIPPED_LOCKED, with none. The
-- counts are written when it ends, by the phases that have something to count; null says that
-- nothing counted them, which is not zero.
CREATE TABLE control.ingestion_run (
    run_id            uuid        PRIMARY KEY,  -- UUID of version 7, made by the instance
    source_id         text        NOT NULL,
    layer             text        NOT NULL CHECK (layer IN ('bronze', 'silver')),
    run_kind          text        NOT NULL CHECK (run_kind IN ('regular')),
    txn_app_id        text        NOT NULL,
    batch_seq         bigint      CHECK (batch_seq >= 1),  -- the batch it planned or recovered
    status            text        NOT NULL
        CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'SKIPPED_LOCKED', 'ABANDONED')),
    instance_id       text        NOT NULL,  -- the process that ran it
    app_version       text        NOT NULL,
    config_hash       text        NOT NULL REFERENCES control.config_snapshot,
    lease_epoch       bigint      CHECK (lease_epoch >= 1),
    started_at        timestamptz NOT NULL DEFAULT now(),  -- the database's clock, never a client's
    finished_at       timestamptz,
    rows_read         bigint      CHECK (rows_read >= 0),
    rows_written      bigint      CHECK (rows_written >= 0),
    rows_rejected     bigint      CHECK (rows_rejected >= 0),
    bytes_read        bigint      CHECK (bytes_read >= 0),
    retries           integer     CHECK (retries >= 0),
    write_conflicts   integer     CHECK (write_conflicts >= 0),
    commit_contention integer     CHECK (commit_contention >= 0),
    error_class       text        CHECK (error_class IN ('transient', 'conflict', 'permanent', 'data')),
    error_message     text,
    CONSTRAINT finished_iff_ended CHECK ((status = 'RUNNING') = (finished_at IS NULL)),
    CONSTRAINT epoch_iff_lease_taken CHECK ((status = 'SKIPPED_LOCKED') = (lease_epoch IS NULL)),
    CONSTRAINT error_class_iff_failed CHECK ((status = 'FAILED') = (error_class IS NOT NULL))
);

-- The last run of a source, and its runs in order.
CREATE INDEX ingestion_run_by_source ON control.ingestion_run (source_id, layer, started_at DESC);

-- One row for each batch: a unit of data, a range of a watermark or a set of files (ADR-0005).
-- It is the write-ahead log. The plan is written here before anything is read, and a batch that
-- is found PLANNED is one whose run did not get to the end: its plan is run again as it is, or
-- its state is rolled forward if the data is in the table already.
CREATE TABLE control.ingestion_batch (
    txn_app_id            text        NOT NULL,
    batch_seq             bigint      NOT NULL CHECK (batch_seq >= 1),  -- its txnVersion in Delta
    source_id             text        NOT NULL,
    layer                 text        NOT NULL CHECK (layer IN ('bronze', 'silver')),
    run_kind              text        NOT NULL CHECK (run_kind IN ('regular')),
    plan                  jsonb       NOT NULL,  -- its connector's: never changed once written
    status                text        NOT NULL DEFAULT 'PLANNED'
        CHECK (status IN ('PLANNED', 'COMMITTED', 'ABANDONED')),
    planned_by_run        uuid        NOT NULL REFERENCES control.ingestion_run,
    planned_at            timestamptz NOT NULL DEFAULT now(),
    committed_by_run      uuid        REFERENCES control.ingestion_run,
    committed_at          timestamptz,
    outputs               jsonb,      -- table -> the version of Delta it was committed at
    input_bronze_versions jsonb,      -- Silver alone: the versions of Bronze it read (P10)
    PRIMARY KEY (txn_app_id, batch_seq),
    CONSTRAINT committed_iff_by_a_run CHECK ((status = 'COMMITTED') = (committed_by_run IS NOT NULL)),
    CONSTRAINT committed_iff_at_a_time CHECK ((status = 'COMMITTED') = (committed_at IS NOT NULL)),
    CONSTRAINT outputs_of_a_commit_alone CHECK (outputs IS NULL OR status = 'COMMITTED')
);

-- At most one batch of a sequence is planned at a time: the next is planned once it is
-- committed. Two runs that both plan, a zombie and the run that took its place, cannot both
-- write a plan down.
CREATE UNIQUE INDEX ingestion_batch_one_planned
    ON control.ingestion_batch (txn_app_id) WHERE status = 'PLANNED';

ALTER TABLE control.ingestion_run
    ADD CONSTRAINT ingestion_run_batch
    FOREIGN KEY (txn_app_id, batch_seq) REFERENCES control.ingestion_batch;

-- How far each source has been ingested in each layer. A row is written by the commit of the
-- first batch, and moved by every commit after it, in the same transaction that marks the batch
-- COMMITTED and its run SUCCEEDED (ADR-0005 §3, step 7).
CREATE TABLE control.source_state (
    source_id                text        NOT NULL,
    layer                    text        NOT NULL CHECK (layer IN ('bronze', 'silver')),
    watermark                jsonb,      -- its connector's: the control plane does not read it
    last_committed_batch_seq bigint      NOT NULL CHECK (last_committed_batch_seq >= 1),
    updated_by_run           uuid        NOT NULL REFERENCES control.ingestion_run,
    updated_at               timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_id, layer)
);

-- Who may advance the state of a source in a layer, now (ADR-0006). The epoch counts how many
-- times the lease of a key was taken, and is what a statement that advances the state names:
-- a run whose lease was taken again changes nothing.
--
-- The holder is a run that exists. The run and the lease it takes are written in one
-- transaction, the lease first, since the run needs its epoch: the reference is checked when
-- the transaction commits.
CREATE TABLE control.source_lease (
    lease_key     text        PRIMARY KEY,  -- <source_id>/<layer>
    epoch         bigint      NOT NULL DEFAULT 0 CHECK (epoch >= 0),
    holder_run_id uuid        REFERENCES control.ingestion_run DEFERRABLE INITIALLY DEFERRED,
    instance_id   text,
    acquired_at   timestamptz,
    expires_at    timestamptz,  -- by the database's clock, as every time here
    CONSTRAINT held_whole_or_not_at_all CHECK (
        (holder_run_id IS NULL) = (instance_id IS NULL)
        AND (holder_run_id IS NULL) = (acquired_at IS NULL)
        AND (holder_run_id IS NULL) = (expires_at IS NULL)
    )
);

-- What ended stays as it ended. A run that is not RUNNING and a batch that is not PLANNED are
-- part of the audit trail: nothing changes them, whoever asks and whatever it sets. A run that
-- lost its lease and reports late finds its row as the run that took over left it.
CREATE FUNCTION control.refuse_change_of_what_ended() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'a row of % ended as %, and what ended does not change', TG_TABLE_NAME, OLD.status
        USING ERRCODE = 'check_violation';
END;
$$;

-- Nobody calls it: it runs as the trigger of the two tables, whoever updates them.
REVOKE ALL ON FUNCTION control.refuse_change_of_what_ended() FROM PUBLIC;

CREATE TRIGGER ingestion_run_ended_stays
    BEFORE UPDATE ON control.ingestion_run
    FOR EACH ROW WHEN (OLD.status <> 'RUNNING')
    EXECUTE FUNCTION control.refuse_change_of_what_ended();

CREATE TRIGGER ingestion_batch_ended_stays
    BEFORE UPDATE ON control.ingestion_batch
    FOR EACH ROW WHEN (OLD.status <> 'PLANNED')
    EXECUTE FUNCTION control.refuse_change_of_what_ended();

-- Privileges. Nothing is granted to PUBLIC, the migration tool's history table is granted to
-- nobody, and no role but the engine's has anything in this schema.
--
-- The engine adds rows and moves a run and a batch to their end. It deletes nothing: how long
-- the audit trail is kept is not its to decide. And it cannot rewrite what identifies a run, nor
-- the plan of a batch, so a defect in it cannot change what the trail says was run.
GRANT USAGE ON SCHEMA control TO ingest_control;
GRANT SELECT ON control.schema_version TO ingest_control;

GRANT SELECT, INSERT ON control.config_snapshot TO ingest_control;

GRANT SELECT, INSERT ON control.ingestion_run TO ingest_control;
GRANT UPDATE (
    status, finished_at, batch_seq,
    rows_read, rows_written, rows_rejected, bytes_read, retries, write_conflicts, commit_contention,
    error_class, error_message
) ON control.ingestion_run TO ingest_control;

GRANT SELECT, INSERT ON control.ingestion_batch TO ingest_control;
GRANT UPDATE (status, committed_by_run, committed_at, outputs, input_bronze_versions)
    ON control.ingestion_batch TO ingest_control;

GRANT SELECT, INSERT, UPDATE ON control.source_state TO ingest_control;
GRANT SELECT, INSERT, UPDATE ON control.source_lease TO ingest_control;
