-- A backfill: a range of the watermark of a source, read on a sequence of its own (ADR-0021 §5).
--
-- Its sequence is <source_id>/<layer>/backfill/<backfill_id>, which is also the key of its
-- lease and of its epoch. Its chunks are batches of that sequence, numbered from 1, of the kind
-- `backfill`, and written to the tables of the source beside the batches of its regular
-- sequence. Its progress is kept here, and not in source_state, which it does not touch.

-- The runs of a backfill, and the batches they plan.
ALTER TABLE control.ingestion_run
    DROP CONSTRAINT ingestion_run_run_kind_check,
    ADD CONSTRAINT ingestion_run_run_kind_check
        CHECK (run_kind IN ('regular', 'abandon', 'reset', 'reprocess', 'backfill'));

ALTER TABLE control.ingestion_batch
    DROP CONSTRAINT ingestion_batch_run_kind_check,
    ADD CONSTRAINT ingestion_batch_run_kind_check
        CHECK (run_kind IN ('regular', 'reprocess', 'backfill'));

-- A run acts on a sequence of its own source and layer, and on one that its kind may act on: a
-- backfill on that of a backfill, an operator's abandonment on either, every other kind on the
-- regular one. A batch is of the same: a chunk of a backfill on its sequence, a batch of a run or
-- of a reprocess on the regular one. The id of a backfill is in the grammar of the application
-- (BackfillId), and so has no '/'.
ALTER TABLE control.ingestion_run
    ADD CONSTRAINT run_of_a_sequence_of_its_kind CHECK (
        CASE
            WHEN txn_app_id = source_id || '/' || layer THEN run_kind <> 'backfill'
            WHEN starts_with(txn_app_id, source_id || '/' || layer || '/backfill/')
                 AND substr(txn_app_id, char_length(source_id || '/' || layer || '/backfill/') + 1)
                     ~ '^[a-z0-9][a-z0-9._-]{0,62}$'
                THEN run_kind IN ('backfill', 'abandon')
            ELSE false
        END
    );

ALTER TABLE control.ingestion_batch
    ADD CONSTRAINT batch_of_a_sequence_of_its_kind CHECK (
        CASE
            WHEN txn_app_id = source_id || '/' || layer THEN run_kind IN ('regular', 'reprocess')
            WHEN starts_with(txn_app_id, source_id || '/' || layer || '/backfill/')
                 AND substr(txn_app_id, char_length(source_id || '/' || layer || '/backfill/') + 1)
                     ~ '^[a-z0-9][a-z0-9._-]{0,62}$'
                THEN run_kind = 'backfill'
            ELSE false
        END
    );

-- What a backfill reads, fixed when it is created, and how far it has read.
--
-- The range is (range_from, range_to]: the rows whose watermark is after range_from and at or
-- before range_to. It is kept as the moments that the operator gave, and not as a watermark of
-- its connector, which the control plane does not read; the progress is one, as source_state
-- keeps one. The moments are those that a plan holds, from 1900 to the end of 9999.
--
-- It is created by a run of its sequence, under its lease: that run's configuration is the one
-- that the backfill is held to, and who asked for it and why are that run's. It is moved by
-- the commit of each chunk, by a run of its sequence, in the transaction of the commit, and is
-- complete when the chunk that ends at range_to is committed. A backfill that is complete does
-- not change.
CREATE TABLE control.backfill (
    source_id                text        NOT NULL,
    layer                    text        NOT NULL CHECK (layer IN ('bronze', 'silver')),
    backfill_id              text        NOT NULL CHECK (backfill_id ~ '^[a-z0-9][a-z0-9._-]{0,62}$'),
    -- The name of its sequence: the txn_app_id of its runs and of its chunks.
    txn_app_id               text        NOT NULL
        GENERATED ALWAYS AS (source_id || '/' || layer || '/backfill/' || backfill_id) STORED,
    range_from               timestamptz NOT NULL,
    range_to                 timestamptz NOT NULL,
    chunk                    interval    NOT NULL,
    created_by_run           uuid        NOT NULL,
    created_at               timestamptz NOT NULL DEFAULT now(),  -- the database's clock
    progress                 jsonb,      -- its connector's: the control plane does not read it
    last_committed_batch_seq bigint      CHECK (last_committed_batch_seq >= 1),
    updated_by_run           uuid,
    updated_at               timestamptz,
    completed_at             timestamptz,
    PRIMARY KEY (source_id, layer, backfill_id),
    UNIQUE (txn_app_id),
    CONSTRAINT range_holds_a_moment CHECK (range_from < range_to),
    CONSTRAINT range_of_moments_a_plan_holds CHECK (
        range_from >= timestamptz '1900-01-01 00:00:00+00'
        AND range_to <= timestamptz '9999-12-31 23:59:59.999999+00'
    ),
    CONSTRAINT chunk_is_a_stretch_of_time CHECK (chunk > interval '0'),
    CONSTRAINT moved_whole_or_not_at_all CHECK (
        (progress IS NULL) = (last_committed_batch_seq IS NULL)
        AND (progress IS NULL) = (updated_by_run IS NULL)
        AND (progress IS NULL) = (updated_at IS NULL)
    ),
    CONSTRAINT completed_by_a_chunk CHECK (completed_at IS NULL OR progress IS NOT NULL)
);

-- What a run of a sequence does is to that sequence: a run, and the sequence that it acts on,
-- as a pair that a row can name. It is unique, since run_id is.
ALTER TABLE control.ingestion_run
    ADD CONSTRAINT ingestion_run_of_its_sequence UNIQUE (run_id, txn_app_id);

-- A batch is planned, committed and given up by runs of its sequence, each under its lease: a
-- source has more than one sequence now, and a run of one has nothing to do with a batch of
-- another.
ALTER TABLE control.ingestion_batch
    ADD CONSTRAINT batch_planned_by_a_run_of_its_sequence
        FOREIGN KEY (planned_by_run, txn_app_id) REFERENCES control.ingestion_run (run_id, txn_app_id),
    ADD CONSTRAINT batch_committed_by_a_run_of_its_sequence
        FOREIGN KEY (committed_by_run, txn_app_id) REFERENCES control.ingestion_run (run_id, txn_app_id),
    ADD CONSTRAINT batch_abandoned_by_a_run_of_its_sequence
        FOREIGN KEY (abandoned_by_run, txn_app_id) REFERENCES control.ingestion_run (run_id, txn_app_id);

ALTER TABLE control.backfill
    ADD CONSTRAINT backfill_created_by_a_run_of_its_sequence
        FOREIGN KEY (created_by_run, txn_app_id) REFERENCES control.ingestion_run (run_id, txn_app_id),
    ADD CONSTRAINT backfill_moved_by_a_run_of_its_sequence
        FOREIGN KEY (updated_by_run, txn_app_id) REFERENCES control.ingestion_run (run_id, txn_app_id);

-- What is complete stays as it is: a run that commits a chunk late, or anyone else, finds it so.
CREATE FUNCTION control.refuse_change_of_a_complete_backfill() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'the backfill % is complete, and what is complete does not change', OLD.txn_app_id
        USING ERRCODE = 'check_violation';
END;
$$;

-- Nobody calls it: it runs as the trigger of the table, whoever updates it.
REVOKE ALL ON FUNCTION control.refuse_change_of_a_complete_backfill() FROM PUBLIC;

CREATE TRIGGER backfill_complete_stays
    BEFORE UPDATE ON control.backfill
    FOR EACH ROW WHEN (OLD.completed_at IS NOT NULL)
    EXECUTE FUNCTION control.refuse_change_of_a_complete_backfill();

UPDATE control.schema_version SET version = 7;

-- The engine creates a backfill and moves its progress. What it reads, its range and its chunk,
-- and the run that created it, are written once: it has no UPDATE on them. It deletes nothing.
GRANT SELECT, INSERT ON control.backfill TO ingest_control;
GRANT UPDATE (progress, last_committed_batch_seq, updated_by_run, updated_at, completed_at)
    ON control.backfill TO ingest_control;
