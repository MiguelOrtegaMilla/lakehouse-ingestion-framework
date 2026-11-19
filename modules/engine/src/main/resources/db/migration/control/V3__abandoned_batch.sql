-- A batch that is given up says who gave it up, when and why (ADR-0018).
--
-- Until now a batch became ABANDONED with its status alone, and nothing in the engine made one
-- so. From P5.8 the run that holds the lease of a source gives up the batch whose plan the
-- source no longer has as it was planned, a file written again or gone, so that the next run
-- plans anew and the source is not stopped. An operator gives one up to reset a source or to
-- reprocess it (P7).
--
-- The plan of an abandoned batch stays as it was written, and its number is not used again: the
-- write-ahead log says what was to be read, and these columns why it never was.
ALTER TABLE control.ingestion_batch
    ADD COLUMN abandoned_by_run uuid REFERENCES control.ingestion_run,  -- none: not a run's doing
    ADD COLUMN abandoned_at     timestamptz,
    ADD COLUMN abandon_reason   text,   -- for a person to read
    ADD COLUMN abandon_detail   jsonb;  -- the engine's: what of the plan could not be run

-- A batch that was abandoned before this migration could say nothing of it but its status.
-- Nothing in the application made one so. One made so by hand is given the moment of this
-- migration and a reason that says as much, and not one invented for it: without them the
-- constraints below would refuse the row, and what ended cannot be changed to mend it. That
-- rule is lifted for this one statement, by the owner of the table, inside the transaction of
-- the migration.
ALTER TABLE control.ingestion_batch DISABLE TRIGGER ingestion_batch_ended_stays;
UPDATE control.ingestion_batch
   SET abandoned_at = now(),
       abandon_reason = 'abandoned before the control plane kept why (schema version 2); '
                        || 'the time is that of the migration to version 3'
 WHERE status = 'ABANDONED';
ALTER TABLE control.ingestion_batch ENABLE TRIGGER ingestion_batch_ended_stays;

ALTER TABLE control.ingestion_batch
    ADD CONSTRAINT abandoned_iff_at_a_time
        CHECK ((status = 'ABANDONED') = (abandoned_at IS NOT NULL)),
    ADD CONSTRAINT abandoned_iff_for_a_reason
        CHECK ((status = 'ABANDONED') = (abandon_reason IS NOT NULL)),
    ADD CONSTRAINT abandon_reason_says_something
        CHECK (abandon_reason IS NULL OR abandon_reason ~ '\S'),  -- not white space alone
    ADD CONSTRAINT abandoned_by_a_run_only_if_abandoned
        CHECK (abandoned_by_run IS NULL OR status = 'ABANDONED'),
    ADD CONSTRAINT abandon_detail_of_an_abandoned_alone
        CHECK (abandon_detail IS NULL OR status = 'ABANDONED');

UPDATE control.schema_version SET version = 3;

-- The engine says, once, how a planned batch ended. What ended stays as it ended, by the trigger
-- of V1, so neither who gave a batch up nor why is ever rewritten.
GRANT UPDATE (abandoned_by_run, abandoned_at, abandon_reason, abandon_detail)
    ON control.ingestion_batch TO ingest_control;
