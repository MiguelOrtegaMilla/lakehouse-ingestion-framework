-- What an operator does to a source is a run, of a kind of its own (ADR-0021 §1).
--
-- A run that gives up a batch, resets the state of a source, or reads its files again takes the
-- lease of the sequence that it acts on, as a run does, and is written down as a run: its id,
-- its instance, the version of the application, the configuration it was given, the epoch of its
-- lease, when it started and ended and how. It also says who asked for it, and why.
--
-- Who is the user of the operating system that ran the process, as the process reports it: a
-- datum of the audit trail, which says where to ask, and not an identity that anything verified.
-- Why is a reason, which the command requires. A regular run has neither.
--
-- A backfill is a kind of its own as well, and comes with its own migration (P7.6).

ALTER TABLE control.ingestion_run
    DROP CONSTRAINT ingestion_run_run_kind_check,
    ADD CONSTRAINT ingestion_run_run_kind_check
        CHECK (run_kind IN ('regular', 'abandon', 'reset', 'reprocess')),
    ADD COLUMN requested_by text
        CONSTRAINT requested_by_is_a_name CHECK (requested_by ~ '\S' AND char_length(requested_by) <= 200),
    ADD COLUMN reason text
        CONSTRAINT reason_says_something CHECK (reason ~ '\S' AND char_length(reason) <= 1000),
    ADD CONSTRAINT asked_for_iff_an_operator_action
        CHECK ((run_kind = 'regular') = (reason IS NULL)),
    ADD CONSTRAINT asked_by_whom_iff_why
        CHECK ((reason IS NULL) = (requested_by IS NULL));

-- Of the kinds of an operator, a reprocess alone plans a batch: the files of a batch that was
-- committed, read again on the regular sequence (ADR-0021 §4).
ALTER TABLE control.ingestion_batch
    DROP CONSTRAINT ingestion_batch_run_kind_check,
    ADD CONSTRAINT ingestion_batch_run_kind_check
        CHECK (run_kind IN ('regular', 'reprocess'));

-- Every change of the state of a source that is not the commit of a batch: a reset (ADR-0021
-- §3). One row for each, written in the transaction that makes the change, by the run that makes
-- it. Who asked and why are that run's, and are not repeated here.
--
-- The state before and after is as `source_state` has it: the watermark, which its connector
-- gives a form to, the last batch committed, and the run that last moved it. A row is written
-- and never changed.
CREATE TABLE control.state_change_log (
    change_id  bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id     uuid        NOT NULL REFERENCES control.ingestion_run,
    source_id  text        NOT NULL,
    layer      text        NOT NULL CHECK (layer IN ('bronze', 'silver')),
    before     jsonb       NOT NULL,
    after      jsonb       NOT NULL,
    changed_at timestamptz NOT NULL DEFAULT now()  -- the database's clock
);

-- The changes of a source, in order.
CREATE INDEX state_change_log_by_source ON control.state_change_log (source_id, layer, change_id);

UPDATE control.schema_version SET version = 5;

-- The engine writes the two columns of a run with its INSERT, which it has, and cannot change
-- them afterwards: it has no UPDATE on them. It adds a change of state, and can neither change
-- nor delete one.
GRANT SELECT, INSERT ON control.state_change_log TO ingest_control;
