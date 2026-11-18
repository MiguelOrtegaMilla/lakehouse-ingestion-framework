-- The files that batches have taken (discovery §13.2, P5.3): what makes a file source take each
-- file once. A file is known by its path, its size and when it was last changed; one written
-- again under its path is another file, and is taken again (ADR-0008: Bronze keeps what was
-- received, and Silver deduplicates).
--
-- A row is written by the commit of the batch that took the file, in the transaction that marks
-- the batch COMMITTED and moves the state of its source (ADR-0005 §3, step 7): a file is
-- processed exactly when the batch that read it is. Before that, the file is named in the plan
-- of its batch, and a run that finds the batch planned replays that plan.
--
-- Its status and the error of a file that could not be read come with the policy for such
-- files (P8), in a migration of their own.
CREATE TABLE control.processed_file (
    source_id   text        NOT NULL,
    path        text        NOT NULL,  -- relative to the landing root: <directory>/<name>
    size        bigint      NOT NULL CHECK (size >= 0),
    modified_at timestamptz NOT NULL   -- as the file system reports it, to the millisecond
        CHECK (date_trunc('milliseconds', modified_at) = modified_at),
    txn_app_id  text        NOT NULL,
    batch_seq   bigint      NOT NULL,
    PRIMARY KEY (source_id, path, size, modified_at),
    FOREIGN KEY (txn_app_id, batch_seq) REFERENCES control.ingestion_batch
);

-- The files that a batch took: what its lineage asks, from a batch to its input (P11).
CREATE INDEX processed_file_by_batch ON control.processed_file (txn_app_id, batch_seq);

UPDATE control.schema_version SET version = 2;

-- The engine adds a row and reads them. It neither changes nor deletes one: that a file was
-- taken is part of the audit trail, and taking it again is a decision of an operator (P7).
GRANT SELECT, INSERT ON control.processed_file TO ingest_control;
