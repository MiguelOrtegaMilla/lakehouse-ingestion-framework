-- A batch that reads the files of another again says which (ADR-0021 §4).
--
-- An operator's reprocess plans a batch of the regular sequence, of the kind `reprocess`, whose
-- plan is the list of files of a batch that was committed before it. The plan says the files,
-- and not the batch that took them first: that is written here, so that what was read again,
-- and from which batch, is asked of the control plane and not of the reason that an operator
-- typed.
--
-- A batch of the kind `reprocess` names one, and no other batch does. The batch it names is of
-- the same sequence, and before it. That the batch named was committed is the engine's to ask,
-- under the lease of the sequence, before the plan is written.
ALTER TABLE control.ingestion_batch
    ADD COLUMN reprocess_of bigint,
    ADD CONSTRAINT reprocess_of_a_reprocess_alone
        CHECK ((run_kind = 'reprocess') = (reprocess_of IS NOT NULL)),
    ADD CONSTRAINT reprocess_of_a_batch_before CHECK (reprocess_of < batch_seq),
    ADD CONSTRAINT reprocess_of_a_batch_of_the_sequence
        FOREIGN KEY (txn_app_id, reprocess_of) REFERENCES control.ingestion_batch (txn_app_id, batch_seq);

UPDATE control.schema_version SET version = 6;

-- No privilege is added. The engine writes the column with the INSERT of the plan, which it
-- has, and cannot change it afterwards: it has no UPDATE on it.
