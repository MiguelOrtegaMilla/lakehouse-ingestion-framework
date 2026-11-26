-- A run that failed for what nothing classified: a mistake of the application (ADR-0022 §3).
--
-- Such a run used to be left as a process that died leaves one, RUNNING until the run that
-- took its lease over marked it ABANDONED. It ends now as FAILED, with the class `internal`, and
-- gives its lease up; the process ends in 70. What its error_message holds is the type of the
-- exception and where it was thrown, and not what the exception said: the exception is whole in
-- the log of its instance.

ALTER TABLE control.ingestion_run
    DROP CONSTRAINT ingestion_run_error_class_check,
    ADD CONSTRAINT ingestion_run_error_class_check
        CHECK (error_class IN ('transient', 'conflict', 'permanent', 'data', 'internal'));

UPDATE control.schema_version SET version = 8;
