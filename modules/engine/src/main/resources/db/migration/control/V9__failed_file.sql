-- A file that a batch could not read, under a source that skips such files (ADR-0023).
--
-- Until now a row of processed_file was a file that a committed batch took, and nothing else.
-- Under `connector.on_unreadable_file: skip`, the run that finds files of its plan that cannot
-- be read gives the batch up, and records each such file here as FAILED, with why, in the
-- transaction that gives the batch up (ADR-0018, as ADR-0023 amends it). The next plan leaves
-- out every file that this table has, taken or failed; a file written again is another file,
-- by its size and time, and is planned as a new one.
--
-- A file that was taken names the committed batch that took it, and one that failed the batch
-- that was given up for it. Every row of before is a file that was taken.
--
-- Why the error holds no text of an exception: such a text may hold what the file held. The
-- engine writes what Spark said of the file by its condition and the type of its cause, or what
-- the landing zone found (a file outside the root, a link to nothing, not a file).
ALTER TABLE control.processed_file
    ADD COLUMN status text NOT NULL DEFAULT 'TAKEN' CHECK (status IN ('TAKEN', 'FAILED')),
    ADD COLUMN error  text,  -- why a file failed, for a person: none for a file that was taken
    ADD CONSTRAINT error_iff_failed CHECK ((status = 'FAILED') = (error IS NOT NULL)),
    ADD CONSTRAINT error_says_something CHECK (error IS NULL OR error ~ '\S');

-- The rows of before are taken. From now on a row says which it is: an insert that does not is
-- refused, rather than taken for a file that was taken.
ALTER TABLE control.processed_file ALTER COLUMN status DROP DEFAULT;

UPDATE control.schema_version SET version = 9;

-- The engine adds a row and reads them, as before (V2): it inserts the status and the error
-- with the rest of the row, and neither changes nor deletes one.
