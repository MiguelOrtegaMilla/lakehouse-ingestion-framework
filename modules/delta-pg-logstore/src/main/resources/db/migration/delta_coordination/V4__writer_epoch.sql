-- The epoch that a writer commits under, which the claim of a version checks (ADR-0019).
--
-- An application that lets one writer at a time do some work, as the engine does with the lease
-- of a source, numbers its writers: each one that takes the work over has an epoch greater than
-- the one before. Until now nothing held a writer to that here. One that had lost its turn and
-- was stalled rather than stopped went on afterwards, and took a version of a table with data
-- that the application had already decided was not written.
--
-- From here on a writer may say under which epoch of which key it commits. The epoch in force
-- under a key is announced by whoever takes the work over, before it asks a table anything, and
-- a claim that names an epoch takes its version only while that epoch is the one in force. The
-- check is part of the transaction that takes the version: it locks the row of the key, so a
-- claim and an announcement of the same key exclude each other. Either the claim takes its
-- version first, and the announcement returns once the commit is recorded, where the next
-- reader of the table finds it; or the announcement is first, and the claim is refused.
--
-- A commit that names no epoch is taken as before: maintenance, and writers that no such
-- application numbers.

-- The epoch in force under each key. What a key is, is the application's: for the engine, the
-- key of a lease (<source_id>/<layer>).
CREATE TABLE delta_coordination.writer_epoch (
    epoch_key    text        PRIMARY KEY CHECK (epoch_key ~ '\S'),
    epoch        bigint      NOT NULL CHECK (epoch >= 1),
    announced_at timestamptz NOT NULL DEFAULT now()  -- the database's clock, never a client's
);

-- An epoch only grows, whoever asks and whatever it sets: an earlier one in force would let
-- back in the writers that the later one shut out.
CREATE FUNCTION delta_coordination.refuse_earlier_epoch() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'the epoch in force under % is %, and an epoch only grows: % is refused',
        OLD.epoch_key, OLD.epoch, NEW.epoch
        USING ERRCODE = 'check_violation';
END;
$$;

-- Nobody calls it: it runs as the trigger of the table, whoever updates it.
REVOKE ALL ON FUNCTION delta_coordination.refuse_earlier_epoch() FROM PUBLIC;

CREATE TRIGGER writer_epoch_only_grows
    BEFORE UPDATE ON delta_coordination.writer_epoch
    FOR EACH ROW WHEN (NEW.epoch < OLD.epoch)
    EXECUTE FUNCTION delta_coordination.refuse_earlier_epoch();

-- The epoch that an entry was taken under, for the audit trail of a commit: none, for a commit
-- that named none, and for every entry made before this migration.
ALTER TABLE delta_coordination.commit_entry
    ADD COLUMN epoch_key text,
    ADD COLUMN epoch     bigint,
    ADD CONSTRAINT epoch_whole_or_not_at_all CHECK ((epoch_key IS NULL) = (epoch IS NULL));

-- The commit path announces an epoch and reads the one in force, and locks its row while it
-- takes a version, which takes the privilege to update a column of it. It cannot delete a row,
-- nor rename a key. Inserting an entry with its epoch is the INSERT that it already has.
GRANT SELECT, INSERT ON delta_coordination.writer_epoch TO ingest_coordination;
GRANT UPDATE (epoch, announced_at) ON delta_coordination.writer_epoch TO ingest_coordination;

-- Maintenance forgets the epochs of a key, as it forgets a registration: when the application
-- that numbers its writers starts its numbers again, as a control plane that is made anew does.
GRANT SELECT, DELETE ON delta_coordination.writer_epoch TO ingest_maintenance;

UPDATE delta_coordination.schema_version SET version = 4;
