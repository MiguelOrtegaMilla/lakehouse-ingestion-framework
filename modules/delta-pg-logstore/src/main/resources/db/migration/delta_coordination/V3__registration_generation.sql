-- A generation for every registration, which the claim of a version checks (ADR-0004 §3.5).
--
-- An operator adopts a table again, or resets the registry of one that was dropped, while its
-- writers are stopped. Until now nothing held a writer to that. One that had looked at the
-- registry before the operator's change, and was stalled rather than stopped, went on afterwards
-- with what it had seen, and took a version of a table that was no longer the one it had checked.
--
-- From here on a registration has a generation: a number that no registration had before, given
-- by the database when a table is registered and again whenever an operator adopts it. An entry
-- records the generation its writer read, and the foreign key below makes the database refuse an
-- entry whose generation is not the registration's any more. The check is part of the INSERT
-- that takes the version: an INSERT that meets an operator's change in flight waits for it, and
-- is refused if it commits.

-- GENERATED ALWAYS: nobody chooses a generation. An INSERT gets the next one, and an UPDATE can
-- only ask for the next one (SET generation = DEFAULT). No role needs a privilege on the
-- sequence behind it.
ALTER TABLE delta_coordination.coordinated_table
    ADD COLUMN generation bigint GENERATED ALWAYS AS IDENTITY;

-- What the entries refer to. A change of generation is therefore a change of a referenced key:
-- it takes the row lock that the foreign key check of a concurrent INSERT waits for.
ALTER TABLE delta_coordination.coordinated_table
    ADD CONSTRAINT coordinated_table_generation UNIQUE (table_uri, generation);

-- The entries that exist were taken under the registrations that exist.
ALTER TABLE delta_coordination.commit_entry ADD COLUMN generation bigint;

UPDATE delta_coordination.commit_entry e
SET generation = c.generation
FROM delta_coordination.coordinated_table c
WHERE c.table_uri = e.table_uri;

ALTER TABLE delta_coordination.commit_entry ALTER COLUMN generation SET NOT NULL;

-- It replaces the foreign key on table_uri alone, and refuses what that one refused: an entry
-- for a table that is not registered. ON UPDATE CASCADE: the entries an operator keeps when a
-- table is adopted again belong to its new registration.
ALTER TABLE delta_coordination.commit_entry
    ADD CONSTRAINT commit_entry_registration
    FOREIGN KEY (table_uri, generation)
    REFERENCES delta_coordination.coordinated_table (table_uri, generation)
    ON UPDATE CASCADE;

ALTER TABLE delta_coordination.commit_entry DROP CONSTRAINT commit_entry_table_uri_fkey;

-- Adopting a table again gives it a new generation. The commit path gets nothing: it cannot
-- change a generation, nor lock a registration.
GRANT UPDATE (generation) ON delta_coordination.coordinated_table TO ingest_maintenance;

UPDATE delta_coordination.schema_version SET version = 3;
