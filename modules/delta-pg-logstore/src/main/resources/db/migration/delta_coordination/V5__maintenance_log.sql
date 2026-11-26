-- What operators did to the tables of the registry (ADR-0004 §3.5, §3.8), for an audit: who
-- asked, why, when, and what each operation found and changed.
--
-- Taking the taint off a table, moving the version it is registered at, or forgetting it
-- overrides a safeguard of the commit path, and who did it and why is what an audit asks. Each
-- row is written by the maintenance role in the transaction that makes the change, so a change
-- and its row are committed together or not at all, and one that is called off or refused
-- leaves none. What a row says it changed is read by the database from the rows it changes, in
-- that transaction. Collecting the temporary files of a table changes nothing of the registry:
-- its row is written once the files are removed.
--
-- No foreign key to the registration: a table that is reset is registered no longer, and its
-- rows stay. Rows are kept: nobody but the owner of the schema changes or deletes one.

CREATE TABLE delta_coordination.maintenance_log (
    entry_id     bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    table_uri    text        NOT NULL CHECK (table_uri ~ '\S'),
    operation    text        NOT NULL
        CHECK (operation IN ('adopt', 'readopt', 'reset', 'collect_temps')),
    requested_by text        NOT NULL
        CHECK (requested_by ~ '\S' AND char_length(requested_by) <= 200),
    reason       text        NOT NULL CHECK (reason ~ '\S' AND char_length(reason) <= 1000),
    detail       jsonb       NOT NULL CHECK (jsonb_typeof(detail) = 'object'),
    made_at      timestamptz NOT NULL DEFAULT now()  -- the database's clock, never a client's
);

-- The maintenance role writes a row, and does nothing else to the log: it neither reads, changes
-- nor deletes one, and it cannot say when a row was made, which the database does. The commit
-- path has nothing of it. An audit reads it as the owner of the schema.
GRANT INSERT (table_uri, operation, requested_by, reason, detail)
    ON delta_coordination.maintenance_log TO ingest_maintenance;

UPDATE delta_coordination.schema_version SET version = 5;
