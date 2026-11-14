-- Commit coordination for PostgresLogStore (ADR-0004 §3.1) and the privileges of the roles that
-- use it (ADR-0013 §3).
--
-- Applied by Flyway as ingest_migrator, which owns schema delta_coordination. The schema and the
-- roles named below come from the cluster bootstrap (docker/postgres/init): creating them takes
-- privileges that no runtime role has. Every name is schema-qualified, so nothing depends on a
-- search_path.

-- The version of this schema, which the LogStore checks before it touches a table (ADR-0004,
-- deviation 10). It is a table of this schema, not the migration tool's history, so that the
-- LogStore neither depends on that tool nor needs to read its bookkeeping. A later migration
-- updates the row.
CREATE TABLE delta_coordination.schema_version (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    version   integer NOT NULL
);

INSERT INTO delta_coordination.schema_version (version) VALUES (1);

-- The tables whose commits are coordinated (safeguard layer 2). A table is registered when it is
-- created through the LogStore, or adopted at the version it has then.
CREATE TABLE delta_coordination.coordinated_table (
    table_uri          text        PRIMARY KEY,  -- canonical table URI (ADR-0004 §3.6)
    delta_table_id     text,                     -- the table's metaData.id, when known
    -- -1 if created through the LogStore; N if adopted at version N. Commits above it must
    -- have an entry.
    registered_version bigint      NOT NULL CHECK (registered_version >= -1),
    registered_at      timestamptz NOT NULL DEFAULT now()
);

-- One row per commit. Inserting the row for a version is what gives it to a writer: the primary
-- key is the mutual exclusion the storage does not provide.
--
-- Only commit files (<version>.json) get an entry, so the version is the key and the file name
-- follows from it. The foreign key makes the database refuse a commit to a table that is not
-- registered, such as one reached through an alias of its path.
CREATE TABLE delta_coordination.commit_entry (
    table_uri       text        NOT NULL REFERENCES delta_coordination.coordinated_table,
    version         bigint      NOT NULL CHECK (version >= 0),
    temp_path       text        NOT NULL,  -- relative to _delta_log: .tmp/<file>.<uuid>
    complete        boolean     NOT NULL DEFAULT false,
    created_at      timestamptz NOT NULL DEFAULT now(),  -- the database's clock, never a client's
    completed_at    timestamptz,
    writer_instance text        NOT NULL,  -- host:pid:spark.app.id
    run_id          uuid,                  -- best effort (ADR-0004 §3.7)
    PRIMARY KEY (table_uri, version),
    CONSTRAINT completed_at_iff_complete CHECK (complete = (completed_at IS NOT NULL))
);

-- A table where a commit made outside the LogStore was detected (safeguard layers 3 and 4).
-- While its row exists, writes to the table are refused.
--
-- A row of its own, not a status column of coordinated_table: the commit path can then be
-- allowed to add the taint without being able to clear it, or to change a registration.
CREATE TABLE delta_coordination.table_taint (
    table_uri  text        PRIMARY KEY REFERENCES delta_coordination.coordinated_table,
    reason     text        NOT NULL,
    tainted_at timestamptz NOT NULL DEFAULT now()
);

-- Privileges. Nothing is granted to PUBLIC, and the migration tool's history table is granted
-- to nobody.

GRANT USAGE ON SCHEMA delta_coordination TO ingest_coordination, ingest_maintenance;
GRANT SELECT ON delta_coordination.schema_version TO ingest_coordination, ingest_maintenance;

-- The commit path (PostgresLogStore). It adds rows and marks an entry complete. It cannot
-- delete anything, rewrite an entry, change a registration or clear a taint, so a defect there
-- cannot erase the record of a commit.
GRANT SELECT, INSERT ON delta_coordination.coordinated_table TO ingest_coordination;
GRANT SELECT, INSERT ON delta_coordination.commit_entry TO ingest_coordination;
GRANT UPDATE (complete, completed_at) ON delta_coordination.commit_entry TO ingest_coordination;
GRANT SELECT, INSERT ON delta_coordination.table_taint TO ingest_coordination;

-- Maintenance and runbooks: pruning entries, clearing a taint, resetting a registration.
GRANT SELECT, DELETE ON delta_coordination.commit_entry TO ingest_maintenance;
GRANT SELECT, DELETE ON delta_coordination.table_taint TO ingest_maintenance;
GRANT SELECT, DELETE ON delta_coordination.coordinated_table TO ingest_maintenance;
