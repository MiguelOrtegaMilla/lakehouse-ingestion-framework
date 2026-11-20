-- Cluster bootstrap: roles, databases, schemas and database-level privileges.
-- ADR-0003 (placement) and ADR-0013 §3 (least-privilege roles).
--
-- Split of responsibilities:
--   * this script: objects only a superuser or the database owner can create or grant;
--   * Flyway migrations: tables inside the schemas and the grants on them
--     (delta_coordination in P2a, control in P4), run as ingest_migrator.
--
-- Runs once, as the bootstrap superuser, when the data directory is empty.

\set ON_ERROR_STOP on

-- psql reads the passwords from the Docker secrets itself, so they never appear in a process
-- argument list or in the container environment. :'name' renders a quoted SQL literal.
\set pw_migrator     `cat /run/secrets/ingest_migrator_password`
\set pw_control      `cat /run/secrets/ingest_control_password`
\set pw_coordination `cat /run/secrets/ingest_coordination_password`
\set pw_maintenance  `cat /run/secrets/ingest_maintenance_password`
\set pw_sales_owner  `cat /run/secrets/sales_owner_password`
\set pw_sales_ro     `cat /run/secrets/sales_ro_password`

-- Runtime roles. All are plain login roles: no SUPERUSER, CREATEDB, CREATEROLE, REPLICATION
-- or BYPASSRLS (the defaults, checked by the integration tests).
CREATE ROLE ingest_migrator     LOGIN PASSWORD :'pw_migrator';      -- DDL through Flyway only
CREATE ROLE ingest_control      LOGIN PASSWORD :'pw_control';       -- engine: schema control
CREATE ROLE ingest_coordination LOGIN PASSWORD :'pw_coordination';  -- PostgresLogStore
CREATE ROLE ingest_maintenance  LOGIN PASSWORD :'pw_maintenance';   -- pruning, registry reset
CREATE ROLE sales_owner         LOGIN PASSWORD :'pw_sales_owner';   -- demo source: its own owner
CREATE ROLE sales_ro            LOGIN PASSWORD :'pw_sales_ro';      -- JDBC connector, read-only

-- The maintenance database is for the superuser only.
REVOKE CONNECT ON DATABASE postgres FROM PUBLIC;

-- Control plane and commit coordination (ADR-0003). Owned by the superuser, so no runtime
-- role, ingest_migrator included, can create schemas or change database settings.
CREATE DATABASE ingestion_control;
REVOKE ALL ON DATABASE ingestion_control FROM PUBLIC;
GRANT CONNECT ON DATABASE ingestion_control
  TO ingest_migrator, ingest_control, ingest_coordination, ingest_maintenance;

-- Demo source system read by the JDBC connector (P6). Owned by the superuser, as the other
-- database is: the owner of its tables can neither create a schema nor change its settings.
CREATE DATABASE sales;
REVOKE ALL ON DATABASE sales FROM PUBLIC;
GRANT CONNECT ON DATABASE sales TO sales_owner, sales_ro;

\connect ingestion_control
-- Nothing lives in public here.
REVOKE ALL ON SCHEMA public FROM PUBLIC;
-- Owned by the migrator, which creates the tables and grants access to the runtime roles.
CREATE SCHEMA control AUTHORIZATION ingest_migrator;
CREATE SCHEMA delta_coordination AUTHORIZATION ingest_migrator;

\connect sales
-- The source system's own role owns public, and creates and writes the tables in it: neither the
-- data generator nor a test needs the superuser for them. It stands for whoever runs the source
-- system, and the engine never connects as it.
REVOKE ALL ON SCHEMA public FROM PUBLIC;
ALTER SCHEMA public OWNER TO sales_owner;
-- sales_ro can read whatever that owner creates in public, and nothing else: a table that
-- another role creates there is not the source's, and is not granted.
GRANT USAGE ON SCHEMA public TO sales_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE sales_owner IN SCHEMA public GRANT SELECT ON TABLES TO sales_ro;
