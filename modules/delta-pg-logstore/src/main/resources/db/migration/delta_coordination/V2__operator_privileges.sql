-- What an operator's tools need of the registry (ADR-0004 §3.5), for the role that runs them
-- (ADR-0013 §3).
--
-- V1 let ingest_maintenance delete rows: pruning entries, clearing a taint, forgetting a table.
-- Two operations need more:
--   - adopting a table that was created without the LogStore registers it;
--   - adopting a table again, once an operator has verified its log, moves the version it is
--     registered at, and says when.
-- A registration's key stays out of reach: a table is never renamed in the registry. The commit
-- path gets nothing new, and still cannot change a registration.

GRANT INSERT ON delta_coordination.coordinated_table TO ingest_maintenance;
GRANT UPDATE (registered_version, delta_table_id, registered_at)
    ON delta_coordination.coordinated_table TO ingest_maintenance;

UPDATE delta_coordination.schema_version SET version = 2;
