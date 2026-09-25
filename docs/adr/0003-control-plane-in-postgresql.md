# ADR-0003 — Control plane in PostgreSQL

| | |
|---|---|
| **Status** | Accepted (D2, 2026-09-25) |
| **Related** | [ADR-0004](0004-postgresql-coordinated-delta-logstore.md), [ADR-0005](0005-batch-wal-and-idempotent-writes.md), [ADR-0006](0006-source-leases-with-epoch-fencing.md) |

## Context

The engine keeps operational metadata next to the data it writes:
- incremental state (watermarks, processed files);
- source leases;
- the batch write-ahead log;
- the run audit trail;
- configuration snapshots.

Several instances, possibly on different machines or clusters, read and write it concurrently.

## Decision

The control plane lives in **PostgreSQL** (database `ingestion_control`, schema `control`), with schema migrations managed by Flyway. Data, quarantine and the Delta log stay in the lake.

**This is not a workaround for the local filesystem.** The reasons below hold identically on S3, ADLS or GCS, and in managed platforms:

| Need | Why a transactional RDBMS is the right tool | Why Delta tables are not |
|---|---|---|
| **Leases need compare-and-set** (`UPDATE … WHERE holder IS NULL OR expires_at < now() RETURNING epoch`) | Single-row atomic conditional update, server clock, sub-millisecond | Delta has no row-level CAS; the closest thing is a whole-table optimistic commit, far too coarse for a lock, and conflict-prone under contention |
| **One atomic transaction across watermark + batch + run** | A single `COMMIT` over three rows in three tables | Delta commits are atomic **per table**; three tables mean three commits, so a crash between them leaves the state inconsistent. Multi-table transactions are only emerging through catalog-managed tables. |
| **Operational queries** (joins across runs, batches, leases, freshness views, dashboards) | Indexed, relational, cheap point lookups; Grafana reads it natively | Small-row, high-frequency point reads and updates are the worst workload for a columnar table format (small files, log growth, frequent compaction) |
| **Many concurrent small writers** (every instance writes audit rows) | Row-level concurrency is the RDBMS's core competence | Every audit row would be a commit to a shared table: pure contention |

**Industry precedent.** Airflow (metadata DB), Airbyte (config/state DB on PostgreSQL), Dagster, Hive Metastore, and the catalogs that coordinate table formats (Unity Catalog, Nessie, Polaris) all keep this kind of state in a transactional service, next to the lake rather than inside it.

**Placement.** In the local profile, one PostgreSQL 17 container hosts:
- the source database;
- `ingestion_control`, with schema `control` (this ADR) and schema `delta_coordination` (ADR-0004).

Separate roles:
- the source role is read-only;
- the control role reads and writes `control`;
- the coordination role reads and writes only the coordination tables.

In production these would be separate instances. The coordination store sits on the commit path and needs the stricter availability.

## Alternatives

| Option | Verdict |
|---|---|
| Delta tables for state and audit | Rejected: see the table above. Still fine for *exporting* audit to the lake for analytics (future). |
| Files (one JSON per source) | No CAS, no multi-object atomicity, no queries. Rejected. |
| etcd / ZooKeeper for leases + a DB for audit | Two new infrastructure components for no gain over PostgreSQL. Rejected. |
| Embedded DB (H2/SQLite) | Cannot be shared by several processes or machines. Rejected. |

## Consequences

- There is no single transaction spanning data and state. That is solved by the WAL + idempotent-write protocol (ADR-0005), a trade-off worth being able to explain.
- The audit trail is queried through JDBC/SQL, not Spark. Joined views (`v_table_commits`) link runs to Delta versions.
- PostgreSQL becomes a hard dependency of every run. It already is one, as a source and as the commit coordinator.
