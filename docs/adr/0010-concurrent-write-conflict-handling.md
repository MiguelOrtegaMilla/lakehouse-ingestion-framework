# ADR-0010 — Handling concurrent write conflicts on shared Delta tables

| | |
|---|---|
| **Status** | Accepted (consequence of D3, 2026-09-25) |
| **Related** | [ADR-0004](0004-postgresql-coordinated-delta-logstore.md), [ADR-0005](0005-batch-wal-and-idempotent-writes.md), [ADR-0008](0008-layer-responsibilities.md) |
| **Superseded in part** | [ADR-0022](0022-the-classification-of-failures-and-the-retries-of-a-run.md) (P8.1, 2026-11-26) replaces §2, the retry policy: a budget of time from the profile, in place of a count of attempts. §2 keeps its original text and points to it. |

## Context

With Type 2 concurrency, several processes commit to the same Delta table:
- fan-in sources;
- a backfill alongside the incremental run;
- `OPTIMIZE` alongside ingestion.

Conflicts happen at two levels, and they must not be confused:

| Level | What happens | Who handles it |
|---|---|---|
| **Physical**: two writers try to create the same version N | The LogStore's `INSERT` for `(table, N)` fails for the loser → `FileAlreadyExistsException` | **Delta internally**: `OptimisticTransaction` checks the winning commits for logical conflicts; if there are none, it retries at N+1 transparently. Counted as `commit_contention`. |
| **Logical**: a concurrent commit invalidates what our transaction read or deleted | Delta raises a `DeltaConcurrentModificationException` subclass | **The engine**, per the classification below. Counted as `write_conflicts`. |

Blind appends (Bronze) have no read set, so under Delta's default `WriteSerializable` isolation they **never** conflict logically with each other. Fan-in to Bronze only produces physical contention, which Delta absorbs.

## Decision

### 1. Classification of Delta conflict exceptions (`io.delta.exceptions.*`, verified in 4.4.0)

| Exception | Meaning | Class | Engine reaction |
|---|---|---|---|
| `ConcurrentAppendException` | Files were added to data our transaction read (`MERGE`/`UPDATE`/`DELETE`) | **Retry** | Re-run the whole operation on a fresh snapshot |
| `ConcurrentDeleteReadException` | Files we read were removed (e.g. by `OPTIMIZE` or another `MERGE`) | **Retry** | Same |
| `ConcurrentDeleteDeleteException` | We and another transaction removed the same files | **Retry** | Same |
| `ConcurrentWriteException` (generic) | Other write–write conflict | **Retry** | Same |
| `MetadataChangedException` | Concurrent metadata change: typically **schema evolution by another writer** | **Re-plan** | Reload the table schema, re-evaluate the source's schema policy (P9) against it. Compatible → retry; policy says `fail` → permanent. |
| `ProtocolChangedException` | Concurrent protocol / table-feature upgrade | **Re-plan** | Check that this client supports the new protocol → retry once; otherwise permanent. |
| `ConcurrentTransactionException` | Another transaction committed with **our `txnAppId`** concurrently | **Identity** — never blind-retried | This means two writers with the same identity: a zombie, or a coordination bug. Go to recovery: if the table's `txn` version is `≥ batch_seq`, roll forward (the other writer committed the same immutable plan); otherwise fail permanently and alert. |
| LogStore `IOException` (coordination unavailable) | PostgreSQL unreachable | **Transient** | Backoff retry of the step. **Never** fall back to an uncoordinated write. |
| LogStore integrity errors (`TAINTED`, unregistered table, content mismatch) | Uncoordinated writer or misconfiguration | **Permanent** | Fail (exit 4), alert, runbook |

### 2. Retry policy

- Bounded attempts (default 5), exponential backoff with **full jitter** (base 500 ms, cap 30 s), so that colliding writers do not retry in lockstep.
- A retry re-executes the **whole Delta operation**, re-reading the snapshot and recomputing the `MERGE` source. This is safe because every operation is idempotent: `txnAppId`/`txnVersion` for appends and `MERGE` (ADR-0005), and the plan is immutable.
- On exhaustion: transient failure (exit 3); the state is untouched (WAL batch stays `PLANNED`), so the next run replays it.

> **Superseded by [ADR-0022](0022-the-classification-of-failures-and-the-retries-of-a-run.md)** (P8.1, 2026-11-26): the budget of a run is one of time, from the profile (`retries.budget`, `first_backoff` and `max_backoff`), and not a count of attempts. It is shared with the failures that pass, and a retry resumes the run where it is durable: until its batch is committed, at step 2 of ADR-0005, which runs the whole Delta operation again on a fresh snapshot as this section has it.

### 3. Reducing `MERGE` conflicts in Silver fan-in tables

- A fan-in Silver table is **partitioned by the column that separates its writers** (typically `_source_id`, or a business partition the sources already own, such as region).
- The engine puts that partition **literally in the `MERGE` condition** (`t._source_id = 'crm_es' AND t.<key> = s.<key>`). Delta's conflict detection then sees disjoint read sets, so writers on different partitions do not conflict. Without the literal predicate, a `MERGE` is treated as reading the whole table, and any concurrent append conflicts.
- **Config validation rule (generic, not business logic):** for a table written by more than one source, the key must include the partition column. Overlapping key spaces across sources would be a business-level merge rule, which is out of scope for the framework.
  - *As built (P3.5, 2026-11-17):* `Configuration` in `core`. Such a table must be partitioned, and the same way by every source that writes it. `_source_id` is the engine's own column and counts as part of every key without being listed in one; any other partition column must be among the keys of each source. A table that one source writes is partitioned as that source likes.
- **Maintenance:** `OPTIMIZE` is scoped by partition (`WHERE`) and scheduled away from active partitions where possible. Its conflicts with `MERGE` are classified as *retry*.

### 4. Silver cursor = Bronze Delta version

With several writers, `batch_seq` order ≠ commit order. Silver therefore tracks `last_applied_bronze_version` and reads Bronze by version range (ADR-0005 §5). The version order is total because every commit of the table is serialised by the LogStore.

### 5. Session isolation

Delta's DML idempotence confs are session-scoped. Every concurrent task (the in-JVM parallel runner, P11) runs in its own `SparkSession` from `spark.newSession()`.

## Consequences

- Correctness does not depend on avoiding conflicts, only on classifying them. Partitioning is a *performance* measure that reduces retries.
- Two separate, real metrics per run: `commit_contention` (physical) and `write_conflicts` (logical).
- `ConcurrentTransactionException` becomes an alarm for coordination bugs, a second line of defence behind leases.
