# ADR-0006 — Source leases with expiry, heartbeat and epoch fencing

| | |
|---|---|
| **Status** | Accepted (2026-09-25) |
| **Related** | [ADR-0003](0003-control-plane-in-postgresql.md), [ADR-0005](0005-batch-wal-and-idempotent-writes.md) |

## Context

Two runs advancing the same incremental state at the same time would corrupt it: two watermarks, double-planned files. This happens routinely with overlapping orchestrator triggers or with automatic retries launched while the original run is still alive.

**Scope.** A lease protects the **state of a (source, layer)**. It is *not* a table lock. Several sources may write the same table concurrently (Type 2); that is the LogStore's job (ADR-0004).

## Decision

A **lease** row per lease key in `control.source_lease`:

- **Keys:** `<source_id>/<layer>` for regular runs; `backfill/<backfill_id>` for backfills. Backfills do not touch incremental state, so they do not contend with the regular lease.
- **Acquire** with one atomic statement, using the **PostgreSQL clock**:

  ```sql
  UPDATE control.source_lease
     SET holder_run_id = :run, instance_id = :inst, epoch = epoch + 1,
         acquired_at = now(), expires_at = now() + :ttl
   WHERE lease_key = :key AND (holder_run_id IS NULL OR expires_at < now())
  RETURNING epoch;
  ```

  (The row is created on first use with `INSERT … ON CONFLICT DO NOTHING`.)
- **Heartbeat:** a daemon thread renews `expires_at` at TTL/3, conditional on `holder_run_id = :run AND epoch = :epoch`. If a renewal fails (lease lost), the run is flagged as fenced and aborts before its state commit.
- **Busy lease:** the run is recorded as `SKIPPED_LOCKED` and exits with code 2. It does not wait; retrying is the orchestrator's decision.
- **Fencing token:** every state-changing statement includes `WHERE epoch = :epoch`, so a zombie that lost its lease cannot advance state (ADR-0005 §4).
- **Release:** a normal end clears the holder. A crash leaves the lease to expire, so there are no orphan locks.

## Alternatives

| Option | Verdict |
|---|---|
| `pg_advisory_lock` | Bound to one JDBC connection: if the connection drops while the process lives, the lock vanishes silently. No fencing token, nothing auditable. Rejected. |
| Row lock (`SELECT … FOR UPDATE`) held for the whole run | Holds a transaction open for minutes; same connection-binding problem. Rejected. |
| ZooKeeper / etcd | Extra infrastructure for what one table does. Rejected. |
| Rely on the orchestrator to prevent overlaps | Common in enterprise frameworks, but it pushes correctness outside the engine and fails on manual reruns. Rejected. |

## Consequences

- Exactly one effective executor per (source, layer) across instances, machines and clusters (server clock, shared DB).
- The TTL must be much longer than the longest pause (GC, network) and shorter than the acceptable takeover delay. Default 2 minutes with a 40-second heartbeat, configurable.
- Residual zombie *data* writes are neutralised by Delta's idempotent writes (ADR-0005), not by the lease itself.
