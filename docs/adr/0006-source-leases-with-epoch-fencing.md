# ADR-0006 — Source leases with expiry, heartbeat and epoch fencing

| | |
|---|---|
| **Status** | Accepted (2026-09-25) |
| **Related** | [ADR-0003](0003-control-plane-in-postgresql.md), [ADR-0005](0005-batch-wal-and-idempotent-writes.md) |
| **Amended by** | [ADR-0019](0019-a-commit-names-the-epoch-of-its-lease.md) (P5.10, 2026-11-19): the epoch is also what a commit to a table names, and the LogStore refuses a commit whose epoch is not the one in force. The last consequence below keeps its original text and points to it. [ADR-0021](0021-operator-actions-and-backfills.md) (P7.1, 2026-11-22): the key of the lease of a backfill is the name of its sequence, `<source_id>/<layer>/backfill/<backfill_id>`. The first point of the decision keeps its original text and points to it. |

## Context

Two runs advancing the same incremental state at the same time would corrupt it: two watermarks, double-planned files. This happens routinely with overlapping orchestrator triggers or with automatic retries launched while the original run is still alive.

**Scope.** A lease protects the **state of a (source, layer)**. It is *not* a table lock. Several sources may write the same table concurrently (Type 2); that is the LogStore's job (ADR-0004).

## Decision

A **lease** row per lease key in `control.source_lease`:

- **Keys:** `<source_id>/<layer>` for regular runs; `backfill/<backfill_id>` for backfills. Backfills do not touch incremental state, so they do not contend with the regular lease.

  > **Amended by [ADR-0021](0021-operator-actions-and-backfills.md)** (P7.1, 2026-11-22): the key of the lease of a backfill is `<source_id>/<layer>/backfill/<backfill_id>`, the name of its sequence, as the key of a regular lease is the name of the regular sequence. An id is then unique within its source.

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

*As built (P4.5 and P4.6, reviewed in P4.7, 2026-11-18):* `Leases`, `LeaseHeartbeat` and `LeasedRun` in `engine`, and `LeaseTiming` in `core`.

- **A lease and the run that takes it are one transaction.** The row of the lease is locked while it is looked at, so of the runs that ask at the same moment one is answered and the others wait for it and are recorded as `SKIPPED_LOCKED`. The run whose lease expired is marked `ABANDONED` in that transaction: a status that the decision does not name, for a run that ended halfway and said nothing.
- **Whether a lease has expired is judged by the time at which the transaction started**, which is what `now()` is. A transaction that waited for the row may skip a lease that has just expired. It never takes one early.
- **A renewal that fails is not a lease that is lost.** The decision has a run flagged as fenced when a renewal fails. As built, a renewal is due a third of the time to live after the one before was asked for, and one that fails is tried again every twelfth. A run loses its lease in one of two ways: the database answers that the lease is another's, or the run has seen no renewal succeed for five sixths of the time to live, counted from when the last one that did was asked for.
- **Self-fencing: the run stops by its own count, before the database's runs out.** The database counts the whole time to live from when it got the request, the run five sixths of it from when it sent the request. The count is kept apart from the renewals, so a renewal that hangs does not delay it. The sixth between the two is what their clocks may differ by, and it has three limits, none of which the lease can remove:
  - the database counts by the time of day of its machine, which may be stepped forward: 866 ms at once was measured on a WSL2 machine. A step beyond the margin lets another run take a lease that its holder still counts as good;
  - the run counts by a clock that does not count the time its machine is suspended. A run that is resumed after its lease expired still has time on its count, and learns that the lease is gone at its next renewal, a third of the time to live later at most;
  - a process that is frozen while its machine runs is stopped at the first thing it asks, and not before.

  In all three it is the epoch that keeps the run from advancing anything (ADR-0005 §4). Self-fencing makes the time a zombie works as short as a process can.
- **"Every state-changing statement includes `WHERE epoch = :epoch`" is, as built, a lock** (`Leases.holds`). Each transaction that advances the state starts by locking the row of the lease and asking whether it is its run's under its epoch. That holds for everything the transaction then does, a takeover waits for it to end, and no other table needs a column for the epoch. A lease that has expired and that nobody took is still its holder's: what keeps two runs apart is whose the row is, and no time.
- **The lease is locked first by every transaction that touches both it and a run.** The review of P4 found one that did not: a run that ended itself and then gave its lease up held the two rows in the order opposite to a takeover's, and PostgreSQL ended one of them as a deadlock.
- **Nothing around the heartbeat may stop it.** Whoever asked to be told that the lease is lost is told before anything is logged, nothing is logged while the state of the heartbeat is held, a log line that fails is only missing, a task that ends in an error says so, and closing a heartbeat interrupts none of its threads. The first version did none of this, and one interrupt in the middle of the first log line of a process left it with no logging and a heartbeat that told nobody anything.
- **`leases.ttl`** is `10s` to `1d`, `2m` by default, and at least three times the connect and statement timeouts of the control database: a renewal that takes as long as it may still arrives in time.

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

  > **Amended by [ADR-0019](0019-a-commit-names-the-epoch-of-its-lease.md)** (P5.10, 2026-11-19): from P5.10 they are refused where a commit is made. The epoch of a lease is announced to the LogStore by the run that takes the lease, and named by every commit of that run; a commit under an earlier epoch takes no version. The fencing token is checked by the control plane for the state, as above, and by the LogStore for the tables.
