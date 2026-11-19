# ADR-0019 — A commit names the epoch of its lease, and the LogStore refuses one that is not in force

| | |
|---|---|
| **Status** | Accepted (P5.10, 2026-11-19) |
| **Supersedes** | [ADR-0018](0018-a-run-gives-up-a-batch-whose-source-changed.md), its last two consequences: the known limit of a zombie that writes an abandoned batch, and the tombstone that was to close it in P5.10. The rest of ADR-0018 stands. |
| **Amends** | [ADR-0004](0004-postgresql-coordinated-delta-logstore.md) §3.1, §3.2 and §3.7: the claim of a version has one more condition, and a commit context may hold its commits to it. [ADR-0005](0005-batch-wal-and-idempotent-writes.md) §4 and [ADR-0006](0006-source-leases-with-epoch-fencing.md), consequences: a run that lost its lease no longer commits data that Delta then has to ignore. It commits nothing. |
| **Related** | [ADR-0010](0010-concurrent-write-conflict-handling.md), [ADR-0012](0012-module-structure.md), [ADR-0013](0013-application-security-baseline.md) |

## Context

A lease lets one run at a time advance the state of a source, and its epoch keeps a run that lost it from advancing anything: every transaction of the control plane names the epoch (ADR-0006). The tables of the lake had no such check. A run that lost its lease while it was stalled, and not stopped, could still commit to them. Until now that was left to Delta: a batch is written under its number, and Delta skips a number that a table already has (ADR-0005 §4).

That is enough while a batch can only end by being committed. ADR-0018 gave a batch a second way out: the run that holds the lease gives it up when its source no longer has what the plan names, after asking the table whether it has the batch. The question and the abandonment are in two systems, and a run that lost its lease could write the batch between them, or at any time after:

- Bronze then has the rows of a batch that the control plane says was given up, and the next batch reads the files that did not change again. Those rows are in Bronze twice.
- The same question is what recovery decides by (ADR-0005 §3, step 2). An answer that can change after it was given is a decision made on nothing.

ADR-0018 planned to close it with a tombstone: an empty commit with the transaction of the batch, made before the batch is marked abandoned. P5.10 looked at it, and at a record of intent in the control plane to tell a tombstone from data, and found three things.

1. **A record of intent does not say what is in the table.** A run records "batch N is being given up", and a zombie then commits the data of N before the tombstone. Delta skips the tombstone without a word, since the table has that transaction already (`DeltaCommand.hasBeenExecuted`, read in 4.4.1). The control plane says "intent", the table says "N", and N is data. A recovery that reads that as a tombstone abandons a batch whose rows are in Bronze: the defect that the tombstone was for, in a shorter window.
2. **A table that is not there yet cannot be given a tombstone**, and it is where the first batch of a source is written.
3. **"The table has transaction N" would stop meaning "the data of N is there."** Recovery, the writer of a batch and, later, Silver would each have to tell the two apart, for ever.

What makes the problem tractable here is that this project owns the place where a commit happens. Every commit of every table goes through `PostgresLogStore`, which takes the version in PostgreSQL before the commit exists (ADR-0004 §3.2). That is where a writer can be refused.

## Decision

**A run commits under the epoch of its lease, and the LogStore takes a commit only while that epoch is the one in force. A run that takes a lease announces its epoch before it asks a table anything.**

1. **The LogStore knows epochs, and not leases.** A writer may say under which epoch of which key it commits (`WriterEpoch`). The store keeps the epoch in force under each key (`delta_coordination.writer_epoch`), which only grows. What a key is, is the application's: the engine uses the key of the lease, `<source_id>/<layer>`, and the epoch of the lease. The module still reads nothing of the control plane, and can still be taken out as a library (ADR-0012).
2. **A run announces its epoch as soon as it holds its lease** (`WriterEpochs.announce`), before it looks for a planned batch and before it asks a table whether it has one. If a later epoch is in force already, the run has lost its lease, and stops.
3. **Every write of the run names its epoch**, through its commit context (`CommitContext.forRun(run, epoch)`): the quarantine and the data alike, and whatever a later layer writes.
4. **The claim of a version checks the epoch, in the transaction that takes the version.** It locks the row of the key, shared, reads the epoch in force, and inserts its entry only if that epoch is its own. An announcement updates that row. So a claim and an announcement of one key exclude each other in PostgreSQL, and one of two things is true once an announcement has returned:
   - a commit under an earlier epoch took its version before, and its entry is in the registry, where the next listing of the log finds it and publishes it (ADR-0004 §3.3);
   - or it is refused, now and at every later version.

   A run that has announced its epoch is therefore answered for good by a table, as far as the runs before it go. The question of recovery and what follows it, an abandonment or a roll-forward, need no tombstone to hold.
5. **The epoch must be the one in force, and not only not earlier.** A claim under an epoch that nobody announced is refused too: a writer says its turn before it uses it, and the store holds it to that.
6. **The refusal is an error of its own, and it does not pass** (`EpochNotInForceException`). It is an answer of the store, and not a step that failed: the LogStore does not ask again, and it is not the exception that Delta answers by trying the next version. The engine takes it for what it is, a lease that is lost, and stops the run there, with nothing said to the control plane, where the run that took over has ended it. The process reports a lost lease as it does anywhere else.
7. **A version that a claim took in its turn is its own, whatever is announced afterwards.** A writer whose reply was lost asks again and is told that the commit exists. Told otherwise, it would throw away the bytes of a commit that is made.
8. **A commit that names no epoch is held to none.** Maintenance (`OPTIMIZE`, `VACUUM`, an operator's `DELETE`), a writer that no lease numbers, and a commit outside any context are taken as before.
9. **Giving a batch up stays as ADR-0018 has it, without its limit**: the table is asked, and the batch is abandoned in one transaction of the control plane, under the epoch. No commit is made to any table for a batch that is given up. Recovery keeps the two branches of ADR-0005: a table that has the transaction of a batch has its data.

## Alternatives

| Option | Verdict |
|---|---|
| A tombstone in the table, with a record of intent in the control plane to tell it from data | The intent says what a run meant to do, and not what the table has: the counterexample of the context. It leaves the same defect in a shorter window. Rejected. |
| A tombstone that says what it is in its own commit (a commit with the transaction and no file) | Closes the race for a table that is there. It does not cover a table that is not there yet, nor the quarantine; it cannot be told from data once the log no longer keeps the commit; and every reader of "the table has N" has to know that it may be nothing. Rejected. |
| The LogStore reads `control.source_lease` itself when it takes a version | One statement fewer, and no announcement. It puts the schema of the engine inside the LogStore, requires both schemas in one database, and gives the role of the commit path a privilege in the control plane (ADR-0012, ADR-0013 §3). Rejected. |
| Take a commit whose epoch is the one in force or later | A writer that never announced would be let in, and a zombie with it, until somebody announced. Equality costs nothing, and makes "announce, then write" a rule that the store enforces. Rejected. |
| Hold the epoch with a foreign key, as the generation of a registration is held (ADR-0004 §3.5) | A new epoch would have to rewrite the entries of the earlier ones, or leave them without a key to refer to. An entry is not rewritten. Rejected. |
| Ask the control plane whether the lease is still held, before each write (`LeasedRun.verify`) | It is asked before the commit and not with it: the lease can go between the two. It shortens the time a zombie works, which P5.11 builds, and proves nothing. Not enough alone. |

## Consequences

- **The window of ADR-0018 is closed, for every table.** A batch that was given up is written by nobody afterwards: not to a table that is there, not to one that is not there yet, and not to a quarantine.
- **What a table says of a batch is true for the run that asked**, once that run has announced its epoch. Recovery, the abandonment of a batch and the write of a batch rest on it.
- **"The table has the transaction of batch N" means that the data of N is there**, as ADR-0005 has it. Nothing in the engine knows of tombstones, because there are none.
- **Delta's idempotent write is still there**, and still what makes a batch that is run again add nothing. It is no longer what stands between a zombie and a table.
- **A commit under an epoch costs a transaction of two statements** where it cost one: the lock on the epoch, and the `INSERT`. Claims of one key do not wait for each other. An announcement waits for the claims in flight of its key, which is the point of it.
- **A run that is refused leaves what a write that fails leaves**: data files that no commit names, which `VACUUM` removes, and, for a table that was not there, a registration with no commit, which the next writer of the table uses (ADR-0004 §3.5).
- **The protection is this LogStore's.** A writer that commits without it is not held to an epoch, as it is not held to anything else here: the caveat of ADR-0004, enforced at startup and detected at runtime. A move to another commit coordinator would have to bring an equivalent.
- **The epochs are kept by the coordination store, and numbered by the control plane.** A control plane that is made anew starts its epochs at 1 again, under keys whose epochs in force are later: every run is then refused, which is fail closed. The epochs of those keys are forgotten with the maintenance role (`DELETE FROM delta_coordination.writer_epoch`), as a registration is.
- **A writer of the version before this one is held to no epoch.** Its commits name none, and migration V4 does not refuse them. The processes of an application are stopped while it is upgraded to this version, as for any migration of the coordination schema: one that went on would be a zombie that nothing refuses.
- **An announcement that fails ends the run as a transient failure**, with its lease given up and nothing read or written.
- **A run that died between its lease and its announcement** left nothing: it asked no table and wrote none. The run after it announces a later epoch.
- **What this does not do.** It does not stop the jobs of a run the moment its lease is lost: the run goes on reading until its next commit, or until it asks the control plane. That is P5.11. And it does not order commits of different keys: several sources of one table are the LogStore's as before (ADR-0010).

## As built (P5.10, 2026-11-19)

- **In `delta-pg-logstore`:** `WriterEpoch`, `CoordinationStore.announce`, `Claim.epoch`, `TakeResult.EpochNotInForce`, `EpochNotInForceException`, `CommitContext.forRun(run, epoch)` and `WriterEpochs.announce`, the one entry point of an application.
- **Migration V4 of `delta_coordination`** (the schema is at version 4): `writer_epoch` (`epoch_key`, `epoch`, `announced_at`), with a trigger that refuses an earlier epoch whoever asks; `commit_entry.epoch_key` and `epoch`, for the audit trail of a commit, both or neither. The commit path may insert an epoch and update it, which is also what lets it lock the row; it cannot delete one. Maintenance may delete one, and nothing else.
- **The claim** is `PostgresCoordinationStore.take`: for a claim with an epoch, `SELECT … FOR SHARE` on the row of the key and the `INSERT`, in one transaction at `READ COMMITTED`. The announcement is one `INSERT … ON CONFLICT DO UPDATE`, which locks the row whether or not it changes it, so one that changes nothing has waited for the claims in flight all the same. A claim that names no epoch is the one statement that it was.
- **In `engine`:** `IngestionService` announces the epoch of the lease when a run starts, before `LeasedRun.planned`, and writes inside a context that names it. A commit that is refused for its epoch, in whatever Spark and Delta wrapped it, ends the run as one that lost its lease.
- **Tests.**
  - `CoordinationStoreContract`, for the store in memory and the one in PostgreSQL: an epoch only grows; a claim under the epoch in force, under one that was replaced, under one that nobody announced, and under none; a version taken in its turn and asked for again; and claims and an announcement made at once, twenty times over.
  - `WriterEpochRaceSpec`, on PostgreSQL: both orders of a claim and an announcement, forced with transactions that are left open and read from `pg_stat_activity`. An announcement waits for a claim in flight and then finds its entry; a claim waits for an announcement in flight and is refused. With the lock taken out of the claim, four of its five tests fail.
  - `PostgresLogStoreWriteSpec` and `PostgresLogStoreDeltaSpec`: the refusal is asked once, leaves no temporary file, is not contention, and Delta neither tries it again nor moves to the next version; a table is not made by a writer out of its turn; `OPTIMIZE`, `DELETE`, a `MERGE` and an append that name no epoch go on.
  - `DeltaTableTransactionsSpec`: a commit that took its version and is not published yet is in what a table answers, for a table that is there and for one whose first commit it is.
  - `IngestionServiceSpec`: a run that is taken over between its read and its writes, while its batch is given up, writes nothing to a table that is not there, to one that is, or to its quarantine, and the next batch has each row once. With the epoch taken out of the commit context, those tests fail.
  - `CoordinationSchemaSpec` and `CoordinationSchemaUpgradeSpec`: the privileges of both roles, the trigger, and the entries of version 3 after the migration.
