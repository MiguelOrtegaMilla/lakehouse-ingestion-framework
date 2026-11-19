# ADR-0018 — A run gives up the batch that its source no longer has as planned

| | |
|---|---|
| **Status** | Accepted (P5.8, 2026-11-19) |
| **Amends** | [ADR-0005](0005-batch-wal-and-idempotent-writes.md) §3: a planned batch had one way out, its commit, and an operator's hand. It gains a second, taken by the run that holds the lease. The rest of ADR-0005 stands: a plan is immutable, and a `batch_seq` maps to one plan for ever. |
| **Related** | [ADR-0006](0006-source-leases-with-epoch-fencing.md), [ADR-0008](0008-layer-responsibilities.md), [ADR-0010](0010-concurrent-write-conflict-handling.md) |

## Context

ADR-0005 writes the plan of a batch down before anything is read, and replays it exactly until it is committed. That rests on what a connector guarantees: a read of a plan is the same each time, *for as long as the source still has what the plan names*.

A file source cannot hold its producer to that. Between the plan and the read a file may be written again, or deleted. The read finds out (P5.7): each file must still be the file that was planned, by its size and time before Spark reads it, and by its device and inode after. A batch that fails that check is not read, because what would reach Bronze is not what the plan says was read, nor what `processed_file` would record.

With ADR-0005 alone, that batch stays `PLANNED`. Every run of the source finds it, replays it, and fails the same way, until an operator gives the batch up, with a command that comes in P7. So one file written twice by its producer stops a source, and nothing in the system caused it or can prevent it:

- `connector.min_file_age` (a minute by default) keeps a file out of a plan until it has gone unchanged for that long. It is a guess about the producer, not a promise from it.
- Taking a file out of the producer's reach before it is planned, or storage whose objects never change, would close it. Neither is this project's landing zone (ADR-0008, E2).

## Decision

**The run that holds the lease gives up a batch whose plan the source no longer has, when nothing of the batch is in its tables. The next run plans anew.**

1. **A connector says it with a type of its own.** When a read finds that the source no longer has what the plan names, as the plan names it, the connector raises a signal of the connector contract that names each thing that changed and how. It is not a class of failure: it is a fact about the plan, and the engine decides what follows. Every other failure of a read keeps its class.
2. **The engine asks the table before it gives anything up.** It asks the table that the batch writes last for the last `txnVersion` of the batch's `txnAppId`, which is the question of recovery (ADR-0005 §3, step 2):
   - **The batch is not in the table.** It is given up: marked `ABANDONED`, with the run that gave it up, the time, the reason, and what of the plan changed; the run ends `FAILED`; the lease is released. All of it is one PostgreSQL transaction, conditional on the epoch of the lease, as the commit of a batch is. A zombie gives nothing up.
   - **The batch is in the table.** Nothing is given up and nothing is changed. A batch whose data is written is rolled forward, never abandoned, and the run that finds it written while it was reading it has met another writer of its own sequence, or a defect. It ends as an error that nothing classified (exit code 70 from P8), and the next run recovers the batch as the table says.
3. **Nothing of an abandoned batch is accounted for.** Its files are not recorded as taken and the state of its source does not move. Its plan stays in the write-ahead log as it was written, and its number is not used again: the next batch has the next number, which Delta takes (a `txnVersion` only has to be greater than the last), and the state only has to move forward.
4. **The next run plans from what the source has by then.** A file that was written again is another file, by its size and time, and is planned as a new file (P5.2). The files of the abandoned plan that did not change are planned again. A file that is gone is not planned.
5. **The run that gives a batch up ends as a transient failure, exit code 3.** What an orchestrator does with 3 is what is wanted: run it again later, and alert if it goes on. It is not 0, because the run ingested nothing, and a producer that keeps writing its files again has to be seen. It is not 4, because running again does mend it.
6. **An operator's command to give a batch up stays** (P7): for a plan that cannot be run for any other reason, and to reset or reprocess a source. It is no longer the only way out of a file that changed.

## Alternatives

| Option | Verdict |
|---|---|
| Leave it to the operator (ADR-0005 as written, with the command of P7) | The availability of a source depends on a person, for something its producer does in the ordinary course. Rejected. |
| Read the files as they are now, under the same batch | The plan would say one thing was read and Bronze would have another; a replay after a crash could write a third. It breaks "a `batch_seq` maps to exactly one plan". Rejected. |
| Rewrite the plan of the batch in place | The same break, made in the write-ahead log itself. The role of the engine cannot update a plan, by design (ADR-0013 §3). Rejected. |
| Drop the changed files from the batch and commit the rest | A partial commit of a plan: `processed_file` would have to record some of a plan's files and not others, and a replay would have to know which. A new batch does the same with no new state. Rejected. |
| Give the batch up without asking the table | A batch that a zombie wrote would be abandoned with its rows in Bronze, and read again by the next. Rejected. |
| Copy every file into a staging area before it is planned | Closes the problem at its root, at the cost of a second copy of every file and of a landing zone the engine writes to. It is the object-store profile's answer (E2), not this one's. |

## Consequences

- A source is not stopped by a file that its producer writes again or removes. It loses one run, which ends in 3.
- **`ABANDONED` has two authors**: a run, automatically, and an operator (P7). The batch says which, with who, when and why. The audit trail of a source can be asked how often its producer changes files under it.
- **Numbers of a sequence may be passed over.** Nothing may assume that the batches of a `txnAppId` are consecutive; the simulation of Delta and the real one already take a number that was passed over (P5.4).
- **A producer that changes its files on every run gets a run that fails in 3 every time.** That is the alert working. Raising `min_file_age` is the first answer.
- **Known limit: a zombie that writes the abandoned batch after it was given up.** The question to the table and the abandonment are in two systems, and nothing makes them one. A run that lost its lease, had read the files before they changed, and is still running may commit batch N to Bronze after N was abandoned:
  - Bronze then has the rows of a batch that the control plane says was given up, and the next batch reads the unchanged files of that plan again: those rows are in Bronze twice, under two batch numbers.
  - The window closes when the next batch is committed to the table: Delta skips any `txnVersion` that is not beyond the last.
  - Self-fencing and the check of the lease before a write (P5.11) make it narrower. Silver keeps one row per key (ADR-0008), so the copy does not go further than Bronze. The audit trail does not account for it.
- **To be closed in P5.10, with a tombstone in the table.** Before a batch N is marked abandoned, the engine commits to its table an empty transaction that carries `txn (txnAppId, N)`, so that a write of N that comes later is skipped by Delta. What P5.10 has to settle first:
  - a table that does not exist yet, which an empty commit cannot be made to;
  - whether Delta takes a commit that has a transaction and no data;
  - how recovery tells a tombstone from the data of a batch. A run that died between the tombstone and the abandonment leaves a batch that is `PLANNED` and whose number is in the table: today that reads as "written", and rolling it forward would record its files as taken with none of their rows in Bronze.
- **The replay of a batch that a run inherits gives it up the same way** (P5.10): a file that changed fails the read of whoever replays the plan, and the same question to the table decides.
