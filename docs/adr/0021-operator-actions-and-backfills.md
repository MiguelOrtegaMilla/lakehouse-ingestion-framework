# ADR-0021 — What an operator does to a source is a run, and a backfill is a sequence of its own

| | |
|---|---|
| **Status** | Accepted (P7.1, 2026-11-22) |
| **Amends** | [ADR-0006](0006-source-leases-with-epoch-fencing.md), the key of the lease of a backfill: `<source_id>/<layer>/backfill/<backfill_id>`, the name of its sequence, and not `backfill/<backfill_id>`. The rest of ADR-0006 stands. |
| **Amends** | [ADR-0020](0020-the-watermark-of-an-incremental-load.md), points 6 and 7: what `load.watermark.start` means. The rows at or before it were not the source's to ingest; they are not the regular sequence's to read. `start` is where that sequence begins, and a backfill is not bounded by it (§5, point 4). |
| **Related** | [ADR-0005](0005-batch-wal-and-idempotent-writes.md), [ADR-0007](0007-metadata-format.md), [ADR-0008](0008-layer-responsibilities.md), [ADR-0013](0013-application-security-baseline.md), [ADR-0018](0018-a-run-gives-up-a-batch-whose-source-changed.md), [ADR-0019](0019-a-commit-names-the-epoch-of-its-lease.md), [ADR-0020](0020-the-watermark-of-an-incremental-load.md) |

## Context

Until P7 a source has one way forward: its runs, one batch after another, under one lease. Four things need another, and each was left to P7 by a decision before it:

1. **A batch whose plan cannot be run** is replayed by every run, for ever (ADR-0005 §3). ADR-0018 lets a run give up a batch whose files changed, and leaves the rest to "an operator's command": a directory or a table that the source no longer has, a plan of a form that the application no longer reads.
2. **A configuration that Bronze cannot follow** (another table, another lake, another watermark column, another origin of the rows) is refused, for as long as it differs from the one that the state was last advanced with (ADR-0007, discovery §17, P6.7a). A change that is meant has no way through.
3. **Files that were taken** are never taken again (`processed_file`). A parser that is fixed, or a Bronze that lost what a batch wrote, has nothing that reads them again.
4. **A history too long for one invocation** (a table with years behind it, a source with `load.watermark.start` at the present) can only be read by the regular sequence, one window after another, holding the lease of the source for as long as it takes.

What is common to the four is that they change what the protocol of ADR-0005 rests on: the state of a source, the batches of its sequence, or which sequence writes a table. So they have the same questions as a run: who may do it while a run is in flight, what a run that lost its lease may still write, and what the audit trail says of it.

## Decision

**Whatever an operator does to a source is a run, of a kind of its own, under the lease of the sequence that it acts on. A backfill is a sequence of its own, with its own lease, beside the regular one.**

### 1. An operator action is a run

1. **A run of its own kind**: `abandon`, `reset`, `reprocess` or `backfill`, beside `regular`, in `ingestion_run` and, for those that plan a batch, in `ingestion_batch`. It has what every run has: an id, the instance and the version of the application that ran it, the hash of the configuration it was given, the epoch of its lease, when it started and ended, how it ended. It also records who asked for it and why. *Who* is the user of the operating system that ran the process, as the process reports it (`user.name`): a datum of the audit trail, which says where to ask, and not an identity that anything verified. Anyone who can run the command with the secret of `ingest_control` can say they are anyone. *Why* is a reason, which the command requires.
2. **It takes the lease of the sequence that it acts on**, as a run does (ADR-0006). A lease held by a run that is alive is not taken: the action is recorded as `SKIPPED_LOCKED`, ends in 2, and nothing is changed. It does not wait and does not take a lease over before it expires.
3. **It announces its epoch before it asks a table anything** (ADR-0019). A run that held the lease before, and is still working, commits nothing to any table after that. What a table says of a batch is then true for the action, as it is for a run.
4. **What it changes, it changes in one transaction under its epoch**, with the end of its run and the release of its lease, as `LeasedRun.commit` and `abandon` do. A zombie changes nothing; an action that dies changes nothing.
5. **No role of its own.** The actions run as `ingest_control`, which can do all of it already but write a row of `state_change_log`. A role of its own would enforce nothing that the database can read: the watermark is JSON of its connector, which the control plane does not interpret, so "the state only moves forward" is held by the engine (`last_committed_batch_seq`) and not by a privilege. An operator role with a `SECURITY DEFINER` function is an enterprise feature, and stays out.
6. **The exit codes are those of §14.1, by their meaning**: 0 done, 2 the lease is held, 3 a database or the coordination store did not answer, 4 refused, with nothing changed, 64 a command line that is not one. Each command's cases are written into §14.1 with it.

### 2. Giving a batch up (`abandon-batch`)

1. **It gives up the batch that is planned in a sequence**: the regular one, or that of a backfill (`--backfill`). It names the batch (`--batch N`), which must be the one that is planned: an operator gives up what was looked at, and not what happens to be there.
2. **It asks the table that the batch was planned for** (`ingestion_batch.target_table`), as ADR-0018 does. A batch that the table has is not given up: its rows are in Bronze, and the next run of the source rolls it forward. The action is refused, in 4.
3. **A batch that the table does not have is marked `ABANDONED`**, by the run of the action, with its reason. Its number is not used again; the next run plans the next one, from the state as it is.
4. **It mends a plan that is wrong, and not data that is.** The next plan of a source of files takes the same files again, and a window of a table is the same window. A batch that fails for what its files or rows hold (a header of CSV, a field twice in a record of JSON, a value that its type does not hold) fails again after it is given up. Taking a file out of the landing zone gets past it today; a policy for a file that cannot be read is P8's.

### 3. Resetting the state of a source (`reset-state`)

1. **It sets the watermark of the regular sequence** to a moment (`--watermark`), or forgets it (`--forget`): the next run plans as a source with no watermark does, from `load.watermark.start` or from the beginning of the table. A moment is made into the form of its connector by the connector, which may not have one: a source of files, or a full load, has no watermark to set, and the action is refused.
2. **The configuration that it was given is the one that later runs are compared with** (ADR-0007). The state is compared with the configuration of the run that last advanced it (`source_state.updated_by_run`), and the reset is that run. That is how a change that Bronze cannot follow is let through: it is reset, with a reason, and the audit trail has both configurations.
3. **The sequence goes on.** `last_committed_batch_seq` does not move, and the next batch has the next number. Delta keeps, in each table, the last `txnVersion` of every `txnAppId` that wrote to it, and skips a write that is not beyond it: a sequence that started at 1 again would be skipped by every table it had written, without a word.
4. **Files that were taken stay taken.** Reading them again is `reprocess`, which says which.
5. **It is refused while a batch is planned.** That batch is the next run's to finish, under the configuration it was planned under, or the operator's to give up first. A reset does one thing.
6. **Every reset writes a row of `state_change_log`** (discovery §13.2), in its transaction: the state before and after, the reason, who, and the run.

### 4. Reading files again (`reprocess`)

1. **It plans a batch of the regular sequence with the files of a batch that was committed** (`--batch N`). The batch has the next number, the kind `reprocess`, and the list of files of N as its plan, by their names, sizes and times, as any plan of files (ADR-0005 §2 has reprocessing on the regular sequence).
2. **It is a batch like any other** from there: replayed after a crash, rolled forward, given up by a run when one of its files changed since (ADR-0018). Its commit writes nothing to `processed_file`: the files are taken already, and its plan is the record that they were taken again.
3. **Bronze keeps both copies.** It is appended to, and each copy says its run. Silver keeps one row for each key (ADR-0008).
4. **For sources of files alone.** A window of a table read again on the regular sequence would move its watermark back; that is a backfill of the window. A full load reads the whole table at each run, and has nothing to read again.
5. **It is refused while a batch is planned**, as a reset is.

### 5. A backfill

1. **A backfill reads a range of the watermark column, `(from, to]`, on a sequence of its own.** It is for sources loaded by a watermark alone. A connector says that it can read one (a capability of the contract, which a connector does not have unless it says so); for any other source the command is refused before anything is planned.
2. **Its sequence is `<source_id>/<layer>/backfill/<backfill_id>`**, as ADR-0005 §2 has it: the `txnAppId` of its writes, the key of its lease and the key of its epoch. Its chunks are numbered from 1 in that sequence. **The key of its lease is the name of its sequence**, as the key of the regular lease is the name of the regular sequence, and not `backfill/<backfill_id>` as ADR-0006 had it: an id is then unique within its source, and one name means one thing in the control plane and in the LogStore.
3. **Its id is given by the operator**, in a grammar of its own, with no `/`. The command creates the backfill the first time and resumes it each time after, with the same arguments; other arguments under the same id are refused. A generated id would have an orchestrator that runs the command again create another backfill.
4. **Its range is fixed when it is created.** `from` is before `to`, and `to` is not after the horizon of the source at that moment (its clock less `safety_margin`, ADR-0020): a backfill does not read time that has not settled. `load.watermark.start` does not bound it. `start` is where the regular sequence begins, and a backfill reads what its range says, before `start` too: it is how a history is loaded beside an incremental load that starts at the present.
5. **It is read in chunks**, each a window of ADR-0020 with the same plan, planned by `WatermarkPlanner` with the progress of the backfill as its watermark, `from` as its start, `to` as its horizon, and no lookback. A chunk is as long as `max_batch_range` of the source, or `--chunk`; a backfill with neither is refused, since a history in one statement is what a backfill is for avoiding.
6. **It does not touch the state of the source.** Its progress, the upper bound of its last chunk, is kept with the backfill (`control.backfill`), and moved by the commit of each chunk in its transaction, as the commit of a regular batch moves `source_state`. The backfill is complete when its progress reaches `to`.
7. **It runs beside the regular sequence, on the same tables.** Two leases, two sequences, two epochs: neither waits for the other, and neither changes what the other has done. Both append to Bronze, which the LogStore orders (ADR-0004, ADR-0010), and both may write the quarantine of the source.
8. **A range that the regular sequence has read is read again**, and Bronze has both copies, each with its run. It is warned of, and not refused: whether a copy is wanted is the operator's to say, and Silver keeps one row for each key.
9. **A backfill is held to the configuration it was created with**, by the comparison of ADR-0007 (the changes that Bronze cannot follow), against the snapshot of its first run. A reset of the source does not change it.
10. **The command chains its chunks** as `run` chains its runs (discovery §17, P6.10): a run for each chunk, `--max-batches`, and a signal that ends the invocation after the chunk in course.

### 6. A row says its batch by its run

`_batch_seq` of a row of Bronze, and `batch_seq` of a record of a quarantine, are numbers within a sequence, and a source has more than one: chunk 3 of a backfill and batch 3 of the regular sequence are written to the same tables. **What says which batch a row is of is its run** (`_run_id`, `run_id`): a run has one batch, and `ingestion_run` has its sequence and its number. `QuarantineReader` leaves out the records whose run's batch was given up; whoever reads a quarantine, or Bronze, without it joins on the run.

## Alternatives

| Option | Verdict |
|---|---|
| Runbooks of SQL against the control plane | Nothing fences a run that is still working, nothing is audited but what the operator remembers to write, and the engine's role is the one that runs them. Rejected. |
| An operator mode that changes the control plane without a lease | A run in flight commits its batch after the reset, from the state that was there before it. Rejected. |
| A role of the database for operators | Enforces nothing that `ingest_control` does not already hold, since the database does not read a watermark. An enterprise feature. Not now. |
| A reset that starts the sequence again at 1 | Every table that the sequence wrote skips its writes up to the last number it has, with no error. Rejected. |
| A reset that also forgets the files that were taken | Two decisions in one command, and the one that reads data again is the one that cannot say which. Rejected: `reprocess` says which. |
| Reprocessing on a sequence of its own | ADR-0005 has it on the regular one; a set of files is short, and a lease of its own would buy concurrency that nothing needs. Rejected. |
| Reprocessing a window of a table on the regular sequence | Its commit would move the watermark back, and the state only moves forward. Rejected: it is a backfill. |
| A backfill under the regular lease, one window after another | No backfill beside the incremental load (Type 2), and a history of hours holds the freshness of the source for as long. Rejected. |
| The lease of a backfill under `backfill/<backfill_id>` (ADR-0006) | Ids would have to be unique across sources, and the key of a lease would not be the name of its sequence, as it is for every other. Rejected. |
| A backfill whose id the application makes | An orchestrator that runs the command again creates another backfill. Rejected. |
| A backfill that joins the regular sequence when it reaches its watermark | Two writers of one state. Rejected: each sequence has its own. |
| A column `_txn_app_id` in Bronze and in the quarantine | A change of the schema of every table that exists, before the policies of P9, and one more name that a source may not use. The run says the same with what each row has. Rejected. |
| A backfill bounded by `load.watermark.start` | A backfill exists for the history before it. Rejected. |

## Consequences

- **Every way out of a stuck source is audited.** A batch given up says by whom and why, a reset has the state before and after and both configurations, and a reprocess and a backfill are runs with their batches. The trail of a source can be asked what an operator did to it.
- **Who asked is not authenticated.** The user of the process is recorded as the process says it. What holds an operator to account is who can reach the secret of `ingest_control` and run the command, which is the threat model's to say (P12). An identity that is verified, as a role of the database for each operator, is an enterprise feature.
- **An operator action is fenced as a run is.** A run that lost its lease commits nothing after the action announced its epoch, so a reset, an abandonment or a reprocess cannot be overtaken by work that was in flight.
- **An operator action waits for no run.** A source with a run in flight answers 2; the action is run again once the lease is free, as an orchestrator does with a run.
- **Numbers of a sequence are passed over** by abandonments, as ADR-0018 has it already, and are never used again after a reset.
- **Bronze may hold copies**, by reprocessing and by backfills that overlap the regular sequence. Each copy says its run. Silver keeps one row for each key.
- **`_batch_seq` alone does not name a batch.** Lineage of a row goes through its run, which needs the control plane.
- **`abandon-batch` does not get a source past data that it cannot read.** The open item of §20 is closed for plans that cannot be run, and stays open, for P8 and P9, for data.
- **A backfill makes a second writer of the same tables as a matter of course**, which is the second type of concurrency of §6.2. It is tested as such: two processes, one table, and a kill of the backfill in the middle (P7.8).
- **A connector that can read a range says so.** One that does not is refused a backfill, before anything is planned, and adding a connector still asks nothing of the engine (RNF-04).

## As built (P7.3, 2026-11-22)

- **An operator's action is a run** of the kind `abandon`, `reset` or `reprocess` (migration V5 of `control`), with `requested_by` and `reason`, which the constraints of `ingestion_run` require of those kinds and refuse of a regular run. `OperatorActions`, in `engine`, takes the lease, announces the epoch by the helper that every run uses (`EpochAnnouncement`), and does the action under it. `state_change_log` is made with the migration, and written from P7.4.
- **`abandon-batch`** (§2) gives up the planned batch whose number it is told, in one transaction with the end of its run and the release of its lease (`LeasedRun.abandon`, which takes how the run ends: an operator's run that gives a batch up succeeds).
- **"The table does not have the batch" is said only where it is known** (`BatchLookup`). The table that is asked is the one that the batch was planned for, wherever it is. A directory that is not there may be of a table never made, or on a disk that is not mounted, and the file system does not tell them apart; the coordination store does, since the first commit of a table registers it (`CoordinatedTables`, in the LogStore):
  - a table that is not registered has no batch;
  - a table that is registered is read: its directory and its log must be there and readable, and Delta is asked;
  - anything else, a registered table whose directory is not there, a path whose existence cannot be told, a directory or a log that cannot be listed, a read that fails, is not an answer. Nothing is given up, and the action ends as a transient failure (3): once the table can be read, it is asked again.
- **The limit, as the tests show it:** a file whose records are more rejected than its source allows fails the next batch the same way once its batch is given up.

## As built (P7.4, 2026-11-22)

- **`reset-state`** (§3) takes one of three ways of the watermark: `--watermark <moment>`, `--forget-watermark`, or `--keep-watermark`, which §3 does not name. Keeping it resets what later runs are compared with and nothing else, which is what a source of files needs, having no watermark to set or forget, and what a column renamed at the source or a lake that was moved need. The way is the operator's, and is not checked against the change it lets through; a watermark kept across a change that moved the Bronze table or the watermark column is warned of, each with its risk: the history stays in the old table, and a column that does not mean what the old one did has rows skipped.
- **A moment is made into a watermark by the kind of the source** (`ConnectorKind.watermarkAt`, in `core`, with no I/O): the reset neither opens the source nor resolves its secrets. A JDBC source loaded by a watermark makes it as a commit does (`StoredWatermark`); any other kind and load has none, and `--watermark` is refused. A moment before `load.watermark.start` is refused, as the rows at or before it are a backfill's (§5); a moment that the source has not reached is not, as its clock is not asked. `--forget-watermark` is refused for a state that has no watermark.
- **One transaction** (`LeasedRun.reset`): under the lease and its epoch, with no batch planned and the state as it was read, the watermark is changed and `updated_by_run` made the reset's run, which is how its configuration becomes the one compared with (ADR-0007, P4.4); a row of `state_change_log` has the state before and after; the run ends as succeeded and the lease is released. A source that has no state is refused: it has nothing to reset, and nothing to break.
- **What it lets through** is said with its outcome: the changes that Bronze cannot follow, from the configuration of the state before to its own. The refusal of a run for such a change names the command.

## As built (P7.5, 2026-11-22)

- **The run that reprocesses plans and ingests its batch** (`IngestionService.reprocess`): its outcome is that of a run, and its codes are those of `run`. A run that dies leaves a planned batch that the next run finishes as it finishes any.
- **The batch says what it reads again** (`ingestion_batch.reprocess_of`, migration V6 of `control`): required of a batch of the kind `reprocess` and refused of any other, of its sequence and before it. That it was committed is asked under the lease before the plan is written.
- **Whether a commit takes the files of its plan is the batch's to say**, not the run's: a regular run that finishes a reprocess left halfway takes none either.
- **A source whose batches are no files is told by its connector** (`progress` of the plan of the batch named has no files): the engine names no kind of source.
- **The table is the one that the configuration names now**, so that a reprocess after a reset to another Bronze table gives that table the history that the reset warns of.
