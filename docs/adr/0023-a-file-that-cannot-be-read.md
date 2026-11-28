# ADR-0023 — A file that cannot be read stops its source, or is skipped and recorded, as the source says

| | |
|---|---|
| **Status** | Accepted (P8.7a, 2026-11-27) |
| **Amends** | [ADR-0018](0018-a-run-gives-up-a-batch-whose-source-changed.md) §3 and §5, for a batch given up under `skip`: its files that cannot be read are recorded, and the run that gives it up ends well. For a batch whose source changed, ADR-0018 stands as it is. |
| **Amends** | [ADR-0008](0008-layer-responsibilities.md), the errors of the landing zone ("unreadable file → `FAILED` + policy"): a file is recorded as `FAILED` under `skip` alone. Under `fail` it is named by the run that fails, and nothing of it is recorded as taken or failed. |
| **Amends** | [ADR-0013](0013-application-security-baseline.md) §2, a file outside the root: it is refused when it is listed under `fail`, as it is today, and under `skip` it is planned and recorded as `FAILED` without ever being opened (§2, §6). |
| **Related** | [ADR-0005](0005-batch-wal-and-idempotent-writes.md), [ADR-0006](0006-source-leases-with-epoch-fencing.md), [ADR-0007](0007-metadata-format.md), [ADR-0021](0021-operator-actions-and-backfills.md), [ADR-0022](0022-the-classification-of-failures-and-the-retries-of-a-run.md) |

## Context

A file that Spark cannot read fails its batch, whatever the session is set to (P5.14): no file is passed over without a word. That is all the engine does with one. The batch stays planned, and every run of the source replays its plan and fails the same way (ADR-0005), so one file that its producer cut short stops its source until somebody takes it out of the landing zone. `abandon-batch` does not mend it: the next plan takes the same file (ADR-0021 §2, point 4). Nothing classifies the failure either: since P8.3 such a run ends as `internal`, in 70, as a mistake of the application, though nothing is wrong with the application.

A file that the landing zone refuses to open stops its source the same way, a step earlier: a symbolic link out of the root, a link to nothing, a pipe or a device fail the listing as permanent, and nothing is planned (ADR-0013 §2).

ADR-0008 and ADR-0013 have the answer in a line: such a file is recorded as `FAILED`, and the source says what follows, `fail` or `skip`. What a file that cannot be read is, how a run finds it, and what each policy does to the batch that has it are this ADR's.

### What Spark 4.1 does with such a file

A read of the file connector has Spark read the files of a batch twice: once to infer its columns (the header of the first file of CSV, every record of JSON, the footer of every file of Parquet), and once to read its rows, which are kept apart from the files (`localCheckpoint`). `SparkUnreadableFileContractSpec` breaks real files and reads them as the connector does:

| File | Where its batch fails | What Spark raises |
|---|---|---|
| Parquet cut short | The inference, wherever the file is in the batch | `FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER`, under a job aborted with no condition |
| Parquet with its footer whole and its pages written over | The checkpoint | `FAILED_READ_FILE.NO_HINT`, from an `IOException` |
| CSV in gzip cut short, after the first file of the batch | The checkpoint (the header of the first file is whole) | `FAILED_READ_FILE.NO_HINT`, from an `EOFException` |
| JSON in gzip cut short | The inference | `FAILED_READ_FILE.NO_HINT`, from an `EOFException` |
| CSV that may not be opened (`chmod 000`) | The checkpoint; the inference if it is the first file | `FAILED_READ_FILE.FILE_NOT_EXIST` at the checkpoint, and no condition at all at the inference: a `FileNotFoundException` under a job aborted |
| JSON that may not be opened | The inference | `FAILED_READ_FILE.FILE_NOT_EXIST` |
| Parquet that may not be opened | The inference | No condition: a `FileNotFoundException` under a job aborted |

And three facts beside the table:

- **Each of those files, read alone at both moments, fails, and the same way at every read**: the same moment, the same conditions, the same causes. The sound file of the same batch, read alone, reads whole.
- **Spark says that a file that it may not open does not exist**, while the file is there.
- **A batch may fail with no file that fails alone.** Two files of Parquet with one column of two types fail the inference of their batch (`CANNOT_MERGE_SCHEMAS`), and each reads alone. *Corrected by the review of P8 (P8.11e):* that one is known where it is raised, and fails its batch as permanent, whatever the policy of the source, with no file read alone: no file of it is one to skip, and it had no class, so its run ended as `internal`, in 70. Once the file of the other type is written again, the next run gives the batch up (ADR-0018). What fails a batch and no file alone, and is not known, is still reported as it was.

So what Spark raises does not say which file cannot be read, nor whether one cannot: the conditions are several or none, the moment is either, and what Spark calls a file that is not there may be one that it may not open. What a file does alone does say it, and says it the same way each time.

## Decision

**A source says what is done with a file that cannot be read: its run fails, naming it, or the run gives the batch up, records the file as `FAILED` with why, and the next plan is made without it. The files are found by reading each file of the plan alone, once a read of the batch has failed with what the run does not know.**

### 1. The policy

`connector.on_unreadable_file`, for a source of files: `fail`, the default, or `skip`.

- **`fail` by default**: no file leaves a source without somebody choosing it.
- **Validated when the configuration is loaded**, as every field is (P3): another value, or the field on a source of another kind, is refused before anything is opened.
- **In the hash of the configuration when it is `skip`, and only then** (ADR-0007). `fail` is what every source did before the field: a source that says nothing, or says `fail`, keeps the hash that it had, and is not taken for one whose configuration changed. `skip` changes what a source ingests, and the audit trail has to tell the runs made under it.

### 2. What a file that cannot be read is

1. **A file of the plan that, read alone as its batch is read, at both moments, fails with what the read does not know, at each of three reads**: a failure that nothing classified where it was raised. The failure of a file read alone with a class (a header of CSV that is not the batch's, a field twice) is that failure, as it is today.
2. **A file that the landing zone refuses to open**: one outside the root through a symbolic link, a link to nothing or round a loop, something that is not a regular file. It is known as such by a look at the file system, and it is never opened or read by the engine, nor by Spark. A link out of the root is listed and planned by its own size and time; of what it leads to, the file system is asked only whether it is a directory, which is not listed, as for any link. Where it leads is said nowhere.
3. *Added by [ADR-0024](0024-the-dialect-and-the-encoding-of-a-source-of-csv.md) §4 and §5 (P8.9a, 2026-11-27):* a file whose bytes are not of the encoding of its source.

Not one:

- **A file that changed or that is gone since it was planned.** That is a source that changed (ADR-0018), and it is told by the look at the files of the plan, as before and after a read, and never by what Spark raises: Spark says that a file that it may not open does not exist.
- **A failure of a batch that no file has alone**, as two files of Parquet of two types. It is not a file's, and it keeps its class.
- **A name that no run may read** (`FileName`: a `:`, a pattern of Spark, a character that a terminal obeys). It is refused when the batch is planned, under both policies, as today: a name that cannot be written into a plan, a log or an error cannot be the name of a record either.

### 3. How they are found

When the read of a batch fails with what the run does not know, and the run still holds its lease:

1. **The files of the plan are looked at again**, as after every read. One that changed makes it a batch whose source changed, whatever Spark said (ADR-0018).
2. **Each file of the plan is read alone**, at both moments, as the batch is read, up to three times, in rounds: each file once, and those that failed again after a wait of 1 s, and again after 2 s. A file that reads is read once. The run waits within its budget of retries (ADR-0022 §5), counted from the failure of the batch, and looks at its lease while it waits: a wait is cut to what is left of the budget, and a budget that is spent waits no more. A wait is for a round, not for each file, so a plan whose files all fail waits no longer than one with a file that fails. The waits are no retry of the run, and are not counted as one. The files are looked at once more after the reads alone, as after the read of the batch. What the reads alone read is not kept: the batch is never made from them.
3. **The files that fail at each of three reads alone, with no class, are those that cannot be read**, each with why: Spark's conditions and the type of the cause where it began, and never the message of an exception, which may hold what the file held (ADR-0022, as built in P8.3). A file of the second kind of §2 says what the landing zone found (`path_outside_root`, a link to nothing, not a file), and it is not read alone: it is known before Spark is asked anything.
4. **If no file cannot be read**, and a file failed alone and then read, the failure of the batch is one that passed: it is transient, and the run tries again within its budget (ADR-0022 §4). Otherwise the failure of the batch is raised as it was, and keeps its class. A file that fails alone with a class (a header, a field twice) is not one that cannot be read, and the read of a batch meets its failure.

*Changed with P8.7b (2026-11-27), before the part was merged.* Point 4 had the first failure with a class of a file read alone be the batch's. A read of a batch with such a file fails with that failure before any file is read alone, unless a file that cannot be read fails it first, and those come first; so the rule is left to the read of the next batch, which meets it. And a file read three times is the part's (decided on 2026-11-27): a disk that errs for a moment is not to leave a sound file `FAILED` under `skip`; and so are the waits between the reads (decided on 2026-11-27), for a mount that takes a few seconds to come back.

The cost is paid only once a batch has failed: one more read of each sound file of the plan, three of each that cannot be read, which `max_files_per_batch` bounds, and 3 s of waits at most, whatever the number of files. A run that loses its lease while it reads its files alone has its jobs stopped (ADR-0006), and ends as a run that lost its lease, with no file named.

### 4. Under `fail`

The run fails as permanent (4), naming each file that cannot be read and why, up to ten and a count of the rest, as the errors of the landing zone name them. The batch stays planned, and nothing of the files is recorded. Every run of the source fails the same way until the file is mended: a file written again is another file, which the plan no longer has, and the batch is then given up as ADR-0018 has it, and the next plan takes the new file. Or until the source says `skip`.

A file that the landing zone refuses is refused when it is listed, as today: permanent, and nothing planned.

### 5. Under `skip`

1. **The batch is given up as ADR-0018 gives one up**: the table that it writes last is asked for it, and only a batch that is not there is given up. In one transaction, on condition of the epoch of the lease: the batch `ABANDONED`, by the run, with a reason that says that files of it cannot be read and a detail that has each with why; each such file recorded in `processed_file` as `FAILED`, with its error and the batch that found it; the end of the run, and its lease given up. A zombie gives nothing up and records nothing.
2. **The next plan leaves out every file that `processed_file` has**, `TAKEN` or `FAILED`. The files of the plan that could be read are planned again.
3. **A file that was skipped is taken again once it is written again**: a file with another size or another time is another file (P5.2). `touch` is how an operator has it read again, once it is mended.
4. **A file that the landing zone refuses is planned**, as any other file, and found at the look before the read (`Landing.confirm`), before Spark is asked anything. The batch is then given up with it, as in point 1, and nothing of the batch is read.
5. **The run that gives the batch up ends well** (`SUCCEEDED`): it did what its source says. `run` goes on in the same invocation, plans anew without the files skipped, and ends in the code of its last run: 0 if the rest was ingested, with a warning for each file skipped on its error output. §14.1 has 0 for a run that set records apart within what its source allows; a file skipped under `skip` is within what its source allows. A run that skipped files commits no batch, and is not counted by `--max-batches`. Each such run takes at least one file out of what any plan can take, so an invocation that meets them comes to an end.

ADR-0018 has a batch whose source changed end in 3, so that a producer that keeps writing its files again is seen. A file that cannot be read under `skip` is seen in three places, none of them an alert of the orchestrator: the warning of the command, the record of the file, and the batch that was given up for it.

### 6. Where the records are

`processed_file` gains a status, `TAKEN` or `FAILED`, and an error (migration V9 of `control`). The error is there for a file that failed, and for no other, which the database holds (`CHECK`). A file `TAKEN` names the committed batch that took it, as every row before V9, which becomes `TAKEN`; a file `FAILED` names the abandoned batch that found it.

### 7. What it does not cover

- **A reprocess** (ADR-0021 §4) reads files that are taken already, which an operator named. A file of it that cannot be read fails it as permanent, whatever the policy, naming the file: the file has its record, `TAKEN` by the batch that read it, and there is nothing true to record of it as `FAILED`. The batch stays planned, and `abandon-batch` gives it up.
- **A source of a database.** The policy is of files.
- **A file whose read fails for a moment**, as a disk that errs once, is not one that cannot be read: it fails one read alone, or two, and then reads (§3, point 4). The files of the table above fail the same way at every read.

## Alternatives

| Option | Verdict |
|---|---|
| Read every file alone when it is planned | Each file read twice, at every batch and not only at one that fails, and a file that fails only as its rows are read (a gzip whose header is whole) is not found unless the probe reads all of it. Rejected with the breakdown of P8. |
| Take the file from Spark's error (the path of `FAILED_READ_FILE`) | One file for each failure, so a batch with n files that cannot be read takes n runs; some failures name no file (a file of Parquet that may not be opened, the first file of an inference of CSV); and a file named as not there may be one that is. Rejected. |
| Halve the plan until the files that fail are found | Fewer reads than one for each file when a plan is large and few of its files fail, at the cost of reading each sound file more than once. The same answer. Not now: the cost of one read for each file is paid only when a batch fails, and is bounded by `max_files_per_batch`. A measurement (E3) may change it. |
| Take the files that cannot be read out of the plan and commit the rest | A plan that changes under its batch (ADR-0005), and a commit that records some of a plan's files and not others. A new batch does the same with no new state (ADR-0018, alternatives). Rejected. |
| Spark's `ignoreCorruptFiles` | Passes over a file without a word, and may keep what it had read of it. Refused in P5.14. |
| Move a file that cannot be read aside | The engine neither copies nor moves files (discovery §6.3): a move is not atomic with a commit. |
| Under `skip`, end in 3 as ADR-0018 does | 3 tells an orchestrator to run it again later and alert if it goes on, for what the source said to skip. `skip` would be `fail` with more steps. |
| Under `fail`, record the file as `FAILED` too | The next plan would leave it out, and the next run would go on without it: that is `skip`. |
| `on_unreadable_file` in the hash always | The hash of every source of files would change with the field, for a default that changes nothing it ingests. |

## Consequences

- Under `skip`, a source is not stopped by a file that cannot be read: it loses one run, which ends well, and the file is recorded. Under `fail`, the default, it is stopped, with the files named, until somebody mends them or chooses `skip`.
- A failure of a read that is not of a file, or that the run knows, is what it was, with its class. What nothing classifies still ends in 70.
- **A sound file whose read fails for a moment is not taken for one that cannot be read**: it must fail at each of three reads alone, the last of them some 3 s after the first. One that fails once or twice and then reads makes the run try again, as for a failure that passes.
- **Known limit: a failure that lasts through the three reads of a file alone, and the waits between them, is taken for the file's.** The waits are 1 s and 2 s at most, and less once the budget of retries of the run is spent: a run with no budget, `retries.budget: 0s`, does not wait. So a disk or a mount that is away for longer than that leaves a sound file as one that cannot be read: under `fail`, the run ends in 4, and the next run reads it; under `skip`, the file is recorded as `FAILED` and is not read again until it is written again or touched. Longer waits would hold every batch that has a file that cannot be read for as long, since they are what a file that is cut short waits too. A failure of the whole landing zone fails the listing or the look before the read, before any file is read alone.
- A sound file of a plan that failed is read three times under `skip`: by the batch, alone, and by the next batch; one that cannot be read, four, and then never again. A plan bounds it.
- `processed_file` is the record of every file that a source dealt with, taken or not, and the question "which files of this source were skipped, when, and why" is one query.
- The oracle of the tests (`IngestionLedger`) takes a file `FAILED` by a batch that was given up: each file of the landing zone is taken by one committed batch, failed by one abandoned batch, or not yet dealt with.
- `SparkUnreadableFileContractSpec` fails on an upgrade of Spark that changes what the table above says.

## As built (P8.7b, 2026-11-27)

- **The field is `connector.on_unreadable_file`** (`FileConnectorConfig.OnUnreadableFile`, in `core`), `fail` or `skip`, with `fail` when it is left out; it is in what is stored and hashed of a source only when it is `skip` (`FileConnectorConfig.canonical`), and the hash of a source of files that says `fail`, or nothing, is the one it had (`ConfigVersionSpec` pins it).
- **The reads alone are the connector's** (`FileSourceConnector`): the read of the batch and each read alone are one function, the inference of the columns and the checkpoint (`readOf`), and a file is read alone up to `AloneReads` (3) times, in rounds, with the waits of `AloneWaits` (1 s, 2 s) before the second round and the third. Before each read alone the connector asks the lease of the run, which the engine gives it with the read (`ReadContext.verify`), and a test hook may act (`beforeAlone`). The waits are the run's: the connector asks for one (`ReadContext.pause`), and the run waits it within its budget, looking at its lease (`RunRetries.pause`).
- **What is recorded of why** is `unreadable: it failed each of 3 reads of it alone: <conditions>, from <type>`, and for a file that the landing zone refuses `<code>: it <what it found>, and was not opened`, with the codes `path_outside_root`, `broken_link` and `not_a_file` (`Landing.Refusal`). No text of an exception, and no place where a link leads.
- **Under `skip` the connector raises `UnreadableFilesException`**, a signal of the connector contract as `SourceChangedException` is; under `fail`, a permanent failure. The engine gives the batch up (`IngestionService`, `LeasedRun.abandon` with the files that failed), each file `FAILED` in `processed_file` with its error (`ProcessedFiles.fail`, migration V9 of `control`, the schema at version 9), the reason and the detail of the batch say them (`abandon_detail`: `{"unreadable": [{"item", "why"}], "total": n}`), and the run ends `SUCCEEDED` (`Outcome.FilesSkipped`). The files that a transaction records as failed must be of the plan that was written down for the batch. A reprocess raises a permanent failure in place of the signal.
- **`run` goes on in the same invocation** (`Chain`), against the horizon that the run that skipped was given, with a warning for each file and a line for the batch on the error output, and ends in 0 when the rest is ingested (`RunCommand.exitOf`).
- **The landing zone, under `skip`**, lists a file that no run may open by the size and the time of the name itself, and finds it refused at the look before the read when it still has them (`Landing.Confirmed.refused`); one planned so that leads to a file by then is a file that changed. Under `fail` the listing refuses it, as P5 built it.
- **The oracle of the tests** has a rule of its own: a file `FAILED` is of the plan of an abandoned batch that it names, and no batch took it (`IngestionLedger`, `FailedNotOfAbandoned`).
- **Tests** (`SourceSpec`, `ConfigVersionSpec`, `ConfigurationReferenceSpec`, `LandingSpec`, `FileSourceConnectorSpec`, `UnreadableFilesSpec`, `ProcessedFilesSpec`, `ControlSchemaSpec`, `ControlSchemaUpgradeSpec`, `RunCommandSpec`, `IngestionLedgerSpec`). With files broken for real: a file of Parquet cut short, one of CSV in gzip cut short, and one that the process may not open, each under `fail` (4, named, the batch planned, and the file mended or written again) and under `skip` (recorded, the rest ingested, not taken again, and taken once written again or touched), with the ledger; a symbolic link out of the landing zone to a named pipe, planned and recorded with a file cut short beside it, which no read opens: a thread that writes to the pipe counts each reader that it lets through, and counts none; a file that may not be opened at its first read alone and may at its second (`FileSourceConnectorSpec`); a file that the process may not open for two seconds after its first read alone, given back by a thread by the time, which the waits ride out within a budget of 30 s, with the reads 1 s and 2 s apart, and whose run tries again and commits; the same file with no budget, which is not waited for and is recorded, as the known limit says; a wait asked for each round and not for each file; the waits of `RunRetries.pause`, cut to the budget and no retry; two files of Parquet of two types, which keep their class; a file that changes during the reads alone, which is a source that changed; a lease lost before the reads alone, which starts none; a reprocess under `skip`, 4; and an invocation of `run` under each policy. *Mutations:* one read alone in place of three records the file that failed once, and fails its test; the landing zone that opens what a link out of the root leads to is seen five times by the thread of the pipe, and fails its test.

## As built (P8.11, 2026-11-28)

- **Two more kinds of file that cannot be read** (P8.11j, P8.11k). A file with a value of CSV, or a name of its header, longer than 1,048,576 characters (`maxCharsPerColumn`), and a file of JSON with a line as long (`LineLengths`, `line_too_long`), which Spark would hold whole: each fails the same way at every read, so it is known at its first read alone, without the two waits. Why says the bound, and never what the value or the line held.
- **A failure of a batch that no file has, and that the connector knows, keeps its class** (P8.11e): files of Parquet with one column of two types fail as permanent, whatever the policy, and no file is read alone.
- **An error of the JVM met as a file is read** (P8.11m) is not caught. Spark makes it a `scala.MatchError` with no cause in the lines of JSON and the rows of Parquet, an ordinary failure of a task: the file is found by the reads alone, with `from scala.MatchError` for why. In CSV it is the cause of univocity's failure, and an `OutOfMemoryError` there ends the process in 52; in the inference of Parquet the read never ends (discovery §20). `SparkUnreadableFileContractSpec` pins the three.
- **A run that lost its lease gives no file up** (P8.11n). A read alone after the loss, or one stopped by it, fails as a file that cannot be read does; the run asks its lease before it acts on a read that failed, so under `skip` nothing is recorded and the batch stays planned (`LeaseLossSpec`).
