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
- **A batch may fail with no file that fails alone.** Two files of Parquet with one column of two types fail the inference of their batch (`CANNOT_MERGE_SCHEMAS`), and each reads alone.

So what Spark raises does not say which file cannot be read, nor whether one cannot: the conditions are several or none, the moment is either, and what Spark calls a file that is not there may be one that it may not open. What a file does alone does say it, and says it the same way each time.

## Decision

**A source says what is done with a file that cannot be read: its run fails, naming it, or the run gives the batch up, records the file as `FAILED` with why, and the next plan is made without it. The files are found by reading each file of the plan alone, once a read of the batch has failed with what the run does not know.**

### 1. The policy

`connector.on_unreadable_file`, for a source of files: `fail`, the default, or `skip`.

- **`fail` by default**: no file leaves a source without somebody choosing it.
- **Validated when the configuration is loaded**, as every field is (P3): another value, or the field on a source of another kind, is refused before anything is opened.
- **In the hash of the configuration when it is `skip`, and only then** (ADR-0007). `fail` is what every source did before the field: a source that says nothing, or says `fail`, keeps the hash that it had, and is not taken for one whose configuration changed. `skip` changes what a source ingests, and the audit trail has to tell the runs made under it.

### 2. What a file that cannot be read is

1. **A file of the plan that, read alone as its batch is read, at both moments, fails with what the read does not know**: a failure that nothing classified where it was raised. The failure of a file read alone with a class (a header of CSV that is not the batch's, a field twice) is that failure, as it is today.
2. **A file that the landing zone refuses to open**: one outside the root through a symbolic link, a link to nothing or round a loop, something that is not a regular file. It is known as such by a look at the file system, and it is never opened or read by the engine, nor by Spark. Of a link out of the root nothing is looked at beyond the link itself: its size and its time are its own, and where it leads is said nowhere.

Not one:

- **A file that changed or that is gone since it was planned.** That is a source that changed (ADR-0018), and it is told by the look at the files of the plan, as before and after a read, and never by what Spark raises: Spark says that a file that it may not open does not exist.
- **A failure of a batch that no file has alone**, as two files of Parquet of two types. It is not a file's, and it keeps its class.
- **A name that no run may read** (`FileName`: a `:`, a pattern of Spark, a character that a terminal obeys). It is refused when the batch is planned, under both policies, as today: a name that cannot be written into a plan, a log or an error cannot be the name of a record either.

### 3. How they are found

When the read of a batch fails with what the run does not know, and the run still holds its lease:

1. **The files of the plan are looked at again**, as after every read. One that changed makes it a batch whose source changed, whatever Spark said (ADR-0018).
2. **Each file of the plan is read alone**, at both moments, as the batch is read, and looked at before and after its read as the batch is. What it reads is not kept: the batch is never made from those reads.
3. **The files that fail alone, with no class, are those that cannot be read**, each with why: Spark's conditions and the type of the cause where it began, and never the message of an exception, which may hold what the file held (ADR-0022, as built in P8.3). A file of the second kind of §2 says what the landing zone found (`path_outside_root`, a link to nothing, not a file), and it is not read alone: it is known before Spark is asked anything.
4. **If no file cannot be read**, the first failure with a class of a file read alone is the batch's; if there is none, the failure of the batch is raised as it was, and keeps its class.

The cost is paid only once a batch has failed: one more read of each file of the plan, which `max_files_per_batch` bounds. A run that loses its lease while it reads its files alone has its jobs stopped (ADR-0006), and ends as a run that lost its lease, with no file named.

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
- **A file whose read fails for a moment**, as a disk that errs once. Nothing is retried: §3 relies on what fails alone failing every time, which is what the files of the table above do, and a failure that passes is not of them (Consequences).

## Alternatives

| Option | Verdict |
|---|---|
| Read every file alone when it is planned | Each file read twice, at every batch and not only at one that fails, and a file that fails only as its rows are read (a gzip whose header is whole) is not found unless the probe reads all of it. Rejected with the breakdown of P8. |
| Take the file from Spark's error (the path of `FAILED_READ_FILE`) | One file for each failure, so a batch with n files that cannot be read takes n runs; some failures name no file (a file of Parquet that may not be opened, the first file of an inference of CSV); and a file named as not there may be one that is. Rejected. |
| Halve the plan until the files that fail are found | Fewer reads than one for each file when a plan is large and few of its files fail, at the cost of reading each sound file more than once. The same answer. Not now: the cost of one read for each file is paid only when a batch fails, and is bounded by `max_files_per_batch`. A measurement (P17) may change it. |
| Take the files that cannot be read out of the plan and commit the rest | A plan that changes under its batch (ADR-0005), and a commit that records some of a plan's files and not others. A new batch does the same with no new state (ADR-0018, alternatives). Rejected. |
| Spark's `ignoreCorruptFiles` | Passes over a file without a word, and may keep what it had read of it. Refused in P5.14. |
| Move a file that cannot be read aside | The engine neither copies nor moves files (discovery §6.3): a move is not atomic with a commit. |
| Under `skip`, end in 3 as ADR-0018 does | 3 tells an orchestrator to run it again later and alert if it goes on, for what the source said to skip. `skip` would be `fail` with more steps. |
| Under `fail`, record the file as `FAILED` too | The next plan would leave it out, and the next run would go on without it: that is `skip`. |
| `on_unreadable_file` in the hash always | The hash of every source of files would change with the field, for a default that changes nothing it ingests. |

## Consequences

- Under `skip`, a source is not stopped by a file that cannot be read: it loses one run, which ends well, and the file is recorded. Under `fail`, the default, it is stopped, with the files named, until somebody mends them or chooses `skip`.
- A failure of a read that is not of a file, or that the run knows, is what it was, with its class. What nothing classifies still ends in 70.
- **Known limit: a sound file whose read fails alone for a moment is taken for one that cannot be read.** Under `fail`, the run ends in 4 for a sound file, and the next run reads it. Under `skip`, the file is recorded as `FAILED` and is not read again until it is written again or touched. The files that the contract spec breaks fail the same way at every read; a disk that errs for a moment is not one of them, and is rare on the landing zone of this deployment.
- A sound file of a plan that failed is read three times under `skip`: by the batch, alone, and by the next batch. A plan bounds it.
- `processed_file` is the record of every file that a source dealt with, taken or not, and the question "which files of this source were skipped, when, and why" is one query.
- The oracle of the tests (`IngestionLedger`) takes a file `FAILED` by a batch that was given up: each file of the landing zone is taken by one committed batch, failed by one abandoned batch, or not yet dealt with.
- `SparkUnreadableFileContractSpec` fails on an upgrade of Spark that changes what the table above says.
