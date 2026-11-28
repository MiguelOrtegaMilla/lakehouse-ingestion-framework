# Running it: the services, a source, and what an operator does

How the framework is run on one machine: the PostgreSQL that it needs, the command that an orchestrator starts for each source, its exit codes, and the commands of an operator for a source that is stuck, whose configuration changed, whose files are to be read again, or whose history is to be loaded. The [README](../../README.md) has the short way through all of it.

## Local services

One PostgreSQL 17 container hosts the control plane, the commit coordination store and a demo source database ([ADR-0003](../adr/0003-control-plane-in-postgresql.md)). The image is pinned by digest, and the container runs as a non-root user with no capabilities. It listens on `127.0.0.1` only.

```bash
scripts/dev-secrets.sh                                    # once: a random password per role, in docker/secrets/ (git-ignored)
docker compose -f docker/docker-compose.yml up -d --wait  # start; waits until the bootstrap has finished
docker compose -f docker/docker-compose.yml down          # stop; data stays in the pgdata volume
docker compose -f docker/docker-compose.yml down -v       # stop and delete the data
```

The bootstrap script (`docker/postgres/init/`) runs only on an empty volume. After changing it, recreate the volume with `down -v`. A volume made before the script had a role lacks it, and its secret: run `scripts/dev-secrets.sh` again, which keeps the passwords that are there and makes the one that is missing, and recreate the volume.

| Database | Contents | Roles that can connect |
|---|---|---|
| `ingestion_control` | Schemas `control` (control plane) and `delta_coordination` (commit coordination), both owned by `ingest_migrator` | `ingest_migrator` (DDL through migrations), `ingest_control`, `ingest_coordination`, `ingest_maintenance` |
| `sales` | Demo source system for the JDBC connector. Its tables are in `public`, which `sales_owner` owns | `sales_owner` (the source system's own role: it creates and writes the tables, and the engine never connects as it), `sales_ro` (the JDBC connector: it reads what `sales_owner` creates, and nothing else) |

Least-privilege roles: [ADR-0013](../adr/0013-application-security-baseline.md) §3. `PostgresBootstrapSpec` tests the bootstrap against the same image and scripts.

The tables of the two schemas, and the privileges on them, come from migrations: `engine` ships those of `control`, and `delta-pg-logstore` those of `delta_coordination` (`src/main/resources/db/migration/` in each). The command `migrate` applies them, as the role that owns the schemas. Its password is in no profile: the command is told where it is.

```bash
export INGEST_MIGRATOR_PASSWORD="$(cat docker/secrets/ingest_migrator_password)"
sbt "cli/run migrate --profile conf/app/local.yaml --password env:INGEST_MIGRATOR_PASSWORD"
```

```text
control: version 9, 9 migrations applied (jdbc:postgresql://localhost:5432/ingestion_control)
delta_coordination: version 5, 5 migrations applied (jdbc:postgresql://localhost:5432/ingestion_control)
```

It can be run again at any time: a schema that is at its version is left as it is. The engine and the LogStore run no DDL. Each checks the version of its schema before it touches a table, and refuses to work on another.

`ControlSchemaSpec` and `CoordinationSchemaSpec` test what the constraints of each schema guarantee whatever the code does, and what each role may and may not do; `MigrateCommandSpec`, the command against a database as the bootstrap leaves it.

## Running a source

`run` runs one source once: the new files of a file source, or what changed in a table of a database, into its table of Bronze. It is what an orchestrator starts, one process for each source, and its exit code is all it says to it.

```bash
mkdir -p ~/lakehouse-data/lake ~/lakehouse-data/landing/sales/returns   # once: the engine makes no root
export INGEST_CONTROL_PASSWORD="$(cat docker/secrets/ingest_control_password)"
export INGEST_COORDINATION_PASSWORD="$(cat docker/secrets/ingest_coordination_password)"
printf 'id,amount\n1,10\n2,20\n' > ~/lakehouse-data/landing/sales/returns/returns-1.csv
sleep 61                                                                # a file is taken once it is a minute old (connector.min_file_age)
sbt "cli/run run --profile conf/app/local.yaml --sources conf/sources --source sales.returns"
```

```text
The run 019a… of sales.returns committed the batch sales.returns/bronze#1: 2 rows read, 2 written, 0 rejected, into bronze/sales/returns at version 0.
```

Run it again and it finds nothing new. A source with more than one batch takes (`max_files_per_batch`, or a window that `max_batch_range` cuts) is run again in the same process, a batch a run, up to what was there when the invocation started; `--max-batches <n>` stops it sooner. `SIGTERM` or `SIGINT` lets the batch in course finish and starts no other; a second signal ends the process at once. The whole configuration is validated first, and what can be refused is refused before anything is started: a source that is not there, roots that are not there, a database that does not answer, a Spark setting that the engine sets or that would have Delta forget the batches of a table.

| Code | Meaning |
|---|---|
| 0 | A batch committed, nothing new, or a source that is disabled. Under `connector.on_unreadable_file: skip`, also a file that cannot be read skipped and recorded, with a warning on the error output, and the rest ingested by the same invocation |
| 2 | Another run holds the lease of the source: nothing was done |
| 3 | A database did not answer, the run lost its lease, or its files changed under it: run it again later |
| 4 | Something that running again does not mend: the configuration, a database that refused, a run that failed for good, a file that cannot be read (named, under `fail`, the default) |
| 5 | More records were rejected than the source allows: nothing was committed, and they are in quarantine |
| 128 + n | The signal n came before the first run: nothing was run. A run stopped after one ends in the code of its last run |
| 64 | The command line is not one |
| 70 | A mistake of the application, which nothing classified: the run is recorded as failed with the class `internal`, and its lease is free. The log has the failure whole. Alert |

The whole contract is in [discovery §14.1](../architecture/00-architecture-discovery.md#141-exit-codes-the-contract-with-the-orchestrator). `RunCommandSpec` runs the command as an orchestrator does, a process of its own among them; `JdbcRunCommandSpec`, with a table of a database, cut for real by a proxy.

### Where a command logs

**Every command logs to its error output, at INFO, and its standard output has what the command writes and nothing else**: the records of `quarantine --records` among them. The command line has a configuration of log4j2 of its own (`modules/cli/src/main/resources/log4j2.properties`), with the pattern of Spark's default, and a deployment may give another with `-Dlog4j2.configurationFile`. Without one, log4j2 logs errors alone, to the standard output, until Spark sets its own default the first time that it logs: what a command logs before then is lost, or written among what it prints.

**A configuration of its own is to take out what univocity parsed.** When a value of CSV is longer than the bound, univocity, Spark's parser of CSV, fails with what it had parsed in its message, after `content parsed=`: up to 1003 characters of the source. Spark logs that failure at WARN and at ERROR with each task that fails, so the configuration of the application replaces those characters in every line, of the message and of its stack trace, and writes no stack trace outside its pattern. A deployment that gives another configuration copies both:

```properties
appender.console.layout.alwaysWriteExceptions = false
appender.console.layout.pattern = %d{yy/MM/dd HH:mm:ss} %p %c{1}: %replace{%m%n%ex}{(?s)content parsed=.{0,1003}(?<![\\r\\n])}{content parsed=<not logged>}
```

Without `alwaysWriteExceptions = false`, log4j2 writes the stack trace again, whole, after the pattern. `RunCommandLogsSpec` runs a command with the configuration of the application, and finds none of what was parsed in its log.

### What a run logs of what its source holds

**At INFO, which is the level that the command line logs at, no line of the log of a run has a value of a row**, nor of a record that was set apart: not a line of the application, nor of Spark, of Delta or of Parquet. What a run prints says how many rows it read, wrote and rejected and the names of its files, and never what they hold. The names of the columns of a source are logged: Delta says the schema of a table with each snapshot of it.

- **Delta would log values, and the engine has it not.** Delta logs each snapshot of a table with the checksum of its version, and that checksum has the files of a small table, each with the least and the greatest value of each column, unless the session writes its checksums without them. Every session of a run does (`spark.databricks.delta.allFilesInCrc.enabled`, off, which a profile may not set; [the reference](../configuration/reference.md) has what the engine sets). What is given up for it is a check that Delta makes only in its verification mode, which is off by default: that of the files of a snapshot against those of its checksum.
- **A table that another writer of Delta committed to last is logged with those values** by whoever reads it, a run among them, until a run commits to it: the setting is of what a session writes, and not of what it reads.
- **Below INFO, Parquet logs each value that it writes.** With `org.apache.parquet.io` at DEBUG, or the root logger, its writer of records (`MessageColumnIO`, `RecordConsumerLoggingWrapper`) logs each value that goes into a file of a table of Bronze or of a quarantine: a number as it is, a text as the list of its bytes. That is Parquet's own, and the engine sets the level of no logger. A deployment that logs at DEBUG keeps `org.apache.parquet.io` at INFO or above, or keeps its log as it keeps its data.
- **The statistics stay where the data is.** The least and the greatest value of each column of each file are in the commits of a table, under its `_delta_log`, where a read passes over files by them: who may read the log of a table may read the table.

### A file that cannot be read

A file cut short, one that the process may not open, a symbolic link out of the landing zone, a value of CSV longer than 1,048,576 characters, or a line of JSON as long (`line_too_long`, most often a file that is one document of JSON and not JSON Lines) stops its source by default: the run ends in 4 and names each file and why, and every run does the same until the file is mended. Write it again, whole (another size or time makes it another file, which the next run takes), or give the process the permission to read it. A source that says `connector.on_unreadable_file: skip` goes on without it: the file is recorded in the control plane as `FAILED`, with why, and `touch` has it read again once it is mended ([ADR-0023](../adr/0023-a-file-that-cannot-be-read.md)). The files a source skipped, and why:

```sql
SELECT path, size, modified_at, error, batch_seq
  FROM control.processed_file WHERE source_id = 'sales.returns' AND status = 'FAILED';
```

### A file that is not of the encoding of its source

A file of CSV with a byte that is not of the encoding that its source says (`connector.csv.encoding`, UTF-8 unless it says another), or a file of JSON with one that is not UTF-8, is a file that cannot be read: read, it would have the character U+FFFD where its source had another ([ADR-0024](../adr/0024-the-dialect-and-the-encoding-of-a-source-of-csv.md) §4). The run says where the first such byte is, by its offset and its line, and never the byte:

```text
'prices.csv' not_of_its_encoding: the byte at offset 18 of the file, in line 3, is not of UTF-8
```

Either the file is wrong, or what the source says of its files is:

- **The file is wrong**, one among others that are right: have it written again, in the encoding of the source. Written again, it is another file, which the next run takes.
- **The source is wrong**, and every file of it is in another encoding, as a spreadsheet's in `windows-1252` under a source that says nothing: correct `connector.csv.encoding`. Under `fail`, the batch then stays as it was planned, in the encoding of before, and is given up with `abandon-batch` (below), so that the next plan takes its files as the source says now. Under `skip`, the batch was given up already and its files recorded as `FAILED`: `touch` them, and the next run takes them as new files. A `reprocess` does not take them: it reads the files of a batch that was committed.
- **Under `ISO-8859-1`**, a byte of 0x80 to 0x9F is refused, and the message says to try `windows-1252`: those bytes are the euro and the quotes of a spreadsheet, and control characters in ISO-8859-1.

A file of JSON has no field for its encoding: it is to be written again, in UTF-8.

### What a source had set apart: `quarantine`

A record that cannot be made a row is kept in the quarantine of its source, and a run that sets apart more of them than its source allows ends in 5, with its batch planned. To see what they are:

```bash
sbt "cli/run quarantine --profile conf/app/local.yaml --sources conf/sources --source sales.returns"
```

```text
The quarantine of sales.returns has 14 records of what was asked, in 2 groups by run, file and type:

RUN                                   BATCH  STATUS     FILE                         TYPE      RECORDS  LATEST
0193a5f0-7c2e-7b1a-9f3e-2d4c6a8b0e1f  7      PLANNED    sales_returns/2026-03-02.csv  unparsed       12  2026-03-02T06:00:04Z
0193a0c9-11d4-7e02-8a55-90b1c3d2e4f6  6      COMMITTED  sales_returns/2026-03-01.csv  unparsed        2  2026-03-01T06:00:03Z

By type: unparsed 14.
```

A group is the records that one run set apart of one file for one reason, the group with the latest records first; `STATUS` is that of the batch of the run, and `PLANNED` is a batch that its threshold stopped. What is asked may be less: `--run <id>`, `--batch <n>` (with `--backfill <id>` for a chunk of a backfill), `--file <path>`, as the table shows it, and `--type` (`unparsed`, `trailing_text` or `duplicate_field`). A thousand groups are shown at most, with how many there are.

**The records of a batch that was given up are left out**, and the command says how many: the batch after it sets the same records apart again, under its own number.

**The command reads, and writes nothing.** It is no run, takes no lease, and leaves nothing in the control plane; it may be run while the source is, and on a source that is disabled.

**`--records <n>` shows records as they were received, and they may hold personal data.** Up to n of them, from 1 to 1000, the latest first, each as a line of JSON on the standard output, which then has nothing else: the warning, and how many were written, are on the error output.

```bash
sbt -error "cli/run quarantine --profile conf/app/local.yaml --sources conf/sources --source sales.returns --batch 7 --records 20" > rejected.jsonl
jq -r .raw_record rejected.jsonl
```

`-error` has sbt say nothing of its own, so that the file has the records alone.

- **Where the records go is yours to mind.** The command writes them to its standard output and nowhere else: not to its error output, and not to a log, of the application or of Spark. Under an orchestrator, the standard output of a task is a log: `--records` is for a terminal, or for a file that is kept as the quarantine is.
- **A line is safe to show.** Each character that a terminal acts on, or that ends a line, is written as its escape (`\u001b`): a control character, the separators of lines U+2028 and U+2029, and the characters that turn the direction of a text. `jq` gives the text back as it was.
- **A long record is cut** at its first 8192 characters (`truncated`), with how many it has (`raw_record_chars`): a record may be the rest of a file, after a quote that is not closed. The whole of it is in the table.
- **It does not page.** The records are the first of their order, and less is asked for by a run, a batch, a file or a type. Whoever needs every record reads the table of Delta, at `_quarantine/bronze/<source id>` under the lake, leaving out the runs of the batches that were given up ([the reference](../configuration/reference.md#records-that-are-rejected) has the query).

| Code | Meaning |
|---|---|
| 0 | What was asked was shown, were it nothing: a source with no quarantine never had a record set apart |
| 3 | A database did not answer |
| 4 | The configuration is not valid, or the source has no such run or batch |

### When a source is stuck: `abandon-batch`

A batch is planned before anything is read, and every run replays its plan until it is committed. A plan that cannot be run, because the directory or the table it names is no longer the source's, stops the source. An operator gives it up:

```bash
sbt "cli/run abandon-batch --profile conf/app/local.yaml --sources conf/sources --source sales.returns --batch 3 --reason 'the landing directory was moved'"
```

It is a run of its own under the lease of the source, and records who asked (the user of the process) and why. It gives the batch up only when the table it was planned for is known not to have it: a batch whose rows are written is committed by the next run, and a table that cannot be read is asked again later (3). The next run plans anew from the state of the source. It mends a plan, not data: a file whose records cannot be read is planned again, and fails the same way ([ADR-0021](../adr/0021-operator-actions-and-backfills.md)).

| Code | Meaning |
|---|---|
| 0 | The batch was given up |
| 2 | A run holds the lease of the source: nothing was changed |
| 3 | A database did not answer, the table could not be read, or the action lost its lease |
| 4 | Refused, with nothing changed: no batch is planned, another is, or its table has it |
| 64 | The command line is not one: `--reason` and `--batch` are required |
| 70 | A mistake of the application, which nothing classified: the log has it whole. Alert |

### When the dialect or the encoding of a source of CSV was wrong

A source whose `connector.csv` was wrong has values under the wrong columns, records in the quarantine that were right, or a batch stopped by more of them than it allows (5). Correct the configuration; the next plan reads its files as it says. What was planned or ingested before is read as it was, and is mended so ([ADR-0024](../adr/0024-the-dialect-and-the-encoding-of-a-source-of-csv.md) §3):

- **a batch that is planned** is read as it was planned, by every run, which says so with a warning that names both dialects: give it up (`abandon-batch`), and the next run plans the same files, read as the source says now. The run that warns of it commits it, unless it fails, and then it is a batch that was committed: the warning says what to do in both cases;
- **a batch that was committed** is read again with `reprocess`, which reads its files as the source says when it is asked for. Bronze keeps both copies, each with its run.

### When a source's configuration changed: `reset-state`

A run compares the configuration it is given with the one that the state of its source was last moved with, and refuses a change that what Bronze has cannot follow: another lake, another Bronze table, another watermark column, or another table to read from. When the change is meant, an operator resets the state of the source, and says what happens to its watermark:

```bash
sbt "cli/run reset-state --profile conf/app/local.yaml --sources conf/sources --source sales.orders --reason 'updated_at was renamed modified_at' --keep-watermark"
```

| Option | The watermark | For |
|---|---|---|
| `--watermark <moment>` | Set to the moment, written as `load.watermark.start` is. The next batch counts from it. Not before `start`: those rows are a backfill's | Reading part of the history again, or skipping it |
| `--forget-watermark` | None. The next run plans as a first run does, from `start` or the whole table | Reading everything again |
| `--keep-watermark` | As it was. Only what later runs are compared with is reset | A table that was moved, a column that was renamed, a source of files |

The configuration of the reset is the one that later runs are compared with, and the change it let through is said with it. The numbers of the batches go on, since a table skips a number it has had, and the files that were taken stay taken. A reset is a run of its own under the lease of the source, with who asked and why, and writes the state before and after to `control.state_change_log` ([ADR-0021](../adr/0021-operator-actions-and-backfills.md)).

A watermark kept across a change has two risks, and the command warns of each one that applies:

- **Another Bronze table, or another lake.** The history of the source stays in the old table, and the new one receives only what is ingested from now on. Reading it into the new one is a reset to an earlier watermark, a backfill or a reprocess.
- **Another watermark column.** The watermark was a value of the old column, and is taken as a value of the new one. If the new column does not mean what the old one did, rows whose new column is at or before the watermark are skipped without a word. Keeping it is right for a column that was renamed at the source; for any other, use `--watermark` or `--forget-watermark`.

| Code | Meaning |
|---|---|
| 0 | The state was reset, with a warning on standard error for each risk of a kept watermark |
| 2 | A run holds the lease of the source: nothing was changed |
| 3 | A database did not answer, or the action lost its lease |
| 4 | Refused, with nothing changed: a batch is planned (it is the next run's, or `abandon-batch`'s), the source has no state, or it keeps no watermark of the kind asked for, or the moment cannot be one |
| 64 | The command line is not one: `--reason` and exactly one of the three options are required |
| 70 | A mistake of the application, which nothing classified: the log has it whole. Alert |

### Reading files again: `reprocess`

A file that was taken is not read again by a run. When it has to be, because the parser was fixed, or the Bronze table was moved by a reset and is to have the history, an operator reprocesses the batch that took it:

```bash
sbt "cli/run reprocess --profile conf/app/local.yaml --sources conf/sources --source sales.returns --batch 3 --reason 'the parser of dates was fixed'"
```

It is a run of its own under the lease of the source, with who asked and why, that plans the next batch of the source with the files of batch 3, as they were then (names, sizes and times), and ingests it into the table that the configuration names now. The batch says which batch it reads again, and takes no file: the files stay taken by the batch that took them. Bronze keeps both copies, each with its run. From its plan on it is a batch like any other: one that a run left halfway is finished by the next run, and one whose file changed since, or is gone, is given up. It is for sources of files: a window of a table is read again by a backfill ([ADR-0021](../adr/0021-operator-actions-and-backfills.md)).

Its codes are those of `run`: 0 the batch was committed, 2 a run holds the lease, 3 a database did not answer, the lease was lost or a file changed since, 4 refused with nothing planned (a batch is planned, the batch named was not committed or read no files, or a configuration that Bronze cannot follow), 5 more records rejected than the source allows, 64 the command line is not one.

### A history, or a stretch read again: `backfill`

A source that is loaded by a watermark reads forward from `load.watermark.start`, a window after another. What is before `start`, or a stretch that has to be read again, is read by a backfill: a range of the same column, on a sequence of batches of its own, with a lease of its own. The regular runs of the source go on beside it, on the same table, and neither waits for the other nor moves its state ([ADR-0021](../adr/0021-operator-actions-and-backfills.md) §5).

```bash
sbt 'cli/run backfill --profile conf/app/local.yaml --sources conf/sources --source sales.orders --backfill march-again --from 2026-03-01T00:00:00Z --to 2026-03-15T00:00:00Z --chunk 7d --reason "two weeks of March read again, to show a backfill"'
```

```text
The run 01a2150c-67f0-… of sales.orders created the backfill march-again: it reads (2026-03-01T00:00:00Z, 2026-03-15T00:00:00Z] of updated_at, in chunks of 7d. The rows whose updated_at is 2026-03-01T00:00:00Z exactly are not read; those at 2026-03-15T00:00:00Z are.
warning: the regular sequence of sales.orders reads the rows whose updated_at is after 2026-01-01T00:00:00Z, its load.watermark.start: all of (2026-03-01T00:00:00Z, 2026-03-15T00:00:00Z] is read by both, and is in Bronze twice, each copy with its run, unless a reset of the state took the watermark of the source past it.
The run 01a2150c-67f0-… of sales.orders committed the chunk 1 of the backfill march-again: 43 rows read, 43 written, 0 rejected, into bronze/sales/orders at version 13.
The run 01a2150c-b98a-… of sales.orders committed the chunk 2 of the backfill march-again: 30 rows read, 30 written, 0 rejected, into bronze/sales/orders at version 14.
The backfill march-again of sales.orders is complete: it has read (2026-03-01T00:00:00Z, 2026-03-15T00:00:00Z].
The invocation of the backfill march-again of sales.orders made 2 runs and committed 2 chunks.
```

- **The range is `(from, to]`.** A row whose watermark is `from` exactly is not read, and one at `to` is. `to` is no later than the clock of the source less its `safety_margin`: a backfill does not read time in which a transaction may still commit rows. `load.watermark.start` does not bound it.
- **The id is the operator's**: lower-case letters, digits, `.`, `_` and `-`. The first invocation under an id creates the backfill, and each one after it, with the same arguments, resumes it where its last chunk ended; another range or another chunk under the same id is refused. So an orchestrator that runs the command again does not start another backfill, and one that is complete ends in 0 with nothing read.
- **It is read in chunks**, a run for each, as long as `--chunk` says or as `max_batch_range` of the source; a backfill with neither is refused. `--max-batches` and a signal stop it as they stop `run`, and the next invocation goes on.
- **What the regular runs read too is in Bronze twice**, each copy with its run, and is warned of, not refused: whether a copy is wanted is the operator's to say, and Silver keeps one row for each key.
- **A chunk that cannot be run is given up** with `abandon-batch --backfill <id> --batch <n>`, under the lease of the backfill: its next run reads the window of that chunk again.
- **A backfill is held to the configuration that it was created with**, as a source is to the one that it last ingested with. A reset of the state of the source does not change that: what is left of a backfill under another configuration is read as a backfill of its own, under another id.

Its codes are those of `run`, with 2 for the lease of the backfill, which no regular run ever holds: 0 chunks were committed, or the backfill is complete; 3 a database did not answer or the lease was lost; 4 refused, with nothing created or planned (a source that is not loaded by a watermark, no chunk, a `to` that has not settled, another range or chunk under the id, a configuration that the backfill cannot follow); 64 the command line is not one, an id out of its grammar or a `from` that is not before `to` among them.

### When the LogStore refuses a table: `maintain-table`

The LogStore refuses to commit to a table whose log it cannot vouch for: one that a writer without it committed to (tainted), one that was made without it, or one that was dropped and whose registry still knows it. A run that meets one fails for good (4), and names the command that mends it, with the table:

```text
error: the LogStore refused a table of the run, and running again does not mend it: file:/…/bronze/sales/returns is tainted and refuses writes until an operator verifies and re-adopts it: … Once its log has been looked at and is right, it is adopted again with: lakehouse-ingestion maintain-table readopt --table file:/…/bronze/sales/returns --profile <file> --password <reference> --reason <text>
```

Stop the writers of the table, look at its log, and once it is right:

```bash
export INGEST_MAINTENANCE_PASSWORD="$(cat docker/secrets/ingest_maintenance_password)"
sbt "cli/run maintain-table readopt --profile conf/app/local.yaml --password env:INGEST_MAINTENANCE_PASSWORD --table file:/…/bronze/sales/returns --reason 'the foreign commit was the backfill of the team, and is right'"
```

| Operation | For |
|---|---|
| `adopt` | A table made without the LogStore: it is registered at the version its log is at, with the id of its first commit |
| `readopt` | A tainted table, or one whose registry and log disagree, once its log has been looked at and is right |
| `reset` | A table that was dropped: its registry is removed, so that one can be created at its path again |
| `collect-temps` | Any table: removes the temporary files that nothing needs, and keeps, with a warning for each, those that something may. A role that cannot write the maintenance log removes nothing |

It runs as `ingest_maintenance`, whose password, as the migrator's, is in no profile, takes no lease and is no run. Each operation looks at the log of the table first and refuses what the log does not bear out, and is recorded in `delta_coordination.maintenance_log` with who asked (the user of the process), why, when, and what it found and changed; an operation that is refused leaves no row. The table must be in the lake of the profile.

| Code | Meaning |
|---|---|
| 0 | The operation was made, and recorded |
| 3 | The coordination database did not answer, or the table is being written while it is maintained: stop its writers, and ask again |
| 4 | Refused, with nothing changed: the log or the registry of the table is not in the state the operation is for, the table is not in the lake, or the database refused the role |
| 64 | The command line is not one: the operation first, and `--table` and `--reason` are required |
| 70 | A mistake of the application, which nothing classified: the log has it whole. Alert |

### A table of a database: `sales.orders`

The example source [`sales.orders`](../../conf/sources/sales.orders.yaml) reads `public.orders` of the database `sales` by `updated_at`, four weeks of changes a batch. The table is the source system's, and the engine makes none: a demo makes it and changes it as the source's applications would, as `sales_owner`. It is test code (`it.demo.SalesOrders`, with the generator of the tests), and is in no artifact of the application.

```bash
export SALES_OWNER_PASSWORD="$(cat docker/secrets/sales_owner_password)"
sbt "integration-tests/Test/runMain lakehouse.ingestion.it.demo.SalesOrders --create"   # once: orders since 2026-01-01, up to two minutes ago

export INGEST_CONTROL_PASSWORD="$(cat docker/secrets/ingest_control_password)"
export INGEST_COORDINATION_PASSWORD="$(cat docker/secrets/ingest_coordination_password)"
export SALES_RO_PASSWORD="$(cat docker/secrets/sales_ro_password)"
sbt "cli/run run --profile conf/app/local.yaml --sources conf/sources --source sales.orders"
```

```text
The run 019a… of sales.orders committed the batch sales.orders/bronze#1: 80 rows read, 80 written, 0 rejected, into bronze/sales/orders at version 0.
…
The invocation of sales.orders made 12 runs and committed 12 batches.
```

The first invocation reads from `load.watermark.start` up to a minute before the clock of the database, its `safety_margin`: a run for each four weeks of the table (`load.max_batch_range`), each a batch. The numbers are those of a table made on 2026-11-21, and there is a batch more every four weeks. Then the demo changes some orders, and the next invocation reads what changed, and only that, once the margin has passed:

```bash
sbt "integration-tests/Test/runMain lakehouse.ingestion.it.demo.SalesOrders --changes"   # rows inserted, updated and deleted, at moments of the last minute
sleep 61
sbt "cli/run run --profile conf/app/local.yaml --sources conf/sources --source sales.orders"
```

```text
The run 019a… of sales.orders committed the batch sales.orders/bronze#13: 14 rows read, 14 written, 0 rejected, into bronze/sales/orders at version 12.
```

The demo says that it inserted 9 orders, updated 6 and deleted 2, and the batch has 14 rows: the 9 that are new, one of them inserted and updated again before it was read, which is its second image alone; and the 5 that were there and changed. A deleted order is not seen by a load by a watermark: it stays in Bronze as it was last read, until the reconciliation of keys (P10). The demo needs the role `sales_owner`, which a volume made before it was added lacks: see [Local services](#local-services).
