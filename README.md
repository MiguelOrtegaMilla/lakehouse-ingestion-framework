# lakehouse-ingestion-framework

[![CI](https://github.com/MiguelOrtegaMilla/lakehouse-ingestion-framework/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/MiguelOrtegaMilla/lakehouse-ingestion-framework/actions/workflows/ci.yml)

A metadata-driven ingestion framework on Apache Spark and Delta Lake: adding a source is writing configuration, not code. It takes data from relational databases (JDBC) and file drops into a Bronze/Silver lakehouse. It is built to stay correct when several instances run at the same time and when a run dies halfway.

> **Status: Phases 2 to 6 are built.** Phase 2 is the PostgreSQL-coordinated LogStore: its protocol with every fault injected in one JVM (2a), and its proof with several processes, writers that are killed and a PostgreSQL that is killed (2b). Over 33 runs on two machines it acknowledged 10,954 commits and lost none. Phase 3 is the configuration: sources and profiles as files, validated whole and without a connection. Phase 4 is the control plane, in PostgreSQL: one run at a time on a source, by a lease with an epoch and a heartbeat; a plan written down before anything is read; and the state of a source, its batch and its run committed together or not at all. With six instances on one source, and runs that die at any step and wake up as zombies, every batch is in its table once. Phase 5 is the engine and its first connector: the new files of a directory, CSV, JSON or Parquet, into a table of Bronze, once, with the records that cannot be parsed in a quarantine of their source; a batch that a run left halfway finished by the next; a run that loses its lease stopped, and refused by the LogStore if it goes on; and two sources, in two processes, into one table. The command `run` is what an orchestrator starts. Phase 6 is the JDBC connector: a table of PostgreSQL read whole, as one snapshot, or by a watermark, as windows of the clock of its source that lose no change committed within a margin; in one statement with a cursor, with the types of the source and no value changed without a word; with nothing but quoted names written into a statement, and the password of a source in no log. `run` reads a source batch after batch, up to where its first plan saw, and a signal ends it after the batch in course. Recovery of every kind, reprocessing and backfills come next. The architecture is settled: [discovery document](docs/architecture/00-architecture-discovery.md) and [ADRs](docs/adr/README.md). Code is being built phase by phase (discovery §17).

## Stack

Java 17 · Scala 2.13.17 · Apache Spark 4.1.4 · Delta Lake 4.4.1 · sbt 1.13.1 · PostgreSQL 17. Versions and the reasons for them: [ADR-0002](docs/adr/0002-technology-stack.md), [ADR-0014](docs/adr/0014-stack-versions-pinned-at-p1.md).

## Modules

| Module | Responsibility |
|---|---|
| `core` | Domain and configuration model, validation, planning. No Spark, no JDBC. So far: the configuration, read from YAML and validated ([reference](docs/configuration/reference.md)); the model of the control plane and the recovery decision; and the plan of a batch, of files and of a window of a watermark. |
| [`delta-pg-logstore`](modules/delta-pg-logstore/README.md) | PostgreSQL-coordinated Delta `LogStore` for safe multi-writer tables. Depends on no other module. Its README has how to use it, the fault scenarios with their tests, and its known limitations. |
| `engine` | Connectors, Delta writers, control-plane repositories, engine service. So far: the control plane, the connector contract with the file connector and the JDBC connector, the writers of Bronze and of the quarantine, and `IngestionService`, one run of one source. |
| `cli` | Entry point, wiring, exit codes: the commands `validate`, `migrate`, `run`, `abandon-batch` and `reset-state`. |
| `integration-tests` | Testcontainers, end-to-end, fault-injection and stress tests |

The dependency rules ([ADR-0012](docs/adr/0012-module-structure.md)) are enforced on the resolved classpath by `sbt checkBoundaries`.

## Configuration

Adding a source is writing one file, [`conf/sources/<id>.yaml`](conf/sources/sales.orders.yaml): what is read, how it is loaded, and into which tables. Where the application runs is another file, the profile ([`conf/app/local.example.yaml`](conf/app/local.example.yaml)): its lake, its databases, how long it waits.

```bash
sbt "cli/run validate --profile conf/app/local.example.yaml --sources conf/sources"
```

```text
The configuration is valid: conf/app/local.example.yaml, and 2 sources.
The lake is file:///home/CHANGE_ME/lakehouse-data/lake.
  sales.orders   enabled  jdbc  incremental  bronze/sales/orders, silver/sales/orders  5828380c988b
  sales.returns  enabled  file  new_files    bronze/sales/returns                      2526a17a25b6
```

The last column is the start of the hash of the configuration that each source would run with. For a profile of your own, copy the example to `conf/app/local.yaml`, which git ignores, and put the directories of your machine in it.

`validate` reads the profile and every source and says everything that is wrong in them, each mistake with its file and its field. It opens no connection, so it runs anywhere in a moment, and it ends in 0 for a valid configuration and in 4 for one that is not.

- **A file is not trusted.** YAML is read without a loader that could construct anything, with limits on size, nesting and aliases, and no tag. Names and paths are allow-lists: `orders; DROP TABLE orders` is no table, and `bronze/../../x` is no path.
- **Nothing in a file is a secret.** A password is written as where it is (`env:NAME`, `file:/run/secrets/name`), and one that is written out is refused and never repeated in an error.
- **A field that the model does not have is an error**, with the one that was probably meant. A misspelt `watermark` does not become a full load that nobody asked for.
- **A configuration has a version**: the SHA-256 of the profile and the source as they ran, defaults included. A change that what is already ingested cannot follow, another key or another table to read from, is found by comparing two of them.

[Adding a source](docs/configuration/adding-a-source.md), step by step. [The reference](docs/configuration/reference.md) of every field, which a test holds to the code.

## Build

Prerequisites: JDK 17 and any recent `sbt` launcher. The launcher reads the build's sbt version from `project/build.properties`. Environment setup: [docs/setup/dev-environment.md](docs/setup/dev-environment.md).

```bash
sdk env                 # switch to the JDK pinned in .sdkmanrc (SDKMAN)
sbt compile             # compile all modules
sbt checkBoundaries     # verify the module dependency rules
sbt scalafmtCheckAll scalafmtSbtCheck   # formatting gate
sbt scalafmtAll scalafmtSbt             # reformat
sbt core/test cli/test                  # the configuration and the command line (no Docker, no Spark)
sbt delta-pg-logstore/test              # the LogStore module's unit tests (no Docker)
sbt integration-tests/test              # integration tests (needs Docker)
sbt "integration-tests/testOnly *DeltaSmokeSpec"   # Spark + Delta smoke test only (no Docker)
sbt "integration-tests/testOnly *DeltaLogStoreContractSpec"   # what Delta asks of a LogStore (no Docker)
sbt "integration-tests/testOnly *DeltaSetTransactionContractSpec"   # what Delta records of an idempotent write, and how it is read back (no Docker)
sbt "integration-tests/testOnly *JdbcConnectionProviderContractSpec"   # how Spark opens the connection of a read: its own provider logs the password at DEBUG, and the engine's is given where the password is (needs Docker)
sbt "integration-tests/testOnly *DeltaTableTransactionsSpec"   # which batches a real table has, and at which version (needs Docker)
sbt "integration-tests/testOnly *FileSourceConnectorSpec"   # the file connector on real files: CSV, JSON and Parquet as they are read, what is set apart, what is refused, and files that change under a plan (no Docker)
sbt "integration-tests/testOnly *BronzeWriterSpec"   # the writer of Bronze: once, with the engine's columns, and what it refuses (needs Docker)
sbt "integration-tests/testOnly *QuarantineWriterSpec"   # the quarantine of a source: a rejected record kept as it was, once (needs Docker)
sbt "integration-tests/testOnly *IngestionServiceSpec"   # a run from end to end: files to Bronze once, rejected records in quarantine and their threshold, a batch given up when its files change, a batch with no rows; a run that dies at each fault point and the next that finishes its batch; a run that lost its lease, refused by the LogStore (needs Docker)
sbt "integration-tests/testOnly *BreakingChangeSpec"   # a source whose configuration changed in a way that what was ingested cannot follow: refused before its planned batch is looked for, and run on with the configuration that it had (needs Docker)
sbt "integration-tests/testOnly *LeaseLossSpec"   # a run that loses its lease, with the control plane cut for real: its jobs stopped, no step after it, and how it ends (needs Docker)
sbt "integration-tests/testOnly *RunCommandSpec"   # the command run as an orchestrator starts it, each outcome with its exit code (needs Docker)
sbt "integration-tests/testOnly *SparkJdbcContractSpec"   # what Spark's JDBC source does with PostgreSQL, read from the server's log: the statements of a read, a bound under each time zone, each type as it arrives, a cursor and its timeout (needs Docker)
sbt "integration-tests/testOnly *JdbcSourceConnectorSpec"   # the JDBC connector against a real server: the plan of a table and of a window, each type as Bronze keeps it, infinity, the columns read as text, the settings of a session, TLS, and each failure with its class (needs Docker)
sbt "integration-tests/testOnly *JdbcIngestionSpec"   # a table through a run: snapshots, windows, their margin against the clock of the source, a run that dies at each fault point, a source cut by a proxy, and what a table must not have (needs Docker)
sbt "integration-tests/testOnly *JdbcRunCommandSpec"   # the command with a table of a database: the example source as the README runs it, a chain of windows in a process of its own, the source cut by a proxy (3), and a password, a table or a value that is refused (4): about three minutes (needs Docker)
sbt "integration-tests/testOnly *GeneratedFilesSpec"   # files made from a seed (a column more or less, broken records, empty and repeated files) through a run, compared with what the generator says is in them (needs Docker)
sbt "integration-tests/testOnly *GeneratedTableSpec"   # a table of PostgreSQL changed from a seed (inserts, updates, deletes, ties, transactions left open) and read after each round, by a watermark and whole, compared with what the generator says each batch is to have: about a minute (needs Docker)
sbt "integration-tests/testOnly *SqlInjectionSpec"   # what reaches a source as SQL: values that would be SQL read as data, names in mixed case, keywords and names that a source gives its columns, each quoted; a column that Delta cannot name refused before a plan (needs Docker)
sbt "integration-tests/testOnly *SourcePasswordSpec"   # run in a process of its own at DEBUG, with the password of a source and with a wrong one: neither in any log, file or row that it leaves (needs Docker)
sbt "integration-tests/testOnly *FanInSpec"   # two sources, in two processes at once, into one table of Bronze: about a minute and a half (needs Docker)
sbt "integration-tests/testOnly *WriterEpochRaceSpec"   # a commit under an epoch against the announcement of the next, in both orders, held in PostgreSQL (needs Docker)
sbt "integration-tests/testOnly *PostgresLogStoreDeltaSpec"   # Delta on the PostgreSQL-coordinated LogStore (needs Docker)
sbt "integration-tests/testOnly *PostgresLogStoreEndToEndSpec"   # the operations of an ingestion on it: idempotent append, MERGE, OPTIMIZE, VACUUM, checkpoints (needs Docker)
sbt "delta-pg-logstore/testOnly * -- -z F14"   # the tests of one fault scenario of ADR-0004, here F14
```

Tests that run Spark fork their own JVM with the Java 17 module options Spark needs (`project/SparkTestJvm.scala`), 1 GB of heap and UTC. `SparkJvmOptionsSpec` fails if a Spark upgrade changes those options and the build has not followed. The LogStore module's tests also run in a JVM of their own, because they use Hadoop's local file system.

## Continuous integration

GitHub Actions ([`ci.yml`](.github/workflows/ci.yml)) runs on every push to `main` and every pull request:

| Job | What it checks |
|---|---|
| Pinned images and actions | `scripts/check-pins.sh`: compose and Testcontainers images pinned by tag and digest, actions by commit SHA |
| Format, boundaries, compile and test | The same sbt command as locally (below): unit and integration tests, PostgreSQL through Testcontainers. Stress runs are not part of it. |
| Dependency submission | Sends the resolved sbt dependency graph to GitHub, which raises Dependabot alerts on known vulnerabilities |
| Dependency review | Pull requests only: fails if the change introduces a dependency with a known vulnerability of high severity or above |

The stress runs have a workflow of their own ([`stress.yml`](.github/workflows/stress.yml)), on demand and every Monday, and not on pull requests: they take more than an hour and are evidence under load, not a gate. It runs `scripts/stress-logstore.sh` with the natural race at eight writers and 500 commits each, shows what each run's verifier found in the job summary, and keeps every report as an artifact. A change to the LogStore is run through it by hand, on its branch, before it is merged.

Every job that runs sbt is set up by one local action ([`setup-build`](.github/actions/setup-build/action.yml)): the JDK, sbt, and the cache of what sbt downloads. The cache is found by the hash of the build definition, or failing that by the newest there is, so a change to the build downloads what it changed and not everything again. The job that runs the tests saves it; the others read it, so what another job needs is something the build itself has: the plugin that resolves the dependency graph is one of the build's plugins for that reason.

What is left to download is asked of Maven Central, and of Google's mirror of it when Central answers 429 (Too Many Requests): the runners share their addresses, and Central limits how much one address may ask for. sbt is given the two in that order ([`.github/sbt/repositories`](.github/sbt/repositories)), which covers a library whose description Central refused. One whose description came and whose jar was then refused is not asked of the mirror: sbt downloads a jar from where its description came. So a command that fails on a 429 is run again by `scripts/retry-on-429.sh` with the mirror first, and only such a command: any other failure is the build's own. Outside CI, sbt downloads from where it always does.

The workflow token is read-only; only the submission job may write, to the dependency graph. Dependabot proposes updates for the pinned actions. The security baseline behind these checks is [ADR-0013](docs/adr/0013-application-security-baseline.md) §4–5.

```bash
scripts/check-pins.sh
sbt scalafmtCheckAll scalafmtSbtCheck checkBoundaries Test/compile test   # needs Docker
```

## Stress runs

Several writer JVMs commit to one Delta table at the same time. A verifier then checks the result against what each writer saw succeed ([ADR-0004](docs/adr/0004-postgresql-coordinated-delta-logstore.md) §7). These runs take minutes, so `sbt test` leaves them out:

```bash
scripts/stress-logstore.sh   # runs `sbt stress` and lists the reports written: about 80 minutes, ~4 GB of free memory, Docker for the runs on PostgresLogStore
STRESS_NATURAL_RUNS=0 scripts/stress-logstore.sh   # without the ten natural runs of the GREEN spike: about 25 minutes
sbt "integration-tests/Stress/testOnly *SpikeGreenSpec"   # only the runs on PostgresLogStore (needs Docker)
sbt "integration-tests/Stress/testOnly *SpikeGreenKillSpec"   # only the writers killed inside a commit: about 9 minutes (needs Docker)
sbt "integration-tests/Stress/testOnly *SpikeGreenOutageSpec"   # only PostgreSQL killed in the middle of a run: about 6 minutes (needs Docker)
sbt "integration-tests/Stress/testOnly *SpikeGreenEpochSpec"   # only the writers that commit under an epoch and are taken over as they commit (needs Docker); STRESS_TAKEOVERS says how many times each
STRESS_WRITERS=8 STRESS_COMMITS=500 STRESS_NATURAL_RUNS=1 scripts/stress-logstore.sh   # the natural race at the size of the stress job in CI: about 9 GB of memory
scripts/stress-summary.sh   # one row for each report in target/stress: acknowledged, lost, and what the verifier found
```

Each run keeps its table, writer journals, logs and probes in `target/stress/<run>/`, next to its `report.md`.

### Results: Delta's default LogStore and `PostgresLogStore`

Three writer JVMs append to one table on `file://`, each in batches that carry a transaction identifier. RED is Delta's default configuration ([write-up](docs/spikes/logstore-red.md)). GREEN is the same harness and the same verifier on `PostgresLogStore` ([write-up](docs/spikes/logstore-green.md)). The runs are of 2026-11-13, 2026-11-16 and 2026-11-17, on one development machine, but for the one that says it was in CI; those under epochs are of 2026-11-19, on the same machine.

| | RED: Delta's default LogStore | GREEN: `PostgresLogStore` |
|---|---|---|
| **Widened window**: every writer is held at the point where a version is claimed | Lost a commit that its writer saw succeed in **7 runs of 7**, one of them in CI | **0 lost** of 60 commits in 2 runs, one on each side of the `INSERT`. Held before it, the writers found their version taken at the `INSERT` 52 times, and went on at the next one |
| **Natural race**: 3 writers × 200 commits, no fault injection | Lost commits in **2 runs of 7**, neither of them the one in CI | **0 lost** of 6,000 commits in 10 runs. Two writers were committing one version at the same time 376 times |
| **Natural race in CI**: 8 writers × 500 commits, on a runner of GitHub Actions | Not run at that size | **0 lost** of 4,000 commits in 1 run |
| **Every run on `PostgresLogStore`**, of the four suites, here and in CI | | **0 lost** of 10,954 commits in 33 runs: 6,477 in 21 runs on the development machine, 4,477 in the 12 runs of the stress job in CI |
| **A writer killed** with `kill -9` inside a commit, and started again | Not run | **0 lost, 0 written twice**, of 222 batches in 4 runs: a kill before the `INSERT`, one after it, one after the publish, and six at random moments |
| **PostgreSQL killed** in the middle of a run, and started again 30 s later | Does not apply | **0 lost, 0 written twice**, of 150 batches in 2 runs. Every attempt made meanwhile failed, 15 in each run, and was repeated |
| **Writers under epochs, taken over as they commit** ([ADR-0019](docs/adr/0019-a-commit-names-the-epoch-of-its-lease.md)): each process commits under an epoch, and a process under the next takes its writer over while it is still committing, and is not told | Does not apply | **0 lost, 0 written twice, and no commit of an epoch after a later one was announced**, of 510 batches in 2 runs. In one, each of 4 announcements was made while a commit of the earlier epoch was held with its version taken: every one waited for it, and was then told of that batch. In the other, 6 takeovers as they came, and the LogStore refused the next commit of all 6 processes that were taken over. With the lock taken out of the claim, the first run reports a commit after a later epoch at each of its 4 takeovers |
| **Readers** | The one that checks `.crc` sidecars fails on the replaced commit. The one that does not reads the table, without the commit | Both read every table |

The rows do not say equally much, and the write-ups keep them apart:

- **The widened window is the decisive evidence.** The race is made to happen on every commit, and the default LogStore fails in it every time.
- **The natural race is weak evidence.** The one RED run of the same day as GREEN lost nothing either: a clean natural run can be luck.
- **The kills at chosen points are the ones that count.** The six at random moments all fell outside a commit.
- **Two machines, local file systems, appends.** Three writers on a development machine, and eight in the stress job in CI, once. Object storage is not in these runs.

On the default LogStore Delta notices nothing: the lost commit's file is replaced, not removed, so the log has no gap. On `file://`, Hadoop's `.crc` sidecar usually goes stale, and every reader that checks it then fails. A reader that checks no sidecars, as on object storage, reads the table without error and without the commit. The fix is [ADR-0004](docs/adr/0004-postgresql-coordinated-delta-logstore.md): a version is claimed with an `INSERT` in PostgreSQL, and its commit is published as a hard link, which never replaces a file.

## Local services

One PostgreSQL 17 container hosts the control plane, the commit coordination store and a demo source database ([ADR-0003](docs/adr/0003-control-plane-in-postgresql.md)). The image is pinned by digest, and the container runs as a non-root user with no capabilities. It listens on `127.0.0.1` only.

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

Least-privilege roles: [ADR-0013](docs/adr/0013-application-security-baseline.md) §3. `PostgresBootstrapSpec` tests the bootstrap against the same image and scripts.

The tables of the two schemas, and the privileges on them, come from migrations: `engine` ships those of `control`, and `delta-pg-logstore` those of `delta_coordination` (`src/main/resources/db/migration/` in each). The command `migrate` applies them, as the role that owns the schemas. Its password is in no profile: the command is told where it is.

```bash
export INGEST_MIGRATOR_PASSWORD="$(cat docker/secrets/ingest_migrator_password)"
sbt "cli/run migrate --profile conf/app/local.yaml --password env:INGEST_MIGRATOR_PASSWORD"
```

```text
control: version 4, 4 migrations applied (jdbc:postgresql://localhost:5432/ingestion_control)
delta_coordination: version 4, 4 migrations applied (jdbc:postgresql://localhost:5432/ingestion_control)
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
| 0 | A batch committed, nothing new, or a source that is disabled |
| 2 | Another run holds the lease of the source: nothing was done |
| 3 | A database did not answer, the run lost its lease, or its files changed under it: run it again later |
| 4 | Something that running again does not mend: the configuration, a database that refused, a run that failed for good |
| 5 | More records were rejected than the source allows: nothing was committed, and they are in quarantine |
| 128 + n | The signal n came before the first run: nothing was run. A run stopped after one ends in the code of its last run |
| 64 | The command line is not one |

The whole contract is in [discovery §14.1](docs/architecture/00-architecture-discovery.md#141-exit-codes-the-contract-with-the-orchestrator). `RunCommandSpec` runs the command as an orchestrator does, a process of its own among them; `JdbcRunCommandSpec`, with a table of a database, cut for real by a proxy.

### When a source is stuck: `abandon-batch`

A batch is planned before anything is read, and every run replays its plan until it is committed. A plan that cannot be run, because the directory or the table it names is no longer the source's, stops the source. An operator gives it up:

```bash
sbt "cli/run abandon-batch --profile conf/app/local.yaml --sources conf/sources --source sales.returns --batch 3 --reason 'the landing directory was moved'"
```

It is a run of its own under the lease of the source, and records who asked (the user of the process) and why. It gives the batch up only when the table it was planned for is known not to have it: a batch whose rows are written is committed by the next run, and a table that cannot be read is asked again later (3). The next run plans anew from the state of the source. It mends a plan, not data: a file whose records cannot be read is planned again, and fails the same way ([ADR-0021](docs/adr/0021-operator-actions-and-backfills.md)).

| Code | Meaning |
|---|---|
| 0 | The batch was given up |
| 2 | A run holds the lease of the source: nothing was changed |
| 3 | A database did not answer, the table could not be read, or the action lost its lease |
| 4 | Refused, with nothing changed: no batch is planned, another is, or its table has it |
| 64 | The command line is not one: `--reason` and `--batch` are required |

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

The configuration of the reset is the one that later runs are compared with, and the change it let through is said with it. The numbers of the batches go on, since a table skips a number it has had, and the files that were taken stay taken. A reset is a run of its own under the lease of the source, with who asked and why, and writes the state before and after to `control.state_change_log` ([ADR-0021](docs/adr/0021-operator-actions-and-backfills.md)).

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

### A table of a database: `sales.orders`

The example source [`sales.orders`](conf/sources/sales.orders.yaml) reads `public.orders` of the database `sales` by `updated_at`, four weeks of changes a batch. The table is the source system's, and the engine makes none: a demo makes it and changes it as the source's applications would, as `sales_owner`. It is test code (`it.demo.SalesOrders`, with the generator of the tests), and is in no artifact of the application.

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

## License

[Apache License 2.0](LICENSE)
