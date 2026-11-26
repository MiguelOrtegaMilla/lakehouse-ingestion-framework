# delta-pg-logstore

A Delta Lake `LogStore` that takes the mutual exclusion of commits from PostgreSQL, so that several processes can write one Delta table on storage that gives none. It depends on no other module of this repository.

- Design and decisions: [ADR-0004](../../docs/adr/0004-postgresql-coordinated-delta-logstore.md), with [ADR-0015](../../docs/adr/0015-hard-link-publish-on-local-file-systems.md) (the publish), [ADR-0016](../../docs/adr/0016-delta-storage-provided-to-the-logstore-module.md) (the dependency on `delta-storage`) and [ADR-0017](../../docs/adr/0017-a-failed-publish-is-reported-not-swallowed.md) (a publish that fails).
- The problem it solves, measured: [the RED spike](../../docs/spikes/logstore-red.md). Three processes that append to one table with Delta's default LogStore on `file://` lose commits their writers saw succeed.

## How a commit goes

Delta asks a LogStore to write `<version>.json`, and expects that only one writer can. On a local disk nothing makes that true across processes. Here a row in PostgreSQL does: the primary key `(table, version)`.

1. **Prepare.** The commit's bytes go to a temporary file in `_delta_log/.tmp/`, forced to disk.
2. **Take the version.** One `INSERT`. This is where the commit happens: the version belongs to this writer from then on, even if it dies. A writer that loses gets `java.nio.file.FileAlreadyExistsException`, which Delta answers by checking for a conflict and trying the next version.
3. **Publish.** The temporary file gets the commit's name with a hard link: the name appears with the whole file behind it, and never replaces one.
4. **Acknowledge.** The row is marked complete.

A writer that dies after step 2 leaves a row that is not complete. Whoever comes next finishes the commit: a writer before it writes the next version, a reader before it lists the log. Steps 3 and 4 can be repeated by anybody, any number of times.

Everything fails closed. If PostgreSQL does not answer, nothing is written and nothing is listed.

## Using it

**The schema.** The migrations are on the classpath, under `db/migration/delta_coordination`. Run them with Flyway as the role that owns schema `delta_coordination`; in this repository the command `migrate` of the application does. The LogStore runs no DDL: it checks the schema version and refuses to work on another one.

**The roles** ([ADR-0013](../../docs/adr/0013-application-security-baseline.md) §3). The commit path connects as a role that can add rows and mark an entry complete, and nothing else. An operator's tools connect as another one.

**Spark.** Each LogStore setting takes the prefix `spark.hadoop.`:

```properties
spark.delta.logStore.file.impl=lakehouse.ingestion.logstore.PostgresLogStore
spark.hadoop.lakehouse.logstore.jdbc.url=jdbc:postgresql://db.internal:5432/ingestion_control
spark.hadoop.lakehouse.logstore.jdbc.user=ingest_coordination
spark.hadoop.lakehouse.logstore.jdbc.passwordRef=file:/run/secrets/ingest_coordination
```

The password is a reference, `env:NAME` or `file:/path`, resolved in the JVM that connects. The pool size and the timeouts have defaults; all the keys are in ADR-0004 §3.9.

**At startup**, before a table is touched:

```scala
StartupGuard.require(spark.sparkContext.hadoopConfiguration, new Path(lakeRoot))
```

It refuses to go on, with every problem at once, if Delta is not told to use this LogStore for the lake's scheme, if the lake root cannot hold a coordinated log, or if the database or its schema is not usable.

**Attribution** (optional). The commits a thread makes inside a context record the run they are for, and the context counts the versions they lose to other writers:

```scala
val commits = CommitContext.forRun(runId)
commits.attribute { rows.write.format("delta").mode("append").save(path) }
commits.contention          // versions lost to other writers
commits.contentionAtInsert  // of them, the ones lost while both were committing the version
```

**Epochs** (optional, ADR-0019). An application that lets one writer at a time do some work, and numbers the writers that take it over, can have the LogStore hold each writer to its turn. The writer announces its epoch before it asks a table anything, and commits inside a context that names it:

```scala
val turn = WriterEpoch("sales.orders/bronze", leaseEpoch)
if (WriterEpochs.announce(spark.sessionState.newHadoopConf(), turn) != turn.epoch) stop() // a later one is in force
val commits = CommitContext.forRun(runId, turn)
commits.attribute { rows.write.format("delta").mode("append").save(path) }
```

A commit made in such a context takes its version only while its epoch is the one in force under its key. Otherwise it is refused with `EpochNotInForceException`, which is not retried, by the LogStore or by Delta: the writer has lost its turn. A claim and an announcement of one key exclude each other in the database, so once an announcement has returned, a commit under an earlier epoch is either recorded, where the next reader of the table finds it, or will never be. A commit that names no epoch is held to none.

**Tables.** A table created through the LogStore is registered by its first commit. A table that exists already is adopted (below). Commits to any other table are refused. `CoordinatedTables.registered(conf, path)` says whether a table is registered: one that is not has no commit made through the LogStore, which a directory that is not there cannot say, since it may be on a disk that is not mounted.

## What an operator does

`TableMaintenance`, with the operator's role. Each operation looks at the table's log first and refuses what the log does not bear out. They are for a table whose writers are stopped. A writer that was not is neither harmed nor does harm:

- a registration has a generation, which the database changes when a table is adopted again and which goes with the registration when a table is reset. A writer's claim names the generation it read, and gets no version under another: `RegistrationChangedException`, and the transaction starts again;
- `readopt` and `reset` change the registry only if its entries are still the ones they examined. If a writer took or finished a version in between, the table is examined again, and a table that is written every time is refused.

| Operation | For |
|---|---|
| `adopt` | A table made without the LogStore: it is registered at the version its log is at |
| `readopt` | A table that is tainted, or whose registry and log disagree, once somebody has looked at its log and accepts it as it stands |
| `reset` | A table that was dropped: its registry is removed, so that one can be created at its path again |
| `collectTemps` | Any table: removes the temporary files of published commits, of commits that Delta's log cleanup has expired, and the orphans of writers that died |

The states that call for each are in ADR-0004 §3.5 and §3.8.

Each operation is given who asked for it and why (`MaintenanceRequest`), and is recorded in `delta_coordination.maintenance_log` with what it found and changed: `adopt`, `readopt` and `reset` in the transaction that changes the registry, so an operation that is refused or called off leaves no row; `collectTemps`, once the files are removed, and only by a role that may write the log: one that may not is refused before a file is looked at (`RegistryMaintenance.mayRecord`). The maintenance role may insert a row and do nothing else to the log; an audit reads it as the owner of the schema. The application runs these operations with its command `maintain-table`.

## Scenarios and their tests

The fault scenarios are F1 to F17 of ADR-0004 §7.1. Each is tested up to three ways:

- **One JVM**, in this module (`sbt delta-pg-logstore/test`, no Docker): the LogStore is driven through the methods Delta calls, on a real directory, over a coordination store that fails on request and fault hooks that stop a writer between two steps.
- **A real PostgreSQL**, in `integration-tests` (needs Docker): the same scenarios with the database behind a proxy that cuts connections, or in a container that is paused or stopped.
- **Delta itself** on the LogStore, in `integration-tests`: where a scenario is worth showing with the engine the LogStore is written for.

A test of a scenario has its number in its name, as `F14:` or `after F14`, so `sbt "delta-pg-logstore/testOnly * -- -z F14"` runs the ones of F14. The few that do not are quoted.

| # | Scenario | One JVM | Real PostgreSQL | Delta |
|---|---|---|---|---|
| F1 | Crash before the `INSERT` | `PostgresLogStoreWriteSpec`, `TableMaintenanceSpec` | `PostgresLogStoreFaultSpec`, `PostgresMaintenanceAndGuardSpec` | |
| F2 | Crash after the `INSERT`, before the publish | `PostgresLogStoreReadSpec`; `PostgresLogStoreWriteSpec` ("a commit that was taken and not published…") | `PostgresLogStoreFaultSpec` | `PostgresLogStoreDeltaSpec` ("a commit that a dead writer took…"), `PostgresLogStoreEndToEndSpec` ("a reader that starts from a checkpoint…") |
| F3 | Crash during the publish | `PostgresLogStoreReadSpec`; `LocalLogStorageSpec` ("a reader never finds part of a commit…") | `PostgresLogStoreFaultSpec` | |
| F4 | Crash after the publish, before the acknowledgement | `PostgresLogStoreReadSpec`; `PostgresLogStoreWriteSpec` ("a commit that was published and not acknowledged…") | `PostgresLogStoreFaultSpec` | |
| F5 | PostgreSQL down before the `INSERT` | `PostgresLogStoreWriteSpec` | `PostgresLogStoreFaultSpec` | |
| F6 | PostgreSQL down after the `INSERT` | `PostgresLogStoreWriteSpec` | `PostgresLogStoreFaultSpec` | |
| F7 | PostgreSQL down during a listing | `PostgresLogStoreReadSpec` | `PostgresLogStoreFaultSpec` | |
| F8 | Several recoverers of one commit at once | `PostgresLogStoreReadSpec`; `LocalLogStorageSpec` ("of many recoverers…") | `PostgresLogStoreFaultSpec` | |
| F9 | Table deleted and created again at its path | `PostgresLogStoreWriteSpec`, `TableMaintenanceSpec` | `PostgresLogStoreFaultSpec`, `PostgresMaintenanceAndGuardSpec` | `PostgresLogStoreEndToEndSpec` |
| F10 | A commit made without the LogStore | `UncoordinatedWriterSpec`, `TableMaintenanceSpec` | `PostgresLogStoreFaultSpec` | `PostgresLogStoreDeltaSpec` ("a commit made without the LogStore…") |
| F11 | A foreign commit lands between our `INSERT` and our publish | `UncoordinatedWriterSpec`, `TableMaintenanceSpec` | `PostgresLogStoreFaultSpec`, `PostgresMaintenanceAndGuardSpec` | |
| F12 | Commit to a table that is not registered | `PostgresLogStoreWriteSpec` | `PostgresLogStoreFaultSpec`; `CoordinationSchemaSpec` ("the database refuses an entry or a taint…") | |
| F13 | Schema missing or at another version | Needs the database | `CoordinationSchemaCheckSpec`, `PostgresMaintenanceAndGuardSpec` | |
| F14 | The latest commit is recorded as published and is not in the log | `PostgresLogStoreReadSpec`, `PostgresLogStoreWriteSpec`, `TableMaintenanceSpec` | `PostgresLogStoreFaultSpec` | `PostgresLogStoreEndToEndSpec` ("Delta's log cleanup…": the cleanup never causes it) |
| F15 | The outcome of the `INSERT` is unknown | `PostgresLogStoreWriteSpec`, `CommitContextSpec` | `PostgresLogStoreFaultSpec`; `PostgresCoordinationFaultSpec` ("a claim whose reply is lost…") | |
| F16 | The publish fails without a crash | `PostgresLogStoreWriteSpec` | `PostgresLogStoreFaultSpec` | `PostgresLogStoreEndToEndSpec` |
| F17 | An unfinished commit whose temporary file is gone | `PostgresLogStoreReadSpec`, `TableMaintenanceSpec`; `PostgresLogStoreWriteSpec` ("a taken commit whose temporary file is gone…") | `PostgresLogStoreFaultSpec` | |

Around the scenarios:

- **What Delta asks of a LogStore** is pinned by `DeltaLogStoreContractSpec`, over a LogStore that records its calls: which files Delta writes through it, on which thread, how it retries.
- **Every implementation of the coordination store and of the registry's maintenance** passes one contract suite: `CoordinationStoreContract` and `RegistryMaintenanceContract`, in memory and on PostgreSQL.
- **The epoch of a writer** (ADR-0019) is in that contract, in `PostgresLogStoreWriteSpec` and `CommitContextSpec` for the protocol, in `PostgresLogStoreDeltaSpec` for Delta, and in `WriterEpochRaceSpec`, which forces both orders of a claim and an announcement on a real PostgreSQL with transactions that are left open.
- **The operations of an ingestion** run with Delta in `PostgresLogStoreEndToEndSpec`: an idempotent append, `MERGE`, `UPDATE`, `DELETE`, `OPTIMIZE`, `VACUUM`, checkpoints, the log cleanup, and the creation of a table by two transactions at once.
- **Several processes at once**, and processes killed at random, are the stress run of P2b. It also has the epochs (`SpikeGreenEpochSpec`): every writer JVM commits under an epoch and is taken over, while it commits, by a process under the next, and the verifier holds the run to no commit of an epoch after a later one was announced.

## Known limitations

**By design**

- **PostgreSQL is on the read path and on the write path.** While it is down, the tables can be neither written nor read.
- **Every writer of a table must use this LogStore.** A writer without it is detected (the table is tainted and refuses writes), not prevented. One case is not detected: a foreign writer that replaces a commit of ours after it was published (ADR-0004 §3.5).
- **A commit that was reported as failed may exist.** If the reply to the `INSERT` is lost and the database stays out of reach (F15), or the publish fails (F16), the writer is told so with an exception and the commit appears later. A caller must look at the table before writing the same data again; with Delta, an idempotent write (`txnAppId`, `txnVersion`) does that.
- **A reader may have to write.** Whoever finds an unfinished commit publishes it, so every process that reads a table needs write access to its log, as one operating-system user (ADR-0015).
- **The database must not lose a transaction it acknowledged.** If it does, a published commit has no entry and the table is tainted: closed, and a false alarm.
- **In a cluster, executors may need the database**: resolving a timestamp to a version over a long history lists the log in tasks (ADR-0004 §3.9).

- **The epochs of a key are forgotten by hand.** An epoch in force only grows. An application that starts its numbers again, as one whose own database is made anew, has every commit refused until an operator deletes the rows of its keys from `delta_coordination.writer_epoch`, with the maintenance role (ADR-0019).

**Not built**

- **Only `file://`.** The storage interface has one implementation; other schemes are refused at startup. `s3a://` is planned (E1).
- **Entries are never pruned.** The registry grows by one row for each commit.

**Found by the review of P2a, and open**

- **The schema version is checked once.** A store checks it before its first operation. A process that is running when a later migration is applied does not notice until it restarts. With V3 it does not go on unnoticed for long: its next claim names no generation, and the database refuses it. V4 does not stop it: its claims are those of version 3, which name no epoch, and are taken as such. A writer of the version before is therefore held to no epoch until it is restarted, and writers are to be stopped for that migration.
- **A taint is not held to a generation.** A reader that stalls between finding a foreign commit and recording it, while the table is dropped, its registry reset and a table created in its place, taints the new table: closed, and a false alarm. (A writer's claim is held to the registration it read; see above.)
- **The table id in the registry is informative.** A table created through the LogStore is registered with the `metaData.id` of the first writer that tried to create it. If that writer lost version 0 to another, the id in the registry is not the table's. Nothing reads it; the log is the source.
- **`collectTemps` is not built for large logs.** It asks the registry once for each temporary file, and compares a file with its commit by reading both.
