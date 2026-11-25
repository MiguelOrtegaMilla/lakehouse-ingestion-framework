# Building, testing and continuous integration

Every command of the build, each suite with what it shows, what CI runs on every change, and the stress runs of the LogStore with their results. The [README](../../README.md) has the short version.

## Build

Prerequisites: JDK 17 and any recent `sbt` launcher. The launcher reads the build's sbt version from `project/build.properties`. Environment setup: [docs/setup/dev-environment.md](../setup/dev-environment.md).

```bash
sdk env                 # switch to the JDK pinned in .sdkmanrc (SDKMAN)
sbt compile             # compile all modules
sbt checkBoundaries     # verify the module dependency rules
sbt checkUnconnected    # no code of production that only the tests reach (compiles the tests)
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

GitHub Actions ([`ci.yml`](../../.github/workflows/ci.yml)) runs on every push to `main` and every pull request:

| Job | What it checks |
|---|---|
| Pinned images and actions | `scripts/check-pins.sh`: compose and Testcontainers images pinned by tag and digest, actions by commit SHA |
| Format, boundaries, compile and test | The same sbt command as locally (below): unit and integration tests, PostgreSQL through Testcontainers. Stress runs are not part of it. |
| Dependency submission | Sends the resolved sbt dependency graph to GitHub, which raises Dependabot alerts on known vulnerabilities |
| Dependency review | Pull requests only: fails if the change introduces a dependency with a known vulnerability of high severity or above |

The stress runs have a workflow of their own ([`stress.yml`](../../.github/workflows/stress.yml)), on demand and every Monday, and not on pull requests: they take more than an hour and are evidence under load, not a gate. It runs `scripts/stress-logstore.sh` with the natural race at eight writers and 500 commits each, shows what each run's verifier found in the job summary, and keeps every report as an artifact. A change to the LogStore is run through it by hand, on its branch, before it is merged.

Every job that runs sbt is set up by one local action ([`setup-build`](../../.github/actions/setup-build/action.yml)): the JDK, sbt, and the cache of what sbt downloads. The cache is found by the hash of the build definition, or failing that by the newest there is, so a change to the build downloads what it changed and not everything again. The job that runs the tests saves it; the others read it, so what another job needs is something the build itself has: the plugin that resolves the dependency graph is one of the build's plugins for that reason.

What is left to download is asked of Maven Central, and of Google's mirror of it when Central answers 429 (Too Many Requests): the runners share their addresses, and Central limits how much one address may ask for. sbt is given the two in that order ([`.github/sbt/repositories`](../../.github/sbt/repositories)), which covers a library whose description Central refused. One whose description came and whose jar was then refused is not asked of the mirror: sbt downloads a jar from where its description came. So a command that fails on a 429 is run again by `scripts/retry-on-429.sh` with the mirror first, and only such a command: any other failure is the build's own. Outside CI, sbt downloads from where it always does.

The workflow token is read-only; only the submission job may write, to the dependency graph. Dependabot proposes updates for the pinned actions. The security baseline behind these checks is [ADR-0013](../adr/0013-application-security-baseline.md) §4–5.

```bash
scripts/check-pins.sh
sbt scalafmtCheckAll scalafmtSbtCheck checkBoundaries checkUnconnected Test/compile test   # needs Docker
```

## Stress runs

Several writer JVMs commit to one Delta table at the same time. A verifier then checks the result against what each writer saw succeed ([ADR-0004](../adr/0004-postgresql-coordinated-delta-logstore.md) §7). These runs take minutes, so `sbt test` leaves them out:

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

Three writer JVMs append to one table on `file://`, each in batches that carry a transaction identifier. RED is Delta's default configuration ([write-up](../spikes/logstore-red.md)). GREEN is the same harness and the same verifier on `PostgresLogStore` ([write-up](../spikes/logstore-green.md)). The runs are of 2026-11-13, 2026-11-16 and 2026-11-17, on one development machine, but for the one that says it was in CI; those under epochs are of 2026-11-19, on the same machine.

| | RED: Delta's default LogStore | GREEN: `PostgresLogStore` |
|---|---|---|
| **Widened window**: every writer is held at the point where a version is claimed | Lost a commit that its writer saw succeed in **7 runs of 7**, one of them in CI | **0 lost** of 60 commits in 2 runs, one on each side of the `INSERT`. Held before it, the writers found their version taken at the `INSERT` 52 times, and went on at the next one |
| **Natural race**: 3 writers × 200 commits, no fault injection | Lost commits in **2 runs of 7**, neither of them the one in CI | **0 lost** of 6,000 commits in 10 runs. Two writers were committing one version at the same time 376 times |
| **Natural race in CI**: 8 writers × 500 commits, on a runner of GitHub Actions | Not run at that size | **0 lost** of 4,000 commits in 1 run |
| **Every run on `PostgresLogStore`**, of the four suites, here and in CI | | **0 lost** of 10,954 commits in 33 runs: 6,477 in 21 runs on the development machine, 4,477 in the 12 runs of the stress job in CI |
| **A writer killed** with `kill -9` inside a commit, and started again | Not run | **0 lost, 0 written twice**, of 222 batches in 4 runs: a kill before the `INSERT`, one after it, one after the publish, and six at random moments |
| **PostgreSQL killed** in the middle of a run, and started again 30 s later | Does not apply | **0 lost, 0 written twice**, of 150 batches in 2 runs. Every attempt made meanwhile failed, 15 in each run, and was repeated |
| **Writers under epochs, taken over as they commit** ([ADR-0019](../adr/0019-a-commit-names-the-epoch-of-its-lease.md)): each process commits under an epoch, and a process under the next takes its writer over while it is still committing, and is not told | Does not apply | **0 lost, 0 written twice, and no commit of an epoch after a later one was announced**, of 510 batches in 2 runs. In one, each of 4 announcements was made while a commit of the earlier epoch was held with its version taken: every one waited for it, and was then told of that batch. In the other, 6 takeovers as they came, and the LogStore refused the next commit of all 6 processes that were taken over. With the lock taken out of the claim, the first run reports a commit after a later epoch at each of its 4 takeovers |
| **Readers** | The one that checks `.crc` sidecars fails on the replaced commit. The one that does not reads the table, without the commit | Both read every table |

The rows do not say equally much, and the write-ups keep them apart:

- **The widened window is the decisive evidence.** The race is made to happen on every commit, and the default LogStore fails in it every time.
- **The natural race is weak evidence.** The one RED run of the same day as GREEN lost nothing either: a clean natural run can be luck.
- **The kills at chosen points are the ones that count.** The six at random moments all fell outside a commit.
- **Two machines, local file systems, appends.** Three writers on a development machine, and eight in the stress job in CI, once. Object storage is not in these runs.

On the default LogStore Delta notices nothing: the lost commit's file is replaced, not removed, so the log has no gap. On `file://`, Hadoop's `.crc` sidecar usually goes stale, and every reader that checks it then fails. A reader that checks no sidecars, as on object storage, reads the table without error and without the commit. The fix is [ADR-0004](../adr/0004-postgresql-coordinated-delta-logstore.md): a version is claimed with an `INSERT` in PostgreSQL, and its commit is published as a hard link, which never replaces a file.
