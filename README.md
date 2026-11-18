# lakehouse-ingestion-framework

[![CI](https://github.com/MiguelOrtegaMilla/lakehouse-ingestion-framework/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/MiguelOrtegaMilla/lakehouse-ingestion-framework/actions/workflows/ci.yml)

A metadata-driven ingestion framework on Apache Spark and Delta Lake: adding a source is writing configuration, not code. It takes data from relational databases (JDBC) and file drops into a Bronze/Silver lakehouse. It is built to stay correct when several instances run at the same time and when a run dies halfway.

> **Status: Phases 2 and 3 are built.** Phase 2 is the PostgreSQL-coordinated LogStore: its protocol with every fault injected in one JVM (2a), and its proof with several processes, writers that are killed and a PostgreSQL that is killed (2b). Over 33 runs on two machines it acknowledged 10,954 commits and lost none. Phase 3 is the configuration: sources and profiles as files, validated whole and without a connection. The engine that runs a source comes next. The architecture is settled: [discovery document](docs/architecture/00-architecture-discovery.md) and [ADRs](docs/adr/README.md). Code is being built phase by phase (discovery §17).

## Stack

Java 17 · Scala 2.13.17 · Apache Spark 4.1.4 · Delta Lake 4.4.1 · sbt 1.13.1 · PostgreSQL 17. Versions and the reasons for them: [ADR-0002](docs/adr/0002-technology-stack.md), [ADR-0014](docs/adr/0014-stack-versions-pinned-at-p1.md).

## Modules

| Module | Responsibility |
|---|---|
| `core` | Domain and configuration model, validation, planning. No Spark, no JDBC. So far: the configuration, read from YAML and validated ([reference](docs/configuration/reference.md)). |
| [`delta-pg-logstore`](modules/delta-pg-logstore/README.md) | PostgreSQL-coordinated Delta `LogStore` for safe multi-writer tables. Depends on no other module. Its README has how to use it, the fault scenarios with their tests, and its known limitations. |
| `engine` | Connectors, Delta writers, control-plane repositories, engine service |
| `cli` | Entry point, wiring, exit codes. So far: the command `validate`. |
| `integration-tests` | Testcontainers, end-to-end, fault-injection and stress tests |

The dependency rules ([ADR-0012](docs/adr/0012-module-structure.md)) are enforced on the resolved classpath by `sbt checkBoundaries`.

## Configuration

Adding a source is writing one file, [`conf/sources/<id>.yaml`](conf/sources/sales.orders.yaml): what is read, how it is loaded, and into which tables. Where the application runs is another file, the profile ([`conf/app/local.example.yaml`](conf/app/local.example.yaml)): its lake, its databases, how long it waits.

```bash
sbt "cli/run validate --profile conf/app/local.example.yaml --sources conf/sources"
```

```text
The configuration is valid: conf/app/local.example.yaml, and 2 sources.
The lake is file:///home/CHANGE_ME/lakehouse/lake.
  sales.orders   enabled  jdbc  incremental  bronze/sales/orders, silver/sales/orders  78dd136c673a
  sales.returns  enabled  file  new_files    bronze/sales/returns                      0e31fa35b306
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
sbt "integration-tests/testOnly lakehouse.ingestion.it.delta.*"   # Spark + Delta smoke test only (no Docker)
sbt "integration-tests/testOnly *DeltaLogStoreContractSpec"   # what Delta asks of a LogStore (no Docker)
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
STRESS_WRITERS=8 STRESS_COMMITS=500 STRESS_NATURAL_RUNS=1 scripts/stress-logstore.sh   # the natural race at the size of the stress job in CI: about 9 GB of memory
scripts/stress-summary.sh   # one row for each report in target/stress: acknowledged, lost, and what the verifier found
```

Each run keeps its table, writer journals, logs and probes in `target/stress/<run>/`, next to its `report.md`.

### Results: Delta's default LogStore and `PostgresLogStore`

Three writer JVMs append to one table on `file://`, each in batches that carry a transaction identifier. RED is Delta's default configuration ([write-up](docs/spikes/logstore-red.md)). GREEN is the same harness and the same verifier on `PostgresLogStore` ([write-up](docs/spikes/logstore-green.md)). The runs are of 2026-11-13, 2026-11-16 and 2026-11-17, on one development machine, but for the one that says it was in CI.

| | RED: Delta's default LogStore | GREEN: `PostgresLogStore` |
|---|---|---|
| **Widened window**: every writer is held at the point where a version is claimed | Lost a commit that its writer saw succeed in **7 runs of 7**, one of them in CI | **0 lost** of 60 commits in 2 runs, one on each side of the `INSERT`. Held before it, the writers found their version taken at the `INSERT` 52 times, and went on at the next one |
| **Natural race**: 3 writers × 200 commits, no fault injection | Lost commits in **2 runs of 7**, neither of them the one in CI | **0 lost** of 6,000 commits in 10 runs. Two writers were committing one version at the same time 376 times |
| **Natural race in CI**: 8 writers × 500 commits, on a runner of GitHub Actions | Not run at that size | **0 lost** of 4,000 commits in 1 run |
| **Every run on `PostgresLogStore`**, of the four suites, here and in CI | | **0 lost** of 10,954 commits in 33 runs: 6,477 in 21 runs on the development machine, 4,477 in the 12 runs of the stress job in CI |
| **A writer killed** with `kill -9` inside a commit, and started again | Not run | **0 lost, 0 written twice**, of 222 batches in 4 runs: a kill before the `INSERT`, one after it, one after the publish, and six at random moments |
| **PostgreSQL killed** in the middle of a run, and started again 30 s later | Does not apply | **0 lost, 0 written twice**, of 150 batches in 2 runs. Every attempt made meanwhile failed, 15 in each run, and was repeated |
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

The bootstrap script (`docker/postgres/init/`) runs only on an empty volume. After changing it, recreate the volume with `down -v`.

| Database | Contents | Roles that can connect |
|---|---|---|
| `ingestion_control` | Schemas `control` (control plane) and `delta_coordination` (commit coordination), both owned by `ingest_migrator` | `ingest_migrator` (DDL through migrations), `ingest_control`, `ingest_coordination`, `ingest_maintenance` |
| `sales` | Demo source system for the JDBC connector | `sales_ro` (read-only) |

Least-privilege roles: [ADR-0013](docs/adr/0013-application-security-baseline.md) §3. `PostgresBootstrapSpec` tests the bootstrap against the same image and scripts.

The tables of the two schemas, and the privileges on them, come from migrations: `engine` ships those of `control`, and `delta-pg-logstore` those of `delta_coordination` (`src/main/resources/db/migration/` in each). The command `migrate` applies them, as the role that owns the schemas. Its password is in no profile: the command is told where it is.

```bash
export INGEST_MIGRATOR_PASSWORD="$(cat docker/secrets/ingest_migrator_password)"
sbt "cli/run migrate --profile conf/app/local.yaml --password env:INGEST_MIGRATOR_PASSWORD"
```

```text
control: version 1, 1 migration applied (jdbc:postgresql://localhost:5432/ingestion_control)
delta_coordination: version 3, 3 migrations applied (jdbc:postgresql://localhost:5432/ingestion_control)
```

It can be run again at any time: a schema that is at its version is left as it is. The engine and the LogStore run no DDL. Each checks the version of its schema before it touches a table, and refuses to work on another.

`ControlSchemaSpec` and `CoordinationSchemaSpec` test what the constraints of each schema guarantee whatever the code does, and what each role may and may not do; `MigrateCommandSpec`, the command against a database as the bootstrap leaves it.

## License

[Apache License 2.0](LICENSE)
