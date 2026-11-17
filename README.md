# lakehouse-ingestion-framework

[![CI](https://github.com/MiguelOrtegaMilla/lakehouse-ingestion-framework/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/MiguelOrtegaMilla/lakehouse-ingestion-framework/actions/workflows/ci.yml)

A metadata-driven ingestion framework on Apache Spark and Delta Lake: adding a source is writing configuration, not code. It takes data from relational databases (JDBC) and file drops into a Bronze/Silver lakehouse. It is built to stay correct when several instances run at the same time and when a run dies halfway.

> **Status: Phase 2a (the PostgreSQL-coordinated LogStore, tested in one JVM and with Delta) is complete; Phase 2b (its proof with several processes) is next.** The architecture is settled: [discovery document](docs/architecture/00-architecture-discovery.md) and [ADRs](docs/adr/README.md). Code is being built phase by phase (discovery §17).

## Stack

Java 17 · Scala 2.13.17 · Apache Spark 4.1.4 · Delta Lake 4.4.1 · sbt 1.13.1 · PostgreSQL 17. Versions and the reasons for them: [ADR-0002](docs/adr/0002-technology-stack.md), [ADR-0014](docs/adr/0014-stack-versions-pinned-at-p1.md).

## Modules

| Module | Responsibility |
|---|---|
| `core` | Domain and configuration model, validation, planning. No Spark, no JDBC. |
| [`delta-pg-logstore`](modules/delta-pg-logstore/README.md) | PostgreSQL-coordinated Delta `LogStore` for safe multi-writer tables. Depends on no other module. Its README has how to use it, the fault scenarios with their tests, and its known limitations. |
| `engine` | Connectors, Delta writers, control-plane repositories, engine service |
| `cli` | Entry point, wiring, exit codes |
| `integration-tests` | Testcontainers, end-to-end, fault-injection and stress tests |

The dependency rules ([ADR-0012](docs/adr/0012-module-structure.md)) are enforced on the resolved classpath by `sbt checkBoundaries`.

## Build

Prerequisites: JDK 17 and any recent `sbt` launcher. The launcher reads the build's sbt version from `project/build.properties`. Environment setup: [docs/setup/dev-environment.md](docs/setup/dev-environment.md).

```bash
sdk env                 # switch to the JDK pinned in .sdkmanrc (SDKMAN)
sbt compile             # compile all modules
sbt checkBoundaries     # verify the module dependency rules
sbt scalafmtCheckAll scalafmtSbtCheck   # formatting gate
sbt scalafmtAll scalafmtSbt             # reformat
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

The workflow token is read-only; only the submission job may write, to the dependency graph. Dependabot proposes updates for the pinned actions. The security baseline behind these checks is [ADR-0013](docs/adr/0013-application-security-baseline.md) §4–5.

```bash
scripts/check-pins.sh
sbt scalafmtCheckAll scalafmtSbtCheck checkBoundaries Test/compile test   # needs Docker
```

## Stress runs

Several writer JVMs commit to one Delta table at the same time. A verifier then checks the result against what each writer saw succeed ([ADR-0004](docs/adr/0004-postgresql-coordinated-delta-logstore.md) §7). These runs take minutes, so `sbt test` leaves them out:

```bash
scripts/stress-logstore.sh   # runs `sbt stress` and lists the reports written: about 65 minutes, ~4 GB of free memory, Docker for the runs on PostgresLogStore
STRESS_NATURAL_RUNS=0 scripts/stress-logstore.sh   # without the ten natural runs of the GREEN spike: about 12 minutes
sbt "integration-tests/Stress/testOnly *SpikeGreenSpec"   # only the runs on PostgresLogStore (needs Docker)
```

Each run keeps its table, writer journals, logs and probes in `target/stress/<run>/`, next to its `report.md`.

### Spike RED: Delta's default LogStore on `file://`

Three writer JVMs append to one table with Delta's default configuration ([full write-up](docs/spikes/logstore-red.md)):

| Variant | Runs that lost a commit its writer saw succeed |
|---|---|
| Natural race (3 × 200 commits, no fault injection) | **2 of 5** |
| Widened window (a delay just before the rename that publishes the commit) | **5 of 5** |

Delta notices nothing: the lost commit's file is replaced, not removed, so the log has no gap. On `file://`, Hadoop's `.crc` sidecar usually goes stale, and every reader that checks it then fails. A reader that checks no sidecars, as on object storage, reads the table without error and without the commit. The fix is [ADR-0004](docs/adr/0004-postgresql-coordinated-delta-logstore.md). The same suite on `PostgresLogStore` lost no commit: [Spike GREEN](docs/spikes/logstore-green.md), which is completed as P2b goes on.

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

The tables of `delta_coordination`, and the privileges on them, come from the migrations that `delta-pg-logstore` ships (`src/main/resources/db/migration/delta_coordination`). `CoordinationSchemaSpec` applies them with Flyway, as `ingest_migrator`, and tests what each role may and may not do.

## License

[Apache License 2.0](LICENSE)
