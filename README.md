# lakehouse-ingestion-framework

A metadata-driven ingestion framework on Apache Spark and Delta Lake: adding a source is writing configuration, not code. It takes data from relational databases (JDBC) and file drops into a Bronze/Silver lakehouse. It is built to stay correct when several instances run at the same time and when a run dies halfway.

> **Status: Phase 1 (bootstrap) in progress.** The architecture is settled: [discovery document](docs/architecture/00-architecture-discovery.md) and [ADRs](docs/adr/README.md). Code is being built phase by phase (discovery §17).

## Stack

Java 17 · Scala 2.13.17 · Apache Spark 4.1.4 · Delta Lake 4.4.1 · sbt 1.13.1 · PostgreSQL 17. Versions and the reasons for them: [ADR-0002](docs/adr/0002-technology-stack.md), [ADR-0014](docs/adr/0014-stack-versions-pinned-at-p1.md).

## Modules

| Module | Responsibility |
|---|---|
| `core` | Domain and configuration model, validation, planning. No Spark, no JDBC. |
| `delta-pg-logstore` | PostgreSQL-coordinated Delta `LogStore` for safe multi-writer tables. Depends on no other module. |
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
```

## License

[Apache License 2.0](LICENSE)
