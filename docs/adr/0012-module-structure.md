# ADR-0012 — sbt multi-module structure and project identity

| | |
|---|---|
| **Status** | Accepted (D6, 2026-09-25) · repository name and base package decided at Phase 0 closure (2026-11-10) |

## Decision

### Modules

| Module | Content | Allowed dependencies |
|---|---|---|
| `core` | Domain model, config model and validation, pure planning, error classification, run state machine | cats, circe. **No Spark, no JDBC.** |
| `delta-pg-logstore` | `PostgresLogStore`, coordination schema migrations, `CommitContext` API | `delta-storage`, Hadoop client API (`provided`), PostgreSQL JDBC, HikariCP. **No dependency on other project modules**, so it is extractable as a library. |
| `engine` | Connectors, writers, control-plane repositories, engine service, conflict classifier | `core`, `delta-pg-logstore`, Spark + Delta (`provided`) |
| `cli` | Entry point, argument parsing, wiring, exit codes, startup enforcement | `engine` |
| `integration-tests` | Testcontainers, e2e, fault injection, stress harness + verifier | all |

- **The dependency rules are enforced by the build graph**, so the compiler rejects a Spark import in `core`.
- Integration tests live in a separate subproject: sbt 1.9+ deprecates the `IntegrationTest` configuration.

### Project identity (D6)

- All code, comments, commit messages and repository documentation are in **English**.
- **Repository name: `lakehouse-ingestion-framework`.** Chosen for keyword clarity: it states what the project is (ingestion into a lakehouse) and that it is a framework, which is what a reviewer scanning a CV or a GitHub profile looks for.
- **Base package: `lakehouse.ingestion`.**

  | Module | Package |
  |---|---|
  | `core` | `lakehouse.ingestion.core` |
  | `delta-pg-logstore` | `lakehouse.ingestion.logstore` |
  | `engine` | `lakehouse.ingestion.engine` |
  | `cli` | `lakehouse.ingestion.cli` |
  | `integration-tests` | `lakehouse.ingestion.it` |

- The GitHub handle appears in **one place**: `ThisBuild / organization := "io.github.changeme"` in `build.sbt` (the Maven `groupId`). The handle is still undecided; the placeholder stays until it is.
- The **base package does not contain the handle**. A package names the software and a `groupId` names the publisher, so decoupling them is standard. It also makes the placeholder a one-line change, since Scala package declarations, repeated in every file, cannot be parameterised from sbt.
- **Artifact names** are prefixed with the project name (`lakehouse-ingestion-<module>`). The LogStore module keeps its descriptive directory name, but would be published as `lakehouse-ingestion-pg-logstore`, for the same reason the repository avoids `delta-*` names (implied affiliation with the Delta Lake project).

## Consequences

- Fast unit tests for `core` and the LogStore protocol (no `SparkSession`).
- The LogStore can later be published on its own.
- Renaming the handle is one line; renaming the package would be an IDE refactor, and the design avoids needing it.
- Spark configuration refers to the LogStore by its fully qualified name: `spark.delta.logStore.<scheme>.impl=lakehouse.ingestion.logstore.PostgresLogStore`.
