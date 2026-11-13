# ADR-0002 — Technology stack and versions

| | |
|---|---|
| **Status** | Accepted (D4, 2026-09-25) · **version rows for Scala, Spark, Delta and sbt superseded by [ADR-0014](0014-stack-versions-pinned-at-p1.md)** (P1, 2026-11-13) |
| **Verified against** | spark.apache.org downloads and docs, Maven Central (`io.delta`), delta-io/delta GitHub releases — 2026-09-25 |

## Decision

| Component | Version / artifact | Rationale |
|---|---|---|
| Java | **17** (Temurin) | Spark 4.1 runs on Java 17/21. 17 is the conservative LTS choice; 21 adds nothing the project uses. |
| Scala | **2.13.x**, the patch Spark 4.1.3 is built with (pinned in P1) | Spark 4 is published for Scala 2.13 only. Scala 3 is not viable with Dataset encoders, which rely on `TypeTag`. |
| Apache Spark | **4.1.3** | A mature 4.1 line (three patch releases). 4.2.0 (July 2026) is a `.0` release. Upgrading later means changing two coordinates. |
| Delta Lake | **4.4.0**, `io.delta:delta-spark_4.1_2.13` | Since Delta 4.1, artifacts carry the Spark minor in their name. 4.4.0 (August 2026) supports Spark 4.0, 4.1 and 4.2. `delta-storage` (the `LogStore` API) comes transitively. |
| sbt | Latest 1.x (pinned in P1) | Standard Scala build tool; `sbt-assembly` for the fat jar with Spark `provided` |
| PostgreSQL | **17** (`postgres:17`) | Source DB, control plane and commit coordination (separate databases, schemas and roles) |
| JDBC driver | `org.postgresql:postgresql` 42.7.x | Standard |
| Connection pool | HikariCP | The LogStore makes several round trips per commit plus one per snapshot refresh; opening a connection each time would dominate latency |
| Migrations | Flyway | Versioned control-plane and coordination schemas |
| Config | circe + circe-yaml | YAML → case classes with precise decoding errors; `cats.data.Validated` for accumulating validation |
| Tests | ScalaTest 3.2.x, Testcontainers (PostgreSQL; MinIO in E1) | Real dependencies in integration tests; Docker available in WSL and CI |
| Logging | log4j2 (bundled with Spark) + `JsonTemplateLayout` | No second logging backend (avoids SLF4J binding conflicts with Spark) |
| Quality | scalafmt, `-Xlint` + selective `-Werror`, scoverage | Enough to demonstrate discipline without heavy tooling |
| CI | GitHub Actions | Free for public repos; Linux runners ship with Docker |

## Rejected / deferred

- **Scala 3:** incompatible with Spark's encoder derivation.
- **Spark 3.5 + Delta 3.3:** older line; misses Delta 4.x features such as catalog-managed tables and type widening.
- **A DI framework, cats-effect or ZIO:** manual wiring is sufficient. An effect system would add a learning curve unrelated to the project's goals.

## Consequences

- One JDK across local, CI and the Spark runtime.
- Delta upgrades require re-running the LogStore suite (ADR-0004), because the LogStore depends on Delta's `LogStore` developer API and on commit behaviour.
