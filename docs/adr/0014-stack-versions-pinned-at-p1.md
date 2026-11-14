# ADR-0014 — Stack versions pinned at P1

| | |
|---|---|
| **Status** | Accepted (P1, 2026-11-13) |
| **Supersedes** | The version rows of [ADR-0002](0002-technology-stack.md) for Scala, Spark, Delta Lake and sbt. The rest of ADR-0002 (component choices, rejected options, consequences) stands. |
| **Verified against** | Maven Central (`spark-parent_2.13`, `delta-spark_4.1_2.13`, `delta-storage`, `org.scala-sbt:sbt`), Apache JIRA (`fixVersion = 4.1.4`), `delta-io/delta` compare `v4.4.0...v4.4.1` — 2026-11-13 |

## Context

ADR-0002 (2026-09-25) fixed Spark 4.1.3 and Delta 4.4.0, and left the Scala patch and the sbt version to be pinned in P1. Since then two patch releases have appeared on the same lines:
- **Spark 4.1.4** (2026-10-22);
- **Delta 4.4.1** (2026-10-05).

ADR-0002 also records that a Delta upgrade means re-running the LogStore suite (ADR-0004). Before P2a there is no suite to re-run, so this is the cheapest moment to move.

## Decision

| Component | Version | Source of the value |
|---|---|---|
| Java | 17 (Temurin) | Unchanged |
| Scala | **2.13.17** | `scala.version` in `spark-parent_2.13:4.1.4`; also the `scala-library` that `delta-spark_4.1_2.13:4.4.1` is built with |
| Apache Spark | **4.1.4** | Latest patch of the 4.1 line |
| Delta Lake | **4.4.1** — `io.delta:delta-spark_4.1_2.13`, `io.delta:delta-storage` | Latest patch of the 4.4 line; supports Spark 4.0, 4.1 and 4.2 |
| Hadoop client API (`provided`, LogStore module) | **3.4.2** | `hadoop.version` in `spark-parent_2.13:4.1.4` |
| sbt | **1.13.1** | Latest 1.x (2026-10-28), in `project/build.properties` |

The versions live in `project/Dependencies.scala` (libraries) and `project/build.properties` (sbt).

**sbt runner vs build version.** The `sbt` command installed on a machine is only a launcher. It reads `sbt.version` from `project/build.properties` and boots that version. The 2.0.x runner boots a 1.x build in classic mode, without the sbt 2 native client. Checked with runner 2.0.12. Any recent runner works; the repository pins the build version, not the runner.

## Release review

### Spark 4.1.3 → 4.1.4
- 110 JIRA issues. None touches the JDBC data source, the Hadoop `FileSystem` commit path, or the path Delta writes through.
- The relevant ones harden the Parquet readers (SPARK-59352, SPARK-59812, SPARK-59832, SPARK-60262) or fix SQL correctness bugs.
- **No dependency changes:** every `*.version` property in `spark-parent` is identical between 4.1.3 and 4.1.4 (Hadoop 3.4.2, Scala 2.13.17, log4j, Jackson, Netty).

### Delta 4.4.0 → 4.4.1
Six commits, reviewed file by file:

| Change | Effect on this project |
|---|---|
| Block unsupported maintenance on catalog-managed tables (#7603): `OPTIMIZE`, `VACUUM`, metadata cleanup, `REORG`, column-mapping removal, symlink manifests | None. The guards only act on catalog-managed tables, which are *Future* in the phase plan. |
| Ignore staged commits in filesystem listings (#7648). `SnapshotManagement` now accepts only backfilled commit files (`_delta_log/N.json`) from a listing, under the internal conf `spark.databricks.delta.snapshot.filesystemListing.filterStagedCommits.enabled` (default `true`) | None. `PostgresLogStore.listFrom` publishes pending entries and then returns `_delta_log/N.json` (ADR-0004 §3.3). It never returns `_staged_commits/` or `.tmp/` files. |

No changes to `delta-storage`, the `LogStore` API, `OptimisticTransaction`, the conflict checker, `SetTransaction` (`txnAppId`/`txnVersion`), checksums or checkpoints. **The analysis in ADR-0004 and ADR-0010, written against `v4.4.0`, holds for 4.4.1.**

## Alternatives

| Option | Verdict |
|---|---|
| Stay on Spark 4.1.3 / Delta 4.4.0 (ADR-0002 as written) | Valid, but gives up security and correctness fixes, and any later upgrade happens after P2a, when it costs a LogStore suite run. Rejected. |
| Spark 4.2.x or 4.3.0 | A new minor line: a different `delta-spark_4.x` artifact, and the ADR-0004 source study would need redoing. No feature the project needs. Rejected. |
| sbt 2.x for the build | ADR-0002 fixes sbt 1.x. sbt 2 moves build definitions to Scala 3, and every plugin would have to be checked for sbt 2 support. It adds nothing the project needs. Rejected. |

## Consequences

- Discovery §20 item "Exact Scala 2.13 patch used by Spark; latest sbt 1.x" is closed.
- **Procedure for the next patch upgrade:** release notes, plus a diff of the commit path (`delta-storage`, `OptimisticTransaction`, `SnapshotManagement`), plus a LogStore suite run. The same review as above, recorded in a new ADR.
- `hadoop-aws` in E1 must match Hadoop 3.4.2.
- `delta-storage` 4.4.x declares compile dependencies on `unitycatalog-client` and `unitycatalog-hadoop`, which bring `log4j-core` in. That is acceptable for the engine, because Spark ships log4j anyway. For an extractable library module it is noise. P2a decides whether `delta-pg-logstore` excludes them. *(Decided in [ADR-0016](0016-delta-storage-provided-to-the-logstore-module.md), 2026-11-14.)*
