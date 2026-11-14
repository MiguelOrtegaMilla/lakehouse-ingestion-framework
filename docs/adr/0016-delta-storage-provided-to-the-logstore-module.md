# ADR-0016 — The LogStore module takes `delta-storage` as `provided`, without Unity Catalog's client libraries

| | |
|---|---|
| **Status** | Accepted (P2a.2, 2026-11-14) |
| **Related** | [ADR-0012](0012-module-structure.md), whose row for `delta-pg-logstore` it refines · [ADR-0014](0014-stack-versions-pinned-at-p1.md), which left the question to P2a · [ADR-0013](0013-application-security-baseline.md) §4 |
| **Verified against** | The POMs of `delta-storage` and `delta-storage-s3-dynamodb` 4.4.1 on Maven Central, and the resolved classpaths of this build (`sbt delta-pg-logstore/dependencyTree`) |

## Context

`delta-pg-logstore` implements Delta's `LogStore` API, which lives in `io.delta:delta-storage`. The module declared that artifact at compile scope.

Since Delta 4.4, `delta-storage` also holds Delta's Unity Catalog commit coordinator, and declares compile dependencies on `unitycatalog-client` and `unitycatalog-hadoop` 0.6.0. Through them the module's classpath gained `log4j-api`, `log4j-slf4j2-impl`, `log4j-core`, `slf4j-api` and `jsr305`.

- The `LogStore` API uses none of that: `LogStore`, `HadoopFileSystemLogStore` and `CloseableIterator` do not refer to the Unity Catalog classes.
- A module meant to be extracted as a library (ADR-0012) was bringing a logging backend with it. A library logs through an API and leaves the backend to the application.
- log4j was the only package with open alerts on the project's own runtime classpath (T-08 of the [triage register](../security/dependency-triage.md)).
- Whatever the module declares, the `delta-storage` that runs is the one the Delta installation supplies: `delta-spark` depends on it.

## Decision

In `delta-pg-logstore`:
- **`delta-storage` is `provided`**, like the Hadoop client API. The Delta runtime that loads the LogStore supplies it.
- **Its Unity Catalog dependencies are excluded** (`io.unitycatalog`), so they are not on the module's compile classpath either, and code there cannot come to depend on them.
- **`checkBoundaries` forbids `io.unitycatalog` and every `org.apache.logging.log4j` module** in this module. The build fails if either comes back, directly or through a new dependency.
- The module logs through `slf4j-api`, declared `provided` when its first class that logs is written.

## Alternatives

| Option | Verdict |
|---|---|
| Leave it at compile scope | The module ships a logging backend and Unity Catalog's client to whoever depends on it, and the project's runtime classpath keeps log4j's alerts. Rejected. |
| Keep compile scope, exclude Unity Catalog only | Removes the noise, but still claims that the module supplies the LogStore API. It does not: Delta does, and a second copy in an application jar could differ from the one Delta loads. Rejected. |
| `provided` without the exclusion | The right scope, but the Unity Catalog libraries stay on the compile classpath, where an import would compile. Rejected. |

Delta's own external LogStore artifact, `delta-storage-s3-dynamodb`, declares `delta-storage` at compile scope. It is published to sit next to Delta on a classpath, where the repeated declaration does no harm. `provided` states the same fact explicitly, and it is how this module already treats Hadoop.

## Consequences

- **Compile classpath of the module:** `scala-library`, `delta-storage`, `hadoop-client-api` and the `snappy-java` it brings. **Runtime classpath:** `scala-library`. The PostgreSQL driver and HikariCP join both in P2a.5.
- **The project's own runtime classpath carries no package with an open alert.** The alerts themselves stay: log4j is still in the dependency graph through Spark and Delta, which are `provided`. The triage register is updated.
- **Nothing changes at run time under Spark.** Delta brings `delta-storage` and its Unity Catalog libraries, as before. `Dependencies.log4jAlignment` still pins log4j to Spark's version for the classpaths that have it.
- **Using the module outside this build needs Delta on the classpath.** That is true of any LogStore.
- The fat jar of the `cli`, when it is built, must not bundle `delta-storage`; with this scope it will not.
