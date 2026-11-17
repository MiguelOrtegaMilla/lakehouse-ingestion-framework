# Spike GREEN — `PostgresLogStore` on `file://` with concurrent writers

| | |
|---|---|
| **Phase** | P2b.3 (suite and runs), 2026-11-16. The writers killed inside a commit (P2b.4) and the restart of PostgreSQL (P2b.5) are added to this document as they are run. |
| **Plan** | [ADR-0004](../adr/0004-postgresql-coordinated-delta-logstore.md) §7.3 |
| **Before** | [Spike RED](logstore-red.md): the same harness on Delta's default LogStore |
| **Code** | `SpikeGreenSpec`, `CoordinatedStressSuite`, the stress harness and verifier (`modules/integration-tests/.../it/stress/`). The RED suite, `SpikeRedSpec`, is unchanged. |
| **Reproduce** | `scripts/stress-logstore.sh`: both suites, about 65 minutes, Docker, about 4 GB of free memory. With `STRESS_NATURAL_RUNS=0` the natural runs are left out, and it takes about 12 minutes. |
| **Harness** | Commit `68ec1b5`. GREEN and RED were run one after the other on 2026-11-16, by one invocation of the script. |

## Question

The RED spike showed that Delta's default LogStore on `file://` loses commits whose writers were told they succeeded, and that Delta notices nothing. `PostgresLogStore` moves the mutual exclusion of commits into PostgreSQL ([ADR-0004](../adr/0004-postgresql-coordinated-delta-logstore.md)). Under the same harness and the same verifier, does it lose any?

## Answer

**No.** In every run of the suite no acknowledged commit was lost, no commit file had a stale `.crc` sidecar, and both readers read the table.

How much each part of that says is not the same, and the rest of this document keeps the two apart:

- **The decisive evidence is the widened window.** Three writers are held, with their commits ready, between the check that a version is free and the step that takes it. On Delta's default LogStore that run lost an acknowledged commit **every time it was made: 6 runs of 6**, five on 2026-11-13 and one on 2026-11-16. On `PostgresLogStore` it lost **none**, and its writers lost **52 versions at the `INSERT`**: 52 times a writer had found the version free, had waited with its commit ready, and found the version taken when it claimed it. Each was settled by Delta at the next version.
- **The natural race is the weak evidence.** Without any delay, the default LogStore lost commits in **2 runs of 6** between 2026-11-13 and 2026-11-16. `PostgresLogStore` lost none in **10 runs of 10**, during which two writers were committing one version at the same moment **376 times**, without help, and nothing was lost.
- **The natural race of one day does not tell the two apart.** The one natural run of RED on 2026-11-16 lost nothing either. A clean natural run can be luck, with any LogStore: four of the six RED runs were.

## How to read the evidence

A stress run can show that a failure happens. It cannot show that one never does. What it can do is put the system, many times, in the situation where the failure happens, and count.

- In **the widened window** the situation is made, not waited for. Every writer is inside the window on every commit, and the run shows how often: each writer reports the times it waited there and the versions it lost there. The default LogStore fails in that situation every time. The claim "no commit is lost" rests on this run.
- In **the natural race** the situation happens by chance, and for most commits it does not happen at all. A run without a lost commit says little by itself. What it adds is a count of the times the situation did happen unprovoked, 376 here, and that is all it is used for.
- Still to come are the writers **killed** inside the window (P2b.4): the same situation, with a process that dies in it.

## Setup

| | |
|---|---|
| Stack | Java 17.0.21, Spark 4.1.4, Delta 4.4.1, Hadoop 3.4.2 (Spark's), PostgreSQL 17 |
| Machine | WSL2 (Linux 6.18), ext4, one machine: AMD Ryzen 7 4700U, 6 cores and 4.9 GB given to WSL |
| Writers | 3 separate JVMs, `local[1]`, 512 MB heap each, released together by a file barrier: as in RED |
| Workload | Blind appends of 10 rows each, `txnAppId = writer-<i>`, `txnVersion = 1..K`; every row tagged `(writer, txnVersion)`: as in RED |
| Evidence of success | Each writer appends `ACK k` to its own journal and forces it to disk after the write returns: as in RED |
| Coordination | One PostgreSQL in a container, started by the orchestrator, with the schema migrated. Every process reaches it through a proxy, so that later runs can break the connection. |

What differs from RED in a writer is its configuration and nothing else: `spark.delta.logStore.file.impl` names `PostgresLogStore`, and three settings say how to reach PostgreSQL. The writer's code is the same class.

### The verifier

It is the verifier of RED: it reads every commit file as bytes, without Delta or Hadoop, compares the commits with the journals, and then loads the table through two Delta readers, the default one, which checks `.crc` sidecars, and one that does not, as on object storage.

A run on `PostgresLogStore` is held to more (ADR-0004 §7.2), and a run of RED to none of it:

- **The registry**, read with plain SQL after the readers have read the table: the table is registered and not tainted; every commit has a complete entry, and every entry its commit; each writer's commits are attributed to its own run, and no run has commits of two writers.
- **Sidecars**: no commit file has one at all, since a commit is published as a hard link and never through Hadoop's checksummed file system; any other file of the log that has one, has one that matches.

### Preconditions, checked on every writer

Each writer records what its own Delta and Hadoop resolved, and every run asserts it before any result counts:

| Fact | Value in every GREEN run | Why it matters |
|---|---|---|
| LogStore for the table | `lakehouse.ingestion.logstore.PostgresLogStore` | The code path the result is claimed for |
| `FileSystem` of `_delta_log` | `org.apache.hadoop.fs.LocalFileSystem` | The file system of RED |
| Startup guard | Passed, before the writer touched the table | A writer that would commit without coordination refuses to run |

### Variants

The suite of RED, and what RED's own widening can no longer do.

| Variant | Writers × commits | What it shows | Pass condition, besides the criterion |
|---|---|---|---|
| Control, no delay | 1 × 10 | The harness and verifier alone find nothing | No wait at any fault hook |
| Control, delay | 1 × 5, 1.5 s at each hook | The fault hooks themselves lose nothing, and are in the path of every commit | Exactly one wait for each commit at each hook |
| **Widened window, before the `INSERT`** | 3 × 10, 1.5 s | Writers that all find one version free and all claim it | Versions lost at the `INSERT`; each writer waited once for each commit it made and each version it lost there, and for nothing else |
| **Widened window, after the `INSERT`** | 3 × 10, 1.5 s | A commit that exists and is not in the log yet, published by the other writers | Versions lost at the `INSERT`; commits that were complete sooner than their own writer's wait |
| The configuration of RED | 3 × 10, `DelayingLocalFs` with 1.5 s | The experiment of RED, as it was | The delaying file system resolved in every writer, and no rename delayed |
| Natural race | 3 × 200, no delay, ten times | The contention that happens without help | Contention over the ten runs |

**The criterion**, for every variant: no acknowledged commit lost and no other anomaly; no commit file with a stale sidecar; both readers read the table.

**Why the window is widened another way.** RED widened its window with `DelayingLocalFs`, a sleep before the rename that publishes a commit file. `PostgresLogStore` publishes a commit as a hard link and never renames one ([ADR-0015](../adr/0015-hard-link-publish-on-local-file-systems.md)), so that sleep is not in the path of a commit any more. The windows this LogStore has are on either side of the `INSERT` that takes a version, and its own fault hooks hold a writer there (`lakehouse.logstore.fault.<point>.delayMs`). The hooks count their waits, as `DelayingLocalFs` counted its delayed renames: a widened window proves something only if the wait was where the commits went.

## Results

Runs of 2026-11-16, GREEN from 21:55 UTC and RED from 22:51 UTC. Both suites passed: `SpikeGreenSpec` 16 tests in 55 minutes, `SpikeRedSpec` 4 tests in 6.6 minutes, no failure, error or skipped test.

### The widened window

| | Writers × commits | Acknowledged | **Lost** | Stale sidecar | Default reader | Sidecar-free reader |
|---|---|---|---|---|---|---|
| RED, default LogStore (2026-11-16) | 3 × 10 | 1 | **1** (`writer-0`, txnVersion 1) | version 1 | **fails**: `ChecksumException` | reads the table, without the commit |
| RED, the five runs of 2026-11-13 | 3 × 10 | | **1 in each** | | | |
| GREEN, before the `INSERT` | 3 × 10 | 30 | **0** | none | reads the table | reads the table |
| GREEN, after the `INSERT` | 3 × 10 | 30 | **0** | none | reads the table | reads the table |

On the default LogStore the run was over at version 1: one commit acknowledged, that commit lost, and all three writers stopped on the stale sidecar. On `PostgresLogStore` every writer made its ten commits.

What the two GREEN runs were made of:

| | Waits at the hook | Versions lost to other writers | **Of them, at the `INSERT`** | Commits published by another process |
|---|---|---|---|---|
| Before the `INSERT` | 82 (29, 29, 24) | 53 | **52** (19, 19, 14) | |
| After the `INSERT` | 30 (10, 10, 10) | 17 | **10** | 27 of 30 |

- **Before the `INSERT`**, the 82 waits are the 30 commits and the 52 versions lost at the `INSERT`, writer by writer: 29 = 10 + 19, 29 = 10 + 19, 24 = 10 + 14. The wait was in the path of every commit, and it is where the versions were lost. 52 times a writer was where a writer of RED loses its commit, or somebody else's.
- **After the `INSERT`**, 27 of the 30 commits were complete in less than the 1.5 s their own writer waited before publishing them: another process had found the version taken, read the table, and published the commit. That is recovery by another process while the owner is alive, and the owner then finds its commit published.

### The natural race

Three writers, 200 commits each, no delay.

| Run | Acknowledged | **Lost** | Versions lost to other writers | **Of them, at the `INSERT`** | Time |
|---|---|---|---|---|---|
| GREEN 1 | 600 | 0 | 589 | 38 | 287 s |
| GREEN 2 | 600 | 0 | 600 | 48 | 281 s |
| GREEN 3 | 600 | 0 | 586 | 36 | 286 s |
| GREEN 4 | 600 | 0 | 600 | 53 | 279 s |
| GREEN 5 | 600 | 0 | 572 | 30 | 286 s |
| GREEN 6 | 600 | 0 | 586 | 57 | 285 s |
| GREEN 7 | 600 | 0 | 574 | 22 | 286 s |
| GREEN 8 | 600 | 0 | 576 | 35 | 285 s |
| GREEN 9 | 600 | 0 | 588 | 36 | 284 s |
| GREEN 10 | 600 | 0 | 576 | 21 | 285 s |
| **GREEN, ten runs** | **6000** | **0** | **5847** | **376** | |
| RED (2026-11-16) | 600 | 0 | not measured | not measured | about 280 s |
| RED, the five runs of 2026-11-13 | | lost in 2 runs of 5 | not measured | not measured | |

In every GREEN run: no anomaly, no sidecar on a commit file, both readers read the table, and 601 complete entries under three runs, one for each writer.

**The natural run of RED on the same day lost nothing.** Set next to any one GREEN run, it looks the same: 600 commits acknowledged, none lost, both readers reading. Only over several days does the default LogStore show what it does, in 2 runs of 6. This is why the natural race is not what the result rests on.

### What the contention numbers say

- **"Versions lost to other writers"** counts every time a commit found its version taken. Almost one for each commit, in every run. Most of them say little: the version's commit was already in the log when the writer looked, because another writer had committed since this one read the table. That happens as often on the default LogStore, and loses nothing there either.
- **"Of them, at the `INSERT`"** counts the others: the commit was not in the log when the writer looked, and the claim found another writer's. Two writers were committing one version at the same time. That is the moment at which the default LogStore loses a commit, and it happened 21 to 57 times in each natural run, **376 times in ten**.
- **For RED the two columns say "not measured", not zero.** The counter is `PostgresLogStore`'s. A writer of RED reports 0 because nothing counts there, not because its writers did not collide: they collide at least as often.

### The configuration of RED

| | Value |
|---|---|
| `FileContext` of the log, in each of the three writers | `DelayingLocalFs`, as in RED |
| Commit-file renames it delayed | **0** (RED: one for each commit attempt) |
| Acknowledged, lost | 30, 0 |
| Versions lost to other writers, and at the `INSERT` | 28, **0** |

It is the same experiment as RED, and it no longer tests the same thing. The delay sits on a rename that this LogStore never makes, so nothing was widened: no rename was delayed, and no version was lost at the `INSERT`. The result says that the window RED widened is gone. It does not say that the window was survived; the two runs above say that, for the windows this LogStore has.

### Controls

| | Commits | Lost | Anomalies | Waits at the hooks |
|---|---|---|---|---|
| GREEN, no delay | 10 | 0 | 0 | none |
| GREEN, 1.5 s at `beforeInsert` and at `afterInsert` | 5 | 0 | 0 | 5 and 5: one for each commit at each hook |
| RED, no delay | 10 | 0 | 0 | |
| RED, delaying file system | 5 | 0 | 0 | 5 delayed renames |

### Sidecars

| | Commit files with a `.crc` sidecar | Stale ones |
|---|---|---|
| RED | every one (11, 6, 2 and 601 in its four runs) | version 1 of the widened run |
| GREEN | **none**, in any run | none |

On the default LogStore every commit has a sidecar, written in a second step, which is what goes stale when a commit is replaced. On `PostgresLogStore` no commit has one, so there is nothing to go stale. Delta still writes its own version checksums (`N.crc`) through Hadoop's checksummed file system; they have sidecars, and every one matched.

### Pace

A writer made its 200 commits in 261 to 277 s on `PostgresLogStore`, and in 273 to 278 s on the default LogStore in the one RED run of the day. At this size the coordination does not show in the time of a commit, which is spent in Spark and Delta. It is one run of RED on one machine, not a measurement: latency is measured in E3.

## Limits of this result

- **One machine, one file system** (WSL2, ext4), three writers, small commits, appends only. The stress job in CI runs eight writers (P2b.6); `s3a://` is E1.
- **The natural race is not a rate.** How often two writers meet at the `INSERT` depends on hardware, load and commit latency. The 376 are a count on this machine, on this day.
- **The collisions of RED were not counted.** The count at the `INSERT` exists only on `PostgresLogStore`, so the two cannot be compared by it. What can be compared is the outcome: lost commits.
- **The window before the `INSERT` is widened by a hook of the LogStore itself.** The hook only sleeps, between two steps the LogStore takes anyway, and the control shows that it loses nothing by itself.
- **No process dies in these runs, and PostgreSQL stays up.** Those are P2b.4 and P2b.5.
- **Other runs of the day are not in the tables.** The deterministic variants were run twice more while the suite was being written, and two natural runs belong to an execution that was stopped for lack of memory. All were clean. They were made before the count at the `INSERT` existed, so their reports do not have it.

## Consequences for the project

- **The fix of ADR-0004 holds where the default LogStore fails every time**, and where it fails by chance. The engine can be built on tables that several instances write.
- **P2b.4** kills writers inside the two windows: the remaining way to lose a commit is a writer that dies with one half made.
- **The count at the `INSERT` is there for the engine to report next to the total** (`commit_contention`, ADR-0010), if it proves useful there: the total says how busy a table is, the count at the `INSERT` how often its writers were truly in each other's way. That is decided with the engine's metrics (P11).
