# Architecture Decision Records

Each ADR records one decision: its context, the decision, the alternatives considered and the consequences. ADRs are immutable once accepted. A change of mind is a new ADR that supersedes the old one. A factual correction that leaves the decision unchanged is made in place, with a dated note that says what the text said before (for example ADR-0004 §1.2, corrected in P1.4d).

| ADR | Title | Status |
|---|---|---|
| [0001](0001-development-environment.md) | Development environment: WSL2 + Docker Engine + VS Code/Metals | Accepted |
| [0002](0002-technology-stack.md) | Technology stack and versions | Accepted · version rows superseded by ADR-0014 |
| [0003](0003-control-plane-in-postgresql.md) | Control plane in PostgreSQL | Accepted |
| [0004](0004-postgresql-coordinated-delta-logstore.md) | PostgreSQL-coordinated Delta LogStore for multi-writer tables — **highest-risk component** | Accepted · `file://` publish superseded by ADR-0015, handling of a failed publish by ADR-0017 · the epoch of a writer added by ADR-0019 |
| [0005](0005-batch-wal-and-idempotent-writes.md) | Batch write-ahead log and idempotent Delta writes | Accepted · a second way out of a planned batch added by ADR-0018 · what a zombie may commit, by ADR-0019 |
| [0006](0006-source-leases-with-epoch-fencing.md) | Source leases with expiry, heartbeat and epoch fencing | Accepted · the epoch is checked for the tables too, by ADR-0019 · the key of a backfill's lease, by ADR-0021 |
| [0007](0007-metadata-format.md) | Source metadata as YAML with startup validation | Accepted |
| [0008](0008-layer-responsibilities.md) | Layer responsibilities: Landing, Bronze, Silver (Gold out of scope) | Accepted |
| [0009](0009-delete-detection.md) | Detecting deletes in the source | Accepted |
| [0010](0010-concurrent-write-conflict-handling.md) | Handling concurrent write conflicts on shared Delta tables | Accepted · its retry policy superseded by ADR-0022 |
| [0011](0011-storage-profiles.md) | Lake storage profiles and canonical table identity | Accepted |
| [0012](0012-module-structure.md) | sbt multi-module structure and project identity | Accepted |
| [0013](0013-application-security-baseline.md) | Application security baseline: SQL identifiers, path confinement, least privilege, supply chain | Accepted |
| [0014](0014-stack-versions-pinned-at-p1.md) | Stack versions pinned at P1: Spark 4.1.4, Delta 4.4.1, Scala 2.13.17, sbt 1.13.1 | Accepted |
| [0015](0015-hard-link-publish-on-local-file-systems.md) | Commit files on `file://` are published with a hard link | Accepted |
| [0016](0016-delta-storage-provided-to-the-logstore-module.md) | The LogStore module takes `delta-storage` as `provided`, without Unity Catalog's client libraries | Accepted |
| [0017](0017-a-failed-publish-is-reported-not-swallowed.md) | A publish that fails is retried and reported, not swallowed | Accepted |
| [0018](0018-a-run-gives-up-a-batch-whose-source-changed.md) | A run gives up the batch that its source no longer has as planned | Accepted · its known limit and the tombstone that was to close it superseded by ADR-0019 |
| [0019](0019-a-commit-names-the-epoch-of-its-lease.md) | A commit names the epoch of its lease, and the LogStore refuses one that is not in force | Accepted |
| [0020](0020-the-watermark-of-an-incremental-load.md) | The watermark of an incremental load: a window of the source's clock, fixed when the batch is planned | Accepted · what `load.watermark.start` bounds, by ADR-0021 |
| [0021](0021-operator-actions-and-backfills.md) | What an operator does to a source is a run, and a backfill is a sequence of its own | Accepted |
| [0022](0022-the-classification-of-failures-and-the-retries-of-a-run.md) | A failure of a run is classified once, and a run that may pass resumes where it is durable | Accepted |

Superseded decisions:
- *ADR-0002, version rows* (Spark 4.1.3, Delta 4.4.0, Scala and sbt to be pinned): replaced by ADR-0014 in P1.
- *AD-06 "one writer per Delta table"* (discovery v1) was replaced by ADR-0004 before being recorded as an ADR.
- *ADR-0004 §3.4, the `file://` publish* (copy to a second temporary file, then rename): replaced by ADR-0015 in P2a.
- *ADR-0004 §3.2 and deviation 4, a failed publish is swallowed*: replaced by ADR-0017 in P2a.
- *ADR-0018, a tombstone in the table before a batch is marked abandoned* (planned for P5.10, never built): replaced by ADR-0019 in P5.10. A run that lost its lease is refused where it commits, so there is nothing for a tombstone to keep out.
- *ADR-0010 §2, a count of attempts* (5, with a backoff from 500 ms to 30 s): replaced by ADR-0022 in P8.1. The budget of a run is one of time, from the profile (P3), and is shared with the failures that pass.

Amended decisions:
- *ADR-0005 §3, a planned batch is replayed until it is committed, or given up by an operator*: ADR-0018 (P5.8) adds that the run that holds the lease gives it up when its source no longer has what the plan names. No plan is changed, and the rest of the protocol stands.
- *ADR-0005 §4 and ADR-0006, what a zombie commits to a table is ignored by Delta*: ADR-0019 (P5.10) has it commit nothing. The epoch of the lease is named by every commit and checked by the LogStore where the version is taken (ADR-0004 §3.2, which gains that condition).
- *ADR-0006, the key of the lease of a backfill* (`backfill/<backfill_id>`): ADR-0021 (P7.1) has it be the name of the backfill's sequence, `<source_id>/<layer>/backfill/<backfill_id>`, as the regular key is the name of the regular sequence.
- *ADR-0020, points 6 and 7, the rows at or before `load.watermark.start` are not the source's to ingest*: ADR-0021 (P7.1) has them not the regular sequence's to read. `start` is where that sequence begins, and a backfill, a sequence of its own, reads the history before it.

Schema evolution (P9) gets its own ADR in its phase, once the policies are validated against Delta 4.4's type-widening support.

## Template

```markdown
# ADR-NNNN — Title

| | |
|---|---|
| **Status** | Proposed · Accepted · Superseded by ADR-XXXX |
| **Related** | … |

## Context
## Decision
## Alternatives
## Consequences
```
