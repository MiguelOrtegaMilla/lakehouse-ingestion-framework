# ADR-0020 — The watermark of an incremental load: a window of the source's clock, fixed when the batch is planned

| | |
|---|---|
| **Status** | Accepted (P6.4, 2026-11-20) |
| **Related** | [ADR-0005](0005-batch-wal-and-idempotent-writes.md), [ADR-0008](0008-layer-responsibilities.md), [ADR-0009](0009-delete-detection.md), [ADR-0013](0013-application-security-baseline.md), [ADR-0018](0018-a-run-gives-up-a-batch-whose-source-changed.md) |

## Context

An incremental load reads what changed in a table since the last time. Four things have to be decided for it, and each of them can lose rows without a word.

1. **What "since" is measured by.** A row is stamped when its transaction writes it, and is seen when that transaction commits. Between the two, a reader that has gone past the stamp will never see the row: it comes into view behind the reader. Whatever the load goes by has to leave room for that gap.
2. **Where a batch ends.** A batch with no upper bound ends wherever the read happened to end. Nothing says where that was, so nothing can be replayed: ADR-0005 writes the plan of a batch down before a row is read, replays that plan exactly after a crash, and takes what the commit records from the plan alone, since a batch that is rolled forward is committed with nothing read.
3. **Whose clock.** Three take part: that of the machine that runs the engine, that of the database, and the values in the rows, which the database or its applications wrote. The first is the only one that has nothing to do with the rows.
4. **How much one batch may read.** The first batch of a large table, or a source that was left for a month, is a statement that runs for hours, with the snapshot of the source held open and everything to do again if it fails.

Timestamps that tie, a clock that goes back, and a load that goes on while its source is written to are all cases of the same four.

## Decision

**A batch of an incremental load reads the rows whose watermark column is in `(L, U]`. Both bounds are times of the clock of the source, fixed when the batch is planned and written in its plan. The commit of the batch moves the watermark to `U`.**

With `W` the watermark of the source, and the settings of `load`:

```text
H = what the clock of the source says at the first plan of an invocation − safety_margin
A = W, or start for a source that has no watermark yet            (where a batch counts from)
L = max(W − lookback, start) · start, for the first batch · none, for the first batch without start
U = min(H, A + max_batch_range)
```

1. **A watermark is a timestamp, and nothing else.** `timestamptz` is the type to have; a `timestamp` without a zone is taken as UTC. A numeric key that grows is left out on purpose, for correctness and not for want of time: a sequence gives a row its number when the row is inserted, so a lower number can come into view after a higher one was read, exactly as a timestamp can. The difference is that the gap of a timestamp can be said, in time, and waited for. No margin can be said in the numbers of a key.
2. **The bounds are times of the source's clock.** The connector asks it with a constant statement (`SELECT CURRENT_TIMESTAMP`, ADR-0013 §1) when the batch is planned. The clock of the engine bounds nothing: the two machines need not agree on the time, and the rows were stamped by the source.
3. **`safety_margin` is what loses nothing.** The horizon `H` is the clock less the margin, and no window ends after it. A transaction that takes its timestamp and commits within the margin is committed before any window that holds its timestamp is read. It costs freshness, by the margin, and reads nothing twice.
4. **`lookback` reads again.** `L` is the watermark less `lookback`: the last stretch is read at every batch, and Bronze, which is appended to, keeps each copy for Silver to tell apart (ADR-0008). It covers the same gap as the margin, at the cost of copies, and stays at `0s` unless a source says otherwise. `validate` warns of a watermark with both at `0s`.
5. **The commit moves the watermark to `U`**, which the connector says from the plan alone, and not to the highest value that the batch read. A window with no rows is a batch with no rows, and it moves the watermark as any other.
6. **The first batch.** Without `load.watermark.start` it has no lower bound, and reads the table up to `H` in one statement. With it, the rows at or before `start` are not the source's to ingest: the first window begins there, with no lookback.
7. **`lookback` reads no row at or before `start`**, in any batch: `L = max(W − lookback, start)`. Past the first batch that is all `start` does. One that is moved to after the watermark bounds nothing and skips nothing, since `L` is never after `W`: moving it is still no breaking change of a configuration.
8. **`max_batch_range` cuts a window, counted from the watermark and not from `L`**: `U ≤ W + max_batch_range`. Counted from `L`, a `lookback` as long as the range would hold `U` at `W`, and the watermark would never move. The first batch of a source with `start` is counted from `start`. The first batch of a source without it has nothing to count from, and is not cut; `validate` warns of it.
9. **The horizon is fixed by the first plan of an invocation, and is written in every plan.** A plan says that there is more when its `U` is before its `H`. A chain of runs (discovery §17, P6.10) goes on by that and plans each next batch against the same `H`, so it ends though the source goes on writing: what the source writes after the first plan is the next invocation's.
10. **The watermark never goes back.** A horizon that is not after `A` plans no batch, and the run ends well with nothing read. That is the case when the clock of the source went back, when `safety_margin` was raised, when `start` is yet to come, and when a chain has reached its horizon. The lookback is not read on its own: a batch that would not move the watermark is not planned.
11. **The plan and the watermark are JSON of a form with a version, read back strictly.** The plan is `{version, column, lower, upper, horizon}`, and the watermark `{version, value}`. Every moment is in UTC, to the microsecond, which is what PostgreSQL keeps, and written in one way. A document with a field missing or one too many, with another version, or with a moment written otherwise is refused: it is not one that this application wrote, and going on from a moment that was guessed would skip rows. The plan names the watermark column, so a replay under a configuration with another column is refused by its connector, as a plan of files is for its directory.
12. **Every moment of a plan is from 1900 to the end of 9999**, as a moment of configuration is. A clock of a source that says otherwise makes no plan, and the run says why.

## Alternatives

| Option | Verdict |
|---|---|
| The watermark is the highest value that a batch read (`WHERE c > W`, then `max(c)`) | The upper bound is whatever the read saw. A replay reads another range than the plan says; a roll-forward has no value to record without reading; and it takes a second statement, or a pass over the rows before they are written, which for a JDBC batch is the table held in Spark. A row committed late, below the highest value read, is lost with no margin to cover it. Rejected. |
| A numeric key that grows, as the watermark | The same gap as a timestamp, and no margin that can be said in its numbers. Rejected, on purpose (point 1). |
| The clock of the engine as the upper bound | Two clocks that differ by more than the margin lose rows, or read the future. The source's clock costs one constant statement. Rejected. |
| `ORDER BY c LIMIT n`, a batch bounded by a number of rows | It cuts through timestamps that tie: the rows of one moment on both sides of a bound, and the ones after it lost or read twice. And it bounds by what was read. Rejected. |
| `max_batch_range` counted from the lower bound | With `lookback ≥ max_batch_range` the watermark does not move, for ever, with no error. Rejected (point 8). |
| `start` looked at once, and the lookback free to reach before it | Simpler to say, and it reads rows that the configuration says are not the source's. Rejected: a bound of the source holds for every batch. |
| A lookback read on its own when the horizon is not past the watermark | A batch that moves nothing, at every run, for as long as a clock is behind. Rejected (point 10). |
| The transaction ids of PostgreSQL (`xmin`, against the snapshot of the reader) | Loses nothing whatever a transaction lasts, and needs no column. It is of one database, its filter is on a system column that no index has, and the ids wrap around. Rejected for a connector that is JDBC's. |
| Change data capture (logical replication) | It is what loses nothing and sees deletes. It is another kind of plan, a range of LSNs, and a candidate extension for after Silver (discovery §20). The plan and the watermark are JSON that their connector gives a form to, so it takes no change of the control plane. |

## Consequences

- **A transaction shorter than `safety_margin` and `lookback` together loses nothing. A longer one loses its rows, without a word**: the window that held their timestamp was read before they were there. It is the known limit of every load by a column, and what CDC is for. A test pins the loss (P6.8), `validate` warns of a watermark with no margin at all, and the reference says which setting to use.
- **Deletes are not seen** (ADR-0009): a row that is gone is in no window.
- **A row whose watermark column is null is in no window**, and is never ingested. What the engine says of a column that may be null is decided with what is asked of the source before a plan (P6.6).
- **The column has to be set by every writer of the table, at every change.** The engine cannot know of one that is not.
- **A replay reads the window again, and not the rows.** A row that was updated between a run that died and the one that replays its plan has a later timestamp by then, and has left the window: it is in a later one. The rows of a replay may differ from those the dead run read, and nothing of that run is in the table, or the batch is rolled forward and nothing is read. A table has nothing that a plan names and can lose, so a watermark batch is never given up for its source having changed (ADR-0018).
- **Timestamps that tie need nothing.** The bounds are times of a clock and not values that were read, the window is half-open, and no read has a `LIMIT`: the rows of one moment are all on one side of a bound.
- **A clock of the source that went back stops the source until it passes the watermark again.** What the source stamps meanwhile is at or before the watermark, and is lost unless `lookback` reaches it: nothing can be read from behind a watermark without moving it back. A plan that is not made says how far the horizon is behind, and the engine is to warn when that is more than the margin (P6.7).
- **A raised `safety_margin` makes a source wait** for the difference, and loses nothing.
- **A window with no rows is still a batch**: a run, a lease and a row of the write-ahead log, with no commit to a table. A source with a `start` years back and a short `max_batch_range` takes as many batches as that makes, most of them empty. It is what a backfill is for (P7), and `start` can be nearer.
- **`start` is read after the first batch**, where P6.3 had it read only while a source has no watermark. A change of it is still no breaking change: it bounds what is read again, and never what is read for the first time.
- **The engine gains nothing to store.** The watermark and the plan go where ADR-0005 has them (`source_state.watermark`, `ingestion_batch.plan`), as JSON that the control plane does not read.
- **The door to streaming and to CDC stays open**: both are a plan of another form, under the same protocol.

## As built (P6.4, 2026-11-20)

- **In `core`, with no I/O** (`core.watermark`): `WatermarkPlanner.horizon`, from the clock of the source and `safety_margin`, and `WatermarkPlanner.plan`, from the settings of a load, the watermark and a horizon. It answers `Planned`, or `NothingNew` with how far the horizon is behind. `WatermarkPlan` is the window, with `more` and the watermark that its commit records; `StoredWatermark` is the watermark. What asks the clock, reads the window and gives the plan to the engine is the JDBC connector's (P6.6, P6.7).
- **A document of a stored form is read by one helper** (`StoredForm`), which the plan of files uses as well: the fields exactly, the version, and a number written as a number.
- **Tests** (`WatermarkPlanSpec`): each rule above by a case of its own, among them a lookback as long as the range and longer, a lookback that would reach before `start`, a `start` moved to after the watermark, a window of one microsecond, and a horizon at the watermark and before it. Five hundred chains under settings drawn from a seed: each ends in the number of batches that its range makes, its windows follow each other with no gap, no window is wider than the range from where it counts, no lower bound is before `start`, the last upper bound is the horizon, and every plan is read back as itself from its text. Both forms are pinned as text, and each refusal by its message.
