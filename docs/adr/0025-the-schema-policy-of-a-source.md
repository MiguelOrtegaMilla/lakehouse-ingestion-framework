# ADR-0025 — A read is held to its table by the schema policy of its source, and a change is made in the commit of its rows

| | |
|---|---|
| **Status** | Accepted (P9.2, 2026-11-28) |
| **Supersedes in part** | [ADR-0022](0022-the-classification-of-failures-and-the-retries-of-a-run.md) §1, the row of a conflict that asks for a re-plan: tried once on the table as it is now, and then permanent. A conflict whose new table the policy of the source takes is retried within the budget of the run, and one whose table it refuses is permanent at once (§10). With it, the reaction of [ADR-0010](0010-concurrent-write-conflict-handling.md) §1 to `ProtocolChangedException`, "retry once", is the same. |
| **Amends** | [ADR-0008](0008-layer-responsibilities.md): Bronze renames nothing, but a source may give a column of its own another name in Bronze, by its configuration (§5). It is an alias of a name, and changes no value. |
| **Related** | [ADR-0005](0005-batch-wal-and-idempotent-writes.md), [ADR-0013](0013-application-security-baseline.md), [ADR-0018](0018-a-run-gives-up-a-batch-whose-source-changed.md), [ADR-0023](0023-a-file-that-cannot-be-read.md), [ADR-0024](0024-the-dialect-and-the-encoding-of-a-source-of-csv.md) |

## Context

A source says what is done when it does not look as it did (`schema`, discovery §10, P3): `on_new_column` (`add`, `rescue`, `fail`), `on_missing_column` (`null`, `fail`), `on_type_change` (`widen`, `fail`) and `renames`. By default `add`, `null`, `fail` and no renames. P3 reads and validates the policy, and nothing applies it.

Up to P8 a write changes no schema. `BatchAppend` writes with `mergeSchema` off, so a column that the table does not have, another type, or a name that Delta refuses is refused by Delta as a permanent failure (4), with the batch left planned for every run to fail on again (§20, from P4.7). So are what a batch of files has that no file has alone: files of CSV whose headers differ, fields of JSON that differ only in their case, files of Parquet with one column of two types. Rows that lack a column of the table are taken, with nothing in it. A conflict that asks for a re-plan is tried once, and is then permanent (ADR-0022 §1), until the policy is there to be looked at again.

### What Delta 4.4.1 does (P9.1, `DeltaSchemaContractSpec`)

- **The changes of a type that it takes**, by `mergeSchema` on a table with `delta.enableTypeWidening`: a byte, a short or an int to any wider integer, to a double, or to a decimal with ten digits before its point, and a long to a decimal with twenty; a float to a double; a decimal to one with no fewer digits before its point, nor after it; each of them inside a struct, an array or a map. A date to a timestamp without a zone only where the table has the feature `timestampNtz` too, and otherwise a `DeltaTableFeatureException`, which is no `AnalysisException`. Every other change is an `AnalysisException` that writes nothing: a long to a double, an integer to a float, a date to a timestamp with a zone, one timestamp to the other, any narrowing, and any change of kind.
- **Without the feature, `mergeSchema` widens a byte or a short to an int all the same**, and records nothing of it.
- **Delta casts no row to its column.** A type narrower than its column is refused, with `mergeSchema` or without it, but for a byte or a short to an int.
- **Turning the feature on.** No option of a write does it for a table that is there. `ALTER TABLE … SET TBLPROPERTIES` does, in a commit of its own, of the protocol (readers 3, writers 7, `typeWidening` among the features of both) and of the metadata. Set where it is set already, it commits the metadata again. The Delta documentation adds that "only clients that support this table feature can read and write to the table once the table feature is set", and that the feature is fully supported from Delta 4.0 on ([Delta type widening](https://docs.delta.io/latest/delta-type-widening.html)).
- **Taking it off.** `ALTER TABLE … DROP FEATURE typeWidening` does it in one statement. It writes the files of the old types again, keeps the types, the values and the history, and leaves the protocol at readers 1 and writers 7 with `checkpointProtection`.
- **The commit of rows that change a table.** `mergeSchema` on rows that the table takes as they are commits no metadata. Columns that the rows add go at the end, in the order of the rows, in the commit of the rows. A column of the rows is the column of the table of its name, whatever its case. Rows with two columns whose names differ only in their case are refused. An append of no rows changes the schema all the same.
- **Under a write.** A change of the metadata by another writer fails it with `MetadataChangedException`, whatever the write itself changes. A change of the protocol alone fails no write. `ProtocolChangedException` comes only where both change the protocol, so for the engine only where it turns type widening on (§4). Written again on a column that another widened, rows of the old type are refused until they are cast.
- **Reads by version through a widening.** A read of a version has the types of that version. A batch read of the change data feed whose range starts before a widening fails (`DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA`), even one whose range ends before it, since the read takes the schema of the table as it is now. It reads from the widening on; before it with the schema of a version (`versionAsOf`, under the internal setting `changeDataFeed.allowTimeTravelOptionsForSchema`); or whole with no check (`changeDataFeed.unsafeBatchReadOnIncompatibleSchemaChanges`, internal). Databricks documents the limitation for its sharing of tables, with a read split at the version of the change ([Databricks type widening](https://docs.databricks.com/aws/en/tables/features/type-widening)). In Delta 4.4.1 the split alone does not read the part before the change. A stream of the table, or of its change data, reads through a widening from a checkpoint taken before it, unless it tracks its schema (`schemaTrackingLocation`): then it stops at the widening until it is let through (`allowSourceColumnTypeChange`).

## Decision

### 1. A read is compared with its table

The schema of a read is compared with the schema of its table: the one of the snapshot on which its write commits, at each attempt of the write. No schema of a source is remembered. Nothing new is kept, and the table is what another writer changes, so it is the one thing against which the policy can be looked at again (§10).

- The first batch of a table makes it, whatever the policy says.
- Names are compared case aside, as Spark and Delta compare them (§6).
- The columns of the engine are not compared: they are the engine's, and every row has them.
- *A limit, taken:* in a table that several sources write (fan-in), `on_missing_column: fail` holds each source to every column of the table, the columns that other sources added among them.

### 2. What each value does

| Setting | Value | What a read that is not as the table is meets |
|---|---|---|
| `on_new_column` | `add` | The column is added to the table, at its end, in the order of the read, in the commit of the rows (§11) |
| | `rescue` | The table takes no column of the source. The values of the columns it does not have go to `_rescued_data`, as an object of JSON whose fields are their names, each value as Spark writes it in JSON (a text, for CSV and JSON). A table of types that does not have `_rescued_data` is given it, as the column of the engine that it is |
| | `fail` | A permanent failure (§8) |
| `on_missing_column` | `null` | The rows are written without the column, and Delta has nothing in it |
| | `fail` | A permanent failure |
| `on_type_change` | `widen` | The column is widened to the type of the read, if the change is one of §3, with type widening turned on first (§4). Any other change is a permanent failure |
| | `fail` | A permanent failure, for any change |
| `renames` | | An alias at read, before the read is compared (§5) |

A column of a type narrower than that of its table, by the widenings of §3, is cast by the engine to the type of the table, whatever the policy says. It is no change: Delta would refuse it (P9.1), and every value is kept. Only a source that has types meets `on_type_change`: a table of a database, and Parquet. CSV and JSON are text in Bronze (ADR-0008), and Silver types them (P10).

### 3. The changes of a type that `widen` takes

The widenings that P9.1 shows Delta 4.4.1 to make with every value kept, and with no feature but type widening:

- a byte, a short or an int to any wider integer, and to a double;
- a byte, a short or an int to a decimal with ten digits or more before its point; a long, to one with twenty or more;
- a float to a double;
- a decimal to one with no fewer digits before its point, nor after it.

A change inside a struct, an array or a map is a change of the type of its column. `widen` takes it when the shape is the same (the same fields under the same names, of a struct) and each change inside it is one of the above. A field added or removed inside a struct is incompatible: evolution inside a nested column is not in P9.

Every other change is incompatible, under either value: a long to a double and an integer to a float, which lose digits; a date to a timestamp, with a zone or without, the second of which needs another feature of the protocol, for a source that did not ask for it; any narrowing; any change of kind. It fails as permanent, and names the column and both types. The way on is a rename of the column of the source to a new column of Bronze (§5): the old column has nothing from then on, under `on_missing_column: null`.

Delta's own refusal is still there, behind the decision: a write that Delta refuses for its schema or its features (`AnalysisException`, `DeltaTableFeatureException`) is a permanent failure. It is not left for 70.

### 4. Type widening is turned on by the first widening that needs it

Not when a table is made. A table that never widens keeps its protocol, and every reader of Delta reads it.

- The first write of a source with `widen` that widens a column of a table without the feature turns it on, by `ALTER TABLE … SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')`, before the commit of its rows. It does so only where the snapshot of the table does not have the property: set again, it would commit the metadata again, and fail every write in course on the table (P9.1).
- That commit is not the commit of the rows. A run that dies between the two leaves the feature on and the column as it was. A replay finds the feature on, and widens. The feature on with nothing widened changes no value, only the protocol.
- Every widening turns the feature on, those of a byte or a short to an int among them, which Delta would make without it. There is one way to widen, with one cost, recorded by Delta on the column (`delta.typeChanges`) as well as by the engine (§11).
- *What it costs.* From that commit on, a reader or a writer of the table must support the feature `typeWidening`, on the whole table and not only on the column. Of Delta, the versions from 4.0 on, where it is fully supported: an earlier one may know the feature and still refuse a change that it did not support (Delta 4.4.1's own `DELTA_UNSUPPORTED_TYPE_CHANGE_IN_PREVIEW` asks for 4.0 or higher). Of the other engines, those that support the feature, to be checked against each reader and not assumed. And a table that widened is read by version with the limits of P9.1 (Consequences).
- *Taking it off* is an operator's, by `ALTER TABLE … DROP FEATURE typeWidening`. The engine never does it. The table keeps its types and its history, and a writer is then to support `checkpointProtection`.

### 5. `renames` is an alias at read

`renames` maps a name of a column in the source to its name in Bronze. The read gives the column of the source under its name in Bronze, before the read is compared with its table. It touches no table: no column mapping, and no `RENAME COLUMN`.

- **Several names of the source may go to one column of Bronze**: a source that renamed a column twice, or two sources of one table. A row of a table, or a file, that has two of them at once fails its batch as permanent. It is not set apart (§9).
- **What it is for.** A column that the source renamed (`new_name: old_name` keeps writing to the column that Bronze has); a column of the source under a name of the engine's (§17, P5: the way on is `renames`); a name that a table of Delta cannot have, in a header of CSV or a field of JSON as in a table of a database, which the JDBC connector refuses before a plan today; and a change of a type that is incompatible (§3).
- **The key is free text, and the value a name of the grammar.** The key is a name as the source has it, bounded in its length and with no character of control. The value is a `SqlIdentifier`, and no column of the engine, as in P3. *Changed from P3,* whose comment has the map as "old to new" and its key as a `SqlIdentifier`: the key is the source's name, whatever it was before.
- **A name of the source is never read as an expression** (ADR-0013 §1). It reaches the SQL of a source only quoted, as every name does, and Spark only as the name of a column, never parsed: Spark reads `a.b` as a field of a struct, and the grave accent as a quote. The names that were the way in of P6's tests of injection (`a"b`, `it's`, ``t`ick``, `a.b`, `x--y`, `p/*q`, `'; DROP TABLE canary; --`, `ñandú`, `a|b`, `a\b`) are tested at the read, at the write, in the snapshot of the configuration and in the commit that records the change (§11), where a name is written as JSON.
- *Amends ADR-0008*, which has Bronze rename nothing. An alias changes a name and no value, and is the source's own word for it. The configuration of each run is kept by version (P4), so the name in the source of a column of Bronze is known for every batch.

### 6. Names that differ only in their case

Names that differ only in their case are one column: the fields of the records of a batch of JSON, and the headers of the files of a batch of CSV. Spark and Delta compare names so (P9.1). They take the spelling of the table. This is not a value of the policy: it is always so (§9). A record of JSON that has both spellings is set apart, as `duplicate_field`, as a field written twice is (P8.8b).

Where there is no table yet, the spelling is the one of the first file of the batch, in the order of its plan, that has the column, and in that file of its header or of its first record that has it: the producer's first spelling. The plan fixes the order of its files, so a replay gives the same.

Renames are matched first, by the name exactly as the source writes it, and the names are folded after.

### 7. Within a batch of files

A batch of files is read as one schema before it is compared with its table. That is the read's, and not the policy's:

- **CSV:** files are read by groups of the same header, and the groups are joined by name. A file without a column has nothing in it.
- **JSON:** the fields are folded by case (§6).
- **Parquet:** a column whose files have two types, one a widening of the other by §3, has the wider, and the files of the other are cast. Two types that are not so are incompatible: the batch fails as permanent.

### 8. A source that its policy stops

Under `fail`, and for a change that is incompatible, the batch stays planned and the run ends in 4. Its message names the column, the file where there is one, and the ways on: to change the policy, to declare a rename, and to take the file out of the landing zone, after which ADR-0018 gives the batch up and the next run plans without it. Changing the policy or the renames is enough to unblock a batch that is planned (§9). The source stops by its own choice, the criterion of ADR-0023. This closes the item of §20 on a planned batch that blocks its source, for data that is no file's.

`quarantine`, a value that would set apart the rows of a batch that its policy refuses, is a candidate extension, not part of P9.

### 9. The policy is read from the configuration of the run, not from the plan

ADR-0024 §3 has "what the plan has decides what the rows are; what the configuration has decides what is done with a failure". The reason is the quarantine. The records that a batch sets apart may be written before the process dies, and a replay that read otherwise would leave records set apart by one read and rows of Bronze from another, with counts that do not add up.

The policy and the renames decide neither which records are read nor which are set apart. They give a shape to the columns of the rows of Bronze, which a batch writes at once, in one commit, under its transaction (`txnAppId`, `txnVersion`). The quarantine and the counts do not depend on them. So the policy and the renames are those of the run that writes the batch, and that is what lets a change of them unblock a batch that is planned (§8). Changing them is no breaking change (`ConfigVersion.breakingChanges` does not look at `schema`).

For that to hold, nothing of the policy may move a record between Bronze and its quarantine. That is why a row with two names of one rename fails its batch, and is not set apart; why the folding of case is not a value of the policy; and why the read of CSV by groups of header is the read's (§7).

**The test:** a batch whose quarantine is written, and whose run dies before Bronze (a hook of the engine), is replayed under another policy (`add` to `rescue`, and a rename). The quarantine is the same, Bronze has the new policy, and what was read is what was written plus what was set apart.

### 10. A change of the table under a write

The policy is evaluated against the table at each attempt of the write (§1). After a `MetadataChangedException` that is the table as another writer left it, with the same read:

- **The policy takes it** (a column that the other added is null in these rows; a column that the other widened casts these rows to its type): the write is a conflict like those that ADR-0010 retries, within the budget of the run (ADR-0022 §5), counted in `write_conflicts`. A budget that runs out ends the run in 3, as `conflict`. Run again later, it may pass.
- **The policy refuses it:** a permanent failure, at once, in 4.

A `ProtocolChangedException` comes only to the engine's own commit that turns type widening on (§4), where another writer changed the protocol too. The next attempt looks at the table again, finds the feature on or turns it on, within the same budget.

*Supersedes in part ADR-0022 §1*, which makes such a conflict permanent after one retry. That was right while nothing could tell a table that the source takes from one that it does not. The policy can, and a permanent failure would stop a source of a fan-in each time that another source adds a column that this one takes.

### 11. The change is made, and recorded, in the commit of the rows

- **In the same commit.** The rows are adjusted to the table and what was decided: renamed, folded, cast, rescued, with the columns added that the policy adds. They are written with `mergeSchema` on only where the decision changes the table, and with it off otherwise. Never the `autoMerge` of the session: Delta would decide what the policy did not, a byte or a short widened to an int among them (P9.1).
- **A batch with no rows writes nothing, as since P5, so it changes no schema.** The one commit of no rows is that of a table of a database whose first row went between the probe of the run and its write (P6). Its columns are those of the source's table, which the source has, and the change that it commits is as true as with rows.
- **Recorded in the commit of Delta** (`CommitMetadata`): the columns added, widened (from each type to the other), rescued and missing. Names and types, and never a value. It is atomic with the rows, and exact even when the run dies before its state is committed. No migration of `control` is made. P11 shows it in its views of a commit and its run.
- **The run says it in its summary**, with the same names and types.

## Alternatives

- **A schema remembered for each source**, in the control plane. It is state to keep in step with the table, which other writers change. It cannot be looked at again when the table changes under a write, and a fan-in has several of them for one table.
- **Column mapping**, for renames and for names that Delta refuses. It changes the protocol of every table that uses it, and the renaming or removal of a column is a change that the change data feed and streams read through only with their schema tracked. An alias at read gives what is needed, with no change to the table.
- **Type widening on when a table is made.** Every table would pay for it in its protocol and its readers, and most never widen.
- **A byte or a short widened to an int without the feature**, as Delta does by itself. The protocol would be kept for the most frequent change of a table of PostgreSQL, `smallint` to `integer`. But it is a second way to widen, which Delta does not record on the column, and whose reads by version P9.1 did not pin. One way, with one cost, is kept. To be weighed again if a reader of Bronze without the feature has to be served.
- **The `autoMerge` of the session.** Delta would decide what the policy says.
- **The policy taken from the plan**, as the dialect of CSV is (ADR-0024 §3). Changing the policy would not unblock a planned batch, and §9 shows that the plan needs nothing of it.
- **`quarantine` as a value of the policy.** The policy would move records between Bronze and its quarantine, against §9. A candidate extension.
- **A conflict that asks for a re-plan left permanent after one retry** (ADR-0022 §1). It stops a source that its policy lets through, and asks somebody to look at what needs nothing.
- **The change recorded in a table of the control plane.** Not atomic with the rows: a run that dies between the commit and the control plane loses the record, or records a change that was never made.
- **A name that differs only in its case kept as a column of its own.** Spark and Delta take both for one, and a producer that changed the case of a field would stop its source for what is the same column.

## Consequences

- **The cost of `widen`, as P9.1 pins it.** A table that widened can be read by a reader of the feature `typeWidening` alone. A batch read of its change data feed whose range starts before the widening fails, even one that ends before it. ADR-0005 §5 and ADR-0010 §4 have Silver read Bronze by ranges of versions from the change data feed: how it reads through a widening is P10's to choose, among the ways that P9.1 pins, and is an item of §20. A stream of the change data with `Trigger.AvailableNow` reads through with no setting of Delta's own, and is the one preferred (2026-11-28); it is also the way that a stream of Bronze would take (CLAUDE.md §24). A stream that tracks its schema stops at each widening until somebody lets it through.
- `widen` is never the default. A source that leaves `on_type_change` out has `fail`.
- The decision is a function with no I/O, from the schema read, the schema of the table and the policy to an adjustment or a refusal, in `engine` (P9.3), tested by a table of cases.
- It closes, when built, the items of §20 on a planned batch that blocks its source for data that is no file's, on the fields of JSON that differ in their case, and on the headers of files with names that Delta refuses.
- D-123 changes with P9.5: a conflict that asks for a re-plan is permanent only where the policy refuses the table.
- §14 and §14.1 keep their codes: 3 for a conflict whose budget ran out, 4 for a schema change under `fail`. No new code, and no new name of `error_class`.
- A fan-in under `on_missing_column: fail` needs every source to have every column of the table.
