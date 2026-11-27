# Configuration reference

What a configuration is made of, field by field: what each field takes, what it is when it is left out, and the rules that are about more than one field. For how to add a source, step by step, see [Adding a source](adding-a-source.md).

A test reads this document (`ConfigurationReferenceSpec`): the fields of the tables below are those of the model, no more and no fewer, and the examples marked as valid are valid.

- [The files](#the-files)
- [The profile](#the-profile)
- [A connection to a database](#a-connection-to-a-database)
- [A source](#a-source)
- [How values are written](#how-values-are-written)
- [YAML, as it is read here](#yaml-as-it-is-read-here)
- [Rules about several sources](#rules-about-several-sources)
- [Validating](#validating)
- [The version of a configuration](#the-version-of-a-configuration)

## The files

| File | What it says | How many |
|---|---|---|
| `conf/app/<profile>.yaml` | Where the application runs and on what: the lake, the landing zone, the databases, how long it waits | One for each place it runs. `conf/app/local.example.yaml` is the example; `conf/app/local.yaml` is the profile of one machine, and git ignores it |
| `conf/sources/<id>.yaml` | What is ingested from one place, and into what | One for each source, named by its `id`. They are the same in every profile |

- **A source is the `.yaml` file of its name.** In the directory of the sources, a file that ends in `.yml` is an error and so is a directory: nothing that could be a source is passed over in silence. A `README.md`, or a file whose name starts with a dot, is not configuration.
- **A file is UTF-8, and 1 MiB at most.** A byte order mark and Windows line ends are read.
- **Nothing in a file is a secret.** A password is written as where it is, never as what it is ([references to secrets](#a-reference-to-a-secret)).

## The profile

<!-- valid: profile -->
```yaml
lake:
  uri: "file:///srv/lakehouse/lake"
  deployment: single-machine
landing:
  root: "file:///srv/lakehouse/landing"
databases:
  control:
    url: "jdbc:postgresql://localhost:5432/ingestion_control"
    user: ingest_control
    password: { secretRef: "env:INGEST_CONTROL_PASSWORD" }
    tls: { mode: disable }
  coordination:
    url: "jdbc:postgresql://localhost:5432/ingestion_control"
    user: ingest_coordination
    password: { secretRef: "env:INGEST_COORDINATION_PASSWORD" }
    tls: { mode: disable }
    timeouts: { connect: 5s, statement: 10s }
retries:
  budget: 2m
  first_backoff: 1s
  max_backoff: 30s
leases:
  ttl: 2m
spark:
  master: "local[2]"
  settings:
    spark.sql.shuffle.partitions: 4
```

<!-- fields: profile -->
| Field | Takes | Left out |
|---|---|---|
| `lake.uri` | The [root](#a-root) of the lake. Every table of every source is under it | required |
| `lake.deployment` | `single-machine`: every process is on one machine and sees its disk. `cluster`: processes on several machines | required |
| `lake.shared_filesystem` | `true` for a `file://` lake that every machine of a cluster mounts (NFS). Allowed, and warned of | `false` |
| `landing.root` | The [root](#a-root) that the paths of file sources are relative to | required |
| `databases.control` | The [connection](#a-connection-to-a-database) to the control plane: runs, batches, leases, state | required |
| `databases.coordination` | The [connection](#a-connection-to-a-database) to the database that the LogStore claims the versions of a table in | required |
| `retries.budget` | For how long a run tries again after a failure that may pass, counted from its first failure: a database that does not answer, a connection lost, a conflict that Delta asks to be retried. No attempt starts once it is spent, and the one in course may finish, so a run ends within the budget plus the length of one attempt from its first failure. A [duration](#a-duration), `1s` to `1d` | `2m` |
| `retries.first_backoff` | The ceiling of the wait before the first retry. The ceiling doubles at each retry, up to `max_backoff`, and each wait is drawn from nothing to its ceiling, so that runs that failed together do not come back together. `100ms` to `1h` | `1s` |
| `retries.max_backoff` | The highest ceiling of a wait between two attempts. `100ms` to `1h` | `30s` |
| `leases.ttl` | For how long a run holds the lease of its source without renewing it. A run renews it every third of this, so a run that dies leaves its source taken for this long before another may have it. A [duration](#a-duration), `10s` to `1d` | `2m` |
| `spark.master` | `local`, `local[N]`, `local[*]`, or `spark://host:port` for a Spark Standalone cluster | required |
| `spark.settings` | Spark settings to add, by their key. A value is text, a number or a boolean | none |

Rules about several fields of a profile:

- **A lake on a local disk is for one machine.** `lake.uri` on `file://` with `deployment: cluster` is an error, unless `shared_filesystem` is `true`. And `shared_filesystem: true` is an error anywhere else, where it says nothing.
- **A cluster of Spark needs storage that its workers share.** `spark.master: spark://…` with a single-machine lake on a local disk is an error: every worker would write to its own disk.
- **The landing zone and the lake are apart.** Neither is the other, nor inside it: files that land are never read as tables.
- **The two databases have a role each.** `databases.control.user` and `databases.coordination.user` are two roles, each with the privileges of its own work.
- **The backoffs fit**: `first_backoff` ≤ `max_backoff` ≤ `budget`.
- **The retry budget is longer than the LogStore itself waits.** `retries.budget` must be longer than three times `databases.coordination.timeouts.connect`: 15 s with the defaults. A commit without the coordination database takes that long to fail, and a shorter budget would give up on outages that would have passed.
- **A lease outlasts a slow renewal.** `leases.ttl` must be at least three times `databases.control.timeouts.connect` plus `databases.control.timeouts.statement`: 45 s with the defaults. A lease is renewed every third of its time to live, and a renewal can take that long to be answered; a shorter lease would be lost to a renewal that was still waiting. A run that has seen no renewal for five sixths of the time to live stops working, before its lease expires.
- **Spark settings name no master, nothing that the engine sets, and no credential.** A key must start with `spark.`. `spark.master` is the field above. The engine gives every session of a run its own `spark.app.name`, `spark.sql.extensions`, `spark.sql.catalog.spark_catalog`, `spark.databricks.delta.history.metricsEnabled` (on: the rows that a run wrote are read from the commits of its batch), `spark.sql.session.timeZone` (UTC, which `run` sets the JVM to as well, before it connects to anything), `spark.sql.legacy.javaCharsets` (on: Spark reads `windows-1252` only so, [ADR-0024](../adr/0024-the-dialect-and-the-encoding-of-a-source-of-csv.md) §2), `spark.delta.logStore.*` and `spark.hadoop.lakehouse.logstore.*` (made from `databases.coordination`), and a profile that sets one of them is refused rather than overridden. A key with `password`, `secret`, `token` or `access.key` in it is refused: object storage takes its credentials from the environment or from a Hadoop credential provider. Keys are compared with case aside.
- **New tables keep their transactions.** `spark.databricks.delta.properties.defaults.setTransactionRetentionDuration` is refused: it would give every table that Delta makes `delta.setTransactionRetentionDuration`, and Delta would forget the batches written to it, which a replay would then write again. A table that has the property is refused when a run asks it anything, and `run` refuses a session that has the setting from elsewhere (`spark-defaults.conf`, a system property).

What is valid and warned of:

- `lake.shared_filesystem: true`: renames and listings on such storage are not those of a local disk.
- A lake under `/tmp`: it is emptied when the machine restarts, and the coordination database would still have the commits of its tables.
- A database on another machine with `tls.mode: disable`.

## A connection to a database

The same fields for the two databases of a profile and for the database that a JDBC source reads.

<!-- fields: connection -->
| Field | Takes | Left out |
|---|---|---|
| `url` | `jdbc:postgresql://host:port/database`, and nothing after it. The port is 5432 if it is left out | required |
| `user` | The role to connect as: a [name](#a-name) | required |
| `password.secretRef` | A [reference to the secret](#a-reference-to-a-secret): `env:NAME` or `file:/absolute/path` | required |
| `tls.mode` | `disable`, for a database on the same machine, or `verify-full`: the server proves it is the host that was named | required |
| `tls.root_certificate` | The absolute path of the certificate of the authority that signed the server's. Required with `verify-full`, and an error with `disable` | — |
| `timeouts.connect` | How long a connection is waited for, which is how long a call takes to fail when the database is down. `250ms` to `5m` | `5s` |
| `timeouts.statement` | How long one statement may run. Of a source, which is read with a cursor, how long one fetch of it may take, and not the read whole: it tells a read that hangs. `1s` to `1h` | `10s` for a database of the profile, `30m` for that of a source |

- **A URL takes no parameters, no user and no password.** What a parameter would set is a field here, and in a URL it would win over the field without a word. Some parameters of the driver name a class for it to load (`socketFactory`, CVE-2022-21724).
- **There are two modes of TLS**, and none in between that encrypts and trusts anyone.

## A source

The example with every section is [`conf/sources/sales.orders.yaml`](../../conf/sources/sales.orders.yaml); the one with the least a source must say is [`conf/sources/sales.returns.yaml`](../../conf/sources/sales.returns.yaml).

<!-- fields: source -->
| Field | Takes | Left out |
|---|---|---|
| `id` | The name of the source: lower-case words joined by dots, as `sales.orders`, 100 characters at most. The file is `<id>.yaml` | required |
| `enabled` | `false` for a source that is kept and not run | `true` |
| `owner` | Who answers for the source: up to 100 characters of printable ASCII | none |
| `connector.type` | The kind of connector: `jdbc` or `file`. The other fields of `connector` are the kind's, below | required |
| `load.mode` | `full`: everything, every time. `incremental`: what changed, by a watermark. `new_files`: the files not taken yet | required |
| `load.watermark.column` | The column of a timestamp that only grows, as `updated_at`: a [name](#a-name). [The watermark](#the-watermark) says why a timestamp. Required for `incremental`, and an error for any other mode | — |
| `load.watermark.lookback` | How far before the last watermark a batch starts again, reading again what the batch before read, and no row at or before `start`. A [duration](#a-duration) | `0s` |
| `load.watermark.safety_margin` | How far behind the present of the database a batch stops, for transactions that are still open. A [duration](#a-duration) | `0s` |
| `load.watermark.start` | Where the first batch of the source starts: a [moment](#a-moment). The rows at or before it are not the source's to ingest | the first batch reads the whole table |
| `load.max_batch_range` | The widest stretch of time that one batch takes, counted from the watermark. A [duration](#a-duration), `1s` at least. `incremental` alone | no limit |
| `keys` | The columns that tell one row from another: what Silver merges by. A list of [names](#a-name), each once | none |
| `deletes.strategy` | How rows that the source deleted are found, when the load does not find them: `key_reconciliation` or `deleted_flag` | deletes are those the load sees: a full load sees them all |
| `deletes.column` | The column in which the source marks a deleted row. Required for `deleted_flag`, and an error otherwise | — |
| `schema.on_new_column` | `add`, `rescue` or `fail` | `add` |
| `schema.on_missing_column` | `"null"`, in quotes, or `fail` | `"null"` |
| `schema.on_type_change` | `widen` or `fail` | `fail` |
| `schema.renames` | Columns that changed their name, old to new. A rename cannot be told from a column that went and another that came, unless somebody says so | none |
| `columns.<name>.pii` | What kind of personal data the column holds: a word of the team's own, as `email` or `national_id` | none |
| `columns.<name>.silver.mask` | How Silver hides the column: `hmac` or `redact` | not masked |
| `quality.max_rejected_ratio` | The share of the records of a batch that may be [rejected](#records-that-are-rejected) before the batch is not committed. `0` to `1`; the share itself is allowed | `0` |
| `targets.bronze.table` | The Bronze table: a [path](#a-relative-path) relative to `lake.uri` | required |
| `targets.silver.table` | The Silver table. Without it, the source goes to Bronze alone | no Silver |
| `targets.silver.partition_by` | The columns the Silver table is partitioned by, in their order | not partitioned |
| `freshness_sla` | How old the data of the source may get before it is late. A [duration](#a-duration), `1s` at least | none |

What each value of the schema policy and of the masks does is decided where it is built (discovery §17, P9 and P10).

### The watermark

An incremental load reads, at each batch, the rows whose watermark column is after the watermark of the source and no later than an upper bound, and then moves the watermark to that bound. The bound is what the clock of the database says when the batch is planned, less `safety_margin`, or sooner if `max_batch_range` cuts it. Both bounds are times of the database, and the clock of the machine that runs the engine bounds nothing.

- **A timestamp, and not a number that grows.** A row gets the time of its column, or the number of a sequence, when its transaction writes it, and is seen when the transaction commits: a row with an earlier time, or a lower number, can appear after a later one was read. A margin of time covers that for a transaction shorter than the margin. No margin can be said in the numbers of a key, so a key that grows is not taken as a watermark. `timestamptz` is the type to have; a `timestamp` without a zone is taken as UTC.
- **A `timestamp` without a zone has to be of UTC.** The bounds of a batch are moments of the clock of the database, and a time without a zone is compared with them as it is written. If the writers of the table stamp the column in another zone, as a session that is not in UTC does with `now()`, a row is read late where that zone is ahead of UTC, by the difference, and is never ingested where it is behind: its time is before the watermark already when it is written, and only the first batch, which has no lower bound, reads the table. The engine cannot tell whose time a column holds, so it warns of such a column each time a batch is planned.
- **`safety_margin` is what loses nothing; `lookback` reads again.** A row is lost when its transaction committed later than `safety_margin` and `lookback` together after the time it wrote. The margin has a batch wait for such transactions, and reads nothing twice. The lookback reads the last stretch again at every batch, and Bronze keeps every copy for Silver to tell apart. So the margin is the one to set, longer than the longest transaction that writes the table, and `lookback` stays at `0s` unless a source says otherwise. The margin covers whatever comes between the time that a row is stamped with and the moment that the row can be read where the engine reads it: where that is a replica, how far the replica is behind as well.
- **The first batch.** Without `start` it reads the whole table up to its bound, in one statement, however long the history of the table is. With it, it starts there. `max_batch_range` is counted from the watermark, and the first batch without `start` has none: it is not cut.
- **`start` starts a source, once.** It is where a source with no watermark starts. After the first batch it bounds one thing: `lookback` reads no row at or before it, since those are not the source's to ingest. Changed after the first batch, it moves no window forward and skips no row. The history before `start`, and any stretch of the column, is read by the command `backfill`, on a sequence of its own beside the runs of the source: a range `(from, to]` that `start` does not bound, in chunks of `max_batch_range` unless the command says another (`--chunk`). It does not move the watermark of the source ([ADR-0021](../adr/0021-operator-actions-and-backfills.md) §5).
- **The watermark never goes back.** A batch is planned only when its bound is after the watermark. If the clock of the database went back, or `safety_margin` was raised, the source waits until its bound passes the watermark again, and a run meanwhile ends well with nothing read. A run that finds the clock itself before the watermark warns of it, with both: what the database stamps until its clock is past the watermark again is never read, unless `lookback` reaches it.
- **A row with no time is in no batch.** A row whose watermark column is null, or `infinity`, is after no bound and before none: it is never ingested. A column that may be null is warned of when a batch is planned. `-infinity` is in the first batch of a source without `start`, and fails it.
- **The column is the source's to index.** Each batch asks the database for the rows of a stretch of the column, and for the first of them before that. Without an index on the column, each is a pass over the table.
- **A batch is read as it was planned.** A batch that a run left planned is finished under the `load` that it was planned under: with another mode, or another watermark column, the run fails and says so. The source is given back the load that it had, the batch is finished, and the load is changed then. The same goes for the table: a batch that was planned by a column is not read from a table that has lost the column since, or has it under another type.

The decision, with what was left out and what it costs, is [ADR-0020](../adr/0020-the-watermark-of-an-incremental-load.md).

`validate` warns of a watermark with `safety_margin` and `lookback` both at `0s`, which loses the rows of any transaction that is still open when its window is read, and of `max_batch_range` without `start`, which does not cut the first batch.

### Records that are rejected

A record that cannot be made a row is not lost, and does not stop the rest of its batch by itself. It is kept whole, as the text that it was, in the quarantine of its source: a table of the lake at `_quarantine/bronze/<id of the source>`, with the file that it came from, the batch, the run, and why (`error_type`, and `error_detail` where more is known): `unparsed`, a record that the parser of its format could not parse, as a line that is no record of JSON or a record of CSV with more values or fewer than its header; and, for JSON, `trailing_text`, a record with text after its object, with the column where the text starts, and `duplicate_field`, a record with a field twice at its first level, with the name of the field.

`quality.max_rejected_ratio` says how many are too many. It is a share of the records that a batch read, and it is allowed whole: with `0.01`, one record of a hundred is taken, and two are not. When a batch is beyond it, nothing of the batch is written to Bronze, the run ends in 5, and the rejected records are in quarantine to be looked at. The same data gives the same result, so the batch waits until the source allows more, or sends what it meant to. By default no record may be rejected.

A quarantine holds what its source holds, the personal data of the source among it. No source can name a table under `_quarantine`: a [relative path](#a-relative-path) has no segment that starts with `_`.

**Reading a quarantine.** A batch writes its rejected records before its data, whatever becomes of it. A batch that is then given up, because a file of it changed before it could be finished, leaves its records in the table, and the batch after it sets the same records apart again under its own number. Nothing is deleted for it: the control plane says which batches were given up, and a record says its batch (`batch_seq`). So whoever reads the table leaves out the batches that this gives, as the reader of the engine does (`QuarantineReader`):

```sql
SELECT batch_seq FROM control.ingestion_batch
 WHERE txn_app_id = '<id of the source>/bronze' AND status = 'ABANDONED';
```

### The `jdbc` connector

A table of a relational database. PostgreSQL, for now. It loads by `full` or by `incremental`.

<!-- fields: connector jdbc -->
| Field | Takes | Left out |
|---|---|---|
| `connector.connection` | The [connection](#a-connection-to-a-database) to the database | required |
| `connector.table` | The table: a [name](#a-name), or `schema.name` | required |
| `connector.text_columns` | Columns of the table that are read as text, whatever their type: a list of [names](#a-name), each as the source writes it | none |

- **A table is read in one statement, with a cursor.** The rows come a thousand at a time, and are written to Bronze as they come: the table is never held whole, and a full load is one snapshot of it. `timeouts.statement` bounds one fetch.
- **The role needs to read the table, and nothing else.** The table is asked for its columns before a batch is planned: one that is not there, or that the role may not read, fails the run with nothing planned.
- **A session of a read has its own settings**, whatever the database or the role has them at: `standard_conforming_strings` on, `idle_in_transaction_session_timeout` off, `default_transaction_read_only` on, and the statement timeout above. That a session is read-only is a guard against a write by mistake, and no barrier: what keeps the engine from writing to a source is a role that may only read.
- **Three kinds of column are refused, before a batch is planned**, each with its name. Two whose names differ only in their case: a table of Delta cannot have both. One whose name holds a space, a tab, a line break or any of `,;{}()=`, which a table of Delta cannot have at all. And one under the name of a column that the engine adds to a row, whatever its case (`_run_id`, `_batch_seq`, `_ingested_at`, `_source_file`, `_source_id`, `_rescued_data`): in Bronze it would be taken for the engine's own. A view of the table can give such a column another name.
- **A database that is busy is not one that said no.** A server, or a role, with no connection left to give, and a statement that waited for a lock longer than the database lets it (`lock_timeout`), as while a table is being altered, end a run in 3 as a connection that failed does: nothing of the source is to be changed, and the next run may find it free.

Bronze keeps the types of the source, and changes no value without saying so. A type that Spark has no type for that holds its every value is kept as text, which is the value as the server writes it:

| In PostgreSQL | In Bronze |
|---|---|
| `smallint`, `integer`, `bigint`, `real`, `double precision`, `boolean` | The same |
| `numeric(p, s)` of 38 digits at most | `decimal(p, s)` |
| `numeric` with no bound, of more than 38 digits, or with a scale below zero | Text: every digit, and `NaN` |
| `text`, `varchar`, `char(n)` | Text; a `char(n)` with its padding |
| `date`, `timestamp`, `timestamptz` | A date, and a timestamp in UTC to the microsecond. A `timestamp` without a zone is taken as UTC |
| `time`, `timetz` | Text |
| `bytea` | Binary |
| `bit(1)` | Boolean |
| `bit(n)`, `varbit` | Text, of zeros and ones |
| `uuid`, `json`, `jsonb`, `xml`, `inet`, `interval`, `money`, an enum, the geometric types | Text, as the server writes it; `money` with the symbol of the server's locale |
| An array | An array of what its values are kept as. Text, if they are kept as text, or are dates or timestamps |

- **`infinity` fails a batch, unless its column is read as text.** PostgreSQL has `infinity` and `-infinity` for a date and for a timestamp, and Spark has not: it would write the last day of the year 9999 in its place. The source is asked of each row, in the statement that reads it, and a row that has one fails its batch for good, with the column named. `9999-12-31` itself is a date like any other. A column that holds `infinity`, as a `valid_to` often does, goes in `connector.text_columns`: Bronze then has the text `infinity`, and the dates as the source writes them.
- **A bounded `numeric` that holds `NaN` fails its batch as well**: a decimal has none. Its column goes in `connector.text_columns` too.
- **Ten days of 1582 are read as ten days later.** PostgreSQL counts by the Gregorian calendar from its first day, and its driver gives a date to Spark in a calendar that goes from 4 October 1582 to the 15th: a `date`, a `timestamp` or a `timestamptz` from the 5th to the 14th is in Bronze as the day ten days on, with nothing said. Every other day, of any year, is the day of the source. A column that holds such days goes in `connector.text_columns`, where they are kept as the source writes them.
- **`connector.text_columns` names columns that the table has**, each once, as the source writes them; a name that the table has not fails the run before a batch is planned. It cannot name `load.watermark.column`, which is read as the timestamp that it is. A column that is in Bronze already under another type is not changed by it: the table refuses rows of another type, and what a table does with a type that changed is its schema policy.

### The `file` connector

Files that are left in a directory of the landing zone. It loads by `new_files`. The directory is listed, and not the directories under it; a file whose name starts with `.` or `_` is never taken.

**CSV is read in the dialect and the encoding of its source** (`connector.csv`; [ADR-0024](../adr/0024-the-dialect-and-the-encoding-of-a-source-of-csv.md)), with a header. A source that says nothing is read as RFC 4180 writes CSV, in UTF-8: values are separated by commas, a value is quoted if it has a comma, a quote or the end of a line in it, and a quote inside a quoted value is written twice (`"said ""hi"""`); a backslash is a character like any other. A source that writes otherwise says how, in four fields:

- **`delimiter`**, what separates the values of a record: `;`, as a spreadsheet writes CSV in Spain, a tab (`"\t"`, between double quotes: between single quotes it is a backslash and a `t`), `|`, or any other character;
- **`quote`**, what a value is quoted with;
- **`escape`**, what a quote inside a quoted value is written after. It is the quote by default, which is the quote written twice. Under `\`, `\"` is a quote and `\\` a backslash between quotes, and a backslash before any other character, or outside quotes, is itself. The escape is the source's to say, and not a guess: a file that writes its quotes twice, read under `\`, has values with every quote they were written with, or records set apart;
- **`encoding`**, what the bytes of every file of the source are in: `UTF-8`, `ISO-8859-1` or `windows-1252`, by those names, whatever their case (`latin1` and `cp1252` are refused, with the name to write). A spreadsheet in Western Europe writes "CSV" in `windows-1252`, and "CSV UTF-8" in UTF-8 with a mark of the order of bytes, which is taken off and never reaches a name. A file in `windows-1252` said to be `ISO-8859-1` has its euro, its quotes and its dashes as control characters: its encoding is `windows-1252`.

Each of the three is one character: not the end of a line, not NUL, and one that the encoding can write. The delimiter is neither the quote nor the escape; the quote may be the escape. Values over several lines are always read, between quotes. `connector.csv` is for a source of CSV alone. A dialect that changes is no change that Bronze cannot follow, since its columns of CSV are text; a source that says the default, or nothing, keeps the hash of its configuration.

**A batch is read as it was planned.** The plan of a batch says the format of its files and their dialect, and every run that reads it reads so, whatever the configuration says by then, with a warning when the two differ: a batch whose process died halfway is finished as it was begun. To have a batch that is planned read in a dialect that was corrected, give it up (`abandon-batch`): the next plan takes the same files, read as the source says. A reprocess (`reprocess`) reads the files of a batch that was committed as the source says when it is asked for.

**What a read of CSV does not keep as it was written** ([ADR-0024](../adr/0024-the-dialect-and-the-encoding-of-a-source-of-csv.md), as `SparkCsvDialectContractSpec` pins it):

- **Between quotes, the end of line of a file is read as LF.** The end of line of a file is the first that it has, that of its header. A file whose lines end in CRLF, as RFC 4180 and a spreadsheet write them, has LF in Bronze where a quoted value had CRLF; one whose lines end in CR has LF for each CR, and two for a CRLF. Every other end of line between quotes is kept: an LF alone inside a cell, which is what a spreadsheet writes, is an LF, and a CRLF in a file of LF, as PostgreSQL's `COPY` writes a text with one, is a CRLF. Spark has no option for it.
- **A file that mixes its ends of lines.** After a first CRLF, an LF ends no record: two records are read as one, and set apart for their value too many, but in a file of one column, where they are one value and nothing says so. After a first LF, a record that ends in CRLF keeps its CR at the end of its last value, and nothing says so. Nothing finds it: whether a CR before an LF ends a record or is a character of a quoted value is known only by where the quotes are. Such a file is made of pieces that two writers wrote; a spreadsheet, `COPY` and pandas end every record the same way.
- **A byte that is not of the encoding of the source** is read as U+FFFD, without a word. *Until P8.9c*, which makes a file with one a file that cannot be read.
- **A file is read whole, by one task**: a file of many gigabytes is read by one core, and the files of a batch side by side.

**JSON is one record to a line** (JSON Lines), a line ended by `\n` or `\r\n`; a blank line is passed over. A record is one object, with each field of its first level once and nothing after it but spaces, and each value is kept as it was written: a string as what it says, `null` as none, and a number, `true`, an object or a list as the text that it was written as (`10.50` is `10.50`). Everything else is set apart, with why:

- **text after the object** (`trailing_text`), as `{"id":"4"} trailing`, `{"id":"4"} {"id":"5"}` or a comment: Spark's own reader would take the object and drop the rest without a word;
- **a field twice at the first level** (`duplicate_field`), as `{"id":"1","id":"2"}`: Spark's own reader would keep one of the two values, or refuse the batch. A field twice inside an object of a record is kept, as the object's text;
- **a line that is no whole object** (`unparsed`): a number, a string, a broken object, and each line of a record written over several lines, as a pretty-printed one, since a source has no field to read it otherwise. *Changed with P8.8:* a line that is a list of objects, as `[{"id":"1"},{"id":"2"}]`, is set apart too. Spark's reader made a row of each object of it up to P8.8, rows that could not be told apart by their line, and none of an empty list.

They count against `quality.max_rejected_ratio` as any record set apart does. Fields that differ only in their case still fail the batch, as below.

**The columns are the source's, under the names that it gave them, and each name is one column.** Spark and Delta tell the names of columns apart without their case. So a batch of JSON whose records have fields that differ only in their case, as `id` in one and `ID` in another, fails, and names both: read as one column, the values under one of the two names would be lost. A record with a field twice fails its batch the same way; inside an object nothing is told apart, since an object is kept as the JSON it was written as. A header of CSV with a name twice, case aside, or with an empty name fails its batch as files with different headers do. A column may be named `_metadata`.

**A file that cannot be read stops its source, or is skipped and recorded, as the source says** (`connector.on_unreadable_file`; [ADR-0023](../adr/0023-a-file-that-cannot-be-read.md)). A file cannot be read when Spark fails to read it alone, at each of three reads, as one cut short or that the process may not open; the second read is a second after the first and the third two seconds after that, within the budget of retries of the profile (`retries.budget`), so that a mount that is back within them is not taken for a file that cannot be read; or when the landing zone refuses it without opening it: a symbolic link out of the root, a link to nothing, a pipe or a device. A batch whose read fails has each of its files read alone, to find them. A file that fails and then reads makes the run try again, as a failure that passes. A failure of the batch that no file has alone, as two files of Parquet with one column of two types, is not a file's.

- **`fail`, the default**: the run ends in 4 and names each file and why; the batch stays planned, and every run fails the same way until the file is written again, which the next run takes as a new file. A file that the landing zone refuses fails the listing, and nothing is planned.
- **`skip`**: the batch is given up, and each such file is recorded in the control plane as `FAILED`, with why (`processed_file`); the run ends well, and `run` plans again without them, in the same invocation, with a warning for each file skipped. A file that was skipped is taken again once it is written again, or touched: a file with another time is another file. A file outside the landing zone is planned, recorded, and never opened.

What is recorded of why is what Spark said of the file (the condition of its error and the type of its cause), never the text of an error, which may hold what the file holds. A reprocess (`reprocess`) skips nothing: it reads files that an operator named, and one that cannot be read ends it in 4, whatever the source says. No file is passed over without a word, whatever the Spark settings of the profile say of such files (`spark.sql.files.ignoreCorruptFiles`).

A file is taken once, and is known by its name, its size and when it was last changed. **A file that is written again under the same name is another file, and is ingested again, whole, into Bronze**, as is one that is only touched: Bronze keeps what was received, and does not compare a file with another. The rows that the two have in common are then in Bronze twice, each with its batch, and it is Silver that keeps one of them, by `keys`. A producer that corrects a file writes a new one; one that appends to a file it already left makes its rows arrive twice.

**A file that changes while its batch is ingested costs one run, and does not stop its source.** `min_file_age` keeps a file out of a batch until it has gone unchanged for that long. That is a guess about its producer, and not a promise from it. If a file is written again, or removed, after its batch was planned and before it is read, nothing of the batch is written: the batch is given up, with the reason and the files that changed recorded with it in the control plane (`ingestion_batch`, `ABANDONED`), and the run ends in 3. The next run plans anew, and takes the file as it is by then, as a new file ([ADR-0018](../adr/0018-a-run-gives-up-a-batch-whose-source-changed.md)). A source whose runs often end so has a producer that is slower than its `min_file_age`: raise it.

A file with no record, empty or with a header alone, is recorded as taken and writes nothing. The table of a source is made by its first batch that has rows.

<!-- fields: connector file -->
| Field | Takes | Left out |
|---|---|---|
| `connector.path` | The directory: a [path](#a-relative-path) relative to `landing.root` | required |
| `connector.format` | `csv`, `json` or `parquet` | required |
| `connector.pattern` | Which files of the directory, by their name: letters, digits, `_`, `-`, `.`, and the wildcards `*` and `?`. As `returns-*.csv` | every file |
| `connector.min_file_age` | How long a file goes unchanged before it is taken as complete: one that is still being written would be ingested cut short, and one that changes after its batch was planned costs a run. A [duration](#a-duration), `0s` to `1d`; `0s` is warned of | `1m` |
| `connector.max_files_per_batch` | The most files that one batch takes, `1` to `10000`. The rest are the next run's | `1000` |
| `connector.on_unreadable_file` | What a batch with a file that cannot be read comes to: `fail`, its run fails and names the file, or `skip`, the file is recorded as `FAILED` and the batch is made again without it. See below | `fail` |
| `connector.csv.delimiter` | For CSV: what separates the values of a record, one character. See above | `,` |
| `connector.csv.quote` | For CSV: what a value is quoted with, one character | `"` |
| `connector.csv.escape` | For CSV: what a quote inside a quoted value is written after, one character; the quote, for a quote written twice | `"` |
| `connector.csv.encoding` | For CSV: `UTF-8`, `ISO-8859-1` or `windows-1252` | `UTF-8` |

Rules about several fields of a source:

- **A kind of connector loads in its own ways.** `jdbc` by `full` or `incremental`, `file` by `new_files`.
- **Silver needs keys**: with `targets.silver`, `keys` is not empty.
- **Each layer has a table of its own.** The Silver table is not the Bronze table, nor inside it, nor around it.
- **`key_reconciliation` is for incremental loads**, and `deletes` needs `targets.silver`: deletes are marked in Silver.
- **A mask needs `targets.silver`** to be applied in.
- **The columns that the engine adds to every row are not the source's**: `_run_id`, `_batch_seq`, `_ingested_at`, `_source_file`, `_source_id` and `_rescued_data`, case aside, since Spark tells column names apart without it. None of them is a key, a watermark, the column of `deleted_flag`, a column under `columns`, or the new name of a rename. `targets.silver.partition_by` may have `_source_id`, written so: it is what a table that several sources merge into is partitioned by. A source whose data has a column of one of those names renames it (`schema.renames`, whose old name may be one); otherwise its run fails, and names the column.

## How values are written

Each of these is an allow-list: what is not in it is refused, with the character that is in the way named by its code. An error does not repeat the text it refuses.

### A name

Of a column, a table, a schema or a role. A letter of ASCII or an underscore, then letters, digits and underscores, 63 bytes at most: `order_id`, `_source_id`, `Orders`.

- Case is kept. Names are always quoted when they are used, so `Orders` and `orders` are two names.
- A name that needs quotes to be one, with a space or a hyphen, is refused.
- A table is a name, or a schema and a name: `orders`, `public.orders`. No more than one dot.

### A relative path

Of a table under the lake, or of a directory under the landing root. Segments separated by `/`; a segment starts with a letter or a digit and goes on with letters, digits, `_`, `-`, `.` and `=`: `bronze/sales/orders`, `raw/dt=2026-11-17`.

- No `/` in front, no scheme, no `..`, no backslash, no `%`: a path never leads out of its root.
- No segment starts with `_` or `.`: those names are hidden to Spark and Delta, and `_delta_log` is the log of a table.
- One way to write it: no `/` at the end, no empty segment.

### A root

Of the lake, or of the landing zone. `file:///absolute/path` or `s3a://bucket/prefix`, with segments as those of a relative path.

- `s3://` and `s3n://` are refused: another scheme for the same bucket would be another key for the same table.
- No user, no password, no query.
- A lake that is reached through a symbolic link is one table under two names to whoever reads only text. The LogStore resolves the link when it makes the key that commits are coordinated under.

### A duration

A whole number and a unit: `500ms`, `30s`, `10m`, `2h`, `7d`.

- The unit is not optional: a bare `30` is refused, since nobody knows what it was meant in.
- Lower case alone, no sign, no fraction, no space, no zeros in front, and ten years at most.

### A moment

A date, a time of day and a zone, as ISO 8601 writes them: `2026-01-01T00:00:00Z`, or `2026-01-01T01:00:00+01:00` for the same moment.

- The zone is not optional: a moment without one would be read in the zone of whichever machine reads it. A date alone is refused for the same reason.
- Seconds always, and at most six digits after them: PostgreSQL keeps microseconds, and rounds the rest.
- From `1900-01-01T00:00:00Z` to `9999-12-31T23:59:59.999999Z`. It is stored in UTC: the two above have one hash.
- In quotes, or without: it is text either way.

### A reference to a secret

Where a secret is, never the secret: `env:NAME` for an environment variable of the process, `file:/absolute/path` for a file, such as a mounted Docker secret.

- A file is read without the line break at its end. A variable that is empty is not set.
- Text where a reference belongs is not read at all, and no error repeats it: it is most likely the secret.

## YAML, as it is read here

A file is one YAML 1.2 document, read by the JSON schema.

| Written | Is |
|---|---|
| `true`, `false` | a boolean |
| `yes`, `no`, `on`, `off`, `True` | text |
| `null`, or nothing after the colon | no value: the default of the field is taken |
| `~`, `Null` | text |
| `"null"` | the text `null`, which is a value of `schema.on_missing_column` |
| `3`, `0.01` | a number |
| `007`, `0x1F`, `2026-11-17`, `${HOME}` | text |
| `.inf`, `.nan`, `1.` | an error: a number that JSON does not have |
| `<<` | a key like any other, not a merge |

What is refused, each with its line and column:

- a document over 256 KiB, nesting beyond 32, more than 8 aliases, an alias without its anchor;
- any tag, `!!str` included;
- a key twice in one mapping, or a key that is not a plain value;
- no document, or more than one.

And a field that the model does not have is an error, with the field that was probably meant: a misspelt `watermark` does not become a full load that nobody asked for.

## Rules about several sources

They are applied to the sources that are valid by themselves.

- **An id belongs to one source**, and a source is in the file of its name.
- **A table has one layer.** What one source writes as its Bronze table, no other writes as its Silver table.
- **No table is inside another.**
- **Two file sources do not read one directory**, whatever their patterns, and disabled sources too: each would ingest its files. A directory inside another source's is allowed, since neither lists the other's.
- **A Silver table that several sources merge into** is partitioned, the same way by all of them, and each of its partition columns is one of the keys of each source or is `_source_id`. That column is the engine's own: it is part of every key without being written in one. A table that one source writes is partitioned as that source likes, and several sources may append to one Bronze table as it is.

## Validating

```bash
sbt "cli/run validate --profile conf/app/local.yaml --sources conf/sources"
```

It reads the profile and every source and says everything that is wrong in them. It opens no connection and looks no name up, so it runs anywhere, in a moment, with every database down.

| Exit code | The configuration |
|---|---|
| 0 | is valid. Its warnings are on the error stream, and its sources on the output, each with the hash it would run with |
| 4 | is not valid. Every error is on a line of its own, as `file: field problem` |
| 64 | was not looked at: the command line is not one |

The whole table of exit codes is in the [discovery](../architecture/00-architecture-discovery.md), §14.1.

Validation has two stages, and each says everything it finds. The rules about several fields need every field to be right, and the rules about several sources need each source to be valid: a mistake of the second kind is said once those of the first are gone.

## The version of a configuration

Every run of a source records the hash of the configuration it ran with: the SHA-256 of the profile and the source, written one way.

- **What is hashed is what ran**, with every default. The order of the keys of a file, its comments, `120s` for `2m`, a default written out: none of them changes the hash.
- **The profile is in it.** The same source in another lake did not run with the same configuration.
- **The content is stored once for each hash**, and it is a configuration: read back, it gives the same profile and the same source. It holds where each secret is, and no secret.

A change to a source that what is already ingested cannot follow is a **breaking change**. It is found by comparing the configuration of the last run with the one of now:

| Changed | What would go wrong if the source simply ran on |
|---|---|
| `lake.uri` | Every table of the source is in another place, and its state says what was written to the old one |
| `targets.bronze.table`, `targets.silver.table` | The table is another, and what was written is in the old one |
| `targets.silver.partition_by`, for the same table | A table is partitioned one way, and it already is |
| `keys`, while Silver is merged into | The rows in Silver were merged by the old key. The same columns in another order are the same key |
| `load.watermark.column` | The watermark that is stored is a value of the old column |
| `connector.table`, the database of `connector.connection.url`, or `connector.type`, for an incremental load | The watermark that is stored is of what was read before: going on from it would skip rows without a word |

- What only says how a database is reached is no breaking change: the role, the reference to the password, TLS, the timeouts.
- Nor is `load.watermark.start`: it starts a source that has no watermark, and after that bounds only what `lookback` reads again.
- A source that gives Silver up, or goes from a watermark to full loads, breaks nothing that was done. A full load reads from wherever it likes.
- **A run of a source with a breaking change is refused**, as a failure that running again does not mend (exit code 4), before anything is planned, read or written. It says each change, and the hash of the configuration that the source last ingested with, whose content is in `control.config_snapshot`: with that configuration the source runs on.
- **A run into Bronze is refused for what Bronze cannot follow**: `lake.uri`, `targets.bronze.table`, `load.watermark.column` and, for an incremental load, where the rows come from. `keys` and the table of Silver change nothing of what was ingested, and do not stop it.
- **What lets a breaking change through is `reset-state`**, an operator's command with a reason ([ADR-0021](../adr/0021-operator-actions-and-backfills.md) §3): the watermark of the source is set (`--watermark`), forgotten (`--forget-watermark`) or kept (`--keep-watermark`), and the configuration that the command is given is the one that later runs are compared with. The refusal of a run names the command. A watermark kept across another Bronze table or another watermark column is warned of, each with its risk: the history stays in the old table until a backfill or a reprocess reads it into the new one.
