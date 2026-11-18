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
| `retries.budget` | For how long the engine goes on with a step that fails with an error that passes. A [duration](#a-duration), `1s` to `1d` | `2m` |
| `retries.first_backoff` | The wait before the second attempt. `100ms` to `1h` | `1s` |
| `retries.max_backoff` | The longest wait between two attempts. `100ms` to `1h` | `30s` |
| `spark.master` | `local`, `local[N]`, `local[*]`, or `spark://host:port` for a Spark Standalone cluster | required |
| `spark.settings` | Spark settings to add, by their key. A value is text, a number or a boolean | none |

Rules about several fields of a profile:

- **A lake on a local disk is for one machine.** `lake.uri` on `file://` with `deployment: cluster` is an error, unless `shared_filesystem` is `true`. And `shared_filesystem: true` is an error anywhere else, where it says nothing.
- **A cluster of Spark needs storage that its workers share.** `spark.master: spark://…` with a single-machine lake on a local disk is an error: every worker would write to its own disk.
- **The landing zone and the lake are apart.** Neither is the other, nor inside it: files that land are never read as tables.
- **The two databases have a role each.** `databases.control.user` and `databases.coordination.user` are two roles, each with the privileges of its own work.
- **The backoffs fit**: `first_backoff` ≤ `max_backoff` ≤ `budget`.
- **The retry budget is longer than the LogStore itself waits.** `retries.budget` must be longer than three times `databases.coordination.timeouts.connect`: 15 s with the defaults. A commit without the coordination database takes that long to fail, and a shorter budget would give up on outages that would have passed.
- **Spark settings name no LogStore, no master and no credential.** A key must start with `spark.`. `spark.master` is the field above, `spark.delta.logStore.*` is the engine's, and a key with `password`, `secret`, `token` or `access.key` in it is refused: object storage takes its credentials from the environment or from a Hadoop credential provider.

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
| `timeouts.statement` | How long one statement may run. `1s` to `1h` | `10s` |

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
| `load.watermark.column` | The column that only grows, as `updated_at`: a [name](#a-name). Required for `incremental`, and an error for any other mode | — |
| `load.watermark.lookback` | How far before the last watermark a load starts again, for rows that arrived late. A [duration](#a-duration) | `0s` |
| `load.watermark.safety_margin` | How far behind the present a load stops, for transactions that are still open | `0s` |
| `load.max_batch_range` | The widest stretch of the watermark that one batch takes. `incremental` alone | no limit |
| `keys` | The columns that tell one row from another: what Silver merges by. A list of [names](#a-name), each once | none |
| `deletes.strategy` | How rows that the source deleted are found, when the load does not find them: `key_reconciliation` or `deleted_flag` | deletes are those the load sees: a full load sees them all |
| `deletes.column` | The column in which the source marks a deleted row. Required for `deleted_flag`, and an error otherwise | — |
| `schema.on_new_column` | `add`, `rescue` or `fail` | `add` |
| `schema.on_missing_column` | `"null"`, in quotes, or `fail` | `"null"` |
| `schema.on_type_change` | `widen` or `fail` | `fail` |
| `schema.renames` | Columns that changed their name, old to new. A rename cannot be told from a column that went and another that came, unless somebody says so | none |
| `columns.<name>.pii` | What kind of personal data the column holds: a word of the team's own, as `email` or `national_id` | none |
| `columns.<name>.silver.mask` | How Silver hides the column: `hmac` or `redact` | not masked |
| `quality.max_rejected_ratio` | The share of the rows of a batch that may be rejected before the batch is not committed. `0` to `1` | `0` |
| `targets.bronze.table` | The Bronze table: a [path](#a-relative-path) relative to `lake.uri` | required |
| `targets.silver.table` | The Silver table. Without it, the source goes to Bronze alone | no Silver |
| `targets.silver.partition_by` | The columns the Silver table is partitioned by, in their order | not partitioned |
| `freshness_sla` | How old the data of the source may get before it is late. A [duration](#a-duration), `1s` at least | none |

What each value of the schema policy and of the masks does is decided where it is built (discovery §17, P9 and P10).

### The `jdbc` connector

A table of a relational database. PostgreSQL, for now. It loads by `full` or by `incremental`.

<!-- fields: connector jdbc -->
| Field | Takes | Left out |
|---|---|---|
| `connector.connection` | The [connection](#a-connection-to-a-database) to the database | required |
| `connector.table` | The table: a [name](#a-name), or `schema.name` | required |

### The `file` connector

Files that are left in a directory of the landing zone. It loads by `new_files`.

<!-- fields: connector file -->
| Field | Takes | Left out |
|---|---|---|
| `connector.path` | The directory: a [path](#a-relative-path) relative to `landing.root` | required |
| `connector.format` | `csv`, `json` or `parquet` | required |
| `connector.pattern` | Which files of the directory, by their name: letters, digits, `_`, `-`, `.`, and the wildcards `*` and `?`. As `returns-*.csv` | every file |

Rules about several fields of a source:

- **A kind of connector loads in its own ways.** `jdbc` by `full` or `incremental`, `file` by `new_files`.
- **Silver needs keys**: with `targets.silver`, `keys` is not empty.
- **Each layer has a table of its own.** The Silver table is not the Bronze table, nor inside it, nor around it.
- **`key_reconciliation` is for incremental loads**, and `deletes` needs `targets.silver`: deletes are marked in Silver.
- **A mask needs `targets.silver`** to be applied in.

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
- A source that gives Silver up, or goes from a watermark to full loads, breaks nothing that was done. A full load reads from wherever it likes.
- What lets a breaking change through, a reset of the state or a backfill, comes with the control plane and with recovery (discovery §17, P4 and P7).
