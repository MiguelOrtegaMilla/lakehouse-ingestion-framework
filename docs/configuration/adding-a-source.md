# Adding a source

Adding a source is writing one file. Nothing is compiled and no job is written: the engine reads the file and does what it says. Code is needed only for a source of a kind that has no connector yet ([the last section](#a-source-of-a-new-kind)).

Every field, with what it takes and what it is when it is left out, is in the [reference](reference.md). This is the way through it.

> **Where the project is.** A source can be written and validated today. What runs it, the engine, is being built (discovery §17, from P4 on). Everything below about files and their validation is as it works now.

## 1. Write the file

A source is `conf/sources/<id>.yaml`, where `<id>` is its name: lower-case words joined by dots, as `shop.returns`. The file is named by the id, so a source is found where its name says.

Start from one of the two examples: [`sales.orders.yaml`](../../conf/sources/sales.orders.yaml), a table of a database with every section a source can have, or [`sales.returns.yaml`](../../conf/sources/sales.returns.yaml), CSV files with the least a source must say.

Files that are left in a directory:

<!-- valid: source -->
```yaml
id: shop.returns
connector:
  type: file
  path: shop/returns          # under the landing root of the profile
  format: csv
load: { mode: new_files }
targets:
  bronze: { table: bronze/shop/returns }   # under the lake of the profile
```

A table of a database, loaded by what changed, and merged into Silver:

<!-- valid: source -->
```yaml
id: crm.customers
connector:
  type: jdbc
  connection:
    url: "jdbc:postgresql://crm-db.internal:5432/crm"
    user: ingest_ro
    password: { secretRef: "env:CRM_DB_PASSWORD" }
    tls: { mode: verify-full, root_certificate: /etc/ssl/certs/company-ca.pem }
  table: public.customers
load:
  mode: incremental
  watermark: { column: updated_at, lookback: 10m, safety_margin: 1m }
keys: [customer_id]
targets:
  bronze: { table: bronze/crm/customers }
  silver: { table: silver/crm/customers }
```

Paths are relative: a table to the lake of the profile, a directory of files to its landing root. The same source file is used in every profile, on a laptop and in production.

## 2. Decide what a source has to decide

| Question | Field | What to know |
|---|---|---|
| How is it loaded? | `load.mode` | `full` takes everything every time. `incremental` takes what changed, by a column that only grows (`load.watermark.column`). `new_files` takes the files not taken yet. A table is loaded by one of the first two, files by the third |
| What tells one row from another? | `keys` | Silver merges by it, so a source with `targets.silver` needs it. Changing it later is a breaking change |
| What if rows are deleted in the source? | `deletes` | A full load sees what is gone. An incremental load does not: `key_reconciliation` compares the keys in a run of its own, and `deleted_flag` reads the column in which the source marks a deleted row |
| What if the source changes its schema? | `schema` | By default a new column is taken, a missing one is null, and a change of type fails the run |
| Which columns hold personal data? | `columns` | Tag them (`pii: email`), and say how Silver hides them (`silver: { mask: hmac }`) |
| How many bad rows are too many? | `quality.max_rejected_ratio` | Rows that cannot be read go to quarantine. Above this share, the batch is not committed. By default no row may be rejected |
| How fresh must it be? | `freshness_sla` | How old its data may get before it is late |

## 3. Point at the secrets, never write them

A password is written as where it is:

```yaml
password: { secretRef: "env:CRM_DB_PASSWORD" }            # an environment variable
password: { secretRef: "file:/run/secrets/crm_password" } # a file, as a mounted Docker secret
```

A password written out is refused wherever a file could have one, a URL and a Spark setting included, and no error message repeats it. So a file of configuration can be read, reviewed and stored by anybody who may see the repository.

## 4. Validate

```bash
sbt "cli/run validate --profile conf/app/local.yaml --sources conf/sources"
```

It needs a profile: copy `conf/app/local.example.yaml` to `conf/app/local.yaml`, which git ignores, and put the directories of your machine in it.

A configuration that is valid, here the examples of the repository with the example profile:

```text
The configuration is valid: conf/app/local.example.yaml, and 2 sources.
The lake is file:///home/CHANGE_ME/lakehouse/lake.
  sales.orders   enabled  jdbc  incremental  bronze/sales/orders, silver/sales/orders  78dd136c673a
  sales.returns  enabled  file  new_files    bronze/sales/returns                      0e31fa35b306
```

The last column is the start of the hash of the configuration that each source would run with.

One that is not. This source has a parameter in its URL, a password written out, and a field misspelt:

```text
conf/sources/crm.customers.yaml: connector.connection.url has parameters: a URL is jdbc:postgresql://host:port/database and nothing more. TLS and timeouts are fields of their own, and a password is a reference
conf/sources/crm.customers.yaml: connector.connection.password must be a reference to a secret, as { secretRef: "env:NAME" } or { secretRef: "file:/run/secrets/name" }, and never the secret itself
conf/sources/crm.customers.yaml: load.watermark is missing: an incremental load goes by a column that only grows
conf/sources/crm.customers.yaml: load.watermak is not a field that is known here: did you mean 'watermark'?
4 errors in 1 file. The configuration is not valid.
```

- **Every mistake is said at once**, each with its file and its field. There is no fixing one to find the next.
- **Nothing is connected to.** The command reads files. It says the same with every database down, and it says it in a moment.
- **The exit code is 0 or 4**: valid, or not. 64 is for a command line that is wrong.

Two things are worth knowing when an error is a surprise:

- **A field that the model does not have is an error**, not something that is ignored. A misspelt `watermark` must not become a full load that nobody asked for.
- **YAML is read strictly** ([the reference](reference.md#yaml-as-it-is-read-here)): `yes` is text and not a boolean, `null` without quotes is no value at all, a duration needs its unit (`30s`, not `30`), and a key may not be written twice.

## 5. Open a pull request

The tests of the build validate `conf/sources` with the example profile, so a source that is not valid does not reach `main`. A source is reviewed as what it is: a small file that says what is read, how, and where it goes.

## 6. Know what cannot be changed later without saying so

Some changes to a source cannot be followed by what is already ingested. The application finds them by comparing the configuration of the last run with the one of now ([the reference](reference.md#the-version-of-a-configuration) has the list):

- another key, while Silver is merged into;
- another watermark column;
- another table in Bronze or in Silver, or another partitioning of the same Silver table;
- for an incremental load, another table or another database to read from: the watermark that is stored is of the one read before, and going on from it would skip rows without a word.

Anything else can change freely: the owner, the lookback, the schema policy, the password and how the database is reached.

## Several sources into one table

Several sources may go to one table: the customers of two countries, each from a database of its own.

- **Into Bronze**, they just do. Appends do not read, so they do not get in each other's way.
- **Into Silver**, the table is partitioned by the column that separates the sources, the same in all of them. `_source_id` is that column when the sources have none of their own: the engine adds it to every row.

```yaml
# conf/sources/crm_es.customers.yaml, and the same in crm_pt.customers.yaml
keys: [customer_id]
targets:
  bronze: { table: bronze/crm_es/customers }
  silver: { table: silver/crm/customers, partition_by: [_source_id] }
```

A partition column of the sources' own, as `region`, must be one of the keys of each: a row is found by its key within its partition.

## A source of a new kind

A source of a kind that the framework has no connector for needs one. On the side of configuration that is a `ConnectorKind`: the name it has in `connector.type`, the ways of loading it has, and how the fields of its section are read. It reads them with the same reader as the rest of the file, so its mistakes are said as any other, with their field and with the field that was probably meant.

```scala
object RestConnector extends ConnectorKind {
  override val name: String = "rest"
  override val loads: Set[Load.Mode] = Set(Load.Mode.Full)

  override def read(section: Read.Obj): Read.Result[ConnectorConfig] =
    (
      section.required("endpoint")(_.string),
      section.required("token")(_.secret)
    ).mapN(RestConfig.apply)
}
```

Its configuration says how it is written down to be stored and hashed, and which of its fields are where the rows come from, for the breaking changes (`ConnectorConfig.canonical`, `ConnectorConfig.origin`). The model of a source names no kind, and neither will the engine: the kinds are given to it where the application is put together. The connector itself, what reads the rows, is the engine's (discovery §12.2).
