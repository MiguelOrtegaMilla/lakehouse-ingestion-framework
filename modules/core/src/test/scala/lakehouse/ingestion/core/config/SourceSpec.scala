package lakehouse.ingestion.core.config

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}

import scala.concurrent.duration._

import cats.syntax.all._
import lakehouse.ingestion.core.config.Read.{Obj, Result}
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * A source: its fields, its defaults, the rules about more than one field, and that everything
 * wrong in a file is said at once (ADR-0007, discovery §13.1).
 */
final class SourceSpec extends AnyFunSuite with Matchers with EitherValues {

  private val File = "conf/sources/test.yaml"

  private val repository: Path =
    Iterator
      .iterate(Paths.get("").toAbsolutePath)(_.getParent)
      .takeWhile(_ != null)
      .find(dir => Files.isDirectory(dir.resolve("conf/sources")))
      .getOrElse(fail("conf/sources was not found above the working directory"))

  private def example(name: String): Loaded[Source] = {
    val file = s"conf/sources/$name"
    val loaded = Source.load(file, new String(Files.readAllBytes(repository.resolve(file)), UTF_8))
    loaded.left.map(_.toChain.toVector.map(_.message)) shouldBe a[Right[_, _]]
    loaded.value
  }

  /** The least a JDBC source can say. */
  private val Jdbc =
    """id: sales.orders
      |connector:
      |  type: jdbc
      |  connection:
      |    url: "jdbc:postgresql://localhost:5432/sales"
      |    user: sales_ro
      |    password: { secretRef: "env:SALES_RO_PASSWORD" }
      |    tls: { mode: disable }
      |  table: public.orders
      |load: { mode: full }
      |targets:
      |  bronze: { table: bronze/sales/orders }
      |""".stripMargin

  private val Incremental = Jdbc
    .replace(
      "load: { mode: full }",
      "load: { mode: incremental, watermark: { column: updated_at } }"
    )

  private val Merged = Incremental +
    "  silver: { table: silver/sales/orders }\nkeys: [order_id]\n"

  private def errorsOf(text: String): Seq[String] =
    Source.load(File, text).left.value.toChain.toVector.map(_.message)

  private def id(name: String): SqlIdentifier = SqlIdentifier.parse(name).value

  test("the example of a JDBC source is valid, and is read as it says") {
    val Loaded(source, warnings) = example("sales.orders.yaml")

    warnings shouldBe empty
    source.id.value shouldBe "sales.orders"
    source.enabled shouldBe true
    source.owner shouldBe Some("team-sales")
    val connector = source.connector.asInstanceOf[JdbcConnectorConfig]
    connector.kind shouldBe "jdbc"
    connector.table.toString shouldBe "public.orders"
    connector.connection.user shouldBe id("sales_ro")
    connector.connection.password shouldBe SecretRef.Env("SALES_RO_PASSWORD")
    source.load shouldBe
      Load.Incremental(Load.Watermark(id("updated_at"), 10.minutes, 1.minute), Some(7.days))
    source.keys shouldBe Vector(id("order_id"))
    source.deletes shouldBe Some(Deletes.KeyReconciliation)
    source.schema shouldBe SchemaPolicy.Default
    source.columns.toMap shouldBe
      Map("customer_email" -> ColumnSpec(Some("email"), Some(ColumnSpec.Mask.Hmac)))
    source.quality shouldBe Quality(0.01)
    source.targets.bronze.table.value shouldBe "bronze/sales/orders"
    source.targets.silver.map(_.table.value) shouldBe Some("silver/sales/orders")
    source.targets.silver.map(_.partitionBy) shouldBe Some(Vector(id("_source_id")))
    source.freshnessSla shouldBe Some(2.hours)
  }

  test("the example of a file source is valid, with the defaults for what it leaves out") {
    val Loaded(source, warnings) = example("sales.returns.yaml")

    warnings shouldBe empty
    val connector = source.connector.asInstanceOf[FileConnectorConfig]
    connector.path.value shouldBe "sales/returns"
    connector.format shouldBe FileConnectorConfig.Format.Csv
    connector.pattern.map(_.value) shouldBe Some("returns-*.csv")
    connector.minFileAge shouldBe 1.minute
    connector.maxFilesPerBatch shouldBe 1000
    source.load shouldBe Load.NewFiles
    source.enabled shouldBe true
    source.keys shouldBe empty
    source.deletes shouldBe None
    source.schema shouldBe SchemaPolicy.Default
    source.columns shouldBe empty
    source.quality shouldBe Quality(0.0)
    source.targets.silver shouldBe None
    source.freshnessSla shouldBe None
  }

  test("everything wrong in a file is said at once, each with its field") {
    val text =
      """id: Sales.Orders
        |enabled: yes
        |connector:
        |  type: jdbc
        |  connection:
        |    url: "jdbc:postgresql://localhost:5432/sales?password=hunter2"
        |    user: sales_ro
        |    password: hunter2
        |    tls: { mode: disable }
        |  table: "public.orders; DROP TABLE orders"
        |  fetch_size: 1000
        |load:
        |  mode: incremental
        |  watermak: { column: updated_at }
        |keys: [order_id, "order id", order_id]
        |deletes: { strategy: cdc }
        |schema: { on_new_column: ignore, on_missing_column: null, on_type_change: widen }
        |columns:
        |  customer_email: { pii: Email Address, silver: { mask: sha1 } }
        |  "customer name": { pii: name }
        |quality: { max_rejected_ratio: 5 }
        |targets:
        |  bronze: { table: /lake/bronze/orders }
        |  silver: { table: silver/../../orders, partition_by: source }
        |freshness_sla: 2
        |""".stripMargin

    errorsOf(text) shouldBe Seq(
      s"$File: id must be words of lower-case letters, digits and underscores, each starting with a letter and joined by dots, as sales.orders",
      s"$File: enabled must be true or false, and is text",
      s"$File: connector.connection.url has parameters: a URL is jdbc:postgresql://host:port/database and nothing more. TLS and timeouts are fields of their own, and a password is a reference",
      s"""$File: connector.connection.password must be a reference to a secret, as { secretRef: "env:NAME" } or { secretRef: "file:/run/secrets/name" }, and never the secret itself""",
      s"$File: connector.table has a table name that has ';' (U+003B) at position 7: a name is letters of ASCII, digits and underscores",
      s"$File: connector.fetch_size is not a field that is known here",
      s"$File: load.watermark is missing: an incremental load goes by a column that only grows",
      s"$File: load.watermak is not a field that is known here: did you mean 'watermark'?",
      s"$File: keys[1] has U+0020 SPACE at position 6: a name is letters of ASCII, digits and underscores",
      s"$File: deletes.strategy must be one of key_reconciliation, deleted_flag",
      s"$File: schema.on_new_column must be one of add, rescue, fail",
      s"$File: columns.customer_email.pii must be a word of lower-case letters, digits and underscores, as email or national_id",
      s"$File: columns.customer_email.silver.mask must be one of hmac, redact",
      s"$File: columns.customer\\u0020name is under a name that has U+0020 SPACE at position 9: a name is letters of ASCII, digits and underscores",
      s"$File: quality.max_rejected_ratio must be between 0 and 1",
      s"$File: targets.bronze.table is absolute: it must be relative to its root",
      s"$File: targets.silver.table has the segment '..': a path does not climb out of its root",
      s"$File: targets.silver.partition_by must be a list, and is text",
      s"$File: freshness_sla has no unit: write it as 500ms, 30s, 10m, 2h or 7d"
    )
    errorsOf(text).foreach(_ should not include "hunter2")
  }

  test("an incremental load has a watermark, and no other load has one") {
    errorsOf(Jdbc.replace("mode: full", "mode: incremental")) shouldBe
      Seq(s"$File: load.watermark is missing: an incremental load goes by a column that only grows")
    errorsOf(
      Jdbc.replace(
        "mode: full",
        "mode: full, watermark: { column: updated_at }, max_batch_range: 1d"
      )
    ) shouldBe Seq(
      s"$File: load.watermark is for mode incremental alone, and mode is full",
      s"$File: load.max_batch_range is for mode incremental alone, and mode is full"
    )

    val watermark = Source.load(File, Incremental).value.value.load
    watermark shouldBe Load.Incremental(
      Load.Watermark(id("updated_at"), Duration.Zero, Duration.Zero),
      None
    )
  }

  test("a kind of connector loads in its own ways") {
    errorsOf(Jdbc.replace("mode: full", "mode: new_files")) shouldBe
      Seq(
        s"$File: load.mode is new_files, and a connector of type jdbc loads by full or incremental"
      )

    val files =
      """id: sales.returns
        |connector: { type: file, path: sales/returns, format: csv }
        |load: { mode: full }
        |targets: { bronze: { table: bronze/sales/returns } }
        |""".stripMargin
    errorsOf(files) shouldBe
      Seq(s"$File: load.mode is full, and a connector of type file loads by new_files")
    Source.load(File, files.replace("mode: full", "mode: new_files")).isRight shouldBe true
  }

  test("a connector of a type that nobody registered is refused, with those that are") {
    errorsOf(Jdbc.replace("type: jdbc", "type: jdbs")) shouldBe
      Seq(s"$File: connector.type must be one of jdbc, file: did you mean 'jdbc'?")
    errorsOf(Jdbc.replace("  type: jdbc\n", "")) shouldBe Seq(s"$File: connector.type is missing")
  }

  test("a new kind of connector reads its own section, and the model of a source does not change") {
    final case class RestConfig(endpoint: String, pageSize: Int) extends ConnectorConfig {
      override def kind: String = "rest"
      override def canonical: io.circe.Json = io.circe.Json
        .obj(
          "endpoint" -> io.circe.Json.fromString(endpoint),
          "page_size" -> io.circe.Json.fromInt(pageSize)
        )
      override def origin: Seq[String] = Seq("endpoint")
    }
    object Rest extends ConnectorKind {
      override val name: String = "rest"
      override val loads: Set[Load.Mode] = Set(Load.Mode.Full)
      override def read(section: Obj): Result[ConnectorConfig] =
        (
          section.required("endpoint")(_.string),
          section.orElse("page_size", "100")(_.scalarText).map(_.toInt)
        ).mapN(RestConfig.apply)
    }
    val text =
      """id: crm.contacts
        |connector: { type: rest, endpoint: "https://crm.example/contacts", page_size: 50 }
        |load: { mode: full }
        |targets: { bronze: { table: bronze/crm/contacts } }
        |""".stripMargin

    val source = Source.load(File, text, ConnectorKind.BuiltIn :+ Rest).value.value
    source.connector shouldBe RestConfig("https://crm.example/contacts", 50)

    // Its fields are its own: a mistake in them is found as any other.
    Source
      .load(File, text.replace("page_size", "pagesize"), ConnectorKind.BuiltIn :+ Rest)
      .left
      .value
      .toChain
      .toVector
      .map(_.message) shouldBe Seq(
      s"$File: connector.pagesize is not a field that is known here: did you mean 'page_size'?"
    )
    // And without the kind, the type is one that nobody knows.
    errorsOf(text) shouldBe Seq(s"$File: connector.type must be one of jdbc, file")
  }

  test("Silver needs keys, a table of its own, and each key once") {
    errorsOf(Incremental + "  silver: { table: silver/sales/orders }\n") shouldBe Seq(
      s"$File: keys is empty, and targets.silver needs the columns that tell one row from another to merge by"
    )
    errorsOf(Merged.replace("silver/sales/orders", "bronze/sales/orders")).head should
      startWith(s"$File: targets.silver.table is the Bronze table, or inside it, or around it")
    errorsOf(Merged.replace("silver/sales/orders", "bronze/sales/orders/silver")) should have size 1
    errorsOf(Merged.replace("silver/sales/orders", "bronze/sales")) should have size 1
    Source
      .load(File, Merged.replace("silver/sales/orders", "bronze/sales/orders2"))
      .isRight shouldBe true

    errorsOf(Merged.replace("keys: [order_id]", "keys: [order_id, line, order_id]")) shouldBe
      Seq(s"$File: keys[2] is order_id again: it is keys[0] already")
    errorsOf(
      Merged.replace("silver/sales/orders }", "silver/sales/orders, partition_by: [day, day] }")
    ) shouldBe
      Seq(
        s"$File: targets.silver.partition_by[1] is day again: it is targets.silver.partition_by[0] already"
      )
  }

  test("how deletes are found goes with the load and with Silver") {
    val reconciled = Merged + "deletes: { strategy: key_reconciliation }\n"
    Source.load(File, reconciled).value.value.deletes shouldBe Some(Deletes.KeyReconciliation)

    val flagged = Merged + "deletes: { strategy: deleted_flag, column: is_deleted }\n"
    Source.load(File, flagged).value.value.deletes shouldBe Some(
      Deletes.DeletedFlag(id("is_deleted"))
    )

    errorsOf(Merged + "deletes: { strategy: deleted_flag }\n") shouldBe
      Seq(
        s"$File: deletes.column is missing: deleted_flag needs the column that marks a deleted row"
      )
    errorsOf(Merged + "deletes: { strategy: key_reconciliation, column: is_deleted }\n") shouldBe
      Seq(s"$File: deletes.column is for the strategy deleted_flag alone")
    // A full load sees what is gone by itself.
    val full = Jdbc + "  silver: { table: silver/sales/orders }\nkeys: [order_id]\n"
    errorsOf(full + "deletes: { strategy: key_reconciliation }\n") shouldBe Seq(
      s"$File: deletes.strategy is key_reconciliation, which is for incremental loads, and load.mode is full"
    )
    errorsOf(Incremental + "deletes: { strategy: deleted_flag, column: is_deleted }\n") shouldBe
      Seq(
        s"$File: deletes is set, and there is no targets.silver: deletes are marked in Silver (ADR-0009)"
      )
  }

  test("the policy for a schema that changed has defaults, and null must be quoted to be one") {
    def policy(text: String): SchemaPolicy = Source.load(File, Jdbc + text).value.value.schema
    import SchemaPolicy._

    policy("") shouldBe SchemaPolicy.Default
    policy(
      "schema: { on_new_column: rescue, on_missing_column: fail, on_type_change: widen }\n"
    ) shouldBe
      SchemaPolicy(
        OnNewColumn.Rescue,
        OnMissingColumn.Fail,
        OnTypeChange.Widen,
        scala.collection.immutable.SortedMap.empty
      )
    policy(
      "schema: { on_missing_column: \"null\" }\n"
    ).onMissingColumn shouldBe OnMissingColumn.Null
    // Without quotes it is no value: the default is taken, which is the same here.
    policy("schema: { on_missing_column: null }\n").onMissingColumn shouldBe OnMissingColumn.Null

    policy("schema: { renames: { cust_mail: customer_email } }\n").renames.toMap shouldBe
      Map("cust_mail" -> id("customer_email"))
    errorsOf(Jdbc + "schema: { renames: { \"cust mail\": \"customer-email\" } }\n") shouldBe Seq(
      s"$File: schema.renames.cust\\u0020mail is under a name that has U+0020 SPACE at position 5: a name is letters of ASCII, digits and underscores",
      s"$File: schema.renames.cust\\u0020mail has '-' (U+002D) at position 9: a name is letters of ASCII, digits and underscores"
    )
  }

  test("a mask needs Silver to be applied in") {
    errorsOf(Jdbc + "columns:\n  email: { pii: email, silver: { mask: hmac } }\n") shouldBe
      Seq(s"$File: columns.email.silver.mask is set, and there is no targets.silver to mask it in")
    // Every column that is masked is said, not the first alone.
    errorsOf(
      Jdbc + "columns:\n  phone: { silver: { mask: redact } }\n  email: { pii: email, silver: { mask: hmac } }\n"
    ) shouldBe Seq(
      s"$File: columns.email.silver.mask is set, and there is no targets.silver to mask it in",
      s"$File: columns.phone.silver.mask is set, and there is no targets.silver to mask it in"
    )
    // Tagging a column is not masking it: Bronze alone may say what it holds.
    Source
      .load(File, Jdbc + "columns:\n  email: { pii: email }\n")
      .value
      .value
      .columns
      .toMap shouldBe
      Map("email" -> ColumnSpec(Some("email"), None))
  }

  test("a connection of a source to another machine without TLS is warned of") {
    Source
      .load(File, Jdbc.replace("localhost", "db.sales.internal"))
      .value
      .warnings
      .map(_.message) shouldBe Seq(
      s"$File: connector.connection.tls.mode is disable, and the database is on another " +
        "machine: its traffic, the password included, is not encrypted"
    )
  }

  test("the id of a source is lower-case words joined by dots") {
    Seq("sales.orders", "orders", "crm.contacts_v2", "a.b.c", "s1.t2").foreach { text =>
      withClue(text)(SourceId.parse(text).value.value shouldBe text)
    }
    Seq(
      "",
      "Sales.Orders",
      "sales..orders",
      ".orders",
      "sales.",
      "sales orders",
      "sales/orders",
      "../orders",
      "sales.orders;rm",
      "1sales",
      "sales.1orders",
      "sales-orders",
      "sales.оrders",
      "a" * 101
    ).foreach(text => withClue(s"[$text]")(SourceId.parse(text).isLeft shouldBe true))
  }

  test("a pattern is for the name of a file, and never leads out of its directory") {
    Seq("*.csv", "returns-*.csv", "part-????.json", "data.parquet", "*").foreach { text =>
      withClue(text)(FileConnectorConfig.Pattern.parse(text).value.value shouldBe text)
    }
    Seq(
      "",
      "../*.csv",
      "sub/*.csv",
      "*.csv;rm",
      "{a,b}.csv",
      "[a-z].csv",
      "*.c sv",
      ".hidden*",
      "_SUCCESS",
      "*\u0000"
    )
      .foreach(text =>
        withClue(s"[$text]")(FileConnectorConfig.Pattern.parse(text).isLeft shouldBe true)
      )
  }

  /** The least a file source can say. */
  private val Files_ =
    """id: sales.returns
      |connector: { type: file, path: sales/returns, format: csv }
      |load: { mode: new_files }
      |targets:
      |  bronze: { table: bronze/sales/returns }
      |""".stripMargin

  private def fileConnector(fields: String): Loaded[FileConnectorConfig] = {
    val loaded =
      Source.load(File, Files_.replace("format: csv }", s"format: csv, $fields }")).value
    Loaded(loaded.value.connector.asInstanceOf[FileConnectorConfig], loaded.warnings)
  }

  private def fileErrors(fields: String): Seq[String] =
    errorsOf(Files_.replace("format: csv }", s"format: csv, $fields }"))

  test("how old a file must be to be taken, and how many one batch takes, have bounds") {
    fileConnector("min_file_age: 5m, max_files_per_batch: 50").value shouldBe
      FileConnectorConfig(
        RelativePath.parse("sales/returns").value,
        FileConnectorConfig.Format.Csv,
        None,
        minFileAge = 5.minutes,
        maxFilesPerBatch = 50
      )
    fileConnector("min_file_age: 1d, max_files_per_batch: 10000").value.maxFilesPerBatch shouldBe
      10000

    fileErrors("min_file_age: 2d") shouldBe
      Seq(s"$File: connector.min_file_age is 2d, and the longest it may be is 1d")
    fileErrors("min_file_age: 60") shouldBe
      Seq(s"$File: connector.min_file_age has no unit: write it as 500ms, 30s, 10m, 2h or 7d")
    Seq("0", "10001", "-1", "2.5").foreach { count =>
      withClue(count)(
        fileErrors(s"max_files_per_batch: $count") shouldBe
          Seq(s"$File: connector.max_files_per_batch must be a whole number from 1 to 10000")
      )
    }
    fileErrors("max_files_per_batch: \"50\"") shouldBe Seq(
      s"$File: connector.max_files_per_batch must be a whole number from 1 to 10000, and is text"
    )
  }

  test("a file that may be taken the moment it appears is warned of") {
    val Loaded(connector, warnings) = fileConnector("min_file_age: 0s")
    connector.minFileAge shouldBe Duration.Zero
    warnings.map(_.message) shouldBe Seq(
      s"$File: connector.min_file_age is 0s: a file that is still being written may be taken, " +
        "and ingested cut short"
    )
  }

  test("how soon a file is taken, and how many at once, are not where the rows come from") {
    val connector = fileConnector("min_file_age: 5m").value
    connector.origin shouldBe Seq("path", "pattern")
    // A file source has its directory to itself, and a JDBC source shares its table.
    connector.exclusive shouldBe Some("path" -> "sales/returns")
    Source.load(File, Jdbc).value.value.connector.exclusive shouldBe None
  }

  test("a source never names a column of the engine where a column of its own goes") {
    val engine = "is _run_id, which is _run_id: a column that the engine adds to every row, " +
      "and not one of the source. A source that has a column of that name renames it " +
      "(schema.renames)"
    errorsOf(Merged.replace("keys: [order_id]", "keys: [order_id, _run_id]")) shouldBe
      Seq(s"$File: keys[1] $engine")
    errorsOf(Merged.replace("column: updated_at", "column: _run_id")) shouldBe
      Seq(s"$File: load.watermark.column $engine")
    errorsOf(Merged + "deletes: { strategy: deleted_flag, column: _run_id }\n") shouldBe
      Seq(s"$File: deletes.column $engine")
    errorsOf(Merged + "columns: { _run_id: { pii: email } }\n") shouldBe
      Seq(s"$File: columns._run_id $engine")
    errorsOf(Merged + "schema: { renames: { run_id: _run_id } }\n") shouldBe
      Seq(s"$File: schema.renames.run_id $engine")
    errorsOf(
      Merged.replace("silver/sales/orders }", "silver/sales/orders, partition_by: [_run_id] }")
    ) shouldBe Seq(s"$File: targets.silver.partition_by[0] $engine")
  }

  test("every column of the engine is reserved, case aside, and no other name is") {
    EngineColumns.All shouldBe Seq(
      "_run_id",
      "_batch_seq",
      "_ingested_at",
      "_source_file",
      "_source_id",
      "_rescued_data"
    )
    EngineColumns.All.foreach { column =>
      withClue(column)(
        errorsOf(Merged.replace("keys: [order_id]", s"keys: [${column.toUpperCase}]")) shouldBe
          Seq(
            s"$File: keys[0] is ${column.toUpperCase}, which is $column, case aside: a column " +
              "that the engine adds to every row, and not one of the source. A source that has " +
              "a column of that name renames it (schema.renames)"
          )
      )
    }
    // A name that starts with an underscore is a source's like any other.
    Seq("_id", "_run", "run_id", "_source_ids", "__run_id").foreach { name =>
      withClue(name)(
        Source.load(File, Merged.replace("keys: [order_id]", s"keys: [$name]")).isRight shouldBe
          true
      )
    }
  }

  test("a column of the engine may be renamed away, and Silver is partitioned by _source_id") {
    // The source has a column _run_id of its own: the way out is to rename it.
    val renamed =
      Source.load(File, Merged + "schema: { renames: { _run_id: source_run_id } }\n").value.value
    renamed.schema.renames shouldBe Map("_run_id" -> id("source_run_id"))

    val byWriter = Merged.replace(
      "silver/sales/orders }",
      "silver/sales/orders, partition_by: [_source_id] }"
    )
    Source.load(File, byWriter).value.value.targets.silver.map(_.partitionBy) shouldBe
      Some(Vector(id("_source_id")))
    errorsOf(byWriter.replace("[_source_id]", "[_Source_Id]")) shouldBe Seq(
      s"$File: targets.silver.partition_by[0] is _Source_Id, which is the engine's _source_id: " +
        "it is written as the engine writes it"
    )
    // Not a key: it is part of every key without being written in one.
    errorsOf(Merged.replace("keys: [order_id]", "keys: [order_id, _source_id]")) shouldBe Seq(
      s"$File: keys[1] is _source_id, which is _source_id: a column that the engine adds to " +
        "every row, and not one of the source. A source that has a column of that name " +
        "renames it (schema.renames)"
    )
  }
}
