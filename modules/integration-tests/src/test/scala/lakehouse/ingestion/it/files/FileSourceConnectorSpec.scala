package lakehouse.ingestion.it.files

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Path, StandardCopyOption}
import java.time.Instant
import java.time.temporal.ChronoUnit

import io.circe.Json
import lakehouse.ingestion.core.config._
import lakehouse.ingestion.core.control.FailureClass
import lakehouse.ingestion.core.files.{FilePlan, LandedFile}
import lakehouse.ingestion.engine.RunFailedException
import lakehouse.ingestion.engine.connector._
import lakehouse.ingestion.engine.files.{FileSourceConnector, Landing}
import lakehouse.ingestion.it.support.LocalSparkSuite
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{EitherValues, OptionValues}

/**
 * The file connector on real files and a real Spark (P5.7): what it plans, how it reads a plan
 * of CSV, JSON and Parquet, the records it sets apart, and that it reads the files that were
 * planned or fails before anything is written.
 */
final class FileSourceConnectorSpec
    extends AnyFunSuite
    with Matchers
    with EitherValues
    with OptionValues
    with LocalSparkSuite {

  private val now = Instant.now().truncatedTo(ChronoUnit.MILLIS)
  private val anHourAgo = now.minusSeconds(3600)

  private var count = 0

  /** A landing zone of its own for each test, with the directory of the source made. */
  private def landingZone(): Path = {
    count += 1
    val root = Files.createDirectories(workDir.resolve(s"landing-$count"))
    Files.createDirectories(root.resolve("shop/orders"))
    root
  }

  private def source(format: String): Source =
    Source
      .load(
        "shop.orders.yaml",
        s"""id: shop.orders
           |connector: { type: file, path: shop/orders, format: $format }
           |load: { mode: new_files }
           |targets:
           |  bronze: { table: bronze/orders }
           |""".stripMargin
      )
      .value
      .value

  private def configOf(of: Source): FileConnectorConfig =
    of.connector.asInstanceOf[FileConnectorConfig]

  private def connector(root: Path, afterRead: () => Unit = () => ()): FileSourceConnector =
    new FileSourceConnector(new Landing(RootUri.parse(root.toUri.toString).value), afterRead)

  /** A file of the source, changed an hour ago: complete, for a plan made now. */
  private def drop(root: Path, name: String, text: String, at: Instant = anHourAgo): Path = {
    val path = root.resolve("shop/orders").resolve(name)
    Files.write(path, text.getBytes(UTF_8))
    Files.setLastModifiedTime(path, FileTime.from(at))
    path
  }

  /** A file of Parquet of the source, written by Spark and left under a name of its own. */
  private def dropParquet(root: Path, name: String, frame: DataFrame): Path = {
    val out = workDir.resolve(s"written-$count-$name")
    frame.coalesce(1).write.parquet(out.toString)
    val part = Files.list(out).filter(_.toString.endsWith(".parquet")).findFirst().get
    val target = root.resolve("shop/orders").resolve(name)
    Files.copy(part, target)
    Files.setLastModifiedTime(target, FileTime.from(anHourAgo))
    target
  }

  private def taken(files: Set[LandedFile]): FileRegistry = new FileRegistry {
    override def processed(directory: RelativePath, names: Seq[String]): Set[LandedFile] =
      files.filter(file => names.contains(file.name.value))
  }

  private def plan(
      root: Path,
      of: Source,
      processed: Set[LandedFile] = Set.empty
  ): Option[Json] =
    connector(root).plan(of, configOf(of), PlanContext(now, None, taken(processed)))

  private def read(root: Path, of: Source, plan: Json): ReadResult =
    connector(root).read(of, configOf(of), plan, ReadContext(spark))

  /** Plans the files of the directory, and reads the plan. */
  private def ingest(root: Path, of: Source): ReadResult = read(root, of, plan(root, of).value)

  private def rows(frame: DataFrame): Seq[Row] = frame.collect().toSeq.sortBy(_.toString)

  private def refused(body: => Any): String = {
    val failure = the[RunFailedException] thrownBy body
    failure.failure shouldBe FailureClass.Permanent
    failure.getMessage
  }

  /**
   * What a read says of a plan whose files are not as they were planned: not a failure with a
   * class, but what the engine gives the batch up by (ADR-0018).
   */
  private def changedUnder(body: => Any): SourceChangedException = {
    val changed = the[SourceChangedException] thrownBy body
    changed.getMessage shouldBe changed.summary
    changed.summary should include("cannot be read as it was planned")
    changed.summary should include("nothing of it was written")
    changed
  }

  // ---------------------------------------------------------------------------------------------
  // The plan
  // ---------------------------------------------------------------------------------------------

  test("a plan is the new, complete files, by their name, size and time alone") {
    val root = landingZone()
    val orders = source("csv")
    drop(root, "a.csv", "id\n1\n")
    drop(root, "b.csv", "id\n2\n")
    drop(root, "fresh.csv", "id\n3\n", at = now)

    val json = plan(root, orders).value
    val planned = FilePlan.fromJson(json).value
    planned.names.map(_.value) shouldBe Vector("a.csv", "b.csv")
    planned.files.head shouldBe LandedFile(planned.names.head, 5, anHourAgo)
    // Nothing of the file system it was made on, which a restart may number otherwise.
    json.hcursor.downField("files").downArray.keys.value.toSeq shouldBe
      Seq("name", "size", "modified_at")

    // A file that a batch took is not planned again.
    FilePlan
      .fromJson(plan(root, orders, processed = Set(planned.files.head)).value)
      .value
      .names
      .map(_.value) shouldBe Vector("b.csv")
    plan(root, orders, processed = planned.files.toSet) shouldBe None
  }

  test("a directory with nothing new plans nothing") {
    plan(landingZone(), source("csv")) shouldBe None
  }

  test("a file whose name no run may read is refused when the batch is planned") {
    val root = landingZone()
    drop(root, "a[1].csv", "id\n1\n")

    refused(plan(root, source("csv"))) should include("has '[' (U+005B)")
  }

  // ---------------------------------------------------------------------------------------------
  // CSV
  // ---------------------------------------------------------------------------------------------

  test("CSV is read as text, by its header, with the file of each row") {
    val root = landingZone()
    drop(root, "a.csv", "id,amount,note\n1,10.50,\"two\nlines\"\n2,007,\n")
    drop(root, "b.csv", "id,amount,note\n3,1e3,\"a, b\"\n")

    val result = ingest(root, source("csv"))

    result.rows.schema shouldBe StructType(
      Seq("id", "amount", "note", "_source_file", "_rescued_data")
        .map(StructField(_, StringType))
    )
    rows(result.rows) shouldBe Seq(
      Row("1", "10.50", "two\nlines", "shop/orders/a.csv", null),
      Row("2", "007", null, "shop/orders/a.csv", null),
      Row("3", "1e3", "a, b", "shop/orders/b.csv", null)
    )
    result.unparsed.value.count() shouldBe 0
  }

  test("a record of CSV with more values or fewer than the header is set apart, as written") {
    val root = landingZone()
    drop(root, "a.csv", "id,name\n1,ann\n2\n3,bob,extra\n4,cy\n")

    val result = ingest(root, source("csv"))

    rows(result.rows.select("id", "name")) shouldBe Seq(Row("1", "ann"), Row("4", "cy"))
    result.unparsed.value.columns.toSeq shouldBe Seq("_source_file", "_record")
    rows(result.unparsed.value) shouldBe Seq(
      Row("shop/orders/a.csv", "2"),
      Row("shop/orders/a.csv", "3,bob,extra")
    )
  }

  test("a quote that is not closed takes the rest of its file into its value, as it was") {
    // The parser does not say that the quote was never closed, and the record has the values
    // of the header: Bronze keeps what was read, for Silver and its checks to see.
    val root = landingZone()
    drop(root, "a.csv", "id,name\n1,ann\n2,\"bob\n3,cy\n")

    val result = ingest(root, source("csv"))

    rows(result.rows.select("id", "name")) shouldBe Seq(Row("1", "ann"), Row("2", "bob\n3,cy\n"))
    result.unparsed.value.count() shouldBe 0
  }

  test("CSV is read as RFC 4180 writes it: a quote inside a quoted value is the quote twice") {
    // Spark's own escape is a backslash: under it the first value would keep every quote it
    // was written with, and the second would end at its comma, with its record set apart.
    val root = landingZone()
    drop(
      root,
      "a.csv",
      "id,name\n" +
        "1,\"said \"\"hi\"\"\"\n" +
        "2,\"say \"\"hi\"\", then go\"\n" +
        "3,\"a, b\"\n" +
        "4,\"\"\"all of it\"\"\"\n" +
        // A backslash is a character like any other, before a quote as well.
        "5,C:\\temp\\new\n" +
        "6,\"ends in a backslash\\\"\n" +
        "7,\"a \\\"\"quote\\\"\" after one\"\n" +
        "8,cy\n"
    )

    val result = ingest(root, source("csv"))

    rows(result.rows.select("id", "name")) shouldBe Seq(
      Row("1", "said \"hi\""),
      Row("2", "say \"hi\", then go"),
      Row("3", "a, b"),
      Row("4", "\"all of it\""),
      Row("5", "C:\\temp\\new"),
      Row("6", "ends in a backslash\\"),
      Row("7", "a \\\"quote\\\" after one"),
      Row("8", "cy")
    )
    result.unparsed.value.count() shouldBe 0
  }

  test("files of CSV with different headers fail the batch, and nothing is read") {
    val root = landingZone()
    drop(root, "a.csv", "id,name\n1,ann\n")
    drop(root, "b.csv", "id,nombre\n2,bea\n")

    refused(ingest(root, source("csv"))) should include("do not all have one header")
  }

  test("an empty file of CSV has no rows, and no header to differ") {
    val root = landingZone()
    drop(root, "a.csv", "")
    drop(root, "b.csv", "id\n1\n")
    drop(root, "c.csv", "id\n")

    rows(ingest(root, source("csv")).rows.select("id", "_source_file")) shouldBe
      Seq(Row("1", "shop/orders/b.csv"))
  }

  test("a batch of empty files reads no rows, and no column of the source") {
    val root = landingZone()
    drop(root, "a.csv", "")

    val result = ingest(root, source("csv"))
    result.rows.columns.toSeq shouldBe Seq("_source_file", "_rescued_data")
    result.rows.count() shouldBe 0
  }

  test("each row says its file by the name the source left it under, whatever the name") {
    val root = landingZone()
    val names = Seq("a b.csv", "100%20.csv", "n#1.csv", "año+1.csv", "ü.csv")
    names.foreach(name => drop(root, name, s"id\n$name\n"))

    rows(ingest(root, source("csv")).rows.select("id", "_source_file")) shouldBe
      names.sorted.map(name => Row(name, s"shop/orders/$name")).sortBy(_.toString)
  }

  test("a file reached through a link inside the landing zone is said by its own name") {
    val root = landingZone()
    val shared = Files.createDirectories(root.resolve("shared")).resolve("x.csv")
    Files.write(shared, "id\n1\n".getBytes(UTF_8))
    Files.setLastModifiedTime(shared, FileTime.from(anHourAgo))
    Files.createSymbolicLink(root.resolve("shop/orders/today.csv"), shared)

    rows(ingest(root, source("csv")).rows.select("id", "_source_file")) shouldBe
      Seq(Row("1", "shop/orders/today.csv"))
  }

  test("a file left after the plan is not read with it") {
    val root = landingZone()
    val orders = source("csv")
    drop(root, "a.csv", "id\n1\n")
    val planned = plan(root, orders).value
    drop(root, "b.csv", "id\n2\n")

    rows(read(root, orders, planned).rows.select("id")) shouldBe Seq(Row("1"))
  }

  test("a column of the source under a name of the engine's is refused, case aside") {
    val root = landingZone()
    drop(root, "a.csv", "id,_RUN_ID\n1,x\n")

    refused(ingest(root, source("csv"))) should include("'_RUN_ID', which is the engine's _run_id")

    val other = landingZone()
    drop(other, "a.csv", "id,_Lakehouse_Unparsed\n1,x\n")
    refused(ingest(other, source("csv"))) should include("'_Lakehouse_Unparsed'")
  }

  // ---------------------------------------------------------------------------------------------
  // JSON and Parquet
  // ---------------------------------------------------------------------------------------------

  test("JSON is read as text, an object or a list as the JSON it was, and a broken line apart") {
    val root = landingZone()
    drop(
      root,
      "a.json",
      """{"id":1,"amount":10.50,"tags":["x","y"]}
        |not json
        |{"id":"2","customer":{"name":"Bea"},"vip":true}
        |""".stripMargin
    )

    val result = ingest(root, source("json"))

    result.rows.columns.toSeq shouldBe
      Seq("amount", "customer", "id", "tags", "vip", "_source_file", "_rescued_data")
    rows(result.rows.drop("_source_file", "_rescued_data")) shouldBe Seq(
      Row("10.50", null, "1", """["x","y"]""", null),
      Row(null, """{"name":"Bea"}""", "2", null, "true")
    )
    rows(result.unparsed.value) shouldBe Seq(Row("shop/orders/a.json", "not json"))
  }

  test("a file of JSON with no line that parses has every line set apart") {
    val root = landingZone()
    drop(root, "a.json", "not json\n{broken\n")
    drop(root, "b.json", "")

    val result = ingest(root, source("json"))

    result.rows.columns.toSeq shouldBe Seq("_source_file", "_rescued_data")
    result.rows.count() shouldBe 0
    rows(result.unparsed.value.select(ReadResult.Record)) shouldBe Seq(
      Row("not json"),
      Row("{broken")
    )
  }

  test("Parquet is read with its own types, and the columns of every file of the batch") {
    val root = landingZone()
    val session = spark
    import session.implicits._
    def parquet(name: String, frame: DataFrame): Unit = {
      val out = workDir.resolve(s"parquet-$count-$name")
      frame.coalesce(1).write.parquet(out.toString)
      val part = Files.list(out).filter(_.toString.endsWith(".parquet")).findFirst().get
      val target = root.resolve("shop/orders").resolve(name)
      Files.copy(part, target)
      Files.setLastModifiedTime(target, FileTime.from(anHourAgo))
      ()
    }
    parquet("a.parquet", Seq((1L, "ann")).toDF("id", "name"))
    parquet("b.parquet", Seq((2L, 9.5)).toDF("id", "score"))

    val result = ingest(root, source("parquet"))

    result.unparsed shouldBe None
    result.rows.schema("id").dataType shouldBe LongType
    rows(result.rows.select(col("id"), col("name"), col("score"), col("_source_file"))) shouldBe
      Seq(
        Row(1L, "ann", null, "shop/orders/a.parquet"),
        Row(2L, null, 9.5, "shop/orders/b.parquet")
      )
    result.rows.columns should not contain "_rescued_data"
  }

  // ---------------------------------------------------------------------------------------------
  // What a source may bring, and the read must not get wrong (the review of P5)
  // ---------------------------------------------------------------------------------------------

  test("a column of the source named _metadata is a column like any other, in every format") {
    val csv = landingZone()
    drop(csv, "a.csv", "id,_metadata\n1,x\n")
    rows(ingest(csv, source("csv")).rows.select("id", "_metadata", "_source_file")) shouldBe
      Seq(Row("1", "x", "shop/orders/a.csv"))

    // Spark tells the names of columns apart without their case, and so its own under this one.
    val cased = landingZone()
    drop(cased, "a.csv", "id,_METADATA\n1,x\n")
    rows(ingest(cased, source("csv")).rows.select("id", "_METADATA", "_source_file")) shouldBe
      Seq(Row("1", "x", "shop/orders/a.csv"))

    val json = landingZone()
    drop(json, "a.json", """{"id":"1","_metadata":{"etag":"x"}}""" + "\n")
    rows(ingest(json, source("json")).rows.select("id", "_metadata", "_source_file")) shouldBe
      Seq(Row("1", """{"etag":"x"}""", "shop/orders/a.json"))

    val parquet = landingZone()
    val session = spark
    import session.implicits._
    dropParquet(parquet, "a.parquet", Seq((1L, "x")).toDF("id", "_metadata"))
    rows(ingest(parquet, source("parquet")).rows.select("id", "_metadata", "_source_file")) shouldBe
      Seq(Row(1L, "x", "shop/orders/a.parquet"))
  }

  test("fields of JSON that differ only in their case fail the batch: no value is lost") {
    // In two files, or in two records of one: Spark would read them as one column, and leave
    // the values under one of the two names out, without a word.
    val files = landingZone()
    drop(files, "a.json", """{"id":"1"}""" + "\n")
    drop(files, "b.json", """{"ID":"2"}""" + "\n")
    val said = refused(ingest(files, source("json")))
    said should (include("'ID' and 'id'") and include("only in their case"))
    said should include("Nothing was written")

    val records = landingZone()
    drop(
      records,
      "a.json",
      """{"id":"1","amount":"5"}""" + "\n" + """{"Id":"2","AMOUNT":"7"}""" + "\n"
    )
    refused(ingest(records, source("json"))) should
      (include("'AMOUNT' and 'amount'") and include("'Id' and 'id'"))

    // In one record: Spark refuses such columns itself, with an error that says no class.
    val record = landingZone()
    drop(record, "a.json", """{"id":"1","Id":"2"}""" + "\n")
    refused(ingest(record, source("json"))) should include("'Id' and 'id'")
  }

  test("a record of JSON with a field twice fails the batch, as a failure that says its class") {
    val root = landingZone()
    drop(root, "a.json", """{"id":"1"}""" + "\n" + """{"id":"5","id":"6"}""" + "\n")

    refused(ingest(root, source("json"))) should
      (include("a field twice") and include("Nothing was written"))
  }

  test("fields that differ only in their case inside an object of JSON are kept, as its text") {
    val root = landingZone()
    drop(root, "a.json", """{"id":"1","customer":{"name":"Bea","NAME":"B"}}""" + "\n")

    rows(ingest(root, source("json")).rows.select("id", "customer")) shouldBe
      Seq(Row("1", """{"name":"Bea","NAME":"B"}"""))
  }

  test("a header of CSV with a name twice, or with an empty name, fails the batch and says so") {
    // Spark reads such a header under names of its own making, which no file then has.
    Seq("id,name,name\n1,x,y\n", "id,Name,name\n1,x,y\n", "id,,name\n1,x,y\n").foreach { text =>
      val root = landingZone()
      drop(root, "a.csv", text)
      withClue(text) {
        refused(ingest(root, source("csv"))) should
          (include("a name twice") and include("a name that is empty"))
      }
    }
  }

  test("a column of the source is not taken for the one that a batch with no column is read by") {
    val root = landingZone()
    drop(root, "a.csv", "id,_lakehouse_no_column\n1,x\n")

    rows(ingest(root, source("csv")).rows.select("id", "_lakehouse_no_column")) shouldBe
      Seq(Row("1", "x"))
  }

  test("a file that cannot be read fails the batch, whatever the session says of such files") {
    val root = landingZone()
    val session = spark
    import session.implicits._
    dropParquet(root, "a.parquet", Seq((1L, "ann")).toDF("id", "name"))
    drop(root, "b.parquet", "this is no file of Parquet")
    val orders = source("parquet")
    val planned = plan(root, orders).value

    // With this, Spark passes over a file that it cannot read, and says nothing: the file
    // would be recorded as taken, with none of its rows in Bronze.
    val setting = "spark.sql.files.ignoreCorruptFiles"
    spark.conf.set(setting, "true")
    try an[Exception] should be thrownBy read(root, orders, planned)
    finally spark.conf.unset(setting)
  }

  // ---------------------------------------------------------------------------------------------
  // The files that were planned, and no other
  // ---------------------------------------------------------------------------------------------

  test("a file written again after its plan is not read, and the read says what changed") {
    val root = landingZone()
    val orders = source("csv")
    val a = drop(root, "a.csv", "id\n1\n")
    drop(root, "b.csv", "id\n2\n")
    val planned = plan(root, orders).value
    Files.write(a, "id\n1\n2\n".getBytes(UTF_8))

    val changed = changedUnder(read(root, orders, planned))
    changed.summary should include("1 file of it changed since it was planned")
    changed.summary should include("'a.csv' is of 7 bytes, and was of 5")
    // Each file that changed, by the name that the plan has it under: the other is not named.
    changed.changed.map(_.item) shouldBe Seq("a.csv")
    changed.changed.head.change should startWith("is of 7 bytes, and was of 5, was changed at ")
  }

  test("a file that is gone, or a link that leads to another size now, fails the read") {
    val root = landingZone()
    val orders = source("csv")
    drop(root, "a.csv", "id\n1\n")
    val first = Files.createDirectories(root.resolve("shared")).resolve("first.csv")
    Files.write(first, "id\n2\n".getBytes(UTF_8))
    Files.setLastModifiedTime(first, FileTime.from(anHourAgo))
    val link = Files.createSymbolicLink(root.resolve("shop/orders/b.csv"), first)
    val planned = plan(root, orders).value

    Files.delete(root.resolve("shop/orders/a.csv"))
    Files.delete(link)
    val second = Files.write(root.resolve("shared/second.csv"), "id\n22\n".getBytes(UTF_8))
    Files.setLastModifiedTime(second, FileTime.from(anHourAgo))
    Files.createSymbolicLink(link, second)

    val changed = changedUnder(read(root, orders, planned))
    changed.summary should include("2 files of it changed since it was planned")
    changed.summary should include("'a.csv' is not there any more")
    changed.summary should include("'b.csv' is of 6 bytes, and was of 5")
    changed.changed shouldBe Seq(
      SourceChangedException.Changed("a.csv", "is not there any more, or is a directory now"),
      SourceChangedException.Changed("b.csv", "is of 6 bytes, and was of 5")
    )
  }

  test("a link that leads out of the landing zone after the plan fails the read") {
    val root = landingZone()
    val orders = source("csv")
    val inside = Files.createDirectories(root.resolve("shared")).resolve("x.csv")
    Files.write(inside, "id\n1\n".getBytes(UTF_8))
    Files.setLastModifiedTime(inside, FileTime.from(anHourAgo))
    val link = Files.createSymbolicLink(root.resolve("shop/orders/x.csv"), inside)
    val planned = plan(root, orders).value

    val outside = Files.copy(inside, workDir.resolve(s"outside-$count.csv"))
    Files.setLastModifiedTime(outside, FileTime.from(anHourAgo))
    Files.delete(link)
    Files.createSymbolicLink(link, outside)

    // It changed since it was planned, as a file written again did. A plan made now refuses
    // the link, as a permanent failure: the listing does not take a file that leads out.
    val changed = changedUnder(read(root, orders, planned))
    changed.changed shouldBe Seq(
      SourceChangedException
        .Changed("x.csv", "leads out of the landing zone, through a symbolic link")
    )
    refused(plan(root, orders)) should include("'x.csv' leads out of the landing zone")
  }

  test("a file changed while it was read fails the read, after Spark read it") {
    val root = landingZone()
    val orders = source("csv")
    val a = drop(root, "a.csv", "id\n1\n")
    val planned = plan(root, orders).value

    var read = false
    val changing = connector(
      root,
      afterRead = () => {
        read = true
        Files.write(a, "id\n1\n2\n".getBytes(UTF_8))
        ()
      }
    )
    val changed =
      changedUnder(changing.read(orders, configOf(orders), planned, ReadContext(spark)))
    read shouldBe true
    changed.summary should include("1 file of it changed while it was read")
    changed.changed.map(_.item) shouldBe Seq("a.csv")
  }

  test("a file put in another's place while it was read, of the same size and time, fails it") {
    val root = landingZone()
    val orders = source("csv")
    val a = drop(root, "a.csv", "id\n1\n")
    val planned = plan(root, orders).value

    // What a producer does that writes a file whole and renames it over the old one.
    val replacing = connector(
      root,
      afterRead = () => {
        val other = drop(root, ".a.csv.tmp", "id\n9\n")
        Files.move(other, a, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        ()
      }
    )
    val changed =
      changedUnder(replacing.read(orders, configOf(orders), planned, ReadContext(spark)))
    changed.summary should include("1 file of it changed while it was read")
    changed.summary should include(
      "'a.csv' is another file of the file system than the one that was read"
    )
    changed.summary should not include "bytes"
  }

  test("a link that leads to another file while it was read fails the read") {
    val root = landingZone()
    val orders = source("csv")
    val shared = Files.createDirectories(root.resolve("shared"))
    val first = Files.write(shared.resolve("first.csv"), "id\n1\n".getBytes(UTF_8))
    Files.setLastModifiedTime(first, FileTime.from(anHourAgo))
    val link = Files.createSymbolicLink(root.resolve("shop/orders/a.csv"), first)
    val planned = plan(root, orders).value

    val relinking = connector(
      root,
      afterRead = () => {
        val second = Files.copy(first, shared.resolve("second.csv"))
        Files.setLastModifiedTime(second, FileTime.from(anHourAgo))
        Files.delete(link)
        Files.createSymbolicLink(link, second)
        ()
      }
    )
    changedUnder(
      relinking.read(orders, configOf(orders), planned, ReadContext(spark))
    ).summary should include(
      "'a.csv' is another file of the file system than the one that was read"
    )
  }

  test("what was read is what everything after the read is made from, not the files") {
    val root = landingZone()
    val orders = source("csv")
    val a = drop(root, "a.csv", "id,name\n1,ann\n2\n")
    val result = ingest(root, orders)

    Files.write(a, "id,name\n7,zed\n8,yu\n9,xi\n".getBytes(UTF_8))

    rows(result.rows.select("id")) shouldBe Seq(Row("1"))
    rows(result.unparsed.value.select(ReadResult.Record)) shouldBe Seq(Row("2"))
  }

  test("what the checks cannot see: a file changed in its place with its size and time put back") {
    // The limit of a check by size, time and inode (FileSourceConnector): telling this needs
    // what the file holds, read twice. It takes a producer that means to hide the change.
    val root = landingZone()
    val orders = source("csv")
    val a = drop(root, "a.csv", "id\n1\n")
    val planned = plan(root, orders).value
    Files.write(a, "id\n9\n".getBytes(UTF_8))
    Files.setLastModifiedTime(a, FileTime.from(anHourAgo))

    rows(read(root, orders, planned).rows.select("id")) shouldBe Seq(Row("9"))
  }

  test("what the checks cannot see: another file put in its place after the plan, alike") {
    // A plan knows a file by its size and time, as processed_file does: which file of the file
    // system it is, is taken just before the read, and a plan made then would take this one too.
    val root = landingZone()
    val orders = source("csv")
    val a = drop(root, "a.csv", "id\n1\n")
    val planned = plan(root, orders).value
    val other = drop(root, ".a.csv.tmp", "id\n9\n")
    Files.move(other, a, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)

    rows(read(root, orders, planned).rows.select("id")) shouldBe Seq(Row("9"))
  }

  // ---------------------------------------------------------------------------------------------
  // Plans that are not this source's
  // ---------------------------------------------------------------------------------------------

  test("a plan of another form, or of another directory, is refused") {
    val root = landingZone()
    val orders = source("csv")
    drop(root, "a.csv", "id\n1\n")
    val planned = plan(root, orders).value

    refused(read(root, orders, planned.mapObject(_.add("version", Json.fromInt(2))))) should
      include("is not one that this application reads: it is of version 2")
    refused(
      read(root, orders, planned.mapObject(_.add("directory", Json.fromString("shop/returns"))))
    ) should include("is of the directory shop/returns, and the source reads shop/orders")
  }

  // ---------------------------------------------------------------------------------------------
  // What the commit of a plan records
  // ---------------------------------------------------------------------------------------------

  test("what the commit of a plan records is its files, said from the plan alone") {
    val root = landingZone()
    val orders = source("csv")
    drop(root, "a.csv", "id\n1\n")
    drop(root, "empty.csv", "")
    val planned = plan(root, orders).value
    // The files are gone: the plan is all that is read, as for a batch that is rolled forward.
    Files.delete(root.resolve("shop/orders/a.csv"))
    Files.delete(root.resolve("shop/orders/empty.csv"))

    val progress = connector(root).progress(orders, configOf(orders), planned)

    progress.watermark shouldBe None
    progress.files shouldBe Some(FilePlan.fromJson(planned).value)
    progress.files.value.names.map(_.value) shouldBe Vector("a.csv", "empty.csv")
    progress.bytes shouldBe Some(5L)

    // A plan that is not this source's has nothing to record, as it has nothing to read.
    refused(
      connector(root)
        .progress(orders, configOf(orders), planned.mapObject(_.add("version", Json.fromInt(2))))
    ) should include("is not one that this application reads: it is of version 2")
    refused(
      connector(root).progress(
        orders,
        configOf(orders),
        planned.mapObject(_.add("directory", Json.fromString("shop/returns")))
      )
    ) should include("is of the directory shop/returns, and the source reads shop/orders")
  }
}
