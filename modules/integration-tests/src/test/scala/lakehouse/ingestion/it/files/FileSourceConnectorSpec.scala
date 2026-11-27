package lakehouse.ingestion.it.files

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.attribute.{BasicFileAttributeView, FileTime, PosixFilePermissions}
import java.nio.file.{Files, LinkOption, Path, StandardCopyOption}
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.zip.GZIPOutputStream

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Using

import io.circe.Json
import lakehouse.ingestion.core.config._
import lakehouse.ingestion.core.control.FailureClass
import lakehouse.ingestion.core.files.{FileHorizon, FilePlan, LandedFile}
import lakehouse.ingestion.engine.failure.RunFailedException
import lakehouse.ingestion.engine.connector._
import lakehouse.ingestion.engine.files.{FileSourceConnector, Landing}
import lakehouse.ingestion.it.support.{CapturedLogs, LocalSparkSuite}
import org.apache.spark.SparkException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{EitherValues, LoneElement, OptionValues}

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
    with LoneElement
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

  private def source(
      format: String,
      batch: Option[Int] = None,
      policy: String = "fail",
      csv: String = ""
  ): Source =
    Source
      .load(
        "shop.orders.yaml",
        s"""id: shop.orders
           |connector: { type: file, path: shop/orders, format: $format${batch.fold("")(n =>
            s", max_files_per_batch: $n"
          )}, on_unreadable_file: $policy${if (csv.isEmpty) "" else s", csv: { $csv }"} }
           |load: { mode: new_files }
           |targets:
           |  bronze: { table: bronze/orders }
           |""".stripMargin
      )
      .value
      .value

  private def configOf(of: Source): FileConnectorConfig =
    of.connector.asInstanceOf[FileConnectorConfig]

  private def connector(
      root: Path,
      afterRead: () => Unit = () => (),
      beforeAlone: (String, Int) => Unit = (_, _) => ()
  ): FileSourceConnector =
    new FileSourceConnector(
      new Landing(RootUri.parse(root.toUri.toString).value),
      afterRead,
      beforeAlone
    )

  /** A file of the source, changed an hour ago: complete, for a plan made now. */
  private def drop(root: Path, name: String, text: String, at: Instant = anHourAgo): Path = {
    val path = root.resolve("shop/orders").resolve(name)
    Files.write(path, text.getBytes(UTF_8))
    Files.setLastModifiedTime(path, FileTime.from(at))
    path
  }

  /** A file of the source with these bytes, in gzip if it says so, changed an hour ago. */
  private def dropBytes(
      root: Path,
      name: String,
      bytes: Array[Byte],
      gzip: Boolean = false
  ): Path = {
    val path = root.resolve("shop/orders").resolve(name)
    if (gzip) Using.resource(new GZIPOutputStream(Files.newOutputStream(path)))(_.write(bytes))
    else Files.write(path, bytes)
    Files.setLastModifiedTime(path, FileTime.from(anHourAgo))
    path
  }

  /** Bytes: a string as UTF-8, a number as the byte that it is, and bytes as they are. */
  private def bytesOf(parts: Any*): Array[Byte] = parts.flatMap {
    case text: String => text.getBytes(UTF_8).toSeq
    case byte: Int => Seq(byte.toByte)
    case many: Array[Byte] => many.toSeq
    case other => throw new IllegalArgumentException(s"$other is not bytes")
  }.toArray

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
      processed: Set[LandedFile] = Set.empty,
      horizon: Option[Json] = None
  ): Option[Json] =
    connector(root).plan(of, configOf(of), PlanContext(now, None, taken(processed), horizon))

  private def read(root: Path, of: Source, plan: Json): ReadResult.Kept =
    connector(root).read(of, configOf(of), plan, ReadContext(spark))

  /** Plans the files of the directory, and reads the plan. */
  private def ingest(root: Path, of: Source): ReadResult.Kept = read(root, of, plan(root, of).value)

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

  test("a chain takes the files complete at its first plan, and no more of them than there were") {
    val root = landingZone()
    val orders = source("csv", batch = Some(2))
    Seq("a.csv", "b.csv", "c.csv").foreach(name => drop(root, name, "id\n1\n"))

    val first = FilePlan.fromJson(plan(root, orders).value).value
    first.names.map(_.value) shouldBe Vector("a.csv", "b.csv")
    val horizon = connector(root).more(orders, configOf(orders), first.toJson).value
    FileHorizon.fromJson(horizon).value shouldBe
      FileHorizon(now.minusMillis(configOf(orders).minFileAge.toMillis), 1)

    // Meanwhile a file changed now, after the horizon, and a copy that kept the time of a file
    // changed two hours ago: both complete by the time of the next plan.
    drop(root, "now.csv", "id\n2\n", at = now)
    drop(root, "copy.csv", "id\n3\n", at = anHourAgo.minusSeconds(3600))
    val next = FilePlan
      .fromJson(plan(root, orders, processed = first.files.toSet, horizon = Some(horizon)).value)
      .value

    // One file, the one left of the three: the oldest of those changed by the horizon.
    next.names.map(_.value) shouldBe Vector("copy.csv")
    connector(root).more(orders, configOf(orders), next.toJson) shouldBe None
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
    result.unparsed.value.columns.toSeq shouldBe
      Seq("_source_file", "_record", "_error_type", "_error_detail")
    rows(result.unparsed.value) shouldBe Seq(
      Row("shop/orders/a.csv", "2", "unparsed", null),
      Row("shop/orders/a.csv", "3,bob,extra", "unparsed", null)
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

  // ---------------------------------------------------------------------------------------------
  // The dialect and the encoding of a source of CSV (P8.9, ADR-0024)
  // ---------------------------------------------------------------------------------------------

  test("CSV in the dialect of its source: its delimiter, its quote and its escape") {
    val root = landingZone()
    drop(
      root,
      "a.csv",
      "id;name;note\n" +
        // The delimiter between quotes, a quote and the escape escaped, an end of line.
        "1;'a; b';'it\\'s'\n" +
        "2;'C:\\\\temp';'two\nlines'\n" +
        // A backslash before any other character, and outside quotes, is itself.
        "3;a\\b;'a\\nb'\n" +
        // A quote of another kind, and the comma, are characters like any other.
        "4;\"x\";a,b\n"
    )

    val result = ingest(root, source("csv", csv = """delimiter: ";", quote: "'", escape: "\\""""))

    rows(result.rows.select("id", "name", "note")) shouldBe Seq(
      Row("1", "a; b", "it's"),
      Row("2", "C:\\temp", "two\nlines"),
      Row("3", "a\\b", "a\\nb"),
      Row("4", "\"x\"", "a,b")
    )
    result.unparsed.value.count() shouldBe 0
  }

  test("CSV in a tab, its values with commas and quotes as RFC 4180 has them") {
    val root = landingZone()
    drop(root, "a.tsv", "id\tname\n1\t\"a\tb, \"\"c\"\"\"\n2\tplain\n")

    rows(
      ingest(root, source("csv", csv = """delimiter: "\t"""")).rows.select("id", "name")
    ) shouldBe
      Seq(Row("1", "a\tb, \"c\""), Row("2", "plain"))
  }

  test("CSV in the encoding of its source: ISO-8859-1, and windows-1252 in gzip as well") {
    val latin1 = landingZone()
    dropBytes(latin1, "a.csv", bytesOf("id,a", 0xf1, "o\n1,ni", 0xf1, "o\n"))
    val read1 = ingest(latin1, source("csv", csv = "encoding: ISO-8859-1"))
    read1.rows.columns should contain("a\u00f1o")
    rows(read1.rows.select("id", "a\u00f1o")) shouldBe Seq(Row("1", "ni\u00f1o"))

    // As a spreadsheet in Spain writes "CSV": `;`, CRLF, and LF alone inside a cell.
    val cp1252 = landingZone()
    val sheet = bytesOf(
      "id;importe;nota\r\n1;10 ",
      0x80,
      ";\"l",
      0xed,
      "nea uno\nl",
      0xed,
      "nea dos\"\r\n2;",
      0x93,
      "x",
      0x94,
      ";\r\n"
    )
    dropBytes(cp1252, "a.csv", sheet)
    dropBytes(cp1252, "b.csv.gz", sheet, gzip = true)
    val read2 = ingest(cp1252, source("csv", csv = """delimiter: ";", encoding: windows-1252"""))
    rows(read2.rows.select("id", "importe", "nota")) shouldBe Seq(
      Row("1", "10 \u20ac", "l\u00ednea uno\nl\u00ednea dos"),
      Row("1", "10 \u20ac", "l\u00ednea uno\nl\u00ednea dos"),
      Row("2", "\u201cx\u201d", null),
      Row("2", "\u201cx\u201d", null)
    )
    read2.unparsed.value.count() shouldBe 0
  }

  test("the mark of the order of bytes of UTF-8 is taken off, of every file of a batch") {
    val root = landingZone()
    val mark = bytesOf(0xef, 0xbb, 0xbf)
    dropBytes(root, "a.csv", bytesOf(mark, "id,name\n1,ann\n"))
    dropBytes(root, "b.csv", bytesOf("id,name\n2,bea\n"))
    dropBytes(root, "c.csv", bytesOf(mark, "id,name\n3,cy\n"))

    val result = ingest(root, source("csv"))
    result.rows.columns.filterNot(_.startsWith("_")) shouldBe Array("id", "name")
    rows(result.rows.select("id", "name")) shouldBe Seq(
      Row("1", "ann"),
      Row("2", "bea"),
      Row("3", "cy")
    )
  }

  test("a batch is read as it was planned, whatever the source says by then, and the run says so") {
    val root = landingZone()
    drop(root, "a.csv", "id;name\n1;a b\n")
    val semicolons = source("csv", csv = """delimiter: ";"""")
    val planned = plan(root, semicolons).value
    planned.hcursor.downField("csv").get[String]("delimiter").value shouldBe ";"

    // The source says commas now: the plan is read with its semicolons, and the run says so.
    val (result, said) =
      CapturedLogs.of(classOf[FileSourceConnector])(read(root, source("csv"), planned))
    rows(result.rows.select("id", "name")) shouldBe Seq(Row("1", "a b"))
    said.map(_.message) should contain(
      "shop.orders: the batch is read as it was planned, as csv (delimiter ';' (U+003B), " +
        "quote '\"' (U+0022), escape '\"' (U+0022), encoding UTF-8), and the source now says " +
        "csv (delimiter ',' (U+002C), quote '\"' (U+0022), escape '\"' (U+0022), encoding " +
        "UTF-8). A plan is read as it was written; to have its files read as the source says " +
        "now, give the batch up (abandon-batch), and the next plan takes them so"
    )
    // Read as the source says, it says nothing.
    CapturedLogs.of(classOf[FileSourceConnector])(read(root, semicolons, planned))._2 shouldBe empty

    // A plan written before plans said how its files are read is read in RFC 4180 and UTF-8,
    // and in the format of the source, as it was then.
    val before = FilePlan
      .fromJson(planned)
      .value
      .copy(readAs = None)
      .toJson
    before.hcursor.get[Int]("version").value shouldBe 2
    rows(read(root, semicolons, before).rows.select("id;name")) shouldBe Seq(Row("1;a b"))
  }

  test("a plan made again of the files of a batch reads them as the source says now") {
    val root = landingZone()
    drop(root, "a.csv", "id;name\n1;a,b\n")
    val commas = source("csv")
    val planned = plan(root, commas).value
    val semicolons = source("csv", csv = """delimiter: ";"""")

    val again = connector(root).again(semicolons, configOf(semicolons), planned)
    val made = FilePlan.fromJson(again).value
    val was = FilePlan.fromJson(planned).value
    (made.directory, made.files, made.horizon) shouldBe ((was.directory, was.files, was.horizon))
    made.readAs.value.csv.value.delimiter shouldBe ';'
    rows(read(root, semicolons, again).rows.select("id", "name")) shouldBe Seq(Row("1", "a,b"))
  }

  test(
    "a charset that the session of a run does not read is permanent, and no file is read alone"
  ) {
    val root = landingZone()
    dropBytes(root, "a.csv", bytesOf("id,v\n1,", 0x80, "\n"))
    dropBytes(root, "b.csv", bytesOf("id,v\n2,x\n"))
    val cp1252 = source("csv", csv = "encoding: windows-1252")
    val alone = mutable.Buffer.empty[String]
    val sparks = spark.newSession()
    sparks.conf.set(Profile.EngineSpark.JavaCharsets, "false")

    val failure = the[RunFailedException] thrownBy
      connector(root, beforeAlone = (name, _) => alone += name)
        .read(cp1252, configOf(cp1252), plan(root, cp1252).value, ReadContext(sparks))
    failure.failure shouldBe FailureClass.Permanent
    failure.getMessage should startWith(
      "Spark refuses to read the files of shop/orders in csv (delimiter ',' (U+002C), quote " +
        "'\"' (U+0022), escape '\"' (U+0022), encoding windows-1252): the session of the run " +
        "does not read that encoding (spark.sql.legacy.javaCharsets is not on)"
    )
    alone shouldBe empty
    // The session of a run reads it.
    rows(ingest(root, cp1252).rows.select("id", "v")) shouldBe Seq(
      Row("1", "\u20ac"),
      Row("2", "x")
    )
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
    rows(result.unparsed.value) shouldBe Seq(
      Row("shop/orders/a.json", "not json", "unparsed", null)
    )
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

  test("a record of JSON with a field twice at its first level is set apart, by its name") {
    val root = landingZone()
    drop(
      root,
      "a.json",
      """{"id":"1"}""" + "\n" + """{"id":"5","id":"6"}""" + "\n" +
        """{"id":"7","o":{"x":1,"x":2}}""" + "\n"
    )

    val result = ingest(root, source("json"))

    // An object inside a record is kept as its text, field twice and all.
    rows(result.rows.select("id", "o")) shouldBe
      Seq(Row("1", null), Row("7", """{"x":1,"x":2}"""))
    rows(result.unparsed.value) shouldBe Seq(
      Row(
        "shop/orders/a.json",
        """{"id":"5","id":"6"}""",
        "duplicate_field",
        "the field 'id' twice, at the first level of the record"
      )
    )
  }

  test("a record of JSON with text after its object is set apart, with where the text starts") {
    val root = landingZone()
    drop(
      root,
      "a.json",
      Seq(
        """{"id":"1"}""",
        """{"id":"4"} trailing""",
        """{"id":"5"} {"id":"6"}""",
        """{"id":"7"} // a note""",
        """{'id':'8'} x""",
        """{"id":"9"}   """
      ).mkString("", "\n", "\n")
    )

    val result = ingest(root, source("json"))

    // Spark's reader would have read 4, 5, 7 and 8 as their objects, and dropped the rest.
    rows(result.rows.select("id")) shouldBe Seq(Row("1"), Row("9"))
    rows(result.unparsed.value.drop("_source_file")) shouldBe Seq(
      Row("""{"id":"4"} trailing""", "trailing_text", "text after the object, from column 12"),
      Row("""{"id":"5"} {"id":"6"}""", "trailing_text", "text after the object, from column 12"),
      Row("""{"id":"7"} // a note""", "trailing_text", "text after the object, from column 12"),
      Row("""{'id':'8'} x""", "trailing_text", "text after the object, from column 12")
    )
  }

  test("a record of JSON has the values that Spark's reader of files gives it, as written") {
    val root = landingZone()
    val written = Seq(
      """{"id":"1","amount":10.50,"big":12345678901234567890123,"exp":1e2,"neg": -0 }""",
      """{"id":"2","o": { "a" : 1.50 , "b":[1, 2] } ,"l":[ 1.50 ],"t":true,"n":null}""",
      """{"id":"3","s":"a\"b é 😀","e":"","x":NaN,'q':'single'}""",
      """{"id":"4","_metadata":{"etag":"x"}}"""
    )
    val file = drop(root, "a.json", written.mkString("", "\n", "\n"))

    val result = ingest(root, source("json"))
    val columns = result.rows.columns.toSeq.filterNot(Set("_source_file", "_rescued_data"))
    // Spark's own reader of files, given the same columns as text.
    val spark = this.spark.read
      .option("mode", "PERMISSIVE")
      .schema(StructType(columns.map(StructField(_, StringType))))
      .json(s"file:$file")

    rows(result.rows.select(columns.map(col): _*)) shouldBe rows(spark)
    rows(result.rows.select("amount", "exp", "neg", "o", "l", "s", "x"))
      .filter(_.get(0) != null) shouldBe
      Seq(Row("10.50", "1e2", "-0", null, null, null, null))
    result.unparsed.value.count() shouldBe 0
  }

  test("a line of JSON that is no whole object is set apart: a list, a record over several lines") {
    val root = landingZone()
    drop(
      root,
      "a.json",
      Seq(
        """{"id":"1"}""",
        // Spark's reader made rows of a list of objects up to P8.8, and none of an empty one.
        """[{"id":"2"},{"id":"3"}]""",
        "[]",
        "5",
        // A record written over several lines: each of its lines, as Spark's reader has it.
        "{",
        """  "id": "4",""",
        "}"
      ).mkString("", "\n", "\n")
    )

    val result = ingest(root, source("json"))

    rows(result.rows.select("id")) shouldBe Seq(Row("1"))
    rows(result.unparsed.value.select("_record", "_error_type")) shouldBe Seq(
      Row("""  "id": "4",""", "unparsed"),
      Row("5", "unparsed"),
      Row("[]", "unparsed"),
      Row("""[{"id":"2"},{"id":"3"}]""", "unparsed"),
      Row("{", "unparsed"),
      Row("}", "unparsed")
    )
  }

  test("lines of JSON ended as Windows ends them, blank lines, and a file in gzip are read") {
    val root = landingZone()
    drop(root, "a.json", "{\"id\":\"1\"}\r\n\r\n   \n{\"id\":\"2\"}")
    val zipped = root.resolve("shop/orders/b.json.gz")
    Using.resource(new GZIPOutputStream(Files.newOutputStream(zipped))) {
      _.write("\uFEFF{\"id\":\"3\"}\n".getBytes(UTF_8))
    }
    Files.setLastModifiedTime(zipped, FileTime.from(anHourAgo))

    val result = ingest(root, source("json"))

    rows(result.rows.select("id", "_source_file")) shouldBe Seq(
      Row("1", "shop/orders/a.json"),
      Row("2", "shop/orders/a.json"),
      Row("3", "shop/orders/b.json.gz")
    )
    result.unparsed.value.count() shouldBe 0
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
  // A file that cannot be read (ADR-0023), broken for real
  // ---------------------------------------------------------------------------------------------

  /** The file with its second half gone, as a copy that stopped halfway leaves it. */
  private def cutShort(file: Path): Path = {
    val bytes = Files.readAllBytes(file)
    Files.write(file, bytes.take(bytes.length / 2))
    Files.setLastModifiedTime(file, FileTime.from(anHourAgo))
    file
  }

  /** A file of CSV of the source in gzip, of `rows` records. */
  private def dropGzip(root: Path, name: String, rows: Int): Path = {
    val path = root.resolve("shop/orders").resolve(name)
    Using.resource(new GZIPOutputStream(Files.newOutputStream(path))) {
      _.write(("id,name\n" + (1 to rows).map(i => s"$i,name $i").mkString("\n")).getBytes(UTF_8))
    }
    Files.setLastModifiedTime(path, FileTime.from(anHourAgo))
    path
  }

  /** `body` with the file one that this process may not open, and then as it was. */
  private def unopenable[A](file: Path)(body: => A): A = {
    val permissions = Files.getPosixFilePermissions(file)
    Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("---------"))
    try {
      assume(!Files.isReadable(file), "the tests run as a user that may open any file")
      body
    } finally Files.setPosixFilePermissions(file, permissions): Unit
  }

  /** A connector that counts the reads of each file alone. */
  private def counting(root: Path): (FileSourceConnector, mutable.Map[String, Int]) = {
    val reads = mutable.Map.empty[String, Int].withDefaultValue(0)
    connector(root, beforeAlone = (name, _) => reads(name) += 1) -> reads
  }

  private def skipped(body: => Any): UnreadableFilesException = {
    val cannot = the[UnreadableFilesException] thrownBy body
    cannot.getMessage shouldBe cannot.summary
    cannot.summary should include("and the source skips such files")
    cannot.summary should include("Nothing of the batch was written")
    cannot
  }

  test("files that cannot be read fail their batch as permanent, each named with what Spark said") {
    val root = landingZone()
    val orders = source("csv")
    drop(root, "a.csv", "id,name\n1,ann\n")
    cutShort(dropGzip(root, "b.csv.gz", rows = 20000))
    val closed = drop(root, "c.csv", "id,name\n3,cy\n")
    drop(root, "d.csv", "id,name\n4,dee\n")
    val planned = plan(root, orders).value
    val (reading, reads) = counting(root)

    val message = unopenable(closed) {
      refused(reading.read(orders, configOf(orders), planned, ReadContext(spark)))
    }
    message should include("the batch of shop/orders has 2 files that cannot be read")
    message should include(
      "'b.csv.gz' unreadable: it failed each of 3 reads of it alone: FAILED_READ_FILE.NO_HINT, " +
        "from java.io.EOFException"
    )
    // Alone, it is the first file of its read, which fails the inference with no condition.
    message should include(
      "'c.csv' unreadable: it failed each of 3 reads of it alone: from java.io.FileNotFoundException"
    )
    message should include("connector.on_unreadable_file: fail")
    // What Spark said of it, and never the text of an exception.
    message should not include "Permission denied"
    message should not include "Unexpected end"
    // A file that reads alone is read once; one that cannot be read, three times.
    reads.toMap shouldBe Map("a.csv" -> 1, "b.csv.gz" -> 3, "c.csv" -> 3, "d.csv" -> 1)
  }

  test("under skip, the same files are said, for the engine to give the batch up and record them") {
    val root = landingZone()
    val orders = source("parquet", policy = "skip")
    val session = spark
    import session.implicits._
    dropParquet(root, "a.parquet", Seq((1L, "ann")).toDF("id", "name"))
    cutShort(
      dropParquet(root, "b.parquet", (1 to 1000).map(i => (i.toLong, s"n$i")).toDF("id", "name"))
    )
    val planned = plan(root, orders).value

    val cannot = skipped(read(root, orders, planned))
    cannot.unreadable shouldBe Seq(
      UnreadableFilesException.Unreadable(
        "b.parquet",
        "unreadable: it failed each of 3 reads of it alone: " +
          "FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER, from java.lang.RuntimeException"
      )
    )
    cannot.summary should include("1 file of the batch of shop/orders cannot be read")
  }

  test("a file of JSON cut short is found by its reads alone, as its lines are read as text") {
    val root = landingZone()
    val orders = source("json", policy = "skip")
    drop(root, "a.json", "{\"id\":\"1\"}\n")
    val cut = root.resolve("shop/orders/b.json.gz")
    Using.resource(new GZIPOutputStream(Files.newOutputStream(cut))) {
      _.write((1 to 20000).map(i => s"""{"id":"$i"}""").mkString("", "\n", "\n").getBytes(UTF_8))
    }
    cutShort(cut)
    val planned = plan(root, orders).value

    skipped(read(root, orders, planned)).unreadable shouldBe Seq(
      UnreadableFilesException.Unreadable(
        "b.json.gz",
        "unreadable: it failed each of 3 reads of it alone: FAILED_READ_FILE.NO_HINT, " +
          "from java.io.EOFException"
      )
    )
  }

  test("the reads alone wait before each round after the first, longer the second time") {
    val root = landingZone()
    val orders = source("csv", policy = "skip")
    drop(root, "a.csv", "id\n1\n")
    cutShort(dropGzip(root, "b.csv.gz", rows = 20000))
    cutShort(dropGzip(root, "c.csv.gz", rows = 20000))
    val planned = plan(root, orders).value
    val (reading, reads) = counting(root)
    val asked = mutable.Buffer.empty[FiniteDuration]

    skipped(
      reading.read(
        orders,
        configOf(orders),
        planned,
        ReadContext(spark, pause = wait => asked += wait: Unit)
      )
    ).unreadable.map(_.item) shouldBe Seq("b.csv.gz", "c.csv.gz")
    // A wait for each round, whatever the number of files that it reads again.
    asked.toSeq shouldBe Seq(1.second, 2.seconds)
    reads.toMap shouldBe Map("a.csv" -> 1, "b.csv.gz" -> 3, "c.csv.gz" -> 3)
  }

  test("a file that fails a read alone, and then reads, is sound: the failure passed") {
    val root = landingZone()
    val orders = source("csv", policy = "skip")
    drop(root, "a.csv", "id\n1\n")
    val closed = drop(root, "b.csv", "id\n2\n")
    val planned = plan(root, orders).value
    val permissions = Files.getPosixFilePermissions(closed)
    // It may be opened again once its first read alone has failed, as a disk that erred once.
    val mending = connector(
      root,
      beforeAlone = (name, n) =>
        if (name == "b.csv" && n == 2) Files.setPosixFilePermissions(closed, permissions): Unit
    )

    val passed = unopenable(closed) {
      the[RunFailedException] thrownBy mending.read(
        orders,
        configOf(orders),
        planned,
        ReadContext(spark)
      )
    }
    passed.failure shouldBe FailureClass.Transient
    passed.getMessage should include("and read alone, 'b.csv' failed once and then read")
    passed.getMessage should include("a failure that passed. Nothing was written")
    // And it reads, as the run that tries again reads it.
    rows(read(root, orders, planned).rows.select("id")) shouldBe Seq(Row("1"), Row("2"))
  }

  test("a failure of a batch that no file has alone is let through as it was") {
    val root = landingZone()
    val orders = source("parquet", policy = "skip")
    val session = spark
    import session.implicits._
    dropParquet(root, "a.parquet", Seq((1L, "ann")).toDF("id", "name"))
    dropParquet(root, "b.parquet", Seq(("2", "bo")).toDF("id", "name"))
    val planned = plan(root, orders).value
    val (reading, reads) = counting(root)

    val failed =
      the[SparkException] thrownBy reading.read(
        orders,
        configOf(orders),
        planned,
        ReadContext(spark)
      )
    failed.getCondition shouldBe "CANNOT_MERGE_SCHEMAS"
    reads.toMap shouldBe Map("a.parquet" -> 1, "b.parquet" -> 1)
  }

  test("a file that changes while the files are read alone makes it a batch whose source changed") {
    val root = landingZone()
    val orders = source("csv", policy = "skip")
    val a = drop(root, "a.csv", "id\n1\n")
    cutShort(dropGzip(root, "b.csv.gz", rows = 20000))
    val planned = plan(root, orders).value
    val changing = connector(
      root,
      beforeAlone =
        (name, _) => if (name == "b.csv.gz") Files.write(a, "id\n1\n2\n".getBytes(UTF_8)): Unit
    )

    val changed = changedUnder(changing.read(orders, configOf(orders), planned, ReadContext(spark)))
    changed.summary should include("changed while its files were read alone")
    changed.changed.map(_.item) shouldBe Seq("a.csv")
  }

  test("a run that has lost its lease reads no file alone") {
    val root = landingZone()
    val orders = source("csv", policy = "skip")
    drop(root, "a.csv", "id\n1\n")
    cutShort(dropGzip(root, "b.csv.gz", rows = 20000))
    val planned = plan(root, orders).value
    val (reading, reads) = counting(root)
    val lost = new IllegalStateException("the lease is lost")

    val stopped = the[IllegalStateException] thrownBy reading.read(
      orders,
      configOf(orders),
      planned,
      ReadContext(spark, verify = () => throw lost)
    )
    stopped shouldBe theSameInstanceAs(lost)
    reads shouldBe empty
  }

  test(
    "under skip, a link out of the landing zone is planned, said, and nothing of its plan read"
  ) {
    val root = landingZone()
    val orders = source("csv", policy = "skip")
    drop(root, "a.csv", "id\n1\n")
    val outside = Files.write(workDir.resolve(s"outside-$count.csv"), "id\n9\n".getBytes(UTF_8))
    val link = Files.createSymbolicLink(root.resolve("shop/orders/b.csv"), outside)
    // The link itself last changed an hour ago, as the file that it leads to did: complete.
    Files
      .getFileAttributeView(link, classOf[BasicFileAttributeView], LinkOption.NOFOLLOW_LINKS)
      .setTimes(FileTime.from(anHourAgo), null, null)
    val planned = plan(root, orders).value
    FilePlan.fromJson(planned).value.names.map(_.value) shouldBe Vector("a.csv", "b.csv")
    var read = false

    val cannot = skipped(
      connector(root, afterRead = () => read = true)
        .read(orders, configOf(orders), planned, ReadContext(spark))
    )
    cannot.unreadable shouldBe Seq(
      UnreadableFilesException.Unreadable(
        "b.csv",
        "path_outside_root: it leads out of the landing zone, through a symbolic link, and was " +
          "not opened"
      )
    )
    cannot.summary should not include outside.toString
    read shouldBe false
    // Under fail, the same directory plans nothing, as it always did.
    refused(plan(root, source("csv"))) should include("'b.csv' leads out of the landing zone")
  }

  // ---------------------------------------------------------------------------------------------
  // A file whose bytes are not of its encoding (ADR-0024 §4 to §6)
  // ---------------------------------------------------------------------------------------------

  private val Mark = Array(0xef, 0xbb, 0xbf).map(_.toByte)

  /** A batch of three files of CSV said to be UTF-8: one that is, and two with a byte that is not. */
  private def notAllUtf8(root: Path): Unit = {
    drop(root, "a.csv", "id,name\n1,Peña\n")
    dropBytes(root, "b.csv", bytesOf("id,name\n1,ann\n2,ni", 0xf1, "o\n"))
    dropBytes(root, "c.csv.gz", bytesOf("id,name\n3,Ren", 0xe9, "\n"), gzip = true)
    ()
  }

  private val NotUtf8InB =
    "not_of_its_encoding: the byte at offset 18 of the file, in line 3, is not of UTF-8"
  private val NotUtf8InC =
    "not_of_its_encoding: the byte at offset 13 of what the file holds, decompressed, in " +
      "line 2, is not of UTF-8"

  test("a file with a byte that is not of its encoding fails its batch, named with where it is") {
    val root = landingZone()
    notAllUtf8(root)

    val message = refused(ingest(root, source("csv")))
    message should include("the batch of shop/orders has 2 files that cannot be read")
    message should include(s"'b.csv' $NotUtf8InB")
    message should include(s"'c.csv.gz' $NotUtf8InC")
    message should include("connector.on_unreadable_file: fail")
    message should include(
      "correct connector.csv.encoding and give the batch up (abandon-batch)"
    )
    // Where the byte is, and never what the file holds.
    message should not include "ni"
    message should not include "Ren"
    message should not include "�"
  }

  test("under skip, the files that are not of their encoding are said, with what to do") {
    val root = landingZone()
    notAllUtf8(root)

    val cannot = skipped(ingest(root, source("csv", policy = "skip")))
    // Each says what to do, as its record keeps it and as its warning says it.
    val toDo = "To have it taken again, correct connector.csv.encoding, if it is the encoding " +
      "of the source that is wrong, and touch the file (touch)"
    cannot.unreadable shouldBe Seq(
      UnreadableFilesException.Unreadable("b.csv", s"$NotUtf8InB. $toDo"),
      UnreadableFilesException.Unreadable("c.csv.gz", s"$NotUtf8InC. $toDo")
    )
    cannot.summary should include("2 files of the batch of shop/orders cannot be read")
    // A batch that is given up is not one to give up, and no reprocess takes its files.
    cannot.summary should not include "abandon-batch"
    cannot.summary should not include "reprocess"
  }

  test("under ISO-8859-1, a byte of 0x80 to 0x9F is not of it, and the read says windows-1252") {
    val sheet = bytesOf("id;importe\r\n1;10 ", 0x80, "\r\n")
    val root = landingZone()
    dropBytes(root, "a.csv", sheet)

    val message =
      refused(ingest(root, source("csv", csv = """delimiter: ";", encoding: ISO-8859-1""")))
    message should include(
      "'a.csv' not_of_its_encoding: the byte at offset 17 of the file, in line 2, is a control " +
        "character of ISO-8859-1 (0x80 to 0x9F), which a text does not have: the file may be " +
        "of windows-1252"
    )
    // Which it is.
    val read = ingest(root, source("csv", csv = """delimiter: ";", encoding: windows-1252"""))
    rows(read.rows.select("id", "importe")) shouldBe Seq(Row("1", "10 €"))
  }

  test("the mark of UTF-8 in a file whose source says an encoding of one byte is not of it") {
    Seq("ISO-8859-1", "windows-1252").foreach { encoding =>
      val root = landingZone()
      dropBytes(root, "a.csv", bytesOf(Mark, "id,name\n1,ann\n"))

      withClue(encoding) {
        refused(ingest(root, source("csv", csv = s"encoding: $encoding"))) should include(
          "'a.csv' not_of_its_encoding: it starts with the mark of the order of bytes of " +
            s"UTF-8, and its source says $encoding: it is a file of UTF-8"
        )
      }
    }
  }

  test("a U+FFFD that the source wrote is read, and a byte that windows-1252 has not is not") {
    val written = landingZone()
    dropBytes(written, "a.csv", bytesOf("id,name\n1,a", 0xef, 0xbf, 0xbd, "b\n"))
    rows(ingest(written, source("csv")).rows.select("id", "name")) shouldBe
      Seq(Row("1", "a�b"))

    val undefined = landingZone()
    dropBytes(undefined, "a.csv", bytesOf("id\nx", 0x81, "\n"))
    refused(ingest(undefined, source("csv", csv = "encoding: windows-1252"))) should include(
      "'a.csv' not_of_its_encoding: the byte at offset 4 of the file, in line 2, is not of " +
        "windows-1252"
    )
  }

  test("a file of JSON is of UTF-8, and one with a byte that is not cannot be read") {
    val why =
      "not_of_its_encoding: the byte at offset 22 of the file, in line 2, is not of UTF-8"
    def files(root: Path): Unit = {
      drop(root, "a.json", "{\"id\":\"1\",\"name\":\"Peña\"}\n")
      dropBytes(root, "b.json", bytesOf("{\"id\":\"2\"}\n{\"name\":\"ni", 0xf1, "o\"}\n"))
      ()
    }
    val failing = landingZone()
    files(failing)
    val message = refused(ingest(failing, source("json")))
    message should include(s"'b.json' $why")
    message should include("JSON is read as UTF-8")
    message should not include "connector.csv.encoding"

    val skipping = landingZone()
    files(skipping)
    val cannot = skipped(ingest(skipping, source("json", policy = "skip")))
    cannot.unreadable shouldBe Seq(
      UnreadableFilesException.Unreadable(
        "b.json",
        s"$why. JSON is read as UTF-8: the file is to be written again, in UTF-8"
      )
    )
  }

  test("a header that is not the others' for the encoding of its file is said by that file") {
    def files(root: Path): Unit = {
      drop(root, "a.csv", "id,año\n1,x\n")
      dropBytes(root, "b.csv", bytesOf("id,a", 0xf1, "o\n2,y\n"))
      ()
    }
    val why = "not_of_its_encoding: the byte at offset 4 of the file, in line 1, is not of UTF-8"
    val failing = landingZone()
    files(failing)
    val message = refused(ingest(failing, source("csv")))
    message should include(s"'b.csv' $why")
    message should not include "do not all have one header"

    // And it is the source's to say what becomes of it.
    val skipping = landingZone()
    files(skipping)
    val one = skipped(ingest(skipping, source("csv", policy = "skip"))).unreadable.loneElement
    one.item shouldBe "b.csv"
    one.why should startWith(s"$why. To have it taken again")
  }

  test("a name of a header with a NUL, or that starts with a second mark, fails its batch") {
    def refusedOf(bytes: Array[Byte], csv: String = ""): String = {
      val root = landingZone()
      dropBytes(root, "a.csv", bytes)
      refused(ingest(root, source("csv", csv = csv)))
    }
    val utf16 = "id,v\n1,x\n".getBytes("UTF-16LE")
    val nul = "a name with a NUL (U+0000)"

    // A file with nothing but a mark, and one of UTF-16 with no mark, whose bytes are of UTF-8.
    refusedOf(Mark) should include(nul)
    refusedOf(utf16) should include(nul)
    // With its mark, a file of UTF-16 is not of UTF-8, and that is what is said.
    refusedOf(bytesOf(0xff, 0xfe, utf16)) should include(
      "'a.csv' not_of_its_encoding: the byte at offset 0 of the file, in line 1, is not of " +
        "UTF-8: the file starts with the mark of the order of bytes of UTF-16"
    )
    // Under an encoding of one byte its mark is two characters, and its names have NULs.
    refusedOf(bytesOf(0xff, 0xfe, utf16), "encoding: windows-1252") should include(nul)

    // Spark takes one mark off, and the second would be the start of the first name.
    refusedOf(bytesOf(Mark, Mark, "id,name\n1,ann\n")) should include(
      "a name that starts with the mark of the order of bytes (U+FEFF)"
    )
  }

  test("a file that changes before its bytes are looked at is a batch whose source changed") {
    val root = landingZone()
    val orders = source("csv")
    val a = drop(root, "a.csv", "id\n1\n")
    val planned = plan(root, orders).value

    // What it holds by then is not of UTF-8; and it is not the file that was planned.
    val changing = connector(
      root,
      afterRead = () => {
        Files.write(a, bytesOf("id\n", 0xf1, "\n2\n"))
        ()
      }
    )
    val changed =
      changedUnder(changing.read(orders, configOf(orders), planned, ReadContext(spark)))
    changed.summary should include("1 file of it changed while it was read")
    changed.changed.map(_.item) shouldBe Seq("a.csv")
  }

  test("bytes that cannot be looked at, with no file changed, are a failure that passes") {
    val root = landingZone()
    val orders = source("csv")
    val a = drop(root, "a.csv", "id\n1\n")
    unopenable(a)(())
    val planned = plan(root, orders).value
    val permissions = Files.getPosixFilePermissions(a)

    // Spark read it, and it may not be opened by the time its bytes are looked at.
    val closing = connector(
      root,
      afterRead = () => {
        Files.setPosixFilePermissions(a, PosixFilePermissions.fromString("---------"))
        ()
      }
    )
    val failure =
      try
        the[RunFailedException] thrownBy
          closing.read(orders, configOf(orders), planned, ReadContext(spark))
      finally Files.setPosixFilePermissions(a, permissions): Unit
    failure.failure shouldBe FailureClass.Transient
    failure.getMessage should include("their bytes could not be looked at")
    failure.getMessage should include("with no file changed")
    failure.getMessage should not include "Permission denied"
  }

  test("a read refused as permanent stays so, when the bytes of its files cannot be looked at") {
    val root = landingZone()
    val orders = source("csv")
    drop(root, "a.csv", "id,name\n1,ann\n")
    val b = drop(root, "b.csv", "id,nombre\n2,bea\n")
    unopenable(b)(())
    val planned = plan(root, orders).value
    val permissions = Files.getPosixFilePermissions(b)

    // Spark refused the header of b, which may not be opened by the time the bytes are looked
    // at: the look says nothing of the files, and the failure of the read is what is known.
    val closing = connector(
      root,
      afterRead = () => {
        Files.setPosixFilePermissions(b, PosixFilePermissions.fromString("---------"))
        ()
      }
    )
    val failure =
      try
        the[RunFailedException] thrownBy
          closing.read(orders, configOf(orders), planned, ReadContext(spark))
      finally Files.setPosixFilePermissions(b, permissions): Unit
    failure.failure shouldBe FailureClass.Permanent
    failure.getMessage should include("do not all have one header")
    // What the look met goes with it, for whoever reads the failure whole.
    failure.getSuppressed.toSeq should have size 1
  }

  // A header refused, beside a file that cannot be read from the start: the look at the bytes,
  // after the refusal, meets the file as the read did not, and is no reason to try again.
  Seq[(String, Path => Path)](
    "a file in gzip cut short" -> (root => cutShort(dropGzip(root, "c.csv.gz", rows = 2000))),
    "a file that may not be opened" -> (root => drop(root, "c.csv", "id,name\n3,cy\n"))
  ).foreach { case (what, broken) =>
    test(s"a header that is not the others', beside $what, fails its batch as permanent") {
      val root = landingZone()
      val orders = source("csv")
      // Two files with headers that differ, each bigger than the broken one.
      drop(root, "a.csv", "id,name\n" + (1 to 10000).map(i => s"$i,ann").mkString("\n"))
      drop(root, "b.csv", "id,nombre\n" + (1 to 20000).map(i => s"$i,bea").mkString("\n"))
      val c = broken(root)
      val planned = plan(root, orders).value
      def reading = connector(root).read(orders, configOf(orders), planned, ReadContext(spark))

      // Spark reads the files in one task, the biggest first, so that it is the header that
      // stops the read, as it does when its task fails first: on two cores, the broken file is
      // read beside it, and which of the two Spark says is a race.
      val OnePartition = "spark.sql.files.minPartitionNum"
      spark.conf.set(OnePartition, "1")
      val failure =
        try
          if (what.contains("opened")) unopenable(c)(the[RunFailedException] thrownBy reading)
          else the[RunFailedException] thrownBy reading
        finally spark.conf.unset(OnePartition)
      withClue(failure.getMessage) {
        failure.failure shouldBe FailureClass.Permanent
        failure.getMessage should include("do not all have one header")
      }
      failure.getSuppressed.toSeq should have size 1
    }
  }

  test("a run that has lost its lease does not look at the bytes of its files") {
    val root = landingZone()
    val orders = source("csv")
    // A file that would fail its batch, if its bytes were looked at.
    dropBytes(root, "a.csv", bytesOf("id\n", 0xf1, "\n"))
    val planned = plan(root, orders).value
    val lost = new IllegalStateException("the lease is lost")

    val stopped = the[IllegalStateException] thrownBy connector(root).read(
      orders,
      configOf(orders),
      planned,
      ReadContext(spark, verify = () => throw lost)
    )
    stopped shouldBe theSameInstanceAs(lost)
  }

  // ---------------------------------------------------------------------------------------------
  // Plans that are not this source's
  // ---------------------------------------------------------------------------------------------

  test("a plan of another form, or of another directory, is refused") {
    val root = landingZone()
    val orders = source("csv")
    drop(root, "a.csv", "id\n1\n")
    val planned = plan(root, orders).value

    refused(read(root, orders, planned.mapObject(_.add("version", Json.fromInt(4))))) should
      include("is not one that this application reads: it is of version 4")
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
        .progress(orders, configOf(orders), planned.mapObject(_.add("version", Json.fromInt(4))))
    ) should include("is not one that this application reads: it is of version 4")
    refused(
      connector(root).progress(
        orders,
        configOf(orders),
        planned.mapObject(_.add("directory", Json.fromString("shop/returns")))
      )
    ) should include("is of the directory shop/returns, and the source reads shop/orders")
  }
}
