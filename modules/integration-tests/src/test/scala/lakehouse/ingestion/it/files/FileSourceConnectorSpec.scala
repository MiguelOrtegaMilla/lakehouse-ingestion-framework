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

  // ---------------------------------------------------------------------------------------------
  // The plan
  // ---------------------------------------------------------------------------------------------

  test("a plan is the new, complete files, each with the file it is on the file system") {
    val root = landingZone()
    val orders = source("csv")
    val a = drop(root, "a.csv", "id\n1\n")
    drop(root, "b.csv", "id\n2\n")
    drop(root, "fresh.csv", "id\n3\n", at = now)

    val planned = FilePlan.fromJson(plan(root, orders).value).value
    planned.names.map(_.value) shouldBe Vector("a.csv", "b.csv")
    planned.planned.head.identity.inode shouldBe Files.getAttribute(a, "unix:ino")
    planned.planned.head.identity.device shouldBe Files.getAttribute(a, "unix:dev")

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
  // The files that were planned, and no other
  // ---------------------------------------------------------------------------------------------

  test("a file written again after its plan fails the read, and says what changed") {
    val root = landingZone()
    val orders = source("csv")
    val a = drop(root, "a.csv", "id\n1\n")
    val planned = plan(root, orders).value
    Files.write(a, "id\n1\n2\n".getBytes(UTF_8))

    val message = refused(read(root, orders, planned))
    message should include(
      "cannot be read as it was planned: 1 file of it changed since it was planned"
    )
    message should include("'a.csv' is of 7 bytes, and was of 5")
    message should include("nothing of it was written")
  }

  test("a file put in another's place, of the same size and time, fails the read") {
    val root = landingZone()
    val orders = source("csv")
    val a = drop(root, "a.csv", "id\n1\n")
    val planned = plan(root, orders).value
    // What a producer does that writes a file whole and renames it over the old one.
    val other = drop(root, ".a.csv.tmp", "id\n9\n")
    Files.move(other, a, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)

    val message = refused(read(root, orders, planned))
    message should include("'a.csv' is another file of the file system")
    message should not include "bytes"
  }

  test("a file that is gone, or a link that leads elsewhere now, fails the read") {
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
    val second = Files.copy(first, root.resolve("shared/second.csv"))
    Files.setLastModifiedTime(second, FileTime.from(anHourAgo))
    Files.createSymbolicLink(link, second)

    val message = refused(read(root, orders, planned))
    message should include("2 files of it changed")
    message should include("'a.csv' is not there any more")
    message should include("'b.csv' is another file of the file system")
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

    refused(read(root, orders, planned)) should include(
      "'x.csv' leads out of the landing zone"
    )
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
    val message = refused(changing.read(orders, configOf(orders), planned, ReadContext(spark)))
    read shouldBe true
    message should include("1 file of it changed while it was read")
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

  // ---------------------------------------------------------------------------------------------
  // Plans that are not this source's
  // ---------------------------------------------------------------------------------------------

  test("a plan of another form, or of another directory, is refused") {
    val root = landingZone()
    val orders = source("csv")
    drop(root, "a.csv", "id\n1\n")
    val planned = plan(root, orders).value

    refused(read(root, orders, planned.mapObject(_.add("version", Json.fromInt(1))))) should
      include("is not one that this application reads: it is of version 1")
    refused(
      read(root, orders, planned.mapObject(_.add("directory", Json.fromString("shop/returns"))))
    ) should include("is of the directory shop/returns, and the source reads shop/orders")
  }
}
