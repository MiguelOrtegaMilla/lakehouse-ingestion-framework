package lakehouse.ingestion.it.quarantine

import java.net.URI
import java.nio.file.{Files, Path => LocalPath, Paths}
import java.sql.Timestamp
import java.time.Instant

import io.delta.tables.DeltaTable
import lakehouse.ingestion.core.config.{PathTableLocator, RelativePath, RootUri, SourceId}
import lakehouse.ingestion.core.config.TableLocation
import lakehouse.ingestion.core.control.{BatchId, BatchSeq, Layer, Quarantine, RunId, TxnAppId}
import lakehouse.ingestion.engine.bronze.{BronzeBatch, BronzeWriter}
import lakehouse.ingestion.engine.delta.BatchAppend.{AlreadyThere, Appended}
import lakehouse.ingestion.engine.delta.{CommitMetadata, DeltaTableTransactions, Lake}
import lakehouse.ingestion.engine.quarantine.{QuarantineBatch, QuarantineWriter}
import lakehouse.ingestion.it.logstore.CoordinatedDeltaSuite
import lakehouse.ingestion.logstore.CommitContext
import org.apache.spark.sql.types.{LongType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{EitherValues, OptionValues}

/**
 * The writer of a quarantine on real tables, through the coordinated LogStore (P5.9): what is
 * kept of a record that was rejected, that the records of a batch are written once, and that a
 * table under the directory of the lake that is the engine's own is a table like any other.
 */
final class QuarantineWriterSpec
    extends AnyFunSuite
    with Matchers
    with OptionValues
    with EitherValues
    with CoordinatedDeltaSuite {

  private lazy val lakeDirectory: LocalPath = Files.createDirectories(workDir.resolve("lake"))
  private lazy val root = RootUri.parse(s"file://$lakeDirectory").value
  private lazy val locator = new PathTableLocator(root)
  private lazy val lake = new Lake(root, spark.sessionState.newHadoopConf())
  private lazy val delta = new DeltaTableTransactions(spark)

  private val setApartAt = Instant.parse("2026-11-19T10:15:30.123456Z")

  private var count = 0

  /** A source of its own for each test, and so a quarantine of its own. */
  private def newSource(): SourceId = {
    count += 1
    SourceId.parse(s"shop.returns_$count").value
  }

  private def quarantineOf(source: SourceId): TableLocation =
    locator.locate(Quarantine.table(source, Layer.Bronze))

  private def batch(source: SourceId, seq: Long, run: RunId = RunId.generate()): QuarantineBatch =
    QuarantineBatch(
      run,
      source,
      BatchId(TxnAppId.of(source, Layer.Bronze), BatchSeq.of(seq).value),
      setApartAt
    )

  private def write(batch: QuarantineBatch, records: DataFrame) =
    new QuarantineWriter(lake, delta, CommitContext.forRun(batch.run.uuid))
      .write(quarantineOf(batch.source), batch, records)

  /** Records as a read sets them apart: the file of each, and its text. */
  private def unparsed(records: (String, String)*): DataFrame = {
    val session = spark
    import session.implicits._
    records.toDF("_source_file", "_record")
  }

  private def read(table: TableLocation): DataFrame = spark.read.format("delta").load(table.uri)

  test("a record is kept as it was, with its file, its batch, the run, and why it is there") {
    val source = newSource()
    val first = batch(source, 1)
    val table = quarantineOf(source)

    write(
      first,
      unparsed("returns/a.csv" -> "3", "returns/b.json" -> """{"id": 7, "broken""")
    ) shouldBe Appended(0, conflicts = 0, rows = Some(2))

    val written = read(table)
    written.schema.fields.map(field => field.name -> field.dataType).toSeq shouldBe Seq(
      "run_id" -> StringType,
      "batch_seq" -> LongType,
      "source_file" -> StringType,
      "error_type" -> StringType,
      "error_detail" -> StringType,
      "raw_record" -> StringType,
      "quarantined_at" -> TimestampType
    )
    written.schema.fieldNames.toSeq shouldBe Quarantine.Columns
    val at = Timestamp.from(setApartAt)
    written.orderBy("source_file").collect().toSeq shouldBe Seq(
      Row(first.run.toString, 1L, "returns/a.csv", "unparsed", null, "3", at),
      Row(first.run.toString, 1L, "returns/b.json", "unparsed", null, """{"id": 7, "broken""", at)
    )

    // The commit says its batch, the table has the transaction of the batch, and the LogStore
    // has the run in the entry of the commit: as a commit to Bronze.
    DeltaTable
      .forPath(spark, table.uri)
      .history(1)
      .select("userMetadata")
      .head()
      .getString(0) shouldBe CommitMetadata(first.run, source, first.id).text
    delta.lastVersion(table, first.id.app) shouldBe Some(1)
    delta.committedAt(table, first.id) shouldBe Some(0)
    entry(table.uri, 0).runId shouldBe Some(first.run.uuid)
  }

  test("a record that a read says more of is kept with why: its type, and what more it knows") {
    val source = newSource()
    val first = batch(source, 1)
    val session = spark
    import session.implicits._
    val records = Seq(
      ("in/a.json", """{"id":"1","id":"2"}""", "duplicate_field", "the field 'id' twice"),
      ("in/a.json", """{"id":"3"} x""", "trailing_text", "text after the object, from column 12"),
      ("in/a.json", "not json", "unparsed", null)
    ).toDF("_source_file", "_record", "_error_type", "_error_detail")

    write(first, records) shouldBe Appended(0, conflicts = 0, rows = Some(3))

    read(quarantineOf(source))
      .select("error_type", "error_detail", "raw_record")
      .orderBy("error_type")
      .collect()
      .toSeq shouldBe Seq(
      Row("duplicate_field", "the field 'id' twice", """{"id":"1","id":"2"}"""),
      Row("trailing_text", "text after the object, from column 12", """{"id":"3"} x"""),
      Row("unparsed", null, "not json")
    )
    // The types are those of the model, for whoever reads the quarantine.
    Quarantine.ErrorType.All shouldBe Seq("unparsed", "trailing_text", "duplicate_field")
  }

  test("the quarantine of a source is under the engine's own directory of the lake") {
    val source = newSource()
    val table = quarantineOf(source)

    write(batch(source, 1), unparsed("returns/a.csv" -> "3")) shouldBe Appended(
      0,
      conflicts = 0,
      rows = Some(1)
    )

    // A directory whose name starts with `_`, which no source can name as a target, holds a
    // table that Delta writes, reads and lists as any other.
    table.uri shouldBe s"${root.value}/_quarantine/bronze/${source.value}"
    RelativePath.parse(s"_quarantine/bronze/${source.value}").isLeft shouldBe true
    val directory = Paths.get(URI.create(table.uri))
    Files.isDirectory(directory.resolve("_delta_log")) shouldBe true
    directory.getParent.getParent shouldBe lakeDirectory.resolve("_quarantine")
    read(table).count() shouldBe 1L
    spark.sql(s"SELECT raw_record FROM delta.`${table.uri}`").head().getString(0) shouldBe "3"
    write(batch(source, 2), unparsed("returns/b.csv" -> "4")) shouldBe Appended(
      1,
      conflicts = 0,
      rows = Some(1)
    )
    read(table).count() shouldBe 2L
    DeltaTable.forPath(spark, table.uri).history().count() shouldBe 2L
  }

  test("the records of a batch are written once, whoever writes them again") {
    val source = newSource()
    val first = batch(source, 1)
    val records = unparsed("returns/a.csv" -> "3")
    write(first, records) shouldBe Appended(0, conflicts = 0, rows = Some(1))

    // The run that replays the batch, and the one that should have stopped.
    write(batch(source, 1), records) shouldBe AlreadyThere(Some(0))
    write(first, unparsed("returns/a.csv" -> "something else")) shouldBe AlreadyThere(Some(0))

    read(quarantineOf(source)).select("raw_record", "run_id").collect().toSeq shouldBe
      Seq(Row("3", first.run.toString))
    versionOf(quarantineOf(source).uri) shouldBe 0L
  }

  test("a text is kept whatever it holds: no value of it is read as anything") {
    val source = newSource()
    val texts = Seq(
      "",
      " ",
      "a,b,\"c",
      "tab\there",
      "ñandú, 数据, 😀",
      "two\nlines",
      "'; DROP TABLE quarantine; --",
      "x" * 100000
    )

    write(
      batch(source, 1),
      unparsed(texts.zipWithIndex.map { case (text, i) => f"returns/$i%02d.csv" -> text }: _*)
    )

    read(quarantineOf(source))
      .orderBy("source_file")
      .select("raw_record")
      .collect()
      .map(_.getString(0))
      .toSeq shouldBe texts
  }

  test("the quarantine and the data of a batch are two tables, each with the batch once") {
    // One sequence, and each table keeps its own place in it (ADR-0005 §2): a batch whose
    // quarantine is written is not one whose data is.
    val source = newSource()
    val first = batch(source, 1)
    val data = locator.locate(RelativePath.parse(s"bronze/${source.value}").value)
    write(first, unparsed("returns/a.csv" -> "3")) shouldBe Appended(
      0,
      conflicts = 0,
      rows = Some(1)
    )

    delta.lastVersion(quarantineOf(source), first.id.app) shouldBe Some(1)
    delta.lastVersion(data, first.id.app) shouldBe None

    val session = spark
    import session.implicits._
    new BronzeWriter(lake, delta, CommitContext.forRun(first.run.uuid)).write(
      data,
      BronzeBatch(first.run, source, first.id, setApartAt),
      Seq("1").toDF("id")
    ) shouldBe Appended(0, conflicts = 0, rows = Some(1))
    delta.lastVersion(data, first.id.app) shouldBe Some(1)
    read(quarantineOf(source)).count() shouldBe 1L
    read(data).count() shouldBe 1L
  }
}
