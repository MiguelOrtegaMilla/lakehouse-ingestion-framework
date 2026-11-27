package lakehouse.ingestion.it.ingestion

import java.io.ByteArrayOutputStream
import java.nio.file.{Files, Path}
import java.nio.file.attribute.FileTime

import lakehouse.ingestion.core.config.{ConnectorKind, Source}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.FaultPoint
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.connector.UnreadableFilesException.Unreadable
import lakehouse.ingestion.engine.files.FileSourceConnector
import lakehouse.ingestion.engine.operator.OperatorActions
import lakehouse.ingestion.it.support.{CapturedLogs, Died}
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * A source of CSV in a dialect and an encoding of its own (ADR-0024), from end to end: real
 * files, real Delta on the coordinated LogStore and a real control plane.
 *
 *  - '''A batch is read as it was planned.''' A run that dies once its records set apart are
 *    written leaves its batch to the next, which reads it as it was planned, whatever the source
 *    says by then: Delta takes the records set apart once, so a read in another dialect would
 *    have them of one read and the rows of Bronze of another.
 *  - '''A batch whose dialect was wrong is read in the right one once it is given up.'''
 *  - '''Files that PostgreSQL's `COPY` writes''', a producer that the engine does not make, in
 *    each encoding and in a dialect of its own, are in Bronze as the table has them.
 *  - '''A file whose bytes are not of the encoding of its source cannot be read''' (§4): it
 *    stops its source, or is skipped and recorded, as the source says, and never reaches Bronze
 *    with U+FFFD for what it held.
 *
 * Each test ends with the ledger of the source.
 */
final class CsvDialectSpec
    extends AnyFunSuite
    with Matchers
    with OptionValues
    with JdbcIngestionSuite {

  private lazy val actions =
    new OperatorActions(database, spark, lakeRoot, timing, retries, ConnectorKind.BuiltIn)

  private val asked = OperatorRequest.of("ops.oncall", "the source writes ';'").toOption.get

  /** The same source, with another dialect: as an operator changes its configuration. */
  private def withDialect(of: Source, dialect: String, rejected: Option[String] = None): Source =
    fileSource(options = s", csv: { $dialect }", rejected = rejected, id = of.id.value)

  /** A file of the source with these bytes, changed an hour ago. */
  private def dropBytes(of: Source, name: String, bytes: Array[Byte]): Path = {
    val path = directoryOf(of).resolve(name)
    Files.write(path, bytes)
    Files.setLastModifiedTime(path, FileTime.from(anHourAgo))
    path
  }

  /** The rows of Bronze, under the columns given, by the first of them. */
  private def bronzeOf(of: Source, columns: String*): Seq[Seq[String]] =
    spark.read
      .format("delta")
      .load(bronze(of).uri)
      .select(columns.head, columns.tail: _*)
      .collect()
      .map(row => columns.indices.map(row.getString))
      .toSeq
      .sortBy(_.head)

  private def columnsOf(of: Source): Set[String] =
    spark.read.format("delta").load(bronze(of).uri).columns.toSet

  private def said(body: => Outcome): (Outcome, Seq[String]) = {
    val (outcome, logged) = CapturedLogs.of(classOf[FileSourceConnector])(body)
    outcome -> logged.map(_.message)
  }

  private val AsPlanned = "the batch is read as it was planned"

  test("a batch whose run died after its records set apart were written is read as planned") {
    val orders = fileSource(rejected = Some("0.5"))
    // Of commas, two records and one with a value too many. Read with `;`, three records of
    // one column, `id,name`, and none set apart.
    drop(orders, "a.csv", "id,name\n1,ann\n2,bea\n3,cy,extra\n")
    intercept[Died](
      launch(orders, service(hookAt(FaultPoint.AfterQuarantineWrite)(_ => throw new Died)))
    )
    expire(orders)
    quarantinedOf(orders) shouldBe Seq((1L, "a.csv", "3,cy,extra"))

    val semicolons = withDialect(orders, """delimiter: ";"""", rejected = Some("0.5"))
    val (outcome, warnings) = said(launch(semicolons))

    val done = ingested(outcome)
    done.batch shouldBe batchId(orders, 1)
    done.counts.rowsWritten shouldBe Some(2)
    // The run that warns of it commits it: by the time the warning is read, the batch is no
    // longer one to give up, and what reads its files as the source says now is a reprocess.
    batchesOf(orders) shouldBe Seq(1L -> "COMMITTED")
    warnings.find(_.contains(AsPlanned)).value should include(
      "once the batch is committed, as the run that reads it commits it, reprocess it (reprocess)"
    )
    // The rows and the records set apart are of one read, the one that was planned.
    columnsOf(orders) should contain allOf ("id", "name")
    columnsOf(orders) should not contain "id,name"
    bronzeOf(orders, "id", "name") shouldBe Seq(Seq("1", "ann"), Seq("2", "bea"))
    quarantinedOf(orders) shouldBe Seq((1L, "a.csv", "3,cy,extra"))
    assertLedger(semicolons)
  }

  test("a batch stopped by a dialect that was wrong is read in the right one once given up") {
    val orders = fileSource()
    // Written with `;`, read with commas: a record with a value too many, and the source lets
    // none be rejected.
    drop(orders, "a.csv", "id;name\n1;a,b\n2;c\n")
    launch(orders) match {
      case Outcome.Failed(_, FailureClass.Data, _) =>
      case other => fail(s"the batch was let through: $other")
    }

    // The source is corrected; the batch is read as it was planned, and stopped again.
    val semicolons = withDialect(orders, """delimiter: ";"""")
    val (again, warnings) = said(launch(semicolons))
    again match {
      case Outcome.Failed(_, FailureClass.Data, _) =>
      case other => fail(s"the batch was read in the corrected dialect: $other")
    }
    warnings.exists(_.contains(AsPlanned)) shouldBe true
    batchOf(batchId(orders, 1)).status shouldBe BatchStatus.Planned

    // Given up, its files are planned again, read as the source says.
    actions.abandon(semicolons, versionOf(semicolons), BatchSeq.of(1).toOption.get, asked) shouldBe
      a[OperatorActions.Outcome.Done]
    val (read, after) = said(launch(semicolons))
    ingested(read).batch shouldBe batchId(orders, 2)
    after.exists(_.contains(AsPlanned)) shouldBe false
    bronzeOf(orders, "id", "name") shouldBe Seq(Seq("1", "a,b"), Seq("2", "c"))
    assertLedger(semicolons)
  }

  // ---------------------------------------------------------------------------------------------
  // Files that PostgreSQL's COPY writes
  // ---------------------------------------------------------------------------------------------

  /** What a table of the source database holds, with the delimiters, quotes and letters of any. */
  private def tableOfValues(table: String, euro: Boolean): Seq[Seq[String]] =
    asOwner { c =>
      execute(c, s"DROP TABLE IF EXISTS $table")
      execute(c, s"CREATE TABLE $table (id text PRIMARY KEY, name text, note text)")
      val rows = Seq(
        ("1", "Pe\u00f1a; Hijos, S.L.", "dice \"hola\"\ny adi\u00f3s"),
        ("2", "C:\\temp\\nuevo", "a\r\nb"),
        ("3", "O'Brien | \"Sons\"", "\ttab"),
        ("4", "M\u00fcller", "\\\"backslash and quote\\\"")
      ) ++ Option.when(euro)(("5", "10 \u20ac \u201cx\u201d", "\u2014")).toSeq
      val insert = c.prepareStatement(s"INSERT INTO $table VALUES (?, ?, ?)")
      rows.foreach { case (id, name, note) =>
        insert.setString(1, id)
        insert.setString(2, name)
        insert.setString(3, note)
        insert.executeUpdate()
      }
      rows.map { case (id, name, note) => Seq(id, name, note) }
    }

  /** What `COPY ... TO STDOUT` writes of a table, in the options given: the bytes of a file. */
  private def copied(table: String, options: String): Array[Byte] =
    asOwner { c =>
      val out = new ByteArrayOutputStream()
      new CopyManager(c.unwrap(classOf[BaseConnection]))
        .copyOut(s"COPY (SELECT * FROM $table ORDER BY id) TO STDOUT ($options)", out)
      out.toByteArray
    }

  test(
    "files that PostgreSQL's COPY writes, in each encoding, are in Bronze as the table has them"
  ) {
    Seq(
      // RFC 4180 in UTF-8, as COPY writes CSV by default.
      ("copy_utf8", "FORMAT csv, HEADER, ENCODING 'UTF8'", "", true),
      // `;`, an escape of its own, and the encoding of a spreadsheet.
      (
        "copy_win1252",
        "FORMAT csv, HEADER, DELIMITER ';', ESCAPE E'\\\\', ENCODING 'WIN1252'",
        """delimiter: ";", escape: "\\", encoding: windows-1252""",
        true
      ),
      // A tab, a quote of its own, and an encoding with no euro.
      (
        "copy_latin1",
        "FORMAT csv, HEADER, DELIMITER E'\\t', QUOTE '''', ENCODING 'LATIN1'",
        """delimiter: "\t", quote: "'", escape: "'", encoding: ISO-8859-1""",
        false
      )
    ).foreach { case (table, options, dialect, euro) =>
      withClue(s"$table: ") {
        val values = tableOfValues(table, euro)
        val source = if (dialect.isEmpty) fileSource() else withDialect(fileSource(), dialect)
        dropBytes(source, s"$table.csv", copied(table, options))

        ingested(launch(source)).counts.rowsRejected shouldBe Some(0)
        bronzeOf(source, "id", "name", "note") shouldBe values
        assertLedger(source)
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // A file whose bytes are not of the encoding of its source (ADR-0024 §4)
  // ---------------------------------------------------------------------------------------------

  private val Skip = ", on_unreadable_file: skip"

  private def permanent(outcome: Outcome): String = outcome match {
    case Outcome.Failed(run, FailureClass.Permanent, message) =>
      runOf(run).status shouldBe RunStatus.Failed
      message
    case other => fail(s"the run did not fail as permanent: $other")
  }

  test(
    "a file of COPY in windows-1252 under a source that says UTF-8 stops it, until it says so"
  ) {
    val values = tableOfValues("copy_said_utf8", euro = true)
    val orders = fileSource()
    dropBytes(
      orders,
      "copied.csv",
      copied("copy_said_utf8", "FORMAT csv, HEADER, ENCODING 'WIN1252'")
    )

    // The first letter that is not of ASCII is in its first record, the second line.
    val notUtf8 = "'copied.csv' not_of_its_encoding: the byte at offset 18 of the file, in " +
      "line 2, is not of UTF-8"
    val message = permanent(launch(orders))
    message should include("has 1 file that cannot be read, and nothing of it was written")
    message should include(notUtf8)
    message should include("correct connector.csv.encoding and give the batch up (abandon-batch)")
    // Nothing of what the table holds.
    values.flatten.filter(_.length > 3).foreach(value => message should not include value)
    batchesOf(orders) shouldBe Seq(1L -> "PLANNED")
    failedOf(orders) shouldBe empty

    // Every run does the same, and so does one of the source corrected: the batch is read as
    // it was planned.
    permanent(launch(orders)) should include(notUtf8)
    val corrected = withDialect(orders, "encoding: windows-1252")
    val (again, warnings) = said(launch(corrected))
    permanent(again) should include(notUtf8)
    warnings.exists(_.contains(AsPlanned)) shouldBe true
    batchesOf(orders) shouldBe Seq(1L -> "PLANNED")

    // Given up, its file is planned again, and read as the source says.
    actions.abandon(corrected, versionOf(corrected), BatchSeq.of(1).toOption.get, asked) shouldBe
      a[OperatorActions.Outcome.Done]
    val done = ingested(launch(corrected))
    done.batch shouldBe batchId(orders, 2)
    done.counts.rowsRejected shouldBe Some(0)
    bronzeOf(orders, "id", "name", "note") shouldBe values
    failedOf(orders) shouldBe empty
    assertLedger(corrected)
  }

  test(
    "under skip, a file that is not of its encoding is recorded with what to do, and the rest read"
  ) {
    val orders = fileSource(options = Skip)
    drop(orders, "a.csv", "id,name\n1,ann\n")
    // Of ISO-8859-1.
    val b = dropBytes(orders, "b.csv", "id,name\n2,Peña\n".getBytes("ISO-8859-1"))

    val (run, why) = launch(orders) match {
      case Outcome.FilesSkipped(run, batch, Seq(Unreadable(item, why))) =>
        item shouldBe "b.csv"
        batch shouldBe batchId(orders, 1)
        run -> why
      case other => fail(s"no file was skipped: $other")
    }
    why shouldBe
      "not_of_its_encoding: the byte at offset 12 of the file, in line 2, is not of UTF-8. To " +
      "have it taken again, correct connector.csv.encoding, if it is the encoding of the " +
      "source that is wrong, and touch the file (touch)"
    // The run did what its source says: the batch was given up for the file, which says why.
    runOf(run).status shouldBe RunStatus.Succeeded
    val givenUp = batchOf(batchId(orders, 1))
    givenUp.status shouldBe BatchStatus.Abandoned
    givenUp.abandoned.value.reason should include(s"'b.csv' $why")
    failedOf(orders) shouldBe Seq(("b.csv", 1L, why))

    // The next plan leaves it out and takes the rest: a file is not a record, and none was
    // rejected, under a source that lets none be.
    val rest = ingested(launch(orders))
    rest.batch shouldBe batchId(orders, 2)
    rest.counts.rowsRejected shouldBe Some(0)
    bronzeOf(orders, "id", "name") shouldBe Seq(Seq("1", "ann"))
    launch(orders) shouldBe a[Outcome.NothingNew]
    assertLedger(orders)

    // The source says its encoding, and the file is touched: it is taken, as a new file.
    val corrected =
      fileSource(options = s"$Skip, csv: { encoding: ISO-8859-1 }", id = orders.id.value)
    launch(corrected) shouldBe a[Outcome.NothingNew]
    Files.setLastModifiedTime(b, FileTime.from(anHourAgo.plusSeconds(60)))
    ingested(launch(corrected)).batch shouldBe batchId(orders, 3)
    bronzeOf(orders, "id", "name") shouldBe Seq(Seq("1", "ann"), Seq("2", "Peña"))
    // What failed stays recorded.
    failedOf(orders) shouldBe Seq(("b.csv", 1L, why))
    assertLedger(corrected)
  }

  test("a reprocess of a file that is not of the encoding that its source says now fails") {
    val orders = fileSource(options = s"$Skip, csv: { encoding: ISO-8859-1 }")
    dropBytes(orders, "a.csv", "id,name\n1,Peña\n".getBytes("ISO-8859-1"))
    ingested(launch(orders))

    // The source is said to be of UTF-8, which its file is not; and a reprocess skips nothing.
    val wrong = fileSource(options = Skip, id = orders.id.value)
    val message =
      permanent(service().reprocess(wrong, versionOf(wrong), BatchSeq.of(1).toOption.get, asked))
    message should include("a reprocess skips none of them")
    message should include(
      "'a.csv' not_of_its_encoding: the byte at offset 12 of the file, in line 2, is not of UTF-8"
    )
    batchesOf(orders) shouldBe Seq(1L -> "COMMITTED", 2L -> "PLANNED")
    failedOf(orders) shouldBe empty
    bronzeOf(orders, "id", "name") shouldBe Seq(Seq("1", "Peña"))

    // Its batch is given up, and the source goes on.
    actions.abandon(wrong, versionOf(wrong), BatchSeq.of(2).toOption.get, asked) shouldBe
      a[OperatorActions.Outcome.Done]
    launch(wrong) shouldBe a[Outcome.NothingNew]
    assertLedger(wrong)
  }
}
