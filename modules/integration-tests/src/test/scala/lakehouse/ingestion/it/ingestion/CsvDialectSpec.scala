package lakehouse.ingestion.it.ingestion

import java.io.ByteArrayOutputStream
import java.nio.file.{Files, Path}
import java.nio.file.attribute.FileTime

import lakehouse.ingestion.core.config.{ConnectorKind, Source}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.FaultPoint
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.files.FileSourceConnector
import lakehouse.ingestion.engine.operator.OperatorActions
import lakehouse.ingestion.it.support.{CapturedLogs, Died}
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection
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
 *
 * Each test ends with the ledger of the source.
 */
final class CsvDialectSpec extends AnyFunSuite with Matchers with JdbcIngestionSuite {

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
    warnings.exists(_.contains(AsPlanned)) shouldBe true
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
}
