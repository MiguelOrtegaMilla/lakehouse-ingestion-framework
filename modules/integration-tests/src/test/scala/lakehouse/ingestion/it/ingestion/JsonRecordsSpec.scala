package lakehouse.ingestion.it.ingestion

import java.nio.file.Files

import lakehouse.ingestion.core.config.Source
import lakehouse.ingestion.core.control.{FailureClass, RunCounts, RunStatus}
import lakehouse.ingestion.engine.IngestionService.Outcome
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The records of a source of JSON, each read as it was written (P8.8), from its file to Bronze
 * and to its quarantine: on real files, real Delta on the coordinated LogStore, and a real
 * control plane. A record with text after its object and one with a field twice at its first
 * level are set apart with why, as is a line that is no whole object, and count against the
 * share that the source allows. Each test ends with the ledger of the source.
 */
final class JsonRecordsSpec extends AnyFunSuite with Matchers with IngestionSuite {

  /** What the quarantine of a source has: the type, the detail and the record, of each. */
  private def apart(source: Source): Seq[(String, String, String)] =
    spark.read
      .format("delta")
      .load(quarantine(source).uri)
      .collect()
      .map(row =>
        (
          row.getAs[String]("error_type"),
          row.getAs[String]("error_detail"),
          row.getAs[String]("raw_record")
        )
      )
      .sortBy(_.toString)
      .toSeq

  private val written = Seq(
    """{"id":"1","amount":10.50}""",
    """{"id":"2"} {"id":"3"}""",
    """{"id":"4","id":"5"}""",
    """{"id":"6","customer":{"name":"a","name":"b"}}""",
    """[{"id":"7"},{"id":"8"}]""",
    "",
    "{",
    """  "id": "9"""",
    "}"
  )

  test("records of JSON not as they were written are set apart with why, and the rest ingested") {
    val of = fileSource("json", rejected = Some("0.9"))
    val file = drop(of, "a.json", written.mkString("", "\n", "\n"))

    val done = ingested(launch(of))

    rowsOf(of).map(row => row._1 -> row._3) shouldBe Seq("1" -> "a.json", "6" -> "a.json")
    spark.read
      .format("delta")
      .load(bronze(of).uri)
      .select("amount", "customer")
      .collect()
      .map(row => (Option(row.getString(0)), Option(row.getString(1))))
      .toSet shouldBe Set(
      (Some("10.50"), None),
      (None, Some("""{"name":"a","name":"b"}"""))
    )
    apart(of) shouldBe Seq(
      (
        "duplicate_field",
        "the field 'id' twice, at the first level of the record",
        """{"id":"4","id":"5"}"""
      ),
      ("trailing_text", "text after the object, from column 12", """{"id":"2"} {"id":"3"}"""),
      ("unparsed", null, """  "id": "9""""),
      ("unparsed", null, """[{"id":"7"},{"id":"8"}]"""),
      ("unparsed", null, "{"),
      ("unparsed", null, "}")
    )
    // Each line that was a record or was set apart is read; the blank line is no record.
    done.counts shouldBe RunCounts(
      rowsRead = Some(8),
      rowsWritten = Some(2),
      rowsRejected = Some(6),
      bytesRead = Some(Files.size(file)),
      writeConflicts = Some(0),
      commitContention = Some(0)
    )
    assertLedger(of)
  }

  test("records set apart count against the share that the source allows: 5, nothing written") {
    val of = fileSource("json", rejected = Some("0.25"))
    drop(
      of,
      "a.json",
      Seq("""{"id":"1"}""", """{"id":"2"} x""", """{"id":"3","id":"3"}""").mkString("", "\n", "\n")
    )

    launch(of) match {
      case Outcome.Failed(run, FailureClass.Data, message) =>
        message should include("2 of the 3 records of ")
        message should include("were set apart")
        runOf(run).status shouldBe RunStatus.Failed
      case other => fail(s"the run did not fail for its data: $other")
    }
    tableIsThere(of) shouldBe false
    apart(of).map(_._1) shouldBe Seq("duplicate_field", "trailing_text")
    batchesOf(of) shouldBe Seq(1L -> "PLANNED")
    assertLedger(of)
  }

  test("a source of JSON with fields that differ only in their case still fails, as permanent") {
    // The case of names is the policy of a schema (P9): set apart, a record would hide it.
    val of = fileSource("json")
    drop(of, "a.json", """{"id":"1"}""" + "\n" + """{"ID":"2"}""" + "\n")

    launch(of) match {
      case Outcome.Failed(_, FailureClass.Permanent, message) =>
        message should include("'ID' and 'id'")
      case other => fail(s"the run did not fail as permanent: $other")
    }
    assertLedger(of)
  }
}
