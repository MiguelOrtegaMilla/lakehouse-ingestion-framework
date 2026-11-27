package lakehouse.ingestion.cli

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import lakehouse.ingestion.core.config.SourceId
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.quarantine.QuarantineReader._
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * How what a quarantine has is written (P8.10b): a record as one line of JSON that a terminal
 * acts on no character of, and that reads back as it was; and the counts as a table. The lines
 * are read back by a reader of JSON that is not the application's (Jackson).
 */
final class QuarantineLinesSpec extends AnyFunSuite with Matchers with EitherValues {

  private val source = SourceId.parse("sales.returns").value
  private val run = RunId.generate()
  private val at = Instant.parse("2026-11-19T12:00:00Z")
  private val batch = BatchId(TxnAppId.of(source, Layer.Bronze), BatchSeq.of(3).value)
  private val committed = Some(Taken(batch, BatchStatus.Committed))

  private def record(raw: String, chars: Option[Long] = None): Record =
    Record(
      run,
      3L,
      committed,
      Some("sales_returns/a.csv"),
      "unparsed",
      None,
      at,
      raw,
      chars.getOrElse(raw.codePointCount(0, raw.length).toLong)
    )

  private def read(line: String): JsonNode = new ObjectMapper().readTree(line)

  /** The text of a record, read back from its line. */
  private def readBack(raw: String): String =
    read(QuarantineLines.line(record(raw))).get("raw_record").asText

  /** Whether a terminal, or a reader of lines, acts on a character of a text. */
  private def actedOn(text: String): Boolean =
    text.codePoints.toArray.exists { codePoint =>
      Seq[Int](
        Character.CONTROL,
        Character.FORMAT,
        Character.LINE_SEPARATOR,
        Character.PARAGRAPH_SEPARATOR,
        Character.SURROGATE
      ).contains(Character.getType(codePoint))
    }

  test("a record is one line of JSON, with its fields in their order, and reads back as it was") {
    val line = QuarantineLines.line(record("3,cy,extra"))

    line shouldBe
      s"""{"run_id":"$run","backfill":null,"batch_seq":3,"batch_status":"COMMITTED",""" +
      """"source_file":"sales_returns/a.csv","error_type":"unparsed","error_detail":null,""" +
      """"quarantined_at":"2026-11-19T12:00:00Z","raw_record":"3,cy,extra",""" +
      """"raw_record_chars":10,"truncated":false}"""
    val back = read(line)
    back.get("batch_seq").isIntegralNumber shouldBe true
    back.get("truncated").isBoolean shouldBe true
    back.get("backfill").isNull shouldBe true
  }

  test("the escape of a terminal, and its one character of 0x9B, are written as escapes") {
    // ESC [ 31 m, and the same sequence started by U+009B, as some terminals take it.
    val raw = "name\u001b[31mred\u009b0m"
    val line = QuarantineLines.line(record(raw))

    // In the line, the six characters of each escape, and not the character.
    line should include("\"raw_record\":\"name\\u001b[31mred\\u009b0m\"")
    line should not include "\u001b"
    line should not include "\u009b"
    actedOn(line) shouldBe false
    readBack(raw) shouldBe raw
  }

  test("every control character is written as an escape: those of C0, U+007F, and those of C1") {
    val controls = (0x00 to 0x1f) ++ Seq(0x7f) ++ (0x80 to 0x9f)
    controls.foreach { code =>
      val raw = s"a${code.toChar}b"
      val line = QuarantineLines.line(record(raw))
      withClue(f"U+$code%04X in $line: ") {
        actedOn(line) shouldBe false
        // By the short escape of JSON for the end of a line and the tab, and by its number.
        val escape = code match {
          case '\n' => "\\n"
          case '\r' => "\\r"
          case '\t' => "\\t"
          case other => f"\\u$other%04x"
        }
        line should include(s""""raw_record":"a${escape}b"""")
        readBack(raw) shouldBe raw
      }
    }
  }

  test("the separators of lines, and what turns the direction of a text, are written as escapes") {
    // JSON lets U+2028 and U+2029 through as they are, and some readers end a line at them.
    val raw = "one\u2028two\u2029three \u202egnp.exe\u202c \u200b \ufeff \u00ad end"
    val line = QuarantineLines.line(record(raw))

    line should include(
      "one\\u2028two\\u2029three \\u202egnp.exe\\u202c \\u200b \\ufeff \\u00ad end"
    )
    actedOn(line) shouldBe false
    line.linesIterator.toSeq should have size 1
    readBack(raw) shouldBe raw
  }

  test("a record over several lines, with quotes and backslashes, is one line") {
    val raw = "1,\"a \"\"quoted\"\" value\r\nover two lines\",C:\\temp\\x\ttab"
    val line = QuarantineLines.line(record(raw))

    line should include(
      """"raw_record":"1,\"a \"\"quoted\"\" value\r\nover two lines\",C:\\temp\\x\ttab""""
    )
    line.linesIterator.toSeq should have size 1
    readBack(raw) shouldBe raw
  }

  test("letters and emoji are written as they are, and half of a pair as its escape") {
    val raw = "Peña, 10 €, 日本語, \ud83d\ude00"
    QuarantineLines.line(record(raw)) should include(s""""raw_record":"$raw"""")
    readBack(raw) shouldBe raw
    // As UTF-8, the line is the text: nothing of it is lost on its way out.
    val line = QuarantineLines.line(record(raw))
    new String(line.getBytes(UTF_8), UTF_8) shouldBe line

    // Half of a pair is no character, and is not written as UTF-8: it is written by its number.
    val half = "a\ud83db"
    val halved = QuarantineLines.line(record(half))
    halved should include("\"raw_record\":\"a\\ud83db\"")
    new String(halved.getBytes(UTF_8), UTF_8) shouldBe halved
    readBack(half) shouldBe half
    // A character outside the basic plane that has no glyph, as a tag: both of its pair.
    val tag = new String(Character.toChars(0xe0001))
    QuarantineLines.line(record(s"a${tag}b")) should include(
      "\"raw_record\":\"a\\udb40\\udc01b\""
    )
    readBack(s"a${tag}b") shouldBe s"a${tag}b"
  }

  test("every text of a line is written so: the file, the type and its detail") {
    val odd = Record(
      run,
      2L,
      Some(
        Taken(
          BatchId(
            TxnAppId.backfill(source, Layer.Bronze, BackfillId.parse("history-2024").value),
            BatchSeq.of(2).value
          ),
          BatchStatus.Planned
        )
      ),
      Some("dir/a\u001bb.csv"),
      "duplicate_field",
      Some("the field 'na\u009bme' is written twice"),
      at,
      "x" * 8192,
      9000L
    )
    val line = QuarantineLines.line(odd)

    actedOn(line) shouldBe false
    val back = read(line)
    back.get("backfill").asText shouldBe "history-2024"
    back.get("batch_seq").asLong shouldBe 2L
    back.get("batch_status").asText shouldBe "PLANNED"
    back.get("source_file").asText shouldBe "dir/a\u001bb.csv"
    back.get("error_detail").asText shouldBe "the field 'na\u009bme' is written twice"
    back.get("raw_record_chars").asLong shouldBe 9000L
    back.get("truncated").asBoolean shouldBe true

    // A run that the control plane does not have, and a source that is no file.
    val unknown = read(QuarantineLines.line(odd.copy(taken = None, file = None)))
    unknown.get("backfill").isNull shouldBe true
    unknown.get("batch_status").isNull shouldBe true
    unknown.get("source_file").isNull shouldBe true
  }

  test("what is said of the records that were written has no record in it") {
    val cut = record("x" * 8192, chars = Some(9000L))
    QuarantineLines.written(source, Seq(record("SECRET")), asked = 10) shouldBe
      "1 record of the quarantine of sales.returns was written, the latest first."
    QuarantineLines.written(source, Seq(record("SECRET"), cut), asked = 2) shouldBe
      "2 records of the quarantine of sales.returns were written, the latest first; 1 of " +
      "them cut at 8192 characters. It may have more: ask for the counts, or for less of it " +
      "with --run, --batch, --file or --type."
    QuarantineLines.written(source, Nil, asked = 5) shouldBe
      "0 records of the quarantine of sales.returns were written, the latest first."
  }

  test("the counts are a table: a line for each group, then by type, and what was left out") {
    val other = RunId.generate()
    val chunk = Taken(
      BatchId(
        TxnAppId.backfill(source, Layer.Bronze, BackfillId.parse("h").value),
        BatchSeq.of(2).value
      ),
      BatchStatus.Planned
    )
    val counts = Counts(
      Vector(
        Group(other, 2L, Some(chunk), Some("sales_returns/b.json"), "trailing_text", 12L, at),
        Group(run, 3L, committed, Some("sales_returns/a\u001b.json"), "unparsed", 1L, at),
        Group(run, 3L, None, None, "unparsed", 2L, at)
      ),
      Vector(OfType("trailing_text", 12L, 1L), OfType("unparsed", 1500L, 7L)),
      LeftOut(records = 4L, runs = 2L)
    )

    QuarantineLines.table(source, counts) shouldBe Seq(
      "The quarantine of sales.returns has 1512 records of what was asked, in 8 groups by " +
        "run, file and type; the 3 with the latest records are shown:",
      "",
      "RUN                                   BATCH  STATUS     FILE                        " +
        "TYPE           RECORDS  LATEST",
      s"$other  h#2    PLANNED    sales_returns/b.json        " +
        "trailing_text       12  2026-11-19T12:00:00Z",
      s"$run  3      COMMITTED  sales_returns/a\\u001b.json  " +
        "unparsed             1  2026-11-19T12:00:00Z",
      s"$run  3      unknown    -                           " +
        "unparsed             2  2026-11-19T12:00:00Z",
      "",
      "By type: trailing_text 12, unparsed 1500.",
      "5 more groups are not shown: ask for less of it with --run, --batch, --file or --type.",
      "Left out: 4 records of 2 runs that took a batch that was given up. They count for " +
        "nothing: the batch after sets the same records apart again."
    )
    QuarantineLines.table(source, counts).exists(actedOn) shouldBe false

    // With nothing of what was asked, and with nothing left out.
    val nothing = Counts(Vector.empty, Vector.empty, LeftOut(1L, 1L))
    QuarantineLines.table(source, nothing) shouldBe Seq(
      "The quarantine of sales.returns has no record of what was asked.",
      "Left out: 1 record of 1 run that took a batch that was given up. They count for " +
        "nothing: the batch after sets the same records apart again."
    )
    val whole =
      Counts(counts.groups.take(1), Vector(OfType("trailing_text", 1L, 1L)), LeftOut(0, 0))
    QuarantineLines.table(source, whole).head shouldBe
      "The quarantine of sales.returns has 1 record of what was asked, in 1 group by run, " +
      "file and type:"
    QuarantineLines.table(source, whole).last shouldBe "By type: trailing_text 1."
  }
}
