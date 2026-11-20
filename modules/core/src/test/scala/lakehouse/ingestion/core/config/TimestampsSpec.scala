package lakehouse.ingestion.core.config

import java.time.Instant

import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class TimestampsSpec extends AnyFunSuite with Matchers with EitherValues {

  test("a date, a time to the second or finer, and a zone is a moment") {
    Timestamps.parse("2026-01-01T00:00:00Z").value shouldBe Instant.parse("2026-01-01T00:00:00Z")
    Timestamps.parse("2026-01-01T01:00:00+01:00").value shouldBe
      Instant.parse("2026-01-01T00:00:00Z")
    Timestamps.parse("2025-12-31T21:30:00-02:30").value shouldBe
      Instant.parse("2026-01-01T00:00:00Z")
    Timestamps.parse("2026-01-01T00:00:00.5Z").value shouldBe
      Instant.parse("2026-01-01T00:00:00.500Z")
    Timestamps.parse("2026-01-01T00:00:00.123456Z").value shouldBe
      Instant.parse("2026-01-01T00:00:00.123456Z")
    Timestamps.parse("2024-02-29T23:59:59Z").value shouldBe Instant.parse("2024-02-29T23:59:59Z")
  }

  test("a moment without its zone is refused: it would be another moment on another machine") {
    Seq("2026-01-01T00:00:00", "2026-01-01T00:00", "2026-01-01 00:00:00", "2026-01-01T00:00:00.5")
      .foreach { text =>
        withClue(text) {
          Timestamps.parse(text).left.value should startWith("has no zone: write it with Z")
        }
      }
    Timestamps.parse("2026-01-01").left.value shouldBe
      "is a date alone: write a moment, with its zone, as 2026-01-01T00:00:00Z"
  }

  test("more than microseconds is refused: PostgreSQL would round them") {
    Timestamps.parse("2026-01-01T00:00:00.1234567Z").left.value should startWith(
      "has more than six digits after the second"
    )
    Timestamps.parse("2026-01-01T00:00:00.123456789Z").left.value should startWith(
      "has more than six digits after the second"
    )
  }

  test("what is not written the one way is refused") {
    Seq(
      "",
      " 2026-01-01T00:00:00Z",
      "2026-01-01T00:00:00Z ",
      "2026-01-01t00:00:00z",
      "2026-01-01T00:00Z",
      "2026-01-01T00:00:00+0100",
      "2026-01-01T00:00:00+01",
      "2026-01-01T00:00:00.Z",
      "2026-1-1T00:00:00Z",
      "+2026-01-01T00:00:00Z",
      "20260101T000000Z",
      "1767225600",
      "now",
      "2026-01-01T00:00:00Z[UTC]",
      "2026-01-01T00:00:00 UTC"
    ).foreach { text =>
      withClue(s"'$text'") {
        Timestamps.parse(text).left.value should startWith("is not a moment: write a date")
      }
    }
  }

  test("a moment that no calendar has is refused") {
    Seq(
      "2026-02-30T00:00:00Z",
      "2025-02-29T00:00:00Z",
      "2026-13-01T00:00:00Z",
      "2026-01-01T24:00:00Z",
      "2026-01-01T00:60:00Z",
      "2026-01-01T00:00:60Z",
      "2026-01-01T00:00:00+19:00"
    ).foreach { text =>
      withClue(text) {
        Timestamps.parse(text).left.value should startWith("is not a moment that a calendar has")
      }
    }
  }

  test("a moment is from 1900 to the last microsecond of 9999") {
    Timestamps.parse("1900-01-01T00:00:00Z").value shouldBe Timestamps.Min
    Timestamps.parse("9999-12-31T23:59:59.999999Z").value shouldBe Timestamps.Max
    Timestamps.parse("1899-12-31T23:59:59.999999Z").left.value shouldBe
      "is before 1900-01-01T00:00:00Z, the earliest a moment may be"
    // The same day of the calendar, and earlier than 1900 in UTC.
    Timestamps.parse("1900-01-01T00:30:00+01:00").left.value should startWith("is before")
    Timestamps.parse("9999-12-31T23:00:00-05:00").left.value shouldBe
      "is after 9999-12-31T23:59:59.999999Z, the latest a moment may be"
    Timestamps.parse("1582-10-04T00:00:00Z").left.value should startWith("is before")
  }

  test("a moment is written one way, in UTC, and read back as itself") {
    Seq(
      "2026-01-01T01:00:00+01:00" -> "2026-01-01T00:00:00Z",
      "2026-01-01T00:00:00.500000Z" -> "2026-01-01T00:00:00.500Z",
      "2026-01-01T00:00:00.000001Z" -> "2026-01-01T00:00:00.000001Z",
      "9999-12-31T23:59:59.999999Z" -> "9999-12-31T23:59:59.999999Z",
      "1900-01-01T00:00:00Z" -> "1900-01-01T00:00:00Z"
    ).foreach { case (written, rendered) =>
      val instant = Timestamps.parse(written).value
      Timestamps.render(instant) shouldBe rendered
      Timestamps.parse(rendered).value shouldBe instant
    }
  }
}
