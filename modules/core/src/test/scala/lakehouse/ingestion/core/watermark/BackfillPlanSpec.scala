package lakehouse.ingestion.core.watermark

import java.time.{Duration => Span, Instant}

import scala.concurrent.duration._
import scala.util.Random

import lakehouse.ingestion.core.config.{SafeYaml, SqlIdentifier, Timestamps}
import lakehouse.ingestion.core.state.StoredWatermark
import lakehouse.ingestion.core.watermark.WatermarkPlanner.{NothingNew, Planned}
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What a backfill reads (ADR-0021 §5): its range `(from, to]`, as an operator gives it and the
 * clock of its source settles it, and its chunks, each a window of ADR-0020 planned by
 * `WatermarkPlanner` from the progress of the backfill.
 */
final class BackfillPlanSpec extends AnyFunSuite with Matchers with EitherValues {

  private val Column = SqlIdentifier.parse("updated_at").value

  private def at(text: String): Instant = Instant.parse(text)

  private val From = at("2024-01-01T00:00:00Z")
  private val To = at("2024-01-01T10:00:00Z")

  private def range(from: Instant = From, to: Instant = To): BackfillRange =
    BackfillRange.of(from, to).value

  private def planned(planning: WatermarkPlanner.Planning): WatermarkPlan = planning match {
    case Planned(plan) => plan
    case other => fail(s"no chunk was planned: $other")
  }

  private def chunk(
      of: BackfillRange,
      length: FiniteDuration,
      progress: Option[Instant]
  ): WatermarkPlanner.Planning =
    WatermarkPlanner.chunk(Column, of, length, progress.map(StoredWatermark(_)))

  /** Every chunk of a backfill, from its progress, as the runs that commit them one by one. */
  private def chunks(
      of: BackfillRange,
      length: FiniteDuration,
      progress: Option[Instant] = None
  ): Vector[WatermarkPlan] =
    Iterator
      .iterate(Option(chunk(of, length, progress))) {
        case Some(Planned(plan)) => Some(chunk(of, length, Some(plan.upper)))
        case _ => None
      }
      .takeWhile(_.nonEmpty)
      .flatten
      .collect { case Planned(plan) => plan }
      .toVector

  // ---------------------------------------------------------------------------------------------
  // The range
  // ---------------------------------------------------------------------------------------------

  test("a range is (from, to]: it holds to and not from") {
    val window = range()
    window.from shouldBe From
    window.to shouldBe To
    window.toString shouldBe "(2024-01-01T00:00:00Z, 2024-01-01T10:00:00Z]"
    window.holds(From) shouldBe false
    window.holds(From.plusNanos(1000)) shouldBe true
    window.holds(To) shouldBe true
    window.holds(To.plusNanos(1000)) shouldBe false
    // A range of one microsecond holds that microsecond.
    range(To.minusNanos(1000), To).holds(To) shouldBe true
  }

  test("a range whose from is not before its to holds no moment, and is refused") {
    BackfillRange.of(To, To).left.value shouldBe
      "from, 2024-01-01T10:00:00Z, is not before to, 2024-01-01T10:00:00Z: the range " +
      "(from, to] holds no moment"
    BackfillRange.of(To, From).left.value should startWith("from, 2024-01-01T10:00:00Z, is not")
  }

  test("a bound that is not a moment that a plan holds is refused, and says which") {
    BackfillRange.of(at("2024-01-01T00:00:00.0000001Z"), To).left.value shouldBe
      "from is finer than a microsecond (2024-01-01T00:00:00.000000100Z)"
    BackfillRange.of(From, at("2024-01-01T10:00:00.0000001Z")).left.value should startWith(
      "to is finer than a microsecond"
    )
    BackfillRange.of(at("1899-12-31T23:59:59.999999Z"), To).left.value shouldBe
      s"from is 1899-12-31T23:59:59.999999Z, outside ${Timestamps.Min} to ${Timestamps.Max}"
    BackfillRange.of(From, at("+10000-01-01T00:00:00Z")).left.value should startWith(
      "to is +10000-01-01T00:00:00Z, outside"
    )
    // The earliest and the latest moments are bounds.
    BackfillRange.of(Timestamps.Min, Timestamps.Max).value.to shouldBe Timestamps.Max
  }

  test("a range is settled by a horizon at its to or after it, and refused before") {
    range().settledBy(To).value shouldBe range()
    range().settledBy(To.plusSeconds(1)).value shouldBe range()
    range().settledBy(To.minusNanos(1000)).left.value shouldBe
      "to, 2024-01-01T10:00:00Z, is after 2024-01-01T09:59:59.999999Z, the clock of the " +
      "source less safety_margin: a backfill does not read time in which a transaction may " +
      "still commit rows. A range that ends at 2024-01-01T09:59:59.999999Z or before is read"
  }

  test("a range is made by of alone: there is no apply nor copy that skips its rules") {
    // Compiled or not, as the abstract case class has it: the two lines below do not compile,
    // and the one way that does is of, which says why it refuses.
    "BackfillRange.of(To, From)" should compile
    "BackfillRange(To, From)" shouldNot compile
    "range().copy(from = To)" shouldNot compile
  }

  // ---------------------------------------------------------------------------------------------
  // The chunks
  // ---------------------------------------------------------------------------------------------

  test("the first chunk starts at from, which it does not hold, and is as long as a chunk") {
    val first = planned(chunk(range(), 3.hours, progress = None))
    first.column shouldBe Column
    first.lower shouldBe Some(From)
    first.upper shouldBe at("2024-01-01T03:00:00Z")
    first.horizon shouldBe To
    first.more shouldBe true
    first.watermark shouldBe StoredWatermark(at("2024-01-01T03:00:00Z"))
  }

  test("each next chunk starts where the last ended, and the last ends at to and has no more") {
    val plans = chunks(range(), 3.hours)
    plans.map(p => (p.lower.get, p.upper)) shouldBe Vector(
      (From, at("2024-01-01T03:00:00Z")),
      (at("2024-01-01T03:00:00Z"), at("2024-01-01T06:00:00Z")),
      (at("2024-01-01T06:00:00Z"), at("2024-01-01T09:00:00Z")),
      (at("2024-01-01T09:00:00Z"), To)
    )
    plans.map(_.more) shouldBe Vector(true, true, true, false)
    plans.map(_.horizon).distinct shouldBe Vector(To)
  }

  test("a backfill whose progress is at to is complete: no chunk is planned") {
    chunk(range(), 3.hours, progress = Some(To)) shouldBe NothingNew(To, To)
  }

  test("a range shorter than a chunk, or as long, is read in one") {
    planned(chunk(range(), 1.day, None)).upper shouldBe To
    planned(chunk(range(), 10.hours, None)).upper shouldBe To
    chunks(range(), 10.hours) should have size 1
  }

  test("a chunk may be one millisecond, and a range one microsecond") {
    val short = range(From, From.plusNanos(1000))
    chunks(short, 1.milli).map(p => (p.lower.get, p.upper)) shouldBe
      Vector((From, From.plusNanos(1000)))
    chunks(range(From, From.plusMillis(3)), 1.milli) should have size 3
  }

  test("a range before 1970, or reaching back to 1900, is read as any other") {
    chunks(range(Timestamps.Min, at("1900-01-03T00:00:00Z")), 1.day)
      .map(_.upper) shouldBe Vector(at("1900-01-02T00:00:00Z"), at("1900-01-03T00:00:00Z"))
  }

  test("a chunk that is no stretch of time, or not whole milliseconds, is not planned") {
    the[IllegalArgumentException] thrownBy chunk(range(), Duration.Zero, None) should have message
      "requirement failed: a chunk of 0 days is no stretch of time"
    an[IllegalArgumentException] should be thrownBy chunk(range(), 1.micro, None)
  }

  test("a progress outside the range is not one that a chunk of it left, and is refused") {
    Seq(From, From.minusSeconds(1), To.plusNanos(1000)).foreach { outside =>
      withClue(s"[$outside]") {
        val refused = the[IllegalArgumentException] thrownBy chunk(range(), 1.hour, Some(outside))
        refused.getMessage shouldBe s"requirement failed: the progress $outside is not in the " +
          s"range ${range()}, and no chunk of it ends there"
      }
    }
  }

  test("whatever the range and the chunk, the chunks cover the range once, and end") {
    val random = new Random(20261123L)

    (1 to 500).foreach { round =>
      // From 1900 to about 2026, to the microsecond; a chunk of up to a day; a range of up to
      // forty chunks, and as short as a microsecond.
      val from = Timestamps.Min.plusNanos(random.nextLong(4000000000000000L) * 1000L)
      val length = (1L + random.nextLong(86400000L)).millis
      val to = from.plusNanos((1L + random.nextLong(length.toMicros * 40)) * 1000L)
      val of = range(from, to)
      // Resumed from where a run before left it, at a chunk that was committed, or from the start.
      val whole = chunks(of, length)

      withClue(s"round $round, $of in chunks of $length: ") {
        val nanos = Span.between(from, to).toNanos
        whole should have size ((nanos + length.toNanos - 1) / length.toNanos)

        // From from to to, each chunk starting where the last ended: no gap, no overlap, and
        // none longer than a chunk.
        (whole.head.lower.get +: whole.map(_.upper)).sliding(2).foreach {
          case Seq(lower, upper) =>
            upper should be > lower
            Span.between(lower, upper).toNanos should be <= length.toNanos
          case _ =>
        }
        whole.head.lower shouldBe Some(from)
        whole.tail.zip(whole).foreach { case (next, last) => next.lower shouldBe Some(last.upper) }
        whole.last.upper shouldBe to
        whole.map(_.more) shouldBe whole.indices.map(_ < whole.size - 1)
        whole.map(_.horizon).distinct shouldBe Vector(to)

        // A run that resumes from any chunk that was committed plans the rest, as they were.
        val resumedAt = random.nextInt(whole.size)
        chunks(of, length, Some(whole(resumedAt).upper)) shouldBe whole.drop(resumedAt + 1)

        // Complete once the last is committed.
        chunk(of, length, Some(to)) shouldBe NothingNew(to, to)

        // Every chunk is a window that the connector reads back as itself, from its text.
        whole.foreach { plan =>
          WatermarkPlan.fromJson(SafeYaml.parse(plan.toJson.noSpaces).value).value shouldBe plan
        }
      }
    }
  }
}
