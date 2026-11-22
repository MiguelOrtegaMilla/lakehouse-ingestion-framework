package lakehouse.ingestion.core.watermark

import java.time.{Duration => Span, Instant}

import scala.concurrent.duration._
import scala.util.Random

import io.circe.Json
import lakehouse.ingestion.core.config.{Load, SafeYaml, SqlIdentifier, Timestamps}
import lakehouse.ingestion.core.watermark.WatermarkPlanner.{NothingNew, Planned}
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The plan of a batch of a watermark (ADR-0020): the window that a run reads, from the
 * watermark of its source, a horizon and the settings of its load; and the plan and the
 * watermark as they are written down and read back (ADR-0005 §3).
 */
final class WatermarkPlanSpec extends AnyFunSuite with Matchers with EitherValues {

  private val Column = SqlIdentifier.parse("updated_at").value

  private def at(text: String): Instant = Instant.parse(text)

  /** Noon of the day of the tests, and moments before and after it. */
  private val Noon = at("2026-11-20T12:00:00Z")
  private def before(length: FiniteDuration): Instant = Noon.minusNanos(length.toNanos)
  private def after(length: FiniteDuration): Instant = Noon.plusNanos(length.toNanos)

  private def load(
      lookback: FiniteDuration = Duration.Zero,
      margin: FiniteDuration = Duration.Zero,
      start: Option[Instant] = None,
      range: Option[FiniteDuration] = None
  ): Load.Incremental = Load.Incremental(Load.Watermark(Column, lookback, margin, start), range)

  private def planned(planning: WatermarkPlanner.Planning): WatermarkPlan = planning match {
    case Planned(plan) => plan
    case other => fail(s"no batch was planned: $other")
  }

  private def nothing(planning: WatermarkPlanner.Planning): NothingNew = planning match {
    case none: NothingNew => none
    case other => fail(s"a batch was planned: $other")
  }

  /** The plan of a source whose watermark is `watermark`, in an invocation that reads to `horizon`. */
  private def plan(
      under: Load.Incremental,
      watermark: Option[Instant],
      horizon: Instant = Noon
  ): WatermarkPlanner.Planning =
    WatermarkPlanner.plan(under, watermark.map(StoredWatermark(_)), horizon)

  // ---------------------------------------------------------------------------------------------
  // The horizon of an invocation
  // ---------------------------------------------------------------------------------------------

  test("the horizon is the clock of the source less safety_margin, to the microsecond") {
    val clock = at("2026-11-20T12:00:00.123456Z")
    WatermarkPlanner.horizon(load().watermark, clock).value shouldBe clock
    WatermarkPlanner.horizon(load(margin = 5.minutes).watermark, clock).value shouldBe
      at("2026-11-20T11:55:00.123456Z")
    WatermarkPlanner.horizon(load(margin = 1.milli).watermark, clock).value shouldBe
      at("2026-11-20T12:00:00.122456Z")
  }

  test("a clock that says a time that no plan holds makes no horizon, and says why") {
    val margin = load(margin = 1.hour).watermark
    WatermarkPlanner.horizon(margin, at("1900-01-01T01:00:00Z")).value shouldBe Timestamps.Min
    WatermarkPlanner.horizon(margin, at("1900-01-01T00:59:59.999999Z")).left.value shouldBe
      "the clock of the source says 1900-01-01T00:59:59.999999Z, which less safety_margin is " +
      "before 1900-01-01T00:00:00Z"
    WatermarkPlanner.horizon(margin, Timestamps.Max).value shouldBe
      at("9999-12-31T22:59:59.999999Z")
    WatermarkPlanner.horizon(margin, at("+10000-01-01T00:00:00Z")).left.value shouldBe
      "the clock of the source says +10000-01-01T00:00:00Z, after 9999-12-31T23:59:59.999999Z"
  }

  test("a clock finer than a microsecond is not one of a source") {
    the[IllegalArgumentException] thrownBy
      WatermarkPlanner.horizon(load().watermark, at("2026-11-20T12:00:00.000000001Z")) should
      have message "requirement failed: the clock 2026-11-20T12:00:00.000000001Z is finer than a microsecond"
  }

  // ---------------------------------------------------------------------------------------------
  // The first batch of a source
  // ---------------------------------------------------------------------------------------------

  test("the first batch of a source without start reads from the first row to the horizon") {
    val first = planned(plan(load(), watermark = None))
    first.column shouldBe Column
    first.lower shouldBe None
    first.upper shouldBe Noon
    first.horizon shouldBe Noon
    first.more shouldBe false
  }

  test("max_batch_range does not cut the first batch of a source without start") {
    val first = planned(plan(load(range = Some(1.hour), lookback = 10.minutes), watermark = None))
    first.lower shouldBe None
    first.upper shouldBe Noon
    first.more shouldBe false
  }

  test("the first batch of a source with start begins there, and lookback reads nothing before") {
    val start = before(3.hours)
    val first = planned(plan(load(start = Some(start), lookback = 1.hour), watermark = None))
    first.lower shouldBe Some(start)
    first.upper shouldBe Noon
    first.more shouldBe false
  }

  test("max_batch_range cuts the first batch of a source with start, counted from start") {
    val start = before(3.hours)
    val first = planned(plan(load(start = Some(start), range = Some(1.hour)), watermark = None))
    first.lower shouldBe Some(start)
    first.upper shouldBe before(2.hours)
    first.horizon shouldBe Noon
    first.more shouldBe true
  }

  test("a start that the horizon has not passed plans no batch, and makes no watermark") {
    val ahead = nothing(plan(load(start = Some(after(1.hour))), watermark = None))
    ahead shouldBe NothingNew(horizon = Noon, from = after(1.hour))
    ahead.behind shouldBe Span.ofHours(1)

    nothing(plan(load(start = Some(Noon)), watermark = None)).behind shouldBe Span.ZERO
    // One microsecond past it is a window of that microsecond.
    val one = planned(plan(load(start = Some(before(1.micro))), watermark = None))
    (one.lower, one.upper) shouldBe ((Some(before(1.micro)), Noon))
  }

  // ---------------------------------------------------------------------------------------------
  // The batches after the first
  // ---------------------------------------------------------------------------------------------

  test("a batch reads from the watermark to the horizon, and moves the watermark there") {
    val next = planned(plan(load(), watermark = Some(before(10.minutes))))
    next.lower shouldBe Some(before(10.minutes))
    next.upper shouldBe Noon
    next.more shouldBe false
    next.watermark shouldBe StoredWatermark(Noon)
  }

  test("lookback starts a batch before the watermark, and does not move where it ends") {
    val next = planned(plan(load(lookback = 15.minutes), watermark = Some(before(10.minutes))))
    next.lower shouldBe Some(before(25.minutes))
    next.upper shouldBe Noon
    next.watermark shouldBe StoredWatermark(Noon)
  }

  test("max_batch_range cuts a batch, counted from the watermark and not from its lower bound") {
    val watermark = before(5.hours)
    val cut = planned(plan(load(range = Some(1.hour)), Some(watermark)))
    (cut.lower, cut.upper) shouldBe ((Some(watermark), before(4.hours)))
    cut.more shouldBe true

    // With a lookback, the window is wider than the range by it, and ends at the same moment.
    val wider = planned(plan(load(range = Some(1.hour), lookback = 20.minutes), Some(watermark)))
    (wider.lower, wider.upper) shouldBe ((Some(before(5.hours + 20.minutes)), before(4.hours)))
  }

  test("a lookback as long as max_batch_range, or longer, still moves the watermark") {
    // Counted from the lower bound, the window would end at the watermark, or before it.
    val watermark = before(5.hours)
    Seq(1.hour, 3.hours).foreach { lookback =>
      val cut = planned(plan(load(range = Some(1.hour), lookback = lookback), Some(watermark)))
      withClue(s"lookback $lookback: ") {
        cut.upper shouldBe before(4.hours)
        cut.watermark.value should be > watermark
        cut.lower shouldBe Some(watermark.minusNanos(lookback.toNanos))
      }
    }
  }

  test("a range that reaches the horizon, or passes it, ends the batch at the horizon") {
    val watermark = before(1.hour)
    val exact = planned(plan(load(range = Some(1.hour)), Some(watermark)))
    exact.upper shouldBe Noon
    exact.more shouldBe false

    val beyond = planned(plan(load(range = Some(2.hours)), Some(watermark)))
    beyond.upper shouldBe Noon
    beyond.more shouldBe false

    val short = planned(plan(load(range = Some(1.hour - 1.milli)), Some(watermark)))
    short.upper shouldBe before(1.milli)
    short.more shouldBe true
  }

  test("lookback reads no row at or before start") {
    val start = before(3.hours)
    def lower(watermark: Instant, lookback: FiniteDuration): Option[Instant] =
      planned(plan(load(start = Some(start), lookback = lookback), Some(watermark))).lower

    // Short of start, the lookback is whole.
    lower(before(1.hour), 30.minutes) shouldBe Some(before(90.minutes))
    lower(before(1.hour), 2.hours) shouldBe Some(start)
    // Past it, it stops at start.
    lower(before(1.hour), 5.hours) shouldBe Some(start)
    // The batch after the first of a source that has just started reads nothing again.
    lower(before(3.hours - 1.micro), 1.hour) shouldBe Some(start)
  }

  test("a start moved to after the watermark bounds nothing, and skips no row") {
    val watermark = before(2.hours)
    Seq(before(1.hour), after(1.hour)).foreach { moved =>
      val next = planned(plan(load(start = Some(moved), lookback = 30.minutes), Some(watermark)))
      withClue(s"start $moved: ") {
        next.lower shouldBe Some(watermark)
        next.upper shouldBe Noon
      }
    }
    // And with no lookback, start is not looked at.
    planned(plan(load(start = Some(after(1.hour))), Some(watermark))).lower shouldBe
      Some(watermark)
  }

  test("without start, lookback reads no earlier than the earliest moment of a plan") {
    val early = Timestamps.Min.plusSeconds(60)
    val next = planned(plan(load(lookback = 1.hour), Some(early)))
    next.lower shouldBe Some(Timestamps.Min)
  }

  test("a window may be one microsecond, and its bounds keep the microseconds of the source") {
    val watermark = at("2026-11-20T11:59:59.999999Z")
    val one = planned(plan(load(), Some(watermark)))
    (one.lower, one.upper) shouldBe ((Some(watermark), Noon))

    val fine = planned(
      plan(
        load(lookback = 1.milli, range = Some(1.second)),
        Some(at("2026-11-20T10:00:00.000001Z"))
      )
    )
    fine.lower shouldBe Some(at("2026-11-20T09:59:59.999001Z"))
    fine.upper shouldBe at("2026-11-20T10:00:01.000001Z")
  }

  // ---------------------------------------------------------------------------------------------
  // The watermark never goes back
  // ---------------------------------------------------------------------------------------------

  test("a horizon at the watermark, or before it, plans no batch") {
    val watermark = Noon
    // The source was read up to its horizon: the last batch of a chain of runs.
    nothing(plan(load(), Some(watermark), horizon = Noon)) shouldBe NothingNew(Noon, Noon)
    // The clock of the source went back, or safety_margin was raised.
    val back = nothing(plan(load(), Some(watermark), horizon = before(10.minutes)))
    back shouldBe NothingNew(horizon = before(10.minutes), from = Noon)
    back.behind shouldBe Span.ofMinutes(10)
  }

  test("a lookback does not have a batch planned that would not move the watermark") {
    // The window (W - lookback, H] has moments in it, and is not read: the watermark would
    // stay, or go back.
    val under = load(lookback = 1.hour, range = Some(1.hour))
    nothing(plan(under, Some(Noon), horizon = before(10.minutes))).behind shouldBe
      Span.ofMinutes(10)
    nothing(plan(under, Some(Noon), horizon = Noon)).behind shouldBe Span.ZERO
  }

  test(
    "a clock that went back by less than safety_margin is told from one that went back by more"
  ) {
    val settings = load(margin = 5.minutes)
    val watermark = Some(StoredWatermark(Noon))
    def behind(clock: Instant): Span = {
      val horizon = WatermarkPlanner.horizon(settings.watermark, clock).value
      nothing(WatermarkPlanner.plan(settings, watermark, horizon)).behind
    }
    // The clock is past the watermark: the margin alone holds the batch back.
    behind(after(2.minutes)).compareTo(Span.ofMinutes(5)) should be < 0
    // The clock itself is behind the watermark: what the source stamps now is below it.
    behind(before(1.minute)).compareTo(Span.ofMinutes(5)) should be > 0
  }

  // ---------------------------------------------------------------------------------------------
  // A chain of runs, to the horizon
  // ---------------------------------------------------------------------------------------------

  /** The plans of an invocation: each from the watermark that the one before left. */
  private def chain(
      under: Load.Incremental,
      watermark: Option[Instant],
      horizon: Instant
  ): Vector[WatermarkPlan] =
    Vector.unfold(watermark.map(StoredWatermark(_))) { reached =>
      WatermarkPlanner.plan(under, reached, horizon) match {
        case Planned(plan) => Some(plan -> Some(plan.watermark))
        case _: NothingNew => None
      }
    }

  test("a chain of runs reads to its horizon in windows that follow each other, and ends") {
    val start = before(10.hours)
    val plans = chain(load(start = Some(start), range = Some(3.hours)), None, horizon = Noon)

    plans.map(p => (p.lower.get, p.upper)) shouldBe Vector(
      (before(10.hours), before(7.hours)),
      (before(7.hours), before(4.hours)),
      (before(4.hours), before(1.hour)),
      (before(1.hour), Noon)
    )
    plans.map(_.more) shouldBe Vector(true, true, true, false)
    plans.map(_.horizon).distinct shouldBe Vector(Noon)
  }

  test("the plan of a batch that says there is no more is the last of its chain") {
    val under = load(range = Some(1.hour))
    val last = planned(plan(under, Some(before(30.minutes))))
    last.more shouldBe false
    // The horizon is the invocation's: what the source wrote since is not read by this chain.
    nothing(WatermarkPlanner.plan(under, Some(last.watermark), last.horizon))
  }

  test("whatever the settings, a chain covers what is between its start and its horizon, once") {
    val random = new Random(20261120L)
    def millis(upTo: Int): FiniteDuration = random.nextInt(upTo).millis
    def some[A](value: => A): Option[A] = Option.when(random.nextInt(3) > 0)(value)

    (1 to 500).foreach { round =>
      val horizon = Noon.plusNanos(random.nextInt(1000000) * 1000L)
      val start = some(horizon.minusNanos((1 + random.nextInt(86400000)) * 1000000L))
      val range = some((1 + random.nextInt(7200000)).millis)
      val lookback = if (random.nextBoolean()) Duration.Zero else millis(7200000)
      val watermark = some(horizon.minusNanos(random.nextInt(86400000) * 1000000L + 1000L))
      val under = load(lookback = lookback, start = start, range = range)
      val from = watermark.orElse(start)

      withClue(s"round $round, $under, watermark $watermark, horizon $horizon: ") {
        val plans = chain(under, watermark, horizon)

        // It ends, in as many batches as the range has between where it counts from and the
        // horizon; without one, or with nothing to count from, in one.
        val expected = (from, range) match {
          case (Some(counted), Some(stretch)) =>
            val nanos = Span.between(counted, horizon).toNanos
            ((nanos + stretch.toNanos - 1) / stretch.toNanos).toInt
          case _ => 1
        }
        plans should have size expected.toLong

        // The upper bounds are where each next one counts from, with no gap and no overlap,
        // and the last is the horizon.
        (from.toVector ++ plans.map(_.upper)).sliding(2).foreach {
          case Vector(counted, upper) =>
            upper should be > counted
            range
              .foreach(stretch => Span.between(counted, upper).toNanos should be <= stretch.toNanos)
          case _ =>
        }
        plans.last.upper shouldBe horizon
        plans.map(_.more) shouldBe plans.indices.map(_ < plans.size - 1)

        // A lower bound is never after where its batch counts from, nor further back than
        // lookback, nor at a moment before start; with no lookback, it is where it counts from.
        from.toVector.zip(plans).zipWithIndex.foreach { case ((counted, plan), index) =>
          val lower = plan.lower.get
          lower should be <= counted
          Span.between(lower, counted).toNanos should be <= lookback.toNanos
          start.filter(_.isBefore(counted)).foreach(lower should be >= _)
          if (lookback == Duration.Zero || (index == 0 && watermark.isEmpty)) lower shouldBe counted
        }
        if (from.isEmpty) plans.head.lower shouldBe None

        // Every plan is read back as itself, from its text.
        plans.foreach { plan =>
          WatermarkPlan.fromJson(SafeYaml.parse(plan.toJson.noSpaces).value).value shouldBe plan
          StoredWatermark.fromJson(plan.watermark.toJson).value shouldBe plan.watermark
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The plan and the watermark as they are written down
  // ---------------------------------------------------------------------------------------------

  test("a reset sets the watermark to a moment, and the next batch counts from it") {
    val reset = WatermarkPlanner.resetTo(load(lookback = 30.minutes), before(2.hours)).value
    reset shouldBe StoredWatermark(before(2.hours))
    val next = planned(plan(load(lookback = 30.minutes), Some(reset.value)))
    next.lower shouldBe Some(before(150.minutes))
    next.upper shouldBe Noon
  }

  test("a reset to start, or after it, is let through; one before start is refused") {
    val started = load(start = Some(before(1.hour)))
    WatermarkPlanner.resetTo(started, before(1.hour)).value shouldBe StoredWatermark(before(1.hour))
    WatermarkPlanner.resetTo(started, Noon).value shouldBe StoredWatermark(Noon)
    WatermarkPlanner.resetTo(started, before(1.hour).minusNanos(1000)).left.value shouldBe
      "2026-11-20T10:59:59.999999Z is before load.watermark.start, 2026-11-20T11:00:00Z: the " +
      "rows at or before it are not of the regular sequence of the source, which starts there, " +
      "and a backfill reads them"
    // A moment the source has not reached: its clock is not asked.
    WatermarkPlanner.resetTo(load(), after(100.days)).value shouldBe StoredWatermark(
      after(100.days)
    )
  }

  test("a reset to a moment that no watermark holds is refused, and says why") {
    WatermarkPlanner.resetTo(load(), Noon.plusNanos(1)).left.value shouldBe
      "the moment is finer than a microsecond (2026-11-20T12:00:00.000000001Z)"
    WatermarkPlanner.resetTo(load(), Timestamps.Min.minusSeconds(1)).left.value should
      startWith("the moment is 1899-12-31T23:59:59Z, outside")
  }

  private val Written = WatermarkPlan(
    Column,
    lower = Some(at("2026-11-20T10:00:00Z")),
    upper = at("2026-11-20T11:00:00.123456Z"),
    horizon = at("2026-11-20T11:55:00.5Z")
  )

  test("a plan is written in one form, which changes only with its version") {
    Written.toJson.noSpaces shouldBe
      """{"version":1,"column":"updated_at","lower":"2026-11-20T10:00:00Z",""" +
      """"upper":"2026-11-20T11:00:00.123456Z","horizon":"2026-11-20T11:55:00.500Z"}"""
    WatermarkPlan(Column, None, Noon, Noon).toJson.noSpaces shouldBe
      """{"version":1,"column":"updated_at","lower":null,""" +
      """"upper":"2026-11-20T12:00:00Z","horizon":"2026-11-20T12:00:00Z"}"""
  }

  test("a watermark is written in one form, which changes only with its version") {
    Written.watermark.toJson.noSpaces shouldBe
      """{"version":1,"value":"2026-11-20T11:00:00.123456Z"}"""
  }

  test("a plan and a watermark are read back as they were written, from their text as well") {
    WatermarkPlan.fromJson(Written.toJson).value shouldBe Written
    WatermarkPlan.fromJson(SafeYaml.parse(Written.toJson.noSpaces).value).value shouldBe Written
    val first = WatermarkPlan(Column, None, Noon, Noon)
    WatermarkPlan.fromJson(SafeYaml.parse(first.toJson.noSpaces).value).value shouldBe first

    val watermark = Written.watermark
    StoredWatermark.fromJson(watermark.toJson).value shouldBe watermark
    StoredWatermark.fromJson(SafeYaml.parse(watermark.toJson.noSpaces).value).value shouldBe
      watermark
  }

  test("a plan holds no window that is empty, that ends after its horizon, or of no moment") {
    def refused(lower: Option[Instant], upper: Instant, horizon: Instant): String =
      (the[IllegalArgumentException] thrownBy WatermarkPlan(
        Column,
        lower,
        upper,
        horizon
      )).getMessage

    refused(Some(Noon), Noon, Noon) shouldBe
      "requirement failed: lower is not before upper: the window has no moment"
    refused(Some(after(1.second)), Noon, Noon) shouldBe
      "requirement failed: lower is not before upper: the window has no moment"
    refused(None, after(1.micro), Noon) shouldBe "requirement failed: upper is after horizon"
    refused(None, at("2026-11-20T12:00:00.000000001Z"), after(1.hour)) shouldBe
      "upper is finer than a microsecond (2026-11-20T12:00:00.000000001Z)"
    refused(Some(at("1899-12-31T23:59:59Z")), Noon, Noon) shouldBe
      "lower is 1899-12-31T23:59:59Z, outside 1900-01-01T00:00:00Z to 9999-12-31T23:59:59.999999Z"
    the[IllegalArgumentException] thrownBy StoredWatermark(at("+10000-01-01T00:00:00Z")) should
      have message "a watermark is +10000-01-01T00:00:00Z, outside 1900-01-01T00:00:00Z to 9999-12-31T23:59:59.999999Z"
  }

  test("a plan that is not one this application wrote is refused, and says why") {
    def having(field: String, value: Json): Json = Written.toJson.mapObject(_.add(field, value))
    def text(value: String): Json = Json.fromString(value)
    def refusal(json: Json): String = WatermarkPlan.fromJson(json).left.value

    refusal(text("a plan")) shouldBe "is not an object"
    refusal(Written.toJson.mapObject(_.remove("lower"))) shouldBe "has no lower"
    refusal(Written.toJson.mapObject(_.remove("horizon"))) shouldBe "has no horizon"
    refusal(having("rows", Json.fromInt(3))) shouldBe "has a field rows"
    refusal(having("version", Json.fromInt(2))) shouldBe
      "is of version 2, and this application reads version 1"
    // A number written as text is not one, though circe's decoders would take it.
    refusal(having("version", text("1"))) shouldBe "version is not a whole number"

    refusal(having("column", Json.fromInt(1))) shouldBe "column is not text"
    refusal(having("column", text("updated at"))) shouldBe
      "column has U+0020 SPACE at position 8: a name is letters of ASCII, digits and underscores"
    refusal(having("column", text("""x" > now() --"""))) should startWith("column has '\"'")

    refusal(having("upper", Json.fromLong(1763640000L))) shouldBe "upper is not a moment"
    refusal(having("upper", Json.Null)) shouldBe "upper is not a moment"
    refusal(having("upper", text("yesterday"))) shouldBe "upper is not a moment"
    refusal(having("lower", text("2026-11-20 10:00:00"))) shouldBe "lower is not a moment"
    // One moment, written in other ways: with an offset, with zeros that say nothing.
    refusal(having("lower", text("2026-11-20T11:00:00+01:00"))) shouldBe
      "lower is not written as a moment is, in UTC and in one way: 2026-11-20T10:00:00Z"
    refusal(having("horizon", text("2026-11-20T11:55:00.50Z"))) shouldBe
      "horizon is not written as a moment is, in UTC and in one way: 2026-11-20T11:55:00.500Z"
    refusal(having("upper", text("2026-11-20T11:00:00.123456789Z"))) shouldBe
      "upper is finer than a microsecond (2026-11-20T11:00:00.123456789Z)"
    refusal(having("lower", text("1582-10-04T00:00:00Z"))) shouldBe
      "lower is 1582-10-04T00:00:00Z, outside 1900-01-01T00:00:00Z to 9999-12-31T23:59:59.999999Z"

    refusal(having("lower", text("2026-11-20T11:00:00.123456Z"))) shouldBe
      "lower is not before upper: the window has no moment"
    refusal(having("horizon", text("2026-11-20T11:00:00.123455Z"))) shouldBe
      "upper is after horizon"
  }

  test("a watermark that is not one this application wrote is refused, and says why") {
    val written = Written.watermark.toJson
    def refusal(json: Json): String = StoredWatermark.fromJson(json).left.value

    refusal(Json.fromString("2026-11-20T11:00:00Z")) shouldBe "is not an object"
    refusal(written.mapObject(_.remove("version"))) shouldBe "has no version"
    refusal(written.mapObject(_.add("column", Json.fromString("updated_at")))) shouldBe
      "has a field column"
    refusal(written.mapObject(_.add("version", Json.fromInt(0)))) shouldBe
      "is of version 0, and this application reads version 1"
    refusal(written.mapObject(_.add("value", Json.fromLong(42)))) shouldBe "value is not a moment"
    refusal(written.mapObject(_.add("value", Json.fromString("2026-11-20T11:00:00.0Z")))) shouldBe
      "value is not written as a moment is, in UTC and in one way: 2026-11-20T11:00:00Z"
    refusal(
      written.mapObject(_.add("value", Json.fromString("2026-11-20T11:00:00.000000001Z")))
    ) shouldBe "value is finer than a microsecond (2026-11-20T11:00:00.000000001Z)"
  }

  // ---------------------------------------------------------------------------------------------
  // The horizon of a chain of runs (P6.10)
  // ---------------------------------------------------------------------------------------------

  test(
    "a plan cut short of its horizon gives it to the next run of the chain; one that reaches it, none"
  ) {
    val cut = WatermarkPlan(Column, Some(before(3.hours)), before(2.hours), Noon)
    cut.next shouldBe Some(WatermarkHorizon(Noon))
    WatermarkPlan(Column, Some(before(1.hour)), Noon, Noon).next shouldBe None
  }

  test("the horizon of a chain is written in one form, and read back as itself or refused") {
    val horizon = WatermarkHorizon(at("2026-11-20T11:59:00.000001Z"))
    horizon.toJson.noSpaces shouldBe """{"version":1,"until":"2026-11-20T11:59:00.000001Z"}"""
    WatermarkHorizon.fromJson(SafeYaml.parse(horizon.toJson.noSpaces).value).value shouldBe horizon

    def refusal(json: Json): String = WatermarkHorizon.fromJson(json).left.value
    refusal(horizon.toJson.mapObject(_.add("version", Json.fromInt(2)))) shouldBe
      "is of version 2, and this application reads version 1"
    refusal(horizon.toJson.mapObject(_.remove("until"))) shouldBe "has no until"
    refusal(
      horizon.toJson.mapObject(_.add("until", Json.fromString("2026-11-20T12:59:00+01:00")))
    ) shouldBe
      "until is not written as a moment is, in UTC and in one way: 2026-11-20T11:59:00Z"
    an[IllegalArgumentException] should be thrownBy WatermarkHorizon(Noon.plusNanos(1))
  }
}
