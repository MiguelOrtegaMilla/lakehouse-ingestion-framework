package lakehouse.ingestion.core.watermark

import java.time.{Duration => Span, Instant}

import scala.concurrent.duration.FiniteDuration

import cats.syntax.all._
import io.circe.{ACursor, Json}
import lakehouse.ingestion.core.StoredForm
import lakehouse.ingestion.core.config.{Load, SqlIdentifier, Timestamps}

/**
 * A moment that a plan or a watermark holds: a time of the clock of a source.
 *
 *  - '''To the microsecond''', which is what PostgreSQL keeps. A bound that is finer would be
 *    rounded by the source, and would not be the bound that was written down.
 *  - '''From 1900 to the end of 9999''', as a moment of configuration is (`Timestamps`): Spark
 *    writes no later timestamp into a statement, and writes one before 1582 as another day.
 *  - '''Written in UTC, in one way''': `2026-01-01T00:00:00Z`, with the digits after the second
 *    that it has. A document with a moment written otherwise is not one that this application
 *    wrote.
 */
private[watermark] object Moment {

  def toTheMicrosecond(at: Instant): Boolean = at.getNano % 1000 == 0

  def inRange(at: Instant): Boolean =
    !at.isBefore(Timestamps.Min) && !at.isAfter(Timestamps.Max)

  /** What is wrong with a moment, to follow its name: none, if a plan can hold it. */
  def problem(at: Instant): Option[String] =
    if (!toTheMicrosecond(at)) Some(s"is finer than a microsecond ($at)")
    else if (!inRange(at))
      Some(s"is $at, outside ${Timestamps.Min} to ${Timestamps.Max}")
    else None

  def write(at: Instant): Json = Json.fromString(at.toString)

  /** `Left` says what is wrong, starting with the name of the field. */
  def read(at: ACursor, field: String): Either[String, Instant] =
    for {
      text <- at.downField(field).as[String].leftMap(_ => s"$field is not a moment")
      moment <- Either.catchNonFatal(Instant.parse(text)).leftMap(_ => s"$field is not a moment")
      _ <- Either.cond(
        moment.toString == text,
        (),
        s"$field is not written as a moment is, in UTC and in one way: ${moment.toString}"
      )
      _ <- problem(moment).map(what => s"$field $what").toLeft(())
    } yield moment

  /** A duration of configuration, which is whole milliseconds (`Durations`). */
  def millis(duration: FiniteDuration): Long = {
    require(duration.toNanos % 1000000 == 0, s"$duration is not whole milliseconds")
    duration.toMillis
  }

  def earlier(a: Instant, b: Instant): Instant = if (a.isBefore(b)) a else b
  def later(a: Instant, b: Instant): Instant = if (a.isAfter(b)) a else b
}

/**
 * How far a source has been ingested by its watermark column, as the control plane stores it
 * (`source_state.watermark`): every row whose column is at or before `value`, and that was
 * there to be read when its window was, is in Bronze (ADR-0020).
 *
 * It is the upper bound of the last batch that was committed, and never a value that a batch
 * happened to read. It only moves forward.
 */
final case class StoredWatermark(value: Instant) {
  Moment.problem(value).foreach(what => throw new IllegalArgumentException(s"a watermark $what"))

  /** As the control plane stores it. */
  def toJson: Json = Json.obj(
    "version" -> Json.fromInt(StoredWatermark.Version),
    "value" -> Moment.write(value)
  )
}

object StoredWatermark {

  /**
   * The form of the JSON. A watermark outlives every process: an application that reads
   * another form refuses it, and does not go on from a moment that it guessed.
   */
  val Version = 1

  /** @return the watermark, or what is wrong with the document */
  def fromJson(json: Json): Either[String, StoredWatermark] = {
    val top = json.hcursor
    for {
      _ <- StoredForm.exactly(top, "version", "value")
      _ <- StoredForm.version(top, Version)
      value <- Moment.read(top, "value")
    } yield StoredWatermark(value)
  }
}

/**
 * The plan of a batch of a watermark (ADR-0020, ADR-0005 §3): the rows whose `column` is in
 * `(lower, upper]`. Both bounds are times of the clock of the source, fixed when the batch is
 * planned and written down before a row is read, so a run that replays the plan reads the same
 * window.
 *
 * @param column the watermark column that the bounds are of. A plan says what is read, whole:
 *   one that is replayed under a configuration with another column is refused by its connector
 * @param lower where the window starts, and is not in it; none for the first batch of a source
 *   without `load.watermark.start`, which reads from the first row
 * @param upper where the window ends, and is in it. The commit of the batch moves the watermark
 *   here, whatever the rows of the window were, and though there were none
 * @param horizon how far the invocation that planned this batch is to read: what the clock of
 *   the source said at its first plan, less `safety_margin`. `upper` is never after it
 */
final case class WatermarkPlan private (
    column: SqlIdentifier,
    lower: Option[Instant],
    upper: Instant,
    horizon: Instant
) {

  /**
   * Whether the invocation has more to read after this batch: `max_batch_range` cut the window
   * short of the horizon. It is what a chain of runs goes on by, and it ends: the horizon does
   * not move while the source goes on writing.
   */
  def more: Boolean = upper.isBefore(horizon)

  /** What the commit of this batch records: the next batch counts from its upper bound. */
  def watermark: StoredWatermark = StoredWatermark(upper)

  /** As the control plane stores it, in the write-ahead log of batches. */
  def toJson: Json = Json.obj(
    "version" -> Json.fromInt(WatermarkPlan.Version),
    "column" -> Json.fromString(column.value),
    "lower" -> lower.fold(Json.Null)(Moment.write),
    "upper" -> Moment.write(upper),
    "horizon" -> Moment.write(horizon)
  )
}

object WatermarkPlan {

  /**
   * The form of the JSON. A plan outlives the process that wrote it, by a crash, and may be
   * replayed by another version of the application: one that reads another form refuses it.
   */
  val Version = 1

  /**
   * @throws IllegalArgumentException if a bound is not a moment that a plan holds, or the
   *   window is empty, or ends after the horizon
   */
  def apply(
      column: SqlIdentifier,
      lower: Option[Instant],
      upper: Instant,
      horizon: Instant
  ): WatermarkPlan = {
    def held(name: String, at: Instant): Unit =
      Moment.problem(at).foreach(what => throw new IllegalArgumentException(s"$name $what"))
    lower.foreach(held("lower", _))
    held("upper", upper)
    held("horizon", horizon)
    require(lower.forall(_.isBefore(upper)), "lower is not before upper: the window has no moment")
    require(!upper.isAfter(horizon), "upper is after horizon")
    new WatermarkPlan(column, lower, upper, horizon)
  }

  /**
   * Reads a plan back as it was written.
   *
   * @return the plan, or what is wrong with the document
   */
  def fromJson(json: Json): Either[String, WatermarkPlan] = {
    val top = json.hcursor
    for {
      _ <- StoredForm.exactly(top, "version", "column", "lower", "upper", "horizon")
      _ <- StoredForm.version(top, Version)
      name <- top.get[String]("column").leftMap(_ => "column is not text")
      column <- SqlIdentifier.parse(name).leftMap(problem => s"column $problem")
      lower <-
        if (top.downField("lower").focus.exists(_.isNull)) Right(None)
        else Moment.read(top, "lower").map(Some(_))
      upper <- Moment.read(top, "upper")
      horizon <- Moment.read(top, "horizon")
      plan <- Either
        .catchOnly[IllegalArgumentException](WatermarkPlan(column, lower, upper, horizon))
        .leftMap(_.getMessage.stripPrefix("requirement failed: "))
    } yield plan
  }
}

/**
 * Which window a batch of a watermark reads (ADR-0020; ADR-0005 §3, step 2): a function of the
 * settings of the load, of the watermark of the source and of a horizon, with no I/O and no
 * clock of its own. So it is the same whoever runs it with the same three, and is tested with
 * no database.
 *
 * With `W` the watermark, `H` the horizon and the settings of `load`:
 *
 * {{{
 *   H = what the clock of the source says at the first plan of an invocation - safety_margin
 *   A = W, or start for a source that has no watermark yet      (where a batch counts from)
 *   L = max(W - lookback, start), or start, or none             (the three cases of A)
 *   U = min(H, A + max_batch_range)
 * }}}
 *
 *  - '''The clock is the source's, and never the engine's''': the two machines need not agree
 *    on the time, and the rows were stamped by the source.
 *  - '''`max_batch_range` is counted from the watermark, and not from `L`.''' Counted from `L`,
 *    a `lookback` as long as the range would hold `U` at `W`, and the watermark would never
 *    move.
 *  - '''The first batch of a source without `start` is not cut''': it has nothing to count
 *    from, and reads the table up to the horizon.
 *  - '''`lookback` reads no row at or before `start`''': those are not the source's to ingest.
 *    A `start` that was moved to after the watermark bounds nothing, and skips nothing: the
 *    lower bound of a window is never after the watermark.
 *  - '''The watermark never goes back.''' A horizon that is not after `A` plans no batch: the
 *    clock of the source went back, `safety_margin` was raised, `start` is yet to come, or the
 *    invocation has reached its horizon.
 */
object WatermarkPlanner {

  /** What a run is to do. */
  sealed trait Planning

  final case class Planned(plan: WatermarkPlan) extends Planning

  /**
   * No batch is planned, and the run ends well: the horizon is not after where a batch would
   * count from.
   *
   * @param horizon how far the invocation may read
   * @param from the watermark of the source, or `start` if it has none yet
   */
  final case class NothingNew(horizon: Instant, from: Instant) extends Planning {

    /**
     * How far the horizon is behind where a batch would count from: nothing, when the source
     * is read up to its horizon. More than `safety_margin` says that the clock of the source
     * is behind the watermark itself, and what the source stamps meanwhile is below it.
     */
    def behind: Span = Span.between(horizon, from)
  }

  /**
   * The horizon of an invocation, from the first look at the clock of its source: later plans
   * of the invocation are given it, and do not ask the clock again.
   *
   * @param clock what the source says the time is (`CURRENT_TIMESTAMP`), to the microsecond
   * @return the horizon, or why no plan can be made by that clock: it says a time that no plan
   *   holds, before 1900 or after 9999
   * @throws IllegalArgumentException if the clock is finer than a microsecond
   */
  def horizon(watermark: Load.Watermark, clock: Instant): Either[String, Instant] = {
    require(Moment.toTheMicrosecond(clock), s"the clock $clock is finer than a microsecond")
    val horizon = clock.minusMillis(Moment.millis(watermark.safetyMargin))
    if (clock.isAfter(Timestamps.Max))
      Left(s"the clock of the source says $clock, after ${Timestamps.Max}")
    else if (horizon.isBefore(Timestamps.Min))
      Left(
        s"the clock of the source says $clock, which less safety_margin is before " +
          s"${Timestamps.Min}"
      )
    else Right(horizon)
  }

  /**
   * The next batch of a source, or none.
   *
   * @param watermark where the last batch that was committed left the source; none before the
   *   first
   * @param horizon how far the invocation may read (`horizon`, or that of the plan before in a
   *   chain of runs)
   * @throws IllegalArgumentException if the horizon is not a moment that a plan holds
   */
  def plan(
      load: Load.Incremental,
      watermark: Option[StoredWatermark],
      horizon: Instant
  ): Planning = {
    Moment.problem(horizon).foreach(what => throw new IllegalArgumentException(s"horizon $what"))
    val settings = load.watermark
    val from = watermark.map(_.value).orElse(settings.start)

    from match {
      case Some(counted) if !horizon.isAfter(counted) => NothingNew(horizon, counted)
      case _ =>
        val lower = watermark.map(_.value) match {
          case Some(reached) =>
            // No earlier than start, and than the earliest moment where there is no start; and
            // never after the watermark, wherever start is by now.
            val floor = settings.start.fold(Timestamps.Min)(Moment.earlier(_, reached))
            Some(Moment.later(reached.minusMillis(Moment.millis(settings.lookback)), floor))
          case None => settings.start
        }
        val upper = (from, load.maxBatchRange) match {
          case (Some(counted), Some(range)) =>
            require(range.toNanos > 0, "max_batch_range is no stretch of time")
            Moment.earlier(horizon, counted.plusMillis(Moment.millis(range)))
          case _ => horizon
        }
        Planned(WatermarkPlan(settings.column, lower, upper, horizon))
    }
  }
}
