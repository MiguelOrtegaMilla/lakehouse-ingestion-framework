package lakehouse.ingestion.core.watermark

import java.time.{Duration => Span, Instant}

import scala.concurrent.duration.{Duration, FiniteDuration}

import cats.syntax.all._
import io.circe.Json
import lakehouse.ingestion.core.StoredForm
import lakehouse.ingestion.core.config.{Load, SqlIdentifier, Timestamps}
import lakehouse.ingestion.core.state.{Moment, StoredWatermark}

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

  /** The horizon that the next run of the chain plans against, if there is more. */
  def next: Option[WatermarkHorizon] = Option.when(more)(WatermarkHorizon(horizon))

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
 * The horizon of a chain of runs of a source loaded by a watermark (ADR-0020, point 9): what the
 * first plan of an invocation fixed, and what each later run of it plans against, without
 * asking the clock of the source again. It goes from one run to the next in the process, and is
 * of a form with a version as a plan is, so that a connector reads it back strictly.
 */
final case class WatermarkHorizon(value: Instant) {
  Moment.problem(value).foreach(what => throw new IllegalArgumentException(s"horizon $what"))

  def toJson: Json = Json.obj(
    "version" -> Json.fromInt(WatermarkHorizon.Version),
    "until" -> Moment.write(value)
  )
}

object WatermarkHorizon {

  val Version = 1

  /** Reads a horizon back as it was written, or says what is wrong with the document. */
  def fromJson(json: Json): Either[String, WatermarkHorizon] = {
    val top = json.hcursor
    for {
      _ <- StoredForm.exactly(top, "version", "until")
      _ <- StoredForm.version(top, Version)
      until <- Moment.read(top, "until")
    } yield WatermarkHorizon(until)
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

  /**
   * The next chunk of a backfill, or none once its progress has reached the end of its range
   * (ADR-0021 §5.5).
   *
   * A chunk is a window of ADR-0020 with the same plan, and is planned by `plan`: with the
   * progress of the backfill as its watermark, `from` as its start, `to` as its horizon, `chunk`
   * as its `max_batch_range`, and no lookback. So the first chunk starts at `from` and does not
   * hold it, each next one starts where the last ended, none is longer than `chunk`, and the last
   * ends at `to` and says that there is no more (`WatermarkPlan.more`).
   *
   * What the load of the source says of its watermark is not the backfill's, but for its column:
   * `start` bounds the regular sequence and not a backfill; `lookback` reads the end of the last
   * window again, and the chunks of a backfill follow each other with nothing read twice; and
   * `to` was held to `safety_margin` when the backfill was created (`BackfillRange.settledBy`).
   *
   * @param column the watermark column of the source, which the plan names
   * @param chunk how long a chunk is, at most: whole milliseconds, as a duration of a
   *   configuration is
   * @param progress the upper bound of the last chunk that was committed; none before the first
   * @return the chunk, or `NothingNew` with `to` as its horizon: the backfill is complete
   * @throws IllegalArgumentException if the chunk is no stretch of time, or the progress is not
   *   in the range, which no commit of a chunk of it leaves
   */
  def chunk(
      column: SqlIdentifier,
      range: BackfillRange,
      chunk: FiniteDuration,
      progress: Option[StoredWatermark]
  ): Planning = {
    require(chunk.toNanos > 0, s"a chunk of $chunk is no stretch of time")
    progress.foreach { reached =>
      require(
        range.holds(reached.value),
        s"the progress ${reached.value} is not in the range $range, and no chunk of it ends there"
      )
    }
    val settings = Load.Watermark(column, Duration.Zero, Duration.Zero, start = Some(range.from))
    plan(Load.Incremental(settings, Some(chunk)), progress, range.to)
  }

}
