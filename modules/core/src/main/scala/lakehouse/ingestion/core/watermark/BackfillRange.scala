package lakehouse.ingestion.core.watermark

import java.time.Instant

import lakehouse.ingestion.core.state.Moment

/**
 * The range of the watermark column that a backfill reads, `(from, to]` (ADR-0021 §5): the
 * rows whose watermark is after `from` and at or before `to`. It is fixed when the backfill is
 * created, and read in chunks, each a window of ADR-0020 (`WatermarkPlanner.chunk`).
 *
 *  - '''`from` is not in it''', as the lower bound of a window is not: a row whose watermark is
 *    `from` exactly is not read. A backfill that ends where the regular sequence of its source
 *    begins, `to` at `load.watermark.start`, meets it with no gap and no overlap, since the first
 *    window of that sequence starts at `start` and does not hold it either.
 *  - '''`load.watermark.start` does not bound it.''' `start` is where the regular sequence
 *    begins, and a backfill is how the history before it is loaded.
 *  - '''`to` has settled when the backfill is created''' (`settledBy`): it is not after the
 *    clock of the source less `safety_margin`, so no chunk reads time in which a transaction
 *    may still commit rows (ADR-0020, point 3). The range is not asked again: by the time a
 *    later chunk is read, `to` is further behind the clock than it was.
 *
 * Both are moments that a plan holds: from 1900 to the end of 9999, to the microsecond. A range is
 * made by `of` alone, which says why it refuses one: an abstract case class has neither `apply`
 * nor `copy` to make one that it would refuse.
 */
sealed abstract case class BackfillRange private (from: Instant, to: Instant) {

  /**
   * The range, if `to` is not after `horizon`: what the clock of the source says less
   * `safety_margin` (`WatermarkPlanner.horizon`), when the backfill is created.
   *
   * @return the range, or why it is refused
   */
  def settledBy(horizon: Instant): Either[String, BackfillRange] =
    if (to.isAfter(horizon))
      Left(
        s"to, $to, is after $horizon, the clock of the source less safety_margin: a backfill " +
          "does not read time in which a transaction may still commit rows. A range that ends " +
          s"at $horizon or before is read"
      )
    else Right(this)

  /** Whether `at` is in the range: after `from`, and not after `to`. */
  def holds(at: Instant): Boolean = at.isAfter(from) && !at.isAfter(to)

  override def toString: String = s"($from, $to]"
}

object BackfillRange {

  /**
   * A range from its bounds, as an operator gives them or the control plane has them.
   *
   * @return the range, or why it is refused: a bound that is not a moment that a plan holds, or
   *   a `from` that is not before `to`, which leaves the range with no moment
   */
  def of(from: Instant, to: Instant): Either[String, BackfillRange] =
    Moment
      .problem(from)
      .map(what => s"from $what")
      .orElse(Moment.problem(to).map(what => s"to $what"))
      .orElse(
        Option.when(!from.isBefore(to))(
          s"from, $from, is not before to, $to: the range (from, to] holds no moment"
        )
      )
      .toLeft(new BackfillRange(from, to) {})
}
