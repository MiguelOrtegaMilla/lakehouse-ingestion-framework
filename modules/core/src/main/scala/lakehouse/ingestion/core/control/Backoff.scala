package lakehouse.ingestion.core.control

import scala.concurrent.duration._

import lakehouse.ingestion.core.config.Profile

/**
 * How long a run waits before it tries again, after a failure that may pass (ADR-0022 §5).
 *
 * '''A budget of time, from the first failure.''' A run tries again while what is left of its
 * budget (`retries.budget`) is more than nothing, counted from the moment it first failed. The
 * attempt that is in course when the budget runs out is let finish: so, from its first failure,
 * a run ends within its budget plus the length of one attempt.
 *
 * '''Full jitter.''' The wait before the attempt `n` is drawn evenly from nothing to the smaller
 * of `max_backoff` and `first_backoff` doubled `n - 1` times, and is cut to what is left of the
 * budget. Runs that failed together, against one database that went away, do not all come back
 * at the same moment.
 *
 * It has no clock and no randomness of its own: both are given, so that what it decides is the
 * same every time for the same question.
 */
object Backoff {

  /**
   * @param attempt the number of the attempt that is to come, from 1: the first one after the
   *   failure
   * @param since how long ago the run first failed
   * @param draw a number drawn evenly from 0 to the one given, both included
   * @return the wait before the attempt; none, if the budget is spent and no attempt is to start
   */
  def waitBefore(
      retries: Profile.Retries,
      attempt: Int,
      since: FiniteDuration,
      draw: Long => Long
  ): Option[FiniteDuration] = {
    require(attempt >= 1, s"an attempt after a failure is the first or a later one, not $attempt")
    require(since >= Duration.Zero, s"a run failed $since ago")
    val left = retries.budget - since
    Option.when(left > Duration.Zero) {
      val bound = ceiling(retries, attempt).toMillis
      val drawn = draw(bound)
      require(drawn >= 0 && drawn <= bound, s"$drawn was drawn from 0 to $bound")
      drawn.millis.min(left)
    }
  }

  /** `first_backoff` doubled `attempt - 1` times, and no more than `max_backoff`. */
  private def ceiling(retries: Profile.Retries, attempt: Int): FiniteDuration = {
    val doublings = attempt - 1
    // Past this many, the doubling is beyond any max_backoff that a profile may have.
    if (doublings >= 40) retries.maxBackoff
    else (retries.firstBackoff * (1L << doublings)).min(retries.maxBackoff)
  }
}
