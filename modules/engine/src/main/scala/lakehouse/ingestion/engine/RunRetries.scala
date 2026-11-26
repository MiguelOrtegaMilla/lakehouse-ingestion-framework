package lakehouse.ingestion.engine

import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.duration._
import scala.util.control.NonFatal

import lakehouse.ingestion.core.config.{Durations, Profile}
import lakehouse.ingestion.core.control.{Backoff, FailureClass, RunCounts}
import lakehouse.ingestion.engine.failure.RunFailedException
import org.slf4j.LoggerFactory

/**
 * The retries of one run (ADR-0022 §4, §5): what is tried again, how long it waits first, and
 * how many times it was, which the run records.
 *
 * A failure that may pass, or a conflict that Delta asks to be tried again, has the run try
 * again, while it may, after a wait that `Backoff` gives: within the budget of the profile,
 * counted from the run's first failure. Whatever else it fails with is let through as it is. A
 * failure that outlasts the budget is let through with what was tried said in its message, and
 * keeps its class.
 *
 * It is the run's, and is used on the thread of the run alone.
 *
 * @param what the run, as its log says it
 * @param draw a number drawn evenly from 0 to the one given, both included
 */
final private[engine] class RunRetries(
    retries: Profile.Retries,
    what: String,
    draw: Long => Long = RunRetries.Uniform
) {
  import RunRetries._

  private var afterFailures = 0
  private var afterConflicts = 0
  private var firstFailure = Option.empty[Long]

  /** How many times the run tried again, for either reason. */
  def made: Int = afterFailures + afterConflicts

  /**
   * The counts of the run, with its retries: after a failure that passes in `retries`, after a
   * conflict in `writeConflicts`, beside the conflicts that its writes counted. A run that tried
   * nothing again records none, as one did before the engine tried anything again.
   */
  def into(counts: RunCounts): RunCounts =
    counts.copy(
      retries =
        if (afterFailures == 0) counts.retries
        else Some(counts.retries.getOrElse(0) + afterFailures),
      writeConflicts =
        if (afterConflicts == 0) counts.writeConflicts
        else Some(counts.writeConflicts.getOrElse(0) + afterConflicts)
    )

  /**
   * `attempt`, again after each failure that is tried again, until it gives an answer.
   *
   * @param may whether the run may try again now, asked after a failure and while it waits:
   *   not once it has lost its lease, nor once it has started the transaction that ends it
   * @throws java.lang.Throwable what the last attempt failed with: one that is not tried
   *   again, or one that outlasted the budget, said so in a `RunFailedException` of its class
   */
  def apply[A](may: => Boolean)(attempt: => A): A = {
    var answer = Option.empty[A]
    while (answer.isEmpty) {
      val failed =
        try {
          answer = Some(attempt)
          None
        } catch { case NonFatal(failure) => Some(failure) }
      failed.foreach { failure =>
        val again = Failures.Known
          .unapply(failure)
          .filter(classed => Passes(classed.failure) && may)
          .getOrElse(throw failure)
        val now = System.nanoTime()
        val since = (now - firstFailure.getOrElse(now)).nanos
        if (firstFailure.isEmpty) firstFailure = Some(now)
        Backoff.waitBefore(retries, made + 1, since, draw) match {
          case None => throw spent(again, failure, since)
          case Some(wait) =>
            log.warn(
              s"$what failed (${again.failure}), and tries again in ${wait.toMillis} ms " +
                s"(retry ${made + 1}): ${again.getMessage}"
            )
            // The run may not go on once it has lost its lease while it waits: it fails with
            // what it waited after, and whoever ends it finds the lease lost.
            if (!waited(wait, may)) throw failure
            again.failure match {
              case FailureClass.Conflict => afterConflicts += 1
              case _ => afterFailures += 1
            }
        }
      }
    }
    answer.get
  }

  /** What a failure that outlasted the budget is said with: what was tried, and for how long. */
  private def spent(
      classed: RunFailedException,
      failure: Throwable,
      since: FiniteDuration
  ): Throwable =
    if (made == 0) failure
    else
      new RunFailedException(
        classed.failure,
        s"${classed.getMessage}. The run tried again $made times in " +
          s"${Durations.render(since.toSeconds.seconds)}, and its budget of retries " +
          s"(retries.budget, ${Durations.render(retries.budget)}) is spent",
        failure
      )
}

private[engine] object RunRetries {

  private val log = LoggerFactory.getLogger(classOf[RunRetries])

  /** What is tried again: a failure that may pass, and a conflict that Delta asks to retry. */
  private val Passes: Set[FailureClass] = Set(FailureClass.Transient, FailureClass.Conflict)

  /** How often a wait looks at whether the run may still go on. */
  private val Look = 100.millis

  val Uniform: Long => Long = bound => ThreadLocalRandom.current().nextLong(bound + 1)

  /** Waits `wait`, or less if the run may no longer go on: whether it may, after it. */
  private def waited(wait: FiniteDuration, may: => Boolean): Boolean = {
    val until = System.nanoTime() + wait.toNanos
    var left = wait.toNanos
    while (left > 0 && may) {
      Thread.sleep(math.max(1L, math.min(left, Look.toNanos) / 1000000L))
      left = until - System.nanoTime()
    }
    may
  }

}
