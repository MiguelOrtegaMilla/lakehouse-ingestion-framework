package lakehouse.ingestion.engine

import java.util.concurrent.ThreadLocalRandom

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

import lakehouse.ingestion.core.config.{Durations, Profile}
import lakehouse.ingestion.core.control.{Backoff, FailureClass, RunCounts, RunStatus}
import lakehouse.ingestion.engine.control.Settling
import lakehouse.ingestion.engine.failure.RunFailedException
import org.slf4j.LoggerFactory

/**
 * The retries of one run (ADR-0022 §4, §5, §6): what is tried again, how long it waits first,
 * and how many times it was, which the run records.
 *
 * A failure that may pass, or a conflict that Delta asks to be tried again, has the run try
 * again, while it may, after a wait that `Backoff` gives: within the budget of the profile,
 * counted from the run's first failure. Whatever else it fails with is let through as it is. A
 * failure that outlasts the budget is let through with what was tried said in its message, and
 * keeps its class.
 *
 * The transaction that ends the run is settled within the same budget (`settle`): when it gives
 * no answer, the run reads its own row, and makes the transaction again only if the row says
 * that it was not committed. A read of the row is not a retry of the run, which asks and does
 * nothing again; it waits as one does, and draws on the budget.
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
) extends Settling {
  import RunRetries._

  private var afterFailures = 0
  private var afterConflicts = 0
  // Every wait, whatever was asked again after it: what the waits grow with.
  private var waits = 0
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
    while (answer.isEmpty)
      try answer = Some(attempt)
      catch {
        case NonFatal(failure) =>
          val (again, wait) = nextWait(failure, may)
          log.warn(
            s"$what failed (${again.failure}), and tries again in ${wait.toMillis} ms " +
              s"(retry ${made + 1}): ${again.getMessage}"
          )
          // The run may not go on once it has lost its lease while it waits: it fails with
          // what it waited after, and whoever ends it finds the lease lost.
          waitOut(wait, may, failure)
          count(again.failure)
      }
    answer.get
  }

  /**
   * The transaction that ends the run, settled (ADR-0022 §6). Its answer is taken when it gives
   * one: committed, refused, or failed for good, which the database says only of a transaction
   * that it rolled back. A failure that may pass may have come with the transaction committed,
   * and the run's own row is read, again within the budget until it is:
   *  - `ends`: the transaction was committed, and its answer was lost;
   *  - `RUNNING`: it was not committed. It is made again after a wait, as a retry of the run,
   *    and its own conditions, the epoch and what it ends, take it or refuse it as ever;
   *  - any other status: another run ended this one, `ABANDONED` once it took the lease over,
   *    and the transaction was not committed.
   *
   * A transaction made again that is refused, or fails for good, may have waited on the lock of
   * the lease for the one before it, which the database was still finishing as the row was read:
   * the row is read once more, and says whether that one was committed.
   *
   * @throws java.lang.Throwable a failure that may pass and outlasted the budget, said so in a
   *   `RunFailedException` of its class: the row of the run is what the transaction left, and
   *   is not known
   */
  override def settle(ends: RunStatus, status: => RunStatus)(end: => Boolean): Boolean =
    try end
    catch {
      case NonFatal(failure) if passes(failure) =>
        sinceFirst(): Unit
        log.warn(
          s"$what got no answer from the transaction that ends it, which may have been " +
            s"committed: it reads its own row. ${describe(failure)}"
        )
        lost(ends, status, end, failure)
    }

  /** After `end` failed with `failure`, with which it may have been committed. */
  @tailrec
  private def lost(
      ends: RunStatus,
      status: => RunStatus,
      end: => Boolean,
      failure: Throwable
  ): Boolean =
    standing(status) match {
      case `ends` =>
        log.info(s"$what finds the transaction that ends it committed, and its answer lost")
        true
      case RunStatus.Running =>
        val (again, wait) = nextWait(failure, may = true)
        log.warn(
          s"$what finds the transaction that ends it not committed, and makes it again in " +
            s"${wait.toMillis} ms (retry ${made + 1}): ${again.getMessage}"
        )
        waitOut(wait, may = true, failure)
        count(again.failure)
        Try(end) match {
          case Success(true) => true
          case Failure(next) if passes(next) => lost(ends, status, end, next)
          case answer => standing(status) == ends || answer.get
        }
      case other =>
        log.warn(
          s"$what finds itself ${other.name}: another run ended it, and the transaction that " +
            "ends it was not committed"
        )
        false
    }

  /**
   * The status of the run's own row, read again after each failure that may pass, within the
   * budget. Not a retry of the run: nothing is done again.
   */
  private def standing(status: => RunStatus): RunStatus = {
    var answer = Option.empty[RunStatus]
    while (answer.isEmpty)
      try answer = Some(status)
      catch {
        case NonFatal(failure) =>
          val (again, wait) = nextWait(failure, may = true)
          log.warn(
            s"$what could not read its own row (${again.failure}), and reads it again in " +
              s"${wait.toMillis} ms: ${again.getMessage}"
          )
          waitOut(wait, may = true, failure)
      }
    answer.get
  }

  /**
   * The wait before what failed with `failure` is asked again, with the class of the failure.
   *
   * @throws java.lang.Throwable `failure`, if it does not pass or the run may not go on; or
   *   what it is said with once the budget is spent
   */
  private def nextWait(
      failure: Throwable,
      may: => Boolean
  ): (RunFailedException, FiniteDuration) = {
    val again = Failures.Known
      .unapply(failure)
      .filter(classed => Passes(classed.failure) && may)
      .getOrElse(throw failure)
    val since = sinceFirst()
    Backoff.waitBefore(retries, waits + 1, since, draw) match {
      case None => throw spent(again, failure, since)
      case Some(wait) => (again, wait)
    }
  }

  /** Waits `wait`, and throws `failure` if the run may no longer go on after it. */
  private def waitOut(wait: FiniteDuration, may: => Boolean, failure: Throwable): Unit = {
    if (!waited(wait, may)) throw failure
    waits += 1
  }

  private def count(failure: FailureClass): Unit = failure match {
    case FailureClass.Conflict => afterConflicts += 1
    case _ => afterFailures += 1
  }

  /** How long since the run's first failure, which is now if it had none. */
  private def sinceFirst(): FiniteDuration = {
    val now = System.nanoTime()
    if (firstFailure.isEmpty) firstFailure = Some(now)
    (now - firstFailure.get).nanos
  }

  /** What a failure that outlasted the budget is said with: what was tried, and for how long. */
  private def spent(
      classed: RunFailedException,
      failure: Throwable,
      since: FiniteDuration
  ): Throwable =
    if (waits == 0) failure
    else
      new RunFailedException(
        classed.failure,
        s"${classed.getMessage}. The run tried again $waits times in " +
          s"${Durations.render(since.toSeconds.seconds)}, and its budget of retries " +
          s"(retries.budget, ${Durations.render(retries.budget)}) is spent",
        failure
      )
}

private[engine] object RunRetries {

  private val log = LoggerFactory.getLogger(classOf[RunRetries])

  /** What is tried again: a failure that may pass, and a conflict that Delta asks to retry. */
  private val Passes: Set[FailureClass] = Set(FailureClass.Transient, FailureClass.Conflict)

  /** Whether a failure of this class is tried again. */
  def mayPass(failure: FailureClass): Boolean = Passes(failure)

  private def passes(failure: Throwable): Boolean =
    Failures.Known.unapply(failure).exists(classed => Passes(classed.failure))

  private def describe(failure: Throwable): String =
    Failures.Known
      .unapply(failure)
      .fold(failure.toString)(classed => s"It failed (${classed.failure}): ${classed.getMessage}")

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
