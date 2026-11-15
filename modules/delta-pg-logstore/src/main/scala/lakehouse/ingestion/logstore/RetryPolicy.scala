package lakehouse.ingestion.logstore

import java.io.{InterruptedIOException, IOException}

import scala.annotation.tailrec
import scala.concurrent.duration._

import org.slf4j.LoggerFactory

/**
 * How many times a step of the commit protocol is tried when it fails with an `IOException`,
 * and how long the writer waits before trying again: twice as long each time.
 *
 * Only two steps are repeated, the two that leave something behind when they fail: taking a
 * version, whose outcome is unknown when the reply is lost (ADR-0004 §3.2), and publishing a
 * commit whose version is taken (ADR-0017). Everything before them just fails the commit.
 *
 * The repetitions are few and the waits short, so that a storage or a database that stays
 * broken becomes a failure within seconds and not a hang.
 */
final case class RetryPolicy(attempts: Int, firstPause: FiniteDuration) {
  require(attempts >= 1, s"a step is tried at least once, not $attempts times")

  /**
   * Runs `step`, again after each `IOException` while attempts are left.
   *
   * @param what the step, for the log
   * @throws java.io.IOException the failure of the last attempt
   */
  @throws[IOException]
  def apply[T](what: => String)(step: => T): T = {
    @tailrec
    def attempt(number: Int, pause: FiniteDuration): T = {
      val outcome =
        try Right(step)
        catch { case e: IOException if number < attempts => Left(e) }
      outcome match {
        case Right(result) => result
        case Left(failure) =>
          RetryPolicy.logger.warn(
            s"Could not $what (attempt $number of $attempts), trying again in $pause",
            failure
          )
          RetryPolicy.pause(pause)
          attempt(number + 1, pause * 2)
      }
    }
    attempt(1, firstPause)
  }
}

object RetryPolicy {

  private val logger = LoggerFactory.getLogger(classOf[RetryPolicy])

  /** Three attempts, 100 ms and 200 ms apart. */
  val Default: RetryPolicy = RetryPolicy(attempts = 3, firstPause = 100.millis)

  private def pause(length: FiniteDuration): Unit =
    try Thread.sleep(length.toMillis)
    catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new InterruptedIOException("interrupted while waiting to repeat a commit step")
    }
}
