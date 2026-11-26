package lakehouse.ingestion.core.control

import scala.concurrent.duration._

import lakehouse.ingestion.core.config.Profile
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class BackoffSpec extends AnyFunSuite with Matchers with OptionValues {

  private val retries =
    Profile.Retries(budget = 2.minutes, firstBackoff = 1.second, maxBackoff = 30.seconds)

  /** The highest that may be drawn: the ceiling of the wait. */
  private val highest: Long => Long = identity
  private val lowest: Long => Long = _ => 0L

  private def ceilingOf(attempt: Int, since: FiniteDuration = Duration.Zero): FiniteDuration =
    Backoff.waitBefore(retries, attempt, since, highest).value

  test("the ceiling of a wait doubles from first_backoff, up to max_backoff") {
    (1 to 8).map(ceilingOf(_)) shouldBe
      Seq(1, 2, 4, 8, 16, 30, 30, 30).map(_.seconds)
  }

  test("a wait is drawn from nothing to its ceiling, both included") {
    val bounds = scala.collection.mutable.Buffer.empty[Long]
    Backoff.waitBefore(retries, 3, 5.seconds, { bound => bounds += bound; 1234L }).value shouldBe
      1234.millis
    bounds shouldBe Seq(4000L)
    Backoff.waitBefore(retries, 3, 5.seconds, lowest).value shouldBe Duration.Zero
  }

  test("a wait is cut to what is left of the budget, counted from the first failure") {
    ceilingOf(6, since = 1.minute + 50.seconds) shouldBe 10.seconds
    ceilingOf(6, since = 2.minutes - 1.milli) shouldBe 1.milli
  }

  test("once the budget is spent, no attempt is to start") {
    Backoff.waitBefore(retries, 1, 2.minutes, highest) shouldBe None
    Backoff.waitBefore(retries, 7, 3.minutes, highest) shouldBe None
    // Nothing is drawn for an attempt that does not start.
    Backoff.waitBefore(retries, 1, 2.minutes, _ => fail("drawn")) shouldBe None
  }

  test("a budget of nothing tries nothing again") {
    val none = Profile.Retries(Duration.Zero, 100.millis, 100.millis)
    Backoff.waitBefore(none, 1, Duration.Zero, highest) shouldBe None
  }

  test("a late attempt neither overflows nor goes past max_backoff") {
    ceilingOf(64) shouldBe 30.seconds
    ceilingOf(Int.MaxValue) shouldBe 30.seconds
  }

  test("a draw outside its bound, an attempt before the first or a failure to come is refused") {
    an[IllegalArgumentException] should be thrownBy
      Backoff.waitBefore(retries, 1, Duration.Zero, bound => bound + 1)
    an[IllegalArgumentException] should be thrownBy
      Backoff.waitBefore(retries, 1, Duration.Zero, _ => -1L)
    an[IllegalArgumentException] should be thrownBy
      Backoff.waitBefore(retries, 0, Duration.Zero, highest)
    an[IllegalArgumentException] should be thrownBy
      Backoff.waitBefore(retries, 1, -1.second, highest)
  }
}
