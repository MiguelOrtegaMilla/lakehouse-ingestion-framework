package lakehouse.ingestion.engine

import java.sql.SQLException

import scala.concurrent.duration._

import io.delta.exceptions.ConcurrentAppendException
import lakehouse.ingestion.core.config.Profile
import lakehouse.ingestion.core.control.{FailureClass, RunCounts}
import lakehouse.ingestion.engine.failure.RunFailedException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The loop of the retries of a run, with attempts that fail as a test says, and waits drawn at
 * their ceiling, so that how long it waits is known.
 */
final class RunRetriesSpec extends AnyFunSuite with Matchers {

  private val budget =
    Profile.Retries(budget = 10.seconds, firstBackoff = 50.millis, maxBackoff = 200.millis)

  private def retries(of: Profile.Retries = budget) =
    new RunRetries(of, "the run of the test", draw = identity)

  /** An attempt that fails with each of `failures` in turn, and then answers. */
  final private class Failing(failures: Throwable*) {
    private val left = scala.collection.mutable.Queue(failures: _*)
    var attempts = 0
    def apply(): String = {
      attempts += 1
      if (left.nonEmpty) throw left.dequeue() else "done"
    }
  }

  test("a failure that may pass is tried again after waits that double, and is counted") {
    // Waits long enough that nothing but the waits takes as long.
    val run = retries(
      Profile.Retries(budget = 1.minute, firstBackoff = 300.millis, maxBackoff = 1200.millis)
    )
    val attempt = new Failing(
      RunFailedException.transient("a"),
      new SQLException("connection lost", "08006"),
      RunFailedException.transient("c")
    )
    val started = System.nanoTime()
    run(may = true)(attempt()) shouldBe "done"
    val took = (System.nanoTime() - started).nanos

    attempt.attempts shouldBe 4
    run.made shouldBe 3
    // 300, 600 and 1200 ms: the ceilings, as drawn here.
    took should be >= 2100.millis
    run.into(RunCounts.Nothing) shouldBe RunCounts(retries = Some(3))
  }

  test("a conflict is tried again and counted apart, beside the conflicts of the writes") {
    val run = retries()
    val attempt = new Failing(new ConcurrentAppendException("files were added"))
    run(may = true)(attempt()) shouldBe "done"
    run.into(RunCounts(writeConflicts = Some(1))) shouldBe RunCounts(writeConflicts = Some(2))
  }

  test("a run that tried nothing again records none") {
    val run = retries()
    run(may = true)("done") shouldBe "done"
    run.into(RunCounts.Nothing) shouldBe RunCounts.Nothing
  }

  test("what does not pass is let through at once, as it is") {
    Seq(
      RunFailedException.permanent("a configuration"),
      new RunFailedException(FailureClass.Data, "too many rejected"),
      new IllegalStateException("a mistake"),
      new SQLException("permission denied", "42501")
    ).foreach { failure =>
      withClue(failure.toString) {
        val run = retries()
        val attempt = new Failing(failure)
        the[Throwable] thrownBy run(may = true)(attempt()) shouldBe theSameInstanceAs(failure)
        attempt.attempts shouldBe 1
        run.made shouldBe 0
      }
    }
  }

  test("nothing is tried again where the run may not, nor once it may not while it waits") {
    val failure = RunFailedException.transient("the database went away")
    val refused = retries()
    the[Throwable] thrownBy refused(may = false)(new Failing(failure)()) shouldBe
      theSameInstanceAs(failure)
    refused.made shouldBe 0

    // It may when it fails, and may not any more during the wait: the wait ends early.
    val slow =
      Profile.Retries(budget = 1.minute, firstBackoff = 30.seconds, maxBackoff = 30.seconds)
    val run = retries(slow)
    var asked = 0
    val started = System.nanoTime()
    the[Throwable] thrownBy run { asked += 1; asked < 3 }(new Failing(failure)()) shouldBe
      theSameInstanceAs(failure)
    (System.nanoTime() - started).nanos should be < 5.seconds
    run.made shouldBe 0
  }

  test("a failure that outlasts the budget keeps its class, and says what was tried") {
    val short =
      Profile.Retries(budget = 300.millis, firstBackoff = 100.millis, maxBackoff = 100.millis)
    val run = retries(short)
    val failure = RunFailedException.transient("the database went away")
    val spent = the[RunFailedException] thrownBy run(may = true)(throw failure)

    spent.failure shouldBe FailureClass.Transient
    spent.getCause shouldBe theSameInstanceAs(failure)
    spent.getMessage should startWith("the database went away. The run tried again ")
    spent.getMessage should endWith(
      "and its budget of retries (retries.budget, 300ms) is spent"
    )
    run.made should be >= 2
  }

  test("a budget of nothing lets the first failure through as it is") {
    val none = Profile.Retries(Duration.Zero, 100.millis, 100.millis)
    val failure = RunFailedException.transient("the database went away")
    the[Throwable] thrownBy retries(none)(may = true)(throw failure) shouldBe
      theSameInstanceAs(failure)
  }
}
