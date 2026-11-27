package lakehouse.ingestion.engine

import java.sql.SQLException

import scala.concurrent.duration._

import io.delta.exceptions.ConcurrentAppendException
import lakehouse.ingestion.core.config.Profile
import lakehouse.ingestion.core.control.{FailureClass, RunCounts, RunStatus}
import lakehouse.ingestion.engine.failure.RunFailedException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The loop of the retries of a run, and how it settles the transaction that ends it, with
 * attempts and rows that fail or answer as a test says, and waits drawn at their ceiling, so
 * that how long it waits is known.
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

  // ---------------------------------------------------------------------------------------------
  // A wait in the middle of an attempt (ADR-0023 §3)

  private def timed(body: => FiniteDuration): (FiniteDuration, FiniteDuration) = {
    val started = System.nanoTime()
    val said = body
    (said, (System.nanoTime() - started).nanos)
  }

  test("a pause waits within the budget, cut to what is left of it, and is no retry") {
    val short =
      Profile.Retries(budget = 1.second, firstBackoff = 100.millis, maxBackoff = 100.millis)
    val run = retries(short)

    // The budget is counted from the first pause, the failure of the run that it waits after.
    val (first, took) = timed(run.pause(300.millis, may = true))
    first should be >= 300.millis
    took should be >= 300.millis
    // What is left of it, and not what was asked.
    val (second, _) = timed(run.pause(2.seconds, may = true))
    second should be >= 400.millis
    second should be < 1.second
    // Spent: no wait.
    run.pause(1.second, may = true) should be < 100.millis
    run.made shouldBe 0
    run.into(RunCounts.Nothing) shouldBe RunCounts.Nothing

    // And a failure after it is let through, with the budget spent by the pauses.
    val failure = RunFailedException.transient("the disk went away")
    the[Throwable] thrownBy run(may = true)(throw failure) shouldBe theSameInstanceAs(failure)
  }

  test("a pause ends once the run may not go on, and a budget of nothing does not pause") {
    val run = retries()
    var asked = 0
    run.pause(30.seconds, { asked += 1; asked < 3 }) should be < 5.seconds

    val none = Profile.Retries(Duration.Zero, 100.millis, 100.millis)
    retries(none).pause(1.second, may = true) should be < 100.millis
  }

  // ---------------------------------------------------------------------------------------------
  // The transaction that ends a run, settled (ADR-0022 §6)

  /** What `settle` is given, each answering or failing as a test says, and how often it was. */
  final private class Ending(answers: Either[Throwable, Boolean]*) {
    private val left = scala.collection.mutable.Queue(answers: _*)
    var made = 0
    def apply(): Boolean = {
      made += 1
      left.dequeue().fold(failure => throw failure, identity)
    }
  }

  final private class Row(answers: Either[Throwable, RunStatus]*) {
    private val left = scala.collection.mutable.Queue(answers: _*)
    var read = 0
    def apply(): RunStatus = {
      read += 1
      left.dequeue().fold(failure => throw failure, identity)
    }
  }

  private def lostAnswer =
    new SQLException("An I/O error occurred while sending to the backend", "08006")

  test("a transaction that answers is taken at its word, and the row is not read") {
    Seq(true, false).foreach { answer =>
      val run = retries()
      val row = new Row()
      run.settle(RunStatus.Succeeded, row())(answer) shouldBe answer
      row.read shouldBe 0
    }
    // The database said no: it rolled the transaction back.
    Seq(new SQLException("permission denied", "42501"), new IllegalStateException("not planned"))
      .foreach { refused =>
        val run = retries()
        val row = new Row()
        val end = new Ending(Left(refused))
        the[Throwable] thrownBy run.settle(RunStatus.Succeeded, row())(end()) shouldBe
          theSameInstanceAs(refused)
        row.read shouldBe 0
        end.made shouldBe 1
      }
  }

  test("a transaction whose answer is lost and whose row says it ended was committed") {
    Seq(RunStatus.Succeeded, RunStatus.Failed).foreach { ends =>
      withClue(ends) {
        val run = retries()
        val end = new Ending(Left(lostAnswer))
        val row = new Row(Right(ends))
        run.settle(ends, row())(end()) shouldBe true
        end.made shouldBe 1
        row.read shouldBe 1
        // Nothing was done again.
        run.into(RunCounts.Nothing) shouldBe RunCounts.Nothing
      }
    }
  }

  test("one whose row says RUNNING was not committed, and is made again as a retry") {
    val run = retries()
    val end = new Ending(Left(lostAnswer), Right(true))
    val row = new Row(Right(RunStatus.Running))
    run.settle(RunStatus.Succeeded, row())(end()) shouldBe true
    end.made shouldBe 2
    run.into(RunCounts.Nothing) shouldBe RunCounts(retries = Some(1))
  }

  test("one whose row says another run ended this one was not committed") {
    val run = retries()
    val end = new Ending(Left(lostAnswer))
    val row = new Row(Right(RunStatus.Abandoned))
    run.settle(RunStatus.Succeeded, row())(end()) shouldBe false
    end.made shouldBe 1
  }

  test("a row that cannot be read is read again within the budget, and that is no retry") {
    val run = retries()
    val end = new Ending(Left(lostAnswer))
    val row = new Row(Left(lostAnswer), Left(lostAnswer), Right(RunStatus.Succeeded))
    run.settle(RunStatus.Succeeded, row())(end()) shouldBe true
    row.read shouldBe 3
    end.made shouldBe 1
    run.into(RunCounts.Nothing) shouldBe RunCounts.Nothing
  }

  test("a transaction made again that is refused is believed only once the row agrees") {
    // The first was still being committed as the row was read: the one made again waited for
    // it on the lock of the lease, and was refused once it was committed.
    val late = new Row(Right(RunStatus.Running), Right(RunStatus.Succeeded))
    val refused = new Ending(Left(lostAnswer), Right(false))
    retries().settle(RunStatus.Succeeded, late())(refused()) shouldBe true
    late.read shouldBe 2

    // Refused, and still running: it was refused for its lease.
    val stale = new Row(Right(RunStatus.Running), Right(RunStatus.Running))
    val refusedAgain = new Ending(Left(lostAnswer), Right(false))
    retries().settle(RunStatus.Succeeded, stale())(refusedAgain()) shouldBe false

    // Failed for good, and still running: what it failed with is let through.
    val mistake = new IllegalStateException("the state is past the batch")
    val running = new Row(Right(RunStatus.Running), Right(RunStatus.Running))
    val failing = new Ending(Left(lostAnswer), Left(mistake))
    the[Throwable] thrownBy retries().settle(RunStatus.Succeeded, running())(failing()) shouldBe
      theSameInstanceAs(mistake)
  }

  test("one made again whose answer is lost again is settled again") {
    val run = retries()
    val end = new Ending(Left(lostAnswer), Left(lostAnswer))
    val row = new Row(Right(RunStatus.Running), Right(RunStatus.Succeeded))
    run.settle(RunStatus.Succeeded, row())(end()) shouldBe true
    end.made shouldBe 2
    run.into(RunCounts.Nothing) shouldBe RunCounts(retries = Some(1))
  }

  test("a row that cannot be read within the budget leaves how the run ended unknown") {
    val short =
      Profile.Retries(budget = 300.millis, firstBackoff = 100.millis, maxBackoff = 100.millis)
    val run = retries(short)
    val unreadable = RunFailedException.transient("the control plane went away")
    val spent = the[RunFailedException] thrownBy run.settle(RunStatus.Succeeded, throw unreadable)(
      throw lostAnswer
    )

    spent.failure shouldBe FailureClass.Transient
    spent.getCause shouldBe theSameInstanceAs(unreadable)
    spent.getMessage should endWith(
      "and its budget of retries (retries.budget, 300ms) is spent"
    )
    // Nothing was made again: only the row was asked for.
    run.into(RunCounts.Nothing) shouldBe RunCounts.Nothing
  }

  test("the budget of settling is that of the run, counted from its first failure") {
    val short =
      Profile.Retries(budget = 300.millis, firstBackoff = 100.millis, maxBackoff = 100.millis)
    val run = retries(short)
    val attempt = new Failing(RunFailedException.transient("a"))
    run(may = true)(attempt()) shouldBe "done"
    Thread.sleep(400)

    // Spent already: the row is asked for once, and not again.
    val row = new Row(Left(lostAnswer))
    the[Throwable] thrownBy run.settle(RunStatus.Succeeded, row())(throw lostAnswer) shouldBe a[
      RunFailedException
    ]
    row.read shouldBe 1
  }
}
