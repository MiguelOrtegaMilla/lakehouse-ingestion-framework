package lakehouse.ingestion.logstore

import java.io.{InterruptedIOException, IOException}

import scala.concurrent.duration._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class RetryPolicySpec extends AnyFunSuite with Matchers {

  private val quick = RetryPolicy(attempts = 3, firstPause = 1.milli)

  /** A step that fails `failures` times with `failure`, and counts how often it was tried. */
  final private class Step(failures: Int, failure: => Exception = new IOException("not yet")) {
    var tries = 0

    def run(): String = {
      tries += 1
      if (tries <= failures) throw failure
      s"done at try $tries"
    }
  }

  test("a step that succeeds is tried once") {
    val step = new Step(failures = 0)

    quick("do it")(step.run()) shouldBe "done at try 1"
  }

  test("a step that fails with an IOException is tried again, until the attempts run out") {
    val recovers = new Step(failures = 2)
    quick("do it")(recovers.run()) shouldBe "done at try 3"

    val never = new Step(failures = 10)
    val failure = the[IOException] thrownBy quick("do it")(never.run())
    failure.getMessage shouldBe "not yet"
    never.tries shouldBe 3
  }

  test("any other failure is not repeated: it is not the storage or the database failing") {
    val broken = new Step(failures = 10, new IllegalStateException("a defect"))

    an[IllegalStateException] should be thrownBy quick("do it")(broken.run())

    broken.tries shouldBe 1
  }

  test("the wait doubles after each attempt: by default 100 ms, then 200 ms") {
    RetryPolicy.Default shouldBe RetryPolicy(attempts = 3, firstPause = 100.millis)

    val step = new Step(failures = 2)
    val start = System.nanoTime()
    RetryPolicy(attempts = 3, firstPause = 40.millis)("do it")(step.run())

    // 40 ms, then 80 ms, measured with some slack.
    (System.nanoTime() - start).nanos should be >= 110.millis
  }

  test("an interrupted wait ends the step, and the thread stays interrupted") {
    val step = new Step(failures = 10)

    Thread.currentThread().interrupt()
    try {
      an[InterruptedIOException] should be thrownBy
        RetryPolicy(attempts = 3, firstPause = 10.seconds)("do it")(step.run())
      Thread.currentThread().isInterrupted shouldBe true
    } finally Thread.interrupted(): Unit

    step.tries shouldBe 1
  }
}
