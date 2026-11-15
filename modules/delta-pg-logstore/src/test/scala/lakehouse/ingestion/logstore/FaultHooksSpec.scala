package lakehouse.ingestion.logstore

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class FaultHooksSpec extends AnyFunSuite with Matchers {

  private def millisOf(body: => Unit): Long = {
    val start = System.nanoTime()
    body
    (System.nanoTime() - start) / 1000000
  }

  test("a configuration without fault settings selects no hooks at all") {
    FaultHooks.from(new Configuration()) should be theSameInstanceAs FaultHooks.Off
  }

  test("each fault point has its own setting, and a configuration can only make a writer wait") {
    FaultPoint.All.map(FaultHooks.delayKey) shouldBe Seq(
      "lakehouse.logstore.fault.beforeInsert.delayMs",
      "lakehouse.logstore.fault.afterInsert.delayMs",
      "lakehouse.logstore.fault.afterPublish.delayMs"
    )

    val conf = new Configuration()
    conf.setLong(FaultHooks.delayKey(FaultPoint.BeforeInsert), 120L)
    val hooks = FaultHooks.from(conf)

    // Measured with some slack: the sleep and this clock need not agree to the millisecond.
    millisOf(hooks.at(FaultPoint.BeforeInsert)) should be >= 110L
    // The other points are not delayed.
    millisOf {
      hooks.at(FaultPoint.AfterInsert)
      hooks.at(FaultPoint.AfterPublish)
    } should be < 100L
  }

  test("the waits a configuration asks for can be listed, in the order of the protocol") {
    FaultHooks.delaysIn(new Configuration()) shouldBe empty

    val conf = new Configuration()
    conf.setLong(FaultHooks.delayKey(FaultPoint.AfterPublish), 0L)
    conf.setLong(FaultHooks.delayKey(FaultPoint.BeforeInsert), 120L)

    // A wait of zero is still a fault setting that somebody left there.
    FaultHooks.delaysIn(conf) shouldBe Seq(
      FaultPoint.BeforeInsert -> 120L,
      FaultPoint.AfterPublish -> 0L
    )
  }

  test("a delay that is not a number of milliseconds is an error that names the setting") {
    Seq("soon", "-5", "1.5").foreach { text =>
      val conf = new Configuration()
      conf.set(FaultHooks.delayKey(FaultPoint.AfterInsert), text)

      val failure = the[IOException] thrownBy FaultHooks.from(conf)

      failure.getMessage should include("lakehouse.logstore.fault.afterInsert.delayMs")
    }
  }
}
