package lakehouse.ingestion.engine.control

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import scala.concurrent.duration._

import lakehouse.ingestion.core.control.{LeaseLoss, LeaseTiming}
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

/**
 * The heartbeat of a lease, with a database that is played by the test: one that renews, one
 * that says the lease is another's, one that fails, one that never answers.
 *
 * The times are real, and short: a lease of 3 s is renewed every second, tried again 250 ms
 * after a failure, and taken as good for 2.5 s. What a test says about a moment, it says with
 * room for a machine that is busy: one that kept a thread waiting for more than a second would
 * lose a lease of three, and rightly.
 */
final class LeaseHeartbeatSpec extends AnyFunSuite with Matchers with Eventually {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(20, Millis))

  private val Quick = LeaseTiming(3.seconds)

  /** What a test saw of a heartbeat: how often it renewed, and when it said the lease was lost. */
  final private class Watched(
      timing: LeaseTiming,
      renew: Int => Boolean,
      askedAt: Long = System.nanoTime()
  ) {
    val asked: Long = askedAt
    val renewals = new AtomicInteger(0)
    val lostAfter = new AtomicLong(-1L)
    val told = new AtomicInteger(0)
    val why = new AtomicReference[Option[LeaseLoss]](None)
    val heartbeat = new LeaseHeartbeat(
      "test.source/bronze",
      timing,
      asked,
      () => renew(renewals.incrementAndGet())
    )
    heartbeat.onLost { loss =>
      lostAfter.compareAndSet(-1L, System.nanoTime() - asked)
      why.set(Some(loss))
      told.incrementAndGet(): Unit
    }
    def lostAt: FiniteDuration = lostAfter.get.nanos

    /** Waits until the lease is lost and this was told so: it is told a moment after. */
    def awaitLost(): LeaseLoss = {
      eventually(told.get should be >= 1)
      heartbeat.lost shouldBe why.get
      why.get.get
    }
  }

  private def watching[T](
      timing: LeaseTiming,
      renew: Int => Boolean,
      askedAt: Long = System.nanoTime()
  )(
      body: Watched => T
  ): T = {
    val watched = new Watched(timing, renew, askedAt)
    try body(watched)
    finally watched.heartbeat.close()
  }

  private val failing: Int => Boolean = _ =>
    throw new java.sql.SQLException("Connection refused", "08001")

  test("a lease that is renewed stays good, for as long as it is renewed") {
    watching(Quick, _ => true) { watched =>
      // Three times its time to live.
      Thread.sleep(9000)

      watched.heartbeat.lost shouldBe None
      watched.heartbeat.verify()
      watched.told.get shouldBe 0
      // Every second, counted from when the one before was asked for: nine in nine seconds.
      watched.renewals.get should (be >= 7 and be <= 10)
    }
  }

  test("a run whose lease is another's is told at the first renewal, and renews no more") {
    watching(Quick, _ => false) { watched =>
      watched.awaitLost() shouldBe LeaseLoss.TakenOver

      // At the first renewal, a second after the lease was asked for.
      watched.lostAt should (be >= 950.millis and be < Quick.validFor)
      val failure = the[LeaseLostException] thrownBy watched.heartbeat.verify()
      failure.getMessage shouldBe "the lease of test.source/bronze is lost: another run holds it"
      failure.loss shouldBe LeaseLoss.TakenOver

      Thread.sleep(1500)
      watched.renewals.get shouldBe 1
      watched.told.get shouldBe 1
    }
  }

  test("a lease is renewed a third of its time after it was asked for, not after this was made") {
    // Made 800 ms after the lease was asked for, as after a transaction that took its time.
    val asked = System.nanoTime()
    Thread.sleep(800)
    watching(Quick, _ => true, askedAt = asked) { watched =>
      eventually(watched.renewals.get shouldBe 1)

      // At one second from when it was asked for, which is 200 ms from now, and not 1.8 s.
      val first = (System.nanoTime() - asked).nanos
      first should (be >= 950.millis and be < 1700.millis)
      watched.heartbeat.lost shouldBe None
    }
  }

  test("a run that cannot renew its lease stops before the lease expires") {
    // Six seconds: the run takes it as good for five, and has a second to spare.
    val timing = LeaseTiming(6.seconds)
    watching(timing, failing) { watched =>
      watched.awaitLost() shouldBe LeaseLoss.NotRenewed(Some("Connection refused"))

      // Not before it had to, and before the database would give the lease to another.
      watched.lostAt should be >= (timing.validFor - 50.millis)
      watched.lostAt should be < timing.ttl
      // It did not sit and wait: after the first failure, at 2 s, it tried every 500 ms.
      watched.renewals.get should (be >= 4 and be <= 7)
      // The last of them before the time ran out, and none after.
      the[LeaseLostException] thrownBy watched.heartbeat.verify() should have message
        "the lease of test.source/bronze is lost: it was not renewed in time: Connection refused"

      Thread.sleep(800)
      val after = watched.renewals.get
      Thread.sleep(800)
      // Lost is lost: it is not tried again.
      watched.renewals.get shouldBe after
      watched.told.get shouldBe 1
    }
  }

  test("a renewal that never answers does not keep the lease from running out on time") {
    val never = new CountDownLatch(1)
    try
      watching(Quick, _ => { never.await(); true }) { watched =>
        // The renewal is still waiting, and the run was told all the same.
        watched.awaitLost() shouldBe LeaseLoss.NotRenewed(None)
        watched.renewals.get shouldBe 1
        watched.lostAt should (be >= (Quick.validFor - 50.millis) and be < (Quick.validFor + 1.second))
      }
    finally never.countDown()
  }

  test("an answer that comes after the lease ran out does not give it back") {
    val answer = new CountDownLatch(1)
    watching(Quick, _ => { answer.await(10, TimeUnit.SECONDS); true }) { watched =>
      watched.awaitLost() shouldBe LeaseLoss.NotRenewed(None)

      // The database did renew it. The run had stopped by then, and stays stopped.
      answer.countDown()
      Thread.sleep(1200)
      watched.heartbeat.lost shouldBe Some(LeaseLoss.NotRenewed(None))
      a[LeaseLostException] should be thrownBy watched.heartbeat.verify()
      watched.renewals.get shouldBe 1
    }
  }

  test("renewals that fail and then succeed keep the lease: an outage that passed") {
    // The first three fail: at one second, and again 250 ms after each failure.
    watching(Quick, attempt => if (attempt <= 3) failing(attempt) else true) { watched =>
      // Past the two and a half seconds for which the lease was good when it was taken.
      Thread.sleep(3500)

      watched.heartbeat.lost shouldBe None
      watched.heartbeat.verify()
      watched.renewals.get should be >= 4
      watched.told.get shouldBe 0
    }
  }

  test("a process that was frozen past its lease is stopped as soon as it asks") {
    // The lease was asked for ten seconds ago, and nothing has renewed it since.
    val longAgo = System.nanoTime() - 10.seconds.toNanos
    watching(Quick, _ => true, askedAt = longAgo) { watched =>
      val failure = the[LeaseLostException] thrownBy watched.heartbeat.verify()

      failure.loss shouldBe LeaseLoss.NotRenewed(None)
      watched.heartbeat.lost shouldBe Some(LeaseLoss.NotRenewed(None))
      eventually(watched.told.get shouldBe 1)
      Thread.sleep(1500)
      // And it does not renew a lease that it no longer takes as its own: a renewal that is due
      // after the time ran out is not asked for.
      watched.renewals.get shouldBe 0
    }
  }

  test("a heartbeat that is closed renews no more, and tells nobody anything") {
    val watched = new Watched(Quick, _ => true)
    eventually(watched.renewals.get should be >= 1)

    watched.heartbeat.close()
    val after = watched.renewals.get
    // Past the time for which the lease was good: nothing is said of it.
    Thread.sleep(3500)

    watched.renewals.get shouldBe after
    watched.heartbeat.lost shouldBe None
    watched.told.get shouldBe 0
    watched.heartbeat.verify()
    // Closing again is nothing.
    watched.heartbeat.close()
  }

  test("closing a heartbeat interrupts nothing that it is in the middle of") {
    // A reaction that takes its time, as stopping the jobs of Spark does. The thread that closes
    // a heartbeat while it is telling must not cut that short: an interrupt that lands in the
    // middle of logging has left a process with no logging at all.
    val telling = new CountDownLatch(1)
    val interrupted = new AtomicInteger(0)
    val finished = new AtomicInteger(0)
    val watched = new Watched(Quick, _ => false)
    watched.heartbeat.onLost { _ =>
      telling.countDown()
      try Thread.sleep(600)
      catch { case _: InterruptedException => interrupted.incrementAndGet(): Unit }
      finished.incrementAndGet(): Unit
    }

    telling.await(10, TimeUnit.SECONDS) shouldBe true
    watched.heartbeat.close()

    eventually(finished.get shouldBe 1)
    interrupted.get shouldBe 0
  }

  test("everyone who asked to be told is told once, whatever one of them does with it") {
    watching(Quick, _ => false) { watched =>
      val second = new AtomicInteger(0)
      watched.heartbeat.onLost(_ => throw new IllegalStateException("a reaction that fails"))
      watched.heartbeat.onLost(_ => second.incrementAndGet(): Unit)

      watched.awaitLost() shouldBe LeaseLoss.TakenOver
      eventually(second.get shouldBe 1)
      watched.told.get shouldBe 1

      // Who asks after the lease is lost is told at once.
      val late = new AtomicReference[Option[LeaseLoss]](None)
      watched.heartbeat.onLost(loss => late.set(Some(loss)))
      late.get shouldBe Some(LeaseLoss.TakenOver)
      second.get shouldBe 1
    }
  }
}
