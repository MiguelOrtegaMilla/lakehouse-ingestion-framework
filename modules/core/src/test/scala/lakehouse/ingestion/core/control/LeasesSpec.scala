package lakehouse.ingestion.core.control

import scala.concurrent.duration._

import lakehouse.ingestion.core.config.Profile
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The times of a lease, and what a run that lost one is told (ADR-0006). */
final class LeasesSpec extends AnyFunSuite with Matchers {

  test("a lease of two minutes is renewed every forty seconds, as ADR-0006 has it") {
    val timing = LeaseTiming(Profile.Leases.Default.ttl)

    timing.ttl shouldBe 2.minutes
    timing.heartbeat shouldBe 40.seconds
    // After a renewal that failed, the next is not waited for as long.
    timing.retry shouldBe 10.seconds
    // The run stops 20 s before the database would give its lease to another.
    timing.validFor shouldBe 100.seconds
  }

  test("a run takes its lease as good for less than the database holds it") {
    Seq(120.millis, 1.second, 9.seconds, 45.seconds, 2.minutes, 1.hour, 1.day).foreach { ttl =>
      val timing = LeaseTiming(ttl)
      withClue(s"$ttl: ") {
        timing.validFor should be < timing.ttl
        // A renewal is due, and may fail and be tried again, before the run gives its lease up.
        timing.heartbeat should be < timing.validFor
        (timing.heartbeat + timing.retry * 2) should be <= timing.validFor
        timing.retry should be < timing.heartbeat
        timing.retry should be > Duration.Zero
      }
    }
  }

  test("the time between two renewals is the third that the rule of the profile is about") {
    // The profile refuses a lease shorter than three renewals that take as long as they may.
    LeaseTiming(45.seconds).heartbeat shouldBe 15.seconds
    Profile.Leases.RenewalsInATtl shouldBe 3
  }

  test("a lease too short to schedule anything by is not one") {
    an[IllegalArgumentException] should be thrownBy LeaseTiming(100.millis)
    an[IllegalArgumentException] should be thrownBy LeaseTiming(Duration.Zero)
  }

  test("a run that lost its lease is told which way") {
    LeaseLoss.TakenOver.reason shouldBe "another run holds it"
    LeaseLoss.NotRenewed(None).reason shouldBe "it was not renewed in time"
    LeaseLoss.NotRenewed(Some("Connection refused")).reason shouldBe
      "it was not renewed in time: Connection refused"
  }
}
