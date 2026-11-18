package lakehouse.ingestion.core.control

import scala.concurrent.duration._

/**
 * The times of a lease, all from its time to live (ADR-0006).
 *
 * A run that holds a lease renews it in the database every third of the time to live. The
 * database is what says who holds a lease, by its own clock. The run, for its part, keeps count
 * of how long its lease is still good by what it last saw renewed, and stops working when that
 * runs out, a sixth of the time to live before the database would let another run take the
 * lease (self-fencing).
 *
 * With two minutes: a renewal every 40 s; after one that fails, another try every 10 s; and a
 * run that has seen no renewal for 100 s stops, 20 s before its lease expires.
 *
 * @param ttl how long the database holds the lease for, counted from each renewal
 */
final case class LeaseTiming(ttl: FiniteDuration) {
  require(ttl >= LeaseTiming.Shortest, s"a lease of $ttl is shorter than ${LeaseTiming.Shortest}")

  /** How long after a renewal the next one is made. */
  val heartbeat: FiniteDuration = ttl / 3

  /** How long after a renewal that failed the next one is tried: sooner than after one that did not. */
  val retry: FiniteDuration = ttl / 12

  /**
   * For how long, counted from when a renewal was asked for, the run that asked takes its lease
   * as good. It is shorter than the time to live by a sixth of it, and counted from before the
   * database started counting, so it runs out first: by that sixth, less whatever the clock of
   * the database was moved forward by in the meantime.
   */
  val validFor: FiniteDuration = ttl - ttl / 6
}

object LeaseTiming {

  /** Below this the parts of a lease are no times to schedule anything by. */
  val Shortest: FiniteDuration = 120.millis
}

/** Why a run that held a lease no longer takes it as its own. */
sealed trait LeaseLoss {
  def reason: String
}

object LeaseLoss {

  /**
   * No renewal was seen in time. The control plane could not be reached, or did not answer:
   * the lease may still be this run's in the database, and the run cannot know. It stops.
   *
   * @param lastFailure what the last renewal that was tried failed with
   */
  final case class NotRenewed(lastFailure: Option[String]) extends LeaseLoss {
    override def reason: String =
      "it was not renewed in time" + lastFailure.fold("")(failure => s": $failure")
  }

  /**
   * The database says that the lease is not this run's: it expired, and another run took it.
   * This run's own row says `ABANDONED` by then.
   */
  case object TakenOver extends LeaseLoss {
    override def reason: String = "another run holds it"
  }
}
