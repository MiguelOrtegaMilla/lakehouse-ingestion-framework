package lakehouse.ingestion.engine.control

import java.sql.SQLException
import java.time.Instant

import lakehouse.ingestion.core.config.ConfigVersion
import lakehouse.ingestion.core.control._
import org.slf4j.LoggerFactory

/**
 * A run that holds the lease of its source (ADR-0006): what the engine has in its hands from
 * the moment it may work to the moment it says how it ended.
 *
 * It is made by `LeasedRun.open`, which takes the lease and writes the run in one transaction.
 * From then on the lease is renewed behind it (`LeaseHeartbeat`), and the run asks `verify`
 * before each thing it must not do without its lease.
 *
 * @param epoch the epoch of the lease: what every statement that advances the state of the
 *   source names, so that a run whose lease was taken changes nothing
 * @param tookOver the run whose lease this one took after it expired, if there was one: it is
 *   marked `ABANDONED`, and what it left halfway is this run's to recover
 */
final class LeasedRun private (
    database: ControlDatabase,
    val start: RunStart,
    val epoch: Epoch,
    val tookOver: Option[RunId],
    heartbeat: LeaseHeartbeat
) extends AutoCloseable {

  /** Why the lease is lost, if it is. */
  def lost: Option[LeaseLoss] = heartbeat.lost

  /** @throws LeaseLostException if the run has lost its lease, and must stop */
  @throws[LeaseLostException]
  def verify(): Unit = heartbeat.verify()

  /** What to do the moment the lease is lost: stop what the run is in the middle of. */
  def onLost(callback: LeaseLoss => Unit): Unit = heartbeat.onLost(callback)

  /**
   * Ends the run, and gives its lease up, in one transaction. The lease is no longer renewed,
   * whatever the answer.
   *
   * What decides is the database, not what this process believes of its lease: the run is
   * ended only if it is still `RUNNING`, which it is not once another run has taken its lease.
   *
   * @return false if another run ended this one, as `ABANDONED`: this process was a zombie,
   *   and nothing was changed
   * @throws java.sql.SQLException if the control plane could not be told. The run stays
   *   `RUNNING` and its lease expires: the next run of the source takes it over
   */
  @throws[SQLException]
  def finish(end: RunEnd): Boolean = {
    heartbeat.close()
    database.transaction { connection =>
      val ended = Runs.end(connection, start.id, end)
      Leases.release(connection, start.leaseKey, start.id, epoch): Unit
      ended
    }
  }

  /**
   * Stops renewing the lease, and tells the control plane nothing: what is left of a process
   * that died. The run stays `RUNNING`, and its lease expires.
   */
  override def close(): Unit = heartbeat.close()
}

object LeasedRun {

  private val logger = LoggerFactory.getLogger(classOf[LeasedRun])

  /**
   * The lease was taken, and the run was recorded as `SKIPPED_LOCKED`: nothing was read or
   * written, and it is not a failure (exit code 2).
   *
   * @param holder the run that holds the lease
   * @param until when its lease expires, unless it is renewed
   */
  final case class Skipped(start: RunStart, holder: RunId, instance: InstanceId, until: Instant)

  /**
   * Starts a run: stores its configuration, takes the lease of its source and writes the run,
   * `RUNNING` under the epoch of the lease, or `SKIPPED_LOCKED` if another run holds it.
   *
   * The lease and the run are one transaction. So are the lease and the end of the run that
   * held it before and let it expire: no moment has a lease whose holder is not a run that is
   * running.
   *
   * @param config the configuration that `start` names by its hash
   */
  @throws[SQLException]
  def open(
      database: ControlDatabase,
      start: RunStart,
      config: ConfigVersion,
      timing: LeaseTiming
  ): Either[Skipped, LeasedRun] = {
    require(
      start.configHash == config.hash,
      "the run names another configuration than the one given"
    )
    // By itself: it is the same whoever stores it, and a run that then does not start leaves
    // nothing wrong behind. Inside the transaction it would make every run wait on the first.
    database.run(ConfigSnapshots.store(_, start.source, config)): Unit

    // Said before the lease is asked for, and not after: a process that has logged nothing yet
    // sets its logging up here, and not on the time of a lease that it already holds.
    logger.info(s"asking for the lease of ${start.leaseKey}, for the run ${start.id}")

    // Before the lease is asked for: what the run counts from runs out before the lease does.
    val askedAt = System.nanoTime()
    val taken = database.transaction { connection =>
      Leases.acquire(connection, start.leaseKey, start.id, start.instance, timing.ttl) match {
        case acquired: Leases.Acquisition.Acquired =>
          // It held the lease and let it expire: it ended halfway, and said nothing.
          acquired.tookOver.foreach(Runs.abandon(connection, _))
          Runs.open(connection, start, acquired.epoch)
          Right(acquired)
        case Leases.Acquisition.Busy(holder) =>
          Runs.skip(connection, start)
          Left(Skipped(start, holder.run, holder.instance, holder.expiresAt))
      }
    }

    taken.left.foreach { skipped =>
      logger.info(
        s"the lease of ${start.leaseKey} is held by the run ${skipped.holder} (${skipped.instance}) " +
          s"until ${skipped.until}, unless it is renewed: the run ${start.id} is skipped"
      )
    }
    taken.map { acquired =>
      logger.info(
        s"the run ${start.id} holds the lease of ${start.leaseKey} under epoch ${acquired.epoch}" +
          acquired.tookOver.fold("")(previous =>
            s"; it expired for the run $previous, which ended halfway and is marked ABANDONED"
          )
      )
      val heartbeat = new LeaseHeartbeat(
        start.leaseKey.value,
        timing,
        askedAt,
        () => database.run(Leases.renew(_, start.leaseKey, start.id, acquired.epoch, timing.ttl))
      )
      new LeasedRun(database, start, acquired.epoch, acquired.tookOver, heartbeat)
    }
  }
}
