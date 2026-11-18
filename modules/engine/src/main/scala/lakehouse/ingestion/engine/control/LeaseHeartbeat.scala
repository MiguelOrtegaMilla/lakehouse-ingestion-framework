package lakehouse.ingestion.engine.control

import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import lakehouse.ingestion.core.control.{LeaseLoss, LeaseTiming}
import org.slf4j.LoggerFactory

/** A run went on after its lease was lost, and was stopped where it asked. */
final class LeaseLostException(val lease: String, val loss: LeaseLoss)
    extends RuntimeException(s"the lease of $lease is lost: ${loss.reason}")

/**
 * Keeps a lease alive while its run works, and says when the run must stop (ADR-0006).
 *
 * It renews the lease every third of its time to live, on a thread of its own. And it keeps
 * count of how long the lease is still good by the last renewal that it '''saw''' succeed:
 * when that runs out, the run has lost its lease as far as it can know, and stops working
 * before the database would let another run take it. That is self-fencing.
 *
 * Why the run's count runs out first: it is counted from the moment the renewal was asked for,
 * which is before the database started counting the time to live, and it is shorter than the
 * time to live by a sixth. The two are counted by two clocks, and that sixth is what they may
 * differ by. The run counts by a clock that only moves forward at its pace. The database
 * counts by the time of day of its machine, which is corrected now and then, and on some
 * machines by a jump: a WSL2 machine was seen to step it forward by most of a second at a
 * time. A lease of two minutes bears jumps of twenty seconds between two renewals.
 *
 * What it does not do is make a run safe. A process that is frozen for longer than its lease,
 * by its machine or by its own garbage collector, counts nothing while it is frozen, and wakes
 * up a zombie. So does one whose database had its clock moved forward by more than that sixth:
 * its lease is taken while its own count still has time on it, and it finds out at its next
 * renewal. What keeps a zombie from doing harm is the epoch, on every statement that advances
 * the state of a source, and the identity of a batch, in the table. This makes the time in
 * which a zombie works at all as short as a process can make it.
 *
 * The count is kept apart from the renewals. A renewal that hangs, on a network that stopped
 * answering, does not keep the lease from running out on time.
 *
 * Nothing that goes wrong around it may stop it, since what it stops when it stops is the one
 * thing that tells a run to stop. So whoever asked to be told is told before anything is
 * logged, nothing is logged while its state is held, a log line that fails is a log line that
 * is missing, and closing it interrupts none of its threads: a thread that is interrupted in
 * the middle of logging can leave a process with no logging at all.
 *
 * @param lease what the lease is of, for messages
 * @param askedAt `System.nanoTime` just before the lease was asked for
 * @param renew renews the lease in the database: false if it is not this run's any more. It
 *   may fail, and may take as long as a statement may
 */
final class LeaseHeartbeat(lease: String, timing: LeaseTiming, askedAt: Long, renew: () => Boolean)
    extends AutoCloseable {

  import LeaseHeartbeat._

  // Made here, on the thread that makes the heartbeat: no thread of the heartbeat is the first
  // of the process to log, with a lease running out while logging is being set up.
  private val logger = LoggerFactory.getLogger(classOf[LeaseHeartbeat])

  // One thread for the renewals, which may wait on the database, and one to see the time run out.
  private val scheduler = {
    val threads = new ScheduledThreadPoolExecutor(2, new NamedDaemons(s"lease-$lease"))
    threads.setRemoveOnCancelPolicy(true)
    threads
  }

  private val lock = new Object
  private var validUntil: Long = askedAt + timing.validFor.toNanos
  private var loss: Option[LeaseLoss] = None
  private var closed = false
  private var lastFailure: Option[String] = None
  private var callbacks = Vector.empty[LeaseLoss => Unit]
  private var watch: Option[ScheduledFuture[_]] = None
  private var nextRenewal: Option[ScheduledFuture[_]] = None

  lock.synchronized {
    watchUntilItRunsOut()
    // A third of the time to live after the lease was asked for, however long ago that is by
    // now: what is renewed is the lease, not the moment this was made.
    renewAt(askedAt + timing.heartbeat.toNanos)
  }

  /** Why the lease is lost, if it is. Once lost it stays lost: the run is over. */
  def lost: Option[LeaseLoss] = lock.synchronized(loss)

  /**
   * Stops a run that has lost its lease: to be asked before each thing that a run must not do
   * without one.
   *
   * @throws LeaseLostException if the lease is lost, or has just run out
   */
  @throws[LeaseLostException]
  def verify(): Unit = {
    // The time may have run out with nobody there to see it: a process that was frozen.
    tell(lock.synchronized(if (ranOut) lose(LeaseLoss.NotRenewed(lastFailure)) else None))
    lost.foreach(why => throw new LeaseLostException(lease, why))
  }

  /**
   * The database said that the lease is another's, to something else than a renewal that
   * asked: the run is told now, and not at the next renewal.
   */
  def taken(): Unit = tell(lock.synchronized(if (over) None else lose(LeaseLoss.TakenOver)))

  /**
   * What to do the moment the lease is lost, besides answering `verify`: stop what a run is in
   * the middle of, as the jobs of Spark. It is called once, on a thread of the heartbeat or on
   * the one that found the lease run out; at once, if the lease is lost already.
   */
  def onLost(callback: LeaseLoss => Unit): Unit = {
    val already = lock.synchronized {
      if (loss.isEmpty) callbacks :+= callback
      loss
    }
    already.foreach(why => react(callback, why))
  }

  /**
   * Stops renewing, and says nothing more. The lease is left as it is in the database: to be
   * released by whoever closes this, or to expire.
   *
   * No thread is interrupted. A renewal that is waiting for its answer gets it, or gives up
   * when its timeouts say, and its answer is not listened to.
   */
  override def close(): Unit = {
    lock.synchronized {
      closed = true
      cancel()
    }
    scheduler.shutdown()
  }

  // -- under the lock ---------------------------------------------------------------------------

  private def over: Boolean = closed || loss.nonEmpty

  private def ranOut: Boolean = !over && System.nanoTime() - validUntil >= 0

  private def lose(why: LeaseLoss): Option[Lost] = {
    loss = Some(why)
    cancel()
    Some(Lost(why, callbacks))
  }

  private def cancel(): Unit = {
    (watch ++ nextRenewal).foreach(_.cancel(false))
    watch = None
    nextRenewal = None
  }

  private def watchUntilItRunsOut(): Unit = {
    watch.foreach(_.cancel(false))
    val left = math.max(0L, validUntil - System.nanoTime())
    watch = Some(
      scheduler.schedule(guarded("its watch")(itMayHaveRunOut()), left, TimeUnit.NANOSECONDS)
    )
  }

  /** @param at the `System.nanoTime` at which to renew; now, if it is past */
  private def renewAt(at: Long): Unit = {
    val wait = math.max(0L, at - System.nanoTime())
    nextRenewal = Some(
      scheduler.schedule(guarded("a renewal")(renewNow()), wait, TimeUnit.NANOSECONDS)
    )
  }

  // -- on the threads of the heartbeat ----------------------------------------------------------

  private def itMayHaveRunOut(): Unit = tell(lock.synchronized {
    if (ranOut) lose(LeaseLoss.NotRenewed(lastFailure))
    else {
      // It was renewed between the moment this was set and now.
      if (!over) watchUntilItRunsOut()
      None
    }
  })

  private def renewNow(): Unit = {
    // The time ran out while this waited its turn: there is no lease left to renew, and one
    // that is renewed now would be a lease that the run had already stopped for.
    val tooLate = lock.synchronized(if (ranOut) lose(LeaseLoss.NotRenewed(lastFailure)) else None)
    tell(tooLate)
    if (tooLate.isEmpty && !lock.synchronized(over)) askForMore()
  }

  private def askForMore(): Unit = {
    val asked = System.nanoTime()
    // Whatever it fails with, it is a renewal that failed: it is tried again, and the watch
    // goes on. An error that only a few would catch must not end the renewals in silence.
    val answer =
      try Right(renew())
      catch { case failure: Throwable => Left(failure) }

    val (lost, warning) = lock.synchronized {
      if (over) (None, None)
      else
        answer match {
          case Right(false) => (lose(LeaseLoss.TakenOver), None)
          // An answer that comes after the time ran out does not give the lease back.
          case _ if ranOut => (lose(LeaseLoss.NotRenewed(lastFailure)), None)
          case Right(true) =>
            validUntil = asked + timing.validFor.toNanos
            lastFailure = None
            watchUntilItRunsOut()
            // Counted from when this renewal was asked for, as the time it is good for is.
            renewAt(asked + timing.heartbeat.toNanos)
            (None, None)
          case Left(failure) =>
            val said = Option(failure.getMessage).flatMap(_.linesIterator.find(_.trim.nonEmpty))
            lastFailure = Some(said.getOrElse(failure.getClass.getSimpleName))
            // The next attempt first: it is what matters, and it is not kept waiting for a log line.
            renewAt(System.nanoTime() + timing.retry.toNanos)
            val left = (validUntil - System.nanoTime()) / 1000000L
            val warning = s"the lease of $lease was not renewed (${lastFailure.get}); it is good " +
              s"for $left ms more, and is tried again in ${timing.retry.toMillis} ms"
            (None, Some(warning))
        }
    }
    tell(lost)
    warning.foreach(message => logged(logger.warn(message)))
  }

  // -- outside the lock -------------------------------------------------------------------------

  /** The reactions first: they are what stops the run, and a log line is not. */
  private def tell(lost: Option[Lost]): Unit = lost.foreach { case Lost(why, toCall) =>
    toCall.foreach(react(_, why))
    logged(logger.error(s"the lease of $lease is lost: ${why.reason}. The run stops working"))
  }

  /** One that fails does not keep the others from being told. */
  private def react(callback: LeaseLoss => Unit, why: LeaseLoss): Unit =
    try callback(why)
    catch {
      case failure: Throwable =>
        logged(logger.error(s"a reaction to the loss of the lease of $lease failed", failure))
    }

  /** A task that ends in an error is said, and not dropped by the scheduler without a word. */
  private def guarded(what: String)(work: => Unit): Runnable = () =>
    try work
    catch {
      case failure: Throwable =>
        logged(logger.error(s"the heartbeat of the lease of $lease failed in $what", failure))
    }

  /** A log line that fails is a log line that is missing, and nothing else. */
  private def logged(log: => Unit): Unit =
    try log
    catch { case _: Throwable => () }
}

object LeaseHeartbeat {

  /** A lease that was lost, and who is to be told. */
  final private case class Lost(why: LeaseLoss, toCall: Vector[LeaseLoss => Unit])

  /** Threads that do not keep the process alive, named after what they watch. */
  final private class NamedDaemons(name: String) extends ThreadFactory {
    private val count = new AtomicInteger(0)
    override def newThread(work: Runnable): Thread = {
      val thread = new Thread(work, s"$name-${count.incrementAndGet()}")
      thread.setDaemon(true)
      thread
    }
  }
}
