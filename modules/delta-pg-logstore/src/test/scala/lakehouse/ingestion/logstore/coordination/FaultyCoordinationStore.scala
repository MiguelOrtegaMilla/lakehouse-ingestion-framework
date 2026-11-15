package lakehouse.ingestion.logstore.coordination

import java.io.IOException
import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

import lakehouse.ingestion.logstore.coordination.FaultyCoordinationStore._

/**
 * A `CoordinationStore` whose calls a test can make fail the two ways a database behind a
 * network fails: the call never arrives, or it arrives, takes effect, and its answer is lost.
 * The caller sees the same `IOException` both times, which is the whole difficulty.
 *
 * It decides exactly which call fails and how, which a real fault cannot always do. The same
 * scenarios run against a real PostgreSQL behind a proxy in the integration tests.
 */
final class FaultyCoordinationStore(underlying: CoordinationStore) extends CoordinationStore {

  private val journal = new ConcurrentLinkedQueue[Operation]
  private var queued = Map.empty[Operation, List[Fault]]
  private var afterwards = Map.empty[Operation, () => Unit]
  @volatile private var unreachable = false

  /** Every call fails without reaching the store, until `up`. */
  def down(): Unit = unreachable = true

  def up(): Unit = unreachable = false

  /** The next calls of `operation` fail with `faults`, one each and in this order. */
  def failNext(operation: Operation, faults: Fault*): Unit = synchronized {
    queued += operation -> (queued.getOrElse(operation, Nil) ++ faults)
  }

  /**
   * Runs `effect` once, right after the next call of `operation` has got its answer and before
   * the caller sees it: whatever another process does in that instant.
   */
  def afterNext(operation: Operation)(effect: => Unit): Unit = synchronized {
    afterwards += operation -> (() => effect)
  }

  /** Every call made so far, in order, whether it failed or not. */
  def calls: Seq[Operation] = journal.iterator.asScala.toSeq

  override def registration(table: TableUri): Option[Registration] =
    call(ReadRegistration)(underlying.registration(table))

  override def register(
      table: TableUri,
      registeredVersion: Long,
      deltaTableId: Option[String]
  ): RegisterResult = call(Register)(underlying.register(table, registeredVersion, deltaTableId))

  override def entry(table: TableUri, version: Long): Option[CommitEntry] =
    call(ReadEntry)(underlying.entry(table, version))

  override def latestEntry(table: TableUri): Option[CommitEntry] =
    call(ReadLatestEntry)(underlying.latestEntry(table))

  override def take(claim: Claim): TakeResult = call(Take)(underlying.take(claim))

  override def complete(table: TableUri, version: Long, tempPath: String): Boolean =
    call(Complete)(underlying.complete(table, version, tempPath))

  override def taint(table: TableUri, reason: String): Unit =
    call(Taint)(underlying.taint(table, reason))

  private def call[T](operation: Operation)(run: => T): T = {
    journal.add(operation)
    val fault = synchronized {
      if (unreachable) Some(Unreachable)
      else
        queued.getOrElse(operation, Nil) match {
          case next :: later =>
            queued += operation -> later
            Some(next)
          case Nil => None
        }
    }
    fault match {
      case None =>
        val answer = run
        synchronized {
          val effect = afterwards.get(operation)
          afterwards -= operation
          effect
        }.foreach(_.apply())
        answer
      case Some(Unreachable) =>
        throw new IOException(s"injected fault: $operation did not reach the store")
      case Some(ReplyLost) =>
        run: Unit
        throw new IOException(s"injected fault: the reply to $operation was lost")
      case Some(Preempted(by)) =>
        underlying.take(by): Unit
        throw new IOException(s"injected fault: $operation did not reach the store")
    }
  }
}

object FaultyCoordinationStore {

  sealed trait Operation
  case object ReadRegistration extends Operation
  case object Register extends Operation
  case object ReadEntry extends Operation
  case object ReadLatestEntry extends Operation
  case object Take extends Operation
  case object Complete extends Operation
  case object Taint extends Operation

  sealed trait Fault

  /** The call does not reach the store: nothing happens there. */
  case object Unreachable extends Fault

  /** The call reaches the store and takes effect. The caller only learns that it failed. */
  case object ReplyLost extends Fault

  /**
   * The call does not reach the store, and before the caller can repeat it another writer's
   * claim does.
   */
  final case class Preempted(by: Claim) extends Fault
}
