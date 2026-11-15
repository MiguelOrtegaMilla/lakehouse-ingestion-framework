package lakehouse.ingestion.logstore

import scala.concurrent.duration._
import scala.util.control.ControlThrowable

import lakehouse.ingestion.logstore.coordination.CoordinationStore
import lakehouse.ingestion.logstore.storage.LogStorage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

/**
 * The writer stops at a fault point the way a killed process does: nothing after the point
 * runs. It is not an `Exception`, so that no handler of the protocol, present or future, takes
 * it for a failure to clean up after.
 */
final class SimulatedCrash(val point: FaultPoint) extends ControlThrowable

/** `PostgresLogStore`s built the way tests need and Delta's constructor does not offer. */
object LogStores {

  /** The default number of attempts, without the waiting. */
  val Impatient: RetryPolicy = RetryPolicy.Default.copy(firstPause = 1.milli)

  /**
   * A LogStore over `store`.
   *
   * @param hooks what happens at the fault points; by default, what `conf` selects
   * @param storageOf the storage for a file system; by default, the real one
   */
  def over(
      store: CoordinationStore,
      hooks: Option[FaultHooks] = None,
      retry: RetryPolicy = Impatient,
      storageOf: FileSystem => LogStorage = LogStorage.of,
      conf: Configuration = new Configuration()
  ): PostgresLogStore =
    new PostgresLogStore(
      conf,
      _ => store,
      storageOf,
      hooks.fold[Configuration => FaultHooks](FaultHooks.from)(chosen => _ => chosen),
      retry
    )

  /** Hooks that stop the writer the first time it reaches `point`. */
  def crashAt(point: FaultPoint): Option[FaultHooks] = at(point)(throw new SimulatedCrash(point))

  /** Hooks that run `effect` each time a writer reaches `point`. */
  def at(point: FaultPoint)(effect: => Unit): Option[FaultHooks] =
    Some(reached => if (reached == point) effect)
}
