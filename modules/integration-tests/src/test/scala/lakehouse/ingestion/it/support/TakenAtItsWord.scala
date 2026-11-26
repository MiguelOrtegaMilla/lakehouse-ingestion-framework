package lakehouse.ingestion.it.support

import lakehouse.ingestion.core.control.RunStatus
import lakehouse.ingestion.engine.control.Settling

/**
 * A transaction that ends a run taken at its word, with nothing settled: what a test opens a run
 * with by hand when it tests the transaction itself, or holds a lease to make another run wait.
 * The engine settles what it ends with the retries of the run (ADR-0022 §6), and its tests do
 * so through it.
 */
object TakenAtItsWord extends Settling {
  override def settle(ends: RunStatus, status: => RunStatus)(end: => Boolean): Boolean = end
}
