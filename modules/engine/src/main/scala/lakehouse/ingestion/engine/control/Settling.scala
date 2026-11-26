package lakehouse.ingestion.engine.control

import lakehouse.ingestion.core.control.RunStatus

/**
 * How a run settles the transaction that ends it when that transaction gives no answer
 * (ADR-0022 §6). The database may have committed it with its answer lost, so the run reads its
 * own row before it says how it ended: what the row says is what happened.
 *
 * It is the run's, as its retries are (`RunRetries`), and draws on the same budget. A run that
 * holds its lease is given it when it is opened (`LeasedRun.open`), so that every transaction
 * that ends a run is settled, and no caller of one has to remember to.
 */
trait Settling {

  /**
   * `end`, and whether it was committed.
   *
   * @param ends the status of the run once `end` is committed
   * @param status the status of the run as the control plane has it now: its own row, read
   *   anew each time
   * @param end the transaction that ends the run: true if it was committed, false if it was
   *   refused for the lease. It is made again only once the row says that it was not committed
   * @return whether `end` was committed, as its answer says or, when that was lost, as the row
   *   of the run does
   */
  def settle(ends: RunStatus, status: => RunStatus)(end: => Boolean): Boolean
}
