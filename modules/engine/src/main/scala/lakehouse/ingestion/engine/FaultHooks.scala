package lakehouse.ingestion.engine

import lakehouse.ingestion.core.control.PlannedBatch

/**
 * A point of the protocol of a run (ADR-0005 §3) where a test makes the run stop or fail, or
 * changes the world under it (discovery §12.3). Each comes after a step that leaves something
 * behind for the next run to find, or that the rest of this one is made from.
 *
 * The LogStore has points of its own, inside the commit of a table
 * (`lakehouse.ingestion.logstore.FaultPoint`); these are between the steps of a batch.
 *
 * @param name as the discovery names it
 */
sealed abstract class FaultPoint(val name: String) {
  override def toString: String = name
}

object FaultPoint {

  /** The plan of the batch is written down, and nothing of it is read. */
  case object AfterPlan extends FaultPoint("AfterPlan")

  /**
   * The plan was read, and what was read is kept apart from the source. Nothing is written:
   * what a run that dies here leaves is what one that dies after its plan leaves.
   */
  case object AfterRead extends FaultPoint("AfterRead")

  /**
   * The rows of the batch are in its table of Bronze, and the control plane does not know. A
   * batch that wrote no row does not get here: it has no commit.
   */
  case object AfterBronzeCommit extends FaultPoint("AfterBronzeCommit")

  /** All that the batch writes is written, and its state is about to be committed. */
  case object BeforeStateCommit extends FaultPoint("BeforeStateCommit")

  /** In the order a run reaches them. */
  val All: Seq[FaultPoint] = Seq(AfterPlan, AfterRead, AfterBronzeCommit, BeforeStateCommit)
}

/** What a run does when it reaches a fault point. Outside tests: nothing. */
trait FaultHooks {

  /** @param batch the batch that the run is on: every point is after its plan */
  def at(point: FaultPoint, batch: PlannedBatch): Unit
}

object FaultHooks {

  val Off: FaultHooks = (_, _) => ()
}
