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
   * what a run that dies here leaves is what one that dies after its plan leaves. For rows
   * that are read as they are written (`ReadResult.OnePass`), the query of them is made, not
   * run: the source was not asked for a row, and is asked for them as they are written.
   */
  case object AfterRead extends FaultPoint("AfterRead")

  /**
   * What was read is counted: the run knows how many rows the batch has and how many of its
   * records were rejected, and has written none of them. It is the last point before a table is
   * written, and every batch that was read gets here. For rows that are read as they are
   * written, what the run knows is whether there is one: it has asked for the first.
   */
  case object AfterCounts extends FaultPoint("AfterCounts")

  /**
   * The records of the batch that were rejected are in the quarantine of its source, and its
   * rows are not in Bronze: the batch is written in part. A batch with no such record does not
   * get here: it has no commit to its quarantine.
   */
  case object AfterQuarantineWrite extends FaultPoint("AfterQuarantineWrite")

  /**
   * The rows of the batch are in its table of Bronze, and the control plane does not know. A
   * batch that wrote no row does not get here: it has no commit.
   */
  case object AfterBronzeCommit extends FaultPoint("AfterBronzeCommit")

  /** All that the batch writes is written, and its state is about to be committed. */
  case object BeforeStateCommit extends FaultPoint("BeforeStateCommit")

  /** In the order a run reaches them. */
  val All: Seq[FaultPoint] =
    Seq(
      AfterPlan,
      AfterRead,
      AfterCounts,
      AfterQuarantineWrite,
      AfterBronzeCommit,
      BeforeStateCommit
    )
}

/** What a run does when it reaches a fault point. Outside tests: nothing. */
trait FaultHooks {

  /** @param batch the batch that the run is on: every point is after its plan */
  def at(point: FaultPoint, batch: PlannedBatch): Unit
}

object FaultHooks {

  val Off: FaultHooks = (_, _) => ()
}
