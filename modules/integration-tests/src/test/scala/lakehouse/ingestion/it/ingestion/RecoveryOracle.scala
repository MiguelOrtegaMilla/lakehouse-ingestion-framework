package lakehouse.ingestion.it.ingestion

import lakehouse.ingestion.engine.FaultPoint
import lakehouse.ingestion.logstore.{FaultPoint => LogPoint}

/**
 * What the run that takes over the lease of a run that died, or that lost it, does with the batch
 * that the other left planned (ADR-0005 §4): the oracle of the matrix of faults
 * (`FaultMatrixSpec`) and of the runs killed with -9 (`KilledRunSpec`), said once.
 *
 * The table of the data of the batch decides, once a reader has made it what the LogStore says.
 * A batch whose rows are in it is rolled forward, and one whose rows are not is run again under
 * its number, as it was planned; a quarantine that has the records of the batch changes nothing
 * of that.
 */
object RecoveryOracle {

  /** What the run that takes the lease does with the batch left planned. */
  sealed abstract class Recovery(val left: String, val taker: String)

  object Recovery {

    /** Its plan is run again, under its number: nothing of it is in the table of its data. */
    case object Replay
        extends Recovery(
          "leaves its batch to be run again under its number",
          "the run that took it runs the batch again under its number"
        )

    /** It is committed as it was written: its rows are in the table of its data. */
    case object RollForward
        extends Recovery(
          "leaves its batch to be rolled forward",
          "the run that took it rolls the batch forward"
        )
  }

  /**
   * At a point of the engine. The rows of the batch are in the table of its data from
   * `AfterBronzeCommit` on; at `AfterQuarantineWrite` the quarantine has the rejected records,
   * and the data does not.
   */
  def recoveryAt(point: FaultPoint): Recovery = point match {
    case FaultPoint.AfterBronzeCommit | FaultPoint.BeforeStateCommit => Recovery.RollForward
    case _ => Recovery.Replay
  }

  /** The tables that a batch of files commits to, in the order that it does. */
  sealed abstract class LogTable(val name: String)

  object LogTable {
    case object Quarantine extends LogTable("its quarantine")
    case object Bronze extends LogTable("Bronze")

    val All: Seq[LogTable] = Seq(Quarantine, Bronze)
  }

  /**
   * At a point of the LogStore, inside the commit to a table (ADR-0004 §3.8). Before the insert of
   * its version, the commit is not there; from it on, it is durable, and the next reader publishes
   * it if it is not published yet. A commit to the quarantine, durable or not, leaves the data
   * unwritten.
   */
  def recoveryInside(table: LogTable, point: LogPoint): Recovery = (table, point) match {
    case (LogTable.Bronze, LogPoint.BeforeInsert) => Recovery.Replay
    case (LogTable.Bronze, _) => Recovery.RollForward
    case (LogTable.Quarantine, _) => Recovery.Replay
  }
}
