package lakehouse.ingestion.it.control

import lakehouse.ingestion.core.config.TableLocation
import lakehouse.ingestion.core.control.{BatchId, TableTransactions, TxnAppId}

/**
 * Delta, as far as the recovery of a batch goes (ADR-0005 §3, steps 5 and 6), and no further.
 *
 * A table here is its commits, one version after another. Each write is made under the name of
 * a sequence and the number of a batch, as a write to Delta is made with a `txnAppId` and a
 * `txnVersion`, and the table does with them what Delta does: it remembers the last number it
 * committed under each name, and skips a write that is not beyond it.
 *
 * It is what the control plane is tested against until the engine writes to real tables (P5),
 * where the one thing to write is what reads that last number from a log.
 */
final class SimulatedDelta extends TableTransactions {

  import SimulatedDelta.Commit

  private var tables = Map.empty[TableLocation, Vector[Commit]]

  /**
   * Writes the rows of a batch to a table, once.
   *
   * @return the version of the table at which they were committed; none if the table had the
   *   batch already, and the write was skipped
   */
  def write(table: TableLocation, batch: BatchId, rows: Seq[String]): Option[Long] = synchronized {
    if (lastVersion(table, batch.app).exists(_ >= batch.seq.value)) None
    else {
      val commits = tables.getOrElse(table, Vector.empty) :+ Commit(batch, rows.toVector)
      tables += table -> commits
      Some(commits.size - 1L)
    }
  }

  override def lastVersion(table: TableLocation, app: TxnAppId): Option[Long] = synchronized {
    tables
      .getOrElse(table, Vector.empty)
      .collect { case Commit(batch, _) if batch.app == app => batch.seq.value }
      .maxOption
  }

  /** The version at which a batch was committed to a table, as the history of the table has it. */
  def committedAt(table: TableLocation, batch: BatchId): Option[Long] = synchronized {
    Some(tables.getOrElse(table, Vector.empty).indexWhere(_.batch == batch).toLong).filter(_ >= 0)
  }

  /** Every row of a table, in the order it was written. */
  def rows(table: TableLocation): Vector[String] = synchronized {
    tables.getOrElse(table, Vector.empty).flatMap(_.rows)
  }
}

object SimulatedDelta {
  final private case class Commit(batch: BatchId, rows: Vector[String])
}
