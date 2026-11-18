package lakehouse.ingestion.it.delta

import lakehouse.ingestion.core.config.{
  PathTableLocator,
  RelativePath,
  RootUri,
  SourceId,
  TableLocation
}
import lakehouse.ingestion.core.control.{BatchId, TableTransactions}
import lakehouse.ingestion.it.control.SimulatedDelta

/**
 * The simulation of Delta that the control plane is tested against (P4.6) answers for a table
 * as the real one does: the tests of `BatchesSpec` and `ProtocolInvariantsSpec` rest on it.
 */
final class SimulatedDeltaSpec extends TableTransactionsContract {

  private val delta = new SimulatedDelta
  private val locator = new PathTableLocator(RootUri.parse("file:///srv/lake").value)

  override protected def tables: TableTransactions = delta

  override protected def newTable(name: String): TableLocation =
    locator.locate(RelativePath.parse(s"contract/$name").value)

  override protected def write(
      table: TableLocation,
      source: SourceId,
      batch: BatchId,
      rows: Seq[String]
  ): Option[Long] = delta.write(table, batch, rows)

  override protected def rowsOf(table: TableLocation): Seq[String] = delta.rows(table).sorted
}
