package lakehouse.ingestion.engine.delta

import io.delta.tables.DeltaTable
import lakehouse.ingestion.core.config.TableLocation
import lakehouse.ingestion.core.control.{BatchId, TableTransactions, TxnAppId}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}

/**
 * What a Delta table says of the batches that were written to it, read from its log (ADR-0005
 * §3, steps 2 and 7).
 *
 * '''The table is read again every time it is asked.''' What this process remembers of a table
 * is what it was when the process last looked. Another process may have committed since: a run
 * that died, a zombie, another source of the same table. So each answer starts by listing the
 * log anew (`update`), through the LogStore of the session, which is the coordinated one.
 *
 * '''One class of Delta's that is not its API.''' The last `txnVersion` of an application is in
 * the state of a snapshot (`Snapshot.transactions`), which Delta keeps to skip a write that it
 * has, and shows through no public method. It is asked here and nowhere else (discovery §20):
 * an upgrade of Delta that moves it breaks this file, and the tests of this file say so. The
 * history of a table is read through the public `DeltaTable.history`.
 *
 * '''A table must keep its transactions.''' Delta drops the transaction of an application that
 * has not written for `delta.setTransactionRetentionDuration`, if a table sets that property.
 * No table of the engine does: with it, a batch that was written long ago would look as one
 * that never was, and would be written again.
 *
 * @param spark the session of the run, whose configuration names the LogStore
 * @param firstPage how many commits of a history are looked at first. The commit of a batch
 *   that is asked for is nearly always among the last of its table: the run has just made it,
 *   or the run before died making it
 */
final class DeltaTableTransactions(
    spark: SparkSession,
    firstPage: Int = DeltaTableTransactions.DefaultFirstPage
) extends TableTransactions {
  import DeltaTableTransactions.Commit

  require(firstPage >= 1, s"a page of a history has a commit at least, not $firstPage")

  override def lastVersion(table: TableLocation, app: TxnAppId): Option[Long] =
    snapshot(table).flatMap(_.transactions.get(app.value))

  /**
   * The history is looked at from its end, a page at a time and each page larger than the one
   * before, until a commit that names the batch is found (`CommitMetadata`) or there is no
   * history left. If several commits of a page name it, it is the last of them: the rows of a
   * batch are in its table by the last commit that the batch made there.
   *
   * It is only looked at for a batch that the table has, by Delta's own account of it
   * (`lastVersion`). One whose number the table has not reached is in no commit, and saying so
   * takes no reading of a history to its beginning.
   *
   * '''What decides and what describes.''' Whether a batch is in a table is Delta's to say, by
   * the transaction that it records with the write, and nothing else is asked. Where it is, is
   * read from what the commit says of itself, which is text that any writer of the table may
   * set. So a writer that is not the engine can, at worst, make a batch be recorded at another
   * version than its own; it cannot make one be taken for written, nor written twice.
   */
  override def committedAt(table: TableLocation, batch: BatchId): Option[Long] =
    lastVersion(table, batch.app).filter(_ >= batch.seq.value).flatMap { _ =>
      @scala.annotation.tailrec
      def search(limit: Int): Option[Long] = {
        val page = history(table, limit)
        val found = page.collect {
          case Commit(Some(version), Some(text))
              if CommitMetadata.parse(text).exists(_.batch == batch) =>
            version
        }
        // A page that is not full has the first commit that the table keeps: older ones were
        // cleaned up with its log, and what they were is not known any more.
        if (found.nonEmpty) Some(found.max)
        else if (page.size < limit) None
        else search(larger(limit))
      }
      search(firstPage)
    }

  /** The table as it is now; none, if there is no table there. */
  private def snapshot(table: TableLocation): Option[Snapshot] = {
    val current = DeltaLog.forTable(spark, new Path(table.uri)).update()
    Option.when(current.version >= 0)(current)
  }

  /** The last `limit` commits of a table, or all that it keeps if they are fewer. */
  private def history(table: TableLocation, limit: Int): Seq[Commit] =
    DeltaTable
      .forPath(spark, table.uri)
      .history(limit)
      .select("version", "userMetadata")
      .collect()
      .toSeq
      .map { row =>
        Commit(Option.unless(row.isNullAt(0))(row.getLong(0)), Option(row.getString(1)))
      }

  private def larger(limit: Int): Int =
    math.min(limit.toLong * DeltaTableTransactions.Growth, Int.MaxValue.toLong).toInt
}

object DeltaTableTransactions {

  /** A commit as a history shows it: both are there for a commit that Delta wrote whole. */
  final private case class Commit(version: Option[Long], userMetadata: Option[String])

  val DefaultFirstPage = 16

  /** Pages grow by this, so that a history is read at most a seventh more than once. */
  private val Growth = 8
}
