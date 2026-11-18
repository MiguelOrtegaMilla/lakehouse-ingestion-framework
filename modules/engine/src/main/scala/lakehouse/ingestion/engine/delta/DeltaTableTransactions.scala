package lakehouse.ingestion.engine.delta

import scala.annotation.tailrec

import lakehouse.ingestion.core.config.TableLocation
import lakehouse.ingestion.core.control.{BatchId, TableTransactions, TxnAppId}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, SetTransaction}

/**
 * What a Delta table says of the batches that were written to it, read from its log (ADR-0005
 * §3, steps 2 and 7).
 *
 * '''The table is read again every time it is asked.''' What this process remembers of a table
 * is what it was when the process last looked. Another process may have committed since: a run
 * that died, a zombie, another source of the same table. So each answer starts by listing the
 * log anew (`update`), through the LogStore of the session, which is the coordinated one.
 *
 * '''Both answers are Delta's own.''' A write that names its transaction (`txnAppId`,
 * `txnVersion`) has it recorded by Delta in the commit of its rows, as an action of that commit
 * (`txn` in the log, `SetTransaction` here), and Delta skips the write if the table has that
 * transaction already. So the commit that holds the transaction of a batch is the commit of the
 * batch, and no other is. Nothing is taken from what a commit says of itself
 * (`CommitMetadata`), which any writer of a table may set as it likes.
 *
 * '''What is used of Delta that is not its API''' (discovery §5, R3). Delta shows neither
 * answer through a public method, so three of its own classes are asked, in this file and in
 * no other:
 *  - `Snapshot.transactions`, the last transaction of each application, which Delta keeps in
 *    the state of a table to skip a write that it has;
 *  - `DeltaLog.getChanges`, the commits of a log from a version on, each with its actions, read
 *    one at a time as they are asked for;
 *  - `SetTransaction`, the action itself.
 *
 * What is assumed of the three is pinned by tests of Delta alone, with no code of the engine
 * in them (`DeltaSetTransactionContractSpec`): an upgrade of Delta that changes one fails
 * there, by the name of the assumption, or does not compile here.
 *
 * '''Two things that a table must keep for the answers to hold.'''
 *  - ''Its transactions.'' A table that sets `delta.setTransactionRetentionDuration` has Delta
 *    forget the transaction of an application that has not written for that long. A batch that
 *    was written before would then look as one that never was, and would be written again. No
 *    table of the engine sets that property, and none that the engine writes to may have it.
 *  - ''Its commits, to say where a batch is.'' Delta removes the commits that are older than
 *    `delta.logRetentionDuration`, thirty days unless a table says otherwise, when it writes a
 *    checkpoint. The table still knows that a batch of then is written, since its transactions
 *    are in the checkpoint, and no longer at which version. That is asked when a batch is
 *    written, or soon after by the run that recovers it, and what was answered is kept by the
 *    control plane (`BatchOutput`), which does not forget it with the log.
 *
 * @param spark the session of the run, whose configuration names the LogStore
 * @param firstPage how many commits of a log are read first. The commit of a batch that is
 *   asked for is nearly always among the last of its table: the run has just made it, or the
 *   run before died making it
 */
final class DeltaTableTransactions(
    spark: SparkSession,
    firstPage: Int = DeltaTableTransactions.DefaultFirstPage
) extends TableTransactions {
  import DeltaTableTransactions._

  require(firstPage >= 1, s"a page of a log has a commit at least, not $firstPage")

  override def lastVersion(table: TableLocation, app: TxnAppId): Option[Long] =
    current(table).flatMap { case (_, snapshot) => snapshot.transactions.get(app.value) }

  /**
   * The log is read from its end, a page of commits at a time and each page larger than the
   * one before, until a commit that holds the transaction of the batch is found or the log has
   * no older commit. Delta lets one commit hold it; were there several, by a writer that goes
   * around Delta, it is the last.
   *
   * The log is only read for a batch that the table has, by its state (`lastVersion`). One
   * whose number the table has not reached is in no commit, and saying so takes no reading of
   * a log to its beginning.
   */
  override def committedAt(table: TableLocation, batch: BatchId): Option[Long] =
    current(table)
      .filter { case (_, snapshot) =>
        snapshot.transactions.get(batch.app.value).exists(_ >= batch.seq.value)
      }
      .flatMap { case (log, snapshot) =>
        @tailrec
        def search(last: Long, size: Long): Option[Long] = {
          val first = if (size > last) 0L else last - size + 1
          val page = read(log, first, last, batch)
          // A page that does not start where it was asked from starts at the first commit that
          // the log keeps: the older ones went with its cleanup, and what they held is not
          // known any more.
          if (page.found.nonEmpty || first == 0 || !page.first.contains(first)) page.found
          else search(first - 1, larger(size))
        }
        search(snapshot.version, firstPage.toLong)
      }

  /** The log of a table and the table as it is now; none, if there is no table there. */
  private def current(table: TableLocation): Option[(DeltaLog, Snapshot)] = {
    val log = DeltaLog.forTable(spark, new Path(table.uri))
    val snapshot = log.update()
    Option.when(snapshot.version >= 0)((log, snapshot))
  }

  /**
   * The commits of a log from version `first` to version `last`, each read whole and looked at
   * for the transaction of `batch`.
   */
  private def read(log: DeltaLog, first: Long, last: Long, batch: BatchId): Page = {
    // Delta lists the log here, and reads a commit when the iterator is asked for it.
    val commits = log.getChanges(first)
    @tailrec
    def next(page: Page): Page =
      if (!commits.hasNext) page
      else {
        val (version, actions) = commits.next()
        if (version > last) page
        else {
          val seen = page.saw(version, holds(actions, batch))
          // After the last of the page the iterator is let be: asking it whether there is
          // more costs nothing, and taking what follows would read a commit of no use.
          if (version == last) seen else next(seen)
        }
      }
    next(Page.Empty)
  }
}

object DeltaTableTransactions {

  val DefaultFirstPage = 16

  /** Pages grow by this: a log of any length is listed a few times, and not once a commit. */
  private val Growth = 8L

  private def larger(size: Long): Long =
    if (size > Long.MaxValue / Growth) Long.MaxValue else size * Growth

  /**
   * What a part of a log gave.
   *
   * @param first the oldest version that was read: the one asked from, unless the log does not
   *   keep it any more
   * @param found the version of the commit that holds the transaction looked for
   */
  final private case class Page(first: Option[Long], found: Option[Long]) {
    def saw(version: Long, holds: Boolean): Page =
      Page(first.orElse(Some(version)), if (holds) Some(version) else found)
  }

  private object Page {
    val Empty: Page = Page(None, None)
  }

  /**
   * Whether a commit holds the transaction of a batch. An action that this version of Delta
   * does not know is read as none, and is passed over with the rest.
   */
  private def holds(actions: Seq[Action], batch: BatchId): Boolean = actions.exists {
    case SetTransaction(app, version, _) => app == batch.app.value && version == batch.seq.value
    case _ => false
  }
}
