package lakehouse.ingestion.engine.delta

import scala.annotation.tailrec

import lakehouse.ingestion.core.config.TableLocation
import lakehouse.ingestion.core.control.{BatchId, TableTransactions, TxnAppId}
import lakehouse.ingestion.engine.RunFailedException
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, CommitInfo, SetTransaction}

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
 * '''The rows of a batch are counted by its commit''' (`commit`). Delta writes in a commit how
 * many rows the write put in the table (`numOutputRows`, of the metrics of its operation),
 * unless the session says not to, which a session of the engine never does
 * (`Profile.EngineSpark.CommitMetrics`). It is the count of the write itself, made where the
 * rows were written: a batch that is read from its source while it is written has no other,
 * and one that was counted before has a second count to be held against the first.
 *
 * '''What is used of Delta that is not its API''' (discovery §5, R3). Delta shows neither
 * answer through a public method, so its own classes are asked, in this file and in no other:
 *  - `Snapshot.transactions`, the last transaction of each application, which Delta keeps in
 *    the state of a table to skip a write that it has;
 *  - `DeltaLog.getChanges`, the commits of a log from a version on, each with its actions, read
 *    one at a time as they are asked for;
 *  - `SetTransaction`, the action itself;
 *  - `CommitInfo.operationMetrics`, what a commit says of its write, in the same commit;
 *  - `DeltaLog.minSetTransactionRetentionInterval`, whether a table forgets its transactions.
 *
 * What is assumed of them is pinned by tests of Delta alone, with no code of the engine in
 * them (`DeltaSetTransactionContractSpec`, `DeltaAppendContractSpec`): an upgrade of Delta
 * that changes one fails there, by the name of the assumption, or does not compile here.
 *
 * '''Two things that a table must keep for the answers to hold.'''
 *  - ''Its transactions.'' A table that sets `delta.setTransactionRetentionDuration` has Delta
 *    forget the transaction of an application that has not written for that long. A batch that
 *    was written before would then look as one that never was, and would be written again. No
 *    table of the engine sets that property. One that has it gets no answer here, and a
 *    permanent failure in its place: whatever asks, the recovery of a batch or the write of
 *    one, would decide by an answer that may be wrong.
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
    commit(table, batch).map(_.version)

  /**
   * The commit of `batch` in `table`: where its rows are, and how many the commit says that it
   * wrote. It is found as `committedAt` is, which is this without the count.
   *
   * @return none, if the batch is not in the table, or its commit is no longer in the log that
   *   the table keeps
   */
  def commit(table: TableLocation, batch: BatchId): Option[BatchCommit] =
    current(table)
      .filter { case (_, snapshot) =>
        snapshot.transactions.get(batch.app.value).exists(_ >= batch.seq.value)
      }
      .flatMap { case (log, snapshot) =>
        @tailrec
        def search(last: Long, size: Long): Option[BatchCommit] = {
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

  /**
   * The log of a table and the table as it is now; none, if there is no table there.
   *
   * @throws RunFailedException a permanent failure, for a table that forgets its transactions
   */
  private def current(table: TableLocation): Option[(DeltaLog, Snapshot)] = {
    val log = DeltaLog.forTable(spark, new Path(table.uri))
    val snapshot = log.update()
    Option.when(snapshot.version >= 0) {
      // Asked as Delta itself asks whether to drop a transaction when it computes a state.
      if (DeltaLog.minSetTransactionRetentionInterval(snapshot.metadata).nonEmpty)
        throw RunFailedException.permanent(
          s"the table at ${table.uri} sets $TransactionRetention: Delta forgets the batches " +
            "that were written to it longer ago than that, and one of them would be written " +
            "again. Nothing is read from its log or written to it until the property is unset"
        )
      (log, snapshot)
    }
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
          val seen = page.saw(
            version,
            Option.when(holds(actions, batch))(BatchCommit(version, rows(actions)))
          )
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

  /** The property of a table that no table of the engine may have. */
  val TransactionRetention = "delta.setTransactionRetentionDuration"

  /** Pages grow by this: a log of any length is listed a few times, and not once a commit. */
  private val Growth = 8L

  private def larger(size: Long): Long =
    if (size > Long.MaxValue / Growth) Long.MaxValue else size * Growth

  /**
   * What a part of a log gave.
   *
   * @param first the oldest version that was read: the one asked from, unless the log does not
   *   keep it any more
   * @param found the commit that holds the transaction looked for
   */
  final private case class Page(first: Option[Long], found: Option[BatchCommit]) {

    /** @param holding the commit at `version`, if it holds the transaction */
    def saw(version: Long, holding: Option[BatchCommit]): Page =
      Page(first.orElse(Some(version)), holding.orElse(found))
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

  /** The metric of a write that counts the rows it put in the table. */
  val RowsWritten = "numOutputRows"

  /**
   * How many rows a commit says that its write put in the table; none, if it does not say: a
   * commit of a session that records no metrics, or of a writer that goes around Delta. A count
   * that is no whole number is none as well, and is never read as another.
   */
  private def rows(actions: Seq[Action]): Option[Long] =
    actions
      .collectFirst { case info: CommitInfo => info.operationMetrics }
      .flatten
      .flatMap(_.get(RowsWritten))
      .flatMap(_.toLongOption)
      .filter(_ >= 0)
}

/**
 * The commit of a batch in a table.
 *
 * @param version the version of the table at which the rows of the batch are
 * @param rows how many rows the commit says that it wrote; none, if it does not say
 */
final case class BatchCommit(version: Long, rows: Option[Long])
