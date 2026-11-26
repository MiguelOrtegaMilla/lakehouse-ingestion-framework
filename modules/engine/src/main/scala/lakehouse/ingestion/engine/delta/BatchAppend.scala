package lakehouse.ingestion.engine.delta

import scala.annotation.tailrec

import io.delta.exceptions.{
  ConcurrentTransactionException,
  MetadataChangedException,
  ProtocolChangedException
}
import lakehouse.ingestion.core.config.{SourceId, TableLocation}
import lakehouse.ingestion.core.control.{BatchId, RunId}
import lakehouse.ingestion.engine.failure.RunFailedException
import lakehouse.ingestion.logstore.CommitContext
import org.apache.spark.sql.{AnalysisException, DataFrame}

/**
 * Appends the rows of a batch to a table of the lake, once (ADR-0005 §3, steps 5 and 6): what
 * every table that a batch writes is written by, its quarantine and its data alike.
 *
 * '''An append that Delta makes idempotent.''' The write names its transaction, the sequence
 * of the batch as `txnAppId` and its number as `txnVersion`, and Delta records it in the commit
 * of the rows and skips a write that names one the table has. A batch that is written again,
 * by a run that recovers it or by one that should have stopped, adds nothing. The transactions
 * of a sequence are kept by each table for itself, so the quarantine of a batch and its data
 * are each written once, whichever a run got to.
 *
 * '''The commit says its batch''' (`CommitMetadata`), '''inside the context of the run'''
 * (`CommitContext`), so that the LogStore records the run in the entry of each commit and
 * counts the versions that the write lost to other writers.
 *
 * '''The schema of a table is not changed by a write.''' Delta checks the rows against the
 * table, and the write says that nothing is to be merged into its schema, whatever the session
 * is set to.
 *
 * @param lake where every table written to must be
 * @param tables what is asked of a table about its batches, before the write and after it
 * @param context the context of the run that every batch given here is written by
 */
final class BatchAppend(lake: Lake, tables: DeltaTableTransactions, context: CommitContext) {
  import BatchAppend._

  /**
   * @param rows the rows as they are to be in the table. They are computed by the write, and
   *   again if the write is tried again
   * @throws RunFailedException a permanent failure: a table outside the lake, or one that
   *   forgets its transactions; rows that the table does not take as they are; a batch that
   *   another writer of the same sequence got ahead of without writing it; a table whose
   *   schema or protocol others kept changing
   */
  def append(table: TableLocation, batch: Written, rows: DataFrame): Result = {
    require(
      context.runId == batch.run.uuid,
      s"the batch is written by run ${batch.run}, and the context is of ${context.runId}"
    )
    lake.confine(table)
    attempts(table, batch, rows, conflicts = 0)
  }

  private def has(table: TableLocation, batch: BatchId): Boolean =
    tables.lastVersion(table, batch.app).exists(_ >= batch.seq.value)

  @tailrec
  private def attempts(
      table: TableLocation,
      batch: Written,
      rows: DataFrame,
      conflicts: Int
  ): Result =
    // Asked before every attempt: Delta skips a write that the table has without a word, and
    // what this write did is told by what the table said before it.
    if (has(table, batch.id)) AlreadyThere(tables.committedAt(table, batch.id))
    else
      attempt(table, batch, rows) match {
        case None =>
          // The table is asked for the batch after the write: where it is, and how many rows
          // its commit counted. For rows that are read from their source as they are written,
          // it is the one count there is.
          val commit = tables.commit(table, batch.id).getOrElse {
            throw new IllegalStateException(
              s"${batch.id} was written to ${table.uri}, and no commit of its log holds it"
            )
          }
          Appended(commit.version, conflicts, commit.rows)

        // Another writer committed under the name of this sequence while the batch was written:
        // a run that lost its lease and went on, or this one, if it is the one that lost it. It
        // is never tried again (ADR-0010). If the table has the batch, the other wrote the same
        // plan, and the batch is there.
        case Some(e: ConcurrentTransactionException) =>
          if (has(table, batch.id)) AlreadyThere(tables.committedAt(table, batch.id))
          else
            throw RunFailedException.permanent(
              s"another writer committed to ${table.uri} under ${batch.id.app} while ${batch.id} " +
                "was written, and the table does not have the batch: two writers of one " +
                "sequence, which the lease of the source is there to prevent",
              e
            )

        // The table was made, or its schema or protocol changed, after this write looked at it:
        // another source of the same table, most often making it at the same moment. The write
        // is made again on the table as it is now, where Delta checks the rows anew.
        case Some(_) if conflicts < Retries =>
          attempts(table, batch, rows, conflicts + 1)

        // A conflict that asks for a re-plan is a permanent failure once its one retry is spent
        // (ADR-0010 §1, ADR-0022 §1): what changed is the table, and a write that the table as it
        // is now does not take is somebody's to look at. P9 adds the policy of the schema.
        case Some(e) =>
          throw RunFailedException.permanent(
            s"the schema or the protocol of the table at ${table.uri} was changed by another " +
              s"writer under ${batch.id}, ${conflicts + 1} times in a row, and nothing of the " +
              s"batch was written: ${firstLine(e.getMessage)}",
            e
          )
      }

  /**
   * One write of the batch.
   *
   * @return none, if Delta took it; the conflict that it lost to, if the table changed under it
   */
  private def attempt(table: TableLocation, batch: Written, rows: DataFrame): Option[Throwable] =
    try {
      context.attribute {
        rows.write
          .format("delta")
          .mode("append")
          .option("txnAppId", batch.id.app.value)
          .option("txnVersion", batch.id.seq.value)
          .option(
            CommitMetadata.WriteOption,
            CommitMetadata(batch.run, batch.source, batch.id).text
          )
          .option("mergeSchema", "false")
          .save(table.uri)
      }
      None
    } catch {
      case e @ (_: ConcurrentTransactionException | _: ProtocolChangedException |
          _: MetadataChangedException) =>
        Some(e)
      // What Delta says of rows before it writes one: columns that the table does not have,
      // of another type, or under names that it does not take.
      case e: AnalysisException =>
        throw RunFailedException.permanent(
          s"the table at ${table.uri} does not take the rows of ${batch.id} as they are, and " +
            s"nothing was written: ${firstLine(e.getMessage)}",
          e
        )
    }
}

object BatchAppend {

  /**
   * The batch that a write is of.
   *
   * @param run the attempt that writes it, which need not be the run that planned it
   */
  final case class Written(run: RunId, source: SourceId, id: BatchId)

  /** How a batch came to be in a table. */
  sealed trait Result

  /**
   * This write committed the batch.
   *
   * @param version the version of the table at which its rows are
   * @param conflicts how many times the write was made again because the table had changed
   * @param rows how many rows the commit says that the write put in the table; none, if the
   *   commit does not say (`DeltaTableTransactions.commit`)
   */
  final case class Appended(version: Long, conflicts: Int, rows: Option[Long]) extends Result

  /**
   * The table was at this batch of its sequence already, or beyond it, and nothing was written.
   *
   * @param version where the batch is; none, if no commit that the log of the table keeps
   *   holds it
   */
  final case class AlreadyThere(version: Option[Long]) extends Result

  /**
   * How many times a write is made again on a table whose schema or protocol changed under it.
   * Once is what two sources that make one table at the same moment need (ADR-0010 §1); the
   * conflicts that are retried within the budget of a run are the classifier's (ADR-0022).
   */
  val Retries = 1

  private def firstLine(message: String): String =
    Option(message).flatMap(_.linesIterator.map(_.trim).find(_.nonEmpty)).getOrElse("no message")
}
