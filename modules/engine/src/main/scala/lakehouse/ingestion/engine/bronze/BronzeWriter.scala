package lakehouse.ingestion.engine.bronze

import java.sql.Timestamp
import java.time.Instant

import scala.annotation.tailrec

import io.delta.exceptions.{
  ConcurrentTransactionException,
  MetadataChangedException,
  ProtocolChangedException
}
import lakehouse.ingestion.core.config.{EngineColumns, SourceId, TableLocation}
import lakehouse.ingestion.core.control.{BatchId, RunId, TableTransactions}
import lakehouse.ingestion.engine.RunFailedException
import lakehouse.ingestion.engine.delta.{CommitMetadata, Lake}
import lakehouse.ingestion.logstore.CommitContext
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.functions.{col, lit}

/**
 * A batch that is to be written to Bronze.
 *
 * @param run the attempt that writes it, which need not be the run that planned it
 * @param ingestedAt when the run read the rows of the batch, by the clock of its instance
 */
final case class BronzeBatch(run: RunId, source: SourceId, id: BatchId, ingestedAt: Instant)

/**
 * Writes the rows of a batch to a table of Bronze, once (ADR-0005 §3, step 5; ADR-0008).
 *
 * '''An append that Delta makes idempotent.''' The write names its transaction, the sequence
 * of the batch as `txnAppId` and its number as `txnVersion`, and Delta records it in the commit
 * of the rows and skips a write that names one the table has. A batch that is written again,
 * by a run that recovers it or by one that should have stopped, adds nothing.
 *
 * '''Every row says where it came from.''' The engine adds the run, the number of the batch,
 * when it was read and its source (`EngineColumns`) to what was read, and the commit says the
 * same of itself (`CommitMetadata`). The file of a row and what of a record did not fit its
 * columns are the read's to give, under their names, and are written as they come.
 *
 * '''Inside the context of the run''' (`CommitContext`), so that the LogStore records the run
 * in the entry of each commit and counts the versions that the write lost to other writers.
 *
 * '''The schema of a table is not changed by a write.''' Delta checks the rows against the
 * table, and the write says that nothing is to be merged into its schema, whatever the session
 * is set to: what a source may change in a table is the policy of its schema (P9).
 *
 * @param lake where every table written to must be
 * @param tables what is asked of a table about its batches, before the write and after it
 * @param context the context of the run that every batch given here is written by
 */
final class BronzeWriter(lake: Lake, tables: TableTransactions, context: CommitContext) {
  import BronzeWriter._

  /**
   * @param rows what was read for the batch: the columns of the source, and those of the
   *   engine that a read gives (`_source_file`, `_rescued_data`) if it has them. It is computed
   *   by the write, and again if the write is tried again
   * @throws RunFailedException a permanent failure: a table outside the lake, or one that
   *   forgets its transactions; rows with a column under a name of the engine's, or that the
   *   table does not take as they are; a batch that another writer of the same sequence got
   *   ahead of without writing it. A conflict: a table that others kept changing
   */
  def write(table: TableLocation, batch: BronzeBatch, rows: DataFrame): Result = {
    require(
      context.runId == batch.run.uuid,
      s"the batch is written by run ${batch.run}, and the context is of ${context.runId}"
    )
    lake.confine(table)
    refused(rows.columns.toSeq).headOption.foreach { case (name, reserved) =>
      throw RunFailedException.permanent(
        s"the rows of ${batch.source} have a column $name, and $reserved is a column that " +
          "the engine adds to every row. Nothing was written: the column is to have another " +
          "name before it gets here"
      )
    }
    append(table, batch, withEngineColumns(rows, batch), conflicts = 0)
  }

  private def has(table: TableLocation, batch: BatchId): Boolean =
    tables.lastVersion(table, batch.app).exists(_ >= batch.seq.value)

  @tailrec
  private def append(
      table: TableLocation,
      batch: BronzeBatch,
      rows: DataFrame,
      conflicts: Int
  ): Result =
    // Asked before every attempt: Delta skips a write that the table has without a word, and
    // what this write did is told by what the table said before it.
    if (has(table, batch.id)) AlreadyThere(tables.committedAt(table, batch.id))
    else
      attempt(table, batch, rows) match {
        case None =>
          val version = tables.committedAt(table, batch.id).getOrElse {
            throw new IllegalStateException(
              s"${batch.id} was written to ${table.uri}, and no commit of its log holds it"
            )
          }
          Appended(version, conflicts)

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
          append(table, batch, rows, conflicts + 1)

        case Some(e) =>
          throw RunFailedException.conflict(
            s"the table at ${table.uri} was changed by another writer under ${batch.id}, " +
              s"${conflicts + 1} times in a row: ${firstLine(e.getMessage)}",
            e
          )
      }

  /**
   * One write of the batch.
   *
   * @return none, if Delta took it; the conflict that it lost to, if the table changed under it
   */
  private def attempt(
      table: TableLocation,
      batch: BronzeBatch,
      rows: DataFrame
  ): Option[Throwable] =
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

object BronzeWriter {

  /** How a batch came to be in its table. */
  sealed trait Result

  /**
   * This write committed the batch.
   *
   * @param version the version of the table at which its rows are
   * @param conflicts how many times the write was made again because the table had changed
   */
  final case class Appended(version: Long, conflicts: Int) extends Result

  /**
   * The table was at this batch of its sequence already, or beyond it, and nothing was written.
   *
   * @param version where the batch is; none, if no commit that the log of the table keeps
   *   holds it
   */
  final case class AlreadyThere(version: Option[Long]) extends Result

  /**
   * How many times a write is made again on a table that changed under it. Once is what two
   * sources that make one table at the same moment need (ADR-0010); a budget of retries is the
   * classification of errors (P8).
   */
  val Retries = 1

  /** The columns of the engine that a read gives, and that are written as they come. */
  private val OfTheRead = Set(EngineColumns.SourceFile, EngineColumns.RescuedData)

  /**
   * The columns of some rows that may not be written, each with the column of the engine that
   * it would be taken for.
   *
   * A name of the engine's is refused however it is cased, since Spark and Delta resolve
   * names so. The two that a read gives are let through under their exact names alone: under
   * any other casing they are a column of the source.
   */
  def refused(columns: Seq[String]): Seq[(String, String)] =
    columns.flatMap { name =>
      EngineColumns
        .reserved(name)
        .filterNot(reserved => OfTheRead(reserved) && reserved == name)
        .map(name -> _)
    }

  private def withEngineColumns(rows: DataFrame, batch: BronzeBatch): DataFrame =
    rows.select(
      col("*"),
      lit(batch.run.toString).as(EngineColumns.RunId),
      lit(batch.id.seq.value).as(EngineColumns.BatchSeq),
      lit(Timestamp.from(batch.ingestedAt)).as(EngineColumns.IngestedAt),
      lit(batch.source.value).as(EngineColumns.SourceId)
    )

  private def firstLine(message: String): String =
    Option(message).flatMap(_.linesIterator.map(_.trim).find(_.nonEmpty)).getOrElse("no message")
}
