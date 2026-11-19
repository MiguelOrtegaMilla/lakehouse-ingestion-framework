package lakehouse.ingestion.engine.bronze

import java.sql.Timestamp
import java.time.Instant

import lakehouse.ingestion.core.config.{EngineColumns, SourceId, TableLocation}
import lakehouse.ingestion.core.control.{BatchId, RunId, TableTransactions}
import lakehouse.ingestion.engine.RunFailedException
import lakehouse.ingestion.engine.delta.{BatchAppend, Lake}
import lakehouse.ingestion.logstore.CommitContext
import org.apache.spark.sql.DataFrame
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
 * '''Every row says where it came from.''' The engine adds the run, the number of the batch,
 * when it was read and its source (`EngineColumns`) to what was read. The file of a row and
 * what of a record did not fit its columns are the read's to give, under their names, and are
 * written as they come.
 *
 * '''The write is an append that Delta makes idempotent''', inside the context of the run, and
 * it changes nothing of the schema of its table (`BatchAppend`): what a source may change in a
 * table is the policy of its schema (P9).
 *
 * @param lake where every table written to must be
 * @param tables what is asked of a table about its batches, before the write and after it
 * @param context the context of the run that every batch given here is written by
 */
final class BronzeWriter(lake: Lake, tables: TableTransactions, context: CommitContext) {
  import BronzeWriter._

  private val appends = new BatchAppend(lake, tables, context)

  /**
   * @param rows what was read for the batch: the columns of the source, and those of the
   *   engine that a read gives (`_source_file`, `_rescued_data`) if it has them. It is computed
   *   by the write, and again if the write is tried again
   * @throws RunFailedException a permanent failure: a table outside the lake, or one that
   *   forgets its transactions; rows with a column under a name of the engine's, or that the
   *   table does not take as they are; a batch that another writer of the same sequence got
   *   ahead of without writing it. A conflict: a table that others kept changing
   */
  def write(table: TableLocation, batch: BronzeBatch, rows: DataFrame): BatchAppend.Result = {
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
    appends.append(
      table,
      BatchAppend.Written(batch.run, batch.source, batch.id),
      withEngineColumns(rows, batch)
    )
  }
}

object BronzeWriter {

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
}
