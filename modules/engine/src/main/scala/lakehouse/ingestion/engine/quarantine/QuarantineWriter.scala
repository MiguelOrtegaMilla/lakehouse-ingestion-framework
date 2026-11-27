package lakehouse.ingestion.engine.quarantine

import java.sql.Timestamp
import java.time.Instant

import lakehouse.ingestion.core.config.{EngineColumns, SourceId, TableLocation}
import lakehouse.ingestion.core.control.{BatchId, Quarantine, RunId}
import lakehouse.ingestion.engine.connector.ReadResult
import lakehouse.ingestion.engine.delta.{BatchAppend, DeltaTableTransactions, Lake}
import lakehouse.ingestion.logstore.CommitContext
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StringType

/**
 * The records of a batch that are to be set apart.
 *
 * @param run the attempt that writes them, which need not be the run that planned the batch
 * @param quarantinedAt when the run read the records, by the clock of its instance
 */
final case class QuarantineBatch(run: RunId, source: SourceId, id: BatchId, quarantinedAt: Instant)

/**
 * Writes the records of a batch that could not be parsed to the quarantine of their source,
 * once (ADR-0005 §3, step 5; discovery §13.4).
 *
 * '''A record is kept whole''', as the text that it was in the source, with the file that it
 * came from, the run and the batch that set it apart, and why (`Quarantine`). It holds what
 * the source holds, the personal data of the source among it: who may read a quarantine, and
 * for how long it is kept, are those of its source.
 *
 * '''Why is what is known, and no more.''' What a read sets apart is what the parser of its
 * format could not make a row of, and the parser does not say why: the type says that
 * (`unparsed`), and there is no detail. The technical validations that say more come with the
 * classification of errors (P8).
 *
 * '''The write is the one that every table of a batch has''' (`BatchAppend`): an append under
 * the number of the batch, which Delta takes once, inside the context of the run. The
 * quarantine of a batch is written before its data, so a batch whose data is in its table has
 * its quarantine written.
 *
 * '''The log of a quarantine has no text of a record.''' Delta keeps, in the log of a table,
 * the least and the greatest value of each column of each file, to pass over the files that a
 * read does not need; it has them in the checksum of a version as well, and logs both, at INFO,
 * with each snapshot of the table that it makes. For the text of a record that is its first
 * characters, in the log of whatever process reads or writes the table. So a quarantine is
 * made with the columns that those statistics are kept of said (`StatsColumns`): those that a
 * read asks by, and neither the text of a record nor its detail. A quarantine that was made
 * before this keeps the statistics that its files were written with.
 *
 * @param lake where every table written to must be
 * @param tables what is asked of a table about its batches, before the write and after it
 * @param context the context of the run that every batch given here is written by
 */
final class QuarantineWriter(lake: Lake, tables: DeltaTableTransactions, context: CommitContext) {

  private val appends = new BatchAppend(lake, tables, context)

  /**
   * @param unparsed the records that a read set apart (`ReadResult.unparsed`): the file of
   *   each, and its text, and why if the read says it; a record that does not say is one that
   *   could not be parsed
   * @throws lakehouse.ingestion.engine.failure.RunFailedException as `BatchAppend.append`
   */
  def write(
      table: TableLocation,
      batch: QuarantineBatch,
      unparsed: DataFrame
  ): BatchAppend.Result = {
    def said(column: String, otherwise: Column): Column =
      if (unparsed.columns.contains(column)) col(column).cast(StringType) else otherwise
    appends.append(
      table,
      BatchAppend.Written(batch.run, batch.source, batch.id),
      unparsed.select(
        lit(batch.run.toString).as(Quarantine.RunId),
        lit(batch.id.seq.value).as(Quarantine.BatchSeq),
        col(EngineColumns.SourceFile).cast(StringType).as(Quarantine.SourceFile),
        said(ReadResult.ErrorType, lit(Quarantine.ErrorType.Unparsed))
          .as(Quarantine.ErrorTypeColumn),
        said(ReadResult.ErrorDetail, lit(null).cast(StringType)).as(Quarantine.ErrorDetail),
        col(ReadResult.Record).cast(StringType).as(Quarantine.RawRecord),
        lit(Timestamp.from(batch.quarantinedAt)).as(Quarantine.QuarantinedAt)
      ),
      QuarantineWriter.Properties
    )
  }
}

object QuarantineWriter {

  /**
   * The columns of a quarantine that Delta keeps statistics of: what a record is asked for by,
   * and not what it holds.
   */
  val StatsColumns: Seq[String] = Seq(
    Quarantine.RunId,
    Quarantine.BatchSeq,
    Quarantine.SourceFile,
    Quarantine.ErrorTypeColumn,
    Quarantine.QuarantinedAt
  )

  /** The property of a table of Delta that says the columns that statistics are kept of. */
  val StatsColumnsProperty = "delta.dataSkippingStatsColumns"

  /** What a quarantine is made with. */
  val Properties: Map[String, String] = Map(StatsColumnsProperty -> StatsColumns.mkString(","))
}
