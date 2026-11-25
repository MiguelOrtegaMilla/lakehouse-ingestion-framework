package lakehouse.ingestion.engine.quarantine

import java.sql.Timestamp
import java.time.Instant

import lakehouse.ingestion.core.config.{EngineColumns, SourceId, TableLocation}
import lakehouse.ingestion.core.control.{BatchId, Quarantine, RunId}
import lakehouse.ingestion.engine.connector.ReadResult
import lakehouse.ingestion.engine.delta.{BatchAppend, DeltaTableTransactions, Lake}
import lakehouse.ingestion.logstore.CommitContext
import org.apache.spark.sql.DataFrame
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
 * @param lake where every table written to must be
 * @param tables what is asked of a table about its batches, before the write and after it
 * @param context the context of the run that every batch given here is written by
 */
final class QuarantineWriter(lake: Lake, tables: DeltaTableTransactions, context: CommitContext) {

  private val appends = new BatchAppend(lake, tables, context)

  /**
   * @param unparsed the records that a read set apart (`ReadResult.unparsed`): the file of
   *   each, and its text
   * @throws lakehouse.ingestion.engine.failure.RunFailedException as `BatchAppend.append`
   */
  def write(table: TableLocation, batch: QuarantineBatch, unparsed: DataFrame): BatchAppend.Result =
    appends.append(
      table,
      BatchAppend.Written(batch.run, batch.source, batch.id),
      unparsed.select(
        lit(batch.run.toString).as(Quarantine.RunId),
        lit(batch.id.seq.value).as(Quarantine.BatchSeq),
        col(EngineColumns.SourceFile).cast(StringType).as(Quarantine.SourceFile),
        lit(Quarantine.ErrorType.Unparsed).as(Quarantine.ErrorTypeColumn),
        lit(null).cast(StringType).as(Quarantine.ErrorDetail),
        col(ReadResult.Record).cast(StringType).as(Quarantine.RawRecord),
        lit(Timestamp.from(batch.quarantinedAt)).as(Quarantine.QuarantinedAt)
      )
    )
}
