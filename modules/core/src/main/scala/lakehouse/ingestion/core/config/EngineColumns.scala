package lakehouse.ingestion.core.config

import java.util.Locale

/**
 * The columns that the engine adds to the rows it writes (ADR-0008, discovery §20): what a row
 * says of where it came from and of the run that wrote it. They are one list, here, because
 * three things depend on it being the same: the writers that add them, the validation that
 * keeps a configuration from naming them, and the check that a source does not bring them.
 *
 * '''Reserved, case aside.''' Spark resolves the names of columns without regard to case by
 * default, and so does Delta: a column `_RUN_ID` of a source would be the engine's `_run_id`.
 * Names that merely start with `_` are not reserved, since sources have such columns (`_id`).
 */
object EngineColumns {

  /** The run that wrote the row: the attempt, which the audit trail has (ADR-0005 §1). */
  val RunId = "_run_id"

  /** The batch that the row is part of: the `txnVersion` of the write that added it. */
  val BatchSeq = "_batch_seq"

  /** When the run that wrote the row read it. */
  val IngestedAt = "_ingested_at"

  /** The file that the row was read from; none for a source that is no file. */
  val SourceFile = "_source_file"

  /**
   * The source of the row. Several sources may write one table (ADR-0010), and this is what
   * tells their rows apart: a table that several merge into is partitioned by it.
   */
  val SourceId = "_source_id"

  /** What of a record of text did not fit the columns that were expected (ADR-0008). */
  val RescuedData = "_rescued_data"

  val All: Seq[String] = Seq(RunId, BatchSeq, IngestedAt, SourceFile, SourceId, RescuedData)

  private val lowerCase: Map[String, String] = All.map(name => name -> name).toMap

  /** The column of the engine that `name` is, case aside, if it is one. */
  def reserved(name: String): Option[String] = lowerCase.get(name.toLowerCase(Locale.ROOT))
}
