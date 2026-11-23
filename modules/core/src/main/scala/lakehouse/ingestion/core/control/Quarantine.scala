package lakehouse.ingestion.core.control

import lakehouse.ingestion.core.config.{RelativePath, SourceId}

/**
 * The quarantine of a source: where the records that a layer could not take are kept, as they
 * were received, so that nothing that a source sent is lost without a word (ADR-0008, discovery
 * §13.4).
 *
 * It is a table of the lake, one for each source and layer, under the directory that is the
 * engine's own (`RelativePath.Quarantine`): a table by source keeps the retention of rejected
 * records, and who may read them, a matter of one source. A record is kept whole, as text, and
 * it holds whatever the source holds: the personal data of a source is in its quarantine too.
 *
 * A batch writes its quarantine before its data (ADR-0005 §3, step 5), under the same number,
 * and whatever becomes of the batch. So the records of a batch that is given up afterwards
 * (ADR-0018) stay in it, and the batch after it sets the same records apart again under its own
 * number. A record says its run (`run_id`), and the control plane says which batch the run
 * took, in which sequence, and how that batch ended: whoever reads a quarantine leaves out the
 * records of the runs whose batch is `ABANDONED` there, as the reader of the engine does
 * (`QuarantineReader`). The number of a record (`batch_seq`) does not say its batch by itself:
 * every sequence of a source, the regular one and that of each backfill, writes to the one
 * quarantine of the source, and numbers its batches from 1.
 */
object Quarantine {

  /** `_quarantine/<layer>/<source_id>`, relative to the lake. */
  def table(source: SourceId, layer: Layer): RelativePath =
    RelativePath
      .parse(s"${layer.name}/${source.value}")
      .map(RelativePath.quarantine)
      .fold(
        // The id of a source is a segment of a path by its own grammar, and so is a layer.
        reason => throw new IllegalStateException(s"the quarantine of $source/$layer $reason"),
        identity
      )

  /** The run that set the record apart: the attempt, which the audit trail has. */
  val RunId = "run_id"

  /** The number of the batch that the record was read for, in the sequence of its run. */
  val BatchSeq = "batch_seq"

  /** The file that the record was read from; none for a source that is no file. */
  val SourceFile = "source_file"

  /** Why the record was not taken: one of `ErrorType`. */
  val ErrorTypeColumn = "error_type"

  /** What more is known of why; none, where the type says all that is known. */
  val ErrorDetail = "error_detail"

  /** The record, as it was in the source. */
  val RawRecord = "raw_record"

  /** When the run set the record apart, by the clock of its instance. */
  val QuarantinedAt = "quarantined_at"

  /** The columns of a quarantine, in their order. */
  val Columns: Seq[String] =
    Seq(RunId, BatchSeq, SourceFile, ErrorTypeColumn, ErrorDetail, RawRecord, QuarantinedAt)

  /** Why a record is in quarantine. The names are those of `error_type`. */
  object ErrorType {

    /**
     * The parser of the format of the source could not make a row of the record: a line that
     * is not JSON, a record of CSV with more values or fewer than its header. The parser does
     * not say more, so there is no detail.
     */
    val Unparsed = "unparsed"

    val All: Seq[String] = Seq(Unparsed)
  }
}
