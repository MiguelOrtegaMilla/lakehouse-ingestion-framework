package lakehouse.ingestion.core.control

import java.time.Instant

import io.circe.Json
import lakehouse.ingestion.core.config.{RelativePath, SourceId, TableLocation}

/**
 * The identity of a batch: its place in the sequence of its source and layer (ADR-0005 §1). It
 * is what a write to Delta is made idempotent by: the name is the `txnAppId` of the write, and
 * the number its `txnVersion`.
 */
final case class BatchId(app: TxnAppId, seq: BatchSeq) {
  override def toString: String = s"$app#$seq"
}

/**
 * A batch whose plan is written down, and whose state is not committed: what a run finds of the
 * run before it that did not get to the end, or what it has just planned itself.
 *
 * @param plan what is to be read, as its connector wrote it: a range of a watermark, a set of
 *   files. The control plane does not read it, and nothing ever changes it
 * @param plannedBy the run that planned it, which need not be the one that commits it
 */
final case class PlannedBatch(id: BatchId, plan: Json, plannedBy: RunId, plannedAt: Instant)

/**
 * What a batch wrote: a table, by the name its source gives it, and the version of the table
 * at which the batch was committed to it.
 */
final case class BatchOutput(table: RelativePath, version: Long) {
  require(version >= 0, s"a table has no version $version")
}

/** A batch as the control plane has it. */
final case class BatchRecord(
    id: BatchId,
    source: SourceId,
    layer: Layer,
    kind: RunKind,
    plan: Json,
    status: BatchStatus,
    plannedBy: RunId,
    plannedAt: Instant,
    committedBy: Option[RunId],
    committedAt: Option[Instant],
    outputs: Option[Vector[BatchOutput]]
)

/**
 * How far a source has been ingested in a layer.
 *
 * @param watermark where the next batch starts from, as the connector of the source wrote it.
 *   None for a source that has none, as one that is loaded in full
 * @param lastCommitted the last batch whose data is in the table and accounted for here
 * @param updatedBy the run that committed it
 */
final case class SourceState(
    source: SourceId,
    layer: Layer,
    watermark: Option[Json],
    lastCommitted: BatchSeq,
    updatedBy: RunId,
    updatedAt: Instant
)

/**
 * What the engine asks a table about the batches that were written to it (ADR-0005 §3).
 *
 * Delta keeps, in the log of each table, the last `txnVersion` that was committed to it under
 * each `txnAppId`. That is all the control plane needs to know of Delta to tell a batch whose
 * data is in its table from one whose data is not, and it is asked through this, so that the
 * decision is the same whatever reads the log.
 *
 * The table is asked, and not the process that wrote to it: a batch may have been written by a
 * run that died before it said so, and the answer is then the table's alone.
 */
trait TableTransactions {

  /**
   * The last `txnVersion` that `table` has committed under `app`: what recovery decides by
   * (step 2).
   *
   * @return none, if it has committed nothing under it, or is not there yet
   */
  def lastVersion(table: TableLocation, app: TxnAppId): Option[Long]

  /**
   * The version of `table` at which `batch` was committed to it: where its rows are, which is
   * what a batch records of what it wrote (step 7, `BatchOutput`). A run asks it for a batch
   * that it wrote itself, and for one that it found written by a run before it.
   *
   * @return none, if the batch is not in the table, or its commit is no longer in the history
   *   that the table keeps
   */
  def committedAt(table: TableLocation, batch: BatchId): Option[Long]
}

/**
 * What a run does with what the run before it left (ADR-0005 §3, step 2).
 *
 * A run that died left one of three things, and the batch says which. Nothing: there is no
 * planned batch, and the run plans one. A plan and no data: the plan is run again, exactly as
 * it was written down. A plan and its data: the data is not written again, and only what the
 * dead run did not get to say is said, that the state has moved.
 */
sealed trait Recovery

object Recovery {

  /** No batch is planned: the run before this one ended, or there was none. */
  case object NothingPlanned extends Recovery

  /**
   * The data of the batch is in its table. The state is committed for it (step 7), and nothing
   * is read or written again.
   */
  final case class RollForward(batch: PlannedBatch) extends Recovery

  /**
   * The data of the batch is not in its table, or not all of it. Its plan is run again as it
   * is (step 4): what of it had been written is skipped by Delta, which knows the batch.
   */
  final case class Replay(batch: PlannedBatch) extends Recovery

  /**
   * @param planned the planned batch of the sequence, if there is one
   * @param lastWritten the table that a batch writes last. A batch that writes several, as
   *   its quarantine and then its data, has written them all once this one has it
   * @param tables what reads the last `txnVersion` of a table. It is asked only if there is a
   *   planned batch
   */
  def of(
      planned: Option[PlannedBatch],
      lastWritten: TableLocation,
      tables: TableTransactions
  ): Recovery =
    planned match {
      case None => NothingPlanned
      case Some(batch) =>
        // At or beyond: Delta skips a write that is not beyond what it has (step 6).
        val written = tables.lastVersion(lastWritten, batch.id.app).exists(_ >= batch.id.seq.value)
        if (written) RollForward(batch) else Replay(batch)
    }
}
