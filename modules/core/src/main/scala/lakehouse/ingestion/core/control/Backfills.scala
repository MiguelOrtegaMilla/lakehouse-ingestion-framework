package lakehouse.ingestion.core.control

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import io.circe.Json
import lakehouse.ingestion.core.config.SourceId

/**
 * A backfill as the control plane has it (ADR-0021 §5): what it reads, fixed when it was
 * created, and how far it has read. It is to its own sequence what `SourceState` is to the
 * regular sequence of its source, and it does not touch that state.
 *
 * @param from where its range starts, and is not in it: the range is `(from, to]`
 * @param to where its range ends, and is in it. It was not after the clock of the source less
 *   `safety_margin` when the backfill was created
 * @param chunk how long a chunk of it is, at most
 * @param createdBy the run that created it. Its configuration is the one that the backfill is
 *   held to (ADR-0007, ADR-0021 §5.9), and who asked for it and why are that run's
 * @param progress how far it has read: none, before its first chunk is committed
 * @param completedAt when the chunk that ends at `to` was committed; none, while it has more
 */
final case class BackfillRecord(
    source: SourceId,
    layer: Layer,
    id: BackfillId,
    from: Instant,
    to: Instant,
    chunk: FiniteDuration,
    createdBy: RunId,
    createdAt: Instant,
    progress: Option[BackfillProgress],
    completedAt: Option[Instant]
) {

  /** The name of its sequence, which its chunks are written to Delta under. */
  def txnAppId: TxnAppId = TxnAppId.backfill(source, layer, id)

  /** The key of its lease: the name of its sequence. */
  def leaseKey: LeaseKey = LeaseKey.backfill(source, layer, id)
}

/**
 * How far a backfill has read: moved by the commit of each chunk, in its transaction, as the
 * commit of a regular batch moves the state of its source.
 *
 * @param watermark the upper bound of the last chunk that was committed, as the connector of
 *   the source wrote it: where the next chunk starts
 * @param lastCommitted that chunk, in the sequence of the backfill
 * @param updatedBy the run that committed it
 */
final case class BackfillProgress(
    watermark: Json,
    lastCommitted: BatchSeq,
    updatedBy: RunId,
    updatedAt: Instant
)
