package lakehouse.ingestion.core.control

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import io.circe.Json
import lakehouse.ingestion.core.config.SourceId
import lakehouse.ingestion.core.watermark.BackfillRange

/**
 * What an operator asks of a backfill (ADR-0021 §5.3): its id, its range and how long its chunks
 * are. The first run under the id creates the backfill with them; every run after it resumes
 * the backfill, and is refused if it asks for another range or another chunk.
 *
 * @param chunk how long a chunk is, at most: whole milliseconds, as a duration of a
 *   configuration is. None takes `max_batch_range` of the source when the backfill is created,
 *   and the chunk of the backfill when it is resumed
 */
final case class BackfillRequest(
    id: BackfillId,
    range: BackfillRange,
    chunk: Option[FiniteDuration]
) {
  chunk.foreach { length =>
    require(
      length.toNanos > 0 && length.toNanos % 1000000 == 0,
      s"a chunk of $length is no stretch of whole milliseconds"
    )
  }
}

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
