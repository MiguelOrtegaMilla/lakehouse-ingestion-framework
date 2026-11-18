package lakehouse.ingestion.engine.delta

import io.circe.Json
import lakehouse.ingestion.core.config.SourceId
import lakehouse.ingestion.core.control.{BatchId, RunId}

/**
 * What a commit of the engine says of itself, in the `userMetadata` of the commit (ADR-0005 §3,
 * step 5): the batch that it is the write of, and the run that made it. It is what
 * `DESCRIBE HISTORY` shows next to each version, for whoever reads the history of a table.
 *
 * It describes a commit, and decides nothing. Any writer of a table sets the `userMetadata` of
 * its commits as it likes, so the engine does not read it back: whether a batch is in a table,
 * and at which version, is asked of the transaction that Delta records with the write
 * (`DeltaTableTransactions`).
 *
 * The batch is said whole, by the name of its sequence and its number. ADR-0005 lists the
 * number and the source, which do not tell a batch from the chunk of a backfill with the same
 * number: a backfill has a sequence of its own on the same table (ADR-0005 §2).
 *
 * @param run the attempt that wrote the commit, which need not be the run that commits the
 *   batch in the control plane
 */
final case class CommitMetadata(run: RunId, source: SourceId, batch: BatchId) {

  /** As it is written in a commit: one line of JSON, with these names, in this order. */
  def text: String = Json
    .obj(
      "run_id" -> Json.fromString(run.toString),
      "source_id" -> Json.fromString(source.value),
      "txn_app_id" -> Json.fromString(batch.app.value),
      "batch_seq" -> Json.fromLong(batch.seq.value)
    )
    .noSpaces
}

object CommitMetadata {

  /** The option of a write to Delta that sets the `userMetadata` of its commit, and of no other. */
  val WriteOption = "userMetadata"
}
