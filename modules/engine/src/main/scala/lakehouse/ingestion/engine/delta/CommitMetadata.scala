package lakehouse.ingestion.engine.delta

import io.circe.Json
import lakehouse.ingestion.core.config.{SafeYaml, SourceId}
import lakehouse.ingestion.core.control.{BatchId, BatchSeq, RunId, TxnAppId}
import lakehouse.ingestion.engine.control.StoredJson

/**
 * What a commit of the engine says of itself, in the `userMetadata` of the commit (ADR-0005 §3,
 * step 5): the batch that it is the write of, and the run that made it. It is what
 * `DESCRIBE HISTORY` shows next to each version, and what a batch is found by in the history
 * of a table (`DeltaTableTransactions.committedAt`).
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

  /**
   * The `userMetadata` of a commit is text that whoever writes to a table sets as it likes. A
   * commit of the engine has a few hundred characters of it, in one object; what is larger or
   * deeper is another writer's, and is not read at all.
   */
  private val Limits = SafeYaml.Limits(maxCodePoints = 2048, maxDepth = 2, maxAliases = 0)

  /**
   * Reads the `userMetadata` of a commit.
   *
   * @return none, for a commit that is not one of the engine's: a maintenance operation, a
   *   writer of its own with its own text. Fields that this version does not know are let be,
   *   so that a later one may say more
   */
  def parse(text: String): Option[CommitMetadata] =
    for {
      json <- StoredJson.parse(text, Limits).toOption
      fields <- json.asObject
      string = (name: String) => fields(name).flatMap(_.asString)
      run <- string("run_id").flatMap(RunId.parse(_).toOption)
      source <- string("source_id").flatMap(SourceId.parse(_).toOption)
      app <- string("txn_app_id").flatMap(TxnAppId.parse(_).toOption)
      // A number written as one, and not as text: what the engine wrote, or nothing.
      seq <- fields("batch_seq")
        .flatMap(_.asNumber)
        .flatMap(_.toLong)
        .flatMap(BatchSeq.of(_).toOption)
    } yield CommitMetadata(run, source, BatchId(app, seq))
}
