package lakehouse.ingestion.engine.control

import java.sql.{Connection, SQLException}

import scala.util.Using

import io.circe.Json
import lakehouse.ingestion.core.control.{BatchSeq, RunId, SourceState}

/**
 * The changes of the state of a source that are not the commit of a batch: the resets of an
 * operator (`state_change_log`, ADR-0021 §3). One row for each, written in the transaction that
 * makes the change, by the run that makes it; who asked and why are that run's. A row is written
 * and never changed: the engine has no grant to.
 *
 * Every method runs on the connection it is given.
 */
object StateChanges {

  /** The state of a source as a row of the log keeps it, before or after a change. */
  def document(watermark: Option[Json], lastCommitted: BatchSeq, updatedBy: RunId): Json =
    Json.obj(
      "watermark" -> watermark.getOrElse(Json.Null),
      "last_committed_batch_seq" -> Json.fromLong(lastCommitted.value),
      "updated_by_run" -> Json.fromString(updatedBy.toString)
    )

  def document(state: SourceState): Json =
    document(state.watermark, state.lastCommitted, state.updatedBy)

  /** Writes down that the run `run` changed the state of its source from `before` to `after`. */
  @throws[SQLException]
  def record(connection: Connection, run: RunId, before: SourceState, after: Json): Unit =
    Using.resource(
      connection.prepareStatement(
        """INSERT INTO control.state_change_log (run_id, source_id, layer, before, after)
          |VALUES (?, ?, ?, ?::jsonb, ?::jsonb)""".stripMargin
      )
    ) { statement =>
      statement.setObject(1, run.uuid)
      statement.setString(2, before.source.value)
      statement.setString(3, before.layer.name)
      statement.setString(4, document(before).noSpaces)
      statement.setString(5, after.noSpaces)
      statement.executeUpdate(): Unit
    }
}
