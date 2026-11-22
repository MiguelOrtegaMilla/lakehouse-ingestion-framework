package lakehouse.ingestion.engine.control

import java.sql.{Connection, SQLException, Types}
import java.time.OffsetDateTime
import java.util.UUID

import scala.util.Using

import io.circe.Json
import lakehouse.ingestion.core.config.SourceId
import lakehouse.ingestion.core.control.{BatchSeq, Layer, RunId, SourceState}

/**
 * How far each source has been ingested in each layer (discovery §13.2): the watermark that the
 * next batch starts from, and the last batch that is accounted for.
 *
 * It is moved by the commit of a batch, in the transaction that also marks the batch committed
 * and its run succeeded (ADR-0005 §3, step 7): there is no moment at which the state says more,
 * or less, than what the batches say. And by an operator's reset, which says why
 * (`state_change_log`, ADR-0021 §3), and moves the watermark and nothing of the batches.
 *
 * Every method runs on the connection it is given.
 */
object SourceStates {

  @throws[SQLException]
  def find(connection: Connection, source: SourceId, layer: Layer): Option[SourceState] =
    Using.resource(
      connection.prepareStatement(
        """SELECT watermark::text, last_committed_batch_seq, updated_by_run, updated_at
          |  FROM control.source_state WHERE source_id = ? AND layer = ?""".stripMargin
      )
    ) { statement =>
      statement.setString(1, source.value)
      statement.setString(2, layer.name)
      Using.resource(statement.executeQuery()) { row =>
        Option.when(row.next()) {
          def wrong(what: String, reason: String) =
            throw new IllegalStateException(s"the $what of the state of $source/$layer $reason")
          SourceState(
            source = source,
            layer = layer,
            watermark = Option(row.getString(1)).map { text =>
              StoredJson.parse(text).fold(wrong("watermark", _), identity)
            },
            lastCommitted = BatchSeq.of(row.getLong(2)).fold(wrong("last batch", _), identity),
            updatedBy = RunId.of(row.getObject(3, classOf[UUID])).fold(wrong("run", _), identity),
            updatedAt = row.getObject(4, classOf[OffsetDateTime]).toInstant
          )
        }
      }
    }

  /**
   * Moves the state of a source past a batch: the first time, it is made.
   *
   * @param watermark where the next batch starts from; none, for a source that has none
   * @return false if the state was at this batch already, or past it, and nothing was changed:
   *   the state only moves forward
   */
  @throws[SQLException]
  def advance(
      connection: Connection,
      source: SourceId,
      layer: Layer,
      watermark: Option[Json],
      batch: BatchSeq,
      run: RunId
  ): Boolean = {
    watermark.foreach(Batches.readable("watermark", _))
    Using.resource(
      connection.prepareStatement(
        """INSERT INTO control.source_state
          |  (source_id, layer, watermark, last_committed_batch_seq, updated_by_run)
          |VALUES (?, ?, ?::jsonb, ?, ?)
          |ON CONFLICT (source_id, layer) DO UPDATE
          |   SET watermark = EXCLUDED.watermark,
          |       last_committed_batch_seq = EXCLUDED.last_committed_batch_seq,
          |       updated_by_run = EXCLUDED.updated_by_run,
          |       updated_at = now()
          | WHERE control.source_state.last_committed_batch_seq < EXCLUDED.last_committed_batch_seq""".stripMargin
      )
    ) { statement =>
      statement.setString(1, source.value)
      statement.setString(2, layer.name)
      watermark match {
        case Some(value) => statement.setString(3, value.noSpaces)
        case None => statement.setNull(3, Types.VARCHAR)
      }
      statement.setLong(4, batch.value)
      statement.setObject(5, run.uuid)
      statement.executeUpdate() == 1
    }
  }

  /**
   * Resets the state of a source to `watermark`, by the run `run` (ADR-0021 §3): the
   * configuration of that run is the one that later runs are compared with, since the state was
   * last moved by it. The last batch committed does not move: the sequence goes on.
   *
   * @param before the state as it was read: the reset is of that state, and of no other
   * @return false if the state is not `before` any more, and nothing was changed
   */
  @throws[SQLException]
  def reset(
      connection: Connection,
      before: SourceState,
      watermark: Option[Json],
      run: RunId
  ): Boolean = {
    watermark.foreach(Batches.readable("watermark", _))
    Using.resource(
      connection.prepareStatement(
        """UPDATE control.source_state
          |   SET watermark = ?::jsonb, updated_by_run = ?, updated_at = now()
          | WHERE source_id = ? AND layer = ?
          |   AND last_committed_batch_seq = ? AND updated_by_run = ?
          |   AND watermark IS NOT DISTINCT FROM ?::jsonb""".stripMargin
      )
    ) { statement =>
      def setWatermark(index: Int, value: Option[Json]): Unit = value match {
        case Some(json) => statement.setString(index, json.noSpaces)
        case None => statement.setNull(index, Types.VARCHAR)
      }
      setWatermark(1, watermark)
      statement.setObject(2, run.uuid)
      statement.setString(3, before.source.value)
      statement.setString(4, before.layer.name)
      statement.setLong(5, before.lastCommitted.value)
      statement.setObject(6, before.updatedBy.uuid)
      setWatermark(7, before.watermark)
      statement.executeUpdate() == 1
    }
  }
}
