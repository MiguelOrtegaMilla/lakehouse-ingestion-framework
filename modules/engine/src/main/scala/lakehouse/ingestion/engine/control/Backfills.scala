package lakehouse.ingestion.engine.control

import java.sql.{Connection, ResultSet, SQLException}
import java.time.{Instant, OffsetDateTime}
import java.util.UUID

import scala.concurrent.duration._
import scala.util.Using

import io.circe.Json
import lakehouse.ingestion.core.config.SourceId
import lakehouse.ingestion.core.control._

/**
 * The backfills of the control plane (`control.backfill`, ADR-0021 §5): what each reads, fixed
 * when it is created, and how far it has read.
 *
 * A backfill is created by a run of its sequence, under its lease, and moved by the commit of
 * each chunk, in the transaction of the commit (`LeasedRun`). The database holds what it can of
 * it by itself: its range and its chunk are written once, a run of another sequence neither
 * creates it nor moves it, and one that is complete does not change.
 *
 * The moments of its range reach the database as text in UTC and are made times there, exactly:
 * a number of microseconds since the epoch would pass through a `double precision`, which does
 * not hold the years of a range to the microsecond. A chunk is a number of milliseconds, as a
 * duration of a configuration is.
 */
object Backfills {

  private val Columns =
    """source_id, layer, backfill_id, range_from, range_to,
      |(extract(epoch FROM chunk) * 1000)::bigint AS chunk_ms, created_by_run, created_at,
      |progress::text AS progress, last_committed_batch_seq, updated_by_run, updated_at,
      |completed_at""".stripMargin

  /** The backfill of a source in a layer by its id, if there is one. */
  @throws[SQLException]
  def find(
      connection: Connection,
      source: SourceId,
      layer: Layer,
      id: BackfillId
  ): Option[BackfillRecord] =
    Using.resource(
      connection.prepareStatement(
        s"SELECT $Columns FROM control.backfill WHERE source_id = ? AND layer = ? AND backfill_id = ?"
      )
    ) { statement =>
      statement.setString(1, source.value)
      statement.setString(2, layer.name)
      statement.setString(3, id.value)
      Using.resource(statement.executeQuery())(row => Option.when(row.next())(record(row)))
    }

  /**
   * Creates the backfill whose sequence `start` acts on, as `start` is the run that creates it.
   * Its range is not checked here, but for what the database refuses: that is the caller's,
   * which has the clock of the source (`BackfillRange`).
   *
   * @throws IllegalArgumentException if `start` is not a run of a backfill, or the chunk is no
   *   stretch of whole milliseconds
   * @throws java.sql.SQLException if the backfill is there already, or the database refuses it
   */
  @throws[SQLException]
  def create(
      connection: Connection,
      start: RunStart,
      from: Instant,
      to: Instant,
      chunk: FiniteDuration
  ): BackfillRecord = {
    require(start.kind == RunKind.Backfill, s"a backfill is created by a run of a backfill")
    val id = start.backfill.getOrElse(
      throw new IllegalArgumentException("a backfill is created by a run of its own sequence")
    )
    require(
      chunk.toNanos > 0 && chunk.toNanos % 1000000 == 0,
      s"a chunk of $chunk is no stretch of whole milliseconds"
    )
    Using.resource(
      connection.prepareStatement(
        """INSERT INTO control.backfill
          |  (source_id, layer, backfill_id, range_from, range_to, chunk, created_by_run)
          |VALUES (?, ?, ?, ?::timestamptz, ?::timestamptz, ? * interval '1 millisecond', ?)
          |RETURNING created_at""".stripMargin
      )
    ) { statement =>
      statement.setString(1, start.source.value)
      statement.setString(2, start.layer.name)
      statement.setString(3, id.value)
      statement.setString(4, from.toString)
      statement.setString(5, to.toString)
      statement.setLong(6, chunk.toMillis)
      statement.setObject(7, start.id.uuid)
      Using.resource(statement.executeQuery()) { row =>
        if (!row.next())
          throw new IllegalStateException(s"no backfill was written for ${start.txnAppId}")
        BackfillRecord(
          start.source,
          start.layer,
          id,
          from,
          to,
          chunk,
          start.id,
          row.getObject("created_at", classOf[OffsetDateTime]).toInstant,
          progress = None,
          completedAt = None
        )
      }
    }
  }

  /**
   * Moves the progress of a backfill to the watermark that the commit of its chunk `batch`
   * records, and marks it complete if that chunk is its last. It is one statement of the
   * transaction of the commit.
   *
   * @return false if the backfill is past this chunk already, at it, or complete: the progress
   *   is not what the run that commits took it for, and nothing was written
   */
  @throws[SQLException]
  def advance(
      connection: Connection,
      batch: BatchId,
      watermark: Json,
      run: RunId,
      completes: Boolean
  ): Boolean = {
    Batches.readable("progress of a backfill", watermark)
    Using.resource(
      connection.prepareStatement(
        """UPDATE control.backfill
          |   SET progress = ?::jsonb, last_committed_batch_seq = ?, updated_by_run = ?,
          |       updated_at = now(), completed_at = CASE WHEN ? THEN now() END
          | WHERE txn_app_id = ? AND completed_at IS NULL
          |   AND (last_committed_batch_seq IS NULL OR last_committed_batch_seq < ?)""".stripMargin
      )
    ) { statement =>
      statement.setString(1, watermark.noSpaces)
      statement.setLong(2, batch.seq.value)
      statement.setObject(3, run.uuid)
      statement.setBoolean(4, completes)
      statement.setString(5, batch.app.value)
      statement.setLong(6, batch.seq.value)
      statement.executeUpdate() == 1
    }
  }

  private def record(row: ResultSet): BackfillRecord = {
    def known[A](column: String, value: Either[String, A]): A =
      value.fold(
        reason =>
          throw new IllegalStateException(
            s"control.backfill.$column '${row.getString(column)}' $reason"
          ),
        identity
      )
    def instant(column: String): Option[Instant] =
      Option(row.getObject(column, classOf[OffsetDateTime])).map(_.toInstant)
    def run(column: String): Option[RunId] =
      Option(row.getObject(column, classOf[UUID])).map(uuid => known(column, RunId.of(uuid)))

    val source = known("source_id", SourceId.parse(row.getString("source_id")))
    val layer = known("layer", Layer.parse(row.getString("layer")))
    val id = known("backfill_id", BackfillId.parse(row.getString("backfill_id")))
    val progress = Option(row.getString("progress")).map { text =>
      val seq = row.getLong("last_committed_batch_seq")
      BackfillProgress(
        watermark = known("progress", StoredJson.parse(text)),
        lastCommitted = known("last_committed_batch_seq", BatchSeq.of(seq)),
        updatedBy = run("updated_by_run").get,
        updatedAt = instant("updated_at").get
      )
    }
    BackfillRecord(
      source,
      layer,
      id,
      from = instant("range_from").get,
      to = instant("range_to").get,
      chunk = row.getLong("chunk_ms").millis,
      createdBy = run("created_by_run").get,
      createdAt = instant("created_at").get,
      progress = progress,
      completedAt = instant("completed_at")
    )
  }
}
