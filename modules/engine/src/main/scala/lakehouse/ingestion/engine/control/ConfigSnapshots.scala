package lakehouse.ingestion.engine.control

import java.sql.{Connection, SQLException}

import scala.util.Using

import io.circe.Json
import lakehouse.ingestion.core.config.{ConfigVersion, SourceId}
import lakehouse.ingestion.core.control.Layer

/**
 * The configurations that runs ran with (ADR-0007): each stored once, under its hash, and never
 * changed. A run records the hash, so the audit trail can say exactly what a run was told, long
 * after the file was edited.
 *
 * Every method runs on the connection it is given, so that the caller decides what is one
 * transaction.
 */
object ConfigSnapshots {

  /** A snapshot as it was stored. */
  final case class Stored(hash: String, content: Json)

  /**
   * Stores the configuration, unless it is there: the same configuration has the same hash,
   * whoever runs with it and however often.
   *
   * @return whether it was new
   * @throws java.lang.IllegalArgumentException if the configuration could not be read back
   *   (`StoredJson`): it is not stored, and the run that came with it does not start
   */
  @throws[SQLException]
  def store(connection: Connection, source: SourceId, version: ConfigVersion): Boolean = {
    StoredJson.readable(version.content).left.foreach { reason =>
      throw new IllegalArgumentException(
        s"the configuration of $source cannot be stored, since it could not be read back: $reason"
      )
    }
    Using.resource(
      connection.prepareStatement(
        """INSERT INTO control.config_snapshot (config_hash, source_id, content)
          |VALUES (?, ?, ?::jsonb)
          |ON CONFLICT (config_hash) DO NOTHING""".stripMargin
      )
    ) { statement =>
      statement.setString(1, version.hash)
      statement.setString(2, source.value)
      statement.setString(3, version.text)
      statement.executeUpdate() == 1
    }
  }

  @throws[SQLException]
  def find(connection: Connection, hash: String): Option[Stored] =
    one(
      connection,
      "SELECT config_hash, content::text FROM control.config_snapshot WHERE config_hash = ?"
    )(_.setString(1, hash))

  /**
   * The configuration that the state of a source was last advanced with: that of the run that
   * committed its last batch. It is what the configuration of now is compared with, to find a
   * change that the state cannot follow (`ConfigVersion.breakingChanges`).
   *
   * Not the configuration of the last run. A run that failed, or was refused for the very
   * change it came with, advanced nothing: were it the one compared with, the next run would
   * find no change, and go on from a watermark that is another column's.
   *
   * @return none for a source that has committed nothing yet in the layer: nothing to break
   */
  @throws[SQLException]
  def ofState(connection: Connection, source: SourceId, layer: Layer): Option[Stored] =
    one(
      connection,
      """SELECT snapshot.config_hash, snapshot.content::text
        |  FROM control.source_state state
        |  JOIN control.ingestion_run run ON run.run_id = state.updated_by_run
        |  JOIN control.config_snapshot snapshot ON snapshot.config_hash = run.config_hash
        | WHERE state.source_id = ? AND state.layer = ?""".stripMargin
    ) { statement =>
      statement.setString(1, source.value)
      statement.setString(2, layer.name)
    }

  private def one(connection: Connection, sql: String)(
      bind: java.sql.PreparedStatement => Unit
  ): Option[Stored] =
    Using.resource(connection.prepareStatement(sql)) { statement =>
      bind(statement)
      Using.resource(statement.executeQuery()) { rows =>
        Option.when(rows.next()) {
          val hash = rows.getString(1)
          val content = StoredJson
            .parse(rows.getString(2))
            .fold(
              reason =>
                throw new IllegalStateException(s"the snapshot $hash cannot be read back: $reason"),
              identity
            )
          Stored(hash, content)
        }
      }
    }
}
