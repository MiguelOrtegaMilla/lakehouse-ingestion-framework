package lakehouse.ingestion.it.stress

import java.sql.Connection

import scala.util.Using

import lakehouse.ingestion.it.stress.LogModel.{RegistryEntry, RegistryView}

/**
 * Reads what the coordination database holds about a table, with plain SQL on a connection of
 * the verifier's own. It does not go through the LogStore's store: the verifier does not ask
 * the component under test what it did.
 */
object RegistryReader {

  private val Registration =
    """SELECT c.registered_version, t.reason
      |FROM delta_coordination.coordinated_table c
      |LEFT JOIN delta_coordination.table_taint t ON t.table_uri = c.table_uri
      |WHERE c.table_uri = ?""".stripMargin

  private val Entries =
    """SELECT version, complete, run_id::text,
      |       (EXTRACT(EPOCH FROM (completed_at - created_at)) * 1000)::bigint,
      |       epoch_key, epoch
      |FROM delta_coordination.commit_entry
      |WHERE table_uri = ? ORDER BY version""".stripMargin

  private val EpochsInForce =
    "SELECT epoch_key, epoch FROM delta_coordination.writer_epoch WHERE epoch_key = ANY (?)"

  /** @param tableUri the key the table is coordinated under */
  def view(connection: Connection, tableUri: String): RegistryView = {
    val registration = Using.resource(connection.prepareStatement(Registration)) { statement =>
      statement.setString(1, tableUri)
      Using.resource(statement.executeQuery()) { rows =>
        Option.when(rows.next())((rows.getLong(1), Option(rows.getString(2))))
      }
    }
    val entries = Using.resource(connection.prepareStatement(Entries)) { statement =>
      statement.setString(1, tableUri)
      Using.resource(statement.executeQuery()) { rows =>
        Iterator
          .continually(rows)
          .takeWhile(_.next())
          .map { row =>
            val took = row.getLong(4)
            val complete = Option.unless(row.wasNull())(took)
            val epoch = row.getLong(6)
            RegistryEntry(
              row.getLong(1),
              row.getBoolean(2),
              Option(row.getString(3)),
              complete,
              Option(row.getString(5)),
              Option.unless(row.wasNull())(epoch)
            )
          }
          .toList
      }
    }
    RegistryView(registration.map(_._1), registration.flatMap(_._2), entries)
  }

  /** The epoch in force under each of `keys` that has one (ADR-0019). */
  def epochsInForce(connection: Connection, keys: Seq[String]): Map[String, Long] =
    Using.resource(connection.prepareStatement(EpochsInForce)) { statement =>
      statement.setArray(1, connection.createArrayOf("text", keys.toArray[AnyRef]))
      Using.resource(statement.executeQuery()) { rows =>
        Iterator
          .continually(rows)
          .takeWhile(_.next())
          .map(row => row.getString(1) -> row.getLong(2))
          .toMap
      }
    }
}
