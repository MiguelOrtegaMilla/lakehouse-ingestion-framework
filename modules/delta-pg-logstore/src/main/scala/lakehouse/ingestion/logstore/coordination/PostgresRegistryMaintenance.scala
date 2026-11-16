package lakehouse.ingestion.logstore.coordination

import java.sql.{PreparedStatement, Types}

/**
 * `RegistryMaintenance` in PostgreSQL, as the role that runs an operator's tools
 * (`ingest_maintenance`, ADR-0013 §3). The role of the commit path cannot do any of this: the
 * database refuses it.
 *
 * It connects the way the store does: one lazy pool, the password by reference, bounded
 * timeouts, the schema version checked first. Each change is one transaction.
 *
 * @param settings how to reach the coordination database as the maintenance role
 */
final class PostgresRegistryMaintenance(
    settings: CoordinationSettings,
    environment: String => Option[String] = sys.env.get
) extends RegistryMaintenance
    with AutoCloseable {

  import PostgresCoordinationStore.{query, queryAll, toEntry, update}
  import PostgresRegistryMaintenance._

  private val database = new PostgresCoordinationStore(settings, environment)

  override def registration(table: TableUri): Option[Registration] = database.registration(table)

  override def entry(table: TableUri, version: Long): Option[CommitEntry] =
    database.entry(table, version)

  override def unfinished(table: TableUri): Seq[CommitEntry] =
    database.transaction(s"read the unfinished entries of $table") { connection =>
      queryAll(connection, Unfinished)(_.setString(1, table.value))(toEntry(table, _))
    }

  override def adopt(table: TableUri, version: Long, deltaTableId: Option[String]): Boolean = {
    require(version >= 0, s"a table is adopted at a version it has, not at $version")
    database.transaction(s"adopt $table") { connection =>
      update(connection, Adopt) { statement =>
        statement.setString(1, table.value)
        statement.setLong(2, version)
        setId(statement, 3, deltaTableId)
      } == 1
    }
  }

  override def readopt(
      table: TableUri,
      version: Long,
      deltaTableId: Option[String]
  ): Option[Readoption] = {
    require(version >= 0, s"a table is adopted at a version it has, not at $version")
    database.transaction(s"adopt $table again") { connection =>
      val moved = update(connection, MoveRegistration) { statement =>
        statement.setLong(1, version)
        setId(statement, 2, deltaTableId)
        statement.setString(3, table.value)
      }
      Option.when(moved == 1) {
        val taint = query(connection, ClearTaint)(_.setString(1, table.value))(_.getString(1))
        val removed = queryAll(connection, RemoveContradicted) { statement =>
          statement.setString(1, table.value)
          statement.setLong(2, version)
        }(toEntry(table, _))
        Readoption(taint, removed.sortBy(_.version))
      }
    }
  }

  override def forget(table: TableUri): Boolean =
    database.transaction(s"forget $table") { connection =>
      // The registration last: the taint and the entries refer to it.
      Seq(ForgetTaint, ForgetEntries).foreach(update(connection, _)(_.setString(1, table.value)))
      update(connection, ForgetRegistration)(_.setString(1, table.value)) == 1
    }

  override def close(): Unit = database.close()

  private def setId(statement: PreparedStatement, index: Int, id: Option[String]): Unit =
    id match {
      case Some(value) => statement.setString(index, value)
      case None => statement.setNull(index, Types.VARCHAR)
    }
}

object PostgresRegistryMaintenance {

  // SQL text is constant: values go in as bind parameters only (ADR-0013 §1).
  private val Columns = "version, temp_path, complete, writer_instance, run_id"

  private val Unfinished =
    s"""SELECT $Columns FROM delta_coordination.commit_entry
       |WHERE table_uri = ? AND NOT complete ORDER BY version""".stripMargin

  private val Adopt =
    """INSERT INTO delta_coordination.coordinated_table (table_uri, registered_version, delta_table_id)
      |VALUES (?, ?, ?)
      |ON CONFLICT DO NOTHING""".stripMargin

  // The id that is recorded stays unless another is given.
  private val MoveRegistration =
    """UPDATE delta_coordination.coordinated_table
      |SET registered_version = ?, delta_table_id = COALESCE(?, delta_table_id), registered_at = now()
      |WHERE table_uri = ?""".stripMargin

  private val ClearTaint =
    "DELETE FROM delta_coordination.table_taint WHERE table_uri = ? RETURNING reason"

  private val RemoveContradicted =
    s"""DELETE FROM delta_coordination.commit_entry
       |WHERE table_uri = ? AND (version > ? OR NOT complete)
       |RETURNING $Columns""".stripMargin

  private val ForgetTaint = "DELETE FROM delta_coordination.table_taint WHERE table_uri = ?"
  private val ForgetEntries = "DELETE FROM delta_coordination.commit_entry WHERE table_uri = ?"

  private val ForgetRegistration =
    "DELETE FROM delta_coordination.coordinated_table WHERE table_uri = ?"
}
