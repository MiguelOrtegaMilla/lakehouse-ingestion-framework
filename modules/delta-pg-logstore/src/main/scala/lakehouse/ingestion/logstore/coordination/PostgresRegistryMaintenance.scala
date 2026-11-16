package lakehouse.ingestion.logstore.coordination

import java.sql.{Connection, PreparedStatement, Types}

/**
 * `RegistryMaintenance` in PostgreSQL, as the role that runs an operator's tools
 * (`ingest_maintenance`, ADR-0013 §3). The role of the commit path cannot do any of this: the
 * database refuses it.
 *
 * It connects the way the store does: one lazy pool, the password by reference, bounded
 * timeouts, the schema version checked first. Each change is one transaction.
 *
 * '''A change begins by taking the registration's row.''' Adopting a table again updates its
 * generation, and forgetting a table locks the row it is about to delete. The `INSERT` of a
 * writer that claims a version of the table refers to that row, so from that statement on it
 * waits: the entries cannot grow while the change compares them with the ones the operator
 * examined. When the change commits, the waiting `INSERT` is refused, and so is every later one
 * that names the generation the writer read before.
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
    database.transaction(s"read the unfinished entries of $table")(readUnfinished(_, table))

  override def contradicted(table: TableUri, version: Long): Seq[CommitEntry] =
    database.transaction(s"read the entries of $table that its log contradicts") {
      readContradicted(_, table, version)
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
      deltaTableId: Option[String],
      examined: Seq[CommitEntry]
  ): RegistryChange[Readoption] = {
    require(version >= 0, s"a table is adopted at a version it has, not at $version")
    database.transactionUnless[RegistryChange[Readoption]](s"adopt $table again") { connection =>
      // First: it takes the registration's row, and gives the table its new generation.
      val moved = update(connection, MoveRegistration) { statement =>
        statement.setLong(1, version)
        setId(statement, 2, deltaTableId)
        statement.setString(3, table.value)
      }
      if (moved == 0) Right(RegistryChange.NotRegistered)
      else {
        val now = readContradicted(connection, table, version)
        if (now.toSet != examined.toSet) Left(RegistryChange.EntriesChanged(now))
        else {
          val taint = query(connection, ClearTaint)(_.setString(1, table.value))(_.getString(1))
          update(connection, RemoveContradicted) { statement =>
            statement.setString(1, table.value)
            statement.setLong(2, version)
          }: Unit
          Right(RegistryChange.Applied(Readoption(taint, now)))
        }
      }
    }
  }

  override def forget(table: TableUri, examined: Seq[CommitEntry]): RegistryChange[Unit] =
    database.transactionUnless[RegistryChange[Unit]](s"forget $table") { connection =>
      // First: it takes the registration's row, which the last statement deletes.
      val registered =
        query(connection, LockRegistration)(_.setString(1, table.value))(_ => ()).isDefined
      if (!registered) Right(RegistryChange.NotRegistered)
      else {
        val now = readUnfinished(connection, table)
        if (now.toSet != examined.toSet) Left(RegistryChange.EntriesChanged(now))
        else {
          // The registration last: the taint and the entries refer to it.
          Seq(ForgetTaint, ForgetEntries, ForgetRegistration).foreach {
            update(connection, _)(_.setString(1, table.value))
          }
          Right(RegistryChange.Applied(()))
        }
      }
    }

  private def readUnfinished(connection: Connection, table: TableUri): Seq[CommitEntry] =
    queryAll(connection, Unfinished)(_.setString(1, table.value))(toEntry(table, _))

  private def readContradicted(
      connection: Connection,
      table: TableUri,
      version: Long
  ): Seq[CommitEntry] =
    queryAll(connection, Contradicted) { statement =>
      statement.setString(1, table.value)
      statement.setLong(2, version)
    }(toEntry(table, _))

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

  private val Contradicted =
    s"""SELECT $Columns FROM delta_coordination.commit_entry
       |WHERE table_uri = ? AND (version > ? OR NOT complete) ORDER BY version""".stripMargin

  // The id that is recorded stays unless another is given. The generation is the next one: the
  // entries that stay follow it (ON UPDATE CASCADE).
  private val MoveRegistration =
    """UPDATE delta_coordination.coordinated_table
      |SET registered_version = ?, delta_table_id = COALESCE(?, delta_table_id),
      |    registered_at = now(), generation = DEFAULT
      |WHERE table_uri = ?""".stripMargin

  private val LockRegistration =
    "SELECT 1 FROM delta_coordination.coordinated_table WHERE table_uri = ? FOR UPDATE"

  private val ClearTaint =
    "DELETE FROM delta_coordination.table_taint WHERE table_uri = ? RETURNING reason"

  private val RemoveContradicted =
    """DELETE FROM delta_coordination.commit_entry
      |WHERE table_uri = ? AND (version > ? OR NOT complete)""".stripMargin

  private val ForgetTaint = "DELETE FROM delta_coordination.table_taint WHERE table_uri = ?"
  private val ForgetEntries = "DELETE FROM delta_coordination.commit_entry WHERE table_uri = ?"

  private val ForgetRegistration =
    "DELETE FROM delta_coordination.coordinated_table WHERE table_uri = ?"
}
