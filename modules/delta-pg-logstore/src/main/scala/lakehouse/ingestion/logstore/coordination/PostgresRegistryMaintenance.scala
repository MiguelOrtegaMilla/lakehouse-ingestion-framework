package lakehouse.ingestion.logstore.coordination

import java.io.IOException
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
 * '''A change writes its row of the maintenance log''' (`delta_coordination.maintenance_log`) in
 * its own transaction, once it has compared the entries and before it removes anything. What the
 * row says the change found is read by the database from the rows themselves, and not from what
 * this process believes of them.
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

  override def adopt(
      table: TableUri,
      version: Long,
      deltaTableId: Option[String],
      request: MaintenanceRequest
  ): Boolean = {
    require(version >= 0, s"a table is adopted at a version it has, not at $version")
    database.transaction(s"adopt $table") { connection =>
      val adopted = update(connection, Adopt) { statement =>
        statement.setString(1, table.value)
        statement.setLong(2, version)
        setId(statement, 3, deltaTableId)
      } == 1
      if (adopted) record(connection, LogAdoption, request, table)(_ => ())
      adopted
    }
  }

  override def readopt(
      table: TableUri,
      version: Long,
      deltaTableId: Option[String],
      examined: Seq[CommitEntry],
      request: MaintenanceRequest
  ): RegistryChange[Readoption] = {
    require(version >= 0, s"a table is adopted at a version it has, not at $version")
    database.transactionUnless[RegistryChange[Readoption]](s"adopt $table again") { connection =>
      // First: it takes the registration's row, with the version it is registered at.
      query(connection, LockRegistration)(_.setString(1, table.value))(_.getLong(1)) match {
        case None => Right(RegistryChange.NotRegistered)
        case Some(before) =>
          // The table gets its new generation.
          update(connection, MoveRegistration) { statement =>
            statement.setLong(1, version)
            setId(statement, 2, deltaTableId)
            statement.setString(3, table.value)
          }: Unit
          val now = readContradicted(connection, table, version)
          if (now.toSet != examined.toSet) Left(RegistryChange.EntriesChanged(now))
          else {
            // Before the taint and the entries go: the row says what they were.
            record(connection, LogReadoption, request, table)(_.setLong(4, before))
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

  override def forget(
      table: TableUri,
      examined: Seq[CommitEntry],
      request: MaintenanceRequest
  ): RegistryChange[Unit] =
    database.transactionUnless[RegistryChange[Unit]](s"forget $table") { connection =>
      // First: it takes the registration's row, which the last statement deletes.
      val registered =
        query(connection, LockRegistration)(_.setString(1, table.value))(_.getLong(1)).isDefined
      if (!registered) Right(RegistryChange.NotRegistered)
      else {
        val now = readUnfinished(connection, table)
        if (now.toSet != examined.toSet) Left(RegistryChange.EntriesChanged(now))
        else {
          // Before anything goes: the row says what was there.
          record(connection, LogReset, request, table)(_ => ())
          // The registration last: the taint and the entries refer to it.
          Seq(ForgetTaint, ForgetEntries, ForgetRegistration).foreach {
            update(connection, _)(_.setString(1, table.value))
          }
          Right(RegistryChange.Applied(()))
        }
      }
    }

  // The privilege that a row of the log takes, column by column, as V5 grants it: and nothing
  // is written to find out.
  override def mayRecord(): Boolean =
    database.transaction("ask whether the maintenance log may be written") { connection =>
      query(connection, MayRecord)(_ => ())(_.getBoolean(1)).contains(true)
    }

  override def collected(
      table: TableUri,
      request: MaintenanceRequest,
      temps: Seq[(String, String)]
  ): Unit =
    database.transaction(s"record the temporary files of $table that were collected") {
      connection =>
        record(connection, LogCollection, request, table) { statement =>
          statement.setArray(4, connection.createArrayOf("text", temps.map(_._1).toArray[AnyRef]))
          statement.setArray(5, connection.createArrayOf("text", temps.map(_._2).toArray[AnyRef]))
        }
    }

  /**
   * Writes the row of the log of an operation on `table`: each statement of the log takes who
   * asked, why and the table first; `bind` sets what it takes after them, from 4.
   */
  private def record(
      connection: Connection,
      statement: String,
      request: MaintenanceRequest,
      table: TableUri
  )(bind: PreparedStatement => Unit): Unit = {
    val written = update(connection, statement) { prepared =>
      prepared.setString(1, request.requestedBy)
      prepared.setString(2, request.reason)
      prepared.setString(3, table.value)
      bind(prepared)
    }
    if (written != 1)
      throw new IOException(s"the maintenance log has no row for what was done to $table")
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
    """SELECT registered_version FROM delta_coordination.coordinated_table
      |WHERE table_uri = ? FOR UPDATE""".stripMargin

  private val ClearTaint =
    "DELETE FROM delta_coordination.table_taint WHERE table_uri = ? RETURNING reason"

  private val RemoveContradicted =
    """DELETE FROM delta_coordination.commit_entry
      |WHERE table_uri = ? AND (version > ? OR NOT complete)""".stripMargin

  private val ForgetTaint = "DELETE FROM delta_coordination.table_taint WHERE table_uri = ?"
  private val ForgetEntries = "DELETE FROM delta_coordination.commit_entry WHERE table_uri = ?"

  private val ForgetRegistration =
    "DELETE FROM delta_coordination.coordinated_table WHERE table_uri = ?"

  // The rows of the maintenance log (V5). Each takes who asked, why and the table first (`asked`),
  // and says what it found as the database reads it from the rows of the table, in the
  // transaction of the change, before anything of them is removed.
  private val EntryAsJson =
    """jsonb_build_object('version', e.version, 'temp_path', e.temp_path,
      |  'complete', e.complete, 'writer_instance', e.writer_instance, 'run_id', e.run_id)""".stripMargin

  private val TaintOf =
    """(SELECT t.reason FROM delta_coordination.table_taint t WHERE t.table_uri = c.table_uri)"""

  private val Logged =
    """INSERT INTO delta_coordination.maintenance_log
      |  (table_uri, operation, requested_by, reason, detail)""".stripMargin

  private val LogAdoption =
    s"""WITH asked (requested_by, reason, table_uri) AS (VALUES (?::text, ?::text, ?::text))
       |$Logged
       |SELECT c.table_uri, 'adopt', a.requested_by, a.reason,
       |  jsonb_build_object('version', c.registered_version, 'delta_table_id', c.delta_table_id)
       |FROM asked a JOIN delta_coordination.coordinated_table c ON c.table_uri = a.table_uri""".stripMargin

  // After the registration moved, before the taint and the entries go: the entries are those
  // that RemoveContradicted removes.
  private val LogReadoption =
    s"""WITH asked (requested_by, reason, table_uri, version_before) AS
       |  (VALUES (?::text, ?::text, ?::text, ?::bigint))
       |$Logged
       |SELECT c.table_uri, 'readopt', a.requested_by, a.reason, jsonb_build_object(
       |  'version_before', a.version_before,
       |  'version', c.registered_version,
       |  'delta_table_id', c.delta_table_id,
       |  'cleared_taint', $TaintOf,
       |  'removed_entries', (
       |    SELECT COALESCE(jsonb_agg($EntryAsJson ORDER BY e.version), '[]'::jsonb)
       |    FROM delta_coordination.commit_entry e
       |    WHERE e.table_uri = c.table_uri
       |      AND (e.version > c.registered_version OR NOT e.complete)))
       |FROM asked a JOIN delta_coordination.coordinated_table c ON c.table_uri = a.table_uri""".stripMargin

  // Every entry goes; the complete ones are counted, and the unfinished ones said.
  private val LogReset =
    s"""WITH asked (requested_by, reason, table_uri) AS (VALUES (?::text, ?::text, ?::text))
       |$Logged
       |SELECT c.table_uri, 'reset', a.requested_by, a.reason, jsonb_build_object(
       |  'version', c.registered_version,
       |  'delta_table_id', c.delta_table_id,
       |  'cleared_taint', $TaintOf,
       |  'entries', (
       |    SELECT count(*) FROM delta_coordination.commit_entry e WHERE e.table_uri = c.table_uri),
       |  'unfinished_entries', (
       |    SELECT COALESCE(jsonb_agg($EntryAsJson ORDER BY e.version), '[]'::jsonb)
       |    FROM delta_coordination.commit_entry e
       |    WHERE e.table_uri = c.table_uri AND NOT e.complete))
       |FROM asked a JOIN delta_coordination.coordinated_table c ON c.table_uri = a.table_uri""".stripMargin

  private val MayRecord =
    """SELECT bool_and(has_column_privilege('delta_coordination.maintenance_log', c, 'INSERT'))
      |FROM unnest(ARRAY['table_uri', 'operation', 'requested_by', 'reason', 'detail']) AS c""".stripMargin

  // Each file by its name, with its fate.
  private val LogCollection =
    s"""WITH asked (requested_by, reason, table_uri, names, fates) AS
       |  (VALUES (?::text, ?::text, ?::text, ?::text[], ?::text[]))
       |$Logged
       |SELECT a.table_uri, 'collect_temps', a.requested_by, a.reason,
       |  jsonb_build_object('temps', jsonb_object(a.names, a.fates))
       |FROM asked a""".stripMargin
}
