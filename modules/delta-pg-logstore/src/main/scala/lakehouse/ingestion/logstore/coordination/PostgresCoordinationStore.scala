package lakehouse.ingestion.logstore.coordination

import java.io.IOException
import java.sql.{Connection, PreparedStatement, ResultSet, SQLException, Types}
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration._
import scala.util.Using

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

/**
 * The coordination schema is missing, cannot be read, or is not the version this LogStore was
 * built for (ADR-0004, deviation 10). Nothing was read or written. It is a deployment error:
 * retrying does not help, applying the migrations does.
 */
final class CoordinationSchemaException(message: String, cause: Throwable = null)
    extends IOException(message, cause)

/**
 * The `CoordinationStore` in PostgreSQL, on schema `delta_coordination` (ADR-0004 §3.1).
 *
 * '''Every operation is one statement, or an insert that does nothing on conflict followed by a
 * read.''' Each runs in its own transaction, at PostgreSQL's default `READ COMMITTED`. That is
 * enough: an `INSERT … ON CONFLICT DO NOTHING` waits for a concurrent insert of the same key to
 * commit or roll back before it answers, so the primary key linearises the claims, and nothing
 * on this path deletes the row that the following read looks for. No operation needs two rows to
 * change together. Registering a table and taking its first version are two operations: a
 * registered table without entries is a harmless state, and the foreign key rules out the
 * opposite order.
 *
 * '''Fail closed.''' Every SQL error becomes an `IOException`. Waiting is bounded three times
 * over: for a connection, for a lock or a slow statement (by the server), and for a server or
 * a network that stops answering (by the socket).
 *
 * '''Nothing is opened until the first operation.''' Delta creates LogStores freely, and in
 * tasks that never touch a `_delta_log` (ADR-0004 §3.9). The first operation opens the pool,
 * resolves the password reference in this JVM, and checks the schema version.
 *
 * @param environment how the password reference looks up a variable
 */
final class PostgresCoordinationStore(
    settings: CoordinationSettings,
    environment: String => Option[String] = sys.env.get
) extends CoordinationStore
    with AutoCloseable {

  import PostgresCoordinationStore._

  // Not cached if opening fails, for instance on a secret that cannot be read yet: the next
  // operation tries again.
  private lazy val pool: HikariDataSource = openPool(settings, environment)
  @volatile private var poolOpened = false
  private val schemaVerified = new AtomicBoolean(false)

  override def registration(table: TableUri): Option[Registration] =
    run(s"read the registration of $table")(readRegistration(_, table))

  override def register(
      table: TableUri,
      registeredVersion: Long,
      deltaTableId: Option[String]
  ): RegisterResult = {
    require(
      registeredVersion >= Registration.NotYetCreated,
      s"not a registered version: $registeredVersion"
    )
    run(s"register $table") { connection =>
      val inserted = update(connection, Register) { statement =>
        statement.setString(1, table.value)
        statement.setLong(2, registeredVersion)
        deltaTableId match {
          case Some(id) => statement.setString(3, id)
          case None => statement.setNull(3, Types.VARCHAR)
        }
      }
      if (inserted == 1) RegisterResult.Registered
      else
        RegisterResult.AlreadyRegistered(readRegistration(connection, table).getOrElse {
          // Only maintenance deletes registrations, and not while writers are running.
          throw new IOException(s"the registration of $table was deleted while it was being read")
        })
    }
  }

  override def entry(table: TableUri, version: Long): Option[CommitEntry] =
    run(s"read entry $version of $table")(readEntry(_, table, version))

  override def latestEntry(table: TableUri): Option[CommitEntry] =
    run(s"read the latest entry of $table") { connection =>
      query(connection, LatestEntry)(_.setString(1, table.value))(toEntry(table, _))
    }

  override def take(claim: Claim): TakeResult =
    run(s"take version ${claim.version} of ${claim.table}") { connection =>
      val inserted =
        try
          Some(update(connection, Take) { statement =>
            statement.setString(1, claim.table.value)
            statement.setLong(2, claim.version)
            statement.setString(3, claim.tempPath)
            statement.setString(4, claim.writerInstance)
            claim.runId match {
              case Some(run) => statement.setObject(5, run)
              case None => statement.setNull(5, Types.OTHER)
            }
          })
        catch {
          // The foreign key to the registry: the database itself refuses the claim.
          case e: SQLException if e.getSQLState == ForeignKeyViolation => None
        }
      inserted match {
        case None => TakeResult.NotRegistered
        case Some(1) => TakeResult.Taken
        case Some(_) =>
          val owner = readEntry(connection, claim.table, claim.version).getOrElse {
            throw new IOException(
              s"entry ${claim.version} of ${claim.table} was deleted while it was being read"
            )
          }
          // An earlier call of this same claim, whose reply was lost, already took the version.
          if (owner.tempPath == claim.tempPath) TakeResult.Taken else TakeResult.TakenBy(owner)
      }
    }

  override def complete(table: TableUri, version: Long, tempPath: String): Boolean =
    run(s"complete entry $version of $table") { connection =>
      update(connection, Complete) { statement =>
        statement.setString(1, table.value)
        statement.setLong(2, version)
        statement.setString(3, tempPath)
      } == 1
    }

  override def taint(table: TableUri, reason: String): Unit =
    run(s"taint $table") { connection =>
      try
        update(connection, Taint) { statement =>
          statement.setString(1, table.value)
          statement.setString(2, reason)
        }: Unit
      catch {
        case e: SQLException if e.getSQLState == ForeignKeyViolation =>
          throw new IOException(s"cannot taint $table: it is not registered", e)
      }
    }

  /** Closes this store's pool. A store shared by the JVM is never closed; tests close theirs. */
  override def close(): Unit = if (poolOpened) pool.close()

  /** Runs `body` on a pooled connection; every failure leaves as an `IOException`. */
  private def run[T](action: String)(body: Connection => T): T =
    try
      Using.resource(connect()) { connection =>
        verifySchema(connection)
        body(connection)
      }
    catch {
      case e: IOException => throw e
      case e: SQLException =>
        throw new IOException(s"coordination store: cannot $action: ${e.getMessage}", e)
    }

  private def connect(): Connection = {
    val opened = pool
    poolOpened = true
    opened.getConnection
  }

  /** Once per store: nothing is read or written on a schema of another version. */
  private def verifySchema(connection: Connection): Unit =
    if (!schemaVerified.get) {
      val found =
        try query(connection, SchemaVersionQuery)(_ => ())(_.getInt(1))
        catch {
          case e: SQLException if SchemaNotReadable.contains(e.getSQLState) =>
            throw new CoordinationSchemaException(
              s"the coordination schema is missing or ${settings.user} cannot read it. " +
                "Apply the migrations of delta-pg-logstore, as the migrator role.",
              e
            )
        }
      if (!found.contains(SchemaVersion))
        throw new CoordinationSchemaException(
          s"the coordination schema is at version ${found.getOrElse("none")}, and this LogStore " +
            s"needs version $SchemaVersion. Apply the migrations of delta-pg-logstore."
        )
      schemaVerified.set(true)
    }

  private def readRegistration(connection: Connection, table: TableUri): Option[Registration] =
    query(connection, ReadRegistration)(_.setString(1, table.value)) { row =>
      Registration(
        table,
        registeredVersion = row.getLong("registered_version"),
        deltaTableId = Option(row.getString("delta_table_id")),
        taintReason = Option(row.getString("taint_reason"))
      )
    }

  private def readEntry(
      connection: Connection,
      table: TableUri,
      version: Long
  ): Option[CommitEntry] =
    query(connection, ReadEntry) { statement =>
      statement.setString(1, table.value)
      statement.setLong(2, version)
    }(toEntry(table, _))
}

object PostgresCoordinationStore {

  /** The version of schema `delta_coordination` this code works with: its migration V1. */
  val SchemaVersion = 1

  /** What a session of this store calls itself in `pg_stat_activity`. */
  val ApplicationName = "delta-pg-logstore"

  /** An unused connection is closed after this long: an idle JVM holds none. */
  private val IdleTimeout = 1.minute

  /**
   * How much longer than a statement the socket waits, so that the server's own timeout, which
   * reports what happened, fires first when the server is alive.
   */
  private val SocketGrace = 2.seconds

  private val ForeignKeyViolation = "23503"
  // undefined_table, invalid_schema_name, insufficient_privilege
  private val SchemaNotReadable = Set("42P01", "3F000", "42501")

  // SQL text is constant: values go in as bind parameters only (ADR-0013 §1).
  private val SchemaVersionQuery = "SELECT version FROM delta_coordination.schema_version"

  private val ReadRegistration =
    """SELECT c.registered_version, c.delta_table_id, t.reason AS taint_reason
      |FROM delta_coordination.coordinated_table c
      |LEFT JOIN delta_coordination.table_taint t ON t.table_uri = c.table_uri
      |WHERE c.table_uri = ?""".stripMargin

  private val Register =
    """INSERT INTO delta_coordination.coordinated_table (table_uri, registered_version, delta_table_id)
      |VALUES (?, ?, ?)
      |ON CONFLICT DO NOTHING""".stripMargin

  private val EntryColumns = "version, temp_path, complete, writer_instance, run_id"

  private val ReadEntry =
    s"""SELECT $EntryColumns FROM delta_coordination.commit_entry
       |WHERE table_uri = ? AND version = ?""".stripMargin

  // Served by the primary key, read backwards.
  private val LatestEntry =
    s"""SELECT $EntryColumns FROM delta_coordination.commit_entry
       |WHERE table_uri = ? ORDER BY version DESC LIMIT 1""".stripMargin

  private val Take =
    """INSERT INTO delta_coordination.commit_entry
      |  (table_uri, version, temp_path, writer_instance, run_id)
      |VALUES (?, ?, ?, ?, ?)
      |ON CONFLICT DO NOTHING""".stripMargin

  // The completion time is the database's, and the first one stays.
  private val Complete =
    """UPDATE delta_coordination.commit_entry
      |SET complete = true, completed_at = COALESCE(completed_at, now())
      |WHERE table_uri = ? AND version = ? AND temp_path = ?""".stripMargin

  private val Taint =
    """INSERT INTO delta_coordination.table_taint (table_uri, reason)
      |VALUES (?, ?)
      |ON CONFLICT DO NOTHING""".stripMargin

  private val shared = new ConcurrentHashMap[CoordinationSettings, PostgresCoordinationStore]

  /**
   * The store that every LogStore instance of this JVM with these settings uses, and so one
   * connection pool for all of them (ADR-0004 §3.9). Calling this opens nothing.
   */
  def shared(settings: CoordinationSettings): PostgresCoordinationStore =
    shared.computeIfAbsent(settings, new PostgresCoordinationStore(_))

  private def openPool(
      settings: CoordinationSettings,
      environment: String => Option[String]
  ): HikariDataSource = {
    val config = new HikariConfig
    config.setPoolName(ApplicationName)
    config.setJdbcUrl(settings.jdbcUrl)
    config.setUsername(settings.user)
    config.setPassword(settings.password.resolve(environment))
    config.setMaximumPoolSize(settings.poolMaxSize)
    config.setMinimumIdle(0)
    config.setIdleTimeout(IdleTimeout.toMillis)
    // The pool exists even while PostgreSQL is down, and works again when it is back.
    config.setInitializationFailTimeout(-1)
    config.setAutoCommit(true)

    val connect = settings.connectTimeout
    val statement = settings.statementTimeout.toMillis
    config.setConnectionTimeout(connect.toMillis)
    config.setValidationTimeout(connect.toMillis)
    config.addDataSourceProperty("connectTimeout", wholeSeconds(connect).toString)
    config.addDataSourceProperty("loginTimeout", wholeSeconds(connect).toString)
    // A statement that runs too long is cancelled by the server. That includes waiting for a
    // lock, such as another writer's uncommitted claim on the same version: a lock_timeout of
    // the same length would never fire first.
    config.setConnectionInitSql(s"SET statement_timeout = $statement")
    // A server or a network that stops answering is given up on by the socket.
    config.addDataSourceProperty(
      "socketTimeout",
      wholeSeconds(settings.statementTimeout + SocketGrace).toString
    )
    config.addDataSourceProperty("tcpKeepAlive", "true")
    config.addDataSourceProperty("ApplicationName", ApplicationName)
    new HikariDataSource(config)
  }

  /** The driver takes timeouts in whole seconds; zero would mean no timeout. */
  private def wholeSeconds(duration: FiniteDuration): Long =
    math.max(1L, math.ceil(duration.toMillis / 1000.0).toLong)

  private def update(connection: Connection, sql: String)(bind: PreparedStatement => Unit): Int =
    Using.resource(connection.prepareStatement(sql)) { statement =>
      bind(statement)
      statement.executeUpdate()
    }

  /** The first row of a query, if it has one. */
  private def query[T](connection: Connection, sql: String)(
      bind: PreparedStatement => Unit
  )(read: ResultSet => T): Option[T] =
    Using.resource(connection.prepareStatement(sql)) { statement =>
      bind(statement)
      Using.resource(statement.executeQuery())(rows => Option.when(rows.next())(read(rows)))
    }

  private def toEntry(table: TableUri, row: ResultSet): CommitEntry =
    CommitEntry(
      table,
      version = row.getLong("version"),
      tempPath = row.getString("temp_path"),
      complete = row.getBoolean("complete"),
      writerInstance = row.getString("writer_instance"),
      runId = Option(row.getObject("run_id", classOf[UUID]))
    )
}
