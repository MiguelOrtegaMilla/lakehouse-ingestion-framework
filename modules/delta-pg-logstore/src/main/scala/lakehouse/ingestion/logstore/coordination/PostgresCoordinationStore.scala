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
 * '''Every operation but one is one statement, or an insert that does nothing on conflict
 * followed by a read.''' Each runs in its own transaction, at PostgreSQL's default
 * `READ COMMITTED`. That is enough: an `INSERT … ON CONFLICT DO NOTHING` waits for a concurrent
 * insert of the same key to commit or roll back before it answers, so the primary key
 * linearises the claims, and nothing on this path deletes the row that the following read looks
 * for. No operation needs two rows to change together. Registering a table and taking its first
 * version are two operations: a registered table without entries is a harmless state, and the
 * foreign key rules out the opposite order.
 *
 * '''A claim that names an epoch is the one: a transaction of two statements''' (ADR-0019). It
 * locks the row of its key, shared, reads the epoch in force there, and inserts its entry only
 * if that epoch is its own. An announcement updates that row, so the two wait for each other:
 * an announcement returns once every claim that read the epoch before it has committed its
 * entry or given up, and a claim that waited for an announcement reads what it announced.
 * Claims of one key do not wait for each other. A claim that names no epoch locks nothing, and
 * is the one statement that it was.
 *
 * '''A claim and an operator's change exclude each other in the database.''' An entry refers to
 * its table's registration by generation. The foreign key check of the `INSERT` waits for a
 * change of that registration that is in flight, and fails if the change commits: no statement
 * here locks a registration, and the role of the commit path could not.
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
      val generation = query(connection, Register) { statement =>
        statement.setString(1, table.value)
        statement.setLong(2, registeredVersion)
        deltaTableId match {
          case Some(id) => statement.setString(3, id)
          case None => statement.setNull(3, Types.VARCHAR)
        }
      }(_.getLong("generation"))
      generation match {
        case Some(assigned) =>
          RegisterResult.Registered(
            Registration(table, registeredVersion, deltaTableId, taintReason = None, assigned)
          )
        case None =>
          RegisterResult.AlreadyRegistered(readRegistration(connection, table).getOrElse {
            // Only maintenance deletes registrations, and not while writers are running.
            throw new IOException(s"the registration of $table was deleted while it was being read")
          })
      }
    }
  }

  override def entry(table: TableUri, version: Long): Option[CommitEntry] =
    run(s"read entry $version of $table")(readEntry(_, table, version))

  override def latestEntry(table: TableUri): Option[CommitEntry] =
    run(s"read the latest entry of $table") { connection =>
      query(connection, LatestEntry)(_.setString(1, table.value))(toEntry(table, _))
    }

  override def take(claim: Claim): TakeResult = {
    val action = s"take version ${claim.version} of ${claim.table}"
    claim.epoch match {
      case None => run(action)(connection => settled(connection, claim, insert(connection, claim)))

      case Some(epoch) =>
        // The lock on the epoch and the INSERT are one transaction. An announcement of a later
        // epoch waits for it to end, and finds the entry there; one that got to the row first
        // is waited for, and what it announced is what the lock reads.
        val attempt = transactionUnless[Attempt](action) { connection =>
          val inForce = query(connection, LockEpoch)(_.setString(1, epoch.key))(_.getLong(1))
          if (!inForce.contains(epoch.epoch)) Left(OutOfTurn(inForce))
          else
            insert(connection, claim) match {
              // The foreign key ended the transaction: there is nothing to keep.
              case None => Left(Inserted(None))
              case rows => Right(Inserted(rows))
            }
        }
        attempt match {
          case Inserted(rows) => run(action)(settled(_, claim, rows))
          // Unless the version is this claim's already: an earlier call took it while its
          // epoch was in force, and its reply was lost. The commit exists, and its writer is
          // told so, whatever was announced since.
          case OutOfTurn(inForce) =>
            val owned = run(action)(readEntry(_, claim.table, claim.version))
              .exists(_.tempPath == claim.tempPath)
            if (owned) TakeResult.Taken else TakeResult.EpochNotInForce(inForce)
        }
    }
  }

  override def announce(epoch: WriterEpoch): Long =
    run(s"announce $epoch") { connection =>
      val announced = query(connection, Announce) { statement =>
        statement.setString(1, epoch.key)
        statement.setLong(2, epoch.epoch)
      }(_.getLong(1))
      // No row came back: the epoch in force is this one or a later one, and stays.
      announced
        .orElse(query(connection, ReadEpoch)(_.setString(1, epoch.key))(_.getLong(1)))
        .getOrElse {
          // Only maintenance deletes an epoch, and not while its writers are running.
          throw new IOException(s"the epoch of ${epoch.key} was deleted while it was announced")
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

  /**
   * Runs `body` in one transaction of its own: all of what it changes, or none. For the
   * operations of this package that change more than one row at a time, which the commit path
   * never does.
   */
  private[coordination] def transaction[T](action: String)(body: Connection => T): T =
    transactionUnless(action)(connection => Right(body(connection)))

  /**
   * As `transaction`, for a change that `body` may call off once it has looked: `Right` keeps
   * what it changed, `Left` takes all of it back.
   */
  private[coordination] def transactionUnless[T](
      action: String
  )(body: Connection => Either[T, T]): T =
    run(action) { connection =>
      connection.setAutoCommit(false)
      try {
        val outcome = body(connection)
        if (outcome.isRight) connection.commit() else connection.rollback()
        outcome.merge
      } catch {
        case failure: Throwable =>
          try connection.rollback()
          catch { case rollback: SQLException => failure.addSuppressed(rollback) }
          throw failure
      } finally connection.setAutoCommit(true)
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

  /**
   * The INSERT that takes a version.
   *
   * @return how many rows it inserted, one or none; nothing at all, if the database refused it
   *   by the foreign key to the registration. It waited for an operator's change of the
   *   registration, if one was in flight
   */
  private def insert(connection: Connection, claim: Claim): Option[Int] =
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
        statement.setLong(6, claim.generation)
        claim.epoch match {
          case Some(epoch) =>
            statement.setString(7, epoch.key)
            statement.setLong(8, epoch.epoch)
          case None =>
            statement.setNull(7, Types.VARCHAR)
            statement.setNull(8, Types.BIGINT)
        }
      })
    catch { case e: SQLException if e.getSQLState == ForeignKeyViolation => None }

  /** What an INSERT that took a version or did not means for its claim. */
  private def settled(connection: Connection, claim: Claim, inserted: Option[Int]): TakeResult =
    inserted match {
      // No registration of that generation. Which of the two it is only words the refusal.
      case None =>
        if (readRegistration(connection, claim.table).isEmpty) TakeResult.NotRegistered
        else TakeResult.RegistrationChanged
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

  private def readRegistration(connection: Connection, table: TableUri): Option[Registration] =
    query(connection, ReadRegistration)(_.setString(1, table.value)) { row =>
      Registration(
        table,
        registeredVersion = row.getLong("registered_version"),
        deltaTableId = Option(row.getString("delta_table_id")),
        taintReason = Option(row.getString("taint_reason")),
        generation = row.getLong("generation")
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

  /** The version of schema `delta_coordination` this code works with: its migrations up to V4. */
  val SchemaVersion = 4

  /** How far a claim that names an epoch got, in its transaction. */
  sealed private trait Attempt

  /** @param rows what the INSERT said: `insert` */
  final private case class Inserted(rows: Option[Int]) extends Attempt

  /** The epoch of the claim is not the one in force, and nothing was inserted. */
  final private case class OutOfTurn(inForce: Option[Long]) extends Attempt

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
    """SELECT c.registered_version, c.delta_table_id, c.generation, t.reason AS taint_reason
      |FROM delta_coordination.coordinated_table c
      |LEFT JOIN delta_coordination.table_taint t ON t.table_uri = c.table_uri
      |WHERE c.table_uri = ?""".stripMargin

  private val Register =
    """INSERT INTO delta_coordination.coordinated_table (table_uri, registered_version, delta_table_id)
      |VALUES (?, ?, ?)
      |ON CONFLICT DO NOTHING
      |RETURNING generation""".stripMargin

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
      |  (table_uri, version, temp_path, writer_instance, run_id, generation, epoch_key, epoch)
      |VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      |ON CONFLICT DO NOTHING""".stripMargin

  // Shared: claims of one key do not wait for each other, and an announcement, which updates
  // the row, waits for every one of them and is waited for by the ones that come after it.
  private val LockEpoch =
    "SELECT epoch FROM delta_coordination.writer_epoch WHERE epoch_key = ? FOR SHARE"

  private val ReadEpoch = "SELECT epoch FROM delta_coordination.writer_epoch WHERE epoch_key = ?"

  // The row is locked whether or not it is updated, so an announcement that changes nothing
  // has also waited for the claims that were in flight. The time is the database's.
  private val Announce =
    """INSERT INTO delta_coordination.writer_epoch AS held (epoch_key, epoch)
      |VALUES (?, ?)
      |ON CONFLICT (epoch_key) DO UPDATE
      |   SET epoch = EXCLUDED.epoch, announced_at = now()
      | WHERE held.epoch < EXCLUDED.epoch
      |RETURNING epoch""".stripMargin

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

  private[coordination] def update(connection: Connection, sql: String)(
      bind: PreparedStatement => Unit
  ): Int =
    Using.resource(connection.prepareStatement(sql)) { statement =>
      bind(statement)
      statement.executeUpdate()
    }

  /** Every row of a query. */
  private[coordination] def queryAll[T](connection: Connection, sql: String)(
      bind: PreparedStatement => Unit
  )(read: ResultSet => T): Seq[T] =
    Using.resource(connection.prepareStatement(sql)) { statement =>
      bind(statement)
      Using.resource(statement.executeQuery()) { rows =>
        Iterator.continually(rows).takeWhile(_.next()).map(read).toList
      }
    }

  /** The first row of a query, if it has one. */
  private[coordination] def query[T](connection: Connection, sql: String)(
      bind: PreparedStatement => Unit
  )(read: ResultSet => T): Option[T] =
    Using.resource(connection.prepareStatement(sql)) { statement =>
      bind(statement)
      Using.resource(statement.executeQuery())(rows => Option.when(rows.next())(read(rows)))
    }

  private[coordination] def toEntry(table: TableUri, row: ResultSet): CommitEntry =
    CommitEntry(
      table,
      version = row.getLong("version"),
      tempPath = row.getString("temp_path"),
      complete = row.getBoolean("complete"),
      writerInstance = row.getString("writer_instance"),
      runId = Option(row.getObject("run_id", classOf[UUID]))
    )
}
