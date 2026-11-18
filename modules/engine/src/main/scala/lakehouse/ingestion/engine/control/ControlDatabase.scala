package lakehouse.ingestion.engine.control

import java.sql.{Connection, SQLException}

import scala.util.Using
import scala.util.control.NonFatal

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import lakehouse.ingestion.core.config.{DatabaseConnection, SecretResolver}

/**
 * The connection of the engine to its control plane (ADR-0003): a small pool to the database
 * of the profile, as the role of the profile, with the TLS and the timeouts of the profile.
 *
 * It is opened once, when a process starts, and that is where everything that could be wrong
 * with it is found: a password that is not where the profile says, a database that is not
 * there, a server that is not the one the certificate names, a role that is refused, a schema
 * that is not at the version of this application. Nothing has been read or written by then.
 *
 * It runs no DDL and has no way to: the schema is the migrations', applied by another role
 * (the command `migrate`).
 */
final class ControlDatabase private (pool: HikariDataSource) extends AutoCloseable {

  /** Runs `body` on a connection where each statement is committed by itself. */
  @throws[SQLException]
  def run[T](body: Connection => T): T = Using.resource(pool.getConnection)(body)

  /**
   * Runs `body` in one transaction: all that it changes is committed when it returns, and none
   * of it if it throws.
   */
  @throws[SQLException]
  def transaction[T](body: Connection => T): T =
    Using.resource(pool.getConnection) { connection =>
      connection.setAutoCommit(false)
      try {
        val result = body(connection)
        connection.commit()
        result
      } catch {
        case NonFatal(failure) =>
          try connection.rollback()
          catch { case NonFatal(also) => failure.addSuppressed(also) }
          throw failure
      } finally connection.setAutoCommit(true)
    }

  override def close(): Unit = pool.close()
}

object ControlDatabase {

  /**
   * A run holds one connection for a statement or a short transaction, and the heartbeat of its
   * lease another for a moment (P4.5): a process needs two, and no more.
   */
  val DefaultPoolSize = 2

  /** The connection pool does not accept a shorter wait. */
  private val MinConnectMillis = 250L

  /**
   * Opens the pool, connects, and checks the version of the schema.
   *
   * @param secrets where the password that the profile points at is read from
   * @return the open connection, or why there is none. No message has the password in it
   */
  def open(
      connection: DatabaseConnection,
      secrets: SecretResolver,
      poolSize: Int = DefaultPoolSize
  ): Either[DatabaseFailure, ControlDatabase] =
    secrets.resolve(connection.password) match {
      case Left(problem) => Left(DatabaseFailure.Refused(s"the control plane: $problem"))
      case Right(password) =>
        val config = new HikariConfig
        config.setPoolName("control-plane")
        config.setJdbcUrl(connection.url.value)
        config.setUsername(connection.user.value)
        config.setPassword(password.reveal)
        PostgresJdbc.properties(connection).foreach { case (name, value) =>
          config.addDataSourceProperty(name, value)
        }
        config.addDataSourceProperty("tcpKeepAlive", "true")
        config.setMaximumPoolSize(poolSize)
        config.setMinimumIdle(0)
        config.setAutoCommit(true)
        val connect = math.max(MinConnectMillis, connection.timeouts.connect.toMillis)
        config.setConnectionTimeout(connect)
        config.setValidationTimeout(connect)

        val where = s"the control plane (${connection.url.value}, as ${connection.user})"
        // The pool connects when it is made, and fails there if it cannot: a process that
        // starts without its control plane has nothing to do, and should say so at once.
        val opened =
          try Right(new HikariDataSource(config))
          catch {
            case NonFatal(e) =>
              Left(DatabaseFailure.of(s"$where could not be opened", e, DatabaseFailure.said(e)))
          }

        opened.flatMap { pool =>
          val verified =
            try
              Using
                .resource(pool.getConnection)(ControlSchema.verify)
                .left
                .map(problem => DatabaseFailure.Refused(s"$where: $problem"))
            catch {
              case NonFatal(e) =>
                Left(
                  DatabaseFailure.of(
                    s"$where could not be asked its version",
                    e,
                    DatabaseFailure.said(e)
                  )
                )
            }
          verified match {
            case Right(()) => Right(new ControlDatabase(pool))
            case Left(failure) =>
              pool.close()
              Left(failure)
          }
        }
    }
}
