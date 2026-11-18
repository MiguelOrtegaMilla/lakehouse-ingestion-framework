package lakehouse.ingestion.engine.control

import lakehouse.ingestion.core.config.{DatabaseConnection, Tls}

/**
 * From a connection as a configuration says it to what PostgreSQL's JDBC driver is told
 * (ADR-0013 §5). One place for it, so that TLS and timeouts are the same for everything that
 * connects: the control plane, the migrations, and what the LogStore is given.
 *
 * A configuration has no parameters in its URL (`PostgresUrl`). Everything the driver is told
 * beyond the URL is here, by name, and nothing a file says reaches the driver as a parameter
 * that was not meant.
 */
object PostgresJdbc {

  /** What the database shows as the name of the application, in `pg_stat_activity`. */
  val ApplicationName = "lakehouse-ingestion"

  /** How long past the statement timeout the socket waits before it gives a server up. */
  private val SocketGraceSeconds = 2L

  /**
   * The properties of a connection that runs the statements of the application: TLS, and both
   * timeouts.
   *
   * A statement that runs too long is cancelled by the server (`statement_timeout`). A server or
   * a network that stops answering at all is given up on by the socket, a little later.
   */
  def properties(connection: DatabaseConnection): Map[String, String] =
    forMigrations(connection) ++ Map(
      "options" -> s"-c statement_timeout=${connection.timeouts.statement.toMillis}",
      "socketTimeout" -> (wholeSeconds(
        connection.timeouts.statement.toMillis
      ) + SocketGraceSeconds).toString
    )

  /**
   * The properties of a connection that applies migrations: TLS and the wait for a connection,
   * and no limit on a statement. A migration takes as long as it takes, and one that is cut
   * halfway is worse than one that is slow.
   */
  def forMigrations(connection: DatabaseConnection): Map[String, String] = {
    val connect = wholeSeconds(connection.timeouts.connect.toMillis).toString
    val tls = connection.tls match {
      // Said, not left to the driver, whose default is to try TLS and go on without it.
      case Tls.Disabled => Map("sslmode" -> "disable")
      case Tls.VerifyFull(rootCertificate) =>
        Map("sslmode" -> "verify-full", "sslrootcert" -> rootCertificate)
    }
    tls ++ Map(
      "connectTimeout" -> connect,
      "loginTimeout" -> connect,
      "ApplicationName" -> ApplicationName
    )
  }

  /** The driver takes timeouts in whole seconds, and zero would be no timeout. */
  private def wholeSeconds(millis: Long): Long = math.max(1L, math.ceil(millis / 1000.0).toLong)
}
