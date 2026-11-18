package lakehouse.ingestion.engine.control

import java.security.GeneralSecurityException
import java.sql.{SQLException, SQLTransientException}
import javax.net.ssl.SSLException

/**
 * Why a database could not be worked with, as far as what to do next goes (discovery §14).
 *
 * The classification of errors is its own work, with retries and their budget (P8). This is the
 * least of it, and what a process that is starting needs: to tell a database that is not there,
 * which running again may find, from one that answered and said no, which it will not.
 */
sealed trait DatabaseFailure {
  def message: String
}

object DatabaseFailure {

  /** The database was not there to be asked: running again may do. */
  final case class Unreachable(message: String) extends DatabaseFailure

  /** The database said no, or is not the one that was expected: running again will not do. */
  final case class Refused(message: String) extends DatabaseFailure

  /**
   * SQLStates of what may pass (discovery §14): a connection that failed or was lost (class
   * 08), a server that is starting or stopping (57P), a statement that was cancelled for
   * running too long (57014), and a transaction that lost to another (40001).
   */
  private def passes(state: String): Boolean =
    state.startsWith("08") || state.startsWith("57P") || state == "57014" || state == "40001"

  /**
   * Whether running again may do, without anybody having changed anything.
   *
   * A TLS session that could not be agreed is reported by the driver as a connection that
   * failed. It is no such thing: a certificate that this root did not sign, or that is another
   * host's, is as wrong the next time. So what the exception was caused by is looked at first.
   */
  def mayPass(e: Throwable): Boolean = {
    val causes = Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(16).toVector
    val trust = causes.exists {
      case _: SSLException | _: GeneralSecurityException => true
      case sql: SQLException => Option(sql.getMessage).exists(_.contains(HostNotVerified))
      case _ => false
    }
    !trust && causes.exists {
      case _: SQLTransientException => true
      case sql: SQLException => Option(sql.getSQLState).exists(passes)
      case _ => false
    }
  }

  /**
   * What PostgreSQL's driver says of a certificate that is another host's, which it reports
   * with nothing else to tell it by. A test against a real server holds this to the driver.
   */
  private val HostNotVerified = "could not be verified by hostnameverifier"

  /**
   * @param what what was being done, to start the message with
   * @param said what the failure says for itself; never a secret, which no driver repeats
   */
  def of(what: String, e: Throwable, said: String): DatabaseFailure =
    if (mayPass(e)) Unreachable(s"$what: $said") else Refused(s"$what: $said")

  /** What the database itself said, if it said anything, and its SQLState. */
  def said(e: Throwable): String = {
    val causes = Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(16).toVector
    val text = causes
      .collectFirst { case sql: SQLException => sql }
      .map { sql =>
        val state = Option(sql.getSQLState).map(code => s" (SQLState $code)").getOrElse("")
        s"${firstLine(sql.getMessage)}$state"
      }
      .getOrElse(firstLine(e.getMessage))
    // The reason of a TLS failure is under the driver's own words.
    causes.collectFirst { case tls: SSLException => tls }.fold(text) { tls =>
      s"$text: ${firstLine(tls.getMessage)}"
    }
  }

  private def firstLine(message: String): String =
    Option(message).flatMap(_.linesIterator.map(_.trim).find(_.nonEmpty)).getOrElse("no message")
}
