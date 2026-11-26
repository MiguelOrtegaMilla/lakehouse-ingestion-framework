package lakehouse.ingestion.engine.control

import java.security.GeneralSecurityException
import java.sql.{SQLException, SQLTransientException}
import javax.net.ssl.SSLException

/**
 * Why a database could not be worked with, as far as what to do next goes (discovery §14).
 *
 * It is how a failure of a database is classified wherever one is met: by a process that is
 * starting, to tell a database that is not there, which running again may find, from one that
 * answered and said no, which it will not; and where a run ends (`Failures`, ADR-0022 §2).
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
   * running too long (57014), and a transaction that lost to another, by a conflict (40001) or
   * because the database broke a deadlock by ending it (40P01).
   *
   * And a server that is busy, which says so in a state of its own and not as a connection
   * that failed: one that has no connection left to give, or nothing left to work with (class
   * 53: too many connections, of the server or of the role, out of memory, a disk that is
   * full), and a lock that was not given in the time that the database or the role lets a
   * statement wait for one (55P03, `lock_timeout`), as while a table is being altered.
   * Nothing of the configuration is wrong in either, and nobody has to change anything for
   * the next run to find the database free.
   */
  private def passes(state: String): Boolean =
    state.startsWith("08") || state.startsWith("57P") || state == "57014" ||
      state == "40001" || state == "40P01" || state.startsWith("53") || state == "55P03"

  /**
   * Whether running again may do, without anybody having changed anything.
   *
   * A TLS session that could not be agreed is reported by the driver as a connection that
   * failed. It is no such thing: a certificate that this root did not sign, or that is another
   * host's, is as wrong the next time. So what the exception was caused by is looked at first.
   *
   * Then the database's own word, which is the SQLState of the innermost failure that has one.
   * A type that says that a failure is transient is the word of whatever wrapped it, and is
   * taken only when no state is known: a pool that waits for a connection reports every
   * attempt that failed as a connection that did not come in time
   * (`SQLTransientConnectionException`), a password that the database refused among them.
   */
  def mayPass(e: Throwable): Boolean = {
    val causes = Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(16).toVector
    val trust = causes.exists {
      case _: SSLException | _: GeneralSecurityException => true
      case sql: SQLException => Option(sql.getMessage).exists(_.contains(HostNotVerified))
      case _ => false
    }
    val state = causes.reverseIterator.collectFirst {
      case sql: SQLException if Option(sql.getSQLState).exists(_.nonEmpty) => sql.getSQLState
    }
    !trust && state.fold(causes.exists(_.isInstanceOf[SQLTransientException]))(passes)
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

  /**
   * What the database itself said, if it said anything, and its SQLState: the innermost failure
   * that has a state, as `mayPass` decides by it, or else the first failure of SQL. A pool that
   * timed out says only that it did, around what the database said to its attempt.
   */
  def said(e: Throwable): String = {
    val causes = Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(16).toVector
    val sqls = causes.collect { case sql: SQLException => sql }
    val text = sqls.reverseIterator
      .find(sql => Option(sql.getSQLState).exists(_.nonEmpty))
      .orElse(sqls.headOption)
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
