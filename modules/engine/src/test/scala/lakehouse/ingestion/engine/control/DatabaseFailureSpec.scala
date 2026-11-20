package lakehouse.ingestion.engine.control

import java.security.cert.CertificateException
import java.sql.{SQLException, SQLTransientConnectionException}
import javax.net.ssl.SSLHandshakeException

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** What may pass by running again, and what will not (discovery §14). */
final class DatabaseFailureSpec extends AnyFunSuite with Matchers {

  private def sql(state: String, cause: Throwable = null): SQLException =
    new SQLException("the driver's words", state, cause)

  test("a database that is not there, or is busy, may be there the next time") {
    val passing = Seq(
      "08001" -> "the connection could not be made",
      "08006" -> "the connection was lost",
      "57P01" -> "the server is being shut down",
      "57P03" -> "the server is starting",
      "57014" -> "the statement ran past its timeout",
      "40001" -> "the transaction lost to another",
      "40P01" -> "the database broke a deadlock by ending this transaction"
    )
    passing.foreach { case (state, what) =>
      withClue(s"$state, $what: ")(DatabaseFailure.mayPass(sql(state)) shouldBe true)
    }
    // The pool's own, when no connection came in time.
    DatabaseFailure.mayPass(
      new SQLTransientConnectionException("timed out after 5000ms")
    ) shouldBe true
  }

  test("a database that said no will say it again") {
    val staying = Seq(
      "28P01" -> "the password is wrong",
      "28000" -> "no way in for this role, or for a connection without TLS",
      "3D000" -> "the database does not exist",
      "42501" -> "the role may not",
      "42P01" -> "the table is not there",
      "23505" -> "a key is taken"
    )
    staying.foreach { case (state, what) =>
      withClue(s"$state, $what: ")(DatabaseFailure.mayPass(sql(state)) shouldBe false)
    }
    DatabaseFailure.mayPass(new IllegalStateException("not a failure of a database")) shouldBe false
    DatabaseFailure.mayPass(new SQLException("no state")) shouldBe false
  }

  test("a failure is told by what caused it, however it is wrapped") {
    val wrapped = new RuntimeException("Failed to initialize pool", sql("08001"))
    DatabaseFailure.mayPass(wrapped) shouldBe true
    DatabaseFailure.mayPass(
      new RuntimeException("Failed to initialize pool", sql("28P01"))
    ) shouldBe false
  }

  test("a pool that timed out is told by what the database said to its attempts") {
    // HikariCP: no connection in time, with the state of the last attempt and that attempt.
    def timedOut(attempt: SQLException): SQLTransientConnectionException =
      new SQLTransientConnectionException(
        "control-plane - Connection is not available, request timed out after 5000ms",
        attempt.getSQLState,
        attempt
      )

    DatabaseFailure.mayPass(timedOut(sql("08001"))) shouldBe true
    DatabaseFailure.mayPass(timedOut(sql("28P01"))) shouldBe false
    DatabaseFailure.mayPass(timedOut(sql("3D000"))) shouldBe false
    // As the LogStore hands it on: in an IOException of its own.
    DatabaseFailure.mayPass(
      new java.io.IOException("coordination store: cannot read", timedOut(sql("28P01")))
    ) shouldBe false
    // The innermost state is the database's own, whatever a wrapper says.
    DatabaseFailure.mayPass(sql("08006", sql("28P01"))) shouldBe false
  }

  test("a certificate that is not trusted is no connection that failed: it will not pass") {
    // The driver reports them all as 08006, a connection failure.
    val untrusted = sql("08006", new SSLHandshakeException("PKIX path building failed"))
    val unreadable = sql("08006", new CertificateException("Could not parse the root certificate"))
    val otherHost = new SQLException(
      "The hostname 127.0.0.1 could not be verified by hostnameverifier PgjdbcHostnameVerifier.",
      "08006"
    )

    Seq(untrusted, unreadable, otherHost).foreach { failure =>
      withClue(failure.getMessage)(DatabaseFailure.mayPass(failure) shouldBe false)
    }
    DatabaseFailure.of("the database", untrusted, "said") shouldBe DatabaseFailure.Refused(
      "the database: said"
    )
    DatabaseFailure.of("the database", sql("08001"), "said") shouldBe
      DatabaseFailure.Unreachable("the database: said")
  }

  test("what is said is the database's own first line, with its state, and the reason of TLS") {
    val refused = new RuntimeException(
      "Failed to initialize pool",
      new SQLException(
        "FATAL: password authentication failed for user \"x\"\n  Detail: none",
        "28P01"
      )
    )
    DatabaseFailure.said(refused) shouldBe
      "FATAL: password authentication failed for user \"x\" (SQLState 28P01)"

    DatabaseFailure.said(
      sql("08006", new SSLHandshakeException("PKIX path building failed"))
    ) shouldBe
      "the driver's words (SQLState 08006): PKIX path building failed"
    DatabaseFailure.said(new IllegalStateException("something else")) shouldBe "something else"
    DatabaseFailure.said(new IllegalStateException()) shouldBe "no message"
  }
}
