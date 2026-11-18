package lakehouse.ingestion.engine.control

import scala.concurrent.duration._

import lakehouse.ingestion.core.config._
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** What the JDBC driver is told for a connection of a configuration (ADR-0013 §5). */
final class PostgresJdbcSpec extends AnyFunSuite with Matchers with EitherValues {

  private def connection(tls: Tls, timeouts: Timeouts = Timeouts.Default): DatabaseConnection =
    DatabaseConnection(
      PostgresUrl.parse("jdbc:postgresql://db.internal:5432/ingestion_control").value,
      SqlIdentifier.parse("ingest_control").value,
      SecretRef.Env("INGEST_CONTROL_PASSWORD"),
      tls,
      timeouts
    )

  test("TLS that is off is said to the driver, and not left to its default") {
    // The driver's own default is `prefer`: TLS if the server has it, and none without a word.
    PostgresJdbc.properties(connection(Tls.Disabled)) should contain("sslmode" -> "disable")
    PostgresJdbc.properties(connection(Tls.Disabled)).keySet should not contain "sslrootcert"
  }

  test("verify-full is said with the certificate it verifies against") {
    val properties = PostgresJdbc.properties(connection(Tls.VerifyFull("/etc/ssl/certs/ca.pem")))

    properties should contain allOf (
      "sslmode" -> "verify-full",
      "sslrootcert" -> "/etc/ssl/certs/ca.pem"
    )
  }

  test("the timeouts of the profile are those of the driver, in the units it takes") {
    val properties =
      PostgresJdbc.properties(connection(Tls.Disabled, Timeouts(5.seconds, 10.seconds)))

    properties should contain allOf (
      "connectTimeout" -> "5",
      "loginTimeout" -> "5",
      // The server cancels the statement, and the socket gives up a little later.
      "options" -> "-c statement_timeout=10000",
      "socketTimeout" -> "12",
      "ApplicationName" -> "lakehouse-ingestion"
    )
  }

  test("a timeout below a second is a second to the driver, for which zero is no timeout") {
    val properties =
      PostgresJdbc.properties(connection(Tls.Disabled, Timeouts(250.millis, 1500.millis)))

    properties should contain allOf (
      "connectTimeout" -> "1",
      "options" -> "-c statement_timeout=1500",
      "socketTimeout" -> "4"
    )
  }

  test("a connection for migrations has TLS and the wait to connect, and no limit on a statement") {
    val properties = PostgresJdbc.forMigrations(connection(Tls.VerifyFull("/etc/ssl/certs/ca.pem")))

    properties shouldBe Map(
      "sslmode" -> "verify-full",
      "sslrootcert" -> "/etc/ssl/certs/ca.pem",
      "connectTimeout" -> "5",
      "loginTimeout" -> "5",
      "ApplicationName" -> "lakehouse-ingestion"
    )
  }

  test("nothing of the password is in what the driver is told here") {
    PostgresJdbc.properties(connection(Tls.Disabled)).keySet should contain noneOf (
      "password",
      "user"
    )
  }
}
