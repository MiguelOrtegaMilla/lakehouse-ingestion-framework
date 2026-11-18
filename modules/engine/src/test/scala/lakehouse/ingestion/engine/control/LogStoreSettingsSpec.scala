package lakehouse.ingestion.engine.control

import java.net.{URI, URLDecoder}
import java.nio.charset.StandardCharsets.UTF_8

import scala.concurrent.duration._

import lakehouse.ingestion.core.config._
import lakehouse.ingestion.logstore.coordination.{
  CoordinationSettings,
  SecretRef => LogStoreSecretRef
}
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The coordination database of a profile, as the LogStore is told of it (ADR-0004 §3.9). */
final class LogStoreSettingsSpec extends AnyFunSuite with Matchers with EitherValues {

  private def coordination(tls: Tls, timeouts: Timeouts = Timeouts.Default): DatabaseConnection =
    DatabaseConnection(
      PostgresUrl.parse("jdbc:postgresql://db.internal:5432/ingestion_control").value,
      SqlIdentifier.parse("ingest_coordination").value,
      SecretRef.File("/run/secrets/coordination"),
      tls,
      timeouts
    )

  /** What the LogStore makes of the settings: its own reading of them, not this test's. */
  private def read(settings: Map[String, String]): CoordinationSettings =
    CoordinationSettings.from(settings.get _).value

  test("the LogStore takes the settings, and they are the connection of the profile") {
    val settings = read(
      LogStoreSettings.hadoop(coordination(Tls.Disabled, Timeouts(3.seconds, 7.seconds)))
    )

    settings.jdbcUrl shouldBe "jdbc:postgresql://db.internal:5432/ingestion_control?sslmode=disable"
    settings.user shouldBe "ingest_coordination"
    settings.connectTimeout shouldBe 3.seconds
    settings.statementTimeout shouldBe 7.seconds
    settings.poolMaxSize shouldBe CoordinationSettings.DefaultPoolMaxSize
  }

  test("the password travels as the reference of the profile, and never as itself") {
    val file = LogStoreSettings.hadoop(coordination(Tls.Disabled))
    file(CoordinationSettings.PasswordRef) shouldBe "file:/run/secrets/coordination"
    read(file).password shouldBe a[LogStoreSecretRef.File]

    val env = LogStoreSettings.hadoop(
      coordination(Tls.Disabled).copy(password = SecretRef.Env("INGEST_COORDINATION_PASSWORD"))
    )
    env(CoordinationSettings.PasswordRef) shouldBe "env:INGEST_COORDINATION_PASSWORD"
    read(env).password shouldBe LogStoreSecretRef.Env("INGEST_COORDINATION_PASSWORD")
    // Nothing among the settings is named as a password would be, but its reference.
    env.keys.filter(_.toLowerCase.contains("password")).toSeq shouldBe Seq(
      CoordinationSettings.PasswordRef
    )
  }

  test("the mode of TLS is always said: the driver's own default goes on without TLS") {
    read(LogStoreSettings.hadoop(coordination(Tls.Disabled))).jdbcUrl should endWith(
      "?sslmode=disable"
    )

    val verified =
      read(LogStoreSettings.hadoop(coordination(Tls.VerifyFull("/etc/ssl/certs/ca.pem"))))
    verified.jdbcUrl shouldBe
      "jdbc:postgresql://db.internal:5432/ingestion_control" +
      "?sslmode=verify-full&sslrootcert=%2Fetc%2Fssl%2Fcerts%2Fca.pem"
  }

  test("a path that has in it what a URL reads as its own reaches the driver whole") {
    val path = "/etc/ssl/our certs/ca&sslmode=disable#x.pem"
    val url = read(LogStoreSettings.hadoop(coordination(Tls.VerifyFull(path)))).jdbcUrl

    val parameters = new URI(url.stripPrefix("jdbc:")).getRawQuery.split("&").toSeq
    parameters.map(_.takeWhile(_ != '=')) shouldBe Seq("sslmode", "sslrootcert")
    parameters.head shouldBe "sslmode=verify-full"
    URLDecoder.decode(parameters(1).stripPrefix("sslrootcert="), UTF_8) shouldBe path
  }
}
