package lakehouse.ingestion.it.config

import lakehouse.ingestion.core.config.{Profile, Timeouts}
import lakehouse.ingestion.logstore.RetryPolicy
import lakehouse.ingestion.logstore.coordination.CoordinationSettings
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * A profile is validated by `core`, which may not depend on the LogStore (ADR-0012), and one of
 * its rules is about how long the LogStore waits for its database: the retry budget of the
 * engine must be longer (P2b.5). `core` has the LogStore's numbers written down for that. This
 * holds them to the LogStore's own, so that a change there is a failure here and not a rule
 * that has quietly stopped meaning anything.
 */
final class LogStoreNumbersAgreementSpec extends AnyFunSuite with Matchers {

  test("core counts the attempts that the LogStore makes") {
    Profile.LogStoreAttempts shouldBe RetryPolicy.Default.attempts
  }

  test("the timeouts a profile leaves out are the LogStore's defaults") {
    Timeouts.Default.connect shouldBe CoordinationSettings.DefaultConnectTimeout
    Timeouts.Default.statement shouldBe CoordinationSettings.DefaultStatementTimeout
  }

  test("the shortest connect timeout of a profile is the shortest the LogStore takes") {
    val file = "conf/app/test.yaml"
    def profile(connect: String): String =
      s"""lake: { uri: "file:///data/lake", deployment: single-machine }
         |landing: { root: "file:///data/landing" }
         |databases:
         |  control:
         |    url: "jdbc:postgresql://localhost:5432/ingestion_control"
         |    user: ingest_control
         |    password: { secretRef: "env:INGEST_CONTROL_PASSWORD" }
         |    tls: { mode: disable }
         |  coordination:
         |    url: "jdbc:postgresql://localhost:5432/ingestion_control"
         |    user: ingest_coordination
         |    password: { secretRef: "env:INGEST_COORDINATION_PASSWORD" }
         |    tls: { mode: disable }
         |    timeouts: { connect: $connect }
         |spark: { master: "local[2]" }
         |""".stripMargin
    val shortest = CoordinationSettings.MinConnectTimeout.toMillis

    Profile.load(file, profile(s"${shortest}ms")).isRight shouldBe true
    Profile.load(file, profile(s"${shortest - 1}ms")).isLeft shouldBe true
  }
}
