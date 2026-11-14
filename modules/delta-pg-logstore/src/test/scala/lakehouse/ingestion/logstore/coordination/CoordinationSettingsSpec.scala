package lakehouse.ingestion.logstore.coordination

import scala.concurrent.duration._

import lakehouse.ingestion.logstore.coordination.CoordinationSettings._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class CoordinationSettingsSpec extends AnyFunSuite with Matchers {

  private val required = Map(
    JdbcUrl -> "jdbc:postgresql://db.internal:5432/ingestion_control?sslmode=verify-full",
    User -> "ingest_coordination",
    PasswordRef -> "file:/run/secrets/ingest_coordination_password"
  )

  private def problems(settings: Map[String, String]): Seq[String] =
    CoordinationSettings.from(settings.get _).left.getOrElse(Nil)

  test("the URL, the user and the password reference are enough; the rest has defaults") {
    val settings = CoordinationSettings.from(required.get _)

    settings.map(_.jdbcUrl) shouldBe Right(required(JdbcUrl))
    settings.map(_.user) shouldBe Right("ingest_coordination")
    settings.map(_.password.toString) shouldBe Right(required(PasswordRef))
    settings.map(_.poolMaxSize) shouldBe Right(4)
    settings.map(_.connectTimeout) shouldBe Right(5.seconds)
    settings.map(_.statementTimeout) shouldBe Right(10.seconds)
  }

  test("the pool size and the timeouts can be set") {
    val settings = CoordinationSettings.from(
      (required ++ Map(
        PoolMaxSize -> "2",
        ConnectTimeoutMs -> "750",
        StatementTimeoutMs -> "1500"
      )).get _
    )

    settings.map(_.poolMaxSize) shouldBe Right(2)
    settings.map(_.connectTimeout) shouldBe Right(750.millis)
    settings.map(_.statementTimeout) shouldBe Right(1500.millis)
  }

  test("every problem is reported at once, by the name of its setting") {
    val found = problems(
      Map(
        JdbcUrl -> "jdbc:mysql://db/x",
        PasswordRef -> "hunter2",
        PoolMaxSize -> "0",
        ConnectTimeoutMs -> "soon"
      )
    )

    found should have size 5
    Seq(JdbcUrl, User, PasswordRef, PoolMaxSize, ConnectTimeoutMs).foreach { key =>
      withClue(s"$key: ") { found.count(_.startsWith(key)) shouldBe 1 }
    }
  }

  test("a password written where its reference belongs is refused, and not repeated") {
    val found = problems(required + (PasswordRef -> "hunter2"))

    found should have size 1
    found.head should startWith(PasswordRef)
    found.head should not include "hunter2"
  }

  test("the URL cannot carry a password, nor the timeouts the LogStore sets itself") {
    ReservedUrlParameters.foreach { parameter =>
      val url = s"jdbc:postgresql://db/ingestion_control?sslmode=verify-full&$parameter=hunter2"
      val found = problems(required + (JdbcUrl -> url))
      withClue(s"$parameter: ") {
        found should have size 1
        found.head should include(parameter)
        found.head should not include "hunter2"
      }
    }
    // Whatever the capitals: the driver reads parameter names as written, a typo must not pass.
    problems(required + (JdbcUrl -> "jdbc:postgresql://db/x?PASSWORD=hunter2")) should have size 1
  }

  test("a connection timeout below the pool's minimum is refused") {
    problems(required + (ConnectTimeoutMs -> "100")) should have size 1
    problems(required + (ConnectTimeoutMs -> "250")) shouldBe empty
  }
}
