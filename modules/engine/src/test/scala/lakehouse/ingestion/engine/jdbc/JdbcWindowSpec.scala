package lakehouse.ingestion.engine.jdbc

import java.time.Instant

import io.circe.Json
import lakehouse.ingestion.core.config.{JdbcConnectorConfig, Load, Source}
import lakehouse.ingestion.core.control.FailureClass
import lakehouse.ingestion.core.state.StoredWatermark
import lakehouse.ingestion.engine.RunFailedException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{EitherValues, OptionValues}

/**
 * The plan of the next window of a JDBC source, by a clock that the test gives in the place of
 * the source's (P6.7): what the connector makes of the clock, of the watermark as the control
 * plane has it and of the settings of the load, with no database. Which window that is, rule by
 * rule, is the planner's (`WatermarkPlanSpec`); that the clock is the source's own, and what a
 * window reads, are held to a real server (`JdbcSourceConnectorSpec`).
 */
final class JdbcWindowSpec extends AnyFunSuite with Matchers with EitherValues with OptionValues {

  private def orders(watermark: String, range: String = ""): Source = Source
    .load(
      "sales.orders.yaml",
      s"""id: sales.orders
         |connector:
         |  type: jdbc
         |  connection:
         |    url: jdbc:postgresql://db:5432/sales
         |    user: sales_ro
         |    password: { secretRef: "env:SALES_RO_PASSWORD" }
         |    tls: { mode: disable }
         |  table: public.orders
         |load:
         |  mode: incremental
         |  watermark: { column: updated_at, safety_margin: 1m$watermark }
         |  $range
         |targets: { bronze: { table: bronze/sales/orders } }
         |""".stripMargin
    )
    .value
    .value

  private def window(of: Source, watermark: Option[Json], clock: Instant): Option[Json] =
    JdbcSourceConnector.nextWindow(
      of,
      of.connector.asInstanceOf[JdbcConnectorConfig],
      of.load.asInstanceOf[Load.Incremental],
      watermark,
      clock
    )

  private def stored(at: String): Option[Json] =
    Some(StoredWatermark(Instant.parse(at)).toJson)

  private def refused(of: Source, watermark: Option[Json], clock: Instant): String = {
    val failure = intercept[RunFailedException](window(of, watermark, clock))
    failure.failure shouldBe FailureClass.Permanent
    failure.getMessage
  }

  private val Noon = Instant.parse("2026-11-21T12:00:00.123456Z")

  test("a window ends at what the clock of the source says, less the margin") {
    // The first of a source: the table, up to the horizon.
    window(orders(""), watermark = None, Noon).value.noSpaces shouldBe
      """{"version":1,"column":"updated_at","lower":null,""" +
      """"upper":"2026-11-21T11:59:00.123456Z","horizon":"2026-11-21T11:59:00.123456Z"}"""

    // And every one after it: from the watermark that the control plane has.
    window(orders(""), stored("2026-11-21T11:00:00Z"), Noon).value.noSpaces shouldBe
      """{"version":1,"column":"updated_at","lower":"2026-11-21T11:00:00Z",""" +
      """"upper":"2026-11-21T11:59:00.123456Z","horizon":"2026-11-21T11:59:00.123456Z"}"""
  }

  test("a window is cut by the settings of the load, and says how far its invocation reads") {
    val cut = orders(", lookback: 10m, start: \"2026-01-01T00:00:00Z\"", "max_batch_range: 7d")

    window(cut, watermark = None, Noon).value.noSpaces shouldBe
      """{"version":1,"column":"updated_at","lower":"2026-01-01T00:00:00Z",""" +
      """"upper":"2026-01-08T00:00:00Z","horizon":"2026-11-21T11:59:00.123456Z"}"""
    window(cut, stored("2026-01-08T00:00:00Z"), Noon).value.noSpaces shouldBe
      """{"version":1,"column":"updated_at","lower":"2026-01-07T23:50:00Z",""" +
      """"upper":"2026-01-15T00:00:00Z","horizon":"2026-11-21T11:59:00.123456Z"}"""
  }

  test("a source that is read up to its horizon, or whose clock went back, has no window") {
    // Read up to the horizon of this clock, to the microsecond.
    window(orders(""), stored("2026-11-21T11:59:00.123456Z"), Noon) shouldBe None
    // Within the margin of the clock.
    window(orders(""), stored("2026-11-21T11:59:30Z"), Noon) shouldBe None
    // The clock itself is behind the watermark.
    window(orders(""), stored("2026-11-22T00:00:00Z"), Noon) shouldBe None
    // And a source that is to start later has not started.
    window(orders(", start: \"2027-01-01T00:00:00Z\""), watermark = None, Noon) shouldBe None
    // A microsecond later, there is a window of that microsecond.
    window(orders(""), stored("2026-11-21T11:59:00.123455Z"), Noon).value.noSpaces should include(
      """"lower":"2026-11-21T11:59:00.123455Z","upper":"2026-11-21T11:59:00.123456Z""""
    )
  }

  test("a watermark that this application did not write plans nothing") {
    refused(orders(""), Some(Json.fromLong(1763726400L)), Noon) shouldBe
      "the watermark that the control plane has of sales.orders is not one that this " +
      "application wrote, and nothing was planned: it is not an object. A batch is not " +
      "planned from a moment that was guessed"

    val ofAnotherForm =
      Json.obj("version" -> Json.fromInt(2), "value" -> Json.fromString("2026-11-21T11:00:00Z"))
    refused(orders(""), Some(ofAnotherForm), Noon) should include("and nothing was planned: it ")
    // A moment that is written as no moment of this application is.
    val withAnOffset = Json.obj(
      "version" -> Json.fromInt(1),
      "value" -> Json.fromString("2026-11-21T12:00:00+01:00")
    )
    refused(orders(""), Some(withAnOffset), Noon) should include("value is not written as")
  }

  test("a clock that says a time that no plan holds plans nothing") {
    val where = "the source sales.orders (jdbc:postgresql://db:5432/sales, as sales_ro)"
    val whyNot = "A batch of a watermark is bounded by the clock of its source, and a plan " +
      "holds no time outside 1900-01-01T00:00:00Z to 9999-12-31T23:59:59.999999Z"

    refused(orders(""), watermark = None, Instant.parse("+10000-01-01T00:00:00Z")) shouldBe
      s"no batch of $where can be planned: the clock of the source says " +
      s"+10000-01-01T00:00:00Z, after 9999-12-31T23:59:59.999999Z. $whyNot"
    refused(orders(""), watermark = None, Instant.parse("1900-01-01T00:00:30Z")) shouldBe
      s"no batch of $where can be planned: the clock of the source says " +
      s"1900-01-01T00:00:30Z, which less safety_margin is before 1900-01-01T00:00:00Z. $whyNot"
  }
}
