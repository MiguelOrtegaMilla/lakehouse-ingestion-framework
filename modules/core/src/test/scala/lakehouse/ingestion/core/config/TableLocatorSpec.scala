package lakehouse.ingestion.core.config

import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class TableLocatorSpec extends AnyFunSuite with Matchers with EitherValues {

  private def root(text: String): RootUri = RootUri.parse(text).value
  private def path(text: String): RelativePath = RelativePath.parse(text).value

  private val tables = Seq(
    "bronze/sales/orders",
    "silver/sales/orders",
    "bronze/sales/orders2",
    "bronze/sales",
    "orders",
    "raw/dt=2026-11-17/part-0001"
  )

  test("a table is the root of the lake and its path after it") {
    val local = new PathTableLocator(root("file:///data/lake"))
    local.locate(path("bronze/sales/orders")).uri shouldBe "file:///data/lake/bronze/sales/orders"

    val objects = new PathTableLocator(root("s3a://lake/prod"))
    objects.locate(path("bronze/sales/orders")).uri shouldBe "s3a://lake/prod/bronze/sales/orders"
    new PathTableLocator(root("s3a://lake")).locate(path("orders")).uri shouldBe "s3a://lake/orders"
  }

  test("one table has one URI, however the lake was written") {
    val plain = new PathTableLocator(root("file:///data/lake"))
    val slashed = new PathTableLocator(root("file:///data/lake/"))

    tables.map(path).foreach { table =>
      withClue(table)(slashed.locate(table) shouldBe plain.locate(table))
    }
    new PathTableLocator(root("s3a://lake/")).locate(path("orders")) shouldBe
      new PathTableLocator(root("s3a://lake")).locate(path("orders"))
  }

  test("two tables never have one URI") {
    val locator = new PathTableLocator(root("file:///data/lake"))
    val located = tables.map(table => locator.locate(path(table)))

    located.distinct should have size tables.size.toLong
    located.map(_.uri).toSet should have size tables.size.toLong
  }

  test("every table is under the lake, and no text names one outside it") {
    val lake = root("file:///data/lake")
    val locator = new PathTableLocator(lake)

    tables.foreach { table =>
      withClue(table)(locator.locate(path(table)).uri should startWith(lake.value + "/"))
    }
    // What would lead out of the lake is no path at all: there is nothing to locate.
    Seq(
      "bronze/../../x",
      "../other-lake/orders",
      "/etc/passwd",
      "file:///etc/passwd",
      "s3a://other-bucket/x",
      "bronze/%2e%2e/%2e%2e/x",
      "..\\x",
      "bronze//orders",
      ""
    ).foreach(text => withClue(s"[$text]")(RelativePath.parse(text).isLeft shouldBe true))
    // A lake whose name another starts with does not hold the other's tables.
    locator.locate(path("orders")).uri should not startWith root("file:///data/lake2").value + "/"
    new PathTableLocator(root("file:///data/lake2")).locate(path("orders")).uri should not startWith
      lake.value + "/"
  }

  test("a location prints as its URI") {
    val location = new PathTableLocator(root("file:///data/lake")).locate(path("bronze/orders"))

    location.toString shouldBe "file:///data/lake/bronze/orders"
    s"$location" shouldBe location.uri
  }
}
