package lakehouse.ingestion.core.control

import lakehouse.ingestion.core.config.{Quality, RelativePath, SourceId}
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Where the quarantine of a source is, what a record in it says, and how many rejected records
 * are too many for a batch (discovery §13.4, ADR-0008).
 */
final class QuarantineSpec extends AnyFunSuite with Matchers with EitherValues {

  private def source(id: String): SourceId = SourceId.parse(id).value

  test("the quarantine of a source is a table of its own, under the engine's directory") {
    Quarantine.table(source("sales.orders"), Layer.Bronze).value shouldBe
      "_quarantine/bronze/sales.orders"
    Quarantine.table(source("sales.orders"), Layer.Silver).value shouldBe
      "_quarantine/silver/sales.orders"
    Quarantine.table(source("sales.orders"), Layer.Bronze).segments shouldBe
      Vector("_quarantine", "bronze", "sales.orders")

    // The longest id that a source may have, and one with every character of an id.
    val long = source("a" * SourceId.MaxBytes)
    Quarantine.table(long, Layer.Bronze).value shouldBe s"_quarantine/bronze/${"a" * 100}"
    Quarantine.table(source("a_1.b2.x_c"), Layer.Bronze).value shouldBe
      "_quarantine/bronze/a_1.b2.x_c"
  }

  test("no source can name a quarantine as its table: a configuration has no such path") {
    val quarantine = Quarantine.table(source("sales.orders"), Layer.Bronze)

    // What a configuration is read with refuses it, and every path under the directory.
    RelativePath.parse(quarantine.value).left.value should include("starts with '_'")
    RelativePath.parse("_quarantine").isLeft shouldBe true
    RelativePath.parse("_quarantine/bronze").isLeft shouldBe true
    // Two sources have two quarantines, and a source has one for each layer.
    Quarantine.table(source("sales.orders"), Layer.Bronze) should not be
      Quarantine.table(source("sales.orders_v2"), Layer.Bronze)
    Quarantine.table(source("sales.orders"), Layer.Bronze) should not be
      Quarantine.table(source("sales.orders"), Layer.Silver)
  }

  test("the path of a quarantine is read back from where a batch recorded it, and no other") {
    val quarantine = Quarantine.table(source("sales.orders"), Layer.Bronze)

    RelativePath.stored(quarantine.value).value shouldBe quarantine
    RelativePath.stored("bronze/sales/orders").value.value shouldBe "bronze/sales/orders"

    // Under the engine's directory, a path is held to the grammar as any other is.
    Seq(
      "_quarantine",
      "_quarantine/",
      "_quarantine/../bronze/x",
      "_quarantine/bronze/_delta_log",
      "_quarantine//x",
      "_quarantine/bronze/x/",
      "_other/bronze/x",
      "bronze/_quarantine/x",
      "/_quarantine/bronze/x"
    ).foreach(path => withClue(s"[$path]")(RelativePath.stored(path).isLeft shouldBe true))
  }

  test("a record in quarantine says where it came from, why, and what it was") {
    Quarantine.Columns shouldBe Seq(
      "run_id",
      "batch_seq",
      "source_file",
      "error_type",
      "error_detail",
      "raw_record",
      "quarantined_at"
    )
    Quarantine.ErrorType.All shouldBe Seq("unparsed", "trailing_text", "duplicate_field")
  }

  test("the share of a batch that may be rejected is allowed whole, and no more") {
    val onePercent = Quality(0.01)
    onePercent.exceededBy(rejected = 0, read = 100) shouldBe false
    onePercent.exceededBy(rejected = 1, read = 100) shouldBe false
    onePercent.exceededBy(rejected = 2, read = 100) shouldBe true
    onePercent.exceededBy(rejected = 1, read = 99) shouldBe true
    onePercent.exceededBy(rejected = 10000000000L, read = 1000000000000L) shouldBe false
    onePercent.exceededBy(rejected = 10000000001L, read = 1000000000000L) shouldBe true

    // At a limit that a product of doubles misses: 0.07 * 100 is 7.000000000000001.
    Quality(0.07).exceededBy(rejected = 7, read = 100) shouldBe false
    Quality(0.07).exceededBy(rejected = 8, read = 100) shouldBe true
    // And one that it would let through: 0.29 * 100 is 28.999999999999996.
    Quality(0.29).exceededBy(rejected = 29, read = 100) shouldBe false
    Quality(0.29).exceededBy(rejected = 30, read = 100) shouldBe true
  }

  test("by default no record may be rejected, and a source may let every one be") {
    Quality.Default.exceededBy(rejected = 0, read = 0) shouldBe false
    Quality.Default.exceededBy(rejected = 0, read = 1000) shouldBe false
    Quality.Default.exceededBy(rejected = 1, read = 1000000) shouldBe true

    Quality(1.0).exceededBy(rejected = 5, read = 5) shouldBe false
    Quality(0.5).exceededBy(rejected = 1, read = 2) shouldBe false
    Quality(0.5).exceededBy(rejected = 2, read = 3) shouldBe true

    an[IllegalArgumentException] should be thrownBy Quality(1.0).exceededBy(rejected = 2, read = 1)
  }
}
