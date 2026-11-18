package lakehouse.ingestion.core.control

import lakehouse.ingestion.core.config.SourceId
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The keys of the control plane: what a lease is taken on, and what batches are numbered by. */
final class KeysSpec extends AnyFunSuite with Matchers with EitherValues {

  private val orders: SourceId = SourceId.parse("sales.orders").value

  test("a layer is bronze or silver") {
    Layer.All.map(_.name) shouldBe Seq("bronze", "silver")
    Layer.parse("bronze").value shouldBe Layer.Bronze
    Layer.parse("silver").value shouldBe Layer.Silver
    Seq("gold", "Bronze", "", "landing").foreach { text =>
      withClue(s"[$text]")(Layer.parse(text).left.value shouldBe "must be one of bronze, silver")
    }
  }

  test("the key of a lease is the source and the layer, as ADR-0006 writes it") {
    LeaseKey.of(orders, Layer.Bronze).value shouldBe "sales.orders/bronze"
    LeaseKey.of(orders, Layer.Silver).toString shouldBe "sales.orders/silver"
    // One source in two layers is two leases: Bronze and Silver are ingested apart.
    LeaseKey.of(orders, Layer.Bronze) should not be LeaseKey.of(orders, Layer.Silver)
    LeaseKey.parse("sales.orders/bronze").value shouldBe LeaseKey.of(orders, Layer.Bronze)
  }

  test(
    "the name batches are written to Delta under is the source and the layer, as ADR-0005 writes it"
  ) {
    TxnAppId.of(orders, Layer.Bronze).value shouldBe "sales.orders/bronze"
    TxnAppId.parse("sales.orders/silver").value shouldBe TxnAppId.of(orders, Layer.Silver)
    // Not by table: two sources that write one table have a sequence each.
    val returns = SourceId.parse("sales.returns").value
    TxnAppId.of(orders, Layer.Bronze) should not be TxnAppId.of(returns, Layer.Bronze)
  }

  test("a key that is not a source and a layer is refused") {
    val notKeys = Seq(
      "",
      "sales.orders",
      "sales.orders/",
      "/bronze",
      "sales.orders/gold",
      "Sales.Orders/bronze",
      "sales.orders/bronze/",
      "sales.orders/bronze/extra",
      "sales orders/bronze",
      "../bronze",
      "sales.orders/bronze\n"
    )
    notKeys.foreach { text =>
      withClue(s"[$text]") {
        LeaseKey.parse(text).isLeft shouldBe true
        TxnAppId.parse(text).isLeft shouldBe true
      }
    }
    LeaseKey.parse("sales.orders").left.value shouldBe
      "must be <source_id>/<layer>, as sales.orders/bronze"
    LeaseKey.parse("sales.orders/gold").left.value shouldBe
      "has a layer that must be one of bronze, silver"
    TxnAppId.parse("Sales.Orders/bronze").left.value should startWith(
      "has a source id that must be"
    )
  }

  test("an epoch is 1 or more, and a later one is greater") {
    Epoch.of(1).value.value shouldBe 1L
    Epoch.of(0).left.value shouldBe "is 0, and an epoch is 1 or more"
    Epoch.of(-3).isLeft shouldBe true
    Seq(3L, 1L, 2L).map(Epoch.of(_).value).sorted.map(_.value) shouldBe Seq(1L, 2L, 3L)
    Epoch.of(7).value.toString shouldBe "7"
  }

  test("the batches of a sequence are numbered from 1") {
    BatchSeq.First.value shouldBe 1L
    BatchSeq.First.next.next.value shouldBe 3L
    BatchSeq.of(12).value.value shouldBe 12L
    BatchSeq.of(0).left.value shouldBe "is 0, and the first batch of a sequence is 1"
    Seq(BatchSeq.First.next, BatchSeq.First).sorted shouldBe Seq(
      BatchSeq.First,
      BatchSeq.First.next
    )
  }
}
