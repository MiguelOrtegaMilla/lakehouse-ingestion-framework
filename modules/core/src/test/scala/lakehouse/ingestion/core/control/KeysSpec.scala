package lakehouse.ingestion.core.control

import lakehouse.ingestion.core.config.SourceId
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The keys of the control plane: what a lease is taken on, and what batches are numbered by, for the
 * regular sequence of a source and for a backfill of it.
 */
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

  test("a key that is not the name of a sequence is refused") {
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
      "sales.orders/bronze\n",
      // ADR-0006 had the lease of a backfill under backfill/<backfill_id>; ADR-0021 has it under
      // the name of its sequence, and no key is of the old form.
      "backfill/history-2024",
      "sales.orders/bronze/backfill",
      "sales.orders/bronze/backfill/",
      "sales.orders/bronze/refill/history-2024",
      "sales.orders/bronze/backfill/history-2024/",
      "sales.orders/bronze/backfill/history/2024",
      "sales.orders/bronze/Backfill/history-2024",
      "sales.orders/bronze/backfill/History-2024",
      "sales.orders/gold/backfill/history-2024",
      "/bronze/backfill/history-2024"
    )
    notKeys.foreach { text =>
      withClue(s"[$text]") {
        LeaseKey.parse(text).isLeft shouldBe true
        TxnAppId.parse(text).isLeft shouldBe true
      }
    }
    val form = "must be <source_id>/<layer>, as sales.orders/bronze, or " +
      "<source_id>/<layer>/backfill/<backfill_id>, as sales.orders/bronze/backfill/history-2024"
    LeaseKey.parse("sales.orders").left.value shouldBe form
    TxnAppId.parse("sales.orders/bronze/refill/history-2024").left.value shouldBe form
    LeaseKey.parse("sales.orders/gold").left.value shouldBe
      "has a layer that must be one of bronze, silver"
    TxnAppId.parse("Sales.Orders/bronze").left.value should startWith(
      "has a source id that must be"
    )
    TxnAppId.parse("sales.orders/bronze/backfill/").left.value shouldBe
      "has a backfill id that is empty"
    LeaseKey.parse("sales.orders/bronze/backfill/History-2024").left.value should startWith(
      "has a backfill id that must be lower-case letters"
    )
  }

  // -----------------------------------------------------------------------------------------------
  // Backfills (ADR-0021 §5)
  // -----------------------------------------------------------------------------------------------

  private def backfillId(text: String): BackfillId = BackfillId.parse(text).value

  test("the id of a backfill is lower-case letters, digits, '.', '_' and '-', with no '/'") {
    Seq("history-2024", "2024", "a", "q1_2025.retry", "0-9", "x" * BackfillId.MaxLength)
      .foreach { text =>
        withClue(s"[$text]")(BackfillId.parse(text).value.value shouldBe text)
      }
    val grammar = "must be lower-case letters, digits, '.', '_' and '-', starting with a " +
      "letter or a digit, as history-2024"
    Seq("History", "-x", ".x", "_x", "a/b", "a b", "a\n", "a\\b", "año", "a:b", "a*", "..")
      .foreach { text =>
        withClue(s"[$text]")(BackfillId.parse(text).left.value shouldBe grammar)
      }
    BackfillId.parse("").left.value shouldBe "is empty"
    BackfillId.parse("x" * 64).left.value shouldBe
      "is 64 characters long, and an id is 63 at most"
  }

  test("a backfill is a sequence of its own, whose name is also the key of its lease") {
    val history = backfillId("history-2024")
    val app = TxnAppId.backfill(orders, Layer.Bronze, history)
    app.value shouldBe "sales.orders/bronze/backfill/history-2024"
    LeaseKey.backfill(orders, Layer.Bronze, history).value shouldBe app.value
    app.backfill shouldBe Some(history)
    TxnAppId.of(orders, Layer.Bronze).backfill shouldBe None

    // Not the regular sequence, nor a backfill of another layer, source or id: the same id in
    // two sources is two backfills.
    val returns = SourceId.parse("sales.returns").value
    Seq(
      TxnAppId.of(orders, Layer.Bronze),
      TxnAppId.backfill(orders, Layer.Silver, history),
      TxnAppId.backfill(returns, Layer.Bronze, history),
      TxnAppId.backfill(orders, Layer.Bronze, backfillId("history-2023"))
    ).foreach(other => app should not be other)
    LeaseKey.backfill(orders, Layer.Bronze, history) should not be LeaseKey.of(orders, Layer.Bronze)
  }

  test("the name of the sequence of a backfill is read back as itself") {
    val history = backfillId("history-2024")
    TxnAppId.parse("sales.orders/bronze/backfill/history-2024").value shouldBe
      TxnAppId.backfill(orders, Layer.Bronze, history)
    TxnAppId.parse("sales.orders/bronze/backfill/history-2024").value.backfill shouldBe
      Some(history)
    LeaseKey.parse("sales.orders/silver/backfill/history-2024").value shouldBe
      LeaseKey.backfill(orders, Layer.Silver, history)
    // And the regular one as before.
    TxnAppId.parse("sales.orders/bronze").value.backfill shouldBe None
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
