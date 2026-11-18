package lakehouse.ingestion.core.control

import java.time.Instant

import io.circe.Json
import lakehouse.ingestion.core.config.{
  PathTableLocator,
  RelativePath,
  RootUri,
  SourceId,
  TableLocation
}
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** What a run does with a batch that the run before it left planned (ADR-0005 §3, step 2). */
final class RecoverySpec extends AnyFunSuite with Matchers with EitherValues {

  private val locator = new PathTableLocator(RootUri.parse("file:///srv/lake").value)
  private val orders: TableLocation =
    locator.locate(RelativePath.parse("bronze/sales/orders").value)
  private val quarantine: TableLocation =
    locator.locate(RelativePath.parse("quarantine/sales/orders").value)
  private val app = TxnAppId.of(SourceId.parse("sales.orders").value, Layer.Bronze)

  private def planned(seq: Long): PlannedBatch = PlannedBatch(
    BatchId(app, BatchSeq.of(seq).value),
    Json.obj("from" -> Json.fromInt(0), "to" -> Json.fromInt(10)),
    RunId.generate(),
    Instant.parse("2026-11-18T09:00:00Z")
  )

  /** A Delta that has these versions, and remembers what it was asked. */
  final private class Tables(versions: Map[(TableLocation, TxnAppId), Long])
      extends TableTransactions {
    var asked = Vector.empty[(TableLocation, TxnAppId)]
    override def lastVersion(table: TableLocation, app: TxnAppId): Option[Long] = {
      asked :+= ((table, app))
      versions.get((table, app))
    }
    // Recovery decides by the last version alone: where a batch is, is asked after.
    override def committedAt(table: TableLocation, batch: BatchId): Option[Long] =
      fail("the decision asked where a batch was committed")
  }

  test("with no batch planned there is nothing to recover, and no table is asked") {
    val tables = new Tables(Map((orders, app) -> 7L))

    Recovery.of(None, orders, tables) shouldBe Recovery.NothingPlanned
    tables.asked shouldBe empty
  }

  test("a batch whose table has never heard of its sequence is replayed") {
    val batch = planned(1)
    // The table is not there yet, or has batches of other sources alone.
    Recovery.of(Some(batch), orders, new Tables(Map.empty)) shouldBe Recovery.Replay(batch)
    val others = TxnAppId.of(SourceId.parse("sales.returns").value, Layer.Bronze)
    Recovery.of(Some(batch), orders, new Tables(Map((orders, others) -> 9L))) shouldBe Recovery
      .Replay(batch)
  }

  test("a batch that its table has not got to is replayed, and one that it has is rolled forward") {
    val batch = planned(5)
    def with_(last: Long): Recovery =
      Recovery.of(Some(batch), orders, new Tables(Map((orders, app) -> last)))

    with_(3) shouldBe Recovery.Replay(batch)
    with_(4) shouldBe Recovery.Replay(batch)
    // Its own number: the data is in the table, and is not written again.
    with_(5) shouldBe Recovery.RollForward(batch)
    // Beyond it, as ADR-0005 has it: Delta would skip the write all the same.
    with_(6) shouldBe Recovery.RollForward(batch)
  }

  test("the table that is asked is the one a batch writes last, under the name of its sequence") {
    val batch = planned(2)
    // The quarantine has the batch, and the data, which is written after it, has not.
    val tables = new Tables(Map((quarantine, app) -> 2L, (orders, app) -> 1L))

    Recovery.of(Some(batch), orders, tables) shouldBe Recovery.Replay(batch)
    tables.asked shouldBe Vector((orders, app))
  }

  test("the batch that is replayed or rolled forward is the one that was planned, plan and all") {
    val batch = planned(3)

    Recovery.of(Some(batch), orders, new Tables(Map.empty)) match {
      case Recovery.Replay(replayed) =>
        replayed.plan shouldBe batch.plan
        replayed.id.toString shouldBe "sales.orders/bronze#3"
      case other => fail(s"not replayed: $other")
    }
  }

  test("what a batch wrote is a table and a version of it") {
    val output = BatchOutput(RelativePath.parse("bronze/sales/orders").value, 12)
    output.version shouldBe 12L
    an[IllegalArgumentException] should be thrownBy
      BatchOutput(RelativePath.parse("bronze/sales/orders").value, -1)
  }
}
