package lakehouse.ingestion.it.control

import java.time.Instant

import scala.concurrent.duration._

import io.circe.Json
import lakehouse.ingestion.core.config.Source
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.control._
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The backfills of the control plane on a real PostgreSQL (ADR-0021 §5): created by the first
 * run of their sequence under its lease, and moved by the commit of each chunk, as the commit of
 * a regular batch moves the state of its source, which a backfill does not touch.
 *
 * What the engine reads and writes of a chunk is `IngestionService`'s (P7.6c). Here the chunks
 * are plans of a test's own, and the tables are not asked: what is shown is the control plane.
 */
final class BackfillsSpec extends AnyFunSuite with Matchers with ControlPlaneSuite {

  private val timing = LeaseTiming(2.minutes)

  private val History = BackfillId.parse("history-2024").value
  private val Asked = OperatorRequest.of("ops", "load the history before start").value

  private val From = Instant.parse("2024-01-01T00:00:00Z")
  private val To = Instant.parse("2024-06-01T00:00:00Z")

  private def newSource(): Source = source(newSourceId().value)

  /** A run of the backfill `id` of `of` that is about to start, with its configuration stored. */
  private def startingBackfill(of: Source, id: BackfillId = History): RunStart =
    starting(of).copy(kind = RunKind.Backfill, request = Some(Asked), backfill = Some(id))

  private def openBackfill(of: Source, id: BackfillId = History): LeasedRun =
    LeasedRun.open(database, startingBackfill(of, id), versionOf(of), timing).value

  private def openRegular(of: Source): LeasedRun =
    LeasedRun.open(database, starting(of), versionOf(of), timing).value

  private def found(of: Source, id: BackfillId = History): Option[BackfillRecord] =
    database.run(Backfills.find(_, of.id, Layer.Bronze, id))

  private def created(leased: LeasedRun, chunk: FiniteDuration = 7.days): BackfillRecord =
    leased.backfill(From, To, chunk) match {
      case LeasedRun.Backfill.Created(record) => record
      case other => fail(s"the backfill was there: $other")
    }

  /** Where the chunk that ends at `upper` leaves the backfill, as its connector would say it. */
  private def watermark(upper: String): Json =
    Json.obj("version" -> Json.fromInt(1), "value" -> Json.fromString(upper))

  private def output(of: Source): Seq[BatchOutput] = Seq(BatchOutput(of.targets.bronze.table, 0L))

  /** The lease of a run runs out in the database, whatever the run believes of it. */
  private def expires(leased: LeasedRun): Unit = asSuperuser { c =>
    update(
      c,
      "UPDATE control.source_lease SET expires_at = now() - interval '1 second' WHERE lease_key = ?",
      leased.start.leaseKey.value
    ) shouldBe 1: Unit
  }

  test("a backfill is created by its first run, under its lease, and found by the runs after it") {
    val orders = newSource()
    val first = openBackfill(orders)
    first.start.leaseKey.value shouldBe s"${orders.id}/bronze/backfill/history-2024"
    val before = databaseNow()

    val record = created(first)

    record shouldBe BackfillRecord(
      orders.id,
      Layer.Bronze,
      History,
      From,
      To,
      7.days,
      createdBy = first.start.id,
      createdAt = record.createdAt,
      progress = None,
      completedAt = None
    )
    record.createdAt should (be >= before and be <= databaseNow())
    record.txnAppId shouldBe first.start.txnAppId
    record.leaseKey shouldBe first.start.leaseKey
    found(orders).value shouldBe record
    first.finish(RunEnd.Succeeded()) shouldBe true

    // The next run finds it as it was made, whatever it was asked for: whether that is what it
    // was asked for is its caller's to say.
    val next = openBackfill(orders)
    next.backfill(From.minusSeconds(1), To, 1.day) shouldBe LeasedRun.Backfill.There(record)
    found(orders).value shouldBe record
    next.finish(RunEnd.Succeeded()) shouldBe true

    // A backfill of another id, or of another source, is another.
    found(orders, BackfillId.parse("history-2023").value) shouldBe None
    found(newSource()) shouldBe None
  }

  test("what is read back is what was written: moments to the microsecond, and the chunk") {
    val orders = newSource()
    val cases = Seq(
      (
        "the widest range",
        Instant.parse("1900-01-01T00:00:00Z"),
        Instant.parse("9999-12-31T23:59:59.999999Z"),
        1.milli
      ),
      (
        "microseconds",
        Instant.parse("1969-12-31T23:59:59.999999Z"),
        Instant.parse("1970-01-01T00:00:00.000001Z"),
        (1.day + 1.milli)
      ),
      (
        "a leap year",
        Instant.parse("2024-02-28T23:00:00.123456Z"),
        Instant.parse("2024-03-01T01:00:00.654321Z"),
        36.hours
      )
    )
    cases.zipWithIndex.foreach { case ((what, from, to, chunk), index) =>
      withClue(s"$what: ") {
        val id = BackfillId.parse(s"case-$index").value
        val leased = openBackfill(orders, id)
        val made = leased.backfill(from, to, chunk) match {
          case LeasedRun.Backfill.Created(record) => record
          case other => fail(s"the backfill was there: $other")
        }
        leased.finish(RunEnd.Succeeded()) shouldBe true
        val read = found(orders, id).value
        read shouldBe made
        (read.from, read.to, read.chunk) shouldBe ((from, to, chunk))
      }
    }
  }

  test("the commit of a chunk moves its backfill, and not the state of its source") {
    val orders = newSource()
    val leased = openBackfill(orders)
    val record = created(leased)
    val chunk = leased.plan(Json.obj("chunk" -> Json.fromInt(1)))
    chunk.id shouldBe BatchId(record.txnAppId, BatchSeq.First)

    leased.commit(chunk.id, Some(watermark("2024-01-08T00:00:00Z")), output(orders)) shouldBe true

    val moved = found(orders).value
    moved.progress.value.watermark shouldBe watermark("2024-01-08T00:00:00Z")
    moved.progress.value.lastCommitted shouldBe BatchSeq.First
    moved.progress.value.updatedBy shouldBe leased.start.id
    moved.completedAt shouldBe None
    // What it reads and who created it are as they were.
    moved.copy(progress = None) shouldBe record
    // The state of the source is the regular sequence's, and the backfill did not touch it.
    database.run(SourceStates.find(_, orders.id, Layer.Bronze)) shouldBe None
    database.run(Batches.find(_, chunk.id)).value.kind shouldBe RunKind.Backfill
    database.run(Runs.find(_, leased.start.id)).value.status shouldBe RunStatus.Succeeded
    database.run(Leases.find(_, leased.start.leaseKey)).value.holder shouldBe None

    // The next chunk moves it on, and the one that ends at to completes it.
    val next = openBackfill(orders)
    val second = next.plan(Json.obj("chunk" -> Json.fromInt(2)))
    second.id.seq shouldBe BatchSeq.First.next
    next.commit(
      second.id,
      Some(watermark("2024-06-01T00:00:00Z")),
      output(orders),
      completes = true
    ) shouldBe true
    val complete = found(orders).value
    complete.progress.value.lastCommitted shouldBe second.id.seq
    complete.progress.value.updatedBy shouldBe next.start.id
    complete.completedAt.value should be >= complete.progress.value.updatedAt
  }

  test("a complete backfill is moved by nothing, and a progress that is past a chunk by no chunk") {
    val orders = newSource()
    val leased = openBackfill(orders)
    val record = created(leased)
    val chunk = leased.plan(Json.obj("chunk" -> Json.fromInt(1)))
    leased.commit(chunk.id, Some(watermark("2024-02-01T00:00:00Z")), output(orders)) shouldBe true
    val moved = found(orders).value

    // Whoever asks, and with whatever watermark: a chunk that the backfill is at, or past,
    // moves nothing.
    database.run(
      Backfills.advance(_, chunk.id, watermark("2024-09-09T00:00:00Z"), leased.start.id, false)
    ) shouldBe false
    found(orders).value shouldBe moved

    val last = BatchId(record.txnAppId, BatchSeq.First.next)
    database.run(
      Backfills.advance(_, last, watermark("2024-06-01T00:00:00Z"), leased.start.id, true)
    ) shouldBe true
    val complete = found(orders).value
    complete.completedAt should not be empty

    val later = BatchId(record.txnAppId, BatchSeq.First.next.next)
    database.run(
      Backfills.advance(_, later, watermark("2024-07-01T00:00:00Z"), leased.start.id, false)
    ) shouldBe false
    found(orders).value shouldBe complete
  }

  test("a zombie of a backfill moves nothing, and its chunk is the next run's to finish") {
    val orders = newSource()
    val zombie = openBackfill(orders)
    created(zombie)
    val chunk = zombie.plan(Json.obj("chunk" -> Json.fromInt(1)))
    zombie.close()
    expires(zombie)

    // The run that takes the lease over finds the chunk, and commits it.
    val next = openBackfill(orders)
    next.tookOver shouldBe Some(zombie.start.id)
    next.planned().value.id shouldBe chunk.id
    next.commit(chunk.id, Some(watermark("2024-01-08T00:00:00Z")), output(orders)) shouldBe true
    val moved = found(orders).value

    // The zombie wakes up, with a progress of its own to commit.
    zombie.commit(chunk.id, Some(watermark("2024-05-05T00:00:00Z")), output(orders)) shouldBe false
    found(orders).value shouldBe moved
    database.run(Runs.find(_, zombie.start.id)).value.status shouldBe RunStatus.Abandoned
    a[LeaseLostException] should be thrownBy zombie.backfill(From, To, 7.days)
  }

  test("a chunk says where the next one starts, and only a backfill is complete") {
    val orders = newSource()
    val leased = openBackfill(orders)
    created(leased)
    val chunk = leased.plan(Json.obj("chunk" -> Json.fromInt(1)))
    // Before anything is changed, and with the lease still the run's.
    an[IllegalArgumentException] should be thrownBy leased.commit(chunk.id, None, output(orders))
    found(orders).value.progress shouldBe None
    leased.commit(chunk.id, Some(watermark("2024-01-08T00:00:00Z")), output(orders)) shouldBe true

    val regular = openRegular(orders)
    val batch = regular.plan(Json.obj("batch" -> Json.fromInt(1)))
    an[IllegalArgumentException] should be thrownBy
      regular.commit(batch.id, None, output(orders), completes = true)
    an[IllegalArgumentException] should be thrownBy regular.backfill(From, To, 7.days)
    regular.commit(batch.id, None, output(orders)) shouldBe true
  }

  test("a backfill and the regular sequence of its source each have a lease, numbers and state") {
    val orders = newSource()
    // Both at once: two leases, neither waits for the other.
    val (backfill, regular) = (openBackfill(orders), openRegular(orders))
    created(backfill)
    val (chunk, batch) = (
      backfill.plan(Json.obj("chunk" -> Json.fromInt(1))),
      regular.plan(Json.obj("batch" -> Json.fromInt(1)))
    )
    // Chunk 1 and batch 1: one number in two sequences.
    chunk.id.seq shouldBe BatchSeq.First
    batch.id.seq shouldBe BatchSeq.First
    chunk.id.app should not be batch.id.app
    Seq(backfill, regular).map(leased =>
      database.run(Leases.find(_, leased.start.leaseKey)).value.holder.map(_.run)
    ) shouldBe Seq(Some(backfill.start.id), Some(regular.start.id))

    regular.commit(batch.id, Some(watermark("2026-11-23T00:00:00Z")), output(orders)) shouldBe
      true
    backfill.commit(chunk.id, Some(watermark("2024-01-08T00:00:00Z")), output(orders)) shouldBe
      true

    val state = database.run(SourceStates.find(_, orders.id, Layer.Bronze)).value
    state.watermark shouldBe Some(watermark("2026-11-23T00:00:00Z"))
    state.updatedBy shouldBe regular.start.id
    found(orders).value.progress.value.updatedBy shouldBe backfill.start.id
  }

  test("a backfill is held to the configuration of the run that created it, and to no other") {
    val orders = newSource()
    val first = openBackfill(orders)
    created(first)
    first.finish(RunEnd.Succeeded()) shouldBe true
    val createdWith = versionOf(orders)

    // A later run of it, with another configuration, and a run of the source that advances its
    // state with a third: neither is what the backfill is compared with.
    val changed = source(orders.id.value, _.replace("lookback: 0s", "lookback: 1s"))
    versionOf(changed).hash should not be createdWith.hash
    LeasedRun
      .open(database, startingBackfill(changed), versionOf(changed), timing)
      .value
      .finish(RunEnd.Succeeded()) shouldBe true
    val regular = openRegular(orders)
    regular.commit(
      regular.plan(Json.obj("batch" -> Json.fromInt(1))).id,
      None,
      output(orders)
    ) shouldBe true

    val snapshot = database.run(ConfigSnapshots.ofBackfill(_, first.start.txnAppId)).value
    snapshot.hash shouldBe createdWith.hash
    snapshot.content shouldBe createdWith.content
    database.run(
      ConfigSnapshots.ofBackfill(
        _,
        TxnAppId.backfill(orders.id, Layer.Bronze, BackfillId.parse("none").value)
      )
    ) shouldBe None
  }

  test("a run of a backfill is read back acting on its sequence") {
    val orders = newSource()
    val leased = openBackfill(orders)
    val run = database.run(Runs.find(_, leased.start.id)).value
    run.start shouldBe leased.start
    run.start.backfill shouldBe Some(History)
    run.txnAppId shouldBe leased.start.txnAppId
    leased.finish(RunEnd.Succeeded()) shouldBe true
  }
}
