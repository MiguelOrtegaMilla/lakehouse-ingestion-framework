package lakehouse.ingestion.it.control

import java.sql.SQLException

import scala.concurrent.duration._
import scala.util.Using

import io.circe.Json
import lakehouse.ingestion.core.config.{PathTableLocator, RelativePath, Source, TableLocation}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.control._
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The write-ahead log of batches and the commit of the state, on a real PostgreSQL, with the
 * protocol of ADR-0005 §3 played out step by step: a plan is written down, its data is written
 * to a table, and the state is committed. Runs die between any two of them, and the run that
 * comes after finds what was left and finishes it, once.
 *
 * The tables are a Delta that is simulated (`SimulatedDelta`): what recovery asks of a table is
 * one number, and the simulation keeps it as Delta does. The control plane is the real one.
 *
 * A run dies here by having its heartbeat stopped and its lease expired at once in the
 * database: that a lease expires by itself, and when, is `LeasesSpec`'s to show.
 */
final class BatchesSpec extends AnyFunSuite with Matchers with ControlPlaneSuite {

  private val timing = LeaseTiming(2.minutes)
  private lazy val locator = new PathTableLocator(profile.lake.uri)

  private def newSource(): Source = source(newSourceId().value)

  private def open(
      of: Source,
      layer: Layer = Layer.Bronze,
      on: ControlDatabase = database
  ): LeasedRun =
    LeasedRun.open(on, starting(of, layer), versionOf(of), timing).value

  /** The process of a run dies: nothing renews its lease, and the lease runs out. */
  private def dies(leased: LeasedRun): Unit = {
    leased.close()
    expires(leased)
  }

  /** The lease of a run runs out in the database, whatever the run believes of it. */
  private def expires(leased: LeasedRun): Unit = asSuperuser { c =>
    update(
      c,
      "UPDATE control.source_lease SET expires_at = now() - interval '1 second' WHERE lease_key = ?",
      leased.start.leaseKey.value
    ) shouldBe 1: Unit
  }

  private def bronze(of: Source): TableLocation = locator.locate(of.targets.bronze.table)
  private def quarantineOf(of: Source): RelativePath =
    RelativePath.parse(s"quarantine/${of.id.value.replace('.', '/')}").value

  // What a connector would write as its plan: a range of a watermark, and what it reads by.
  private def range(from: Int, to: Int): Json = Json.obj(
    "from" -> Json.fromInt(from),
    "to" -> Json.fromInt(to),
    "predicate" -> Json.fromString("updated_at > ? AND updated_at <= ?"),
    "lookback" -> Json.fromString("10m")
  )

  /** What reading a plan gives: the same rows however often it is read. */
  private def read(plan: Json): Seq[String] = {
    val bounds = plan.hcursor
    (bounds.get[Int]("from").value until bounds.get[Int]("to").value).map(i => s"row-$i")
  }

  /** Where the batch after this one starts from. */
  private def watermarkAfter(plan: Json): Json =
    Json.obj("updated_at" -> plan.hcursor.get[Json]("to").value)

  private def batch(id: BatchId): BatchRecord = database.run(Batches.find(_, id)).value
  private def run(id: RunId): RunRecord = database.run(Runs.find(_, id)).value
  private def state(of: Source, layer: Layer = Layer.Bronze): Option[SourceState] =
    database.run(SourceStates.find(_, of.id, layer))
  private def lease(of: Source): Leases.Lease =
    database.run(Leases.find(_, LeaseKey.of(of.id, Layer.Bronze))).value

  /** Steps 4 to 8 for a batch: reads its plan, writes its data, and commits its state. */
  private def finish(
      leased: LeasedRun,
      of: Source,
      delta: SimulatedDelta,
      planned: PlannedBatch
  ): Boolean = {
    val version = delta.write(bronze(of), planned.id, read(planned.plan))
    leased.commit(
      planned.id,
      Some(watermarkAfter(planned.plan)),
      Seq(
        BatchOutput(
          of.targets.bronze.table,
          version.orElse(delta.committedAt(bronze(of), planned.id)).value
        )
      ),
      RunCounts(rowsRead = Some(read(planned.plan).size.toLong))
    )
  }

  test(
    "the plan of a batch is written down before anything is read, as the first of its sequence"
  ) {
    val orders = newSource()
    val leased = open(orders)
    leased.planned() shouldBe None
    state(orders) shouldBe None

    val planned = leased.plan(range(0, 10))

    planned.id shouldBe BatchId(leased.start.txnAppId, BatchSeq.First)
    planned.plannedBy shouldBe leased.start.id
    val record = batch(planned.id)
    record.status shouldBe BatchStatus.Planned
    record.plan shouldBe range(0, 10)
    record.source shouldBe orders.id
    record.plannedBy shouldBe leased.start.id
    record.plannedAt shouldBe planned.plannedAt
    record.committedBy shouldBe None
    record.outputs shouldBe None
    // The run says which batch it is on, and nothing has moved yet.
    run(leased.start.id).batch shouldBe Some(BatchSeq.First)
    state(orders) shouldBe None
    // It is found again as it was written, by whoever looks.
    leased.planned() shouldBe Some(planned)

    leased.finish(RunEnd.Failed(FailureClass.Transient, "the source did not answer")) shouldBe true
  }

  test(
    "the commit of a batch moves the state, ends the batch and the run, and gives the lease up"
  ) {
    val orders = newSource()
    val delta = new SimulatedDelta
    val leased = open(orders)
    val planned = leased.plan(range(0, 10))
    val version = delta.write(bronze(orders), planned.id, read(planned.plan)).value
    val before = databaseNow()

    leased.commit(
      planned.id,
      Some(watermarkAfter(planned.plan)),
      Seq(BatchOutput(orders.targets.bronze.table, version)),
      RunCounts(rowsRead = Some(10), rowsWritten = Some(10), rowsRejected = Some(0))
    ) shouldBe true

    val moved = state(orders).value
    moved.watermark shouldBe Some(Json.obj("updated_at" -> Json.fromInt(10)))
    moved.lastCommitted shouldBe BatchSeq.First
    moved.updatedBy shouldBe leased.start.id
    moved.updatedAt should (be >= before and be <= databaseNow())

    val committed = batch(planned.id)
    committed.status shouldBe BatchStatus.Committed
    committed.committedBy shouldBe Some(leased.start.id)
    committed.outputs shouldBe Some(Vector(BatchOutput(orders.targets.bronze.table, 0L)))
    committed.plan shouldBe planned.plan

    val ended = run(leased.start.id)
    ended.status shouldBe RunStatus.Succeeded
    ended.counts shouldBe RunCounts(
      rowsRead = Some(10),
      rowsWritten = Some(10),
      rowsRejected = Some(0)
    )
    ended.batch shouldBe Some(BatchSeq.First)
    // One moment for all of it: the batch, the state and the run were committed together.
    committed.committedAt shouldBe Some(moved.updatedAt)
    ended.finishedAt shouldBe Some(moved.updatedAt)

    lease(orders).holder shouldBe None
    // What is committed is committed once.
    leased.commit(planned.id, None, Nil) shouldBe false
    state(orders).value shouldBe moved
  }

  test("the next batch has the next number, and starts from where the state is") {
    val orders = newSource()
    val delta = new SimulatedDelta
    (1 to 3).foreach { number =>
      val leased = open(orders)
      leased.planned() shouldBe None
      val from =
        state(orders).flatMap(_.watermark).map(_.hcursor.get[Int]("updated_at").value).getOrElse(0)
      val planned = leased.plan(range(from, from + 10))

      planned.id.seq.value shouldBe number.toLong
      finish(leased, orders, delta, planned) shouldBe true
      state(orders).value.lastCommitted.value shouldBe number.toLong
    }

    delta.rows(bronze(orders)) shouldBe (0 until 30).map(i => s"row-$i")
    delta.lastVersion(bronze(orders), TxnAppId.of(orders.id, Layer.Bronze)) shouldBe Some(3L)
    state(orders).value.watermark shouldBe Some(Json.obj("updated_at" -> Json.fromInt(30)))
  }

  test(
    "a run that died after it planned leaves a plan that the next run finds unchanged, and replays"
  ) {
    val orders = newSource()
    val delta = new SimulatedDelta
    // A plan with every kind of thing in it: it is the connector's, and comes back as it went.
    val plan = range(0, 10).deepMerge(
      Json.obj(
        "files" -> Json
          .arr(Json.fromString("landing/a b.csv"), Json.fromString("landing/ñ-😀.csv")),
        "ratio" -> Json.fromBigDecimal(BigDecimal("0.010")),
        "nothing" -> Json.Null,
        "nested" -> Json.obj("deep" -> Json.arr(Json.True, Json.obj()))
      )
    )
    val dead = open(orders)
    val written = dead.plan(plan)
    dies(dead)

    // Another process, with nothing of the first but the database.
    Using.resource(anotherInstance()) { other =>
      val next = open(orders, on = other)
      next.tookOver shouldBe Some(dead.start.id)

      val found = next.planned().value
      found shouldBe written
      found.plan shouldBe plan
      found.plannedBy shouldBe dead.start.id
      // The table has never heard of the batch: its plan is run again, as it is.
      Recovery.of(Some(found), bronze(orders), delta) shouldBe Recovery.Replay(found)
      finish(next, orders, delta, found) shouldBe true

      delta.rows(bronze(orders)) shouldBe (0 until 10).map(i => s"row-$i")
      val committed = batch(found.id)
      committed.status shouldBe BatchStatus.Committed
      committed.plannedBy shouldBe dead.start.id
      committed.committedBy shouldBe Some(next.start.id)
      // One batch, and two runs of it: the one that died halfway and the one that finished it.
      run(dead.start.id).status shouldBe RunStatus.Abandoned
      run(dead.start.id).batch shouldBe Some(BatchSeq.First)
      run(next.start.id).status shouldBe RunStatus.Succeeded
      run(next.start.id).batch shouldBe Some(BatchSeq.First)
      state(orders).value.updatedBy shouldBe next.start.id
    }
  }

  test(
    "a run that died after it wrote is rolled forward: its data is accounted for, and not written again"
  ) {
    val orders = newSource()
    val delta = new SimulatedDelta
    val dead = open(orders)
    val planned = dead.plan(range(0, 10))
    delta.write(bronze(orders), planned.id, read(planned.plan)) shouldBe Some(0L)
    // The data is in the table, and the state does not know.
    dies(dead)
    state(orders) shouldBe None

    val next = open(orders)
    val found = next.planned().value
    val recovery = Recovery.of(Some(found), bronze(orders), delta)

    recovery shouldBe Recovery.RollForward(found)
    // Nothing is read and nothing is written: what the dead run did not get to say is said.
    next.commit(
      found.id,
      Some(watermarkAfter(found.plan)),
      Seq(
        BatchOutput(orders.targets.bronze.table, delta.committedAt(bronze(orders), found.id).value)
      )
    ) shouldBe true

    delta.rows(bronze(orders)) shouldBe (0 until 10).map(i => s"row-$i")
    state(orders).value.watermark shouldBe Some(Json.obj("updated_at" -> Json.fromInt(10)))
    batch(found.id).outputs shouldBe Some(Vector(BatchOutput(orders.targets.bronze.table, 0L)))
    batch(found.id).committedBy shouldBe Some(next.start.id)

    // And the batch after it goes on from there.
    val after = open(orders)
    after.planned() shouldBe None
    after.plan(range(10, 20)).id.seq.value shouldBe 2L
    after.finish(RunEnd.Failed(FailureClass.Transient, "enough")) shouldBe true
  }

  test("a batch of two tables that died between them is replayed, and each table has it once") {
    val orders = newSource()
    val delta = new SimulatedDelta
    val quarantine = locator.locate(quarantineOf(orders))
    val dead = open(orders)
    val planned = dead.plan(range(0, 10))
    // The quarantine first, then the data (step 5). It died in between.
    delta.write(quarantine, planned.id, Seq("rejected-3")) shouldBe Some(0L)
    dies(dead)

    val next = open(orders)
    val found = next.planned().value
    // The table that is written last has not got the batch: it is not all there.
    Recovery.of(Some(found), bronze(orders), delta) shouldBe Recovery.Replay(found)
    // Replayed whole. The table that had it skips it, and the other takes it.
    delta.write(quarantine, found.id, Seq("rejected-3")) shouldBe None
    val version = delta.write(bronze(orders), found.id, read(found.plan)).value
    next.commit(
      found.id,
      Some(watermarkAfter(found.plan)),
      Seq(
        BatchOutput(quarantineOf(orders), delta.committedAt(quarantine, found.id).value),
        BatchOutput(orders.targets.bronze.table, version)
      )
    ) shouldBe true

    delta.rows(quarantine) shouldBe Vector("rejected-3")
    delta.rows(bronze(orders)) should have size 10
    batch(found.id).outputs.value.map(_.table) shouldBe Vector(
      quarantineOf(orders),
      orders.targets.bronze.table
    )
  }

  test("a zombie advances nothing: not the state, not a plan, and its data is in the table once") {
    val orders = newSource()
    val delta = new SimulatedDelta
    // It planned, wrote, and was frozen before it committed.
    val zombie = open(orders)
    val planned = zombie.plan(range(0, 10))
    delta.write(bronze(orders), planned.id, read(planned.plan)) shouldBe Some(0L)
    dies(zombie)
    // The run that takes over finishes the batch, and goes on to the next.
    val next = open(orders)
    finish(next, orders, delta, next.planned().value) shouldBe true
    val further = open(orders)
    val second = further.plan(range(10, 20))
    val moved = state(orders).value

    // The zombie wakes up, with a state of its own to commit.
    zombie.commit(
      planned.id,
      Some(Json.obj("updated_at" -> Json.fromInt(999))),
      Seq(BatchOutput(orders.targets.bronze.table, 0L))
    ) shouldBe false
    state(orders).value shouldBe moved
    batch(planned.id).committedBy shouldBe Some(next.start.id)
    run(zombie.start.id).status shouldBe RunStatus.Abandoned

    // It cannot plan, nor take the batch that is planned as its own.
    a[LeaseLostException] should be thrownBy zombie.plan(range(500, 600))
    a[LeaseLostException] should be thrownBy zombie.planned()
    further.planned() shouldBe Some(second)
    batch(second.id).plan shouldBe range(10, 20)
    run(zombie.start.id).batch shouldBe Some(BatchSeq.First)

    // And what it writes late, of a batch that the table has, is skipped by the table.
    delta.write(bronze(orders), planned.id, read(planned.plan)) shouldBe None
    delta.rows(bronze(orders)) shouldBe (0 until 10).map(i => s"row-$i")

    finish(further, orders, delta, second) shouldBe true
    delta.rows(bronze(orders)) shouldBe (0 until 20).map(i => s"row-$i")
  }

  test("a run that is told that its lease is another's knows it from then on") {
    val orders = newSource()
    // Frozen, and not dead: its heartbeat is there, and its next renewal is 40 s away.
    val zombie = open(orders)
    expires(zombie)
    val next = open(orders)
    try {
      next.tookOver shouldBe Some(zombie.start.id)
      zombie.lost shouldBe None

      // It is told by the first thing it asks of the control plane, and not at its next renewal.
      val stopped = the[LeaseLostException] thrownBy zombie.plan(range(0, 10))

      stopped.loss shouldBe LeaseLoss.TakenOver
      zombie.lost shouldBe Some(LeaseLoss.TakenOver)
      a[LeaseLostException] should be thrownBy zombie.verify()
      database.run(Batches.planned(_, zombie.start.txnAppId)) shouldBe None
    } finally zombie.close()
    next.finish(RunEnd.Succeeded()) shouldBe true
  }

  test("while a batch is planned no other is, and its plan is the one that was written first") {
    val orders = newSource()
    val leased = open(orders)
    val planned = leased.plan(range(0, 10))

    // The same run, with another plan of the same thing: a number has one plan, for ever.
    val refused = intercept[SQLException](leased.plan(range(0, 99)))

    refused.getSQLState shouldBe "23505"
    leased.planned() shouldBe Some(planned)
    batch(planned.id).plan shouldBe range(0, 10)
    database.run(Batches.find(_, BatchId(planned.id.app, planned.id.seq.next))) shouldBe None
    leased.finish(RunEnd.Failed(FailureClass.Permanent, "why")) shouldBe true

    // The run after it does not plan either: the batch is still there to be finished.
    val next = open(orders)
    next.planned() shouldBe Some(planned)
    intercept[SQLException](next.plan(range(10, 20))).getSQLState shouldBe "23505"
    next.finish(RunEnd.Failed(FailureClass.Permanent, "why")) shouldBe true
  }

  test("the commit of a batch is all of it or none: what cannot be committed changes nothing") {
    val orders = newSource()
    val delta = new SimulatedDelta
    val first = open(orders)
    finish(first, orders, delta, first.plan(range(0, 10))) shouldBe true
    val moved = state(orders).value

    // A batch that is not the planned one: the state would move, and the batch could not follow.
    val leased = open(orders)
    val planned = leased.plan(range(10, 20))
    val notPlanned = BatchId(planned.id.app, planned.id.seq.next)
    val refused = intercept[IllegalStateException] {
      leased.commit(notPlanned, Some(Json.obj("updated_at" -> Json.fromInt(999))), Nil)
    }
    refused.getMessage shouldBe s"the batch $notPlanned was not committed: it is not a planned batch"
    state(orders).value shouldBe moved
    batch(planned.id).status shouldBe BatchStatus.Planned
    run(leased.start.id).status shouldBe RunStatus.Running
    lease(orders).holder.value.run shouldBe leased.start.id
    leased.finish(RunEnd.Failed(FailureClass.Permanent, refused.getMessage)) shouldBe true

    // A run that somebody ended while it still held its lease: the batch and the state would
    // be committed by a run that is not there to have committed them.
    val ended = open(orders)
    val found = ended.planned().value
    asSuperuser { c =>
      update(
        c,
        "UPDATE control.ingestion_run SET status = 'ABANDONED', finished_at = now() WHERE run_id = ?",
        ended.start.id.uuid
      ) shouldBe 1
    }
    val again = intercept[IllegalStateException](finish(ended, orders, delta, found))
    again.getMessage should endWith(s"the run ${ended.start.id} is not running")
    state(orders).value shouldBe moved
    batch(found.id).status shouldBe BatchStatus.Planned
    batch(found.id).committedBy shouldBe None
    lease(orders).holder.value.run shouldBe ended.start.id
  }

  test(
    "a run does not end as succeeded past a batch that is planned: the way past it is to commit it"
  ) {
    val orders = newSource()
    val delta = new SimulatedDelta
    val leased = open(orders)
    val planned = leased.plan(range(0, 10))

    val refused = intercept[IllegalStateException](leased.finish(RunEnd.Succeeded()))

    refused.getMessage shouldBe
      s"the run ${leased.start.id} cannot end as succeeded: the batch ${planned.id} is planned " +
      "and not committed. A run succeeds past a batch by committing it"
    run(leased.start.id).status shouldBe RunStatus.Running
    batch(planned.id).status shouldBe BatchStatus.Planned
    lease(orders).holder.value.run shouldBe leased.start.id
    // It may say that it failed, and it may commit the batch.
    finish(leased, orders, delta, planned) shouldBe true
    run(leased.start.id).status shouldBe RunStatus.Succeeded

    // With nothing planned, a run that found nothing to do ends as succeeded.
    val idle = open(orders)
    idle.planned() shouldBe None
    idle.finish(RunEnd.Succeeded()) shouldBe true
    run(idle.start.id).batch shouldBe None
  }

  test("the state of a source only moves forward") {
    val orders = newSource()
    val delta = new SimulatedDelta
    (1 to 2).foreach { _ =>
      val leased = open(orders)
      finish(leased, orders, delta, leased.plan(range(0, 10))) shouldBe true
    }
    val moved = state(orders).value
    moved.lastCommitted.value shouldBe 2L

    // Whoever asks, and with whatever watermark: a batch that the state is past moves nothing.
    Seq(1L, 2L).foreach { seq =>
      database.run { c =>
        SourceStates.advance(
          c,
          orders.id,
          Layer.Bronze,
          None,
          BatchSeq.of(seq).value,
          moved.updatedBy
        )
      } shouldBe false
    }
    state(orders).value shouldBe moved
  }

  test("each source, and each layer of a source, has a sequence and a state of its own") {
    val delta = new SimulatedDelta
    val (orders, returns) = (newSource(), newSource())
    val runs = Seq(open(orders), open(returns), open(orders, Layer.Silver))

    val planned = runs.map(_.plan(range(0, 5)))

    planned.map(_.id.seq) shouldBe Seq.fill(3)(BatchSeq.First)
    planned.map(_.id.app.value) shouldBe
      Seq(s"${orders.id}/bronze", s"${returns.id}/bronze", s"${orders.id}/silver")
    runs.zip(planned).foreach { case (leased, batch) =>
      val table = locator.locate(
        RelativePath
          .parse(s"${leased.start.layer}/${leased.start.source.value.replace('.', '/')}")
          .value
      )
      val version = delta.write(table, batch.id, read(batch.plan)).value
      leased.commit(
        batch.id,
        None,
        Seq(BatchOutput(orders.targets.bronze.table, version))
      ) shouldBe true
    }

    state(orders, Layer.Bronze).value.lastCommitted shouldBe BatchSeq.First
    state(orders, Layer.Silver).value.lastCommitted shouldBe BatchSeq.First
    state(returns, Layer.Bronze).value.lastCommitted shouldBe BatchSeq.First
    state(returns, Layer.Silver) shouldBe None
    // A source with no watermark has none: it is loaded in full.
    state(orders).value.watermark shouldBe None
  }

  test("a plan or a watermark that could not be read back is not written down") {
    val orders = newSource()
    val leased = open(orders)
    val unreadable = Json.obj(("k" * 2000) -> Json.Null)

    val noPlan = intercept[IllegalArgumentException](leased.plan(unreadable))
    noPlan.getMessage should startWith(
      "the plan cannot be stored, since it could not be read back: "
    )
    leased.planned() shouldBe None

    val planned = leased.plan(range(0, 10))
    val noWatermark = intercept[IllegalArgumentException] {
      leased.commit(planned.id, Some(unreadable), Nil)
    }
    noWatermark.getMessage should startWith(
      "the watermark cannot be stored, since it could not be read back: "
    )
    state(orders) shouldBe None
    batch(planned.id).status shouldBe BatchStatus.Planned
    leased.finish(RunEnd.Failed(FailureClass.Permanent, noWatermark.getMessage)) shouldBe true
  }
}
