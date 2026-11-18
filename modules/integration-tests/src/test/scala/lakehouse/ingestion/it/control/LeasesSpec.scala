package lakehouse.ingestion.it.control

import scala.concurrent.duration._
import scala.util.Using

import lakehouse.ingestion.core.config.Source
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.control.{
  ControlDatabase,
  LeasedRun,
  LeaseLostException,
  Leases,
  Runs
}
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.logstore.Concurrently
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The leases of sources on a real PostgreSQL (ADR-0006): one run at a time on a source, however
 * many ask and whenever; a run that died is taken over when its lease expires, by the clock of
 * the database; and a run whose lease was taken changes nothing, and is told so.
 *
 * Every instance is a pool of its own to the database, as the role of the engine. Where a test
 * waits for a lease to expire, it waits for real: the leases here last two seconds.
 */
final class LeasesSpec extends AnyFunSuite with Matchers with ControlPlaneSuite {

  /** Renewed every 667 ms, and taken as good by its run for 1.67 s. */
  private val Short = LeaseTiming(2.seconds)

  /** A lease as a deployment has it: nothing here waits for it to expire. */
  private val Long = LeaseTiming(2.minutes)

  private def newSource(): Source = source(newSourceId().value)

  private def open(
      of: Source,
      layer: Layer = Layer.Bronze,
      timing: LeaseTiming = Long,
      on: ControlDatabase = database
  ): Either[LeasedRun.Skipped, LeasedRun] =
    LeasedRun.open(on, starting(of, layer), versionOf(of), timing)

  private def lease(of: Source, layer: Layer = Layer.Bronze): Leases.Lease =
    database.run(Leases.find(_, LeaseKey.of(of.id, layer))).value

  private def run(id: RunId): RunRecord = database.run(Runs.find(_, id)).value

  test("a run takes the lease of its source, and is running under its first epoch") {
    val orders = newSource()
    val before = databaseNow()

    val leased = open(orders).value

    leased.epoch shouldBe epoch(1)
    leased.tookOver shouldBe None
    leased.lost shouldBe None
    leased.verify()
    val record = run(leased.start.id)
    record.status shouldBe RunStatus.Running
    record.epoch shouldBe Some(epoch(1))

    val held = lease(orders)
    held.epoch shouldBe 1L
    val holder = held.holder.value
    holder.run shouldBe leased.start.id
    holder.instance shouldBe leased.start.instance
    // For its time to live, counted by the database from when it was taken.
    holder.acquiredAt should (be >= before and be <= databaseNow())
    holder.expiresAt shouldBe holder.acquiredAt.plusSeconds(120)

    leased.finish(RunEnd.Succeeded(RunCounts(rowsWritten = Some(7)))) shouldBe true
    run(leased.start.id).status shouldBe RunStatus.Succeeded
    run(leased.start.id).counts.rowsWritten shouldBe Some(7L)
    // Given up, for the next run to take at once; the epoch stays, to be counted from.
    lease(orders) shouldBe Leases.Lease(LeaseKey.of(orders.id, Layer.Bronze), 1L, None)
  }

  test("a run that finds the lease taken is skipped: recorded, and nothing else is changed") {
    val orders = newSource()
    val first = open(orders).value
    val held = lease(orders)

    val skipped = open(orders).left.value

    skipped.holder shouldBe first.start.id
    skipped.instance shouldBe first.start.instance
    skipped.until shouldBe held.holder.value.expiresAt
    val record = run(skipped.start.id)
    record.status shouldBe RunStatus.SkippedLocked
    record.epoch shouldBe None
    record.finishedAt should not be empty
    // The lease is as it was, to the moment at which it expires.
    lease(orders) shouldBe held
    run(first.start.id).status shouldBe RunStatus.Running

    first.finish(RunEnd.Succeeded()) shouldBe true
    // And the next run has the next epoch.
    val next = open(orders).value
    next.epoch shouldBe epoch(2)
    next.tookOver shouldBe None
    next.finish(RunEnd.Failed(FailureClass.Transient, "the source did not answer")) shouldBe true
    lease(orders).holder shouldBe None
  }

  test(
    "of the instances that ask for a source at the same moment, one runs and the others are skipped"
  ) {
    // RNF-03. Twelve instances, each with connections of its own, three rounds on one source.
    val orders = newSource()
    val instances = Seq.fill(12)(anotherInstance())
    try
      (1 to 3).foreach { round =>
        val answers = Concurrently.atOnce(instances)(instance => open(orders, on = instance))

        val running = answers.collect { case Right(leased) => leased }
        val skipped = answers.collect { case Left(skip) => skip }
        withClue(s"round $round: ") {
          running should have size 1
          skipped should have size 11
          val winner = running.head
          winner.epoch shouldBe epoch(round.toLong)
          // Every one that was skipped was skipped by the one that runs.
          skipped.map(_.holder).distinct shouldBe Seq(winner.start.id)
          lease(orders).holder.value.run shouldBe winner.start.id
          lease(orders).epoch shouldBe round.toLong

          // The audit trail has them all, and tells them apart.
          run(winner.start.id).status shouldBe RunStatus.Running
          skipped.map(skip => run(skip.start.id).status).distinct shouldBe Seq(
            RunStatus.SkippedLocked
          )
          (skipped.map(_.start.id) :+ winner.start.id).distinct should have size 12

          winner.finish(RunEnd.Succeeded()) shouldBe true
        }
      }
    finally instances.foreach(_.close())

    asSuperuser { c =>
      query(
        c,
        "SELECT status, count(*) FROM control.ingestion_run WHERE source_id = ? GROUP BY status ORDER BY status",
        orders.id.value
      ) shouldBe Seq(Seq("SKIPPED_LOCKED", "33"), Seq("SUCCEEDED", "3"))
    }
  }

  test("runs of other sources, and of the other layer of a source, do not wait for each other") {
    val sources = Seq.fill(8)(newSource())
    val instances = Seq.fill(8)(anotherInstance())
    try {
      val together = Concurrently.atOnce(sources.zip(instances)) { case (one, instance) =>
        open(one, on = instance)
      }
      together.foreach(_.isRight shouldBe true)
      together.map(_.value.epoch).distinct shouldBe Seq(epoch(1))

      // Bronze and Silver of one source are two leases: the two are ingested apart.
      val silver = open(sources.head, Layer.Silver).value
      silver.epoch shouldBe epoch(1)
      lease(sources.head, Layer.Silver).holder.value.run shouldBe silver.start.id
      lease(sources.head, Layer.Bronze).holder.value.run shouldBe together.head.value.start.id

      (together.map(_.value) :+ silver).foreach(_.finish(RunEnd.Succeeded()) shouldBe true)
    } finally instances.foreach(_.close())
  }

  test("a lease that is renewed outlives its time to live, for as long as its run works") {
    val orders = newSource()
    val leased = open(orders, timing = Short).value
    val first = lease(orders).holder.value

    // Two and a half times its time to live.
    Thread.sleep(5000)

    leased.lost shouldBe None
    leased.verify()
    val now = lease(orders).holder.value
    now.run shouldBe leased.start.id
    now.acquiredAt shouldBe first.acquiredAt
    now.expiresAt should be > first.expiresAt.plusSeconds(3)
    now.expiresAt should be > databaseNow()
    lease(orders).epoch shouldBe 1L
    // And nobody else had it in the meantime.
    open(orders).left.value.holder shouldBe leased.start.id

    leased.finish(RunEnd.Succeeded()) shouldBe true
  }

  test("a run that died is taken over when its lease expires, and not a moment before") {
    val orders = newSource()
    val dead = open(orders, timing = Short).value
    // What is left of a process that died: nothing renews its lease, and nothing was said.
    dead.close()

    // Not yet: the lease is its own until the database says that it expired.
    open(orders, timing = Short).left.value.holder shouldBe dead.start.id
    run(dead.start.id).status shouldBe RunStatus.Running

    awaitExpiry(dead.start.leaseKey)
    val next = open(orders, timing = Short).value

    next.epoch shouldBe epoch(2)
    next.tookOver shouldBe Some(dead.start.id)
    lease(orders).holder.value.run shouldBe next.start.id
    // The run that held it ended halfway, and the audit trail says so.
    val abandoned = run(dead.start.id)
    abandoned.status shouldBe RunStatus.Abandoned
    abandoned.finishedAt should not be empty
    abandoned.epoch shouldBe Some(epoch(1))
    run(next.start.id).status shouldBe RunStatus.Running

    next.finish(RunEnd.Succeeded()) shouldBe true
  }

  test(
    "a zombie changes nothing: it is told that its run was ended, and its lease is not its own"
  ) {
    val orders = newSource()
    val key = LeaseKey.of(orders.id, Layer.Bronze)
    // A process that was frozen: it holds a lease, renews nothing, and does not know.
    val zombie = open(orders, timing = Short).value
    zombie.close()
    awaitExpiry(key)
    val next = open(orders).value
    val held = lease(orders)

    // It wakes up, finishes its work, and reports.
    zombie.finish(RunEnd.Succeeded(RunCounts(rowsWritten = Some(99)))) shouldBe false

    val abandoned = run(zombie.start.id)
    abandoned.status shouldBe RunStatus.Abandoned
    abandoned.counts shouldBe RunCounts.Nothing
    // Its epoch is the one before: nothing it asks on condition of it is done.
    database.run(Leases.renew(_, key, zombie.start.id, zombie.epoch, Short.ttl)) shouldBe false
    database.run(Leases.release(_, key, zombie.start.id, zombie.epoch)) shouldBe false
    // Nor with its run and the epoch of the other.
    database.run(Leases.release(_, key, zombie.start.id, next.epoch)) shouldBe false
    database.run(Leases.renew(_, key, next.start.id, zombie.epoch, Short.ttl)) shouldBe false
    lease(orders) shouldBe held
    run(next.start.id).status shouldBe RunStatus.Running

    next.finish(RunEnd.Succeeded()) shouldBe true
  }

  test("a run that ended and left its lease behind is not ended again by the one that takes it") {
    val orders = newSource()
    val first = open(orders, timing = Short).value
    // It said how it ended, and died before it gave its lease up.
    first.close()
    database.run(
      Runs.end(_, first.start.id, RunEnd.Failed(FailureClass.Permanent, "why"))
    ) shouldBe true

    awaitExpiry(first.start.leaseKey)
    val next = open(orders).value

    next.tookOver shouldBe Some(first.start.id)
    next.epoch shouldBe epoch(2)
    // What ended stays as it ended.
    run(first.start.id).status shouldBe RunStatus.Failed
    next.finish(RunEnd.Succeeded()) shouldBe true
  }

  test("a run that ends while another takes its lease over waits for it, and does not deadlock") {
    // Found by the review of P4: a takeover locks the lease and then ends the run that held it;
    // a run that ended itself first and gave the lease up after held the two the other way
    // round, and PostgreSQL ended one of them as a deadlock.
    val orders = newSource()
    val leased = open(orders).value
    val pool = java.util.concurrent.Executors.newSingleThreadExecutor()
    try
      Using.resource(anotherInstance()) { other =>
        // What a takeover does, in its order, held between its two steps.
        val finishing = other.transaction { c =>
          query(
            c,
            "SELECT 1 FROM control.source_lease WHERE lease_key = ? FOR UPDATE",
            leased.start.leaseKey.value
          ): Unit
          val ending = pool.submit[Boolean](() =>
            leased.finish(RunEnd.Failed(FailureClass.Transient, "it stopped by itself"))
          )
          // Until the run that is ending waits for a lock.
          val deadline = System.nanoTime() + 10.seconds.toNanos
          def waiting: Boolean = asSuperuser { s =>
            query(
              s,
              "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND usename = 'ingest_control'"
            ).head.head.toInt > 0
          }
          while (!waiting && System.nanoTime() < deadline) Thread.sleep(20)
          waiting shouldBe true
          Runs.abandon(c, leased.start.id) shouldBe true
          ending
        }

        // The takeover went through, and the run was told that another had ended it.
        finishing.get(20, java.util.concurrent.TimeUnit.SECONDS) shouldBe false
        run(leased.start.id).status shouldBe RunStatus.Abandoned
      }
    finally pool.shutdown()
  }

  test("a renewal holds the lease for its time to live again, counted from the renewal") {
    val orders = newSource()
    val key = LeaseKey.of(orders.id, Layer.Bronze)
    val leased = open(orders).value
    // Renewed by hand here, and not behind the test's back.
    leased.close()
    val taken = lease(orders).holder.value

    Thread.sleep(300)
    database.run(Leases.renew(_, key, leased.start.id, leased.epoch, 10.minutes)) shouldBe true

    val renewed = lease(orders).holder.value
    renewed.acquiredAt shouldBe taken.acquiredAt
    renewed.expiresAt should be >= taken.acquiredAt.plusSeconds(600)
    renewed.expiresAt should be <= databaseNow().plusSeconds(600)
    lease(orders).epoch shouldBe 1L

    leased.finish(RunEnd.Succeeded()) shouldBe true
    // What was given up is not renewed, nor given up twice.
    database.run(Leases.renew(_, key, leased.start.id, leased.epoch, 10.minutes)) shouldBe false
    database.run(Leases.release(_, key, leased.start.id, leased.epoch)) shouldBe false
    leased.finish(RunEnd.Succeeded()) shouldBe false
  }

  test(
    "a lease that nobody asked for is not there, and one that is asked for and not taken is free"
  ) {
    val key = LeaseKey.of(newSourceId(), Layer.Bronze)
    database.run(Leases.find(_, key)) shouldBe None

    // Asked for in a transaction that then gives up: the lease was made, and is nobody's.
    val asking = starting(source(newSourceId().value))
    intercept[IllegalStateException] {
      database.transaction { c =>
        Leases.acquire(c, key, asking.id, asking.instance, Long.ttl) shouldBe a[
          Leases.Acquisition.Acquired
        ]
        throw new IllegalStateException("the run was not written after all")
      }
    }
    database.run(Leases.find(_, key)) shouldBe None

    // And a lease cannot be held by a run that was never written.
    val refused = intercept[java.sql.SQLException] {
      database.transaction(c => Leases.acquire(c, key, asking.id, asking.instance, Long.ttl))
    }
    refused.getSQLState shouldBe "23503"
    database.run(Leases.find(_, key)) shouldBe None
  }

  test("a run whose lease was taken finds out at its next renewal, and stops") {
    val orders = newSource()
    val leased = open(orders, timing = Short).value
    // Its lease is taken from under it, as a run that was frozen past its lease finds it: the
    // database is told that it expired, and another run takes it.
    asSuperuser { c =>
      update(
        c,
        "UPDATE control.source_lease SET expires_at = now() - interval '1 second' WHERE lease_key = ?",
        leased.start.leaseKey.value
      ) shouldBe 1
    }
    val next = open(orders).value
    next.tookOver shouldBe Some(leased.start.id)

    // Within one renewal, which is a third of two seconds.
    val deadline = System.nanoTime() + 5.seconds.toNanos
    while (leased.lost.isEmpty && System.nanoTime() < deadline) Thread.sleep(20)

    leased.lost shouldBe Some(LeaseLoss.TakenOver)
    val stopped = the[LeaseLostException] thrownBy leased.verify()
    stopped.getMessage shouldBe s"the lease of ${orders.id}/bronze is lost: another run holds it"
    leased.finish(RunEnd.Succeeded()) shouldBe false
    lease(orders).holder.value.run shouldBe next.start.id

    next.finish(RunEnd.Succeeded()) shouldBe true
    Using.resource(anotherInstance())(other =>
      open(orders, on = other).value.finish(RunEnd.Succeeded())
    )
  }
}
