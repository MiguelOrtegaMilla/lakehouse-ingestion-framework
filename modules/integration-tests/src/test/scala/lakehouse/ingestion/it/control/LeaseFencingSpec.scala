package lakehouse.ingestion.it.control

import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration._
import scala.util.Using

import eu.rekawek.toxiproxy.model.ToxicDirection
import lakehouse.ingestion.core.config.{PostgresUrl, SecretResolver, Source, Timeouts}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.control.{
  ControlDatabase,
  LeasedRun,
  LeaseLostException,
  Leases,
  Runs
}
import lakehouse.ingestion.it.support.{BootstrappedPostgres, ProxiedPostgres}
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

/**
 * Self-fencing, with the network broken for real (ADR-0006): a run that cannot renew its lease
 * stops working before the lease expires, whether its connection is refused or just never
 * answered, and a run that rode an outage out goes on.
 *
 * The run under test reaches PostgreSQL through a proxy that the test cuts or stalls. The test
 * itself looks at the database another way, which stays whole: what it says of the lease at a
 * given moment is what the database had, not what the run believed.
 *
 * The leases last twelve seconds: renewed every four, tried again every second after a
 * failure, and taken as good by their run for ten. A call to the database gives up within a
 * second.
 *
 * When a run stops is said twice, by two clocks. By its own, which only moves forward: at ten
 * seconds, give or take a moment. And by the database's, which is the time of day of its
 * machine: with the lease still good, for the two seconds that are left of the twelve less
 * whatever that clock was stepped forward by in the meantime, which on some machines is most of
 * a second.
 */
final class LeaseFencingSpec
    extends AnyFunSuite
    with Matchers
    with ControlPlaneSuite
    with Eventually {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(25, Seconds), interval = Span(25, Millis))

  import LeaseFencingSpec.Seen

  private lazy val proxied = ProxiedPostgres.start()
  override protected lazy val pg: BootstrappedPostgres = proxied.postgres
  override protected def stopServers(): Unit = proxied.stop()

  private val timing = LeaseTiming(12.seconds)

  /** An instance that reaches the control plane through the proxy that the test breaks. */
  private def throughTheProxy(): ControlDatabase = ControlDatabase
    .open(
      profile.databases.control.copy(
        url = PostgresUrl.parse(proxied.faultyJdbcUrl(Database)).value,
        timeouts = Timeouts(connect = 1.second, statement = 1.second)
      ),
      SecretResolver(environment(pg).get)
    )
    .value

  private def newSource(): Source = source(newSourceId().value)

  private def open(of: Source, on: ControlDatabase): LeasedRun =
    LeasedRun.open(on, starting(of), versionOf(of), timing).value

  private def lease(of: Source): Leases.Lease =
    database.run(Leases.find(_, LeaseKey.of(of.id, Layer.Bronze))).value

  private def run(id: RunId): RunRecord = database.run(Runs.find(_, id)).value

  /**
   * Has `leased` say, the moment it loses its lease, how long it had held it by its own clock,
   * and what the database has of the lease then, asked over a connection that the faults do
   * not touch and that is open beforehand.
   *
   * @param before `System.nanoTime` from before the lease was asked for
   */
  private def seenWhenLost[T](leased: LeasedRun, before: Long)(
      body: AtomicReference[Option[Seen]] => T
  ): T =
    Using.resource(proxied.observe(BootstrappedPostgres.Superuser, Database)) { observer =>
      val seen = new AtomicReference[Option[Seen]](None)
      leased.onLost { _ =>
        val after = (System.nanoTime() - before).nanos
        val row = query(
          observer,
          """SELECT holder_run_id = ?, expires_at > now(), extract(epoch FROM expires_at - now())
            |  FROM control.source_lease WHERE lease_key = ?""".stripMargin,
          leased.start.id.uuid,
          leased.start.leaseKey.value
        ).head
        seen.set(Some(Seen(after, row(0) == "t", row(1) == "t", row(2).toDouble)))
      }
      body(seen)
    }

  /** Stopped on time by its own clock, and with its lease still its own by the database's. */
  private def stoppedInTime(seen: Seen): Unit = withClue(seen.toString) {
    // Not before it had to, and with the sixth of its lease still ahead of it.
    seen.after should be >= timing.validFor
    seen.after should be < (timing.validFor + 1.second)
    // No other run could have taken the lease: the database had it as this run's, and good.
    seen.stillItsOwn shouldBe true
    seen.notExpired shouldBe true
    info(
      f"it stopped ${seen.after.toMillis / 1000.0}%.2f s after it asked for its lease, which the " +
        f"database had as good for ${seen.secondsLeft}%.2f s more"
    )
  }

  test("a run that is cut off from the control plane stops before its lease expires") {
    val orders = newSource()
    Using.resource(throughTheProxy()) { cutOff =>
      val before = System.nanoTime()
      val leased = open(orders, cutOff)
      seenWhenLost(leased, before) { seen =>
        proxied.faulty.disable()
        try {
          eventually(seen.get should not be empty)

          // It stopped by its own count, with nobody to tell it.
          leased.lost.value shouldBe a[LeaseLoss.NotRenewed]
          leased.lost.value.reason should startWith("it was not renewed in time: ")
          a[LeaseLostException] should be thrownBy leased.verify()
          stoppedInTime(seen.get.value)

          // Another instance, that reaches the database, takes over when the lease expires.
          awaitExpiry(leased.start.leaseKey)
          val next = open(orders, database)
          next.tookOver shouldBe Some(leased.start.id)
          next.epoch shouldBe epoch(2)
          run(leased.start.id).status shouldBe RunStatus.Abandoned

          // The network is back, and the run that was cut off reports what it did. It is refused.
          proxied.faulty.enable()
          leased.finish(RunEnd.Succeeded(RunCounts(rowsWritten = Some(42)))) shouldBe false
          run(leased.start.id).status shouldBe RunStatus.Abandoned
          run(leased.start.id).counts shouldBe RunCounts.Nothing
          lease(orders).holder.value.run shouldBe next.start.id
          lease(orders).epoch shouldBe 2L

          next.finish(RunEnd.Succeeded()) shouldBe true
        } finally proxied.faulty.enable()
      }
    }
  }

  test(
    "a network that stops answering, without refusing, does not keep a run from stopping on time"
  ) {
    val orders = newSource()
    Using.resource(throughTheProxy()) { stalled =>
      val before = System.nanoTime()
      val leased = open(orders, stalled)
      seenWhenLost(leased, before) { seen =>
        // Nothing comes back from the database, and no connection is closed: every call waits.
        proxied.faulty.toxics().timeout("stall", ToxicDirection.DOWNSTREAM, 0): Unit
        try {
          eventually(seen.get should not be empty)

          leased.lost.value shouldBe a[LeaseLoss.NotRenewed]
          stoppedInTime(seen.get.value)
        } finally proxied.faulty.toxics().get("stall").remove()

        // The network answers again before anybody took the lease. The run says itself how it
        // ended, and gives its lease up: the next run does not wait for it to expire.
        leased.finish(RunEnd.Failed(FailureClass.Transient, leased.lost.value.reason)) shouldBe true
        val failed = run(leased.start.id)
        failed.status shouldBe RunStatus.Failed
        failed.failure.value.failure shouldBe FailureClass.Transient
        lease(orders).holder shouldBe None

        val next = open(orders, database)
        next.epoch shouldBe epoch(2)
        next.tookOver shouldBe None
        next.finish(RunEnd.Succeeded()) shouldBe true
      }
    }
  }

  test(
    "an outage that is shorter than the lease is ridden out: the run goes on, and so does its lease"
  ) {
    val orders = newSource()
    Using.resource(throughTheProxy()) { instance =>
      val leased = open(orders, instance)
      val opened = System.nanoTime()
      val taken = lease(orders).holder.value

      Thread.sleep(500)
      proxied.faulty.disable()
      try {
        // Past the renewal that was due after four seconds, and a try or two after it.
        Thread.sleep(5000)
        // None of them got through: the lease is as it was taken.
        lease(orders).holder.value.expiresAt shouldBe taken.expiresAt
        leased.lost shouldBe None
      } finally proxied.faulty.enable()

      // The next try does, well before the run would have given its lease up.
      eventually(lease(orders).holder.value.expiresAt should be > taken.expiresAt)
      leased.lost shouldBe None

      // And the run goes on past the moment at which the lease it first took would have expired.
      val left = 14.seconds.toMillis - (System.nanoTime() - opened) / 1000000L
      Thread.sleep(math.max(0L, left))
      leased.lost shouldBe None
      leased.verify()
      val now = lease(orders)
      now.epoch shouldBe 1L
      now.holder.value.run shouldBe leased.start.id
      now.holder.value.expiresAt should be > databaseNow()
      run(leased.start.id).status shouldBe RunStatus.Running

      leased.finish(RunEnd.Succeeded()) shouldBe true
    }
  }
}

object LeaseFencingSpec {

  import scala.concurrent.duration.FiniteDuration

  /** What the database had of a lease at the moment a run was told that it lost it. */
  final private case class Seen(
      after: FiniteDuration,
      stillItsOwn: Boolean,
      notExpired: Boolean,
      secondsLeft: Double
  )
}
