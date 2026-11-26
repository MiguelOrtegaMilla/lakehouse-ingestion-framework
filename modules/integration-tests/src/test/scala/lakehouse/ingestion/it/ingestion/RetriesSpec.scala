package lakehouse.ingestion.it.ingestion

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Using

import io.delta.exceptions.ConcurrentAppendException
import lakehouse.ingestion.core.config._
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.FaultPoint
import lakehouse.ingestion.engine.control.ControlDatabase
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.it.support.{
  BootstrappedPostgres,
  CoordinationSchema,
  CoordinationStores,
  ProxiedPostgres
}
import lakehouse.ingestion.logstore.{FaultPoint => LogPoint, HookedLogStore}
import org.apache.hadoop.fs.Path
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * A run tries again where it is durable after a failure that may pass, within the budget of
 * its profile (ADR-0022 §4, §5), against a real PostgreSQL behind a proxy that is cut for real:
 * the coordination database of the LogStore in the middle of a commit, for less than the
 * LogStore waits by itself and for longer; the control plane as the run takes its lease; and
 * both for longer than the run may wait. Each test ends with the ledger.
 *
 * The coordination database is reached through the proxy, and so is the control plane where a
 * test says so. A lease lives 12 s here, so that a run that cannot renew it stops by its own
 * count within a test.
 */
final class RetriesSpec extends AnyFunSuite with Matchers with OptionValues with IngestionSuite {

  private lazy val proxied = ProxiedPostgres.start()
  override protected lazy val pg: BootstrappedPostgres = proxied.postgres
  override protected def stopServers(): Unit = proxied.stop()

  // The LogStore waits about 3.3 s for its database: three attempts of the INSERT that takes a
  // version, a second each to connect, 100 ms and 200 ms apart.
  override protected lazy val coordination =
    CoordinationStores.settings(
      proxied.faultyJdbcUrl(CoordinationSchema.Database),
      pg,
      connectTimeout = 1.second,
      statementTimeout = 2.seconds
    )

  override protected def sparkConf: Map[String, String] =
    super.sparkConf.updated("spark.delta.logStore.file.impl", classOf[HookedLogStore].getName)

  override protected def timing: LeaseTiming = LeaseTiming(12.seconds)

  override protected def retries: Profile.Retries =
    Profile.Retries(budget = 30.seconds, firstBackoff = 200.millis, maxBackoff = 1.second)

  private val threads = Executors.newCachedThreadPool()
  implicit private val onThreads: ExecutionContext = ExecutionContext.fromExecutor(threads)

  override def afterAll(): Unit =
    try threads.shutdownNow(): Unit
    finally super.afterAll()

  // Each cut of the proxy, so that what is to make it whole again is for that cut alone: a
  // thread of an earlier test that wakes up late leaves a later cut as it is.
  private val cuts = new AtomicInteger

  /** The proxy is cut now, until it is made whole. */
  private def cut(): Unit = {
    cuts.incrementAndGet()
    proxied.faulty.disable()
  }

  /** The proxy is cut now, and whole again `after` that, in a thread of its own. */
  private def cutFor(after: FiniteDuration): Unit = {
    cut()
    val thisCut = cuts.get
    Future {
      Thread.sleep(after.toMillis)
      if (cuts.get == thisCut) proxied.faulty.enable()
    }: Unit
  }

  /** The proxy whole again, whatever cut it. */
  private def whole(): Unit = {
    cuts.incrementAndGet()
    proxied.faulty.enable()
  }

  /** At the moment the next commit to Bronze of `of` is about to take its version. */
  private def beforeItsVersion(of: Source)(action: => Unit): Unit =
    HookedLogStore.runAt(LogPoint.BeforeInsert, times = 1) { file =>
      file.toUri.getPath.startsWith(new Path(bronze(of).uri).toUri.getPath + "/_delta_log/")
    }(_ => action)

  /**
   * An instance that reaches the control plane through the proxy as well, and gives up on a
   * call to it within a second. The proxy is whole again afterwards.
   */
  private def throughTheProxy[T](body: ControlDatabase => T): T = {
    val control = ControlDatabase
      .open(
        profile.databases.control.copy(
          url = PostgresUrl.parse(proxied.faultyJdbcUrl(Database)).toOption.get,
          timeouts = Timeouts(connect = 1.second, statement = 1.second)
        ),
        SecretResolver(environment(pg).get)
      )
      .toOption
      .get
    try Using.resource(control)(body)
    finally whole()
  }

  private def retriesOf(outcome: Outcome): Option[Int] = runOf(outcome.run).counts.retries

  test(
    "a coordination database away for less than the LogStore waits is waited out in the commit"
  ) {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    // Well within the LogStore's own wait, which also covers the pool of its connections
    // making a new one once the database is back.
    beforeItsVersion(orders)(cutFor(300.millis))

    val done =
      try ingested(launch(orders))
      finally HookedLogStore.disarm()

    // The LogStore tried the INSERT again by itself: the run had nothing to try again.
    retriesOf(done) shouldBe None
    done.counts.retries shouldBe None
    rowsOf(orders).map(_._1) shouldBe Seq("1")
    assertLedger(orders)
  }

  test("a coordination database away for longer than that has the run try again, and commit") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    beforeItsVersion(orders)(cutFor(6.seconds))

    val done =
      try ingested(launch(orders))
      finally HookedLogStore.disarm()

    // The commit to Bronze failed after the LogStore's own wait, and the run tried again from
    // its batch: written once, under its number, by this run.
    done.batch shouldBe batchId(orders, 1)
    retriesOf(done).value should be >= 1
    done.counts.retries shouldBe retriesOf(done)
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"))
    runOf(done.run).status shouldBe RunStatus.Succeeded
    assertLedger(orders)
  }

  test("a failure that outlasts the budget ends the run in 3, saying what it tried") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    val short =
      Profile.Retries(budget = 4.seconds, firstBackoff = 200.millis, maxBackoff = 1.second)
    beforeItsVersion(orders)(cutFor(30.seconds))

    val started = System.nanoTime()
    val outcome =
      try launch(orders, service(tries = short))
      finally {
        HookedLogStore.disarm()
        whole()
      }
    val took = (System.nanoTime() - started).nanos

    val message = outcome match {
      case Outcome.Failed(_, FailureClass.Transient, said) => said
      case other => fail(s"the run did not fail as transient: $other")
    }
    message should include("The run tried again")
    message should include("and its budget of retries (retries.budget, 4s) is spent")
    retriesOf(outcome).value should be >= 1
    // Within the budget and the length of an attempt, from the first failure.
    took should be < 4.seconds + 15.seconds
    batchOf(batchId(orders, 1)).status shouldBe BatchStatus.Planned
    holderOf(orders) shouldBe None

    // With the database back, the next run commits the batch that was left.
    ingested(launch(orders)).batch shouldBe batchId(orders, 1)
    assertLedger(orders)
  }

  test("a control plane away as the run starts has the run try to take its lease again") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")

    val done = throughTheProxy { control =>
      cutFor(2.seconds)
      ingested(launch(orders, service(on = control)))
    }

    retriesOf(done).value should be >= 1
    rowsOf(orders).map(_._1) shouldBe Seq("1")
    assertLedger(orders)
  }

  test("a run that loses its lease while it waits to try again ends as one that lost it") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")

    // The control plane and the coordination database go before Bronze is written, and stay
    // away: the run tries again until it stops by its own count of its lease, 10 s on.
    val started = System.nanoTime()
    val outcome = throughTheProxy { control =>
      val cutOff = hookAt(FaultPoint.AfterCounts)(_ => cut())
      launch(orders, service(cutOff, on = control))
    }
    val took = (System.nanoTime() - started).nanos

    outcome match {
      case Outcome.Failed(_, FailureClass.Transient, message) =>
        message should include("lost the lease")
      case other => fail(s"the run did not end as one that lost its lease: $other")
    }
    // At its lease, and not at the end of its budget of 30 s.
    took should be < 25.seconds
    // It said nothing that reached the control plane; the next run takes its lease over.
    batchOf(batchId(orders, 1)).status shouldBe BatchStatus.Planned
    expire(orders)
    ingested(launch(orders)).batch shouldBe batchId(orders, 1)
    assertLedger(orders)
  }

  test("a failure in the transaction that ends a run is not tried again: its answer is P8.5's") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")

    // The rows are in Bronze, and the commit of the state cannot reach the control plane.
    val started = System.nanoTime()
    val outcome = throughTheProxy { control =>
      val cutOff = hookAt(FaultPoint.BeforeStateCommit)(_ => cut())
      launch(orders, service(cutOff, on = control))
    }
    val took = (System.nanoTime() - started).nanos

    outcome match {
      case Outcome.Failed(_, FailureClass.Transient, message) =>
        message should startWith("the run could not go on with its control plane: ")
      case other => fail(s"the run did not fail as transient: $other")
    }
    // Once: the transaction may have been committed with its answer lost.
    took should be < 10.seconds
    runOf(outcome.run).status shouldBe RunStatus.Running
    batchOf(batchId(orders, 1)).status shouldBe BatchStatus.Planned

    expire(orders)
    launch(orders) shouldBe a[Outcome.RolledForward]
    assertLedger(orders)
  }

  test("what running again does not mend is not tried again") {
    val refused = fileSource()
    drop(refused, "a.csv", "id,_run_id\n1,x\n")

    val started = System.nanoTime()
    val outcome = launch(refused)
    val took = (System.nanoTime() - started).nanos

    outcome shouldBe a[Outcome.Failed]
    outcome.asInstanceOf[Outcome.Failed].failure shouldBe FailureClass.Permanent
    retriesOf(outcome) shouldBe None
    took should be < 10.seconds
  }

  test("a retryable conflict of Delta has the run try again, counted apart (injected: P10)") {
    // No write of the engine meets one before the MERGE of Silver (P10, §20): the appends of
    // Bronze have nothing that another writer can invalidate. Delta's own exception is thrown
    // by a hook of the engine, once, where the batch is planned and nothing is written yet.
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    var thrown = false
    val conflicting = hookAt(FaultPoint.AfterCounts) { _ =>
      if (!thrown) {
        thrown = true
        throw new ConcurrentAppendException("files were added to the table by another writer")
      }
    }

    val done = ingested(launch(orders, service(conflicting)))

    thrown shouldBe true
    done.batch shouldBe batchId(orders, 1)
    val counts = runOf(done.run).counts
    counts.writeConflicts shouldBe Some(1)
    counts.retries shouldBe None
    rowsOf(orders).map(_._1) shouldBe Seq("1")
    assertLedger(orders)
  }
}
