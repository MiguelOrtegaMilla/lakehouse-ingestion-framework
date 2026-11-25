package lakehouse.ingestion.it.ingestion

import java.time.{Clock, ZoneOffset}
import java.util.concurrent.Executors

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Try, Using}

import cats.syntax.all._
import io.circe.Json
import lakehouse.ingestion.core.config.Read.{Obj, Result}
import lakehouse.ingestion.core.config._
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.connector._
import lakehouse.ingestion.engine.control.{ControlDatabase, LeasedRun}
import lakehouse.ingestion.engine.failure.RunFailedException
import lakehouse.ingestion.engine.{FaultHooks, FaultPoint, IngestionService, RunJobs}
import lakehouse.ingestion.it.support.CapturedLogs.Logged
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.it.support.{
  BootstrappedPostgres,
  CapturedLogs,
  HeardJobs,
  Pace,
  ProxiedPostgres
}
import org.apache.logging.log4j.Level
import org.apache.spark.{SparkException, SparkThrowable}
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

/**
 * A run that loses its lease stops working (P5.11, ADR-0006): the job of Spark that it is in
 * the middle of is stopped at that moment, it takes no step after it that starts a job or
 * writes a table, and it ends as a transient failure, with what it left for the next run to
 * finish.
 *
 * The lease is lost for real, both ways that it can be:
 *
 *  - ''by the count of the run'': the run reaches its control plane through a proxy, which the
 *    test cuts. Nothing is renewed, and the count of the run runs out before its lease does;
 *  - ''to another run'': the lease expires in the database and another run takes it, and the
 *    run is told at its next renewal.
 *
 * And what the run is in the middle of is a job that is slow for real, that nobody else stops:
 * a read of records that take a minute between them, or, at a fault point, a job of the run's
 * own thread that takes as long. A run that went on would be seen to.
 *
 * The leases last twelve seconds, as in `LeaseFencingSpec`: renewed every four, and taken as
 * good by their run for ten. The store that takes the versions of the tables is reached
 * another way, which stays whole: what is cut is the control plane, and nothing else.
 */
final class LeaseLossSpec extends AnyFunSuite with Matchers with IngestionSuite with Eventually {

  import LeaseLossSpec._

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(25, Millis))

  private lazy val proxied = ProxiedPostgres.start()
  override protected lazy val pg: BootstrappedPostgres = proxied.postgres
  override protected def stopServers(): Unit = proxied.stop()

  override protected def timing: LeaseTiming = LeaseTiming(12.seconds)

  private val threads = Executors.newCachedThreadPool()
  implicit private val onThreads: ExecutionContext = ExecutionContext.fromExecutor(threads)

  private var heard = Option.empty[HeardJobs]
  private def jobs: HeardJobs = heard.getOrElse(throw new IllegalStateException("no session"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    heard = Some(new HeardJobs(spark))
  }

  override def afterAll(): Unit =
    try threads.shutdownNow(): Unit
    finally super.afterAll()

  // ---------------------------------------------------------------------------------------------
  // A control plane that can be cut, and a run that is in the middle of something
  // ---------------------------------------------------------------------------------------------

  /**
   * An instance that reaches the control plane through the proxy that the test breaks, and
   * gives up on a call to it within a second. The proxy is whole again afterwards.
   */
  private def throughTheProxy[T](body: ControlDatabase => T): T = {
    val control = ControlDatabase
      .open(
        profile.databases.control.copy(
          url = PostgresUrl.parse(proxied.faultyJdbcUrl(Database)).value,
          timeouts = Timeouts(connect = 1.second, statement = 1.second)
        ),
        SecretResolver(environment(pg).get)
      )
      .value
    try Using.resource(control)(body)
    finally proxied.faulty.enable()
  }

  /** An instance of its own, that reaches the control plane as any other does. */
  private def anInstance[T](body: ControlDatabase => T): T = Using.resource(anotherInstance())(body)

  /** A source whose records are a range of numbers, read at the pace of whoever reads it. */
  private def numbersSource(): Source = {
    val id = newSourceId().value
    Source
      .load(
        s"$id.yaml",
        s"""id: $id
           |connector: { type: paced, rows: $Rows }
           |load: { mode: full }
           |targets: { bronze: { table: bronze/${id.replace('.', '/')} } }
           |""".stripMargin,
        Seq(PacedKind)
      )
      .value
      .value
  }

  /**
   * A service that reads the numbers of a source at a pace.
   *
   * @param millis what each record takes: at `Slow`, the read of a source takes a minute
   */
  private def reader(pace: String, millis: Long, on: ControlDatabase = database): IngestionService =
    new IngestionService(
      on,
      ConnectorRegistry(new PacedConnector(pace, millis)),
      spark,
      lakeRoot,
      timing,
      Clock.fixed(Now, ZoneOffset.UTC)
    )

  /** The rows of Bronze of a source of numbers: how many, how many of them differ, and by whom. */
  private def numbersIn(of: Source): (Long, Long, Seq[String]) = {
    val rows = spark.read.format("delta").load(bronze(of).uri)
    val writers = rows.select("_run_id").distinct().collect().map(_.getString(0)).toSeq
    (rows.count(), rows.select("id").distinct().count(), writers)
  }

  /**
   * What a run is in the middle of when its lease is lost, for a run that is at a fault point:
   * a job of the run's own thread that takes a minute, unless it is stopped.
   *
   * @return what the job failed with; none, if nobody stopped it
   */
  private def workUntilStopped(): Option[Throwable] = {
    val pace = Pace.fresh()
    Try(
      spark.sparkContext.parallelize(0 until Rows, 2).map(Pace.paced(pace, Slow)).count()
    ).failed.toOption
  }

  /** The job that a run was in the middle of was stopped for its lease, and for nothing else. */
  private def stoppedForItsLease(failure: Option[Throwable], of: Source): Unit = {
    val stopped = failure.value
    stopped shouldBe a[SparkException]
    stopped.asInstanceOf[SparkException].getCondition shouldBe JobCancelled
    stopped.getMessage should include(
      s"lost the lease of ${LeaseKey.of(of.id, Layer.Bronze)}"
    ): Unit
  }

  /** The run ended as one that lost its lease: a failure that passes, which says so. */
  private def lostItsLease(outcome: Outcome, of: Source): String = outcome match {
    case Outcome.Failed(_, failure, message) =>
      failure shouldBe FailureClass.Transient
      message should include(s"lost the lease of ${LeaseKey.of(of.id, Layer.Bronze)}")
      message
    case other => fail(s"the run did not lose its lease: $other")
  }

  /** How each job that a run started ended, in the order they started. */
  private def jobsOf(run: RunId): Seq[Option[Boolean]] = {
    jobs.settle()
    jobs.tagged(s"${RunJobs.TagPrefix}$run").map(job => jobs.succeeded(job.id))
  }

  /** What the service logged of a failure, with that failure whole. */
  private def loggedWith(logged: Seq[Logged])(is: Throwable => Boolean): Seq[Logged] =
    logged.filter(_.thrown.exists(failure => causes(failure).exists(is)))

  private def jobStopped(failure: Throwable): Boolean = failure match {
    case spark: SparkThrowable => spark.getCondition == JobCancelled
    case _ => false
  }

  // ---------------------------------------------------------------------------------------------
  // In the middle of a read
  // ---------------------------------------------------------------------------------------------

  test("a run that is cut off from its control plane stops its read before its lease expires") {
    val numbers = numbersSource()
    val first = batchId(numbers, 1)
    val (slow, fast) = (Pace.fresh(), Pace.fresh())

    throughTheProxy { control =>
      val (outcome, logged) = CapturedLogs.of(classOf[IngestionService]) {
        val running = Future(launch(numbers, reader(slow, Slow, on = control)))
        eventually(Pace.done(slow) should be > 10L)
        proxied.faulty.disable()
        Await.result(running, 40.seconds)
      }

      // It stopped by its own count, with nobody to tell it, and long before its read ended.
      lostItsLease(outcome, numbers) should include("it was not renewed in time")
      Pace.done(slow) should be < Rows.toLong
      // The job that it was in was stopped, and its tasks with it, none of them interrupted.
      jobsOf(outcome.run).last shouldBe Some(false)
      eventually(Pace.doneInTheNextSecond(slow) shouldBe 0L)
      Pace.interrupted(slow) shouldBe 0L
      // What the read failed with is in the log, whole: a job that was stopped, here.
      val said = loggedWith(logged)(jobStopped)
      said.map(_.level) shouldBe Seq(Level.ERROR)
      said.head.message should include(s"the run ${outcome.run} failed after it lost the lease")

      // Nothing was written. The control plane could not be told: the run is as a process
      // that died leaves it, with its batch planned and a lease that is still to expire.
      isThere(bronze(numbers)) shouldBe false
      batchOf(first).status shouldBe BatchStatus.Planned
      runOf(outcome.run).status shouldBe RunStatus.Running
      holderOf(numbers) shouldBe Some(outcome.run)

      // The network is back. The next run waits for the lease to expire, and finishes the batch.
      proxied.faulty.enable()
      awaitExpiry(LeaseKey.of(numbers.id, Layer.Bronze))
      val next = ingested(launch(numbers, reader(fast, 0L)))
      next.batch shouldBe first
      runOf(outcome.run).status shouldBe RunStatus.Abandoned
      numbersIn(numbers) shouldBe ((Rows.toLong, Rows.toLong, Seq(next.run.toString)))
    }
  }

  test("a run whose lease is taken while it reads is stopped at its next renewal") {
    val numbers = numbersSource()
    val first = batchId(numbers, 1)
    val (slow, fast) = (Pace.fresh(), Pace.fresh())

    anInstance { own =>
      val (outcome, logged) = CapturedLogs.of(classOf[IngestionService]) {
        val running = Future(launch(numbers, reader(slow, Slow, on = own)))
        eventually(Pace.done(slow) should be > 10L)

        // Its lease runs out in the database, and another run takes it and finishes the batch
        // while this one is still reading it.
        expire(numbers)
        val successor = ingested(launch(numbers, reader(fast, 0L)))
        successor.batch shouldBe first
        numbersIn(numbers) shouldBe ((Rows.toLong, Rows.toLong, Seq(successor.run.toString)))

        Await.result(running, 30.seconds)
      }

      // It is told when it asks for more time, and stops there: it does not read on to the end.
      lostItsLease(outcome, numbers) should include("another run holds it")
      Pace.done(slow) should be < Rows.toLong
      jobsOf(outcome.run).last shouldBe Some(false)
      eventually(Pace.doneInTheNextSecond(slow) shouldBe 0L)
      loggedWith(logged)(jobStopped).map(_.level) shouldBe Seq(Level.ERROR)

      runOf(outcome.run).status shouldBe RunStatus.Abandoned
      batchOf(first).status shouldBe BatchStatus.Committed
      numbersIn(numbers)._1 shouldBe Rows.toLong
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Between two steps
  // ---------------------------------------------------------------------------------------------

  /**
   * At a fault point of a run, its lease runs out in the database and another run takes it,
   * which announces nothing to the LogStore: the epoch of the first is still the one in force
   * for its tables, and nothing but the run itself keeps it from writing them. The run then
   * works until it is stopped.
   */
  final private class TakenAt(point: FaultPoint, of: Source, andThen: () => Unit = () => ()) {
    @volatile var taker = Option.empty[LeasedRun]
    @volatile var stoppedWith = Option.empty[Throwable]
    @volatile var reads = 0

    val hooks: FaultHooks = hookAt(point) { _ =>
      expire(of)
      taker = Some(LeasedRun.open(database, starting(of), versionOf(of), timing).value)
      stoppedWith = workUntilStopped()
      andThen()
    }

    def service(on: ControlDatabase): IngestionService =
      LeaseLossSpec.this.service(hooks, afterRead = () => reads += 1, on = on)

    /** The run that took the lease has the batch to finish, and lets the lease go. */
    def release(batch: BatchId): Unit = {
      val holder = taker.value
      holder.planned().value.id shouldBe batch
      holder.finish(
        RunEnd.Failed(FailureClass.Transient, "stopped by the test")
      ) shouldBe true: Unit
    }
  }

  Seq(
    FaultPoint.AfterPlan,
    FaultPoint.AfterRead,
    FaultPoint.AfterCounts,
    FaultPoint.AfterQuarantineWrite
  ).foreach { point =>
    test(s"a run that lost its lease by $point takes no step after it") {
      val orders = fileSource(rejected = Some("0.5"))
      drop(orders, "a.csv", "id,amount\n1,2\n3\n")
      val first = batchId(orders, 1)
      val taken = new TakenAt(point, orders)

      val outcome = anInstance(own => launch(orders, taken.service(own)))

      stoppedForItsLease(taken.stoppedWith, orders)
      lostItsLease(outcome, orders) should include("another run holds it")
      // The job that was stopped is the last that the run started.
      jobsOf(outcome.run).last shouldBe Some(false)
      // And what the steps before it left is all that there is.
      point match {
        case FaultPoint.AfterPlan =>
          taken.reads shouldBe 0
          isThere(quarantine(orders)) shouldBe false
        // After its read, and after its counts, which are the last jobs before a table is
        // written: the record that was rejected is counted, and is not in quarantine.
        case FaultPoint.AfterRead | FaultPoint.AfterCounts =>
          taken.reads shouldBe 1
          isThere(quarantine(orders)) shouldBe false
        case _ =>
          delta.lastVersion(quarantine(orders), first.app) shouldBe Some(1L)
      }
      tableIsThere(orders) shouldBe false
      runOf(outcome.run).status shouldBe RunStatus.Abandoned
      batchOf(first).status shouldBe BatchStatus.Planned

      // The batch is the next run's, which writes each of its records once.
      taken.release(first)
      ingested(launch(orders)).batch shouldBe first
      rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"))
      quarantinedOf(orders) shouldBe Seq((1L, "a.csv", "3"))
    }
  }

  test("a run whose lease ran out says that it failed, if the control plane has it still") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    val first = batchId(orders, 1)
    @volatile var stoppedWith = Option.empty[Throwable]

    throughTheProxy { control =>
      // The run is cut off after its read, until its count runs out. The network is back by
      // the time it has something to say, and nobody took its lease meanwhile.
      val cutOff = hookAt(FaultPoint.AfterRead) { _ =>
        proxied.faulty.disable()
        stoppedWith = workUntilStopped()
        proxied.faulty.enable()
      }

      val outcome = launch(orders, service(cutOff, on = control))

      stoppedForItsLease(stoppedWith, orders)
      val message = lostItsLease(outcome, orders)
      message should include("it was not renewed in time")
      tableIsThere(orders) shouldBe false
      // It says itself how it ended, and gives its lease up.
      val ended = runOf(outcome.run)
      ended.status shouldBe RunStatus.Failed
      ended.failure.value shouldBe RunFailure(FailureClass.Transient, message)
      holderOf(orders) shouldBe None
      batchOf(first).status shouldBe BatchStatus.Planned

      // So the next run does not wait for a lease to expire: it finishes the batch at once.
      ingested(launch(orders)).batch shouldBe first
      rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"))
    }
  }

  test("a run that failed for a reason of its own says so, though its control plane is not told") {
    // For good: a column of the engine's in the data, which the read finds.
    val refused = fileSource()
    drop(refused, "a.csv", "id,_run_id\n1,x\n")
    // For its data: more rejected records than the source allows, once they are in quarantine.
    val rejecting = fileSource()
    drop(rejecting, "a.csv", "id\n1\n1,2\n")

    Seq(
      (refused, FaultPoint.AfterPlan, FailureClass.Permanent, "_run_id"),
      (rejecting, FaultPoint.AfterQuarantineWrite, FailureClass.Data, "could not be parsed")
    ).foreach { case (source, point, failure, said) =>
      val first = batchId(source, 1)

      val outcome = throughTheProxy { control =>
        // The run is cut off from its control plane before it fails, and stays so: what it
        // has to say of how it ended reaches nobody. Its lease is still good by its count.
        val cutOff = hookAt(point)(_ => proxied.faulty.disable())
        launch(source, service(cutOff, on = control))
      }

      // What the process reports is what the run failed with, and not that nobody was told.
      withClue(s"$failure: ") {
        outcome match {
          case Outcome.Failed(_, reported, message) =>
            reported shouldBe failure
            message should include(said)
          case other => fail(s"the run did not fail with its class: $other")
        }
        // The control plane has it as a process that died left it.
        runOf(outcome.run).status shouldBe RunStatus.Running
        holderOf(source) shouldBe Some(outcome.run)
        batchOf(first).status shouldBe BatchStatus.Planned

        // The run that takes its lease, once it expires, ends it and fails as it did: this
        // time the control plane is told.
        expire(source)
        val next = launch(source)
        runOf(outcome.run).status shouldBe RunStatus.Abandoned
        runOf(next.run).failure.value.failure shouldBe failure
        holderOf(source) shouldBe None
      }
    }
    // The records that were rejected are in quarantine once, for the two runs that found them.
    quarantinedOf(rejecting) shouldBe Seq((1L, "a.csv", "1,2"))
  }

  test(
    "a run whose count ran out with its data written commits its batch, if its lease is its own"
  ) {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    val first = batchId(orders, 1)
    @volatile var stoppedWith = Option.empty[Throwable]

    throughTheProxy { control =>
      // Its rows are in Bronze when it is cut off, and its count runs out. What is left is the
      // commit of its state, which asks the database, and the database has the lease as its own.
      val cutOff = hookAt(FaultPoint.AfterBronzeCommit) { _ =>
        proxied.faulty.disable()
        stoppedWith = workUntilStopped()
        proxied.faulty.enable()
      }

      val done = ingested(launch(orders, service(cutOff, on = control)))

      stoppedForItsLease(stoppedWith, orders)
      done.batch shouldBe first
      runOf(done.run).status shouldBe RunStatus.Succeeded
      batchOf(first).status shouldBe BatchStatus.Committed
      holderOf(orders) shouldBe None
      rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"))
    }
  }

  // ---------------------------------------------------------------------------------------------
  // What a run fails with once its lease is lost
  // ---------------------------------------------------------------------------------------------

  Seq[(String, () => RuntimeException)](
    "a mistake of the application" -> (() => new IllegalStateException("a mistake of its own")),
    "a failure that says its class" -> (() => RunFailedException.permanent("it does not pass"))
  ).foreach { case (what, failure) =>
    test(
      s"$what in a run that lost its lease: the run ends as one, and the failure is logged whole"
    ) {
      val orders = fileSource()
      drop(orders, "a.csv", "id\n1\n")
      val first = batchId(orders, 1)
      val thrown = failure()
      val taken = new TakenAt(FaultPoint.AfterRead, orders, andThen = () => throw thrown)

      val (outcome, logged) = CapturedLogs.of(classOf[IngestionService]) {
        anInstance(own => launch(orders, taken.service(own)))
      }

      stoppedForItsLease(taken.stoppedWith, orders)
      // Not what it failed with, which a run without its lease has no say in.
      lostItsLease(outcome, orders) should include("another run holds it")
      runOf(outcome.run).status shouldBe RunStatus.Abandoned
      // And not lost either: it is there for whoever looks into it, with its stack.
      val said = loggedWith(logged)(_ eq thrown)
      said.map(_.level) shouldBe Seq(Level.ERROR)
      said.head.message should include(s"the run ${outcome.run} failed after it lost the lease")
      said.head.message should include("another run holds it")

      taken.release(first)
      ingested(launch(orders)).batch shouldBe first
      rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"))
    }
  }
}

object LeaseLossSpec {

  /** What a record takes, for a read that is to be stopped. */
  val Slow = 5L

  /** A minute of records at that pace, in two tasks. */
  val Rows = 24000

  /** The condition of Spark's error for a job that was stopped. */
  val JobCancelled = "SPARK_JOB_CANCELLED"

  /** A failure and its causes, as far as they go. */
  def causes(failure: Throwable): Seq[Throwable] =
    Iterator.iterate(failure)(_.getCause).takeWhile(_ != null).take(64).toSeq

  /** A source of numbers: `rows` of them in each batch, from where the last one left off. */
  final case class Paced(rows: Int) extends ConnectorConfig {
    override def kind: String = PacedKind.name
    override def canonical: Json = Json.obj("rows" -> Json.fromInt(rows))
    override def origin: Seq[String] = Nil
  }

  object PacedKind extends ConnectorKind {
    override val name = "paced"
    override val loads: Set[Load.Mode] = Set(Load.Mode.Full)
    override def read(section: Obj): Result[ConnectorConfig] =
      section.orElse("rows", 1)(_.integer(1, 1000000)).map(Paced(_))
  }

  /**
   * Reads the numbers of a plan at a pace, and keeps them apart from the source as the
   * connector of files does: the read is a job of Spark, inside `read`, that takes as long as
   * its records do.
   *
   * @param pace what the tasks of its reads count under (`Pace`)
   * @param millis what each record takes
   */
  final class PacedConnector(pace: String, millis: Long) extends SourceConnector[Paced] {

    override val kind: ConnectorKind = PacedKind

    override def plan(source: Source, config: Paced, context: PlanContext): Option[Json] = {
      val from = context.watermark.flatMap(_.asNumber).flatMap(_.toInt).getOrElse(0)
      Some(
        Json.obj(
          "version" -> Json.fromInt(1),
          "from" -> Json.fromInt(from),
          "to" -> Json.fromInt(from + config.rows)
        )
      )
    }

    override def read(
        source: Source,
        config: Paced,
        plan: Json,
        context: ReadContext
    ): ReadResult = {
      val (from, to) = rangeOf(source, plan)
      val session = context.spark
      import session.implicits._
      val numbers = session.sparkContext.parallelize(from until to, 2).map(Pace.paced(pace, millis))
      ReadResult.Kept(numbers.toDF("id").localCheckpoint(eager = true), unparsed = None)
    }

    override def progress(source: Source, config: Paced, plan: Json): Progress =
      Progress(watermark = Some(Json.fromInt(rangeOf(source, plan)._2)), files = None)

    private def rangeOf(source: Source, plan: Json): (Int, Int) = {
      val at = plan.hcursor
      val range = (
        at.get[Int]("version").toOption.filter(_ == 1),
        at.get[Int]("from").toOption,
        at.get[Int]("to").toOption
      ).mapN((_, from, to) => (from, to))
      range.getOrElse(
        throw RunFailedException.permanent(s"the plan of ${source.id} is not of version 1")
      )
    }
  }
}
