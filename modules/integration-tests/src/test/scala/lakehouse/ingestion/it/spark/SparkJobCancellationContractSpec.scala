package lakehouse.ingestion.it.spark

import java.util.concurrent.Executors

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

import lakehouse.ingestion.core.control.RunId
import lakehouse.ingestion.engine.RunJobs
import lakehouse.ingestion.it.support.Pace.{done, interrupted, paced}
import lakehouse.ingestion.it.support.{HeardJobs, LocalSparkSuite, Pace}
import org.apache.spark.SparkException
import org.apache.spark.sql.delta.DeltaLog
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

/**
 * What Spark 4.1 does with the jobs of a thread when they are stopped by a tag: the facts that
 * `RunJobs` is built on, and with it what a run does the moment it loses its lease (ADR-0006).
 *
 * These are characterisation tests, as those of `DeltaSetTransactionContractSpec` are: what
 * they pin is Spark's and Delta's, asked through `RunJobs` as the engine asks it. An upgrade
 * that changes one of these facts fails here, with the name of the assumption that it breaks
 * (ADR-0014, upgrade procedure).
 *
 * The jobs that are stopped are slow for real: each of their records takes its time, and a job
 * that nobody stopped would take a minute.
 */
final class SparkJobCancellationContractSpec
    extends AnyFunSuite
    with Matchers
    with LocalSparkSuite
    with Eventually {

  import SparkJobCancellationContractSpec._

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(25, Millis))

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

  private def newRun(): RunJobs = new RunJobs(spark, RunId.generate())

  /** How each job that a run started ended, once all that has happened is heard of. */
  private def endsOf(run: RunJobs): Seq[Option[Boolean]] = {
    jobs.settle()
    jobs.tagged(run.tag).map(job => jobs.succeeded(job.id))
  }

  /** The jobs that `body` started, each with its tags. */
  private def startedBy(body: => Unit): Seq[HeardJobs.Started] = {
    jobs.settle()
    val before = jobs.started.size
    body
    jobs.settle()
    jobs.started.drop(before)
  }

  /** A query of Spark SQL whose every record takes `millis`: a minute of them, if not stopped. */
  private def slowQuery(pace: String, millis: Long = 5L, tasks: Int = 2): Long = {
    val session = spark
    import session.implicits._
    val records = (60.seconds.toMillis / millis).toInt * tasks
    spark.sparkContext
      .parallelize(0 until records, tasks)
      .map(paced(pace, millis))
      .toDF("id")
      .count()
  }

  private def newPace(): String = Pace.fresh()

  /** Waits until the tasks of a job are at work. */
  private def atWork(pace: String): Unit = eventually(done(pace) should be > 10L): Unit

  /** Waits for the first record of a slow pace, however long each takes. */
  private def atWorkAtAll(pace: String): Unit = eventually(Pace.started(pace) should be > 0L): Unit

  private def doneInTheNextSecond(pace: String): Long = Pace.doneInTheNextSecond(pace)

  // ---------------------------------------------------------------------------------------------
  // Which jobs are a run's
  // ---------------------------------------------------------------------------------------------

  test("every job that a thread starts inside a run has the tag of the run, and none after it") {
    val run = newRun()
    val path = tablePath("tagged")
    val session = spark
    import session.implicits._

    val inTheRun = startedBy {
      run.during {
        // What a run does: a read that Spark keeps, a count, a write to Delta, and a question
        // to the log of the table.
        val kept = (1 to 100).toDF("id").repartition(2).localCheckpoint(eager = true)
        kept.count() shouldBe 100L
        kept.write
          .format("delta")
          .mode("append")
          .option("txnAppId", "a")
          .option("txnVersion", 1L)
          .save(path)
        DeltaLog.forTable(spark, path).update().transactions shouldBe Map("a" -> 1L)
        spark.read.format("delta").load(path).count() shouldBe 100L: Unit
      }
    }
    inTheRun.size should be >= 4
    inTheRun.filterNot(_.tags.contains(run.tag)) shouldBe empty

    val after = startedBy(spark.range(10).repartition(2).count() shouldBe 10L: Unit)
    after.filter(_.tags.contains(run.tag)) shouldBe empty
  }

  test("the thread of a run is left as it was: its tags, and whether its tasks are interrupted") {
    val context = spark.sparkContext
    val run = newRun()

    context.getJobTags() shouldBe empty
    context.getLocalProperty(InterruptOnCancel) shouldBe null
    run.during {
      context.getJobTags() shouldBe Set(run.tag)
      context.getLocalProperty(InterruptOnCancel) shouldBe "false"
    }
    context.getJobTags() shouldBe empty
    context.getLocalProperty(InterruptOnCancel) shouldBe null

    // A thread that had something to say of its own has it back, also when the run fails.
    context.addJobTag("its-own")
    context.setInterruptOnCancel(true)
    try {
      an[IllegalStateException] should be thrownBy run.during(throw new IllegalStateException)
      context.getJobTags() shouldBe Set("its-own")
      context.getLocalProperty(InterruptOnCancel) shouldBe "true"
    } finally {
      context.clearJobTags()
      context.setLocalProperty(InterruptOnCancel, null)
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Stopping them
  // ---------------------------------------------------------------------------------------------

  test("the jobs of a run that are running are stopped: who started one is told why, at once") {
    val run = newRun()
    val pace = newPace()
    val work = Future(run.during(slowQuery(pace)))
    atWork(pace)

    val stoppedAt = System.nanoTime()
    run.stop("the lease of test.orders/bronze is lost")

    val failure = the[SparkException] thrownBy Await.result(work, 20.seconds)
    (System.nanoTime() - stoppedAt).nanos should be < 5.seconds
    failure.getCondition shouldBe "SPARK_JOB_CANCELLED"
    failure.getMessage should include("the lease of test.orders/bronze is lost")
    // Its tasks stop too, between two records: nothing goes on behind the error.
    eventually(doneInTheNextSecond(pace) shouldBe 0L)
    done(pace) should be < 2000L
    endsOf(run) shouldBe Seq(Some(false))
  }

  test("no task of a run is interrupted when its jobs are stopped, as Spark SQL would have it") {
    // Inside a run: the tasks are told to stop, and find out between two records.
    val (run, inARun) = (newRun(), newPace())
    val work = Future(run.during(slowQuery(inARun)))
    atWork(inARun)
    run.stop("stopped by the test")
    a[SparkException] should be thrownBy Await.result(work, 20.seconds)
    eventually(doneInTheNextSecond(inARun) shouldBe 0L)
    interrupted(inARun) shouldBe 0L

    // Outside one, the same query stopped the same way: Spark SQL has its tasks interrupted
    // in the middle of a record, which is what a run turns off.
    val (tag, byItself) = (s"by-itself-${java.util.UUID.randomUUID()}", newPace())
    val alone = Future {
      spark.sparkContext.addJobTag(tag)
      try slowQuery(byItself, millis = 200L)
      finally spark.sparkContext.removeJobTag(tag)
    }
    atWorkAtAll(byItself)
    spark.sparkContext.cancelJobsWithTag(tag, "stopped by the test")
    a[SparkException] should be thrownBy Await.result(alone, 20.seconds)
    eventually(interrupted(byItself) should be > 0L)
  }

  test("the jobs of another run of the process go on") {
    val (lost, lostPace) = (newRun(), newPace())
    val (other, otherPace) = (newRun(), newPace())
    // One task each: the session has two cores, and each job is to have one.
    val stopped = Future(lost.during(slowQuery(lostPace, tasks = 1)))
    val session = spark
    import session.implicits._
    val goesOn = Future(other.during {
      spark.sparkContext.parallelize(0 until 400, 1).map(paced(otherPace, 5L)).toDF("id").count()
    })
    atWork(lostPace)
    atWork(otherPace)

    lost.stop("stopped by the test")

    a[SparkException] should be thrownBy Await.result(stopped, 20.seconds)
    Await.result(goesOn, 30.seconds) shouldBe 400L
    done(otherPace) shouldBe 400L
  }

  test("a job that a run starts after its jobs were stopped runs: stopping is of what is running") {
    val run = newRun()
    run.stop("stopped by the test, with nothing running")

    // Which is why a run asks for its lease before each step that starts a job (`verify`).
    run.during(spark.range(1000).repartition(2).count()) shouldBe 1000L
    endsOf(run).distinct shouldBe Seq(Some(true))

    // And stopping the jobs of a run that is over changes nothing, for it or for anybody.
    run.stop("stopped by the test, after the run")
    spark.range(1000).repartition(2).count() shouldBe 1000L
  }
}

object SparkJobCancellationContractSpec {

  /** The property of a thread that says whether the tasks of its jobs are interrupted. */
  val InterruptOnCancel = "spark.job.interruptOnCancel"
}
