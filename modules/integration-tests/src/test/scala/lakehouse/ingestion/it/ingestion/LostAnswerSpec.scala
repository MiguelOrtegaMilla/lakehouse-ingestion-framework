package lakehouse.ingestion.it.ingestion

import java.time.{Clock, ZoneOffset}

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import eu.rekawek.toxiproxy.model.ToxicDirection
import io.circe.Json
import lakehouse.ingestion.core.config._
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.connector._
import lakehouse.ingestion.engine.files.{FileSourceConnector, Landing}
import lakehouse.ingestion.engine.operator.OperatorActions
import lakehouse.ingestion.engine.{FaultPoint, IngestionService}
import lakehouse.ingestion.it.support.{CapturedLogs, Died}
import org.apache.logging.log4j.Level
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The transaction that ends a run, settled when it gives no answer (ADR-0022 §6), against a real
 * PostgreSQL behind a proxy that is cut for real. The control plane is reached through the proxy,
 * and the coordination database is not: what breaks is the end of a run, and nothing before it.
 *
 * An answer is lost as `PostgresCoordinationFaultSpec` loses one: the replies of the database are
 * held back, what the transaction writes is watched for on a path that the proxy does not touch,
 * and the proxy is cut once it is there, before the answer gets through. A transaction that does
 * not reach the database is cut by the proxy alone. Each test ends with the ledger.
 */
final class LostAnswerSpec
    extends AnyFunSuite
    with Matchers
    with OptionValues
    with ProxiedIngestionSuite {

  override protected def timing: LeaseTiming = LeaseTiming(12.seconds)

  override protected def retries: Profile.Retries =
    Profile.Retries(budget = 30.seconds, firstBackoff = 200.millis, maxBackoff = 1.second)

  /** How long each reply of the database is held back: the time there is to cut it off. */
  private val Held = 700.millis

  /** The name of what holds the replies back, in the proxy. */
  private val HeldBack = "held-back-replies"

  /**
   * A control plane that waits for a reply for longer than one is held back, and for a
   * connection for as long as the several replies that its login takes.
   */
  private val Patient = Timeouts(connect = 5.seconds, statement = 2.seconds)

  private val asked = OperatorRequest.of("ops.oncall", "the plan cannot be run").toOption.get

  // What the retries of a run log as they settle the transaction that ends it.
  private val Retries = "lakehouse.ingestion.engine.RunRetries"
  private val CommittedLost = "finds the transaction that ends it committed, and its answer lost"
  private val MadeAgain = "finds the transaction that ends it not committed, and makes it again"

  /** What the retries of the run logged while `body` ran, with what `body` gave. */
  private def settling[T](body: => T): (T, Seq[String]) = {
    val (value, logged) = CapturedLogs.under(Retries, Level.INFO)(body)
    (value, logged.map(_.message))
  }

  @tailrec
  private def eventually(deadline: Deadline)(condition: => Boolean): Unit =
    if (!condition) {
      if (deadline.isOverdue()) fail("the condition did not become true in time")
      Thread.sleep(25)
      eventually(deadline)(condition)
    }

  /**
   * From now on, the replies through the proxy are held back. Once `committed` is true, as the
   * suite reads it on its own path, the proxy is cut before the answer of the transaction that
   * made it true gets through, and is whole again a while after, with nothing held back: the run
   * reads its own row while the proxy is cut, and again until it is whole.
   */
  private def loseTheAnswer(committed: => Boolean): Unit = {
    val held = proxied.faulty.toxics().latency(HeldBack, ToxicDirection.DOWNSTREAM, Held.toMillis)
    Future {
      try eventually(30.seconds.fromNow)(committed)
      finally {
        cutFor(1500.millis)
        held.remove()
      }
    }: Unit
  }

  /** Nothing is held back by the proxy any more, whatever a test left. */
  private def heldBackNoMore(): Unit =
    proxied.faulty.toxics().getAll.asScala.foreach(_.remove())

  test("a run whose commit is made with its answer lost says that it ended well") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")

    val (outcome, logged) =
      try
        settling(throughTheProxy(Patient) { control =>
          val losing = hookAt(FaultPoint.BeforeStateCommit) { batch =>
            loseTheAnswer(batchOf(batch.id).status == BatchStatus.Committed)
          }
          launch(orders, service(losing, on = control))
        })
      finally heldBackNoMore()

    val done = ingested(outcome)
    // The answer was lost, and the row said what had happened.
    logged.filter(_.contains(CommittedLost)) should have size 1
    done.batch shouldBe batchId(orders, 1)
    runOf(done.run).status shouldBe RunStatus.Succeeded
    // Nothing was made again: the row is the one that the commit wrote.
    runOf(done.run).counts.retries shouldBe None
    done.counts.retries shouldBe None
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"))
    holderOf(orders) shouldBe None

    // Once, and not again: the next run has nothing new.
    launch(orders) shouldBe a[Outcome.NothingNew]
    assertLedger(orders)
  }

  test("an abandon-batch whose transaction is made with its answer lost says that it was done") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    intercept[Died](launch(orders, service(hookAt(FaultPoint.AfterPlan)(_ => throw new Died))))
    expire(orders)
    val planned = batchId(orders, 1)

    val (outcome, logged) =
      try
        settling(throughTheProxy(Patient) { control =>
          val actions =
            new OperatorActions(control, spark, lakeRoot, timing, retries, ConnectorKind.BuiltIn)
          // An action has no point of its own to break it at: its replies are held back from
          // its start, and only the control plane is behind the proxy.
          loseTheAnswer(batchOf(planned).status == BatchStatus.Abandoned)
          actions.abandon(orders, versionOf(orders), planned.seq, asked)
        })
      finally heldBackNoMore()

    outcome shouldBe a[OperatorActions.Outcome.Done]
    logged.filter(_.contains(CommittedLost)) should have size 1
    batchOf(planned).status shouldBe BatchStatus.Abandoned
    runOf(outcome.run).status shouldBe RunStatus.Succeeded
    holderOf(orders) shouldBe None

    // The next run plans anew, and takes the file in the next batch.
    ingested(launch(orders)).batch shouldBe batchId(orders, 2)
    rowsOf(orders) shouldBe Seq(("1", 2L, "a.csv"))
    assertLedger(orders)
  }

  test("a commit cut before it reaches the database is made again, as a retry of the run") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")

    val (outcome, logged) = settling(throughTheProxy() { control =>
      val cutOff = hookAt(FaultPoint.BeforeStateCommit)(_ => cutFor(3.seconds))
      launch(orders, service(cutOff, on = control))
    })

    val done = ingested(outcome)
    logged.exists(_.contains(MadeAgain)) shouldBe true
    logged.exists(_.contains(CommittedLost)) shouldBe false
    // What the run says it made again is what its row has.
    runOf(done.run).counts.retries.value should be >= 1
    done.counts.retries shouldBe runOf(done.run).counts.retries
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"))
    assertLedger(orders)
  }

  test("a run that cannot learn within its budget how its commit went ends in 3, and is found") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    val short =
      Profile.Retries(budget = 4.seconds, firstBackoff = 200.millis, maxBackoff = 1.second)

    val started = System.nanoTime()
    val outcome = throughTheProxy() { control =>
      val cutOff = hookAt(FaultPoint.BeforeStateCommit)(_ => cut())
      launch(orders, service(cutOff, on = control, tries = short))
    }
    val took = (System.nanoTime() - started).nanos

    val message = outcome match {
      case Outcome.Failed(_, FailureClass.Transient, said) => said
      case other => fail(s"the run did not fail as transient: $other")
    }
    message should startWith(
      s"the run ${outcome.run} could not learn whether the transaction that ends it was " +
        "committed: "
    )
    message should include("and its budget of retries (retries.budget, 4s) is spent")
    // Within the budget and the length of an attempt, from the failure of the commit.
    took should be < 4.seconds + 15.seconds
    // It said nothing more: the commit left nothing, and the lease of the run runs out.
    runOf(outcome.run).status shouldBe RunStatus.Running
    batchOf(batchId(orders, 1)).status shouldBe BatchStatus.Planned
    holderOf(orders) shouldBe Some(outcome.run)

    // The next run finds the batch, and commits it as its table says.
    expire(orders)
    launch(orders) shouldBe a[Outcome.RolledForward]
    assertLedger(orders)
  }

  test(
    "a run that found nothing new and whose end is cut before it reaches the database ends well"
  ) {
    // Nothing is dropped: the source has nothing new. The control plane is cut as the plan
    // finds so, and the run ends next.
    val orders = fileSource()
    val cuttingAsItPlans =
      new PlannedThen(new FileSourceConnector(new Landing(landingRoot)), () => cutFor(3.seconds))

    val (outcome, logged) = settling(throughTheProxy() { control =>
      val runs = new IngestionService(
        control,
        ConnectorRegistry(cuttingAsItPlans),
        spark,
        lakeRoot,
        timing,
        retries,
        Clock.fixed(Now, ZoneOffset.UTC)
      )
      launch(orders, runs)
    })

    outcome shouldBe a[Outcome.NothingNew]
    logged.exists(_.contains(MadeAgain)) shouldBe true
    runOf(outcome.run).status shouldBe RunStatus.Succeeded
    runOf(outcome.run).counts.retries.value should be >= 1
    holderOf(orders) shouldBe None
    assertLedger(orders)
  }

  /** The connector of files, which does `andThen` once it has made a plan, whatever it found. */
  final private class PlannedThen(files: FileSourceConnector, andThen: () => Unit)
      extends SourceConnector[FileConnectorConfig] {

    override def kind: ConnectorKind = files.kind

    override def plan(
        source: Source,
        config: FileConnectorConfig,
        context: PlanContext
    ): Option[Json] = {
      val made = files.plan(source, config, context)
      andThen()
      made
    }

    override def read(
        source: Source,
        config: FileConnectorConfig,
        plan: Json,
        context: ReadContext
    ): ReadResult = files.read(source, config, plan, context)

    override def progress(source: Source, config: FileConnectorConfig, plan: Json): Progress =
      files.progress(source, config, plan)

    override def more(source: Source, config: FileConnectorConfig, plan: Json): Option[Json] =
      files.more(source, config, plan)
  }
}
