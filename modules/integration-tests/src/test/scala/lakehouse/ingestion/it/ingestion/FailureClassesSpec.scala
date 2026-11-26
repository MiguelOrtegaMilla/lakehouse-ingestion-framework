package lakehouse.ingestion.it.ingestion

import java.nio.file.{Files, Paths}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.Using

import io.delta.storage.HDFSLogStore
import lakehouse.ingestion.core.config._
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.control.{ControlDatabase, Runs}
import lakehouse.ingestion.engine.operator.OperatorActions
import lakehouse.ingestion.engine.FaultPoint
import lakehouse.ingestion.it.logstore.CoordinatedDeltaSuite.EmptyCommit
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.it.support.{BootstrappedPostgres, ProxiedPostgres}
import lakehouse.ingestion.logstore.{FaultPoint => LogPoint, HookedLogStore, PostgresLogStore}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.DeltaLog
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * A failure that reaches the end of a run, from the LogStore, from Delta or from the control
 * plane, ends the run with its class (ADR-0022 §2), on real files, real Delta on the coordinated
 * LogStore and a real control plane, each broken for real: a commit that another process made
 * without the LogStore, a commit gone from the log of a table, the control plane cut off by a
 * proxy, a privilege taken away, and another writer that changes a table under a write. Each
 * test then has the next run finish what the failed one left, and ends with the ledger.
 */
final class FailureClassesSpec
    extends AnyFunSuite
    with Matchers
    with OptionValues
    with IngestionSuite {

  private lazy val proxied = ProxiedPostgres.start()
  override protected lazy val pg: BootstrappedPostgres = proxied.postgres
  override protected def stopServers(): Unit = proxied.stop()

  // The commits of the session go through the LogStore behind hooks of the tests, which do
  // nothing until a test arms them.
  override protected def sparkConf: Map[String, String] =
    super.sparkConf.updated("spark.delta.logStore.file.impl", classOf[HookedLogStore].getName)

  private val threads = Executors.newCachedThreadPool()
  implicit private val onThreads: ExecutionContext = ExecutionContext.fromExecutor(threads)

  override def afterAll(): Unit =
    try threads.shutdownNow(): Unit
    finally super.afterAll()

  /**
   * An instance that reaches the control plane through the proxy that the test breaks, and
   * gives up on a call to it within a second. The proxy is whole again afterwards.
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
    finally proxied.faulty.enable()
  }

  private def failed(outcome: Outcome, as: FailureClass): String = outcome match {
    case Outcome.Failed(_, failure, message) if failure == as => message
    case other => fail(s"the run did not fail as $as: $other")
  }

  /** The batch that a run finished: one that it wrote, or one that it rolled forward. */
  private def finished(outcome: Outcome): BatchId = outcome match {
    case done: Outcome.Ingested => done.batch
    case rolled: Outcome.RolledForward => rolled.batch
    case other => fail(s"no batch was finished: $other")
  }

  private def commitFile(of: Source, version: Long): Path =
    new Path(s"${bronze(of).uri}/_delta_log/${f"$version%020d"}.json")

  private def inTheLogOf(of: Source)(file: Path): Boolean =
    file.toUri.getPath.startsWith(new Path(bronze(of).uri).toUri.getPath + "/_delta_log/")

  private def asSuperuser(statement: String): Unit =
    asSuperuser(_.createStatement().execute(statement): Unit)

  // ---------------------------------------------------------------------------------------------
  // The LogStore
  // ---------------------------------------------------------------------------------------------

  test("a table that the LogStore refuses as tainted fails its run for good, its batch planned") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    ingested(launch(orders))

    // Another process, with Delta's default LogStore, commits the next version of Bronze. The
    // next look at the table taints it, and its next commit is refused.
    val hadoopConf = spark.sessionState.newHadoopConf()
    new HDFSLogStore(hadoopConf).write(
      commitFile(orders, tableVersion(orders) + 1),
      Iterator(EmptyCommit).asJava,
      false,
      hadoopConf
    )
    drop(orders, "b.csv", "id\n2\n")

    val outcome = launch(orders)
    val said = failed(outcome, FailureClass.Permanent)
    said should startWith("the LogStore refused a table of the run, and running again does not")
    said should include("is tainted")
    runOf(outcome.run).failure.value.failure shouldBe FailureClass.Permanent
    // The lease is given up, as by any run that failed and could say so.
    holderOf(orders) shouldBe None
    batchOf(batchId(orders, 2)).status shouldBe BatchStatus.Planned
    rowsOf(orders).map(_._1) shouldBe Seq("1")
    assertLedger(orders)

    // Run again as it is, it fails the same way: nothing mends a taint but an operator.
    failed(launch(orders), FailureClass.Permanent) should include("is tainted")
  }

  test("a commit gone from the log of a table fails its run for good, until it is back") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    ingested(launch(orders))
    drop(orders, "b.csv", "id\n2\n")

    // The last commit of Bronze is away, as on a volume that is not mounted.
    val last = Paths.get(commitFile(orders, tableVersion(orders)).toUri)
    val away = Files.move(last, last.resolveSibling(s"${last.getFileName}.away"))
    val outcome =
      try launch(orders)
      finally Files.move(away, last): Unit

    val said = failed(outcome, FailureClass.Permanent)
    said should startWith("the LogStore refused a table of the run")
    said should include("its file is not in the log")
    runOf(outcome.run).failure.value.failure shouldBe FailureClass.Permanent
    holderOf(orders) shouldBe None

    // With the commit back, the next run plans what the failed one did not.
    ingested(launch(orders))
    rowsOf(orders).map(_._1).sorted shouldBe Seq("1", "2")
    assertLedger(orders)
  }

  // ---------------------------------------------------------------------------------------------
  // The conflicts of Delta
  // ---------------------------------------------------------------------------------------------

  test("a table whose metadata another writer keeps changing under a write fails it for good") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    ingested(launch(orders))
    drop(orders, "b.csv", "id\n2\n")

    // Before each of the two commits that the write makes takes its version, another writer
    // takes it, with a change of a property of the table: Delta finds a change of metadata
    // between what the write read and the version that it was to take. The other writer is a
    // LogStore of its own, in a thread of its own, as another process would be: it does not ask
    // Delta for the table, whose log the write holds a lock of this JVM on while it commits.
    val metadata = DeltaLog.forTable(spark, bronze(orders).uri).update().metadata
    val changes = new AtomicInteger
    def changeTheTable(version: Path): Unit = Await.result(
      Future {
        val changed = metadata.copy(configuration =
          metadata.configuration
            .updated("lakehouse.test.change", changes.incrementAndGet().toString)
        )
        val hadoopConf = spark.sessionState.newHadoopConf()
        new PostgresLogStore(hadoopConf)
          .write(version, Iterator(EmptyCommit, changed.json).asJava, false, hadoopConf)
      },
      1.minute
    )
    HookedLogStore.runAt(LogPoint.BeforeInsert, times = 2)(inTheLogOf(orders))(changeTheTable)
    val outcome =
      try launch(orders)
      finally HookedLogStore.disarm()

    changes.get shouldBe 2
    val said = failed(outcome, FailureClass.Permanent)
    said should include("the schema or the protocol of the table")
    said should include("2 times in a row")
    runOf(outcome.run).failure.value.failure shouldBe FailureClass.Permanent
    batchOf(batchId(orders, 2)).status shouldBe BatchStatus.Planned

    // Once the table is left alone, the next run writes the batch.
    ingested(launch(orders)).batch shouldBe batchId(orders, 2)
    rowsOf(orders).map(_._1).sorted shouldBe Seq("1", "2")
    assertLedger(orders)
  }

  // ---------------------------------------------------------------------------------------------
  // The control plane
  // ---------------------------------------------------------------------------------------------

  test("a control plane cut off in the middle of a run is a failure that passes") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")

    val outcome = throughTheProxy { control =>
      // Its rows are in Bronze; the commit of its state cannot reach the control plane.
      val cutOff = hookAt(FaultPoint.AfterBronzeCommit)(_ => proxied.faulty.disable())
      launch(orders, service(cutOff, on = control))
    }

    // The commit of its state gave no answer, and the run could not learn how it went (P8.5):
    // what it ends with says so, and says the failure of the control plane, with its class.
    val said = failed(outcome, FailureClass.Transient)
    said should startWith(
      s"the run ${outcome.run} could not learn whether the transaction that ends it was " +
        "committed: "
    )
    said should include("the run could not go on with its control plane: ")
    // Nothing reached the control plane: the run is as a process that died left it.
    runOf(outcome.run).status shouldBe RunStatus.Running
    batchOf(batchId(orders, 1)).status shouldBe BatchStatus.Planned

    // Its rows are in Bronze, so the run that takes its lease over rolls the batch forward.
    expire(orders)
    finished(launch(orders)) shouldBe batchId(orders, 1)
    runOf(outcome.run).status shouldBe RunStatus.Abandoned
    rowsOf(orders).map(_._1) shouldBe Seq("1")
    assertLedger(orders)
  }

  test("a control plane that refuses the run what it asks fails it for good") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")

    // The role of the engine can no longer record the files that a batch took.
    val refused = hookAt(FaultPoint.BeforeStateCommit) { _ =>
      asSuperuser("REVOKE INSERT ON control.processed_file FROM ingest_control")
    }
    val outcome =
      try launch(orders, service(refused))
      finally asSuperuser("GRANT INSERT ON control.processed_file TO ingest_control")

    val said = failed(outcome, FailureClass.Permanent)
    said should startWith("the run could not go on with its control plane: ")
    said should include("(SQLState 42501)")
    runOf(outcome.run).failure.value.failure shouldBe FailureClass.Permanent
    holderOf(orders) shouldBe None
    batchOf(batchId(orders, 1)).status shouldBe BatchStatus.Planned

    // With the privilege back, the next run rolls the batch forward: its rows are in Bronze.
    finished(launch(orders)) shouldBe batchId(orders, 1)
    rowsOf(orders).map(_._1) shouldBe Seq("1")
    assertLedger(orders)
  }

  test("a run, or an operator's action, that cannot take its lease says how, and leaves no row") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    val asked = OperatorRequest.of("ops.oncall", "the plan cannot be run").toOption.get

    val (run, action) = throughTheProxy { control =>
      proxied.faulty.disable()
      val actions =
        new OperatorActions(control, spark, lakeRoot, timing, retries, ConnectorKind.BuiltIn)
      (
        launch(orders, service(on = control)),
        actions.abandon(orders, versionOf(orders), BatchSeq.of(1).toOption.get, asked, None)
      )
    }

    failed(run, FailureClass.Transient) should startWith(
      "the run could not go on with its control plane: "
    )
    action match {
      case OperatorActions.Outcome.Failed(_, FailureClass.Transient, message) =>
        message should startWith("the run could not go on with its control plane: ")
      case other => fail(s"the action did not fail as transient: $other")
    }
    Seq(run.run, action.run).foreach(id => database.run(Runs.find(_, id)) shouldBe None)
    holderOf(orders) shouldBe None

    ingested(launch(orders))
    assertLedger(orders)
  }
}
