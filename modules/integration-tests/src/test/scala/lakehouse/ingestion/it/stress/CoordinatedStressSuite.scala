package lakehouse.ingestion.it.stress

import lakehouse.ingestion.it.support.{
  CoordinationSchema,
  CoordinationStores,
  LocalSparkSuite,
  ProxiedPostgres
}
import lakehouse.ingestion.logstore.TableIdentity
import lakehouse.ingestion.logstore.coordination.{
  CoordinationSettings,
  PostgresCoordinationStore,
  TableUri
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.Suite

import scala.util.Using

/**
 * A stress run on `PostgresLogStore`: the orchestrator's session and every writer JVM commit
 * through it, coordinated by one PostgreSQL that the suite starts with the schema migrated.
 *
 * Every process reaches PostgreSQL through the proxy that a run can break, at an address that
 * stays the same when the server is stopped and started again. The suite itself looks at the
 * registry on the path that is never broken.
 */
trait CoordinatedStressSuite extends LocalSparkSuite { self: Suite =>

  import CoordinatedStressSuite.BatchCommit

  protected lazy val pg: ProxiedPostgres = ProxiedPostgres.start()

  /** How every process of a run reaches the coordination database. */
  protected lazy val coordination: CoordinationSettings =
    CoordinationStores.settings(pg.faultyJdbcUrl(CoordinationSchema.Database), pg.postgres)

  /** The registry as the commit path sees it, read by the suite to check what a run left. */
  protected lazy val registry = new PostgresCoordinationStore(coordination)

  /** The orchestrator creates the table and reads it back through the LogStore, like a writer. */
  override protected def sparkConf: Map[String, String] =
    CoordinationStores.sparkSettings(coordination)

  /**
   * The same settings for a writer JVM, which takes its Spark configuration from `-Dspark.*`
   * options: what makes it commit through `PostgresLogStore`, and to this database.
   */
  protected def coordinatedJvmOptions: Seq[String] =
    CoordinationStores.sparkSettings(coordination).toSeq.sorted.map { case (key, value) =>
      s"-D$key=$value"
    }

  /** The key a stress table is coordinated under. */
  protected def keyOf(run: StressRun): TableUri =
    TableIdentity.of(new Path(run.tableUri), FileSystem.getLocal(new Configuration()))

  /** What the database holds about the table of `run`, read on the path that is never broken. */
  protected def registryView(run: StressRun): LogModel.RegistryView =
    Using.resource(pg.observe(CoordinationSchema.CommitPathRole, CoordinationSchema.Database)) {
      RegistryReader.view(_, keyOf(run).value)
    }

  /** What kills the writers of `run`, looking at them from outside. The caller closes it. */
  protected def killerOf(run: StressRun): WriterKiller =
    new WriterKiller(
      pg.observe(CoordinationSchema.CommitPathRole, CoordinationSchema.Database),
      keyOf(run).value,
      run.tableDir.resolve("_delta_log")
    )

  /**
   * The commit of a writer's batch, read after the run is over: its version in the log, and
   * the run that its entry in the registry names. `None` if the batch is not in the log.
   */
  protected def commitOfBatch(run: StressRun, writer: Int, batch: Long): Option[BatchCommit] = {
    val entries = registryView(run).entries.map(e => e.version -> e.runId).toMap
    LogReader
      .logView(spark, run.tableDir)
      .commits
      .find(_.txns.contains(LogModel.Txn(StressTable.writerId(writer), batch)))
      .map(commit => BatchCommit(commit.version, entries.get(commit.version).flatten))
  }

  /**
   * Verifies a finished run as a coordinated one: what every run is held to, and what
   * `PostgresLogStore` adds, with the registry read after the readers have read the table.
   */
  protected def verify(
      run: StressRun,
      settings: StressHarness.Settings,
      outcomes: Seq[StressHarness.WriterOutcome]
  ): Verifier.Report =
    LogReader.verify(
      spark,
      run,
      Verifier.Expected.of(settings),
      outcomes,
      Some(() => registryView(run))
    )

  override def beforeAll(): Unit = {
    CoordinationSchema.migrate(pg.postgres): Unit
    super.beforeAll()
  }

  override def afterAll(): Unit =
    try super.afterAll()
    finally
      try registry.close()
      finally pg.stop()
}

object CoordinatedStressSuite {

  /** Where a writer's batch is in the log, and the run that the entry of that version names. */
  final case class BatchCommit(version: Long, run: Option[String])
}
