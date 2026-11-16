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

/**
 * A stress run on `PostgresLogStore`: the orchestrator's session and every writer JVM commit
 * through it, coordinated by one PostgreSQL that the suite starts with the schema migrated.
 *
 * Every process reaches PostgreSQL through the proxy that a run can break, at an address that
 * stays the same when the server is stopped and started again. The suite itself looks at the
 * registry on the path that is never broken.
 */
trait CoordinatedStressSuite extends LocalSparkSuite { self: Suite =>

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
