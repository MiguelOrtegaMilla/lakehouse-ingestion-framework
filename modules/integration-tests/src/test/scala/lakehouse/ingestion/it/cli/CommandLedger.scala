package lakehouse.ingestion.it.cli

import java.nio.file.Files

import lakehouse.ingestion.cli.RunSession
import lakehouse.ingestion.core.config.{RootUri, SecretResolver, Source}
import lakehouse.ingestion.engine.connector.ConnectorRegistry
import lakehouse.ingestion.engine.files.{FileSourceConnector, Landing}
import lakehouse.ingestion.engine.jdbc.JdbcSourceConnector
import lakehouse.ingestion.it.ingestion.LedgerFacts
import lakehouse.ingestion.it.support.{
  CoordinationSchema,
  CoordinationStores,
  DeploymentFiles,
  LocalSparkSuite
}
import org.apache.spark.sql.SparkSession
import org.scalatest.Suite
import org.scalatest.matchers.should.Matchers

/**
 * The ledger of a source (P7.2), asked by a suite of a command once its invocations have ended:
 * what the control plane, the tables and `processed_file` say of each other (`LedgerFacts`).
 *
 * A command opens a session of Spark of its own in the JVM of the test, and closes it, and a JVM
 * has one at a time. So the ledger reads with one of its own as well, opened when it is asked
 * and closed once it has read, so that the next command of the test opens its own as it would.
 * It is on the LogStore of the deployment, coordinated: a read publishes a commit that a writer
 * took and did not publish, as the next reader of a deployment would.
 */
trait CommandLedger extends LedgerFacts {
  this: RunCommandQueries with Suite with Matchers =>

  /** The deployment that the commands of the suite run in: its lake and its landing zone. */
  protected def deployment: DeploymentFiles

  private var session: Option[SparkSession] = None

  override protected def ledgerSession: SparkSession =
    session.getOrElse(
      throw new IllegalStateException("the ledger reads with a session that it opens, and closes")
    )

  override protected def ledgerLake: RootUri =
    RootUri.parse(deployment.lake.toUri.toString).value

  /** The connectors of the application, as a command has them. */
  override protected def connectors: ConnectorRegistry =
    ConnectorRegistry(
      new FileSourceConnector(
        new Landing(RootUri.parse(deployment.landing.toUri.toString).value)
      ),
      new JdbcSourceConnector(SecretResolver(sys.env.get))
    )

  override protected def assertLedger(of: Source, earlier: Source*): Unit = {
    val scratch = Files.createTempDirectory("command-ledger-")
    val settings = LocalSparkSuite.baseConf(scratch, "command-ledger") ++
      CoordinationStores.sparkSettings(
        CoordinationStores.settings(pg.jdbcUrl(CoordinationSchema.Database), pg)
      )
    session = Some(
      settings
        .foldLeft(SparkSession.builder()) { case (builder, (key, value)) =>
          builder.config(key, value)
        }
        .getOrCreate()
    )
    try super.assertLedger(of, earlier: _*)
    finally {
      session.foreach(RunSession.close)
      session = None
      LocalSparkSuite.deleteRecursively(scratch)
    }
  }
}
