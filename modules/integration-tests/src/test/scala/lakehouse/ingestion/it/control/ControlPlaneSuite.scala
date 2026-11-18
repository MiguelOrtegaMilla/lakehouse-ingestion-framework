package lakehouse.ingestion.it.control

import java.sql.Connection
import java.time.Instant

import scala.util.Using

import lakehouse.ingestion.cli.Main
import lakehouse.ingestion.core.config.{ConfigVersion, Profile, SecretResolver, Source, SourceId}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.control.{Application, ConfigSnapshots, ControlDatabase}
import lakehouse.ingestion.it.support.{BootstrappedPostgres, ControlPlaneSchema}
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import org.scalatest.{BeforeAndAfterAll, EitherValues, OptionValues, Suite}

/**
 * A control plane for the tests of what the engine writes in it: a PostgreSQL as the bootstrap
 * leaves it, migrated by the command `migrate`, and reached as the engine reaches it, through
 * `ControlDatabase`, as the role and with the settings of the example profile.
 *
 * So a statement that passes here is one that the engine's role may run.
 */
trait ControlPlaneSuite extends BeforeAndAfterAll with EitherValues with OptionValues {
  this: Suite =>

  protected lazy val pg: BootstrappedPostgres = BootstrappedPostgres.start()
  protected lazy val profile: Profile = loaded(pg)
  protected lazy val database: ControlDatabase =
    ControlDatabase.open(profile.databases.control, SecretResolver(environment(pg).get)).value

  // Start, migrate and connect in beforeAll: if any fails, the suite aborts once, with the error.
  override def beforeAll(): Unit = {
    super.beforeAll()
    val ran = ControlPlaneSchema.migrate(pg)
    require(ran.exit == Main.Exit.Ok, ran.err)
    database: Unit
  }

  override def afterAll(): Unit =
    try database.close()
    finally {
      pg.stop()
      super.afterAll()
    }

  /** A source of the repository's examples, under a name of the test's own. */
  protected def source(id: String, change: String => String = identity): Source = {
    val text = change(exampleSource("sales.orders")).replace("id: sales.orders", s"id: $id")
    Source.load(s"$id.yaml", text).value.value
  }

  /** A source that no other test has: no test finds the rows of another. */
  protected def newSourceId(): SourceId =
    SourceId.parse(s"test.s${java.util.UUID.randomUUID().toString.take(8)}").value

  protected def versionOf(source: Source): ConfigVersion = ConfigVersion.of(profile, source)

  /** A run of `source` that is about to start, with its configuration stored. */
  protected def starting(source: Source, layer: Layer = Layer.Bronze): RunStart = {
    val version = versionOf(source)
    database.run(c => ConfigSnapshots.store(c, source.id, version)): Unit
    RunStart(
      RunId.generate(),
      source.id,
      layer,
      RunKind.Regular,
      Application.instance,
      Application.version,
      version.hash
    )
  }

  protected def epoch(value: Long): Epoch = Epoch.of(value).value

  /** The time of the database, which is the one the control plane goes by. */
  protected def databaseNow(): Instant = database.run { c =>
    Instant.parse(
      query(
        c,
        "SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')"
      ).head.head
    )
  }

  /** As the superuser, for what a test sets up that the engine's role may not write. */
  protected def asSuperuser[T](body: Connection => T): T =
    Using.resource(pg.connectAsSuperuser(Database))(body)
}
