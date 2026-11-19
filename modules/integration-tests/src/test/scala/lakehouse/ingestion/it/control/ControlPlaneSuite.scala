package lakehouse.ingestion.it.control

import java.sql.Connection
import java.time.Instant

import scala.concurrent.duration._
import scala.util.Using

import io.circe.Json
import lakehouse.ingestion.cli.Main
import lakehouse.ingestion.core.config.{
  ConfigVersion,
  PathTableLocator,
  Profile,
  RelativePath,
  SecretResolver,
  Source,
  SourceId,
  TableLocation
}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.control.{Application, ConfigSnapshots, ControlDatabase, LeasedRun}
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
      stopServers()
      super.afterAll()
    }

  /** Stops what `pg` was started with: itself, unless a suite put something in front of it. */
  protected def stopServers(): Unit = pg.stop()

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

  /**
   * Where a batch of `source` is planned to write, for the tests that write to no table: the
   * table of Bronze that a source of that id would have, under the lake of the profile.
   */
  protected def targetOf(source: SourceId): TableLocation =
    new PathTableLocator(profile.lake.uri)
      .locate(RelativePath.parse(s"bronze/${source.value.replace('.', '/')}").value)

  /** A plan written down by a test that is not about where its batch writes. */
  implicit final protected class PlansOfATest(leased: LeasedRun) {
    def plan(plan: Json): PlannedBatch = leased.plan(plan, targetOf(leased.start.source))
  }

  /** Another instance of the application: a pool of its own to the same control plane. */
  protected def anotherInstance(): ControlDatabase =
    ControlDatabase.open(profile.databases.control, SecretResolver(environment(pg).get)).value

  /** Waits, by the clock of the database, until the lease of `key` has expired. */
  protected def awaitExpiry(key: LeaseKey, within: FiniteDuration = 30.seconds): Unit = {
    val deadline = System.nanoTime() + within.toNanos
    def expired: Boolean = asSuperuser { c =>
      query(
        c,
        "SELECT expires_at < now() FROM control.source_lease WHERE lease_key = ?",
        key.value
      ) ==
        Seq(Seq("t"))
    }
    while (!expired) {
      require(System.nanoTime() < deadline, s"the lease of $key did not expire in $within")
      Thread.sleep(50)
    }
  }

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
