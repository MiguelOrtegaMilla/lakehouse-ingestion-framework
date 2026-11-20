package lakehouse.ingestion.it.logstore

import java.net.ServerSocket
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Path => LocalPath}
import java.time.Instant
import java.time.temporal.ChronoUnit

import scala.jdk.CollectionConverters._
import scala.util.Using

import io.delta.storage.HDFSLogStore
import lakehouse.ingestion.engine.control.DatabaseFailure
import lakehouse.ingestion.it.support.{BootstrappedPostgres, CoordinationSchema, CoordinationStores}
import lakehouse.ingestion.logstore._
import lakehouse.ingestion.logstore.coordination.{
  CoordinationSettings,
  PostgresCoordinationStore,
  PostgresRegistryMaintenance,
  Registration,
  TableUri
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfterAll, OptionValues}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * An operator's work and the startup guard against a real PostgreSQL (ADR-0004 §3.5): the
 * commit path connects as its role and the operator's tools as `ingest_maintenance`, each with
 * the privileges the migrations give it. `TableMaintenanceSpec` and `StartupGuardSpec` have
 * every case; these are the ones a database can decide differently from a map in memory.
 */
final class PostgresMaintenanceAndGuardSpec
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll
    with OptionValues
    with ScratchDirectories {

  import PostgresMaintenanceAndGuardSpec._

  private lazy val pg = BootstrappedPostgres.start()
  private lazy val commitPath =
    CoordinationStores.settings(pg.jdbcUrl(CoordinationSchema.Database), pg)
  private lazy val store = new PostgresCoordinationStore(commitPath)
  private lazy val registry = new PostgresRegistryMaintenance(
    CoordinationStores.settings(
      pg.jdbcUrl(CoordinationSchema.Database),
      pg,
      role = CoordinationSchema.MaintenanceRole
    )
  )
  private lazy val maintenance = new TableMaintenance(registry)

  private val conf = new Configuration()
  private val fs = FileSystem.getLocal(conf)

  override def beforeAll(): Unit = CoordinationSchema.migrate(pg): Unit

  override def afterAll(): Unit =
    try registry.close()
    finally
      try store.close()
      finally pg.stop()

  /** A table directory with an empty `_delta_log`. */
  final private class Lake {
    val directory: LocalPath = Files.createDirectories(newDirectory().resolve("lake/orders"))
    val log: LocalPath = Files.createDirectory(directory.resolve("_delta_log"))
    val table: TableUri = TableIdentity.of(hadoop(directory), fs)

    def commitFile(version: Long): LocalPath = log.resolve(f"$version%020d.json")

    def commit(version: Long, hooks: Option[FaultHooks] = None)(actions: Seq[String]): Unit =
      LogStores
        .over(store, hooks)
        .write(hadoop(commitFile(version)), actions.iterator.asJava, false, conf)

    def foreignCommit(version: Long): Unit =
      new HDFSLogStore(conf)
        .write(hadoop(commitFile(version)), ForeignActions.iterator.asJava, false, conf)

    def list(): Seq[String] =
      LogStores
        .over(store)
        .listFrom(hadoop(commitFile(0)), conf)
        .asScala
        .map(_.getPath.getName)
        .toSeq

    def temps: Seq[LocalPath] =
      Using.resource(Files.list(log.resolve(".tmp")))(_.iterator.asScala.toList)
  }

  test("a table made without the LogStore is adopted by the operator's role, then coordinated") {
    val lake = new Lake
    (0L to 1L).foreach(lake.foreignCommit)

    maintenance.adopt(hadoop(lake.directory), fs, Some("its-id")) shouldBe Adoption(lake.table, 1L)

    store
      .registration(lake.table)
      .map(r => (r.registeredVersion, r.deltaTableId, r.isTainted)) shouldBe
      Some((1L, Some("its-id"), false))
    lake.commit(2)(append(2))
    store.entry(lake.table, 2L).map(_.complete) shouldBe Some(true)
    lake.list() shouldBe (0L to 2L).map(commit)
  }

  test("after F11, the operator adopts the table again and it is written and read once more") {
    val lake = new Lake
    lake.commit(0)(Creation)
    val foreignWriterWins = LogStores.at(FaultPoint.AfterInsert)(lake.foreignCommit(1))
    an[UncoordinatedWriteException] should be thrownBy lake.commit(1, foreignWriterWins)(append(1))
    an[UncoordinatedWriteException] should be thrownBy lake.list()
    val ours = store.entry(lake.table, 1L)

    val readopted = maintenance.readopt(hadoop(lake.directory), fs, None)

    readopted.version shouldBe 1L
    readopted.removed.clearedTaint.getOrElse("") should include("version 1")
    readopted.removed.removedEntries shouldBe ours.toSeq
    store.registration(lake.table).map(r => (r.registeredVersion, r.isTainted)) shouldBe
      Some((1L, false))
    lake.list() shouldBe Seq(commit(0), commit(1))
    lake.commit(2)(append(2))
  }

  test("F9: the operator resets the registry of a dropped table, and one is created in its place") {
    val lake = new Lake
    lake.commit(0)(Creation)
    lake.commit(1)(append(1))
    Using.resource(Files.walk(lake.log)) {
      _.iterator.asScala.filter(Files.isRegularFile(_)).toList.foreach(Files.delete)
    }
    a[StaleRegistrationException] should be thrownBy lake.commit(0)(Creation)

    maintenance.reset(hadoop(lake.directory), fs)

    store.registration(lake.table) shouldBe None
    lake.commit(0)(Creation)
    store.registration(lake.table).map(_.registeredVersion) shouldBe
      Some(Registration.NotYetCreated)
    lake.list() shouldBe Seq(commit(0))
  }

  test("a writer stalled before its INSERT while its table is adopted again gets no version") {
    val lake = new Lake
    lake.commit(0)(Creation)
    lake.commit(1)(append(1))
    // The operator's change commits between the writer's checks and its INSERT.
    val whileAdopted = LogStores.at(FaultPoint.BeforeInsert) {
      maintenance.readopt(hadoop(lake.directory), fs, None).version shouldBe 1L: Unit
    }

    a[RegistrationChangedException] should be thrownBy lake.commit(2, whileAdopted)(append(2))

    store.entry(lake.table, 2L) shouldBe None
    Files.exists(lake.commitFile(2)) shouldBe false
    // The transaction, started again, reads the registration as it is and commits.
    lake.commit(2)(append(2))
    lake.list() shouldBe (0L to 2L).map(commit)
  }

  test("a writer of a dropped table gets no version of the table created in its place") {
    val lake = new Lake
    lake.commit(0)(Creation)
    lake.commit(1)(append(1))
    // While the writer of version 2 is stalled, the table is dropped, its registry reset, and
    // a table created at its path that gets as far as the version before the writer's.
    val whileReplaced = LogStores.at(FaultPoint.BeforeInsert) {
      Using.resource(Files.walk(lake.log)) {
        _.iterator.asScala.filter(Files.isRegularFile(_)).toList.foreach(Files.delete)
      }
      maintenance.reset(hadoop(lake.directory), fs)
      lake.commit(0)(Creation)
      lake.commit(1)(append(1))
    }

    a[RegistrationChangedException] should be thrownBy lake.commit(2, whileReplaced)(append(2))

    // Version 2 of the new table is free, and its log holds only its own commits.
    store.entry(lake.table, 2L) shouldBe None
    Files.exists(lake.commitFile(2)) shouldBe false
    lake.list() shouldBe Seq(commit(0), commit(1))
    lake.temps should have size 2
  }

  test("F1: the orphan of a writer that died before its version is collected once it is old") {
    val lake = new Lake
    lake.commit(0)(Creation)
    val published = lake.temps
    a[SimulatedCrash] should be thrownBy
      lake.commit(1, LogStores.crashAt(FaultPoint.BeforeInsert))(append(1))
    val orphan = lake.temps.diff(published).head
    Files.setLastModifiedTime(orphan, FileTime.from(Instant.now().minus(2, ChronoUnit.DAYS)))

    val fates = maintenance.collectTemps(hadoop(lake.directory), fs)

    fates shouldBe Map(
      orphan.getFileName.toString -> TempFate.RemovedOrphan,
      published.head.getFileName.toString -> TempFate.RemovedPublished
    )
    lake.temps shouldBe empty
    lake.list() shouldBe Seq(commit(0))
    lake.commit(1)(append(1))
  }

  // ---------------------------------------------------------------------------------------------
  // The startup guard, asking the real database
  // ---------------------------------------------------------------------------------------------

  /** What a deployment puts in the Spark configuration, as the LogStore would be given it. */
  private def deployed(settings: CoordinationSettings): Configuration = {
    val configured = new Configuration()
    configured.set(StartupGuard.logStoreKey("file"), classOf[PostgresLogStore].getName)
    configured.set(CoordinationSettings.JdbcUrl, settings.jdbcUrl)
    configured.set(CoordinationSettings.User, settings.user)
    configured.set(CoordinationSettings.PasswordRef, settings.password.toString)
    configured.setLong(CoordinationSettings.ConnectTimeoutMs, 2000L)
    configured
  }

  test("a process configured for this database may start") {
    StartupGuard.problems(deployed(commitPath), hadoop(newDirectory())) shouldBe empty
  }

  test(
    "F13: without the coordination schema, or with another version of it, a process may not start"
  ) {
    val lake = hadoop(newDirectory())
    // A process that starts now: its settings have no store in this JVM yet. A store checks the
    // schema once, when it is first used, which for a process is when the guard asks.
    val configured =
      deployed(CoordinationStores.settings(pg.jdbcUrl(CoordinationSchema.Database), pg))

    CoordinationSchema.drop(pg)
    try {
      val found = StartupGuard.problems(configured, lake)
      found should have size 1
      found.head should include("migrations")
    } finally CoordinationSchema.migrate(pg): Unit

    // The same process, once the migrator has run.
    StartupGuard.problems(configured, lake) shouldBe empty
  }

  test("a password that the database refuses is a problem, and is not repeated") {
    val wrong = Files.createTempFile("wrong-secret-", ".txt")
    wrong.toFile.deleteOnExit()
    Files.write(wrong, "not-the-password".getBytes(UTF_8))
    val configured = deployed(commitPath)
    configured.set(CoordinationSettings.PasswordRef, s"file:$wrong")

    val found = StartupGuard.problems(configured, hadoop(newDirectory()))

    found should have size 1
    found.head should include("the coordination database cannot be used")
    found.head should not include "not-the-password"

    // The database answered, and said no: running again will not do.
    val refused =
      intercept[StartupRefusedException](StartupGuard.require(configured, hadoop(newDirectory())))
    DatabaseFailure.mayPass(refused.coordinationFailure.value) shouldBe false
  }

  test("a coordination database that is not there keeps a process from starting, and may pass") {
    val closed = Using.resource(new ServerSocket(0))(_.getLocalPort)
    val configured = deployed(commitPath)
    configured.set(
      CoordinationSettings.JdbcUrl,
      s"jdbc:postgresql://127.0.0.1:$closed/${CoordinationSchema.Database}"
    )
    configured.setLong(CoordinationSettings.ConnectTimeoutMs, 500L)

    val refused =
      intercept[StartupRefusedException](StartupGuard.require(configured, hadoop(newDirectory())))

    refused.problems should have size 1
    refused.problems.head should include("the coordination database cannot be used")
    DatabaseFailure.mayPass(refused.coordinationFailure.value) shouldBe true
  }
}

object PostgresMaintenanceAndGuardSpec {

  private val ForeignActions =
    Seq("""{"commitInfo":{"operation":"WRITE","engineInfo":"another writer"}}""")

  private val Creation: Seq[String] = Seq(
    """{"commitInfo":{"timestamp":1763200000000,"operation":"CREATE TABLE"}}""",
    """{"metaData":{"id":"5a0d1a66-0bb5-4d1c-8a8b-6a1f6f5a3c11","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{},"createdTime":1763200000000}}""",
    """{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"""
  )

  private def append(version: Long): Seq[String] =
    Seq(s"""{"add":{"path":"part-$version.parquet","dataChange":true}}""")

  private def commit(version: Long): String = f"$version%020d.json"

  private def hadoop(path: LocalPath): Path = new Path("file", null, path.toString)
}
