package lakehouse.ingestion.it.logstore

import java.net.URI
import java.nio.file.{Files, Path => LocalPath, Paths}

import scala.jdk.CollectionConverters._
import scala.util.Using

import lakehouse.ingestion.it.support.{
  BootstrappedPostgres,
  CoordinationSchema,
  CoordinationStores,
  LocalSparkSuite
}
import lakehouse.ingestion.logstore._
import lakehouse.ingestion.logstore.coordination.{
  CommitEntry,
  CoordinationSettings,
  PostgresCoordinationStore,
  Registration,
  TableUri
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Delta on `PostgresLogStore`: the LogStore driven by the engine it is written for, loaded the
 * way a deployment loads it, by name and with its settings in the Spark configuration, against
 * a real PostgreSQL.
 *
 * It is a smoke test: a table is created, appended to and read; a commit that a dead writer
 * left is found by Delta; a version conflict is retried by Delta. The suites that call the
 * LogStore's methods themselves test each scenario; this one shows that Delta and the LogStore
 * agree on what those methods mean.
 */
final class PostgresLogStoreDeltaSpec extends AnyFunSuite with Matchers with LocalSparkSuite {

  import PostgresLogStoreDeltaSpec._

  private lazy val pg = BootstrappedPostgres.start()
  private lazy val settings =
    CoordinationStores.settings(pg.jdbcUrl(CoordinationSchema.Database), pg)

  /** A store of the test's own, to look at the registry and to play a writer that dies. */
  private lazy val store = new PostgresCoordinationStore(settings)

  private val fs = FileSystem.getLocal(new Configuration())

  override protected def sparkConf: Map[String, String] = Map(
    "spark.delta.logStore.file.impl" -> classOf[PostgresLogStore].getName,
    s"spark.hadoop.${CoordinationSettings.JdbcUrl}" -> settings.jdbcUrl,
    s"spark.hadoop.${CoordinationSettings.User}" -> settings.user,
    s"spark.hadoop.${CoordinationSettings.PasswordRef}" -> settings.password.toString
  )

  override def beforeAll(): Unit = {
    CoordinationSchema.migrate(pg): Unit
    super.beforeAll()
  }

  override def afterAll(): Unit =
    try super.afterAll()
    finally
      try store.close()
      finally pg.stop()

  private def rows(from: Long, until: Long): DataFrame = spark.range(from, until).toDF("id")

  private def create(name: String): String = {
    val path = tablePath(name)
    rows(0L, 10L).write.format("delta").save(path)
    path
  }

  private def append(path: String, from: Long, until: Long): Unit =
    rows(from, until).write.format("delta").mode("append").save(path)

  private def countOf(path: String): Long = spark.read.format("delta").load(path).count()

  private def versionOf(path: String): Long = DeltaLog.forTable(spark, path).update().version

  private def keyOf(path: String): TableUri = TableIdentity.of(new Path(path), fs)

  private def logOf(path: String): LocalPath = Paths.get(URI.create(path)).resolve("_delta_log")

  private def commitFile(path: String, version: Long): LocalPath =
    logOf(path).resolve(f"$version%020d.json")

  private def entry(path: String, version: Long): CommitEntry =
    store.entry(keyOf(path), version).getOrElse(fail(s"version $version has no entry"))

  test("Delta creates, appends to and reads a table, and the registry has every commit") {
    val path = create("orders")
    append(path, 10L, 20L)
    append(path, 20L, 30L)

    countOf(path) shouldBe 30L
    versionOf(path) shouldBe 2L

    // The table registered itself with its first commit, under the id Delta gave it.
    val tableId = DeltaLog.forTable(spark, path).update().metadata.id
    store.registration(keyOf(path)) shouldBe Some(
      Registration(keyOf(path), Registration.NotYetCreated, Some(tableId), taintReason = None)
    )
    (0L to 2L).foreach { version =>
      val committed = entry(path, version)
      committed.complete shouldBe true
      committed.writerInstance should endWith(s":${spark.sparkContext.applicationId}")
      // Each commit is its temporary file under a second name, and has no checksum sidecar.
      Files.isSameFile(
        logOf(path).resolve(committed.tempPath),
        commitFile(path, version)
      ) shouldBe true
      Files.exists(logOf(path).resolve(f".$version%020d.json.crc")) shouldBe false
    }
  }

  test("a commit that a dead writer took is found by Delta the next time it reads the table") {
    val path = create("recovered")
    append(path, 10L, 20L)

    // Another process takes version 2 and dies before publishing it. The commit is empty, as a
    // transaction that changed nothing: what matters is that it has a version.
    val dying = LogStores.over(store, LogStores.crashAt(FaultPoint.AfterInsert))
    a[SimulatedCrash] should be thrownBy dying.write(
      new Path(commitFile(path, 2).toUri),
      Iterator(EmptyCommit).asJava,
      false,
      spark.sessionState.newHadoopConf()
    )
    Files.exists(commitFile(path, 2)) shouldBe false
    entry(path, 2).complete shouldBe false

    // Delta knows nothing of the protocol: it lists the log.
    versionOf(path) shouldBe 2L
    countOf(path) shouldBe 20L
    entry(path, 2).complete shouldBe true
    Files.readAllLines(commitFile(path, 2)).asScala shouldBe Seq(EmptyCommit)

    // And it writes after the recovered commit, not over it.
    append(path, 20L, 30L)
    versionOf(path) shouldBe 3L
    countOf(path) shouldBe 30L
  }

  test("a version that another transaction took is retried by Delta at the next one") {
    val path = create("contended")
    val transaction = DeltaLog.forTable(spark, path).startTransaction(None) // reads version 0
    append(path, 10L, 20L) // another writer takes version 1

    // The LogStore answers with java.nio's FileAlreadyExistsException; Delta finds no logical
    // conflict with the winner, and commits at version 2.
    transaction.commit(Nil, DeltaOperations.ManualUpdate) shouldBe 2L

    versionOf(path) shouldBe 2L
    countOf(path) shouldBe 20L
    (0L to 2L).map(entry(path, _).complete) shouldBe Seq(true, true, true)
  }

  test("a checkpoint's pointer is written through the LogStore, and a new reader follows it") {
    val path = create("checkpointed")
    append(path, 10L, 20L)
    val log = DeltaLog.forTable(spark, path)
    log.checkpoint(log.update())

    // Replaced whole, without a sidecar that could go stale; and no entry for it.
    val logFiles = Using.resource(Files.list(logOf(path))) {
      _.iterator.asScala.map(_.getFileName.toString).toList
    }
    logFiles should contain("_last_checkpoint")
    logFiles should not contain "._last_checkpoint.crc"
    store.latestEntry(keyOf(path)).map(_.version) shouldBe Some(1L)

    // A reader that has nothing cached starts from the pointer.
    DeltaLog.clearCache()
    versionOf(path) shouldBe 1L
    countOf(path) shouldBe 20L
  }
}

object PostgresLogStoreDeltaSpec {

  /** A commit that changes nothing, as Delta writes one. */
  private val EmptyCommit =
    """{"commitInfo":{"timestamp":1763200000000,"operation":"Manual Update","operationParameters":{}}}"""
}
