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
  PostgresRegistryMaintenance,
  TableUri
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.delta.DeltaLog
import org.scalatest.Assertions
import org.scalatest.Suite

/**
 * Delta on `PostgresLogStore`, set up the way a deployment sets it up: the LogStore named in the
 * Spark configuration with its settings next to it, against a real PostgreSQL with the
 * coordination schema migrated.
 *
 * Besides the session, a suite gets a store and an operator's tools of its own, to look at the
 * registry, to play a process that dies, and to do what a runbook says.
 */
trait CoordinatedDeltaSuite extends LocalSparkSuite with Assertions { self: Suite =>

  private lazy val pg = BootstrappedPostgres.start()

  protected lazy val settings: CoordinationSettings =
    CoordinationStores.settings(pg.jdbcUrl(CoordinationSchema.Database), pg)

  /** The registry as the commit path sees it. */
  protected lazy val store = new PostgresCoordinationStore(settings)

  private lazy val registry = new PostgresRegistryMaintenance(
    CoordinationStores.settings(
      pg.jdbcUrl(CoordinationSchema.Database),
      pg,
      role = CoordinationSchema.MaintenanceRole
    )
  )

  /** What an operator does to a table, with the operator's role. */
  protected lazy val maintenance = new TableMaintenance(registry)

  protected val fs: FileSystem = FileSystem.getLocal(new Configuration())

  override protected def sparkConf: Map[String, String] = CoordinationStores.sparkSettings(settings)

  override def beforeAll(): Unit = {
    CoordinationSchema.migrate(pg): Unit
    super.beforeAll()
  }

  override def afterAll(): Unit =
    try super.afterAll()
    finally
      try registry.close()
      finally
        try store.close()
        finally pg.stop()

  protected def rows(from: Long, until: Long): DataFrame = spark.range(from, until).toDF("id")

  /** A new table with the ids 0 to 9, at version 0. */
  protected def create(name: String): String = {
    val path = tablePath(name)
    rows(0L, 10L).write.format("delta").save(path)
    path
  }

  protected def append(path: String, from: Long, until: Long): Unit =
    rows(from, until).write.format("delta").mode("append").save(path)

  protected def countOf(path: String): Long = spark.read.format("delta").load(path).count()

  protected def idsOf(path: String): Seq[Long] =
    spark.read.format("delta").load(path).collect().map(_.getLong(0)).sorted.toSeq

  protected def versionOf(path: String): Long = DeltaLog.forTable(spark, path).update().version

  protected def keyOf(path: String): TableUri = TableIdentity.of(new Path(path), fs)

  protected def directoryOf(path: String): LocalPath = Paths.get(URI.create(path))

  protected def logOf(path: String): LocalPath = directoryOf(path).resolve("_delta_log")

  protected def commitFile(path: String, version: Long): LocalPath =
    logOf(path).resolve(f"$version%020d.json")

  protected def entry(path: String, version: Long): CommitEntry =
    store.entry(keyOf(path), version).getOrElse(fail(s"version $version has no entry"))

  /** The names of the temporary files in the table's log. */
  protected def tempsOf(path: String): Set[String] = {
    val directory = logOf(path).resolve(".tmp")
    if (!Files.isDirectory(directory)) Set.empty
    else Using.resource(Files.list(directory))(_.iterator.asScala.map(_.getFileName.toString).toSet)
  }

  /**
   * A process that takes `version` with the commit `actions` and dies before publishing it,
   * through a LogStore of its own on the same database.
   */
  protected def dieAfterTaking(path: String, version: Long, actions: Seq[String]): Unit = {
    val dying = LogStores.over(store, LogStores.crashAt(FaultPoint.AfterInsert))
    try {
      dying.write(
        new Path(commitFile(path, version).toUri),
        actions.iterator.asJava,
        false,
        spark.sessionState.newHadoopConf()
      )
      fail("the writer was to die after taking its version")
    } catch { case _: SimulatedCrash => () }
  }

  /** `error` and everything that caused it. */
  protected def causesOf(error: Throwable): List[Throwable] =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).toList
}

object CoordinatedDeltaSuite {

  /** A commit that changes nothing, as Delta writes one. */
  val EmptyCommit: String =
    """{"commitInfo":{"timestamp":1763200000000,"operation":"Manual Update","operationParameters":{}}}"""
}
