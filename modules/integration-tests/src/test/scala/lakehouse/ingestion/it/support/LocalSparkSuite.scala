package lakehouse.ingestion.it.support

import java.nio.file.{Files, Path}
import java.util.Comparator

import scala.util.Using

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * One local `SparkSession` with Delta for the whole suite, in its own scratch directory.
 *
 * The session is created in `beforeAll` and stopped in `afterAll`, so each suite can bring its own
 * configuration (P2a suites set `spark.delta.logStore.<scheme>.impl`). A JVM can hold only one
 * `SparkContext`, so suites run one after another in the forked test JVM (sbt's default).
 *
 * Needs the module options of `project/SparkTestJvm.scala` on the JVM command line.
 */
trait LocalSparkSuite extends BeforeAndAfterAll { self: Suite =>

  private var session: Option[SparkSession] = None
  private var scratch: Option[Path] = None

  /** Extra or overriding Spark settings for this suite. */
  protected def sparkConf: Map[String, String] = Map.empty

  final protected def spark: SparkSession =
    session.getOrElse(throw new IllegalStateException("SparkSession used outside the suite"))

  /** A directory private to this suite, deleted after it. Tables live under it. */
  final protected def workDir: Path =
    scratch.getOrElse(throw new IllegalStateException("workDir used outside the suite"))

  /** A fresh table location under `workDir`, as a `file://` URI string. */
  final protected def tablePath(name: String): String =
    workDir.resolve("tables").resolve(name).toUri.toString

  override def beforeAll(): Unit = {
    super.beforeAll()
    val dir = Files.createTempDirectory(s"${suiteName}-")
    scratch = Some(dir)
    val conf = LocalSparkSuite.baseConf(dir, appName = suiteName) ++ sparkConf
    session = Some(
      conf.foldLeft(SparkSession.builder()) { case (b, (k, v)) => b.config(k, v) }.getOrCreate()
    )
  }

  override def afterAll(): Unit =
    try {
      session.foreach(_.stop())
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      // DeltaLog instances are cached per JVM and keep the LogStore of the session that created
      // them; the next suite may configure a different one.
      DeltaLog.clearCache()
      scratch.foreach(LocalSparkSuite.deleteRecursively)
    } finally {
      session = None
      scratch = None
      super.afterAll()
    }
}

object LocalSparkSuite {

  /**
   * Small, deterministic, local: two cores, two shuffle partitions, no UI, UTC, loopback only,
   * and the warehouse inside the suite's scratch directory. Spark's own
   * scratch space (`spark.local.dir`) stays in `java.io.tmpdir`; Spark deletes it on `stop()`.
   */
  def baseConf(dir: Path, appName: String): Map[String, String] = Map(
    "spark.master" -> "local[2]",
    "spark.app.name" -> appName,
    "spark.ui.enabled" -> "false",
    "spark.sql.shuffle.partitions" -> "2",
    "spark.default.parallelism" -> "2",
    "spark.sql.session.timeZone" -> "UTC",
    "spark.driver.host" -> "127.0.0.1",
    "spark.driver.bindAddress" -> "127.0.0.1",
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.warehouse.dir" -> dir.resolve("warehouse").toString
  )

  def deleteRecursively(root: Path): Unit =
    if (Files.exists(root))
      Using.resource(Files.walk(root)) {
        _.sorted(Comparator.reverseOrder[Path]()).forEach(p => Files.delete(p))
      }
}
