package lakehouse.ingestion.it.stress

import java.io.{Reader, Writer}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.util.Properties

import scala.util.Using

import lakehouse.ingestion.logstore.{FaultHooks, FaultPoint}
import org.apache.hadoop.fs.{FileContext, RawLocalFileSystem}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.storage.{DelegatingLogStore, LogStoreAdaptor}

/**
 * What one writer JVM actually used to commit, recorded by the writer itself: the preconditions
 * of a spike, RED or GREEN.
 *
 * A spike result means something only if the writers ran the code path it claims to measure.
 * The settings say what was asked for; the probe says what the writer's own Delta and Hadoop
 * resolved, in the JVM that committed:
 *
 * @param logStore the `LogStore` implementation Delta chose for the table's log path
 * @param logFileSystem the Hadoop `FileSystem` of the log path. `HDFSLogStore` serialises its
 *   check-then-rename with `synchronized (this)` only when this is a `RawLocalFileSystem`; on
 *   `file://` it is `LocalFileSystem`, so no lock is taken. Even when taken, that lock is per
 *   `HDFSLogStore` instance, so it never spans JVMs.
 * @param commitFileContext the `AbstractFileSystem` behind the `FileContext` that
 *   `HDFSLogStore.write` publishes `N.json` through: `LocalFs`, or `DelayingLocalFs`
 * @param delayedRenames commit-file renames `DelayingLocalFs` delayed in this JVM
 * @param guarded whether the writer was asked to pass the startup guard before it touched the
 *   table (`WriterConfig.guard`); one that was and is here, passed
 * @param runId the run the writer attributed its commits to (`CommitContext`). On
 *   `PostgresLogStore` it is in the coordination entry of each of its commits
 * @param contention how many times a commit of this writer found its version taken by another
 *   writer; Delta then tried the next version. Counted by `PostgresLogStore`, so always 0 on
 *   another LogStore
 * @param contentionAtInsert how many of those versions it lost at the `INSERT`: the commit was
 *   not in the log when the writer looked, and its claim found another writer's. Those are the
 *   times that two writers were committing one version at once
 * @param faultWaits how many times this JVM waited at each of `PostgresLogStore`'s fault hooks
 *   because its configuration turned the hook on, in the order of the protocol
 */
final case class WriterProbe(
    logStore: String,
    logFileSystem: String,
    commitFileContext: String,
    delayedRenames: Long,
    guarded: Boolean = false,
    runId: String = "",
    contention: Long = 0,
    contentionAtInsert: Long = 0,
    faultWaits: Seq[(String, Long)] = WriterProbe.NoWaits
) {

  /** `HDFSLogStore.write`'s own condition for entering its `synchronized` block. */
  def hdfsLogStoreLockTaken: Boolean =
    classOf[RawLocalFileSystem].isAssignableFrom(Class.forName(logFileSystem))

  def write(file: Path): Unit = {
    val p = new Properties
    p.setProperty("logStore", logStore)
    p.setProperty("logFileSystem", logFileSystem)
    p.setProperty("commitFileContext", commitFileContext)
    p.setProperty("delayedRenames", delayedRenames.toString)
    p.setProperty("guarded", guarded.toString)
    p.setProperty("runId", runId)
    p.setProperty("contention", contention.toString)
    p.setProperty("contentionAtInsert", contentionAtInsert.toString)
    faultWaits.foreach { case (point, waits) =>
      p.setProperty(s"faultWaits.$point", waits.toString)
    }
    Using.resource(Files.newBufferedWriter(file, UTF_8): Writer)(p.store(_, "writer probe"))
  }
}

object WriterProbe {

  /** No wait at any fault hook: what every writer has until it commits. */
  val NoWaits: Seq[(String, Long)] = FaultPoint.All.map(_.key -> 0L)

  /** The waits of this JVM so far, as `FaultHooks` counted them. */
  def faultWaitsOfThisJvm: Seq[(String, Long)] =
    FaultPoint.All.map(point => point.key -> FaultHooks.waitsAt(point))

  /** The setup half of the probe, resolved exactly as a commit to `tableUri` resolves it. */
  def resolve(spark: SparkSession, tableUri: String): WriterProbe = {
    val log = DeltaLog.forTable(spark, tableUri)
    val conf = log.newDeltaHadoopConf()
    val logStore = log.store match {
      case d: DelegatingLogStore =>
        d.getDelegate(log.logPath) match {
          case a: LogStoreAdaptor => a.logStoreImpl.getClass.getName
          case other => other.getClass.getName
        }
      case other => other.getClass.getName
    }
    WriterProbe(
      logStore = logStore,
      logFileSystem = log.logPath.getFileSystem(conf).getClass.getName,
      commitFileContext =
        FileContext.getFileContext(log.logPath.toUri, conf).getDefaultFileSystem.getClass.getName,
      delayedRenames = 0
    )
  }

  /** `None` if the writer never wrote one (it failed its setup, or was killed). */
  def read(file: Path): Option[WriterProbe] =
    Option.when(Files.exists(file)) {
      val p = new Properties
      Using.resource(Files.newBufferedReader(file, UTF_8): Reader)(p.load)
      def get(key: String) =
        Option(p.getProperty(key)).getOrElse(throw new IllegalStateException(s"$file: no $key"))
      WriterProbe(
        get("logStore"),
        get("logFileSystem"),
        get("commitFileContext"),
        get("delayedRenames").toLong,
        get("guarded").toBoolean,
        get("runId"),
        get("contention").toLong,
        get("contentionAtInsert").toLong,
        FaultPoint.All.map(point => point.key -> get(s"faultWaits.${point.key}").toLong)
      )
    }
}
