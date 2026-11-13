package lakehouse.ingestion.it.stress

import java.io.{Reader, Writer}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.util.Properties

import scala.util.Using

import org.apache.hadoop.fs.{FileContext, RawLocalFileSystem}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.storage.{DelegatingLogStore, LogStoreAdaptor}

/**
 * What one writer JVM actually used to commit, recorded by the writer itself (RED preconditions).
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
 */
final case class WriterProbe(
    logStore: String,
    logFileSystem: String,
    commitFileContext: String,
    delayedRenames: Long
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
    Using.resource(Files.newBufferedWriter(file, UTF_8): Writer)(p.store(_, "writer probe"))
  }
}

object WriterProbe {

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
        get("delayedRenames").toLong
      )
    }
}
