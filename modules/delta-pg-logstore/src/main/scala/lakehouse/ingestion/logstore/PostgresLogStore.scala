package lakehouse.ingestion.logstore

import java.io.IOException
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{FileAlreadyExistsException, Files, Paths}

import scala.jdk.CollectionConverters._

import io.delta.storage.HadoopFileSystemLogStore
import lakehouse.ingestion.logstore.coordination.{
  CoordinationSettings,
  CoordinationStore,
  PostgresCoordinationStore
}
import lakehouse.ingestion.logstore.storage.{LogStorage, PublishResult}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * A Delta `LogStore` that takes the mutual exclusion of commits from PostgreSQL, so that
 * several processes can write one table on storage that gives none (ADR-0004). Delta loads it
 * by name:
 *
 * {{{
 * spark.delta.logStore.file.impl = lakehouse.ingestion.logstore.PostgresLogStore
 * }}}
 *
 * '''What goes where.''' A commit, `<version>.json` directly under `_delta_log`, goes through
 * the commit protocol. Every other file Delta writes here needs to appear whole and needs no
 * exclusion: it is written to a temporary file and given its name in one step.
 *
 * '''Instances.''' Delta creates them freely, by reflection, also in tasks that never touch a
 * `_delta_log`, and never closes them (§3.9). The constructor therefore keeps the
 * configuration and does nothing else. The first commit reads the settings and takes the
 * JVM's shared coordination store; a wrong setting fails that commit, not the constructor.
 *
 * Reading the log is still Hadoop's plain listing here: the coordinated read arrives with
 * recovery.
 *
 * @param coordinationOf the coordination store for a configuration
 * @param storageOf the storage for a file system, or its refusal
 * @param hooksOf the fault hooks a configuration selects
 */
final class PostgresLogStore private[logstore] (
    initHadoopConf: Configuration,
    coordinationOf: Configuration => CoordinationStore,
    storageOf: FileSystem => LogStorage,
    hooksOf: Configuration => FaultHooks,
    retry: RetryPolicy
) extends HadoopFileSystemLogStore(initHadoopConf) {

  import PostgresLogStore._

  /** The constructor Delta calls. */
  def this(initHadoopConf: Configuration) =
    this(
      initHadoopConf,
      PostgresLogStore.sharedCoordination,
      LogStorage.of,
      FaultHooks.from,
      RetryPolicy.Default
    )

  // Not kept if it fails, for instance on a setting that is missing: the next commit tries again.
  private lazy val protocol = new CommitProtocol(
    coordinationOf(initHadoopConf),
    hooksOf(initHadoopConf),
    retry,
    WriterInstance.of(initHadoopConf)
  )

  override def write(
      path: Path,
      actions: java.util.Iterator[String],
      overwrite: java.lang.Boolean,
      hadoopConf: Configuration
  ): Unit = {
    val fs = path.getFileSystem(hadoopConf)
    // First of all: a location this LogStore cannot write to is refused before it is touched.
    val storage = storageOf(fs)
    val file = fs.makeQualified(path)

    commitVersion(file) match {
      case Some(_) if overwrite =>
        throw new IOException(
          s"refusing to overwrite the commit $file: a commit is never rewritten"
        )
      case Some(version) =>
        val directory = file.getParent
        val log = new TableLog(TableIdentity.of(directory.getParent, fs), directory, fs, storage)
        protocol.commit(log, version, file, actions.asScala)
      case None =>
        writeWhole(storage, file, actions.asScala, overwrite)
    }
  }

  override def isPartialWriteVisible(path: Path, hadoopConf: Configuration): java.lang.Boolean =
    storageOf(path.getFileSystem(hadoopConf)).partialWritesVisible
}

object PostgresLogStore {

  private val CommitFileName = """(\d+)\.json""".r

  /**
   * The version `file` is the commit of, if it is a commit: a file named `<digits>.json`
   * directly under a `_delta_log`. Those are the names Delta reads as commits, and the only
   * files that get a coordination entry (ADR-0004 §8, item 1).
   */
  def commitVersion(file: Path): Option[Long] =
    Option(file.getParent).filter(_.getName == TableIdentity.LogDirectory).flatMap { _ =>
      file.getName match {
        case CommitFileName(digits) => digits.toLongOption
        case _ => None
      }
    }

  /**
   * Writes a file that is not a commit: whole, and without a coordination entry. Its temporary
   * file is next to it, in a directory of temporary files.
   */
  private def writeWhole(
      storage: LogStorage,
      file: Path,
      lines: Iterator[String],
      overwrite: Boolean
  ): Unit = {
    val temp = new Path(file.getParent, CommitProtocol.tempPathFor(file))
    storage.writeTemp(temp, lines)
    if (overwrite) storage.replace(temp, file)
    else
      try
        storage.publish(temp, file) match {
          case PublishResult.Published => ()
          // Not to be overwritten, and there it is: what Delta expects to be told.
          case PublishResult.AlreadyPublished | PublishResult.ForeignFile =>
            throw new FileAlreadyExistsException(file.toString)
          case PublishResult.TempFileLost =>
            throw new IOException(s"the temporary file of $file was removed while it was written")
        }
      // The file has its name, or will not get one: the temporary name is of no more use.
      finally storage.discard(temp)
  }

  /** The store that every LogStore of this JVM with these settings shares (ADR-0004 §3.9). */
  private def sharedCoordination(conf: Configuration): CoordinationStore =
    CoordinationSettings.from(conf) match {
      case Right(settings) => PostgresCoordinationStore.shared(settings)
      case Left(problems) =>
        throw new IOException(
          problems.mkString("the LogStore's coordination settings are not usable: ", "; ", "")
        )
    }
}

/**
 * Who a writer is, for the audit trail of its commits: `host:pid:spark.app.id`.
 *
 * The host is the kernel's name for the machine, read without asking a name service: a lookup
 * of the local host name can take seconds.
 */
private[logstore] object WriterInstance {

  private lazy val hostAndProcess: String = {
    val host =
      try new String(Files.readAllBytes(Paths.get("/proc/sys/kernel/hostname")), UTF_8).trim
      catch { case _: IOException => sys.env.getOrElse("HOSTNAME", "") }
    s"${if (host.isEmpty) "unknown-host" else host}:${ProcessHandle.current().pid()}"
  }

  /** @param conf the configuration of a LogStore, where Spark puts the application's id */
  def of(conf: Configuration): String =
    s"$hostAndProcess:${Option(conf.get("spark.app.id")).getOrElse("no-spark-app")}"
}
