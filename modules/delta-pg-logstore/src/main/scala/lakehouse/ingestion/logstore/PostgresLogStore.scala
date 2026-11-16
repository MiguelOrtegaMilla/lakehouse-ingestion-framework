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
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.slf4j.LoggerFactory

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
 * '''Fault hooks.''' The one thing the constructor does say, at WARN and every time, is which
 * fault hooks the configuration turns on. They are for tests, and slow every commit down.
 *
 * '''Attribution.''' A commit written inside a `CommitContext` records the run it is for in
 * its entry, next to the instance that wrote it, and tells the context of each version it
 * loses to another writer (§3.7).
 *
 * '''Reading.''' Delta finds a table's versions by listing its log, so `listFrom` is where a
 * reader meets the protocol: before the listing, a commit that was taken and not published is
 * published. `read` is Hadoop's, unchanged: Delta reads a file after it has listed it.
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

  warnOfConfiguredFaults(initHadoopConf)

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
        // Delta calls this on the thread that called the operation: the thread whose context,
        // if it is in one, says which run the commit is for.
        protocol.commit(
          tableLog(file.getParent, fs, storage),
          version,
          file,
          actions.asScala,
          CommitContext.current
        )
      case None =>
        writeWhole(storage, file, actions.asScala, overwrite)
    }
  }

  /**
   * Lists a directory from the name of `path` on, as Hadoop does. If the directory is a table's
   * log, the log is first brought up to date with the registry (ADR-0004 §3.3), in whatever
   * JVM this runs: a driver, or a task that resolves a timestamp over a long history (§3.9).
   * Then the listing is checked for a commit made without this LogStore, which taints the
   * table and does not stop the reader.
   * `VACUUM` lists data directories through here too; those are not coordinated, and never
   * need the database.
   *
   * @throws java.io.FileNotFoundException the directory does not exist: for Delta, no table
   * @throws java.io.IOException the coordination store did not answer, or the log cannot be
   *   brought up to date: nothing is listed (fail closed)
   */
  override def listFrom(path: Path, hadoopConf: Configuration): java.util.Iterator[FileStatus] = {
    val fs = path.getFileSystem(hadoopConf)
    Option(fs.makeQualified(path).getParent).filter(_.getName == TableIdentity.LogDirectory) match {
      case None => super.listFrom(path, hadoopConf)
      case Some(directory) =>
        val log = tableLog(directory, fs, storageOf(fs))
        val latestEntry = protocol.bringUpToDate(log)
        val listed = super.listFrom(path, hadoopConf).asScala.toVector
        // The listing is complete: a commit in it without an entry now is not ours.
        listed
          .flatMap(file => commitVersion(file.getPath))
          .maxOption
          .foreach(protocol.detectForeignCommit(log, latestEntry, _))
        listed.iterator.asJava
    }
  }

  override def isPartialWriteVisible(path: Path, hadoopConf: Configuration): java.lang.Boolean =
    storageOf(path.getFileSystem(hadoopConf)).partialWritesVisible
}

object PostgresLogStore {

  private val logger = LoggerFactory.getLogger(classOf[PostgresLogStore])

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

  /** The table whose log is `directory`, a `_delta_log`. */
  private def tableLog(directory: Path, fs: FileSystem, storage: LogStorage): TableLog =
    new TableLog(TableIdentity.of(directory.getParent, fs), directory, fs, storage)

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

  /**
   * Says which fault hooks `conf` turns on, one warning for each. A setting that cannot be read
   * is announced too, and fails the first commit, not the creation of the LogStore.
   */
  private def warnOfConfiguredFaults(conf: Configuration): Unit =
    try
      FaultHooks.delaysIn(conf).foreach { case (point, millis) =>
        logger.warn(
          "Fault hook on by configuration: every commit waits {} ms at {} ({}). " +
            "Fault hooks are for tests only.",
          millis,
          point.key,
          FaultHooks.delayKey(point)
        )
      }
    catch {
      case e: IOException =>
        logger.warn(
          "A fault hook setting cannot be used, and every commit will fail on it: {}",
          e.getMessage
        )
    }

  /** The store that every LogStore of this JVM with these settings shares (ADR-0004 §3.9). */
  private[logstore] def sharedCoordination(conf: Configuration): CoordinationStore =
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
