package lakehouse.ingestion.it.logstore

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import io.delta.storage.{CloseableIterator, HDFSLogStore}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

/**
 * Delta's default `file://` LogStore (`HDFSLogStore`) with a journal of every call Delta makes
 * to it, and two switches a test can turn.
 *
 * `PostgresLogStore` (ADR-0004) is a port, so it depends on how Delta 4.4 drives a LogStore:
 * which files it writes and with which `overwrite`, on which thread, how instances are created,
 * and what it does when a write fails. `DeltaLogStoreContractSpec` pins those facts with this
 * class. Delta loads it exactly as it will load `PostgresLogStore`: by name, from
 * `spark.delta.logStore.file.impl`, through the constructor that takes a Hadoop `Configuration`.
 *
 * Delta creates the instances, so the journal and the switches live in the companion object and
 * are shared by the whole JVM. Suites that use this class must not run in parallel.
 */
final class RecordingLogStore(initHadoopConf: Configuration) extends HDFSLogStore(initHadoopConf) {
  import RecordingLogStore._

  private val instance = nextInstance.incrementAndGet()
  record(Created(instance, ThreadRef.current(), probeSettings(initHadoopConf)))

  override def write(
      path: Path,
      actions: java.util.Iterator[String],
      overwrite: java.lang.Boolean,
      hadoopConf: Configuration
  ): Unit = {
    def recorded(outcome: Outcome): Unit =
      record(
        Write(instance, ThreadRef.current(), path, overwrite, outcome, probeSettings(hadoopConf))
      )

    val behaviour =
      if (!overwrite && kindOf(path) == FileKind.Commit) commitBehaviour(commitVersion(path))
      else Delegate
    try
      behaviour match {
        case Delegate =>
          super.write(path, actions, overwrite, hadoopConf)
          recorded(Written)
        case ReturnWithoutWriting =>
          recorded(NotWritten)
        case Fail(error) =>
          throw error()
      }
    catch {
      case e: Throwable =>
        recorded(Failed(e.getClass.getName))
        throw e
    }
  }

  override def read(path: Path, hadoopConf: Configuration): CloseableIterator[String] = {
    record(Read(instance, ThreadRef.current(), path))
    super.read(path, hadoopConf)
  }

  override def listFrom(path: Path, hadoopConf: Configuration): java.util.Iterator[FileStatus] = {
    record(ListFrom(instance, ThreadRef.current(), path))
    super.listFrom(path, hadoopConf)
  }

  override def isPartialWriteVisible(path: Path, hadoopConf: Configuration): java.lang.Boolean =
    partialWriteVisible
}

object RecordingLogStore {

  /** The thread a call was made on. Spark runs tasks, also in local mode, on its own threads. */
  final case class ThreadRef(id: Long, name: String) {
    def isSparkTask: Boolean = name.startsWith("Executor task launch worker")
  }

  object ThreadRef {
    def current(): ThreadRef = of(Thread.currentThread())
    def of(thread: Thread): ThreadRef = ThreadRef(thread.getId, thread.getName)
  }

  sealed trait Outcome
  case object Written extends Outcome

  /** `write` returned normally without creating the file (see `ReturnWithoutWriting`). */
  case object NotWritten extends Outcome
  final case class Failed(exceptionClass: String) extends Outcome

  /**
   * @param instance the LogStore instance that received the call, numbered in creation order
   */
  sealed trait Event {
    def instance: Int
    def thread: ThreadRef
  }

  /** @param settings the probe settings found in the constructor's `Configuration` */
  final case class Created(instance: Int, thread: ThreadRef, settings: Map[String, String])
      extends Event

  sealed trait Call extends Event {

    /** The file written or read; for a listing, the first name wanted in its directory. */
    def path: Path

    /** The local path of the directory the call works in. */
    def directory: String = path.getParent.toUri.getPath
  }

  /** @param settings the probe settings found in the `Configuration` passed with the call */
  final case class Write(
      instance: Int,
      thread: ThreadRef,
      path: Path,
      overwrite: Boolean,
      outcome: Outcome,
      settings: Map[String, String]
  ) extends Call {
    def kind: FileKind = kindOf(path)
  }

  final case class Read(instance: Int, thread: ThreadRef, path: Path) extends Call
  final case class ListFrom(instance: Int, thread: ThreadRef, path: Path) extends Call

  /** The files of `_delta_log` that Delta may write through a LogStore, by name. */
  sealed trait FileKind

  object FileKind {
    case object Commit extends FileKind
    case object LastCheckpoint extends FileKind
    case object LastVacuumInfo extends FileKind

    /** The top-level file of a V2 checkpoint in JSON: `<version>.checkpoint.<uuid>.json`. */
    case object V2CheckpointJson extends FileKind
    final case class Other(name: String) extends FileKind
  }

  private val CommitName = """(\d{20})\.json""".r
  private val V2CheckpointJsonName = """\d{20}\.checkpoint\.[0-9a-f-]+\.json""".r

  def kindOf(path: Path): FileKind = path.getName match {
    case CommitName(_) => FileKind.Commit
    case "_last_checkpoint" => FileKind.LastCheckpoint
    case "_last_vacuum_info" => FileKind.LastVacuumInfo
    case V2CheckpointJsonName() => FileKind.V2CheckpointJson
    case other => FileKind.Other(other)
  }

  def commitVersion(path: Path): Long = path.getName match {
    case CommitName(version) => version.toLong
    case other => throw new IllegalArgumentException(s"not a commit file: $other")
  }

  /** What the store does with the write of a commit file. */
  sealed trait Behaviour

  /** Write it as `HDFSLogStore` does. */
  case object Delegate extends Behaviour

  /** Return as if written, without creating the file. */
  case object ReturnWithoutWriting extends Behaviour

  /** Throw instead of writing. */
  final case class Fail(error: () => Throwable) extends Behaviour

  /** Only settings whose key contains this are copied into the journal. */
  val ProbeSettingMarker = "lakehouse.probe"

  private val nextInstance = new AtomicInteger
  private val journal = new ConcurrentLinkedQueue[Event]

  @volatile private var partialWriteVisible = true
  @volatile private var commitBehaviour: Long => Behaviour = _ => Delegate

  /** What `isPartialWriteVisible` answers; `HDFSLogStore` answers `true`. */
  def answerPartialWriteVisible(visible: Boolean): Unit = partialWriteVisible = visible

  /**
   * Decides each write of a commit file from its version, on the thread that writes it and
   * before anything is written. The function may block: that holds the commit at that point.
   */
  def onCommitWrite(behaviour: Long => Behaviour): Unit = commitBehaviour = behaviour

  /** Every event since the last `clearJournal`, in the order the calls were made. */
  def events: Seq[Event] = journal.iterator.asScala.toSeq

  def clearJournal(): Unit = journal.clear()

  /** Empties the journal and turns both switches back to `HDFSLogStore`'s behaviour. */
  def reset(): Unit = {
    partialWriteVisible = true
    commitBehaviour = _ => Delegate
    journal.clear()
  }

  private def record(event: Event): Unit = journal.add(event): Unit

  private def probeSettings(conf: Configuration): Map[String, String] =
    conf.iterator.asScala
      .filter(_.getKey.contains(ProbeSettingMarker))
      .map(e => e.getKey -> e.getValue)
      .toMap
}
