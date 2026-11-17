package lakehouse.ingestion.it.stress

import java.nio.file.{Files, Path}
import java.sql.Connection

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Using

import lakehouse.ingestion.it.stress.StressHarness.Chaos
import lakehouse.ingestion.it.stress.WriterKiller._
import lakehouse.ingestion.logstore.FaultPoint

/**
 * Kills a writer at a chosen point of a commit, from outside (ADR-0004 §7.2).
 *
 * Nothing in the writer cooperates. It runs the LogStore that is deployed, loaded by name, with
 * one of its fault hooks set to a wait: the writer is then held at that point of every commit
 * for a few seconds. The killer does not know when. It looks, many times a second, at what a
 * process at that point leaves for anybody to see, in the registry and in the log directory,
 * and kills the writer when it sees it:
 *
 *  - '''before the `INSERT`''': a temporary file that no entry points at, and that is older
 *    than any writer needs to go from writing it to claiming its version;
 *  - '''after the `INSERT`''': an entry of the writer's process that is not complete, whose
 *    commit file is not in the log;
 *  - '''after the publish''': an entry of the writer's process that is not complete, whose
 *    commit file is in the log.
 *
 * A kill is reported with what was seen when it was decided, how long the process took to be
 * gone after that look, and what was read once more right after: what the writer left. The
 * two may differ, and both are told. The writers that are alive finish the commit of a dead
 * one when they next look at the table, and that may be in the milliseconds between.
 *
 * @param connection a connection to the coordination database, on a path that a run does not
 *   break; it is the killer's, which looks through it many times a second, and closes it
 * @param tableUri the key the table is coordinated under
 * @param logDir the table's `_delta_log`
 */
final class WriterKiller(connection: Connection, tableUri: String, logDir: Path)
    extends AutoCloseable {

  override def close(): Unit = connection.close()

  /**
   * Kills `writer` the first time it is seen at `point`, once it has acknowledged
   * `afterAcknowledging` batches.
   *
   * @throws java.lang.IllegalStateException the writer was not seen there in time
   */
  def killAt(
      chaos: Chaos,
      writer: Int,
      point: FaultPoint,
      afterAcknowledging: Int,
      within: FiniteDuration = 2.minutes
  ): Killed = {
    val pid = chaos.pid(writer)
    val deadline = within.fromNow

    /** What was seen, and when the look that saw it began. */
    @tailrec
    def await(): (Seen, Long) = {
      val looked = System.nanoTime()
      val seen =
        if (chaos.acknowledged(writer).size >= afterAcknowledging) sighting(pid, point) else None
      seen match {
        case Some(found) => (found, looked)
        case None =>
          if (!chaos.isAlive(writer))
            throw new IllegalStateException(
              s"${StressTable.writerId(writer)} exited before it was seen at ${point.key}"
            )
          if (deadline.isOverdue())
            throw new IllegalStateException(
              s"${StressTable.writerId(writer)} was not seen at ${point.key} within $within"
            )
          Thread.sleep(Poll.toMillis)
          await()
      }
    }

    val (seen, looked) = await()
    kill(chaos, writer, Some(point), Some(seen), looked)
  }

  /** Kills `writer` wherever it is, and says what it left. */
  def kill(chaos: Chaos, writer: Int): Killed = kill(chaos, writer, None, None, System.nanoTime())

  private def kill(
      chaos: Chaos,
      writer: Int,
      aimedAt: Option[FaultPoint],
      seen: Option[Seen],
      looked: Long
  ): Killed = {
    val pid = chaos.pid(writer)
    chaos.kill(writer)
    val goneAfter = (System.nanoTime() - looked).nanos
    val prepared = seen match {
      // Of the files the writer was seen holding, the ones that still belong to no commit. A
      // writer that is alive has claimed its version with its file by now, or removed it.
      case Some(Prepared(temps)) =>
        val orphans = orphanTemps(olderThan = Duration.Zero).toSet
        temps.filter(orphans)
      case _ => Nil
    }
    val entries = entriesOf(pid)
    Killed(
      writer,
      pid,
      aimedAt,
      seen,
      goneAfter,
      acknowledged = chaos.acknowledged(writer).size,
      entriesOfProcess = entries.size,
      run = entries.flatMap(_.run).lastOption,
      unfinished = unfinished(entries),
      preparedLeft = prepared
    )
  }

  /** What shows that the process `pid` is at `point`, if it is there. */
  private def sighting(pid: Long, point: FaultPoint): Option[Seen] = point match {
    case FaultPoint.BeforeInsert =>
      val held = orphanTemps(olderThan = HeldForAtLeast)
      // The writer has no unfinished entry of its own: it is not further on in a commit.
      Option.when(held.nonEmpty && unfinishedOf(pid).isEmpty)(Prepared(held))
    case FaultPoint.AfterInsert => unfinishedOf(pid).find(!_.published)
    case FaultPoint.AfterPublish => unfinishedOf(pid).find(_.published)
  }

  /** The versions that the process took and whose entries are not complete. */
  private def unfinishedOf(pid: Long): Seq[InFlight] = unfinished(entriesOf(pid))

  private def unfinished(entries: Seq[Entry]): Seq[InFlight] =
    entries.filterNot(_.complete).map(e => InFlight(e.version, isInLog(e.version)))

  private def isInLog(version: Long): Boolean = Files.exists(logDir.resolve(f"$version%020d.json"))

  /** The entries that the process `pid` took, lowest version first. */
  private def entriesOf(pid: Long): Seq[Entry] =
    Using.resource(connection.prepareStatement(EntriesOfProcess)) { statement =>
      statement.setString(1, tableUri)
      // host:pid:spark.app.id
      statement.setString(2, s"%:$pid:%")
      Using.resource(statement.executeQuery()) { rows =>
        Iterator
          .continually(rows)
          .takeWhile(_.next())
          .map(row => Entry(row.getLong(1), row.getBoolean(2), Option(row.getString(3))))
          .toList
      }
    }

  /** The temporary files of commits that no entry points at, and that are at least that old. */
  private def orphanTemps(olderThan: FiniteDuration): Seq[String] = {
    val directory = logDir.resolve(".tmp")
    if (!Files.isDirectory(directory)) Nil
    else {
      val now = System.currentTimeMillis()
      val old = Using.resource(Files.list(directory)) {
        _.iterator.asScala
          .filter(file => TempOfCommit.matches(file.getFileName.toString))
          .filter(file => now - lastModified(file) >= olderThan.toMillis)
          .map(file => s".tmp/${file.getFileName}")
          .toList
      }
      if (old.isEmpty) Nil else old.filterNot(tempPathsOfEntries())
    }
  }

  /** 0 for a file that is gone by now: a writer that lost its version removes its own. */
  private def lastModified(file: Path): Long =
    try Files.getLastModifiedTime(file).toMillis
    catch { case _: java.nio.file.NoSuchFileException => Long.MaxValue }

  private def tempPathsOfEntries(): Set[String] =
    Using.resource(connection.prepareStatement(TempPaths)) { statement =>
      statement.setString(1, tableUri)
      Using.resource(statement.executeQuery()) { rows =>
        Iterator.continually(rows).takeWhile(_.next()).map(_.getString(1)).toSet
      }
    }
}

object WriterKiller {

  /** What the killer saw of a writer when it decided to kill it. */
  sealed trait Seen {
    def describe: String
  }

  /**
   * Temporary files of commits that no entry pointed at, kept for longer than a writer that is
   * not held keeps one: a commit that is ready, and no version taken for it.
   */
  final case class Prepared(temps: Seq[String]) extends Seen {
    override def describe: String =
      s"${files(temps.size)} of a commit that no entry pointed at, kept for " +
        s"${HeldForAtLeast.toMillis} ms or more"
  }

  /**
   * A version that the process had taken and whose entry was not complete, and whether its
   * commit file was in the log.
   */
  final case class InFlight(version: Long, published: Boolean) extends Seen {
    override def describe: String =
      if (published) s"version $version of that process in the log, and its entry not complete"
      else s"version $version taken by that process, and its commit not in the log"
  }

  /**
   * A kill: what was seen when it was decided, and what the writer left, read right after its
   * process was gone.
   *
   * @param aimedAt the point the kill was aimed at, or `None` for a kill at any moment
   * @param seen what showed the writer at that point; `None` for a kill at any moment
   * @param goneAfter from the start of the look that saw it (for a kill at any moment, from the
   *   decision) to the process being gone: no longer than this passed between what was seen
   *   and the death
   * @param acknowledged how many batches the writer had acknowledged
   * @param entriesOfProcess how many versions the killed process had taken
   * @param run the run that the entries of the process name: the one its commits are put down
   *   to in the registry. `None` if it had taken no version
   * @param unfinished its entries that were not complete: the commit it died in, if it had
   *   taken its version and nobody had finished the commit yet
   * @param preparedLeft of the temporary files it was seen holding, the ones that no entry
   *   pointed at after the kill either
   */
  final case class Killed(
      writer: Int,
      pid: Long,
      aimedAt: Option[FaultPoint],
      seen: Option[Seen],
      goneAfter: FiniteDuration,
      acknowledged: Int,
      entriesOfProcess: Int,
      run: Option[String],
      unfinished: Seq[InFlight],
      preparedLeft: Seq[String]
  ) {

    /** The batch that was in flight: the one after the last that the writer acknowledged. */
    def batchInFlight: Long = acknowledged + 1L

    /**
     * What the process left, as far as it can be put down to it. Entries name their process. A
     * temporary file does not, so one counts only if the writer was seen holding it.
     */
    def left: String = unfinished.lastOption match {
      case Some(InFlight(version, false)) =>
        s"version $version taken, and its commit not in the log"
      case Some(InFlight(version, true)) =>
        s"version $version in the log, and its entry not complete"
      case None if preparedLeft.nonEmpty =>
        s"no entry of its own unfinished, and ${files(preparedLeft.size)} of a commit that no " +
          "entry points at"
      case None => "no entry of its own unfinished"
    }
  }

  final private case class Entry(version: Long, complete: Boolean, run: Option[String])

  private def files(count: Int): String =
    if (count == 1) "1 temporary file" else s"$count temporary files"

  private val Poll = 10.millis

  /**
   * A writer goes from writing its temporary file to claiming its version in milliseconds. One
   * that is held before the `INSERT` keeps the file for the seconds of its wait.
   */
  private val HeldForAtLeast = 400.millis

  private val TempOfCommit = """\d{20}\.json\..+""".r

  private val EntriesOfProcess =
    """SELECT version, complete, run_id::text FROM delta_coordination.commit_entry
      |WHERE table_uri = ? AND writer_instance LIKE ? ORDER BY version""".stripMargin

  private val TempPaths =
    "SELECT temp_path FROM delta_coordination.commit_entry WHERE table_uri = ?"
}
