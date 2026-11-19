package lakehouse.ingestion.it.stress

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import lakehouse.ingestion.it.stress.LogModel.Turn
import org.apache.spark.launcher.JavaModuleOptions

/**
 * Runs N stress writers as separate JVMs against one table (ADR-0004 §7.2).
 *
 * Separate JVMs, not threads: the race under test is between processes, and anything JVM-wide
 * (a static lock, a cached `DeltaLog`) would hide it. Each writer gets the test JVM's own
 * classpath and Spark's own module options, read from Spark at run time.
 *
 * The orchestrator launches every writer, waits until all of them are ready, then creates the
 * start signal, so that they begin committing within milliseconds of each other. Whatever
 * happens, no writer outlives `run`.
 *
 * While the writers commit, a run may do things to them (`Chaos`): kill one, as `kill -9` does,
 * and start a process in its place, which goes on from the batch that was in flight. Or, in a
 * run whose writers commit under epochs (`Settings.epochs`), have a process take a writer over
 * under the next epoch while the one before it is still committing, and is told nothing.
 *
 * The caller creates the table first (`StressTable.create`); the harness only runs writers.
 */
object StressHarness {

  /**
   * @param extraJvmOptions added to every writer's command line, typically `-Dspark.*` settings
   *   (the LogStore, a delaying file system)
   * @param guard whether every writer must pass the startup guard of `PostgresLogStore` before
   *   it touches the table (`WriterConfig.guard`)
   * @param alsoFor more options for the first process of one writer and of no other, such as
   *   the fault hook that holds the writer that a run is going to kill. A process started in
   *   its place does not get them.
   * @param repeatFor for how long a writer repeats a batch that fails (`WriterConfig.repeatFor`);
   *   zero, and it stops at its first failure
   * @param epochs whether every writer commits under an epoch of its own key (ADR-0019): its
   *   first process under epoch 1, and each process that takes it over under the next
   */
  final case class Settings(
      writers: Int,
      commits: Int,
      rowsPerCommit: Int,
      writerHeap: String = "512m",
      extraJvmOptions: Seq[String] = Nil,
      guard: Boolean = false,
      alsoFor: Map[Int, Seq[String]] = Map.empty,
      repeatFor: FiniteDuration = Duration.Zero,
      readyTimeout: FiniteDuration = 2.minutes,
      runTimeout: FiniteDuration = 15.minutes,
      epochs: Boolean = false
  ) {
    require(writers > 0 && commits > 0 && rowsPerCommit > 0, s"invalid settings: $this")
  }

  /**
   * One process of a writer in a run with epochs, as it ended.
   *
   * @param epoch the epoch that it committed under
   * @param exitCode `StressWriter.ExitCode.OutOfTurn` for one that was taken over and refused
   * @param acked the batches that it acknowledged itself, in its own journal
   */
  final case class WriterProcess(epoch: Int, exitCode: Int, acked: Vector[Long])

  /**
   * How one writer ended: its exit code (`StressWriter.ExitCode`), what it journaled, and what it
   * resolved (`WriterProbe`; `None` if it never got past its setup). For a writer that was
   * killed and started again, the exit code and the probe are those of its last process.
   *
   * In a run with epochs, the exit code and the probe are those of the process that had the
   * turn at the end, and the journal is what the processes of the writer acknowledged between
   * them: each batch once, whichever of them acknowledged it.
   *
   * @param runs the run of each process of the writer, in order (`StressRun.runs`)
   * @param kills how many times the writer was killed during the run
   * @param processes in a run with epochs, every process of the writer, by its epoch
   * @param turns in a run with epochs, what each process wrote down of its turn
   */
  final case class WriterOutcome(
      writer: Int,
      exitCode: Int,
      journal: AckJournal.Contents,
      log: Path,
      probe: Option[WriterProbe] = None,
      runs: Seq[String] = Nil,
      kills: Int = 0,
      processes: Seq[WriterProcess] = Nil,
      turns: Seq[Turn] = Nil
  )

  /**
   * What a run may do to its writers while they commit. Everything here is done from outside
   * the writer, to a process that does not know of it.
   */
  final class Chaos private[StressHarness] (
      stressRun: StressRun,
      settings: Settings,
      processes: mutable.Map[Int, Process],
      kills: mutable.Map[Int, Int],
      epochs: mutable.Map[Int, Int],
      superseded: mutable.Buffer[(Int, Int, Process)]
  ) {

    /** The operating-system process that is the writer now. */
    def pid(writer: Int): Long = processes(writer).pid()

    def isAlive(writer: Int): Boolean = processes(writer).isAlive

    /**
     * What the writer has acknowledged so far, as its journal on disk says; in a run with
     * epochs, as the journals of its processes say between them.
     */
    def acknowledged(writer: Int): Vector[Long] =
      journalOf(stressRun, writer, epochs.getOrElse(writer, 0)).acked

    /** The epoch of the last process that was started for the writer. */
    def epochOf(writer: Int): Int = epochs(writer)

    /**
     * Starts the process that is to take the writer over, under the next epoch, and waits until
     * it could: its session is up and the table is loaded. It then waits to be let go
     * (`takeOver`). The process that has the turn goes on committing, and is told nothing.
     *
     * @return the epoch of the process that was started
     * @throws HarnessFailure it did not get that far within the time a writer has to get ready
     */
    def standBy(writer: Int): Int = {
      require(settings.epochs, "the writers of this run commit under no epoch")
      val next = epochs(writer) + 1
      val standby =
        launch(stressRun, settings, writer, settings.readyTimeout + 30.seconds, epoch = next)
      superseded += ((writer, epochs(writer), processes(writer)))
      processes(writer) = standby
      epochs(writer) = next
      val deadline = settings.readyTimeout.fromNow
      while (!Files.exists(stressRun.turnReady(writer, next))) {
        val id = StressTable.writerId(writer)
        if (!standby.isAlive)
          throw failure(
            stressRun,
            writer,
            s"exited before it could take $id over under epoch $next"
          )
        if (deadline.isOverdue())
          throw failure(stressRun, writer, s"was not ready to take $id over under epoch $next")
        Thread.sleep(ReadyPoll.toMillis)
      }
      next
    }

    /**
     * Lets the process that stands by take the writer over: it announces its epoch, asks the
     * table which batch the writer is at, and goes on from there. Returns once it has written
     * its turn down, which is before its first commit.
     *
     * @return the turn, as the process wrote it down
     * @throws HarnessFailure the process exited, or wrote nothing down within `within`
     */
    def takeOver(writer: Int, within: FiniteDuration = 2.minutes): Turn = {
      val epoch = epochs(writer)
      Files.createFile(stressRun.turnGo(writer, epoch))
      val deadline = within.fromNow
      @tailrec
      def await(): Turn =
        turnsOf(stressRun, writer).find(_.epoch == epoch.toLong) match {
          case Some(turn) => turn
          case None =>
            val id = StressTable.writerId(writer)
            if (!processes(writer).isAlive)
              throw failure(stressRun, writer, s"exited before it took $id over under epoch $epoch")
            if (deadline.isOverdue())
              throw failure(stressRun, writer, s"did not take $id over under epoch $epoch")
            Thread.sleep(AckPoll.toMillis)
            await()
        }
      await()
    }

    /**
     * Waits until every process of the writer that was taken over has exited, and gives how
     * each ended, by its epoch.
     *
     * @throws HarnessFailure one of them was still running after `within`
     */
    def awaitSuperseded(writer: Int, within: FiniteDuration = 2.minutes): Seq[(Int, Int)] = {
      val deadline = within.fromNow
      superseded.toSeq.collect { case (`writer`, epoch, process) =>
        if (!process.waitFor(deadline.timeLeft.max(Duration.Zero).toMillis, TimeUnit.MILLISECONDS))
          throw failure(
            stressRun,
            writer,
            s"was taken over under epoch ${epoch + 1}, and its process of epoch $epoch was " +
              s"still running $within later"
          )
        epoch -> process.exitValue()
      }
    }

    /**
     * Waits until the writer has acknowledged that many batches.
     *
     * @throws HarnessFailure the writer exited first, or took longer than `within`
     */
    def awaitAcknowledged(writer: Int, batches: Int, within: FiniteDuration = 2.minutes): Unit = {
      val deadline = within.fromNow
      while (acknowledged(writer).size < batches) {
        val id = StressTable.writerId(writer)
        if (!isAlive(writer))
          throw new HarnessFailure(s"$id exited before it acknowledged $batches batches")
        if (deadline.isOverdue())
          throw new HarnessFailure(s"$id did not acknowledge $batches batches within $within")
        Thread.sleep(AckPoll.toMillis)
      }
    }

    /**
     * Kills the writer as `kill -9` does: the process gets no chance to finish, flush or clean
     * up anything. Returns when it is gone.
     */
    def kill(writer: Int): Unit = {
      val process = processes(writer)
      process.destroyForcibly().waitFor(): Unit
      kills(writer) = kills.getOrElse(writer, 0) + 1
    }

    /**
     * Starts a process in the place of a writer that was killed. It goes on from the batch
     * after the last one the writer acknowledged, which is the one that was in flight, and
     * without the options that only the writer's first process had.
     */
    def restart(writer: Int): Unit = {
      require(!settings.epochs, "a writer that commits under an epoch is taken over, not restarted")
      require(!processes(writer).isAlive, s"${StressTable.writerId(writer)} is still running")
      val next = acknowledged(writer).maxOption.getOrElse(0L).toInt + 1
      processes(writer) = launch(
        stressRun,
        settings,
        writer,
        startTimeout = settings.readyTimeout,
        firstCommit = next,
        restarted = true
      )
    }
  }

  /** The run itself went wrong (a writer never got ready, a timeout), not what it measured. */
  final class HarnessFailure(message: String) extends RuntimeException(message)

  private val ReadyPoll = 50.millis
  private val AckPoll = 20.millis
  private val LogTailLines = 40

  def run(stressRun: StressRun, settings: Settings): Seq[WriterOutcome] =
    run(stressRun, settings, _ => ())

  /**
   * @param chaos what is done to the writers once they are all committing. The writers that
   *   are running when it returns are then waited for.
   */
  def run(stressRun: StressRun, settings: Settings, chaos: Chaos => Unit): Seq[WriterOutcome] = {
    val writers = 0 until settings.writers
    // Writers wait at the barrier a little longer than the orchestrator waits for them, so a
    // slow start fails here, with a clear message, rather than in a writer.
    val startTimeout = settings.readyTimeout + 30.seconds
    val processes = mutable.Map.empty[Int, Process]
    val kills = mutable.Map.empty[Int, Int]
    // In a run with epochs: the epoch of the process that has the turn of each writer, and the
    // processes that were taken over, which go on until the LogStore refuses them.
    val first = if (settings.epochs) 1 else 0
    val epochs = mutable.Map.from(writers.map(_ -> first))
    val superseded = mutable.Buffer.empty[(Int, Int, Process)]
    try {
      writers.foreach { i =>
        processes(i) = launch(stressRun, settings, i, startTimeout, epoch = first)
      }
      awaitReady(stressRun, processes.toMap, settings.readyTimeout.fromNow)
      Files.createFile(stressRun.startSignal)
      val deadline = settings.runTimeout.fromNow
      chaos(new Chaos(stressRun, settings, processes, kills, epochs, superseded))
      writers.map { i =>
        val exitCode = awaitExit(stressRun, i, processes(i), deadline)
        val before = superseded.toSeq.collect { case (`i`, epoch, process) =>
          WriterProcess(
            epoch,
            awaitExit(stressRun, i, process, deadline),
            AckJournal.read(stressRun.journal(i, epoch)).acked
          )
        }
        val last = Option.when(settings.epochs)(
          WriterProcess(
            epochs(i),
            exitCode,
            AckJournal.read(stressRun.journal(i, epochs(i))).acked
          )
        )
        WriterOutcome(
          i,
          exitCode,
          journalOf(stressRun, i, epochs(i)),
          stressRun.log(i),
          WriterProbe.read(stressRun.probe(i, epochs(i))),
          runsOf(stressRun, i),
          kills.getOrElse(i, 0),
          (before ++ last).sortBy(_.epoch),
          turnsOf(stressRun, i)
        )
      }
    } finally
      (processes.values ++ superseded.map(_._3)).filter(_.isAlive).foreach(_.destroyForcibly())
  }

  /**
   * What a writer acknowledged, by all its processes up to the one of `epoch`. A process that
   * takes a writer over has a journal of its own, since the one before it may still be writing
   * to its own. A batch is acknowledged once, whichever of them did: a process that was taken
   * over may acknowledge a batch that the other wrote, since Delta skips a batch that the table
   * has, and says nothing. The failure that counts is that of the process that had the turn
   * last: one that was taken over stops because it was, and not at a batch that failed.
   */
  private def journalOf(stressRun: StressRun, writer: Int, epoch: Int): AckJournal.Contents = {
    val journals = (1.min(epoch) to epoch)
      .map(stressRun.journal(writer, _))
      .distinct
      .filter(Files.exists(_))
      .map(AckJournal.read)
    if (journals.size <= 1) journals.headOption.getOrElse(AckJournal.Contents(Vector.empty, None))
    else
      AckJournal.Contents(
        journals.flatMap(_.acked).distinct.sorted.toVector,
        journals.last.failure,
        journals.flatMap(_.repeated).toVector
      )
  }

  /** The turns that the processes of a writer wrote down, in the order of their epochs. */
  private def turnsOf(stressRun: StressRun, writer: Int): Seq[Turn] = {
    val file = stressRun.turns(writer)
    if (!Files.exists(file)) Nil
    else
      Files
        .readAllLines(file, UTF_8)
        .asScala
        .toSeq
        .flatMap(Turn.parse(StressTable.writerId(writer), _))
        .sortBy(_.epoch)
  }

  private def runsOf(stressRun: StressRun, writer: Int): Seq[String] = {
    val file = stressRun.runs(writer)
    if (!Files.exists(file)) Nil
    else Files.readAllLines(file, UTF_8).asScala.map(_.trim).filter(_.nonEmpty).toSeq
  }

  private def launch(
      stressRun: StressRun,
      settings: Settings,
      writer: Int,
      startTimeout: FiniteDuration,
      firstCommit: Int = 1,
      restarted: Boolean = false,
      epoch: Int = 0
  ): Process = {
    val config = WriterConfig(
      stressRun.root,
      writer,
      settings.commits,
      settings.rowsPerCommit,
      startTimeout,
      settings.guard,
      firstCommit,
      restarted,
      settings.repeatFor,
      epoch
    )
    // Not the first process of the writer: it goes on in the log of the writer, and has none
    // of the options that only the first one had.
    val later = restarted || config.takesOver
    val command =
      Seq(javaExecutable) ++
        JavaModuleOptions.defaultModuleOptionArray().toSeq ++
        Seq(s"-Xmx${settings.writerHeap}", "-Duser.timezone=UTC") ++
        settings.extraJvmOptions ++
        (if (later) Nil else settings.alsoFor.getOrElse(writer, Nil)) ++
        Seq("-cp", classpath, StressWriter.getClass.getName.stripSuffix("$")) ++
        config.toArgs

    // One log for the writer, whatever the number of its processes.
    val log = stressRun.log(writer).toFile
    val builder = new ProcessBuilder(command.asJava)
      .redirectErrorStream(true)
      .redirectOutput(
        if (later) ProcessBuilder.Redirect.appendTo(log) else ProcessBuilder.Redirect.to(log)
      )
    // Same reason as in project/SparkTestJvm.scala: no hostname lookup at session start.
    builder.environment().put("SPARK_LOCAL_IP", "127.0.0.1")
    builder.start()
  }

  @tailrec
  private def awaitReady(
      stressRun: StressRun,
      processes: Map[Int, Process],
      deadline: Deadline
  ): Unit = {
    val pending = processes.keys.filterNot(i => Files.exists(stressRun.readyMarker(i))).toSeq
    pending.find(i => !processes(i).isAlive).foreach { i =>
      throw failure(
        stressRun,
        i,
        s"exited with code ${processes(i).exitValue()} before the start barrier"
      )
    }
    if (pending.nonEmpty) {
      if (deadline.isOverdue())
        throw failure(stressRun, pending.min, s"not ready in time (also pending: $pending)")
      Thread.sleep(ReadyPoll.toMillis)
      awaitReady(stressRun, processes, deadline)
    }
  }

  private def awaitExit(
      stressRun: StressRun,
      writer: Int,
      p: Process,
      deadline: Deadline
  ): Int =
    if (p.waitFor(deadline.timeLeft.max(Duration.Zero).toMillis, TimeUnit.MILLISECONDS))
      p.exitValue()
    else throw failure(stressRun, writer, "did not finish before the run timeout")

  private def failure(stressRun: StressRun, writer: Int, what: String): HarnessFailure =
    new HarnessFailure(
      s"${StressTable.writerId(writer)} $what. Last lines of ${stressRun.log(writer)}:\n" +
        logTail(stressRun.log(writer))
    )

  private def logTail(log: Path): String =
    if (!Files.exists(log)) "(no log)"
    else Files.readAllLines(log, UTF_8).asScala.takeRight(LogTailLines).mkString("\n")

  private def javaExecutable: String =
    Paths.get(sys.props("java.home"), "bin", "java").toString

  /** The classpath of this (forked test) JVM, which holds the writer and its dependencies. */
  private def classpath: String = sys.props("java.class.path")
}
