package lakehouse.ingestion.it.stress

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

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
 * and start a process in its place, which goes on from the batch that was in flight.
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
   */
  final case class Settings(
      writers: Int,
      commits: Int,
      rowsPerCommit: Int,
      writerHeap: String = "512m",
      extraJvmOptions: Seq[String] = Nil,
      guard: Boolean = false,
      alsoFor: Map[Int, Seq[String]] = Map.empty,
      readyTimeout: FiniteDuration = 2.minutes,
      runTimeout: FiniteDuration = 15.minutes
  ) {
    require(writers > 0 && commits > 0 && rowsPerCommit > 0, s"invalid settings: $this")
  }

  /**
   * How one writer ended: its exit code (`StressWriter.ExitCode`), what it journaled, and what it
   * resolved (`WriterProbe`; `None` if it never got past its setup). For a writer that was
   * killed and started again, the exit code and the probe are those of its last process.
   *
   * @param runs the run of each process of the writer, in order (`StressRun.runs`)
   * @param kills how many times the writer was killed during the run
   */
  final case class WriterOutcome(
      writer: Int,
      exitCode: Int,
      journal: AckJournal.Contents,
      log: Path,
      probe: Option[WriterProbe] = None,
      runs: Seq[String] = Nil,
      kills: Int = 0
  )

  /**
   * What a run may do to its writers while they commit. Everything here is done from outside
   * the writer, to a process that does not know of it.
   */
  final class Chaos private[StressHarness] (
      stressRun: StressRun,
      settings: Settings,
      processes: mutable.Map[Int, Process],
      kills: mutable.Map[Int, Int]
  ) {

    /** The operating-system process that is the writer now. */
    def pid(writer: Int): Long = processes(writer).pid()

    def isAlive(writer: Int): Boolean = processes(writer).isAlive

    /** What the writer has acknowledged so far, as its journal on disk says. */
    def acknowledged(writer: Int): Vector[Long] = AckJournal.read(stressRun.journal(writer)).acked

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
    try {
      writers.foreach(i => processes(i) = launch(stressRun, settings, i, startTimeout))
      awaitReady(stressRun, processes.toMap, settings.readyTimeout.fromNow)
      Files.createFile(stressRun.startSignal)
      val deadline = settings.runTimeout.fromNow
      chaos(new Chaos(stressRun, settings, processes, kills))
      writers.map { i =>
        val exitCode = awaitExit(stressRun, i, processes(i), deadline)
        WriterOutcome(
          i,
          exitCode,
          AckJournal.read(stressRun.journal(i)),
          stressRun.log(i),
          WriterProbe.read(stressRun.probe(i)),
          runsOf(stressRun, i),
          kills.getOrElse(i, 0)
        )
      }
    } finally processes.values.filter(_.isAlive).foreach(_.destroyForcibly())
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
      restarted: Boolean = false
  ): Process = {
    val config = WriterConfig(
      stressRun.root,
      writer,
      settings.commits,
      settings.rowsPerCommit,
      startTimeout,
      settings.guard,
      firstCommit,
      restarted
    )
    val command =
      Seq(javaExecutable) ++
        JavaModuleOptions.defaultModuleOptionArray().toSeq ++
        Seq(s"-Xmx${settings.writerHeap}", "-Duser.timezone=UTC") ++
        settings.extraJvmOptions ++
        (if (restarted) Nil else settings.alsoFor.getOrElse(writer, Nil)) ++
        Seq("-cp", classpath, StressWriter.getClass.getName.stripSuffix("$")) ++
        config.toArgs

    // One log for the writer, whatever the number of its processes.
    val log = stressRun.log(writer).toFile
    val builder = new ProcessBuilder(command.asJava)
      .redirectErrorStream(true)
      .redirectOutput(
        if (restarted) ProcessBuilder.Redirect.appendTo(log) else ProcessBuilder.Redirect.to(log)
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

  private def awaitExit(stressRun: StressRun, writer: Int, p: Process, deadline: Deadline): Int =
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
