package lakehouse.ingestion.it.stress

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit

import scala.annotation.tailrec
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
 * The caller creates the table first (`StressTable.create`); the harness only runs writers.
 */
object StressHarness {

  /**
   * @param extraJvmOptions added to every writer's command line, typically `-Dspark.*` settings
   *   (the LogStore, a delaying file system)
   */
  final case class Settings(
      writers: Int,
      commits: Int,
      rowsPerCommit: Int,
      writerHeap: String = "512m",
      extraJvmOptions: Seq[String] = Nil,
      readyTimeout: FiniteDuration = 2.minutes,
      runTimeout: FiniteDuration = 15.minutes
  ) {
    require(writers > 0 && commits > 0 && rowsPerCommit > 0, s"invalid settings: $this")
  }

  /**
   * How one writer ended: its exit code (`StressWriter.ExitCode`), what it journaled, and what it
   * resolved (`WriterProbe`; `None` if it never got past its setup).
   */
  final case class WriterOutcome(
      writer: Int,
      exitCode: Int,
      journal: AckJournal.Contents,
      log: Path,
      probe: Option[WriterProbe] = None
  )

  /** The run itself went wrong (a writer never got ready, a timeout), not what it measured. */
  final class HarnessFailure(message: String) extends RuntimeException(message)

  private val ReadyPoll = 50.millis
  private val LogTailLines = 40

  def run(stressRun: StressRun, settings: Settings): Seq[WriterOutcome] = {
    val writers = 0 until settings.writers
    // Writers wait at the barrier a little longer than the orchestrator waits for them, so a
    // slow start fails here, with a clear message, rather than in a writer.
    val startTimeout = settings.readyTimeout + 30.seconds
    val processes = writers.map(i => i -> launch(stressRun, settings, i, startTimeout)).toMap
    try {
      awaitReady(stressRun, processes, settings.readyTimeout.fromNow)
      Files.createFile(stressRun.startSignal)
      val deadline = settings.runTimeout.fromNow
      writers.map { i =>
        val exitCode = awaitExit(stressRun, i, processes(i), deadline)
        WriterOutcome(
          i,
          exitCode,
          AckJournal.read(stressRun.journal(i)),
          stressRun.log(i),
          WriterProbe.read(stressRun.probe(i))
        )
      }
    } finally processes.values.filter(_.isAlive).foreach(_.destroyForcibly())
  }

  private def launch(
      stressRun: StressRun,
      settings: Settings,
      writer: Int,
      startTimeout: FiniteDuration
  ): Process = {
    val config = WriterConfig(
      stressRun.root,
      writer,
      settings.commits,
      settings.rowsPerCommit,
      startTimeout
    )
    val command =
      Seq(javaExecutable) ++
        JavaModuleOptions.defaultModuleOptionArray().toSeq ++
        Seq(s"-Xmx${settings.writerHeap}", "-Duser.timezone=UTC") ++
        settings.extraJvmOptions ++
        Seq("-cp", classpath, StressWriter.getClass.getName.stripSuffix("$")) ++
        config.toArgs

    val builder = new ProcessBuilder(command.asJava)
      .redirectErrorStream(true)
      .redirectOutput(stressRun.log(writer).toFile)
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
