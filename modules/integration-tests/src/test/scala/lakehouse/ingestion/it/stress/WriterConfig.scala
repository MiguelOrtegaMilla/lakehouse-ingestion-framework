package lakehouse.ingestion.it.stress

import java.nio.file.{Path, Paths}

import scala.concurrent.duration._

/**
 * What one writer JVM does, passed on its command line as `key=value` arguments, so that `ps`
 * shows each writer's role.
 *
 * Spark settings are not part of it: they travel as `-Dspark.*` JVM options, which a
 * `SparkConf` loads by itself (that is also how `spark-submit` hands settings to a driver).
 *
 * @param startTimeout how long the writer waits at the start barrier before giving up
 * @param guard whether the writer, before it touches the table, must find itself set up to
 *   coordinate its commits (`StartupGuard`), and refuse to run otherwise. A writer that is to
 *   run on Delta's default LogStore, as in the RED spike, is not asked.
 * @param firstCommit the `txnVersion` the process starts at: 1, or, for a process started in
 *   the place of one that was killed, the one after the last that the writer acknowledged. It
 *   repeats the batch that was in flight, as the engine repeats a batch whose outcome it does
 *   not know, and Delta skips it if it is in the table.
 * @param restarted whether the process takes the place of one that was killed. It then goes on
 *   in the writer's journal, and does not wait at the start barrier, which is long open.
 * @param repeatFor for how long the writer repeats a batch that fails, counted from the first
 *   failure of the batch; zero for a writer that stops at its first failure. The batch is
 *   repeated with its `txnVersion`, as the engine repeats a batch whose outcome it does not
 *   know.
 * @param epoch the epoch that the process commits under, of the key of its writer (ADR-0019);
 *   zero for a process that commits under none. The first process of a writer has epoch 1. One
 *   with a later epoch takes the writer over while the process before it may still be
 *   running: it has a journal of its own, waits to be let go and not at the start barrier,
 *   announces its epoch, and goes on from the batch that the table then says the writer is at.
 */
final case class WriterConfig(
    runDir: Path,
    writer: Int,
    commits: Int,
    rowsPerCommit: Int,
    startTimeout: FiniteDuration,
    guard: Boolean,
    firstCommit: Int = 1,
    restarted: Boolean = false,
    repeatFor: FiniteDuration = Duration.Zero,
    epoch: Int = 0
) {

  /** Whether the process takes its writer over from one that had the turn before it. */
  def takesOver: Boolean = epoch > 1

  def toArgs: Seq[String] = Seq(
    s"${WriterConfig.RunDir}=$runDir",
    s"${WriterConfig.Writer}=$writer",
    s"${WriterConfig.Commits}=$commits",
    s"${WriterConfig.RowsPerCommit}=$rowsPerCommit",
    s"${WriterConfig.StartTimeoutSeconds}=${startTimeout.toSeconds}",
    s"${WriterConfig.Guard}=$guard",
    s"${WriterConfig.FirstCommit}=$firstCommit",
    s"${WriterConfig.Restarted}=$restarted",
    s"${WriterConfig.RepeatSeconds}=${repeatFor.toSeconds}",
    s"${WriterConfig.Epoch}=$epoch"
  )
}

object WriterConfig {

  private val RunDir = "run-dir"
  private val Writer = "writer"
  private val Commits = "commits"
  private val RowsPerCommit = "rows-per-commit"
  private val StartTimeoutSeconds = "start-timeout-seconds"
  private val Guard = "guard"
  private val FirstCommit = "first-commit"
  private val Restarted = "restarted"
  private val RepeatSeconds = "repeat-seconds"
  private val Epoch = "epoch"

  private val Keys = Set(
    RunDir,
    Writer,
    Commits,
    RowsPerCommit,
    StartTimeoutSeconds,
    Guard,
    FirstCommit,
    Restarted,
    RepeatSeconds,
    Epoch
  )

  def fromArgs(args: Seq[String]): Either[String, WriterConfig] = {
    val pairs = args.map(_.split("=", 2).toList)
    val malformed = args.zip(pairs).collect { case (arg, List(_)) => arg }
    val values = pairs.collect { case List(k, v) => k -> v }.toMap
    val unknown = values.keySet -- Keys
    val missing = Keys -- values.keySet

    def nonNegative(key: String): Either[String, Int] =
      values(key).toIntOption.filter(_ >= 0).toRight(s"$key must be a non-negative integer")
    def positive(key: String): Either[String, Int] =
      values(key).toIntOption.filter(_ > 0).toRight(s"$key must be a positive integer")

    if (malformed.nonEmpty) Left(s"expected key=value, got: ${malformed.mkString(" ")}")
    else if (unknown.nonEmpty) Left(s"unknown arguments: ${unknown.toSeq.sorted.mkString(", ")}")
    else if (missing.nonEmpty) Left(s"missing arguments: ${missing.toSeq.sorted.mkString(", ")}")
    else
      for {
        writer <- nonNegative(Writer)
        commits <- positive(Commits)
        rows <- positive(RowsPerCommit)
        timeout <- positive(StartTimeoutSeconds)
        guard <- values(Guard).toBooleanOption.toRight(s"$Guard must be true or false")
        first <- positive(FirstCommit)
        restarted <- values(Restarted).toBooleanOption.toRight(s"$Restarted must be true or false")
        repeat <- nonNegative(RepeatSeconds)
        epoch <- nonNegative(Epoch)
        _ <- Either.cond(
          !(restarted && epoch > 0),
          (),
          s"$Restarted and $Epoch are not given together: a process is started in the place of " +
            "one that was killed, or takes over from one that is running"
        )
      } yield WriterConfig(
        Paths.get(values(RunDir)),
        writer,
        commits,
        rows,
        timeout.seconds,
        guard,
        first,
        restarted,
        repeat.seconds,
        epoch
      )
  }
}
