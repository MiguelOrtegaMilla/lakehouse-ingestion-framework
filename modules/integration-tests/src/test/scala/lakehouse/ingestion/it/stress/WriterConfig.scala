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
 */
final case class WriterConfig(
    runDir: Path,
    writer: Int,
    commits: Int,
    rowsPerCommit: Int,
    startTimeout: FiniteDuration
) {

  def toArgs: Seq[String] = Seq(
    s"${WriterConfig.RunDir}=$runDir",
    s"${WriterConfig.Writer}=$writer",
    s"${WriterConfig.Commits}=$commits",
    s"${WriterConfig.RowsPerCommit}=$rowsPerCommit",
    s"${WriterConfig.StartTimeoutSeconds}=${startTimeout.toSeconds}"
  )
}

object WriterConfig {

  private val RunDir = "run-dir"
  private val Writer = "writer"
  private val Commits = "commits"
  private val RowsPerCommit = "rows-per-commit"
  private val StartTimeoutSeconds = "start-timeout-seconds"

  private val Keys = Set(RunDir, Writer, Commits, RowsPerCommit, StartTimeoutSeconds)

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
      } yield WriterConfig(Paths.get(values(RunDir)), writer, commits, rows, timeout.seconds)
  }
}
