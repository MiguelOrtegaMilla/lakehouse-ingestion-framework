package lakehouse.ingestion.it.support

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import lakehouse.ingestion.cli.Main
import org.apache.spark.launcher.JavaModuleOptions
import org.scalatest.Assertions

/**
 * The command line of the application in a JVM of its own, as `cli/run` starts it: with the
 * classpath of this JVM, which has Spark, and the options of a JVM that embeds Spark. It ends
 * with the code of its command by `sys.exit`, as a deployment sees it.
 *
 * A process is started and awaited apart, so that a test may have several at once.
 */
final class CommandProcess private (process: Process, log: Path) {

  /**
   * Waits for the process to end, and fails the test if it does not: the process is then
   * killed, and the failure has the end of what it printed.
   */
  def await(within: FiniteDuration = 3.minutes): CommandProcess.Ended = {
    if (!process.waitFor(within.toMillis, TimeUnit.MILLISECONDS)) {
      process.destroyForcibly()
      Assertions.fail(s"the process did not end in $within:\n${CommandProcess.tail(log)}")
    }
    CommandProcess.Ended(process.exitValue(), CommandProcess.tail(log))
  }
}

object CommandProcess {

  /**
   * How a process ended.
   *
   * @param log the end of what it printed, for the clue of a test that expected another code
   */
  final case class Ended(exit: Int, log: String)

  /** A heap that a run of a few small files fits in, beside the JVM of the tests (§7.3). */
  val Heap = "768m"

  /**
   * Starts the command line, and does not wait for it.
   *
   * @param arguments those of the application, the command first
   * @param log the file that takes what the process prints, both streams in one
   */
  def start(arguments: Seq[String], log: Path, heap: String = Heap): CommandProcess = {
    val command =
      Seq(Paths.get(sys.props("java.home"), "bin", "java").toString) ++
        JavaModuleOptions.defaultModuleOptionArray().toSeq ++
        Seq(s"-Xmx$heap", "-Duser.timezone=UTC", "-cp", sys.props("java.class.path")) ++
        Seq(Main.getClass.getName.stripSuffix("$")) ++
        arguments
    val builder = new ProcessBuilder(command.asJava)
      .redirectErrorStream(true)
      .redirectOutput(log.toFile)
    // Same reason as in project/SparkTestJvm.scala: no hostname lookup at session start.
    builder.environment().put("SPARK_LOCAL_IP", "127.0.0.1")
    new CommandProcess(builder.start(), log)
  }

  private def tail(log: Path): String =
    Files.readAllLines(log, UTF_8).asScala.takeRight(40).mkString("\n")
}
