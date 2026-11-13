package lakehouse.ingestion.it.stress

import java.nio.file.{Files, Path}

/**
 * The directory of one stress run: the shared table, and per writer its acknowledgement journal,
 * its log and its Spark scratch space. Everything a run produces stays here, so a failed run can
 * be inspected afterwards.
 *
 * Writers find the layout from the root path alone (`StressRun.at`), so orchestrator and writer
 * JVMs agree on every location without passing each one around.
 */
final class StressRun private (val root: Path) {

  def tableDir: Path = root.resolve("table")

  /** The table as a `file://` URI, the form every writer and the verifier use. */
  def tableUri: String = tableDir.toUri.toString

  def journal(writer: Int): Path = root.resolve("journals").resolve(s"${name(writer)}.acks")

  def log(writer: Int): Path = root.resolve("logs").resolve(s"${name(writer)}.log")

  /** Created by a writer once its session is up and the table is loaded. */
  def readyMarker(writer: Int): Path = root.resolve("barrier").resolve(s"${name(writer)}.ready")

  /** Created by the orchestrator once every writer is ready: all of them start committing. */
  def startSignal: Path = root.resolve("barrier").resolve("start")

  /** What the writer's Delta and Hadoop resolved (`WriterProbe`), written as it exits. */
  def probe(writer: Int): Path = root.resolve("probes").resolve(s"${name(writer)}.properties")

  /** The Markdown report of the run (`StressReport`), next to the evidence it summarises. */
  def report: Path = root.resolve("report.md")

  /** Spark warehouse and other scratch space of one writer. */
  def scratch(writer: Int): Path = root.resolve("spark").resolve(name(writer))

  private def name(writer: Int): String = StressTable.writerId(writer)
}

object StressRun {

  private val Subdirectories = Seq("journals", "logs", "barrier", "probes", "spark")

  /** Creates a fresh run directory `<parent>/<label>`; fails if it already exists. */
  def create(parent: Path, label: String): StressRun = {
    val run = new StressRun(Files.createDirectories(parent).resolve(label))
    Files.createDirectory(run.root)
    Subdirectories.foreach(d => Files.createDirectory(run.root.resolve(d)))
    run
  }

  /** The layout of an existing run directory, as seen by a writer JVM. */
  def at(root: Path): StressRun = {
    require(Files.isDirectory(root), s"not a stress run directory: $root")
    new StressRun(root)
  }
}
