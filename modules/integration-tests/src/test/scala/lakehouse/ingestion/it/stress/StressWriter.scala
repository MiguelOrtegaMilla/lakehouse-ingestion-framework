package lakehouse.ingestion.it.stress

import java.nio.file.Files

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try, Using}

import io.delta.tables.DeltaTable
import lakehouse.ingestion.it.support.LocalSparkSuite
import org.apache.spark.sql.SparkSession

/**
 * One stress writer: the `main` of each writer JVM that `StressHarness` launches.
 *
 * It opens its journal, starts a `local[1]` Spark session, loads the table, reports ready and
 * waits at the start barrier. Then it makes `commits` blind appends with `txnAppId = writer-<i>`
 * and `txnVersion = 1..commits`, journaling each one after its write returns, and stops at the
 * first failure. Everything else, including which LogStore is used, comes from `-Dspark.*`.
 * As it exits it records what its Delta and Hadoop actually resolved (`WriterProbe`).
 *
 * The exit code tells the orchestrator how far the writer got (`ExitCode`).
 */
object StressWriter {

  object ExitCode {

    /** Every commit returned without error and is journaled. */
    val Ok = 0

    /** A commit threw after the barrier; its `FAIL` entry is the journal's last line. */
    val CommitFailed = 1

    /** Invalid arguments, or no session, table or start signal: nothing was committed. */
    val SetupFailed = 2

    /** The orchestrator JVM died; the writer halted so it does not outlive the run. */
    val Orphaned = 3
  }

  private val BarrierPoll = 5.millis
  private val ParentPoll = 1.second

  def main(args: Array[String]): Unit = sys.exit(run(args.toSeq))

  private def run(args: Seq[String]): Int =
    WriterConfig.fromArgs(args) match {
      case Left(error) =>
        System.err.println(s"invalid arguments: $error")
        ExitCode.SetupFailed
      case Right(config) =>
        haltWhenOrphaned()
        val id = StressTable.writerId(config.writer)
        val opened = Try {
          val run = StressRun.at(config.runDir)
          run -> AckJournal.open(run.journal(config.writer))
        }
        opened match {
          case Failure(error) =>
            report(id, "could not open the run directory or the journal", error)
            ExitCode.SetupFailed
          case Success((run, journal)) =>
            Using.resource(journal)(runWith(config, run, _))
        }
    }

  private def runWith(config: WriterConfig, run: StressRun, journal: AckJournal): Int = {
    val id = StressTable.writerId(config.writer)
    Try(session(config, run)) match {
      case Failure(error) =>
        report(id, "could not start a Spark session", error)
        ExitCode.SetupFailed
      case Success(spark) =>
        try
          prepare(spark, config, run) match {
            case Left(error) =>
              report(id, "setup failed before the start barrier", error)
              ExitCode.SetupFailed
            case Right(probe) =>
              val started = System.nanoTime()
              val code =
                try commitAll(spark, config, run, journal)
                finally
                  probe
                    .copy(delayedRenames = DelayingLocalFs.delayedRenames)
                    .write(run.probe(config.writer))
              val elapsed = (System.nanoTime() - started).nanos.toMillis
              println(s"$id: exit code $code after $elapsed ms")
              code
          }
        finally spark.stop()
    }
  }

  /**
   * The settings in which a writer departs from `LocalSparkSuite.baseConf`, each with the reason.
   * The session applies exactly these, and a stress report lists them, so a reported result
   * states the configuration it was measured with.
   */
  val SparkSettings: Seq[StressReport.Setting] = Seq(
    StressReport.Setting(
      "spark.master",
      "local[1]",
      "one core per writer: N writer JVMs fit the WSL memory budget (discovery §7.3)"
    ),
    StressReport.Setting(
      "spark.default.parallelism",
      "1",
      "one partition per append, so each commit adds exactly one data file"
    ),
    StressReport.Setting("spark.sql.shuffle.partitions", "1", "same reason"),
    StressReport.Setting(
      "spark.databricks.delta.snapshotPartitions",
      "1",
      "partitions Delta uses to rebuild the table state from its log, which every commit with " +
        "a txnAppId does. The default, 50, costs about 8 s per commit on local[1] instead of " +
        "about 1 s (measured in P1.4a). It changes how the state is computed, not the commit " +
        "protocol: the commit is still the LogStore write of N.json, with the same existence " +
        "check and rename. A shorter commit cycle only raises the commit rate, and with it the " +
        "chance of a collision."
    )
  )

  /** Writer settings: `baseConf`, then `SparkSettings`, then any `-Dspark.*` JVM option. */
  private def session(config: WriterConfig, run: StressRun): SparkSession = {
    val id = StressTable.writerId(config.writer)
    val defaults = LocalSparkSuite.baseConf(run.scratch(config.writer), appName = id) ++
      SparkSettings.map(s => s.key -> s.value)
    val overrides = sys.props.toMap.filter { case (k, _) => k.startsWith("spark.") }
    (defaults ++ overrides)
      .foldLeft(SparkSession.builder()) { case (b, (k, v)) => b.config(k, v) }
      .getOrCreate()
  }

  /**
   * Loads the table and resolves the probe before reporting ready, so that the first commits of
   * all writers are not staggered by class loading and log replay; then waits for the start
   * signal.
   */
  private def prepare(
      spark: SparkSession,
      config: WriterConfig,
      run: StressRun
  ): Either[Throwable, WriterProbe] =
    Try {
      DeltaTable.forPath(spark, run.tableUri).toDF.schema
      val probe = WriterProbe.resolve(spark, run.tableUri)
      Files.createFile(run.readyMarker(config.writer))
      awaitStart(run, config.startTimeout.fromNow)
      probe
    }.toEither

  @tailrec
  private def awaitStart(run: StressRun, deadline: Deadline): Unit =
    if (!Files.exists(run.startSignal)) {
      if (deadline.isOverdue())
        throw new IllegalStateException(s"no start signal at ${run.startSignal}")
      Thread.sleep(BarrierPoll.toMillis)
      awaitStart(run, deadline)
    }

  private def commitAll(
      spark: SparkSession,
      config: WriterConfig,
      run: StressRun,
      journal: AckJournal
  ): Int = {
    @tailrec
    def loop(k: Long): Int =
      if (k > config.commits) ExitCode.Ok
      else
        Try(append(spark, config, run, k)) match {
          case Success(()) =>
            journal.ack(k)
            loop(k + 1)
          case Failure(error) =>
            journal.fail(k, error)
            report(StressTable.writerId(config.writer), s"commit txnVersion=$k failed", error)
            ExitCode.CommitFailed
        }
    loop(1)
  }

  private def append(spark: SparkSession, config: WriterConfig, run: StressRun, k: Long): Unit =
    spark
      .createDataFrame(
        StressTable.rows(config.writer, k, config.rowsPerCommit).asJava,
        StressTable.schema
      )
      .write
      .format("delta")
      .mode("append")
      .option("txnAppId", StressTable.writerId(config.writer))
      .option("txnVersion", k)
      .save(run.tableUri)

  /**
   * Halts this JVM if the orchestrator that launched it dies (a killed sbt, a crashed test JVM).
   * Without it a writer would keep committing, or wait at the barrier, holding memory the WSL
   * budget cannot spare.
   */
  private def haltWhenOrphaned(): Unit =
    ProcessHandle.current().parent().ifPresent { parent =>
      val watchdog = new Thread(
        () =>
          while (true) {
            if (!parent.isAlive) Runtime.getRuntime.halt(ExitCode.Orphaned)
            Thread.sleep(ParentPoll.toMillis)
          },
        "orphan-watchdog"
      )
      watchdog.setDaemon(true)
      watchdog.start()
    }

  private def report(id: String, what: String, error: Throwable): Unit = {
    System.err.println(s"$id: $what")
    error.printStackTrace()
  }
}
