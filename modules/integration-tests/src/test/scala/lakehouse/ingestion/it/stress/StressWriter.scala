package lakehouse.ingestion.it.stress

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, StandardOpenOption}
import java.util.UUID

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try, Using}

import io.delta.tables.DeltaTable
import lakehouse.ingestion.it.stress.LogModel.Turn
import lakehouse.ingestion.it.support.LocalSparkSuite
import lakehouse.ingestion.logstore.coordination.WriterEpoch
import lakehouse.ingestion.logstore.{
  CommitContext,
  EpochNotInForceException,
  PostgresLogStore,
  StartupGuard,
  StartupRefusedException,
  WriterEpochs
}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

/**
 * One stress writer: the `main` of each writer JVM that `StressHarness` launches.
 *
 * It opens its journal, starts a `local[1]` Spark session, loads the table, reports ready and
 * waits at the start barrier. Then it makes `commits` blind appends with `txnAppId = writer-<i>`
 * and `txnVersion = 1..commits`, journaling each one after its write returns, and stops at the
 * first failure. Everything else, including which LogStore is used, comes from `-Dspark.*`.
 * As it exits it records what its Delta and Hadoop actually resolved (`WriterProbe`).
 *
 * A writer that is asked to (`WriterConfig.guard`) starts the way an application on
 * `PostgresLogStore` must: before it touches the table it runs the startup guard on its own
 * configuration, and after loading the table it asks Delta which LogStore it resolved, which
 * is the one thing the guard cannot see. It refuses to run on any problem.
 *
 * Every writer attributes its commits to a run of its own (`CommitContext`). On
 * `PostgresLogStore` that puts the run in each coordination entry, and counts the versions the
 * writer lost to others; on any other LogStore it does nothing. The process writes its run
 * down before it commits anything, since one that is killed reports nothing as it goes.
 *
 * A process started in the place of one that was killed (`WriterConfig.restarted`) goes on in
 * the writer's journal from the batch that was in flight, under a run of its own.
 *
 * A writer that is asked to (`WriterConfig.repeatFor`) repeats a batch that failed, with its
 * `txnVersion`, until it is in the table or the time is up. A failed attempt says nothing of
 * whether the commit was made, and Delta skips a batch that is in the table: that is how the
 * engine is to treat a batch whose outcome it does not know.
 *
 * A writer that is given an epoch (`WriterConfig.epoch`) commits under it (ADR-0019), as a run
 * of the engine commits under the epoch of its lease. Before it commits anything it announces
 * its epoch, asks the table which batch the writer is at, writes both down
 * (`StressRun.turns`), and goes on from the batch after: what recovery does. Every commit
 * names the epoch, to the LogStore through its commit context and in the commit itself. A
 * process that takes a writer over (`WriterConfig.takesOver`) does so while the process before
 * it may still be committing. That one is told nothing: its next commit is refused by the
 * LogStore, and it stops (`ExitCode.OutOfTurn`).
 *
 * The exit code tells the orchestrator how far the writer got (`ExitCode`).
 */
object StressWriter {

  object ExitCode {

    /** Every commit returned without error and is journaled. */
    val Ok = 0

    /**
     * A commit threw after the barrier, and was not repeated or not for any longer; its `FAIL`
     * entry is the journal's last line.
     */
    val CommitFailed = 1

    /** Invalid arguments, or no session, table or start signal: nothing was committed. */
    val SetupFailed = 2

    /** The orchestrator JVM died; the writer halted so it does not outlive the run. */
    val Orphaned = 3

    /**
     * The startup guard refused the process: as configured it would not coordinate its commits.
     * The table was not touched. (The exit code the discovery gives the engine for the same.)
     */
    val Refused = 4

    /**
     * The process commits under an epoch, and another process has taken its writer over: the
     * epoch in force is a later one. It stopped at the first commit that failed after that,
     * with nothing of the batch committed by it.
     */
    val OutOfTurn = 5
  }

  private val BarrierPoll = 5.millis

  /** Between two attempts at a batch that fails. */
  private val RepeatPause = 1.second
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
          val journal = run.journal(config.writer, config.epoch)
          run -> (if (config.restarted) AckJournal.reopen(journal) else AckJournal.open(journal))
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
            case Left(refusal: StartupRefusedException) =>
              report(id, "refused to start: its commits would not be coordinated", refusal)
              ExitCode.Refused
            case Left(error) =>
              report(id, "setup failed before the start barrier", error)
              ExitCode.SetupFailed
            case Right(probe) =>
              val started = System.nanoTime()
              val turn = Option.when(config.epoch > 0)(
                WriterEpoch(run.epochKey(config.writer), config.epoch.toLong)
              )
              val commits = turn.fold(CommitContext.forRun(UUID.randomUUID()))(
                CommitContext.forRun(UUID.randomUUID(), _)
              )
              recordRun(run, config, commits.runId)
              val code =
                try
                  turn.fold(commitAll(spark, config, run, journal, commits, None)) { epoch =>
                    takeTurn(spark, config, run, epoch, commits.runId) match {
                      case None => ExitCode.OutOfTurn
                      case Some(told) =>
                        commitAll(
                          spark,
                          config.copy(firstCommit = told.toInt + 1),
                          run,
                          journal,
                          commits,
                          Some(epoch)
                        )
                    }
                  }
                finally
                  probe
                    .copy(
                      delayedRenames = DelayingLocalFs.delayedRenames,
                      runId = commits.runId.toString,
                      contention = commits.contention,
                      contentionAtInsert = commits.contentionAtInsert,
                      faultWaits = WriterProbe.faultWaitsOfThisJvm
                    )
                    .write(run.probe(config.writer, config.epoch))
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
      // First of all, and before the table is looked at: what this process is set up to do.
      if (config.guard)
        StartupGuard.require(spark.sessionState.newHadoopConf(), new Path(run.root.toUri))
      DeltaTable.forPath(spark, run.tableUri).toDF.schema
      val probe = WriterProbe.resolve(spark, run.tableUri).copy(guarded = config.guard)
      // What the guard cannot see, since it does not see Delta: the LogStore Delta resolved.
      val coordinated = classOf[PostgresLogStore].getName
      if (config.guard && probe.logStore != coordinated)
        throw new StartupRefusedException(
          Seq(s"Delta resolves ${probe.logStore} for ${run.tableUri}, not $coordinated")
        )
      // The barrier is for the writers that start a run together. A process that takes a
      // writer over says that it could, and waits to be let go.
      if (config.takesOver) {
        Files.createFile(run.turnReady(config.writer, config.epoch))
        await(run.turnGo(config.writer, config.epoch), config.startTimeout.fromNow)
      } else if (!config.restarted) {
        Files.createFile(run.readyMarker(config.writer))
        await(run.startSignal, config.startTimeout.fromNow)
      }
      probe
    }.toEither

  /**
   * Takes the turn of the process at its writer, as a run of the engine does when it holds its
   * lease (ADR-0019): announces the epoch, and only then asks the table which batch the writer
   * is at. What the table says is written down with the turn, before anything is committed.
   *
   * @return the batch that the table said the writer was at; none, if a later epoch is in force
   *   already, and the turn is another's
   */
  private def takeTurn(
      spark: SparkSession,
      config: WriterConfig,
      run: StressRun,
      epoch: WriterEpoch,
      runId: UUID
  ): Option[Long] = {
    val asked = System.nanoTime()
    val inForce = WriterEpochs.announce(spark.sessionState.newHadoopConf(), epoch)
    val announceMillis = (System.nanoTime() - asked).nanos.toMillis
    Option.when(inForce == epoch.epoch) {
      // The question of recovery: the last transaction of the writer that the table has.
      val told = DeltaLog
        .forTable(spark, run.tableUri)
        .update()
        .transactions
        .getOrElse(StressTable.writerId(config.writer), 0L)
      val turn =
        Turn(StressTable.writerId(config.writer), epoch.epoch, runId.toString, told, announceMillis)
      appendLine(run.turns(config.writer), turn.line)
      told
    }
  }

  /** Adds the run of this process to the writer's runs, on disk before its first commit. */
  private def recordRun(run: StressRun, config: WriterConfig, runId: UUID): Unit =
    appendLine(run.runs(config.writer), runId.toString)

  /** One whole line added to a file that several processes add to, on disk when it returns. */
  private def appendLine(file: java.nio.file.Path, line: String): Unit =
    Using.resource(
      FileChannel.open(
        file,
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
        StandardOpenOption.APPEND
      )
    ) { channel =>
      channel.write(ByteBuffer.wrap(s"$line\n".getBytes(UTF_8))): Unit
      channel.force(true)
    }

  @tailrec
  private def await(signal: java.nio.file.Path, deadline: Deadline): Unit =
    if (!Files.exists(signal)) {
      if (deadline.isOverdue()) throw new IllegalStateException(s"no signal at $signal")
      Thread.sleep(BarrierPoll.toMillis)
      await(signal, deadline)
    }

  /** @param turn the epoch that the process commits under, if it has one */
  private def commitAll(
      spark: SparkSession,
      config: WriterConfig,
      run: StressRun,
      journal: AckJournal,
      commits: CommitContext,
      turn: Option[WriterEpoch]
  ): Int = {
    val id = StressTable.writerId(config.writer)

    /** @param repeatUntil set while batch `k` is failing: when the writer gives it up */
    @tailrec
    def loop(k: Long, repeatUntil: Option[Deadline]): Int =
      if (k > config.commits) ExitCode.Ok
      else
        Try(commits.attribute(append(spark, config, run, k))) match {
          case Success(()) =>
            journal.ack(k)
            loop(k + 1, None)
          // Before anything else: a process whose turn is over stops, and repeats nothing.
          case Failure(error) if turn.exists(outOfTurn(spark, _, error)) =>
            report(id, s"commit txnVersion=$k was not made: the turn is another's", error)
            ExitCode.OutOfTurn
          case Failure(error) =>
            val until = repeatUntil.getOrElse(config.repeatFor.fromNow)
            if (until.hasTimeLeft()) {
              journal.retry(k, error)
              report(id, s"commit txnVersion=$k failed, and is repeated", error)
              Thread.sleep(RepeatPause.toMillis)
              loop(k, Some(until))
            } else {
              journal.fail(k, error)
              report(id, s"commit txnVersion=$k failed", error)
              ExitCode.CommitFailed
            }
        }
    loop(config.firstCommit.toLong, None)
  }

  private def append(spark: SparkSession, config: WriterConfig, run: StressRun, k: Long): Unit = {
    val write = spark
      .createDataFrame(
        StressTable.rows(config.writer, k, config.rowsPerCommit).asJava,
        StressTable.schema
      )
      .write
      .format("delta")
      .mode("append")
      .option("txnAppId", StressTable.writerId(config.writer))
      .option("txnVersion", k)
    // What the commit itself says of the epoch of its writer, for whoever reads the log. A
    // process that commits under none says nothing, and its commits are as they always were.
    (if (config.epoch > 0) write.option("userMetadata", Turn.metadata(config.epoch.toLong))
     else write).save(run.tableUri)
  }

  /**
   * Whether a commit failed because the turn of the process is over. The LogStore says so when
   * it refuses the commit for its epoch. A commit may also fail on the process that took over,
   * before it reaches the LogStore: Delta refuses a transaction when another has committed for
   * the same writer meanwhile. So a process whose commit failed asks which epoch is in force.
   */
  private def outOfTurn(spark: SparkSession, turn: WriterEpoch, error: Throwable): Boolean = {
    val refused = Iterator
      .iterate(error)(_.getCause)
      .takeWhile(_ != null)
      .take(64)
      .exists(_.isInstanceOf[EpochNotInForceException])
    refused || Try(WriterEpochs.announce(spark.sessionState.newHadoopConf(), turn)).toOption
      .exists(_ != turn.epoch)
  }

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
