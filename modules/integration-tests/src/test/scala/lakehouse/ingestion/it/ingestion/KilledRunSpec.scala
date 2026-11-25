package lakehouse.ingestion.it.ingestion

import java.nio.charset.StandardCharsets.UTF_8
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.time.temporal.ChronoUnit
import java.time.{Instant, OffsetDateTime, ZoneOffset}

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.Using

import lakehouse.ingestion.cli.Main
import lakehouse.ingestion.core.config.{EngineColumns, RootUri, Source}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.FaultPoint
import lakehouse.ingestion.it.ingestion.RecoveryOracle.{
  recoveryAt,
  recoveryInside,
  LogTable,
  Recovery
}
import lakehouse.ingestion.it.support.{CommandProcess, DeploymentFiles, HeldMain}
import lakehouse.ingestion.logstore.TableIdentity
import lakehouse.ingestion.logstore.coordination.PostgresCoordinationStore
import lakehouse.ingestion.logstore.{FaultPoint => LogPoint}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The command `run`, killed with -9 at a fault point of the engine or of the LogStore, in a process
 * of its own, and run again (discovery §17, P7.10). The matrix of faults (`FaultMatrixSpec`) has a run die in the
 * JVM of the test; here the process dies, with its connections, its session of Spark and its
 * LogStore, as a deployment loses one, and the next is a process of its own.
 *
 * The process is held at the point by the launcher of the tests (`HeldMain`), which says so once
 * it is there; the test then finds the control plane as a run in the middle of its batch leaves
 * it, kills the process, and runs `run` again. The lease of the run that died is made to run out
 * in the database, as in the matrix: a lease that runs out by itself, with the process that held
 * it killed, is P7.8b's (`BackfillBesideRunSpec`). The next invocation ends in 0, ends the run
 * that died as `ABANDONED`, and finishes its batch as the oracle of the matrix says
 * (`RecoveryOracle`), which it says too; each row is in Bronze once, and the ledger holds.
 *
 * '''Inside a commit''', at a point of the LogStore, the process is held by the LogStore's own
 * wait at the point, which the launcher gives it by code, as no profile may, and which the
 * LogStore says the first time; before it is
 * killed, the test sees from outside that the commit has left what a writer there leaves: its
 * entry in the coordination database, complete or not, and its file in the log of the table.
 *
 * '''Which points.''' A cell takes two processes, half a minute: CI runs a few
 * (`InEveryBuild`, `InEveryBuildInside`): a point before the data and one after it for files, a
 * run of a table killed with the cursor of its source open, and a commit to Bronze that is
 * durable and not published. Every point is run with `LAKEHOUSE_IT_KILL_EVERY_POINT=1`, and the
 * others are reported as ignored without it.
 */
final class KilledRunSpec extends AnyFunSuite with Matchers with JdbcIngestionSuite {

  import KilledRunSpec._

  // The lake that the suite reads is the one that the processes write.
  private lazy val files = new DeploymentFiles(pg, workDir)
  override protected lazy val lakeRoot: RootUri = RootUri.parse(files.lake.toUri.toString).value
  override protected lazy val landingDirectory: Path = files.landing

  /** A source of the kind, among the sources of the deployment, with its first batch to read. */
  private def sourceOf(kind: Kind): Source = kind match {
    case Kind.Files =>
      val of = files.fileSource(newSourceId(), rejected = Some("0.5"))
      files.drop(of, "a.csv", RejectingCsv): Unit
      of
    case Kind.Incremental =>
      val id = newSourceId()
      val table = id.value.replace('.', '_')
      asOwner { c =>
        execute(c, s"CREATE TABLE $table (id int PRIMARY KEY, updated_at timestamptz NOT NULL)")
        Using.resource(c.prepareStatement(s"INSERT INTO $table VALUES (?, ?)")) { statement =>
          Seq(1 -> 1L, 2 -> 2L, 3 -> 26L).foreach { case (row, hours) =>
            statement.setInt(1, row)
            statement.setObject(
              2,
              OffsetDateTime.ofInstant(Day.plus(hours, ChronoUnit.HOURS), ZoneOffset.UTC)
            )
            statement.executeUpdate(): Unit
          }
        }
      }
      // Its first window is the first day of 2026; the row after it is the next one's.
      files.tableSource(
        id,
        s"public.$table",
        files.connection("sales_ro", pg.password("sales_ro"), url = pg.jdbcUrl("sales")),
        load = WindowOfADay
      )
  }

  /** What the process printed so far. */
  private def printed(log: Path): String = new String(Files.readAllBytes(log), UTF_8)

  /** Waits for the process to print `line`: that it is where it is to be killed. */
  private def awaitPrinted(process: CommandProcess, log: Path, line: String): Unit = {
    val deadline = HoldWithin.fromNow
    @tailrec
    def poll(): Unit =
      if (!printed(log).contains(line)) {
        if (!process.isAlive)
          fail(s"the process ended before it printed \"$line\":\n${process.await().log}")
        if (deadline.isOverdue()) fail(s"the process did not print \"$line\" in $HoldWithin")
        Thread.sleep(100)
        poll()
      }
    poll()
  }

  /** The rows of the source in its table of Bronze, by their id, with their batch. */
  private def rowsIn(of: Source): Seq[(String, Long)] =
    spark.read
      .format("delta")
      .load(bronze(of).uri)
      .select(col("id").cast("string"), col(EngineColumns.BatchSeq))
      .collect()
      .map(row => (row.getString(0), row.getLong(1)))
      .toSeq
      .sorted

  /**
   * Kills a process that is in the middle of the first batch of `of`, as a deployment loses one,
   * and runs `run` again: the next invocation ends in 0, ends the run that died as `ABANDONED`,
   * finishes the batch as `recovery` says, and says how. Bronze then has `rows`.
   */
  private def killedAndRunAgain(
      of: Source,
      process: CommandProcess,
      recovery: Recovery,
      rows: Seq[(String, Long)]
  ): Unit = {
    val first = batchId(of, 1)

    // As a run in the middle of its batch leaves it: running, its lease held, its batch planned.
    val dead = holderOf(of).value
    runOf(dead).status shouldBe RunStatus.Running
    batchOf(first).status shouldBe BatchStatus.Planned
    batchOf(first).plannedBy shouldBe dead

    process.signal("KILL")
    process.await(30.seconds).exit shouldBe KilledBySignal
    expire(of)

    // Two batches at most: the one that it finishes, and the next. A table read a day a window
    // from the first day of 2026 has a window for each day since.
    val again = CommandProcess
      .start(files.arguments(of) ++ Seq("--max-batches", "2"), files.newLog(), Heap)
      .await(3.minutes)
    withClue(again.log)(again.exit shouldBe Main.Exit.Ok)

    // The batch is finished by the next invocation as the table of its data says, and it says so.
    val committed = batchOf(first)
    committed.status shouldBe BatchStatus.Committed
    committed.plannedBy shouldBe dead
    val next = committed.committedBy.value
    next should not be dead
    runOf(next).status shouldBe RunStatus.Succeeded
    runOf(dead).status shouldBe RunStatus.Abandoned
    recovery match {
      case Recovery.Replay => again.log should include(s"committed the batch $first: ")
      case Recovery.RollForward =>
        again.log should include(
          s"committed the batch $first, which a run before it had written and not committed"
        )
    }
    holderOf(of) shouldBe None

    rowsIn(of) shouldBe rows
    assertLedger(of)
  }

  /** A run held at a point of the engine by the launcher of the tests, and killed there. */
  private def killedAt(kind: Kind, point: FaultPoint): Unit = {
    val of = sourceOf(kind)
    val log = files.newLog()
    val held = CommandProcess.start(
      Seq("--hold", point.name, "--") ++ files.arguments(of),
      log,
      Heap,
      main = HeldMain.getClass.getName.stripSuffix("$")
    )
    awaitPrinted(held, log, HeldMain.heldAt(point))
    killedAndRunAgain(of, held, recoveryAt(point), kind.rows)
  }

  // -----------------------------------------------------------------------------------------------
  // Inside a commit: held there by the LogStore's own wait at the point
  // -----------------------------------------------------------------------------------------------

  private lazy val coordinationStore = new PostgresCoordinationStore(coordination)
  private lazy val localFs = FileSystem.getLocal(new Configuration())

  override def afterAll(): Unit =
    try coordinationStore.close()
    finally super.afterAll()

  /**
   * What the first commit to a table has left, seen from outside the process: whether the
   * coordination database has its entry, and whether it is complete; and whether its file is in
   * the log of the table.
   */
  private def leftOf(uri: String): (Option[Boolean], Boolean) = {
    val entry = coordinationStore.entry(TableIdentity.of(new HadoopPath(uri), localFs), 0L)
    val file = Paths.get(URI.create(uri)).resolve("_delta_log").resolve(f"${0L}%020d.json")
    (entry.map(_.complete), Files.exists(file))
  }

  /**
   * A run of files whose first commit is to `table`, held inside it at `point` by a wait of the
   * LogStore that the launcher of the tests gives it, and killed there once the commit has left
   * what a writer there leaves: no entry before the insert, an entry that is not complete after it, and the
   * file as well once it is published. A batch with a record rejected commits to its quarantine
   * first; one with none, to Bronze alone.
   */
  private def killedInside(table: LogTable, point: LogPoint): Unit = {
    val of = files.fileSource(newSourceId(), rejected = Some("0.5"))
    val (text, target) = table match {
      case LogTable.Quarantine => (RejectingCsv, quarantine(of))
      case LogTable.Bronze => (CleanCsv, bronze(of))
    }
    files.drop(of, "a.csv", text): Unit

    val log = files.newLog()
    val process = CommandProcess.start(
      Seq("--wait-in", point.key, "--") ++ files.arguments(of),
      log,
      Heap,
      main = HeldMain.getClass.getName.stripSuffix("$")
    )
    awaitPrinted(process, log, HeldMain.waitingIn(point))
    withClue(s"the first commit to ${table.name} at ${point.key}: ")(
      leftOf(target.uri) shouldBe
        leftAt(point)
    )

    killedAndRunAgain(of, process, recoveryInside(table, point), Seq("1" -> 1L, "4" -> 1L))
  }

  private val everyPoint = sys.env.get(EveryPoint).contains("1")

  for (kind <- Kind.All; point <- kind.points) {
    val name = s"${kind.name}: a run killed with -9 at $point ${recoveryAt(point).left}"
    if (everyPoint || InEveryBuild.contains(kind -> point)) test(name)(killedAt(kind, point))
    else ignore(s"$name (with $EveryPoint=1)")(killedAt(kind, point))
  }

  for (table <- LogTable.All; point <- LogPoint.All) {
    val name = s"files: a run killed with -9 at $point of its commit to ${table.name} " +
      recoveryInside(table, point).left
    if (everyPoint || InEveryBuildInside.contains(table -> point))
      test(name)(killedInside(table, point))
    else ignore(s"$name (with $EveryPoint=1)")(killedInside(table, point))
  }
}

object KilledRunSpec {

  /** The kinds of the runs that are killed, each with its points and what Bronze then has. */
  sealed abstract class Kind(val name: String, val points: Seq[FaultPoint]) {

    /** Each row of the source in Bronze, by its id, with its batch, once the next has run. */
    def rows: Seq[(String, Long)]
  }

  object Kind {

    /** A batch of files with a record rejected: it reaches every point. */
    case object Files extends Kind("files", FaultPoint.All) {
      override def rows: Seq[(String, Long)] = Seq("1" -> 1L, "4" -> 1L)
    }

    /** A window of a table, read in one pass as it is written: no quarantine. */
    case object Incremental
        extends Kind(
          "incremental load",
          FaultPoint.All.filterNot(_ == FaultPoint.AfterQuarantineWrite)
        ) {

      // The invocation goes on past the first window: the row after it is in the second.
      override def rows: Seq[(String, Long)] = Seq("1" -> 1L, "2" -> 1L, "3" -> 2L)
    }

    val All: Seq[Kind] = Seq(Files, Incremental)
  }

  /**
   * The cells of every build: for files, a point before its data is written and one after, and
   * a run of a table held once it has asked its source for a first row, its cursor open.
   */
  private val InEveryBuild: Set[(Kind, FaultPoint)] = Set(
    Kind.Files -> FaultPoint.AfterCounts,
    Kind.Files -> FaultPoint.BeforeStateCommit,
    Kind.Incremental -> FaultPoint.AfterCounts
  )

  /** The cell of every build inside a commit: one to Bronze that is durable, not published. */
  private val InEveryBuildInside: Set[(LogTable, LogPoint)] = Set(
    LogTable.Bronze -> LogPoint.AfterInsert
  )

  /** What a commit at a point leaves, as `leftOf` sees it: its entry, complete or not; its file. */
  private def leftAt(point: LogPoint): (Option[Boolean], Boolean) = point match {
    case LogPoint.BeforeInsert => (None, false)
    case LogPoint.AfterInsert => (Some(false), false)
    case LogPoint.AfterPublish => (Some(false), true)
  }

  /** A file with no record rejected: its batch commits to Bronze alone. */
  private val CleanCsv = "id,amount\n1,2\n4,5\n"

  /** The variable of the environment that runs every cell. */
  private val EveryPoint = "LAKEHOUSE_IT_KILL_EVERY_POINT"

  /** A heap for each of the processes, which run one at a time, beside the JVM of the tests. */
  private val Heap = "768m"

  /** How long a process is given to reach its point: the start of a JVM, and of Spark. */
  private val HoldWithin = 3.minutes

  /** What a JVM that SIGKILL ended exits with: 128 + 9. */
  private val KilledBySignal = 137

  /** A file with a record of three that cannot be parsed, which its source lets be rejected. */
  private val RejectingCsv = "id,amount\n1,2\n3\n4,5\n"

  /** The first day of 2026: the first window of the table. */
  private val Day = Instant.parse("2026-01-01T00:00:00Z")

  private val WindowOfADay =
    "{ mode: incremental, watermark: { column: updated_at, safety_margin: 1h, " +
      s"""start: "$Day" }, max_batch_range: 1d }"""
}
