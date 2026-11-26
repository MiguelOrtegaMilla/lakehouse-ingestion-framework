package lakehouse.ingestion.it.ingestion

import java.time.temporal.ChronoUnit
import java.time.{Clock, Instant, OffsetDateTime, ZoneOffset}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise, TimeoutException}
import scala.util.{Failure, Success, Try, Using}

import lakehouse.ingestion.core.config.{EngineColumns, Source, SourceId}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.core.watermark.BackfillRange
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.control.{Backfills, Leases}
import lakehouse.ingestion.engine.{FaultHooks, FaultPoint, IngestionService}
import lakehouse.ingestion.it.support.Died
import lakehouse.ingestion.it.ingestion.RecoveryOracle.{
  recoveryAt,
  recoveryInside,
  LogTable,
  Recovery
}
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.logstore.{FaultPoint => LogPoint, HookedLogStore, SimulatedCrash}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.col
import org.scalatest.OutcomeOf.outcomeOf
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The matrix of faults in one JVM (discovery §17, P7.9): each fault point of the engine, for
 * each kind of batch that a run takes, with a fault there, and the run after it. A cell is a
 * kind, a point and a fault, and a test of its own, on a source of its own; each ends with the
 * ledger of the source, which finds what the cell does not ask.
 *
 * '''The kinds''' are those of the sequences that a batch is in: new files, a table read whole,
 * a window of a table, the files of a committed batch read again, and a chunk of a backfill:
 * the first, which the run that creates the backfill plans, and one of a backfill under way,
 * which a run plans from the progress that the chunks before it saved.
 * Each is read by its real connector, and a point that a kind never reaches is left out of its
 * column: a table has no record rejected, and so no commit to a quarantine. That a kind
 * reaches the points of its column, and no other, is a test of its own.
 *
 * '''What a cell expects''' is said once, from the protocol (ADR-0005, ADR-0021), and not in
 * each test (`RecoveryOracle`): the table of the data says what the run that takes the lease of
 * another does with the batch that it left. A batch whose rows are in it is rolled forward, and one whose rows are not is run again
 * under its number, as it was planned; a quarantine that has the records of the batch changes
 * nothing of that.
 *
 * '''The columns.''' A run that dies at the point, as a process that dies: it goes no further,
 * and tells the control plane nothing. It dies too at each point of the LogStore inside its
 * commits, to its quarantine and to Bronze, for a batch of files: the session commits through
 * `PostgresLogStore` behind hooks of the tests (`HookedLogStore`), which those cells arm, and the
 * crash goes through Delta and the engine as a death does, unclassified (`SimulatedCrash`). A source that changes at the point, as one of its kind
 * can, and a run that goes on to its end, and then the next (`changeAt`). And a run whose lease is taken over at the point: its
 * lease runs out, and the next run of its sequence takes it and runs to its end, inside the hook,
 * on the thread of the run that lost it. That one then goes on, and commits nothing: before
 * each step it asks its heartbeat, which has not renewed yet, and not the database; its write is
 * of a batch that the table has by then, which Delta skips; and its commit to the control plane
 * asks for the lease, and is refused. The jobs of Spark of the run that took the lease carry the
 * mark of the other as well (`RunJobs`), which would have them stopped if the other found its
 * lease lost by its heartbeat: its first renewal is a third of its time to live away, 40 s, long
 * after a cell is over. In those cells the run that lost the lease writes a batch that its table
 * has, which Delta skips before any commit: the epoch that the run that took the lease announced
 * to the LogStore is never asked. Cells of their own ask it: a run of files whose lease is taken
 * where it holds what it read, by a run that finds the file changed and gives the batch up, so
 * that no table has the batch and the epoch alone refuses the write.
 *
 * '''The lease of a run that died is made to run out in the database''' (`runOut`), at once, and
 * is not waited for: what a cell asks is what the next run does with what the dead one left,
 * and a wait of a time to live in each cell would make the matrix minutes longer for nothing. A
 * lease that runs out by itself, with the process that held it killed, is P7.8b's test, in
 * `BackfillBesideRunSpec`.
 */
final class FaultMatrixSpec extends AnyFunSuite with Matchers with JdbcIngestionSuite {

  import FaultMatrixSpec._

  private val asked = OperatorRequest.of("ops.oncall", "the matrix of faults").toOption.get

  /**
   * Each cell has `CellTimeLimit` to end, on a thread of its own. One that does not, as one
   * whose commit Delta tries again and again at a version that is taken, fails with its name and
   * where it was, and the matrix goes on, instead of the job hanging until its own limit.
   *
   * Not `TimeLimitedTests`: it interrupts the thread of the test and then waits for the test to
   * return, which a loop that does not heed an interrupt never does. Here the thread of the cell
   * is interrupted, its jobs of Spark are stopped, and it is left as it is: nothing in it can be
   * made to end, and its source is its own, which no other cell touches.
   */
  override def withFixture(test: NoArgTest): org.scalatest.Outcome = {
    val ended = Promise[org.scalatest.Outcome]()
    val cell = new Thread(() => ended.complete(Try(super.withFixture(test))): Unit)
    cell.setName(s"cell: ${test.name}")
    cell.setDaemon(true)
    cell.start()
    Try(Await.result(ended.future, CellTimeLimit)) match {
      case Success(outcome) => outcome
      case Failure(_: TimeoutException) =>
        // Where it was: what it was doing at that moment, by the frames at the top, and in
        // which step of the run and of the cell, by the frames of this application.
        val frames = cell.getStackTrace.toSeq
        val where = (frames.take(3) ++ Seq("...") ++
          frames
            .drop(3)
            .filter(frame =>
              frame.getClassName.startsWith(Application) && !frame.getClassName.contains("$$Lambda")
            )
            .take(20))
          .mkString("\n    at ")
        cell.interrupt()
        spark.sparkContext.cancelAllJobs()
        outcomeOf(
          fail(
            s"the cell \"${test.name}\" did not end in $CellTimeLimit: it hung, and was left " +
              s"as it was. It was at\n    at $where"
          )
        )
      case Failure(other) => throw other
    }
  }

  // `PostgresLogStore`, behind hooks that stop a commit at a point of the LogStore: the cells of a
  // run that dies inside its commit to a table arm them, and the others go through it untouched.
  override protected def sparkConf: Map[String, String] =
    super.sparkConf.updated("spark.delta.logStore.file.impl", classOf[HookedLogStore].getName)

  /** The service, as an application puts it together, with both connectors and these hooks. */
  private def engine(hooks: FaultHooks = FaultHooks.Off): IngestionService =
    new IngestionService(
      database,
      connectors,
      spark,
      lakeRoot,
      timing,
      Clock.fixed(Now, ZoneOffset.UTC),
      hooks
    )

  // ---------------------------------------------------------------------------------------------
  // A cell: a source of one kind, made ready for the run that the cell is about
  // ---------------------------------------------------------------------------------------------

  /**
   * A source of one kind, with the run that a cell is about still to come.
   *
   * @param batch the batch that the run plans
   * @param lease the lease of its sequence, that it and the run after it ask for
   * @param attempt the run, by a service
   * @param after the run after it, of its sequence: as an orchestrator or an operator starts it
   */
  final private class Cell(
      val source: Source,
      val batch: BatchId,
      val lease: LeaseKey,
      attempt: IngestionService => Outcome,
      after: IngestionService => Outcome
  ) {

    /** The run of the cell, by a service with these hooks. */
    def run(hooks: FaultHooks): Outcome = attempt(engine(hooks))

    /** The run after it, by a service with nothing done to it. */
    def next(): Outcome = after(engine())
  }

  /** A source of the kind, made ready: its first batch, or its first after those committed. */
  private def cellOf(kind: Kind): Cell = kind match {
    case Kind.Files =>
      val of = fileSource(rejected = Some("0.5"))
      drop(of, "a.csv", RejectingCsv)
      regular(of, seq = 1)

    case Kind.Full =>
      val id = newSourceId()
      val of = jdbcSource(
        tableOf(id, Day.plus(1, ChronoUnit.HOURS), Day.plus(26, ChronoUnit.HOURS)),
        id = id.value
      )
      regular(of, seq = 1)

    case Kind.Incremental =>
      val id = newSourceId()
      val table = tableOf(
        id,
        Day.plus(1, ChronoUnit.HOURS),
        Day.plus(2, ChronoUnit.HOURS),
        Day.plus(26, ChronoUnit.HOURS)
      )
      // Its first window is the first day of 2026, and the row after it is the next one's.
      val of = jdbcSource(table, load = s"load: $WindowOfADay", id = id.value)
      regular(of, seq = 1)

    case Kind.Reprocess =>
      val of = fileSource(rejected = Some("0.5"))
      drop(of, "a.csv", RejectingCsv)
      ingested(engine().run(of, versionOf(of))).batch shouldBe batchId(of, 1)
      // The next run of the sequence is a regular one, which finds the reprocess planned.
      new Cell(
        of,
        batchId(of, 2),
        LeaseKey.of(of.id, Layer.Bronze),
        _.reprocess(of, versionOf(of), BatchSeq.of(1).toOption.get, asked),
        _.run(of, versionOf(of))
      )

    case Kind.FirstChunk => chunk(seq = 1)
    case Kind.ChunkUnderWay => chunk(seq = 2)
  }

  /**
   * A cell of the chunk `seq` of a backfill of `(H, H + 3d]`, a day a chunk, two rows in each.
   * The first chunk is planned by the run that creates the backfill; each after it, by a run
   * that resumes the backfill from the progress that the chunks before it saved, which runs
   * before the cell's committed.
   */
  private def chunk(seq: Long): Cell = {
    val id = newSourceId()
    val table = tableOf(
      id,
      History.plus(12, ChronoUnit.HOURS),
      History.plus(1, ChronoUnit.DAYS),
      History.plus(36, ChronoUnit.HOURS),
      History.plus(2, ChronoUnit.DAYS),
      History.plus(60, ChronoUnit.HOURS),
      History.plus(3, ChronoUnit.DAYS)
    )
    val of = jdbcSource(table, load = s"load: $WindowOfADay", id = id.value)
    val request = BackfillRequest(
      Backfilled,
      BackfillRange.of(History, History.plus(3, ChronoUnit.DAYS)).toOption.get,
      Some(1.day)
    )
    val backfill: IngestionService => Outcome = _.backfill(of, versionOf(of), request, asked)
    val app = TxnAppId.backfill(of.id, Layer.Bronze, Backfilled)
    (1L until seq).foreach { before =>
      ingested(backfill(engine())).batch shouldBe BatchId(app, BatchSeq.of(before).toOption.get)
    }
    // What the run of the cell resumes from: the progress of the chunks before it, if any.
    database
      .run(Backfills.find(_, of.id, Layer.Bronze, Backfilled))
      .flatMap(_.progress)
      .map(_.lastCommitted.value) shouldBe Option.when(seq > 1)(seq - 1)
    new Cell(
      of,
      BatchId(app, BatchSeq.of(seq).toOption.get),
      LeaseKey.backfill(of.id, Layer.Bronze, Backfilled),
      backfill,
      backfill
    )
  }

  /** A cell of the regular sequence of a source, whose runs are `run`. */
  private def regular(of: Source, seq: Long): Cell = {
    val run: IngestionService => Outcome = _.run(of, versionOf(of))
    new Cell(of, batchId(of, seq), LeaseKey.of(of.id, Layer.Bronze), run, run)
  }

  /** A table of its own for a source, a row stamped at each moment given. */
  private def tableOf(id: SourceId, stamps: Instant*): String = {
    val table = id.value.replace('.', '_')
    asOwner { c =>
      execute(c, s"CREATE TABLE $table (id int PRIMARY KEY, updated_at timestamptz NOT NULL)")
      Using.resource(c.prepareStatement(s"INSERT INTO $table VALUES (?, ?)")) { statement =>
        stamps.zipWithIndex.foreach { case (stamp, i) =>
          statement.setInt(1, i + 1)
          statement.setObject(2, OffsetDateTime.ofInstant(stamp, ZoneOffset.UTC))
          statement.executeUpdate(): Unit
        }
      }
    }
    s"public.$table"
  }

  // ---------------------------------------------------------------------------------------------
  // What a cell asks of the control plane
  // ---------------------------------------------------------------------------------------------

  private def holderOf(lease: LeaseKey): Option[RunId] =
    database.run(Leases.find(_, lease)).flatMap(_.holder).map(_.run)

  /**
   * The lease runs out in the database now, whatever its holder believes of it: that of a run
   * that died is not waited out in a cell (see the suite).
   */
  private def runOut(lease: LeaseKey): Unit = asSuperuser { c =>
    update(
      c,
      "UPDATE control.source_lease SET expires_at = now() - interval '1 second' WHERE lease_key = ?",
      lease.value
    ) shouldBe 1: Unit
  }

  /** The batches of a sequence, each by its number with how it stands. */
  private def sequenceOf(batch: BatchId): Seq[(Long, String)] = asSuperuser { c =>
    query(
      c,
      "SELECT batch_seq::text, status FROM control.ingestion_batch WHERE txn_app_id = ? ORDER BY 1",
      batch.app.value
    ).map(row => (row(0).toLong, row(1)))
  }

  private def rolledForward(outcome: Outcome): Outcome.RolledForward = outcome match {
    case done: Outcome.RolledForward => done
    case other => fail(s"no batch was rolled forward: $other")
  }

  // ---------------------------------------------------------------------------------------------
  // The matrix
  // ---------------------------------------------------------------------------------------------

  Kind.All.foreach { kind =>
    test(
      s"${kind.name}: a run reaches the ${kind.points.size} fault points of its column, in order"
    ) {
      val cell = cellOf(kind)
      val seen = ArrayBuffer.empty[FaultPoint]
      val hooks: FaultHooks = (point, _) => { seen += point; () }

      ingested(cell.run(hooks)).batch shouldBe cell.batch

      seen.toSeq shouldBe kind.points
      assertLedger(cell.source)
    }
  }

  /**
   * The batch that `left` planned was finished by `next` as the oracle says, under its number,
   * and `left` ends as `ABANDONED`: the run that took its lease over said so.
   */
  private def assertFinished(cell: Cell, recovery: Recovery, left: RunId, next: Outcome): Unit = {
    val finished = recovery match {
      case Recovery.Replay => ingested(next).batch
      case Recovery.RollForward => rolledForward(next).batch
    }
    finished shouldBe cell.batch
    runOf(left).status shouldBe RunStatus.Abandoned
    runOf(next.run).status shouldBe RunStatus.Succeeded
    val committed = batchOf(cell.batch)
    committed.plannedBy shouldBe left
    committed.committedBy shouldBe Some(next.run)
    // Under its number, and no other: the sequence has no batch after it, nor one given up.
    sequenceOf(cell.batch) shouldBe (1L to cell.batch.seq.value).map(_ -> "COMMITTED")
    holderOf(cell.lease) shouldBe None
    assertLedger(cell.source)
  }

  /**
   * The run ended as one that lost its lease: a failure that passes, which says so.
   *
   * @return what it said
   */
  private def lostTheLease(outcome: Outcome, lease: LeaseKey): String = outcome match {
    case Outcome.Failed(_, failure, message) =>
      failure shouldBe FailureClass.Transient
      message should include(s"lost the lease of $lease")
      message
    case other => fail(s"the run did not lose its lease: $other")
  }

  private def givenUp(outcome: Outcome): Outcome.GivenUp = outcome match {
    case done: Outcome.GivenUp => done
    case other => fail(s"no batch was given up: $other")
  }

  private def epochOf(run: RunId): Long = runOf(run).epoch.value.value

  /** The batch after `batch` in its sequence. */
  private def after(batch: BatchId): BatchId =
    BatchId(batch.app, BatchSeq.of(batch.seq.value + 1).toOption.get)

  // A run that dies at the point: the next takes its lease over, and finishes its batch.
  for (kind <- Kind.All; point <- kind.points) {
    val recovery = recoveryAt(point)
    test(s"${kind.name}: a run that dies at $point ${recovery.left}") {
      val cell = cellOf(kind)

      intercept[Died](cell.run(hookAt(point)(_ => throw new Died)))

      // As a process that died leaves it: its run running, its lease held, its batch planned.
      val dead = holderOf(cell.lease).value
      runOf(dead).status shouldBe RunStatus.Running
      batchOf(cell.batch).status shouldBe BatchStatus.Planned
      batchOf(cell.batch).plannedBy shouldBe dead

      runOut(cell.lease)
      assertFinished(cell, recovery, dead, cell.next())
    }
  }

  // A run whose lease is taken over at the point, by the next, which finishes its batch: the
  // one that lost it goes on until it finds so, and commits nothing.
  for (kind <- Kind.All; point <- kind.points) {
    val recovery = recoveryAt(point)
    test(s"${kind.name}: a run whose lease is taken over at $point stops, and ${recovery.taker}") {
      val cell = cellOf(kind)
      var taker = Option.empty[Outcome]

      val taken = cell.run(hookAt(point) { _ =>
        runOut(cell.lease)
        taker = Some(cell.next())
      })

      lostTheLease(taken, cell.lease): Unit
      assertFinished(cell, recovery, taken.run, taker.value)
    }
  }

  // A run whose lease is taken over where it holds what it read, by a run that finds the file of
  // the batch changed and gives the batch up: no table has the batch, and what keeps the other
  // from writing it is the epoch that the run that took the lease announced, alone.
  for (kind <- Seq(Kind.Files, Kind.Reprocess); point <- HoldingWhatItRead) {
    test(
      s"${kind.name}: a run whose lease is taken over at $point by a run that gives the batch " +
        "up is refused its commit by the epoch"
    ) {
      val cell = cellOf(kind)
      var taker = Option.empty[Outcome]

      val taken = cell.run(hookAt(point) { _ =>
        drop(cell.source, "a.csv", ChangedCsv, at = Now.minusSeconds(1800)): Unit
        runOut(cell.lease)
        taker = Some(cell.next())
      })

      // The file is not what the batch planned, and nothing of the batch is in its table.
      givenUp(taker.value).batch shouldBe cell.batch
      val gaveUp = taker.value.run
      runOf(gaveUp).status shouldBe RunStatus.Failed
      batchOf(cell.batch).status shouldBe BatchStatus.Abandoned
      // The run that lost its lease went on to write what it read, and nothing of it is in the
      // table of the data: the LogStore refused the commit, for its epoch.
      withClue(s"the commits of ${cell.batch.app} in the table of its data: ") {
        delta
          .lastVersion(bronze(cell.source), cell.batch.app)
          .filter(_ >= cell.batch.seq.value) shouldBe
          None
      }
      val refused = lostTheLease(taken, cell.lease)
      refused should include(s"the writer commits under epoch ${epochOf(taken.run)} of")
      refused should include(s"the epoch in force is ${epochOf(gaveUp)}")
      runOf(taken.run).status shouldBe RunStatus.Abandoned
      holderOf(cell.lease) shouldBe None

      // The next run takes the file as it is now, a file it has not taken, under the next number.
      ingested(cell.next()).batch shouldBe after(cell.batch)
      sequenceOf(cell.batch) shouldBe
        (1L until cell.batch.seq.value).map(_ -> "COMMITTED") ++
        Seq(cell.batch.seq.value -> "ABANDONED", cell.batch.seq.value + 1 -> "COMMITTED")
      assertLedger(cell.source)
    }
  }

  // ---------------------------------------------------------------------------------------------
  // A source that changes at the point
  // ---------------------------------------------------------------------------------------------

  /** The source of a cell changes at the point, as one of its kind can (`changeAt`). */
  private def change(kind: Kind, of: Source): Unit = kind match {
    case Kind.Files | Kind.Reprocess =>
      // The file of the batch is written again: another size, and another time.
      drop(of, "a.csv", ChangedCsv, at = Now.minusSeconds(1800)): Unit
    case Kind.Full =>
      // A row comes, and one goes.
      asOwner { c =>
        execute(c, s"INSERT INTO ${tableNameOf(of)} VALUES (3, '${Day.plus(3, ChronoUnit.HOURS)}')")
        execute(c, s"DELETE FROM ${tableNameOf(of)} WHERE id = 1")
      }
    // A row of the window, or of the chunk, changes to a time of the next one.
    case Kind.Incremental => moved(of, row = 2, to = Day.plus(30, ChronoUnit.HOURS))
    case Kind.FirstChunk => moved(of, row = 1, to = History.plus(40, ChronoUnit.HOURS))
    case Kind.ChunkUnderWay => moved(of, row = 3, to = History.plus(64, ChronoUnit.HOURS))
  }

  private def tableNameOf(of: Source): String = of.id.value.replace('.', '_')

  private def moved(of: Source, row: Int, to: Instant): Unit = asOwner { c =>
    execute(c, s"UPDATE ${tableNameOf(of)} SET updated_at = '$to' WHERE id = $row")
  }

  /**
   * What Bronze has of the source once the run of the cell and the next have run, as the oracle
   * says: each row by its id, with the number of the batch that wrote it.
   */
  private def bronzeAfter(kind: Kind, seen: Change): Seq[(String, Long)] = {
    val rows: Seq[(String, Long)] = (kind, seen) match {
      // The first batch has the file as it was read, or is given up; the next, the file now.
      case (Kind.Files, Change.GivenUp) => Seq("10" -> 2, "30" -> 2)
      case (Kind.Files, Change.Unseen) => Seq("1" -> 1, "4" -> 1, "10" -> 2, "30" -> 2)
      case (Kind.Reprocess, Change.GivenUp) => Seq("1" -> 1, "4" -> 1, "10" -> 3, "30" -> 3)
      case (Kind.Reprocess, Change.Unseen) =>
        Seq("1" -> 1, "4" -> 1, "1" -> 2, "4" -> 2, "10" -> 3, "30" -> 3)
      // The table whole, without row 1 and with row 3 once it changed; the next has it so.
      case (Kind.Full, Change.Read) => Seq("2" -> 1, "3" -> 1, "2" -> 2, "3" -> 2)
      case (Kind.Full, Change.Unseen) => Seq("1" -> 1, "2" -> 1, "2" -> 2, "3" -> 2)
      // The row that moved is in the next window or chunk, and in its own if it was read there.
      case (Kind.Incremental, Change.Read) => Seq("1" -> 1, "2" -> 2, "3" -> 2)
      case (Kind.Incremental, Change.Unseen) => Seq("1" -> 1, "2" -> 1, "2" -> 2, "3" -> 2)
      case (Kind.FirstChunk, Change.Read) => Seq("2" -> 1, "1" -> 2, "3" -> 2, "4" -> 2)
      case (Kind.FirstChunk, Change.Unseen) =>
        Seq("1" -> 1, "2" -> 1, "1" -> 2, "3" -> 2, "4" -> 2)
      case (Kind.ChunkUnderWay, Change.Read) =>
        Seq("1" -> 1, "2" -> 1, "4" -> 2, "3" -> 3, "5" -> 3, "6" -> 3)
      case (Kind.ChunkUnderWay, Change.Unseen) =>
        Seq("1" -> 1, "2" -> 1, "3" -> 2, "4" -> 2, "3" -> 3, "5" -> 3, "6" -> 3)
      case (other, change) => fail(s"the oracle has no change $change for $other")
    }
    rows.sorted
  }

  /** The rows of the source in its table of Bronze: each by its id, with its batch. */
  private def rowsIn(of: Source): Seq[(String, Long)] =
    spark.read
      .format("delta")
      .load(bronze(of).uri)
      .select(col("id").cast("string"), col(EngineColumns.BatchSeq))
      .collect()
      .map(row => (row.getString(0), row.getLong(1)))
      .toSeq
      .sorted

  // The source changes at the point, and the run goes on to its end; then the next run.
  for (kind <- Kind.All; point <- kind.points) {
    val seen = changeAt(kind, point)
    test(s"${kind.name}: a source that changes at $point ${seen.saying}") {
      val cell = cellOf(kind)

      val outcome = cell.run(hookAt(point)(_ => change(kind, cell.source)))

      seen match {
        case Change.GivenUp =>
          givenUp(outcome).batch shouldBe cell.batch
          runOf(outcome.run).status shouldBe RunStatus.Failed
          batchOf(cell.batch).status shouldBe BatchStatus.Abandoned
        case Change.Read | Change.Unseen =>
          ingested(outcome).batch shouldBe cell.batch
          runOf(outcome.run).status shouldBe RunStatus.Succeeded
      }
      // The next run reads the source as it is now, under the next number.
      ingested(cell.next()).batch shouldBe after(cell.batch)
      rowsIn(cell.source) shouldBe bronzeAfter(kind, seen)
      assertLedger(cell.source)
    }
  }

  // ---------------------------------------------------------------------------------------------
  // A run that dies inside its commit to a table, at a point of the LogStore
  // ---------------------------------------------------------------------------------------------

  /** Whether a file is of the log of the table at `uri`. */
  private def inTheLogOf(uri: String)(file: Path): Boolean =
    file.toUri.getPath.startsWith(new Path(uri).toUri.getPath + "/_delta_log/")

  // A run of files that dies at a point of the LogStore, inside its commit to its quarantine or
  // to Bronze: the next takes its lease over, and finishes its batch as the table of the data
  // says, once a reader has published what the LogStore has of it.
  for (table <- LogTable.All; point <- LogPoint.All) {
    val recovery = recoveryInside(table, point)
    test(s"files: a run that dies at $point of its commit to ${table.name} ${recovery.left}") {
      val cell = cellOf(Kind.Files)
      val target = table match {
        case LogTable.Quarantine => quarantine(cell.source)
        case LogTable.Bronze => bronze(cell.source)
      }

      HookedLogStore.crashAt(point)(inTheLogOf(target.uri))
      try intercept[SimulatedCrash](cell.run(FaultHooks.Off)).point shouldBe point
      finally HookedLogStore.disarm()

      // As a process that died leaves it: its run running, its lease held, its batch planned.
      val dead = holderOf(cell.lease).value
      runOf(dead).status shouldBe RunStatus.Running
      batchOf(cell.batch).status shouldBe BatchStatus.Planned

      runOut(cell.lease)
      assertFinished(cell, recovery, dead, cell.next())
    }
  }
}

object FaultMatrixSpec {

  /**
   * How long a cell may take: some twenty times what one takes, the first of the suite included,
   * which warms Spark up.
   */
  private val CellTimeLimit = 2.minutes

  /** The package whose frames say in which step a cell that hung was. */
  private val Application = "lakehouse.ingestion."

  /** The kinds of batch of the matrix, each with the fault points that its run reaches. */
  sealed abstract class Kind(val name: String, val points: Seq[FaultPoint])

  object Kind {

    case object Files extends Kind("files", FaultPoint.All)

    case object Full
        extends Kind("full load", FaultPoint.All.filterNot(_ == FaultPoint.AfterQuarantineWrite))

    case object Incremental
        extends Kind(
          "incremental load",
          FaultPoint.All.filterNot(_ == FaultPoint.AfterQuarantineWrite)
        )

    case object Reprocess extends Kind("reprocess", FaultPoint.All)

    /** The chunk that the run that creates the backfill plans. */
    case object FirstChunk
        extends Kind(
          "backfill, first chunk",
          FaultPoint.All.filterNot(_ == FaultPoint.AfterQuarantineWrite)
        )

    /** A chunk of a backfill that runs before resumed, from the progress that they saved. */
    case object ChunkUnderWay
        extends Kind(
          "backfill, chunk under way",
          FaultPoint.All.filterNot(_ == FaultPoint.AfterQuarantineWrite)
        )

    val All: Seq[Kind] = Seq(Files, Full, Incremental, Reprocess, FirstChunk, ChunkUnderWay)
  }

  /** What a batch makes of a change of its source at a point. */
  sealed abstract class Change(val saying: String)

  object Change {

    /** The batch is given up, with nothing of it in its tables (ADR-0018). */
    case object GivenUp
        extends Change("has its batch given up, and the next run reads it as it is now")

    /** The batch reads the source as it is once it changed. */
    case object Read extends Change("is read by the batch as it is by then")

    /** The batch has what was read before the change, and the next run reads the change. */
    case object Unseen
        extends Change("is not seen by the batch, which has what it read; the next run reads it")
  }

  /**
   * The oracle of the column of a source that changes (P5.7, ADR-0018, P6). A batch of files is
   * read once, kept apart from the files, and its files are confirmed just before the read and
   * just after it, both inside the read and so before `AfterRead`: a change before the read has
   * the batch given up, and one after it is not seen. A batch of a table is read as it is written,
   * in one pass: the rows are asked for at the write, after `AfterCounts`, and a change before the
   * write is read, while one after it is not.
   */
  def changeAt(kind: Kind, point: FaultPoint): Change = kind match {
    case Kind.Files | Kind.Reprocess =>
      if (point == FaultPoint.AfterPlan) Change.GivenUp else Change.Unseen
    case _ => if (BeforeTheWrite.contains(point)) Change.Read else Change.Unseen
  }

  /** The points of a run of a table before its rows are asked for. */
  private val BeforeTheWrite =
    Seq(FaultPoint.AfterPlan, FaultPoint.AfterRead, FaultPoint.AfterCounts)

  /** A file with a record of three that cannot be parsed, which its source lets be rejected. */
  private val RejectingCsv = "id,amount\n1,2\n3\n4,5\n"

  /** What the file of a batch is written again with: another size, and another time. */
  private val ChangedCsv = "id,amount\n10,20\n30,40\n"

  /**
   * The points where a run of files holds what it read, kept apart from the files, and has
   * written nothing of it to the table of its data.
   */
  private val HoldingWhatItRead =
    Seq(FaultPoint.AfterRead, FaultPoint.AfterCounts, FaultPoint.AfterQuarantineWrite)

  /** The first day of 2026: the first window of a source that reads a day at a time from it. */
  private val Day = Instant.parse("2026-01-01T00:00:00Z")

  /** How a source of a table reads it by windows of a day, from `Day`. */
  private val WindowOfADay =
    "{ mode: incremental, watermark: { column: updated_at, safety_margin: 1h, " +
      s"""start: "$Day" }, max_batch_range: 1d }"""

  /** The start of the range of the backfill of a chunk: long before `Day`. */
  private val History = Instant.parse("2025-06-01T00:00:00Z")

  private val Backfilled = BackfillId.parse("matrix").toOption.get
}
