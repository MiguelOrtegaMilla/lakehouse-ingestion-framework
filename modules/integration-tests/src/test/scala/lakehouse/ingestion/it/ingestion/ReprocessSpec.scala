package lakehouse.ingestion.it.ingestion

import java.nio.file.Files

import lakehouse.ingestion.core.config.{ConnectorKind, Source}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.{FaultPoint, IngestionService}
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.control.LeasedRun
import lakehouse.ingestion.engine.operator.OperatorActions
import lakehouse.ingestion.engine.operator.OperatorActions.WatermarkReset
import lakehouse.ingestion.it.support.{Died, TakenAtItsWord}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * An operator has the files of a committed batch read again (ADR-0021 §4), on real files, real
 * Delta on the coordinated LogStore and a real control plane: a batch of the regular sequence,
 * of the kind `reprocess`, with the plan of the batch it names, written to the table that the
 * configuration names now, and recording no file as taken. From its plan on it is a batch like
 * any other. Each test ends with the ledger of the source.
 */
final class ReprocessSpec extends AnyFunSuite with Matchers with JdbcIngestionSuite {

  private val asked = OperatorRequest.of("ops.oncall", "the parser was fixed").toOption.get

  private def reprocess(
      of: Source,
      seq: Long,
      by: IngestionService = service()
  ): Outcome =
    by.reprocess(of, versionOf(of), BatchSeq.of(seq).toOption.get, asked)

  private def refused(of: Source, outcome: Outcome): String = outcome match {
    case Outcome.Failed(run, FailureClass.Permanent, message) =>
      runOf(run).start.kind shouldBe RunKind.Reprocess
      runOf(run).status shouldBe RunStatus.Failed
      holderOf(of) shouldBe None
      message should include("Nothing was planned, read or written")
      message
    case other => fail(s"the reprocess was not refused: $other")
  }

  /** The rows of the source in its Bronze table: id, batch and run. */
  private def copiesOf(of: Source): Seq[(String, Long, String)] =
    spark.read
      .format("delta")
      .load(bronze(of).uri)
      .select("id", "_batch_seq", "_run_id")
      .collect()
      .map(row => (row.getString(0), row.getLong(1), row.getString(2)))
      .sorted
      .toSeq

  /** A source of files with two batches committed: a.csv in the first, b.csv in the second. */
  private def twoBatches(): Source = {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n2\n")
    ingested(launch(orders))
    drop(orders, "b.csv", "id\n3\n")
    ingested(launch(orders))
    orders
  }

  // -----------------------------------------------------------------------------------------------
  // The files of a batch read again
  // -----------------------------------------------------------------------------------------------

  test("a reprocess reads the files of a committed batch again, as the next batch, taking none") {
    val orders = twoBatches()
    val taken = takenOf(orders)
    val first = runOf(batchOf(batchId(orders, 1)).committedBy.get).start.id

    val again = ingested(reprocess(orders, 1))

    again.batch shouldBe batchId(orders, 3)
    again.counts.rowsWritten shouldBe Some(2)
    // The batch says what it reads again, and its plan is that of batch 1.
    val batch = batchOf(again.batch)
    batch.kind shouldBe RunKind.Reprocess
    batch.reprocessOf shouldBe BatchSeq.of(1).toOption
    batch.plan shouldBe batchOf(batchId(orders, 1)).plan
    batch.status shouldBe BatchStatus.Committed
    // The run says who asked and why.
    val run = runOf(again.run)
    run.start.kind shouldBe RunKind.Reprocess
    run.start.request shouldBe Some(asked)
    run.status shouldBe RunStatus.Succeeded
    // Bronze keeps both copies, each with its run; the files stay taken by batch 1.
    copiesOf(orders) shouldBe Seq(
      ("1", 1L, first.toString),
      ("1", 3L, again.run.toString),
      ("2", 1L, first.toString),
      ("2", 3L, again.run.toString),
      ("3", 2L, runOf(batchOf(batchId(orders, 2)).committedBy.get).start.id.toString)
    )
    takenOf(orders) shouldBe taken
    stateOf(orders).get.lastCommitted shouldBe BatchSeq.of(3).toOption.get
    // The next run plans from the files that are left: none.
    launch(orders) shouldBe a[Outcome.NothingNew]
    assertLedger(orders)
  }

  test("a reprocess reads the files as the source says to read them now: a dialect corrected") {
    val orders = fileSource()
    // The producer escapes with a backslash, and the source said RFC 4180: the value keeps both.
    drop(orders, "a.csv", "id,name\n1,\"a\\\\b\"\n")
    ingested(launch(orders))
    def names: Seq[(Long, String)] =
      spark.read
        .format("delta")
        .load(bronze(orders).uri)
        .select("_batch_seq", "name")
        .collect()
        .map(row => (row.getLong(0), row.getString(1)))
        .sorted
        .toSeq
    names shouldBe Seq((1L, "a\\\\b"))

    val corrected =
      fileSource(options = """, csv: { escape: "\\" }""", id = orders.id.value)
    val again = ingested(reprocess(corrected, 1))

    // Its plan has the files of batch 1, read with the escape that the source says now.
    val plan = batchOf(again.batch).plan
    plan.hcursor.downField("files").focus shouldBe
      batchOf(batchId(orders, 1)).plan.hcursor.downField("files").focus
    plan.hcursor.downField("csv").get[String]("escape").toOption shouldBe Some("\\")
    names shouldBe Seq((1L, "a\\\\b"), (2L, "a\\b"))
    assertLedger(corrected)
  }

  test("after a reset to another Bronze table, a reprocess brings the history to it") {
    val orders = twoBatches()
    val moved = fileSource(
      id = orders.id.value,
      table = Some(s"bronze/moved/${orders.id.value.replace('.', '_')}")
    )
    val actions =
      new OperatorActions(database, spark, lakeRoot, timing, retries, ConnectorKind.BuiltIn)
    actions.reset(moved, versionOf(moved), WatermarkReset.Keep, asked) shouldBe
      a[OperatorActions.Outcome.Done]

    ingested(reprocess(moved, 1)).batch shouldBe batchId(moved, 3)
    ingested(reprocess(moved, 2)).batch shouldBe batchId(moved, 4)

    rowsOf(moved).map(row => (row._1, row._3)) shouldBe
      Seq(("1", "a.csv"), ("2", "a.csv"), ("3", "b.csv"))
    rowsOf(orders).map(_._1) shouldBe Seq("1", "2", "3")
    assertLedger(moved)
  }

  // -----------------------------------------------------------------------------------------------
  // A batch like any other, from its plan on
  // -----------------------------------------------------------------------------------------------

  test("a reprocess that dies after its plan is replayed by the next run, taking no file") {
    val orders = twoBatches()
    val taken = takenOf(orders)
    intercept[Died](
      reprocess(orders, 1, service(hookAt(FaultPoint.AfterPlan)(_ => throw new Died)))
    )
    expire(orders)

    val replayed = ingested(launch(orders))

    replayed.batch shouldBe batchId(orders, 3)
    batchOf(replayed.batch).kind shouldBe RunKind.Reprocess
    takenOf(orders) shouldBe taken
    copiesOf(orders).count(_._2 == 3L) shouldBe 2
    assertLedger(orders)
  }

  test("a reprocess that dies after its commit to Bronze is rolled forward, taking no file") {
    val orders = twoBatches()
    val taken = takenOf(orders)
    intercept[Died](
      reprocess(orders, 1, service(hookAt(FaultPoint.AfterBronzeCommit)(_ => throw new Died)))
    )
    expire(orders)

    launch(orders) shouldBe a[Outcome.RolledForward]

    batchOf(batchId(orders, 3)).status shouldBe BatchStatus.Committed
    takenOf(orders) shouldBe taken
    assertLedger(orders)
  }

  test("a file of the batch that changed since, or that is gone, has the reprocess given up") {
    val orders = twoBatches()
    drop(orders, "a.csv", "id\n1\n2\n4\n")

    reprocess(orders, 1) match {
      case Outcome.GivenUp(_, batch, reason) =>
        batch shouldBe batchId(orders, 3)
        reason should include("a.csv")
      case other => fail(s"the reprocess was not given up: $other")
    }
    batchOf(batchId(orders, 3)).status shouldBe BatchStatus.Abandoned

    Files.delete(directoryOf(orders).resolve("b.csv"))
    reprocess(orders, 2) shouldBe a[Outcome.GivenUp]
    assertLedger(orders)
  }

  test("more records rejected than the source allows now: nothing is committed") {
    val lenient = fileSource(rejected = Some("0.5"))
    drop(lenient, "a.csv", "id,amount\n1,10\n2,20,extra\n")
    ingested(launch(lenient))
    val strict = fileSource(id = lenient.id.value)

    reprocess(strict, 1) match {
      case Outcome.Failed(_, FailureClass.Data, _) => ()
      case other => fail(s"the reprocess was not refused for its data: $other")
    }
    batchOf(batchId(strict, 2)).status shouldBe BatchStatus.Planned
    assertLedger(strict)
  }

  // -----------------------------------------------------------------------------------------------
  // What is refused, with nothing planned
  // -----------------------------------------------------------------------------------------------

  test("a batch that is not committed, or that there is not, is not reprocessed") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    intercept[Died](launch(orders, service(hookAt(FaultPoint.AfterPlan)(_ => throw new Died))))
    expire(orders)
    refused(orders, reprocess(orders, 1)) should include(
      s"the batch ${batchId(orders, 1)} is planned: no batch is reprocessed while one is"
    )
    // The run after gives it up when its file changes, and the batch is then abandoned.
    drop(orders, "a.csv", "id\n1\n2\n")
    launch(orders) shouldBe a[Outcome.GivenUp]
    refused(orders, reprocess(orders, 1)) should include(
      s"the batch ${batchId(orders, 1)} is ABANDONED, and only a batch that was committed"
    )
    refused(orders, reprocess(orders, 7)) should include(
      s"there is no batch ${batchId(orders, 7)} to reprocess"
    )
    batchesOf(orders) shouldBe Seq(1L -> "ABANDONED")
    assertLedger(orders)
  }

  test("a source whose batches are no files is not reprocessed: a window is a backfill's") {
    asOwner { c =>
      execute(c, "CREATE TABLE reprocessed_table (id int, name text)")
      execute(c, "INSERT INTO reprocessed_table VALUES (1, 'a')")
    }
    val table = jdbcSource("public.reprocessed_table")
    ingested(launch(table, reading()))

    refused(table, reprocess(table, 1, reading())) should include(
      "read no files, and only files are reprocessed"
    )
    batchesOf(table) shouldBe Seq(1L -> "COMMITTED")
    assertLedger(table)
  }

  test("a configuration that Bronze cannot follow is refused, as for a run") {
    val orders = twoBatches()
    val moved = fileSource(id = orders.id.value, table = Some("bronze/elsewhere/orders"))

    refused(moved, reprocess(moved, 1)) should include("reset-state")
    batchesOf(orders) shouldBe Seq(1L -> "COMMITTED", 2L -> "COMMITTED")
    assertLedger(orders)
  }

  test("while a run holds the lease, the reprocess is skipped, and plans nothing") {
    val orders = twoBatches()
    val holder = LeasedRun
      .open(database, starting(orders), versionOf(orders), timing, TakenAtItsWord)
      .toOption
      .get

    val skipped =
      try reprocess(orders, 1)
      finally holder.finish(RunEnd.Succeeded()): Unit

    skipped match {
      case Outcome.Skipped(run, by, _) =>
        by shouldBe holder.start.id
        runOf(run).status shouldBe RunStatus.SkippedLocked
        runOf(run).start.kind shouldBe RunKind.Reprocess
      case other => fail(s"the reprocess was not skipped: $other")
    }
    batchesOf(orders) shouldBe Seq(1L -> "COMMITTED", 2L -> "COMMITTED")
    assertLedger(orders)
  }
}
