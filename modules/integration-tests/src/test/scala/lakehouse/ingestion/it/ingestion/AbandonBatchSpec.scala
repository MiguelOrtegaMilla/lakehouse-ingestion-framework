package lakehouse.ingestion.it.ingestion

import java.net.URI
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{Files, Path, Paths}
import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.time.temporal.ChronoUnit

import scala.concurrent.duration._
import scala.util.Using

import lakehouse.ingestion.core.config.{ConnectorKind, Source}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.core.watermark.{BackfillRange, WatermarkPlan}
import lakehouse.ingestion.engine.{FaultPoint, IngestionService}
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.control.LeasedRun
import lakehouse.ingestion.engine.operator.OperatorActions
import lakehouse.ingestion.engine.quarantine.QuarantineReader
import lakehouse.ingestion.it.support.Died
import lakehouse.ingestion.it.support.ControlPlaneSchema.update
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * An operator gives up the planned batch of a source (ADR-0021 §2), on real files, a real table
 * of a database, real Delta on the coordinated LogStore and a real control plane: the batch is
 * given up only when the table that it was planned for is known not to have it, and the next run
 * plans the next batch from the state as it is. Each test ends with the ledger of the source.
 */
final class AbandonBatchSpec extends AnyFunSuite with Matchers with JdbcIngestionSuite {

  private lazy val actions =
    new OperatorActions(database, spark, lakeRoot, timing, ConnectorKind.BuiltIn)

  private val asked = OperatorRequest.of("ops.oncall", "the plan cannot be run").toOption.get

  private def abandon(
      of: Source,
      seq: Long,
      backfill: Option[BackfillId] = None
  ): OperatorActions.Outcome =
    actions.abandon(of, versionOf(of), BatchSeq.of(seq).value, asked, backfill)

  /** A run of the source that dies at `point`, and whose lease then runs out. */
  private def dies(of: Source, at: FaultPoint): RunId = {
    intercept[Died](launch(of, service(hookAt(at)(_ => throw new Died))))
    val dead = holderOf(of).value
    expire(of)
    dead
  }

  private def done(outcome: OperatorActions.Outcome): OperatorActions.Outcome.Done =
    outcome match {
      case abandoned: OperatorActions.Outcome.Done => abandoned
      case other => fail(s"the batch was not given up: $other")
    }

  private def failed(outcome: OperatorActions.Outcome, as: FailureClass): String =
    outcome match {
      case OperatorActions.Outcome.Failed(_, failure, message) =>
        failure shouldBe as
        message
      case other => fail(s"the action did not fail: $other")
    }

  private def directory(table: lakehouse.ingestion.core.config.TableLocation): Path =
    Paths.get(URI.create(table.uri))

  // -----------------------------------------------------------------------------------------------
  // A batch that its table does not have
  // -----------------------------------------------------------------------------------------------

  test("a batch that no table has is given up by a run of its own, and the next plans anew") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    val first = batchId(orders, 1)
    val dead = dies(orders, FaultPoint.AfterPlan)

    val action = done(abandon(orders, 1))

    action.message should include(s"the batch $first was given up by the run ${action.run}")
    // The batch says who gave it up and why; the run says who asked for it, and is the end of
    // the dead run's lease, as any run that takes it over is.
    val batch = batchOf(first)
    batch.status shouldBe BatchStatus.Abandoned
    batch.abandoned.value.by shouldBe Some(action.run)
    batch.abandoned.value.reason shouldBe "the plan cannot be run"
    batch.abandoned.value.detail shouldBe None
    val run = runOf(action.run)
    run.status shouldBe RunStatus.Succeeded
    run.start.kind shouldBe RunKind.Abandon
    run.start.request shouldBe Some(asked)
    run.batch shouldBe Some(first.seq)
    runOf(dead).status shouldBe RunStatus.Abandoned
    holderOf(orders) shouldBe None
    tableIsThere(orders) shouldBe false
    stateOf(orders) shouldBe None

    // The number is not used again, and the file is the next batch's.
    ingested(launch(orders)).batch shouldBe batchId(orders, 2)
    rowsOf(orders) shouldBe Seq(("1", 2L, "a.csv"))
    takenOf(orders).map(file => file._1 -> file._3) shouldBe Seq("a.csv" -> 2L)
    assertLedger(orders)
  }

  test("a batch whose quarantine has it and whose data table has not is given up, records kept") {
    val orders = fileSource(rejected = Some("0.5"))
    drop(orders, "a.csv", "id,amount\n1,2\n3\n")
    dies(orders, FaultPoint.AfterQuarantineWrite)
    quarantinedOf(orders) shouldBe Seq((1L, "a.csv", "3"))

    done(abandon(orders, 1))

    // The data table decides: it has not the batch. The quarantine keeps its records, which a
    // reader of the quarantine leaves out with the batch.
    batchOf(batchId(orders, 1)).status shouldBe BatchStatus.Abandoned
    quarantinedOf(orders) shouldBe Seq((1L, "a.csv", "3"))
    ingested(launch(orders)).batch shouldBe batchId(orders, 2)
    quarantinedOf(orders) shouldBe Seq((1L, "a.csv", "3"), (2L, "a.csv", "3"))
    new QuarantineReader(database, spark, lakeRoot)
      .records(orders.id)
      .value
      .select("batch_seq")
      .collect()
      .map(_.getLong(0))
      .toSeq shouldBe Seq(2L)
    assertLedger(orders)
  }

  test("giving up mends a plan, and not data: the same file fails the next batch the same way") {
    // ADR-0021 §2, point 4. A file with more rejected records than the source allows.
    val orders = fileSource()
    drop(orders, "a.csv", "id,amount\n1,2\n3\n")
    launch(orders) match {
      case Outcome.Failed(_, failure, _) => failure shouldBe FailureClass.Data
      case other => fail(s"the run did not fail: $other")
    }

    done(abandon(orders, 1))

    launch(orders) match {
      case Outcome.Failed(_, failure, _) => failure shouldBe FailureClass.Data
      case other => fail(s"the run did not fail: $other")
    }
    batchesOf(orders) shouldBe Seq((1L, "ABANDONED"), (2L, "PLANNED"))
    assertLedger(orders)
  }

  test("a window of a table that is given up is planned again from the same watermark") {
    asOwner(
      execute(
        _,
        "CREATE TABLE given_up_window (id int PRIMARY KEY, name text, updated_at timestamptz NOT NULL)"
      )
    )
    asOwner { c =>
      Using.resource(
        c.prepareStatement("INSERT INTO given_up_window VALUES (1, 'a', ?)")
      ) { statement =>
        statement.setObject(
          1,
          OffsetDateTime.ofInstant(Instant.now().minus(3, ChronoUnit.HOURS), ZoneOffset.UTC)
        )
        statement.executeUpdate(): Unit
      }
    }
    val window = jdbcSource(
      "public.given_up_window",
      load = """load:
               |  mode: incremental
               |  watermark: { column: updated_at, safety_margin: 1h }""".stripMargin
    )
    intercept[Died](launch(window, reading(hookAt(FaultPoint.AfterPlan)(_ => throw new Died))))
    expire(window)
    val planned = WatermarkPlan.fromJson(batchOf(batchId(window, 1)).plan).toOption.get

    done(abandon(window, 1))

    // The watermark did not move: the next batch reads the window from where the first was to.
    stateOf(window) shouldBe None
    val next = ingested(launch(window, reading()))
    next.batch shouldBe batchId(window, 2)
    WatermarkPlan.fromJson(batchOf(next.batch).plan).toOption.get.lower shouldBe planned.lower
    next.counts.rowsWritten shouldBe Some(1)
    assertLedger(window)
  }

  // -----------------------------------------------------------------------------------------------
  // The chunk of a backfill (P7.7)
  // -----------------------------------------------------------------------------------------------

  test(
    "the chunk of a backfill is given up under its lease, and the batch with its number is not"
  ) {
    asOwner(
      execute(
        _,
        "CREATE TABLE given_up_chunk (id int PRIMARY KEY, updated_at timestamptz NOT NULL)"
      )
    )
    asOwner(
      execute(
        _,
        "INSERT INTO given_up_chunk VALUES (1, '2025-01-01T12:00:00Z'), (2, '2025-01-02T12:00:00Z')"
      )
    )
    val changes = jdbcSource(
      "public.given_up_chunk",
      load = """load:
               |  mode: incremental
               |  watermark: { column: updated_at, safety_margin: 1h }""".stripMargin
    )
    val history = BackfillId.parse("history").value
    val request = BackfillRequest(
      history,
      BackfillRange
        .of(Instant.parse("2025-01-01T00:00:00Z"), Instant.parse("2025-01-03T00:00:00Z"))
        .value,
      Some(1.day)
    )
    def backfill(by: IngestionService): Outcome =
      by.backfill(changes, versionOf(changes), request, asked)
    val dying = reading(hookAt(FaultPoint.AfterPlan)(_ => throw new Died))
    // The batch 1 of the source and the chunk 1 of the backfill, each planned by a run that died.
    intercept[Died](launch(changes, dying))
    expire(changes)
    intercept[Died](backfill(dying))
    asSuperuser { c =>
      update(
        c,
        "UPDATE control.source_lease SET expires_at = now() - interval '1 second' " +
          "WHERE lease_key = ?",
        LeaseKey.backfill(changes.id, Layer.Bronze, history).value
      ) shouldBe 1
    }
    val chunk = BatchId(TxnAppId.backfill(changes.id, Layer.Bronze, history), BatchSeq.of(1).value)
    val window = WatermarkPlan.fromJson(batchOf(chunk).plan).toOption.get

    val action = done(abandon(changes, 1, Some(history)))

    action.message should include(s"the batch $chunk was given up by the run ${action.run}")
    action.message should include(
      "the next run of the backfill history plans the next batch from its progress as it is: " +
        "the window of this one, again"
    )
    batchOf(chunk).status shouldBe BatchStatus.Abandoned
    runOf(action.run).start.backfill shouldBe Some(history)
    runOf(action.run).start.kind shouldBe RunKind.Abandon
    batchOf(batchId(changes, 1)).status shouldBe BatchStatus.Planned
    // Asked again, the backfill has no chunk planned; and one that is not there has none.
    failed(abandon(changes, 1, Some(history)), FailureClass.Permanent) should include(
      s"no batch of ${chunk.app} is planned"
    )
    failed(
      abandon(changes, 1, Some(BackfillId.parse("nope").value)),
      FailureClass.Permanent
    ) should include(s"${changes.id} has no backfill nope")
    batchOf(batchId(changes, 1)).status shouldBe BatchStatus.Planned

    // The next run of the backfill reads the window of the chunk again, as its chunk 2; the next
    // run of the source finishes its batch 1.
    val next = ingested(backfill(reading()))
    next.batch.seq.value shouldBe 2L
    WatermarkPlan.fromJson(batchOf(next.batch).plan).toOption.get.lower shouldBe window.lower
    ingested(launch(changes, reading())).batch shouldBe batchId(changes, 1)
    assertLedger(changes)
  }

  // -----------------------------------------------------------------------------------------------
  // What is refused, with nothing changed
  // -----------------------------------------------------------------------------------------------

  test("a batch that its table has is never given up: the next run commits it") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    dies(orders, FaultPoint.AfterBronzeCommit)

    val refused = abandon(orders, 1)

    failed(refused, FailureClass.Permanent) should include("its rows are written")
    batchOf(batchId(orders, 1)).status shouldBe BatchStatus.Planned
    runOf(refused.run).status shouldBe RunStatus.Failed
    holderOf(orders) shouldBe None
    launch(orders) shouldBe a[Outcome.RolledForward]
    assertLedger(orders)
  }

  test("the table that is asked is the one that the batch was planned for, and no other") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    dies(orders, FaultPoint.AfterBronzeCommit)
    // The source is given another table since: that one has not the batch, the one it was
    // planned for has.
    val moved = fileSource(id = orders.id.value, table = Some(s"bronze/moved/${orders.id.value}"))

    failed(abandon(moved, 1), FailureClass.Permanent) should include(bronze(orders).uri)
    batchOf(batchId(orders, 1)).status shouldBe BatchStatus.Planned

    // With the table that it had, the source commits its batch.
    launch(orders) shouldBe a[Outcome.RolledForward]
    assertLedger(orders)
  }

  test("a batch planned for a table that the source no longer has is given up where it was") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    dies(orders, FaultPoint.AfterPlan)
    val moved = fileSource(id = orders.id.value, table = Some(s"bronze/moved/${orders.id.value}"))
    // A run with the new table is refused: the batch is finished in the table it was planned
    // for. The operator gives it up instead, and the source goes on with its new table.
    launch(moved) shouldBe a[Outcome.Failed]

    done(abandon(moved, 1))

    ingested(launch(moved)).batch shouldBe batchId(moved, 2)
    tableIsThere(orders) shouldBe false
    rowsOf(moved) shouldBe Seq(("1", 2L, "a.csv"))
    assertLedger(moved)
  }

  test("an operator gives up the batch that was looked at, and no other, nor one that is not") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    ingested(launch(orders))

    failed(abandon(orders, 1), FailureClass.Permanent) should include("no batch of")
    drop(orders, "b.csv", "id\n2\n")
    dies(orders, FaultPoint.AfterPlan)
    failed(abandon(orders, 3), FailureClass.Permanent) should include("is 2, and not 3")

    batchesOf(orders) shouldBe Seq((1L, "COMMITTED"), (2L, "PLANNED"))
    launch(orders) shouldBe a[Outcome.Ingested]
    assertLedger(orders)
  }

  test("while a run holds the lease, the action is skipped, recorded with why, and does nothing") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    val holder = LeasedRun.open(database, starting(orders), versionOf(orders), timing).toOption.get

    val skipped = abandon(orders, 1)

    skipped match {
      case OperatorActions.Outcome.Skipped(_, held, _) => held shouldBe holder.start.id
      case other => fail(s"the action was not skipped: $other")
    }
    val recorded = runOf(skipped.run)
    recorded.status shouldBe RunStatus.SkippedLocked
    recorded.start.request shouldBe Some(asked)
    holder.finish(RunEnd.Failed(FailureClass.Transient, "stopped by the test")) shouldBe true
    assertLedger(orders)
  }

  // -----------------------------------------------------------------------------------------------
  // A table that is not there, and one that cannot be read
  // -----------------------------------------------------------------------------------------------

  test("a registered table whose directory is not there is not known to lack the batch: 3") {
    // As a disk that is not mounted leaves it: the table was written, and is not there now.
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    ingested(launch(orders))
    drop(orders, "b.csv", "id\n2\n")
    dies(orders, FaultPoint.AfterPlan)
    val table = directory(bronze(orders))
    val away = table.resolveSibling(s"${table.getFileName}.unmounted")
    Files.move(table, away)

    val unknown = abandon(orders, 2)

    failed(unknown, FailureClass.Transient) should include(
      "is registered in the coordination store, and its directory is not there"
    )
    batchOf(batchId(orders, 2)).status shouldBe BatchStatus.Planned
    runOf(unknown.run).status shouldBe RunStatus.Failed
    holderOf(orders) shouldBe None

    // Once it is there again, it is read, and it has not the batch.
    Files.move(away, table)
    done(abandon(orders, 2))
    ingested(launch(orders)).batch shouldBe batchId(orders, 3)
    assertLedger(orders)
  }

  test("a registered table whose directory is there and whose log is not is not known to: 3") {
    // As a volume of its own that is not mounted leaves a table: the directory that it is
    // mounted on is there, with nothing in it. The batch is written to the table that is away.
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    dies(orders, FaultPoint.AfterBronzeCommit)
    val table = directory(bronze(orders))
    val away = table.resolveSibling(s"${table.getFileName}.unmounted")
    Files.move(table, away)
    Files.createDirectory(table)

    val unknown = abandon(orders, 1)

    failed(unknown, FailureClass.Transient) should include(
      "is registered in the coordination store, and its log is not there"
    )
    batchOf(batchId(orders, 1)).status shouldBe BatchStatus.Planned
    runOf(unknown.run).status shouldBe RunStatus.Failed
    holderOf(orders) shouldBe None

    // Once it is there again, it is read: it has the batch, which the next run commits.
    Files.delete(table)
    Files.move(away, table)
    failed(abandon(orders, 1), FailureClass.Permanent) should include("its rows are written")
    launch(orders) shouldBe a[Outcome.RolledForward]
    assertLedger(orders)
  }

  test("a table that cannot be read is not known to lack the batch: 3, and nothing given up") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    ingested(launch(orders))
    drop(orders, "b.csv", "id\n2\n")
    dies(orders, FaultPoint.AfterPlan)
    val table = directory(bronze(orders))
    val permissions = Files.getPosixFilePermissions(table)
    Files.setPosixFilePermissions(table, PosixFilePermissions.fromString("---------"))
    try {
      // A process that reads what it may not (root) has nothing to tell here.
      assume(!Files.isReadable(table), "the tests run as a user that reads any directory")

      val unknown = abandon(orders, 2)

      failed(unknown, FailureClass.Transient) should include(
        s"the table at ${bronze(orders).uri} could not be read"
      )
      batchOf(batchId(orders, 2)).status shouldBe BatchStatus.Planned
    } finally Files.setPosixFilePermissions(table, permissions): Unit

    done(abandon(orders, 2))
    assertLedger(orders)
  }

  // -----------------------------------------------------------------------------------------------
  // A run in flight
  // -----------------------------------------------------------------------------------------------

  test("a run that lost its lease to the action writes nothing of the batch it gave up") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    var action: Option[OperatorActions.Outcome] = None
    // The run is stalled after its counts, its lease runs out, and an operator gives its batch
    // up meanwhile: the run then goes on to write, under an epoch that is no longer in force.
    val stalled = service(hookAt(FaultPoint.AfterCounts) { batch =>
      expire(orders)
      action = Some(abandon(orders, batch.id.seq.value))
    })

    val outcome = launch(orders, stalled)

    done(action.value)
    outcome match {
      case Outcome.Failed(_, failure, message) =>
        failure shouldBe FailureClass.Transient
        message should include("lost the lease")
      case other => fail(s"the run did not lose its lease: $other")
    }
    // What it wrote is data files that no commit names (ADR-0019): no table has the batch.
    delta.lastVersion(bronze(orders), TxnAppId.of(orders.id, Layer.Bronze)) shouldBe None
    ingested(launch(orders)).batch shouldBe batchId(orders, 2)
    rowsOf(orders) shouldBe Seq(("1", 2L, "a.csv"))
    assertLedger(orders)
  }
}
