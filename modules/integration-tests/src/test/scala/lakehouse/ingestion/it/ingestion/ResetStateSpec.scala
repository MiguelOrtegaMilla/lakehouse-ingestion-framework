package lakehouse.ingestion.it.ingestion

import java.time.temporal.ChronoUnit
import java.time.{Instant, OffsetDateTime, ZoneOffset}

import scala.util.Using

import io.circe.Json
import lakehouse.ingestion.core.config.{ConnectorKind, Source}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.core.state.StoredWatermark
import lakehouse.ingestion.core.watermark.WatermarkPlan
import lakehouse.ingestion.engine.FaultPoint
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.control.{LeasedRun, StateChanges, StoredJson}
import lakehouse.ingestion.engine.operator.OperatorActions
import lakehouse.ingestion.engine.operator.OperatorActions.WatermarkReset
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.it.support.TakenAtItsWord
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * An operator resets the state of a source (ADR-0021 §3), on real files, a real table of a
 * database, real Delta on the coordinated LogStore and a real control plane: the watermark is
 * set, forgotten or kept, the sequence goes on, the files taken stay taken, and the configuration
 * of the reset is the one that later runs are compared with, which lets a source go on that a
 * breaking change had stopped. Each test ends with the ledger of the source.
 */
final class ResetStateSpec extends AnyFunSuite with Matchers with JdbcIngestionSuite {

  private lazy val actions =
    new OperatorActions(database, spark, lakeRoot, timing, retries, ConnectorKind.BuiltIn)

  private val asked = OperatorRequest.of("ops.oncall", "the source was moved").toOption.get

  private def reset(of: Source, to: WatermarkReset): OperatorActions.Outcome =
    actions.reset(of, versionOf(of), to, asked)

  private def done(outcome: OperatorActions.Outcome): OperatorActions.Outcome.Done =
    outcome match {
      case reset: OperatorActions.Outcome.Done => reset
      case other => fail(s"the state was not reset: $other")
    }

  /** Refused for good, with the run recorded as failed, its lease given up, and nothing changed. */
  private def refused(of: Source, outcome: OperatorActions.Outcome): String = outcome match {
    case OperatorActions.Outcome.Failed(run, FailureClass.Permanent, message) =>
      val record = runOf(run)
      record.start.kind shouldBe RunKind.Reset
      record.status shouldBe RunStatus.Failed
      holderOf(of) shouldBe None
      changesOf(of) shouldBe empty
      message should endWith("Nothing was changed")
      message
    case other => fail(s"the reset was not refused: $other")
  }

  /** The rows of `state_change_log` of a source: the run, and the state before and after. */
  private def changesOf(of: Source): Seq[(String, String, String)] = asSuperuser { c =>
    query(
      c,
      "SELECT run_id::text, before::text, after::text FROM control.state_change_log " +
        "WHERE source_id = ? ORDER BY change_id",
      of.id.value
    )
  }.map(row => (row(0), row(1), row(2)))

  private def asJson(text: String): Json = StoredJson.parse(text).toOption.get

  // -----------------------------------------------------------------------------------------------
  // A table of a database, loaded by a watermark
  // -----------------------------------------------------------------------------------------------

  /** What the server of the source says the time is, to the second. */
  private def clockOfTheSource(): Instant =
    asOwner { c =>
      Using.resource(c.createStatement()) { statement =>
        val rows = statement.executeQuery("SELECT CURRENT_TIMESTAMP")
        rows.next()
        rows.getObject(1, classOf[OffsetDateTime]).toInstant.truncatedTo(ChronoUnit.SECONDS)
      }
    }

  /**
   * A table with two clocks, each row stamped by the test: `updated_at`, and `modified_at`,
   * which is `updated_at` moved by `shift`.
   */
  private def twoClocks(name: String, stamps: Seq[(Int, Instant)], shift: Long = 0): Unit =
    asOwner { c =>
      execute(
        c,
        s"CREATE TABLE $name (id int PRIMARY KEY, name text, updated_at timestamptz NOT NULL, " +
          "modified_at timestamptz NOT NULL)"
      )
      stamps.foreach { case (id, at) =>
        Using.resource(c.prepareStatement(s"INSERT INTO $name VALUES (?, ?, ?, ?)")) { insert =>
          insert.setInt(1, id)
          insert.setString(2, s"n$id")
          insert.setObject(3, OffsetDateTime.ofInstant(at, ZoneOffset.UTC))
          insert.setObject(4, OffsetDateTime.ofInstant(at.plusSeconds(shift), ZoneOffset.UTC))
          insert.executeUpdate(): Unit
        }
      }
    }

  private def watermarked(
      table: String,
      id: String = newSourceId().value,
      column: String = "updated_at",
      start: Option[Instant] = None
  ): Source =
    jdbcSource(
      s"public.$table",
      load = s"""load:
                |  mode: incremental
                |  watermark: { column: $column, safety_margin: 1h${start.fold("")(at =>
          s", start: $at"
        )} }""".stripMargin,
      id = id
    )

  /** The ids in the Bronze table of a source, each with the batch that wrote it. */
  private def idsOf(of: Source): Seq[(Int, Long)] =
    spark.read
      .format("delta")
      .load(bronze(of).uri)
      .select("id", "_batch_seq")
      .collect()
      .map(row => row.getInt(0) -> row.getLong(1))
      .sorted
      .toSeq

  private def windowOf(batch: BatchId): WatermarkPlan =
    WatermarkPlan.fromJson(batchOf(batch).plan).toOption.get

  // -----------------------------------------------------------------------------------------------
  // The watermark set, forgotten or kept
  // -----------------------------------------------------------------------------------------------

  test("a watermark set back is read again from, by the next number of the sequence") {
    val now = clockOfTheSource()
    twoClocks(
      "set_back",
      Seq(1 -> now.minus(5, ChronoUnit.HOURS), 2 -> now.minus(3, ChronoUnit.HOURS))
    )
    val orders = watermarked("set_back")
    ingested(launch(orders, reading()))
    val first = stateOf(orders).get
    val back = now.minus(4, ChronoUnit.HOURS)

    val action = done(reset(orders, WatermarkReset.At(back)))

    // The state: the watermark set, the sequence where it was, and moved by the reset's run.
    val state = stateOf(orders).get
    state.watermark shouldBe Some(StoredWatermark(back).toJson)
    state.lastCommitted shouldBe BatchSeq.of(1).toOption.get
    state.updatedBy shouldBe action.run
    action.message should include(s"the last batch committed is still 1, and the next one is 2")
    action.message should include("it lets no breaking change through")
    action.warnings shouldBe empty
    // The run says who asked and why, and the log has the state before and after.
    val run = runOf(action.run)
    run.start.kind shouldBe RunKind.Reset
    run.status shouldBe RunStatus.Succeeded
    run.start.request shouldBe Some(asked)
    holderOf(orders) shouldBe None
    changesOf(orders).map { case (r, b, a) => (r, asJson(b), asJson(a)) } shouldBe Seq(
      (
        action.run.toString,
        StateChanges.document(first),
        StateChanges.document(state.watermark, first.lastCommitted, action.run)
      )
    )

    // The next batch is 2, and Delta does not skip it: row 2 is read again from the watermark.
    val next = ingested(launch(orders, reading()))
    next.batch shouldBe batchId(orders, 2)
    windowOf(next.batch).lower shouldBe Some(back)
    idsOf(orders) shouldBe Seq(1 -> 1L, 2 -> 1L, 2 -> 2L)
    delta.lastVersion(bronze(orders), TxnAppId.of(orders.id, Layer.Bronze)) shouldBe Some(2L)
    assertLedger(orders)
  }

  test("a watermark forgotten has the next run plan from start, as a source with none does") {
    val now = clockOfTheSource()
    val start = now.minus(6, ChronoUnit.HOURS)
    twoClocks(
      "forgotten",
      Seq(1 -> now.minus(5, ChronoUnit.HOURS), 2 -> now.minus(3, ChronoUnit.HOURS))
    )
    val orders = watermarked("forgotten", start = Some(start))
    ingested(launch(orders, reading()))

    done(reset(orders, WatermarkReset.Forget))

    stateOf(orders).get.watermark shouldBe None
    val next = ingested(launch(orders, reading()))
    next.batch shouldBe batchId(orders, 2)
    windowOf(next.batch).lower shouldBe Some(start)
    idsOf(orders) shouldBe Seq(1 -> 1L, 1 -> 2L, 2 -> 1L, 2 -> 2L)
    assertLedger(orders)
  }

  // -----------------------------------------------------------------------------------------------
  // A source that a breaking change stopped goes on (§20)
  // -----------------------------------------------------------------------------------------------

  test("a source whose watermark column changed is refused, and goes on once it is reset") {
    val now = clockOfTheSource()
    // modified_at is updated_at an hour later.
    twoClocks(
      "other_clock",
      Seq(1 -> now.minus(5, ChronoUnit.HOURS), 2 -> now.minus(3, ChronoUnit.HOURS)),
      shift = 3600
    )
    val id = newSourceId().value
    val byUpdated = watermarked("other_clock", id)
    val byModified = watermarked("other_clock", id, column = "modified_at")
    ingested(launch(byUpdated, reading()))
    launch(byModified, reading()) match {
      case Outcome.Failed(_, FailureClass.Permanent, message) =>
        message should include(
          "load.watermark.column of the source was updated_at and is modified_at"
        )
        message should include("reset-state")
      case other => fail(s"the run was not refused: $other")
    }

    val from = now.minus(150, ChronoUnit.MINUTES)
    val action = done(reset(byModified, WatermarkReset.At(from)))

    action.message should include(
      "it lets through: load.watermark.column of the source was updated_at and is modified_at"
    )
    action.warnings shouldBe empty
    // Row 2 was modified two hours ago, after the watermark: the next batch reads it again.
    val next = ingested(launch(byModified, reading()))
    next.batch shouldBe batchId(byModified, 2)
    windowOf(next.batch).column.value shouldBe "modified_at"
    idsOf(byModified) shouldBe Seq(1 -> 1L, 2 -> 1L, 2 -> 2L)
    // What the source is compared with is the reset's: the old configuration is now the change.
    launch(byUpdated, reading()) shouldBe a[Outcome.Failed]
    assertLedger(byModified, byUpdated)
  }

  test("a watermark kept across another column is warned of: rows may be skipped") {
    val now = clockOfTheSource()
    twoClocks("kept_clock", Seq(1 -> now.minus(5, ChronoUnit.HOURS)))
    val id = newSourceId().value
    val byUpdated = watermarked("kept_clock", id)
    val byModified = watermarked("kept_clock", id, column = "modified_at")
    ingested(launch(byUpdated, reading()))
    val kept = stateOf(byUpdated).get.watermark

    val action = done(reset(byModified, WatermarkReset.Keep))

    stateOf(byModified).get.watermark shouldBe kept
    action.warnings shouldBe Seq(
      "the watermark was kept, and it was a value of updated_at: it is taken now as a value of " +
        "modified_at. If modified_at does not mean what updated_at did, rows whose modified_at " +
        "is at or before the watermark are skipped without a word. Keeping it is right for a " +
        "column that was renamed at the source; for another, reset with --watermark or " +
        "--forget-watermark"
    )
    // The next run counts from the watermark that was kept: row 1, modified before it, is not
    // read again, as the warning says of such a column.
    launch(byModified, reading()) should not be a[Outcome.Failed]
    idsOf(byModified) shouldBe Seq(1 -> 1L)
    assertLedger(byModified, byUpdated)
  }

  test("a source of files whose Bronze table was moved goes on in the new one, warned of it") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    ingested(launch(orders))
    val moved = fileSource(
      id = orders.id.value,
      table = Some(s"bronze/moved/${orders.id.value.replace('.', '_')}")
    )
    launch(moved) match {
      case Outcome.Failed(_, FailureClass.Permanent, message) =>
        message should include("reset-state")
      case other => fail(s"the run was not refused: $other")
    }

    val action = done(reset(moved, WatermarkReset.Keep))

    action.message should include("it lets through: targets.bronze.table of the source was")
    action.warnings should have size 1
    action.warnings.head should include(
      "the history of the source stays in the table that it was ingested to, and the new one " +
        "receives only what is ingested from now on"
    )
    // The files taken stay taken: the next run finds nothing new, and the history stays where
    // it was. A new file goes to the new table.
    launch(moved) shouldBe a[Outcome.NothingNew]
    drop(moved, "b.csv", "id\n2\n")
    ingested(launch(moved)).batch shouldBe batchId(moved, 2)
    rowsOf(moved) shouldBe Seq(("2", 2L, "b.csv"))
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"))
    assertLedger(moved)
  }

  // -----------------------------------------------------------------------------------------------
  // What is refused, with nothing changed
  // -----------------------------------------------------------------------------------------------

  test("a reset is refused while a batch is planned, and fences the run that planned it") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    var refusal: Option[OperatorActions.Outcome] = None
    // The run is stalled after its counts, its lease runs out, and an operator resets meanwhile.
    val stalled = service(hookAt(FaultPoint.AfterCounts) { _ =>
      expire(orders)
      refusal = Some(reset(orders, WatermarkReset.Keep))
    })

    val outcome = launch(orders, stalled)

    refused(orders, refusal.get) should include(
      s"the batch ${batchId(orders, 1)} is planned: the state of a source is not reset while a batch is"
    )
    // The reset announced its epoch: the stalled run commits nothing after it.
    outcome match {
      case Outcome.Failed(_, FailureClass.Transient, message) =>
        message should include("lost the lease")
      case other => fail(s"the run did not lose its lease: $other")
    }
    stateOf(orders) shouldBe None
    ingested(launch(orders)).batch shouldBe batchId(orders, 1)
    assertLedger(orders)
  }

  test("a source with no state, or a watermark that it does not keep, is refused") {
    val files = fileSource()
    refused(files, reset(files, WatermarkReset.Keep)) should include(
      s"${files.id} has no state in bronze: no batch of it was ever committed"
    )
    drop(files, "a.csv", "id\n1\n")
    ingested(launch(files))
    refused(files, reset(files, WatermarkReset.At(Now))) should include(
      s"${files.id} keeps no watermark: it is of the kind 'file', loaded new_files"
    )
    refused(files, reset(files, WatermarkReset.Forget)) should include(
      s"${files.id} has no watermark to forget"
    )
    stateOf(files).get.updatedBy shouldBe runOf(batchOf(batchId(files, 1)).committedBy.get).start.id
    assertLedger(files)

    val now = clockOfTheSource()
    twoClocks("whole", Seq(1 -> now.minus(5, ChronoUnit.HOURS)))
    val whole = jdbcSource("public.whole")
    ingested(launch(whole, reading()))
    refused(whole, reset(whole, WatermarkReset.At(now))) should include("loaded full")
    assertLedger(whole)

    val start = now.minus(6, ChronoUnit.HOURS)
    val started = watermarked("whole", start = Some(start))
    ingested(launch(started, reading()))
    refused(started, reset(started, WatermarkReset.At(start.minusSeconds(1)))) should include(
      "is before load.watermark.start"
    )
    assertLedger(started)
  }

  test("while a run holds the lease, the reset is skipped, recorded, and changes nothing") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    ingested(launch(orders))
    val state = stateOf(orders)
    val holder = LeasedRun
      .open(database, starting(orders), versionOf(orders), timing, TakenAtItsWord)
      .toOption
      .get

    val skipped =
      try reset(orders, WatermarkReset.Keep)
      finally holder.finish(RunEnd.Succeeded()): Unit

    skipped match {
      case OperatorActions.Outcome.Skipped(run, by, _) =>
        by shouldBe holder.start.id
        runOf(run).status shouldBe RunStatus.SkippedLocked
      case other => fail(s"the reset was not skipped: $other")
    }
    stateOf(orders) shouldBe state
    changesOf(orders) shouldBe empty
    assertLedger(orders)
  }

  test("a reset whose lease was taken over changes nothing") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    ingested(launch(orders))
    val before = stateOf(orders).get
    val zombie = LeasedRun
      .open(
        database,
        starting(orders).copy(kind = RunKind.Reset, request = Some(asked)),
        versionOf(orders),
        timing,
        TakenAtItsWord
      )
      .toOption
      .get
    expire(orders)
    launch(orders) shouldBe a[Outcome.NothingNew]

    zombie.reset(before, None) shouldBe false

    stateOf(orders) shouldBe Some(before)
    changesOf(orders) shouldBe empty
    runOf(zombie.start.id).status shouldBe RunStatus.Abandoned
    assertLedger(orders)
  }
}
