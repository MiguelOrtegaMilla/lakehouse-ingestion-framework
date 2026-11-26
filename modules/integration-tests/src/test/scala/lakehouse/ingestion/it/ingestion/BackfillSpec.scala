package lakehouse.ingestion.it.ingestion

import java.time.{Clock, Instant, OffsetDateTime, ZoneOffset}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.util.Using

import io.circe.Json
import lakehouse.ingestion.core.config._
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.core.state.StoredWatermark
import lakehouse.ingestion.core.watermark.{BackfillRange, WatermarkHorizon, WatermarkPlan}
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.connector._
import lakehouse.ingestion.engine.control.{Backfills, Leases}
import lakehouse.ingestion.engine.jdbc.JdbcSourceConnector
import lakehouse.ingestion.engine.quarantine.QuarantineReader
import lakehouse.ingestion.engine.{FaultHooks, FaultPoint, IngestionService}
import lakehouse.ingestion.engine.failure.RunFailedException
import lakehouse.ingestion.it.ingestion.BackfillSpec.Rejecting
import lakehouse.ingestion.it.support.Died
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * A backfill of a table of a database from end to end (ADR-0021 §5, P7.6): a range of its
 * watermark read in chunks, on a sequence of its own, with the JDBC connector, on a real
 * PostgreSQL as its source, a real Delta on the coordinated LogStore and a real control plane.
 * It is created by its first run and resumed by the next, refused with other arguments, held to
 * the configuration it was created with, recovered as any batch is, and run beside the regular
 * sequence of its source on the same table, which it neither waits for nor touches. The
 * records that its chunks set apart are in the quarantine of its source, and count by the run
 * that set them apart.
 *
 * The rows are stamped in 2025, far behind the clock of the server, which no test sets: what a
 * chunk reads is decided by the range and the chunk alone. Each test ends with the ledger of
 * the source, of every sequence of it.
 */
final class BackfillSpec extends AnyFunSuite with Matchers with JdbcIngestionSuite {

  private val asked = OperatorRequest.of("ops.oncall", "the history before go-live").toOption.get

  private def day(n: Int): Instant = Instant.parse(f"2025-01-$n%02dT00:00:00Z")

  // -----------------------------------------------------------------------------------------------
  // A source of a table of changes, and a backfill of it
  // -----------------------------------------------------------------------------------------------

  private def tableOf(id: String): String = s"public.${id.replace('.', '_')}"

  /**
   * A source that reads its own table by a watermark column.
   *
   * @param watermark the settings of the watermark after its column, as a file writes them
   * @param range the widest stretch of the watermark that a batch takes; none, unless said
   */
  private def watermarked(
      id: String,
      column: String = "updated_at",
      watermark: String = "safety_margin: 1h",
      range: Option[String] = None
  ): Source =
    jdbcSource(
      tableOf(id),
      load = s"""load:
                 |  mode: incremental
                 |  watermark: { column: $column, $watermark }
                 |  ${range.fold("")(widest => s"max_batch_range: $widest")}""".stripMargin,
      id = id
    )

  /** A source of a table of changes of its own, made empty: each row with when it changed. */
  private def changes(watermark: String = "safety_margin: 1h", range: Option[String] = None) = {
    val id = newSourceId().value
    asOwner(
      execute(
        _,
        s"CREATE TABLE ${tableOf(id)} (id int PRIMARY KEY, updated_at timestamptz NOT NULL, " +
          "created_at timestamptz NOT NULL DEFAULT '2025-01-01T00:00:00Z')"
      )
    )
    watermarked(id, watermark = watermark, range = range)
  }

  /** Rows of the table of a source, each stamped as the test says. */
  private def stamped(of: Source, rows: (Int, Instant)*): Unit =
    asOwner { c =>
      Using.resource(
        c.prepareStatement(s"INSERT INTO ${tableOf(of.id.value)} (id, updated_at) VALUES (?, ?)")
      ) { statement =>
        rows.foreach { case (id, at) =>
          statement.setInt(1, id)
          statement.setObject(2, OffsetDateTime.ofInstant(at, ZoneOffset.UTC))
          statement.executeUpdate(): Unit
        }
      }
    }

  private def backfillId(id: String): BackfillId = BackfillId.parse(id).value

  /** One run of a backfill, as the command runs one for each chunk. */
  private def backfill(
      of: Source,
      id: String,
      from: Instant,
      to: Instant,
      chunk: Option[FiniteDuration] = Some(1.day),
      by: IngestionService = reading()
  ): Outcome =
    by.backfill(
      of,
      versionOf(of),
      BackfillRequest(backfillId(id), BackfillRange.of(from, to).value, chunk),
      asked
    )

  /** The runs of a backfill until one says that there is no more, as the command chains them. */
  private def throughout(
      of: Source,
      id: String,
      from: Instant,
      to: Instant,
      chunk: Option[FiniteDuration] = Some(1.day)
  ): Seq[Outcome.Ingested] = {
    val done = ArrayBuffer.empty[Outcome.Ingested]
    while (done.lastOption.forall(_.next != Outcome.Next.Ends)) {
      withClue(s"after ${done.size} chunks: ")(done.size should be < 20)
      done += ingested(backfill(of, id, from, to, chunk))
    }
    done.toSeq
  }

  private def recordOf(of: Source, id: String): Option[BackfillRecord] =
    database.run(Backfills.find(_, of.id, Layer.Bronze, backfillId(id)))

  private def chunkOf(of: Source, id: String, seq: Long): BatchId =
    BatchId(TxnAppId.backfill(of.id, Layer.Bronze, backfillId(id)), BatchSeq.of(seq).value)

  private def windowOf(batch: BatchId): WatermarkPlan =
    WatermarkPlan.fromJson(batchOf(batch).plan).value

  private def progressOf(of: Source, id: String): Option[Instant] =
    recordOf(of, id).flatMap(_.progress).map(p => StoredWatermark.fromJson(p.watermark).value.value)

  private def backfillLease(of: Source, id: String): LeaseKey =
    LeaseKey.backfill(of.id, Layer.Bronze, backfillId(id))

  private def holderOfBackfill(of: Source, id: String): Option[RunId] =
    database.run(Leases.find(_, backfillLease(of, id))).flatMap(_.holder).map(_.run)

  /** The lease of a backfill runs out in the database, whatever its holder believes of it. */
  private def expireBackfill(of: Source, id: String): Unit = asSuperuser { c =>
    update(
      c,
      "UPDATE control.source_lease SET expires_at = now() - interval '1 second' WHERE lease_key = ?",
      backfillLease(of, id).value
    ) shouldBe 1: Unit
  }

  /** The batches of every sequence of a source: its sequence, number, status and kind. */
  private def sequencesOf(of: Source): Seq[(String, Long, String, String)] = asSuperuser { c =>
    query(
      c,
      "SELECT txn_app_id, batch_seq::text, status, run_kind FROM control.ingestion_batch " +
        "WHERE source_id = ? ORDER BY 1, 2",
      of.id.value
    ).map(row => (row(0), row(1).toLong, row(2), row(3)))
  }

  /** The rows of a source in its Bronze table: id, batch number and run. */
  private def copiesOf(of: Source): Seq[(Int, Long, String)] =
    spark.read
      .format("delta")
      .load(bronze(of).uri)
      .select("id", "_batch_seq", "_run_id")
      .collect()
      .map(row => (row.getInt(0), row.getLong(1), row.getString(2)))
      .sorted
      .toSeq

  /** A run of a backfill that was refused for good, with nothing planned and its lease free. */
  private def refused(of: Source, id: String, outcome: Outcome): String = outcome match {
    case Outcome.Failed(run, FailureClass.Permanent, message) =>
      val record = runOf(run)
      record.start.kind shouldBe RunKind.Backfill
      record.start.backfill shouldBe Some(backfillId(id))
      record.start.request shouldBe Some(asked)
      record.status shouldBe RunStatus.Failed
      record.batch shouldBe None
      holderOfBackfill(of, id) shouldBe None
      message should include("Nothing was")
      message
    case other => fail(s"the backfill was not refused: $other")
  }

  // -----------------------------------------------------------------------------------------------
  // A range read in chunks
  // -----------------------------------------------------------------------------------------------

  test("a backfill reads (from, to] in chunks on a sequence of its own, and not the state") {
    // The regular sequence starts in 2026: a backfill is how what is before it is read.
    val orders = changes(watermark = "safety_margin: 1h, start: \"2026-01-01T00:00:00Z\"")
    stamped(
      orders,
      0 -> Instant.parse("2024-12-31T00:00:00Z"),
      1 -> day(1), // at from: not in the range
      2 -> day(1).plusNanos(1000),
      3 -> day(2).plusSeconds(12 * 3600),
      4 -> day(4), // at to: in the range
      5 -> day(4).plusNanos(1000),
      6 -> Instant.parse("2026-03-01T00:00:00Z")
    )

    val chunks = throughout(orders, "history-2025", day(1), day(4))

    chunks.map(_.batch) shouldBe (1L to 3L).map(chunkOf(orders, "history-2025", _))
    chunks.map(_.next == Outcome.Next.Ends) shouldBe Seq(false, false, true)
    // Each chunk is a day of the range, the first after from and the last up to to.
    chunks.map(chunk => windowOf(chunk.batch)).map(w => (w.lower, w.upper)) shouldBe Seq(
      (Some(day(1)), day(2)),
      (Some(day(2)), day(3)),
      (Some(day(3)), day(4))
    )
    val runs = chunks.map(_.run.toString)
    copiesOf(orders) shouldBe Seq((2, 1L, runs(0)), (3, 2L, runs(1)), (4, 3L, runs(2)))
    chunks.foreach { chunk =>
      val run = runOf(chunk.run)
      run.start.kind shouldBe RunKind.Backfill
      run.start.txnAppId shouldBe TxnAppId.backfill(
        orders.id,
        Layer.Bronze,
        backfillId("history-2025")
      )
      run.start.request shouldBe Some(asked)
      run.status shouldBe RunStatus.Succeeded
      batchOf(chunk.batch).kind shouldBe RunKind.Backfill
    }

    // Its progress is its own, and the state of the source is not touched.
    val record = recordOf(orders, "history-2025").value
    record.createdBy shouldBe chunks.head.run
    record.chunk shouldBe 1.day
    (record.from, record.to) shouldBe ((day(1), day(4)))
    record.progress.map(p => (p.lastCommitted.value, p.updatedBy)) shouldBe Some(
      (3L, chunks(2).run)
    )
    progressOf(orders, "history-2025") shouldBe Some(day(4))
    record.completedAt should not be empty
    stateOf(orders) shouldBe None
    holderOf(orders) shouldBe None
    holderOfBackfill(orders, "history-2025") shouldBe None

    // A backfill that is complete has nothing more to read, and its run ends well.
    val after = backfill(orders, "history-2025", day(1), day(4))
    after shouldBe Outcome.NothingNew(after.run)
    runOf(after.run).status shouldBe RunStatus.Succeeded
    sequencesOf(orders).map(_._2) shouldBe Seq(1L, 2L, 3L)
    assertLedger(orders)
  }

  test(
    "a chunk is as long as max_batch_range unless one is asked for; with neither, none is made"
  ) {
    val orders = changes(range = Some("2d"))
    stamped(orders, 1 -> day(2), 2 -> day(5).plusSeconds(60))

    val chunks = throughout(orders, "by-range", day(1), day(6), chunk = None)

    recordOf(orders, "by-range").value.chunk shouldBe 2.days
    chunks.map(chunk => windowOf(chunk.batch).upper) shouldBe Seq(day(3), day(5), day(6))
    copiesOf(orders).map(_._1) shouldBe Seq(1, 2)
    assertLedger(orders)

    val unbounded = changes()
    val message = refused(unbounded, "whole", backfill(unbounded, "whole", day(1), day(6), None))
    message should include(
      "is not created: it is given no chunk (--chunk), and the source has no max_batch_range"
    )
    recordOf(unbounded, "whole") shouldBe None
    sequencesOf(unbounded) shouldBe empty
    assertLedger(unbounded)
  }

  // -----------------------------------------------------------------------------------------------
  // What is refused
  // -----------------------------------------------------------------------------------------------

  test("a source whose connector reads no range of it is not backfilled, nor a range not settled") {
    // A table read whole has no range, and a connector of files reads none.
    val whole = jdbcSource("public.orders")
    refused(whole, "h", backfill(whole, "h", day(1), day(2))) should include(
      s"${whole.id} is not backfilled: its connector reads no range of it"
    )
    val files = fileSource()
    refused(files, "h", backfill(files, "h", day(1), day(2), by = service())) should include(
      "its connector reads no range of it"
    )

    // A range that ends where a transaction may still commit rows.
    val orders = changes()
    val later = Instant.parse("2099-01-01T00:00:00Z")
    refused(orders, "h", backfill(orders, "h", day(1), later)) should include(
      s"is not created: to, $later, is after "
    )

    Seq(whole, files, orders).foreach { of =>
      recordOf(of, "h") shouldBe None
      sequencesOf(of) shouldBe empty
      assertLedger(of)
    }
  }

  test("a backfill is resumed with what created it, and refused with another range or chunk") {
    val orders = changes()
    stamped(orders, 1 -> day(1).plusSeconds(60), 2 -> day(2).plusSeconds(60))
    ingested(backfill(orders, "h", day(1), day(3))).next should not be Outcome.Next.Ends
    val created = recordOf(orders, "h").value

    val otherRange = refused(orders, "h", backfill(orders, "h", day(1), day(4)))
    otherRange should include(
      s"was created by the run ${created.createdBy} at ${created.createdAt}, and this run asks " +
        s"for the range (${day(1)}, ${day(4)}], and it reads (${day(1)}, ${day(3)}]"
    )
    otherRange should include("another range is another backfill, under another id")
    refused(orders, "h", backfill(orders, "h", day(1), day(3), Some(12.hours))) should include(
      "this run asks for chunks of"
    )
    recordOf(orders, "h").value shouldBe created

    // A run that asks for no chunk takes the backfill's.
    val last = ingested(backfill(orders, "h", day(1), day(3), chunk = None))
    last.batch shouldBe chunkOf(orders, "h", 2)
    last.next shouldBe Outcome.Next.Ends
    copiesOf(orders).map(_._1) shouldBe Seq(1, 2)
    assertLedger(orders)
  }

  test("a backfill is held to the configuration it was created with, not to that of the state") {
    val before = changes()
    stamped(before, 1 -> day(1).plusSeconds(60), 2 -> day(2).plusSeconds(60))
    ingested(backfill(before, "h", day(1), day(3)))
    // Another watermark column: what the backfill read cannot be followed by it.
    val after = watermarked(before.id.value, column = "created_at")

    val message = refused(after, "h", backfill(after, "h", day(1), day(3)))
    message should include(
      s"changed since the run ${recordOf(before, "h").value.createdBy} created the backfill"
    )
    message should include("load.watermark.column of the source was updated_at and is created_at")
    sequencesOf(after).count(_._3 == "PLANNED") shouldBe 0

    // The regular sequence has its own: the new configuration is the one its state moves with.
    ingested(launch(after, reading()))
    // And the backfill goes on under the one it was created with.
    ingested(backfill(before, "h", day(1), day(3))).next shouldBe Outcome.Next.Ends
    recordOf(before, "h").value.completedAt should not be empty
    // Once complete it reads nothing more, and is not refused under any configuration.
    val done = backfill(after, "h", day(1), day(3))
    done shouldBe Outcome.NothingNew(done.run)
    assertLedger(after, before)
  }

  // -----------------------------------------------------------------------------------------------
  // Beside the regular sequence, on the same table
  // -----------------------------------------------------------------------------------------------

  test("a backfill and the regular sequence run on the same table, and neither waits") {
    val orders = changes(watermark = "safety_margin: 1h, start: \"2026-01-01T00:00:00Z\"")
    val march = Instant.parse("2026-03-01T00:00:00Z")
    stamped(orders, 1 -> Instant.parse("2025-06-01T00:00:00Z"), 2 -> march)
    val (from, to) = (day(1), Instant.parse("2026-04-01T00:00:00Z"))
    val chunk = Some(365.days)

    // The regular sequence holds its lease while a chunk of the backfill is read and committed.
    var first = Option.empty[Outcome]
    val regular = ingested(
      launch(
        orders,
        reading(hookAt(FaultPoint.AfterPlan) { _ =>
          first = Some(backfill(orders, "overlap", from, to, chunk))
        })
      )
    )
    ingested(first.value).batch shouldBe chunkOf(orders, "overlap", 1)

    // And the backfill holds its own while the regular sequence runs, and a second run of the
    // backfill is skipped.
    var meanwhile = Option.empty[Outcome]
    var again = Option.empty[Outcome]
    val second = ingested(
      backfill(
        orders,
        "overlap",
        from,
        to,
        chunk,
        reading(hookAt(FaultPoint.AfterPlan) { batch =>
          meanwhile = Some(launch(orders, reading()))
          again = Some(backfill(orders, "overlap", from, to, chunk))
          again.value shouldBe a[Outcome.Skipped]
          again.value.asInstanceOf[Outcome.Skipped].holder shouldBe batch.plannedBy: Unit
        })
      )
    )
    second.next shouldBe Outcome.Next.Ends
    meanwhile.value should not be a[Outcome.Skipped]
    meanwhile.value should not be a[Outcome.Failed]

    // Each sequence numbers its own batches, and a range that the regular sequence read is read
    // again: Bronze has both copies of the row of March, each with its run (ADR-0021 §5.8).
    copiesOf(orders) shouldBe Seq(
      (1, 1L, first.value.run.toString),
      (2, 1L, regular.run.toString),
      (2, 2L, second.run.toString)
    ).sorted
    // The state of the source was moved by its regular runs alone: the second had a window
    // with nothing in it, or none, as the clock of the source went on.
    Set(regular.run, meanwhile.value.run) should contain(stateOf(orders).value.updatedBy)
    recordOf(orders, "overlap").value.completedAt should not be empty
    holderOf(orders) shouldBe None
    holderOfBackfill(orders, "overlap") shouldBe None
    assertLedger(orders)
  }

  // -----------------------------------------------------------------------------------------------
  // A chunk that a run left
  // -----------------------------------------------------------------------------------------------

  test("a chunk that a run left is run again, or rolled forward, and the last completes it") {
    val orders = changes()
    stamped(orders, 1 -> day(1).plusSeconds(60), 2 -> day(2).plusSeconds(60))

    // It dies with the first chunk planned: the backfill is created, and the chunk is not read.
    intercept[Died](
      backfill(
        orders,
        "h",
        day(1),
        day(3),
        by = reading(hookAt(FaultPoint.AfterPlan) { _ =>
          throw new Died
        })
      )
    )
    val first = holderOfBackfill(orders, "h").value
    recordOf(orders, "h").value.createdBy shouldBe first
    expireBackfill(orders, "h")
    val replayed = ingested(backfill(orders, "h", day(1), day(3)))
    replayed.batch shouldBe chunkOf(orders, "h", 1)
    batchOf(replayed.batch).plannedBy shouldBe first
    runOf(first).status shouldBe RunStatus.Abandoned
    // The range of a backfill is the same whoever planned the chunk: there is more of it.
    replayed.next shouldBe Outcome.Next.Within(WatermarkHorizon(day(3)).toJson)

    // It dies with the last chunk written: the next run commits it, and the backfill is done.
    intercept[Died](
      backfill(
        orders,
        "h",
        day(1),
        day(3),
        by = reading(hookAt(FaultPoint.AfterBronzeCommit) { _ =>
          throw new Died
        })
      )
    )
    val second = holderOfBackfill(orders, "h").value
    recordOf(orders, "h").value.completedAt shouldBe None
    expireBackfill(orders, "h")
    val rolled = backfill(orders, "h", day(1), day(3))
    rolled shouldBe a[Outcome.RolledForward]
    recordOf(orders, "h").value.progress.map(p => (p.lastCommitted.value, p.updatedBy)) shouldBe
      Some((2L, rolled.run))
    recordOf(orders, "h").value.completedAt should not be empty
    copiesOf(orders) shouldBe Seq((1, 1L, replayed.run.toString), (2, 2L, second.toString))
    stateOf(orders) shouldBe None
    assertLedger(orders)
  }

  test("a run of a backfill whose lease is taken commits nothing, and the run that took it does") {
    val orders = changes()
    stamped(orders, 1 -> day(1).plusSeconds(60), 2 -> day(2).plusSeconds(60))

    // Its rows are in the table when its lease is taken: the run that takes it rolls the chunk
    // forward, and this one finds its commit refused.
    var successor = Option.empty[Outcome]
    val zombie = reading(hookAt(FaultPoint.AfterBronzeCommit) { _ =>
      expireBackfill(orders, "h")
      successor = Some(backfill(orders, "h", day(1), day(3)))
    })

    val outcome = backfill(orders, "h", day(1), day(3), by = zombie)

    successor.value shouldBe a[Outcome.RolledForward]
    outcome match {
      case Outcome.Failed(run, FailureClass.Transient, message) =>
        message should include(s"lost the lease of ${backfillLease(orders, "h")}")
        runOf(run).status shouldBe RunStatus.Abandoned
      case other => fail(s"the run did not lose its lease: $other")
    }
    recordOf(orders, "h").value.progress.map(p => (p.lastCommitted.value, p.updatedBy)) shouldBe
      Some((1L, successor.value.run))

    ingested(backfill(orders, "h", day(1), day(3))).next shouldBe Outcome.Next.Ends
    copiesOf(orders).map(row => (row._1, row._2)) shouldBe Seq((1, 1L), (2, 2L))
    assertLedger(orders)
  }

  // -----------------------------------------------------------------------------------------------
  // The quarantine of a source with a backfill
  // -----------------------------------------------------------------------------------------------

  test("a record of the quarantine counts by its run, in the sequence that the run acted on") {
    // Each batch sets one record apart, which its source lets it: one of two is not beyond half.
    val orders = changes().copy(quality = Quality(0.5))
    stamped(orders, 1 -> day(1).plusSeconds(60), 2 -> day(2).plusSeconds(60), 3 -> day(5))
    val connector = new Rejecting(new JdbcSourceConnector(SecretResolver(passwords.get)))
    def by(hooks: FaultHooks = FaultHooks.Off): IngestionService =
      new IngestionService(
        database,
        ConnectorRegistry(connector),
        spark,
        lakeRoot,
        timing,
        retries,
        Clock.fixed(Now, ZoneOffset.UTC),
        hooks
      )
    val dying = by(hookAt(FaultPoint.AfterQuarantineWrite)(_ => throw new Died))
    def regular(service: IngestionService = by()): Outcome = service.run(orders, versionOf(orders))
    def chunk(service: IngestionService = by()): Outcome =
      backfill(orders, "h", day(1), day(3), by = service)

    /** A run that finds that what the plan it took names changed, and gives the plan up. */
    def changed(run: => Outcome): RunId = {
      connector.changed = true
      try
        run match {
          case givenUp: Outcome.GivenUp => givenUp.run
          case other => fail(s"the batch was not given up: $other")
        }
      finally connector.changed = false
    }

    // The batch 1 of the regular sequence sets its record apart, and its run dies before its
    // data is written: the run after it gives the batch up.
    intercept[Died](regular(dying))
    val dead = holderOf(orders).value
    expire(orders)
    val regularGivenUp = changed(regular())
    // The chunk 1 of a backfill has the number of that batch, and is committed.
    val first = ingested(chunk())
    // Its chunk 2 is given up as the batch 1 was, and the batch 2 of the regular sequence, which
    // has its number, is committed. The chunk 3 reads the window of the chunk 2 again.
    intercept[Died](chunk(dying))
    val deadChunk = holderOfBackfill(orders, "h").value
    expireBackfill(orders, "h")
    val chunkGivenUp = changed(chunk())
    val second = ingested(regular())
    val third = ingested(chunk())
    third.next shouldBe Outcome.Next.Ends

    val app = TxnAppId.of(orders.id, Layer.Bronze).value
    val history = TxnAppId.backfill(orders.id, Layer.Bronze, backfillId("h")).value
    val batches = sequencesOf(orders).map(batch => (batch._1, batch._2, batch._3))
    batches should contain theSameElementsAs Seq(
      (app, 1L, "ABANDONED"),
      (app, 2L, "COMMITTED"),
      (history, 1L, "COMMITTED"),
      (history, 2L, "ABANDONED"),
      (history, 3L, "COMMITTED")
    )

    // The quarantine has every record that a run set apart, under the number of its batch in the
    // sequence of the run: two of each number but the last.
    val reader = new QuarantineReader(database, spark, lakeRoot)
    def recordsIn(records: org.apache.spark.sql.DataFrame): Seq[(String, Long, String)] =
      records
        .select(Quarantine.RunId, Quarantine.BatchSeq, Quarantine.RawRecord)
        .collect()
        .map(row => (row.getString(0), row.getLong(1), row.getString(2)))
        .toSeq
        .sorted
    def record(run: RunId, batch: BatchId): (String, Long, String) =
      (run.toString, batch.seq.value, Rejecting.recordOf(batchOf(batch).plan))
    val regularFirst = batchId(orders, 1)
    val chunkSecond = chunkOf(orders, "h", 2)
    recordsIn(spark.read.format("delta").load(quarantine(orders).uri)) shouldBe Seq(
      record(dead, regularFirst),
      record(first.run, first.batch),
      record(deadChunk, chunkSecond),
      record(second.run, second.batch),
      record(third.run, third.batch)
    ).sorted

    // Read as it counts, it has the records of the batches that were committed, in either
    // sequence: by the number alone, the chunk 1 would be left out with the batch 1, and the
    // chunk 2 kept with the batch 2.
    reader.leftOut(orders.id) should contain theSameElementsAs
      Seq(dead, regularGivenUp, deadChunk, chunkGivenUp)
    recordsIn(reader.records(orders.id).value) shouldBe Seq(
      record(first.run, first.batch),
      record(second.run, second.batch),
      record(third.run, third.batch)
    ).sorted
    assertLedger(orders)
  }
}

object BackfillSpec {

  /**
   * The JDBC connector, with the reads of a source of text: one record of each batch could not
   * be parsed, and is set apart, as the record that `recordOf` says of its plan. While `changed`
   * is set, it says that what a plan names changed, as the connector of files says of a file
   * that was written again (ADR-0018): the batch is given up, if none of it is in its table.
   *
   * It plans, and plans the chunks of a backfill, as the JDBC connector does: the ledger, which
   * asks the JDBC connector what a plan commits, is told the same.
   */
  final class Rejecting(jdbc: JdbcSourceConnector) extends SourceConnector[JdbcConnectorConfig] {

    @volatile var changed: Boolean = false

    override def kind: ConnectorKind = jdbc.kind

    override def plan(
        source: Source,
        config: JdbcConnectorConfig,
        context: PlanContext
    ): Option[Json] = jdbc.plan(source, config, context)

    override def read(
        source: Source,
        config: JdbcConnectorConfig,
        plan: Json,
        context: ReadContext
    ): ReadResult = {
      if (changed)
        throw new SourceChangedException(
          "the window of the plan was written again",
          Seq(SourceChangedException.Changed("the window", "was written again"))
        )
      val spark = context.spark
      import spark.implicits._
      val rows = jdbc.read(source, config, plan, context).rows
      ReadResult.Kept(
        spark.createDataFrame(rows.collectAsList(), rows.schema),
        Some(
          Seq((Option.empty[String], Rejecting.recordOf(plan)))
            .toDF(EngineColumns.SourceFile, ReadResult.Record)
        )
      )
    }

    override def progress(source: Source, config: JdbcConnectorConfig, plan: Json): Progress =
      jdbc.progress(source, config, plan)

    override def more(source: Source, config: JdbcConnectorConfig, plan: Json): Option[Json] =
      jdbc.more(source, config, plan)

    override def wholeWhenRead(source: Source, config: JdbcConnectorConfig, plan: Json): Boolean =
      jdbc.wholeWhenRead(source, config, plan)

    override def failure(
        source: Source,
        config: JdbcConnectorConfig,
        error: Throwable
    ): Option[RunFailedException] = jdbc.failure(source, config, error)

    override def ranges(source: Source, config: JdbcConnectorConfig): Option[RangePlanner] =
      jdbc.ranges(source, config)
  }

  object Rejecting {

    /** The record that a batch sets apart: one that names where the window of its plan ends. */
    def recordOf(plan: Json): String =
      WatermarkPlan
        .fromJson(plan)
        .fold(
          reason => throw new IllegalStateException(s"the plan is not a window: $reason"),
          window => s"a record up to ${window.upper} that is no row"
        )
  }
}
