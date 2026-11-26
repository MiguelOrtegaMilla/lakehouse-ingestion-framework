package lakehouse.ingestion.it.ingestion

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import java.time.{Clock, Instant, ZoneOffset}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Using

import io.circe.Json
import io.delta.tables.DeltaTable
import lakehouse.ingestion.core.config.Source
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.core.files.FilePlan
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.bronze.{BronzeBatch, BronzeWriter}
import lakehouse.ingestion.engine.connector.ConnectorRegistry
import lakehouse.ingestion.engine.control._
import lakehouse.ingestion.engine.delta.{BatchAppend, CommitMetadata, Lake}
import lakehouse.ingestion.engine.files.{FileSourceConnector, Landing}
import lakehouse.ingestion.engine.quarantine.QuarantineReader
import lakehouse.ingestion.engine.{FaultHooks, FaultPoint, IngestionService}
import lakehouse.ingestion.it.connector.AConnectorOfItsOwnSpec.CountingConnector
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.it.support.{CapturedLogs, CoordinationSchema, Died}
import lakehouse.ingestion.logstore.coordination.{
  CoordinationSettings,
  PostgresCoordinationStore,
  WriterEpoch
}
import lakehouse.ingestion.logstore.{CommitContext, WriterEpochs}
import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * A run of a source from end to end (P5.8): the protocol of ADR-0005 on real files, a real
 * Delta on the coordinated LogStore, and a real control plane, with one PostgreSQL for both
 * schemas, as a deployment has it.
 *
 * What goes wrong is made to go wrong for real: a file is written again on disk, or removed,
 * at a fault point of the engine; a batch is written to its table by another writer; a lease is
 * taken over in the database; a run dies at a fault point, and the run after it finishes its
 * batch (P5.10); a run that lost its lease goes on, and the LogStore refuses its commits
 * (ADR-0019).
 *
 * The clock of the service is the test's own, and so is the time of each file: whether a file
 * is complete does not depend on how long a test takes.
 */
final class IngestionServiceSpec extends AnyFunSuite with Matchers with IngestionSuite {

  // ---------------------------------------------------------------------------------------------
  // The path where nothing fails
  // ---------------------------------------------------------------------------------------------

  test("a run takes the new files of a source to Bronze, and the control plane says all of it") {
    val orders = fileSource()
    val a = drop(orders, "a.csv", "id,amount\n1,10.50\n2,7\n")
    val b = drop(orders, "b.csv", "id,amount\n3,1\n", at = anHourAgo.plusSeconds(60))

    val done = ingested(launch(orders))

    val first = batchId(orders, 1)
    done.batch shouldBe first
    done.outputs shouldBe Seq(BatchOutput(orders.targets.bronze.table, 0))
    done.counts shouldBe RunCounts(
      rowsRead = Some(3),
      rowsWritten = Some(3),
      rowsRejected = Some(0),
      bytesRead = Some(Files.size(a) + Files.size(b)),
      retries = None,
      writeConflicts = Some(0),
      commitContention = Some(0)
    )

    // The lake: the rows as the source wrote them, each with where it came from.
    val written = spark.read.format("delta").load(bronze(orders).uri)
    written.columns.toSeq shouldBe Seq(
      "id",
      "amount",
      "_source_file",
      "_rescued_data",
      "_run_id",
      "_batch_seq",
      "_ingested_at",
      "_source_id"
    )
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"), ("2", 1L, "a.csv"), ("3", 1L, "b.csv"))
    written
      .select("_run_id", "_ingested_at", "_source_id")
      .distinct()
      .collect()
      .map(row => (row.getString(0), row.getTimestamp(1), row.getString(2)))
      .toSeq shouldBe Seq((done.run.toString, Timestamp.from(Now), orders.id.value))
    // The table knows the batch by its number, at the version that the batch records.
    delta.lastVersion(bronze(orders), first.app) shouldBe Some(1L)
    delta.committedAt(bronze(orders), first) shouldBe Some(0L)
    DeltaTable
      .forPath(spark, bronze(orders).uri)
      .history(1)
      .select("userMetadata")
      .head()
      .getString(0) shouldBe CommitMetadata(done.run, orders.id, first).text

    // The control plane: the batch, its files, the state of the source, the run, the lease.
    val committed = batchOf(first)
    committed.status shouldBe BatchStatus.Committed
    committed.plannedBy shouldBe done.run
    committed.committedBy shouldBe Some(done.run)
    committed.outputs shouldBe Some(Vector(BatchOutput(orders.targets.bronze.table, 0)))
    FilePlan.fromJson(committed.plan).value.names.map(_.value) shouldBe Vector("a.csv", "b.csv")
    takenOf(orders) shouldBe Seq(("a.csv", Files.size(a), 1L), ("b.csv", Files.size(b), 1L))
    val state = stateOf(orders).value
    state.lastCommitted shouldBe first.seq
    state.watermark shouldBe None
    state.updatedBy shouldBe done.run
    val ended = runOf(done.run)
    ended.status shouldBe RunStatus.Succeeded
    ended.batch shouldBe Some(first.seq)
    ended.counts shouldBe done.counts
    ended.start.layer shouldBe Layer.Bronze
    ended.start.configHash shouldBe versionOf(orders).hash
    holderOf(orders) shouldBe None
    assertLedger(orders)
  }

  test("a second run of the same files writes nothing, and a new file is the next batch") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    drop(orders, "b.csv", "id\n2\n")
    ingested(launch(orders))

    // Nothing new: a run that ends well, with no batch, and a table that is as it was.
    val idle = launch(orders)
    idle shouldBe Outcome.NothingNew(idle.run)
    val again = runOf(idle.run)
    again.status shouldBe RunStatus.Succeeded
    again.batch shouldBe None
    again.counts shouldBe NothingCounted
    batchesOf(orders) shouldBe Seq(1L -> "COMMITTED")
    tableVersion(orders) shouldBe 0L
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"), ("2", 1L, "b.csv"))
    holderOf(orders) shouldBe None

    // A file that is left after it: the next batch, appended to the table.
    drop(orders, "c.csv", "id\n3\n")
    val next = ingested(launch(orders))
    next.batch shouldBe batchId(orders, 2)
    next.outputs shouldBe Seq(BatchOutput(orders.targets.bronze.table, 1))
    next.counts.rowsWritten shouldBe Some(1)
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"), ("2", 1L, "b.csv"), ("3", 2L, "c.csv"))
    takenOf(orders).map(file => file._1 -> file._3) shouldBe
      Seq("a.csv" -> 1L, "b.csv" -> 1L, "c.csv" -> 2L)
    stateOf(orders).value.lastCommitted.value shouldBe 2L
    assertLedger(orders)
  }

  test("a batch says what comes after it: the horizon of its chain while there is more") {
    val orders = fileSource(options = ", max_files_per_batch: 1")
    drop(orders, "a.csv", "id\n1\n")
    drop(orders, "b.csv", "id\n2\n")

    val first = ingested(launch(orders))

    val horizon = first.next match {
      case Outcome.Next.Within(next) => next
      case other => fail(s"the first batch of two files, one a batch, says $other")
    }
    // The horizon is the plan's, as the write-ahead log has it.
    horizon shouldBe FilePlan.fromJson(batchOf(first.batch).plan).value.next.value.toJson

    // A file left meanwhile, after the horizon: not the chain's.
    drop(orders, "c.csv", "id\n3\n", at = Now.minusSeconds(30))
    val second = ingested(launch(orders, horizon = Some(horizon)))

    second.next shouldBe Outcome.Next.Ends
    rowsOf(orders).map(_._3) shouldBe Seq("a.csv", "b.csv")
    assertLedger(orders)
  }

  test("a batch that a run before it planned is followed by a plan made anew") {
    val orders = fileSource(options = ", max_files_per_batch: 1")
    drop(orders, "a.csv", "id\n1\n")
    drop(orders, "b.csv", "id\n2\n")
    intercept[Died](
      launch(orders, service(hooks = hookAt(FaultPoint.AfterPlan)(_ => throw new Died)))
    )
    expire(orders)

    // The batch of the run that died is run again: its horizon was another invocation's.
    ingested(launch(orders)).next shouldBe Outcome.Next.Afresh
    assertLedger(orders)
  }

  test("a run with no new file ends well with no batch, and makes nothing") {
    val orders = fileSource()

    val idle = launch(orders)

    idle shouldBe Outcome.NothingNew(idle.run)
    runOf(idle.run).status shouldBe RunStatus.Succeeded
    runOf(idle.run).counts shouldBe NothingCounted
    batchesOf(orders) shouldBe empty
    stateOf(orders) shouldBe None
    tableIsThere(orders) shouldBe false
    holderOf(orders) shouldBe None
    assertLedger(orders)
  }

  test("a file is not planned until it has gone unchanged for the time that its source says") {
    // Half a minute old: it may still be being written (`connector.min_file_age`, a minute).
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n", at = Now.minusSeconds(30))
    val early = launch(orders)
    early shouldBe Outcome.NothingNew(early.run)
    batchesOf(orders) shouldBe empty

    ingested(launch(orders, service(at = Now.plusSeconds(30)))).counts.rowsWritten shouldBe Some(1)

    // A source whose producer is slower says so, and waits longer.
    val slow = fileSource(options = ", min_file_age: 10m")
    drop(slow, "a.csv", "id\n1\n", at = Now.minusSeconds(120))
    val waiting = launch(slow, service(at = Now.plusSeconds(7 * 60)))
    waiting shouldBe Outcome.NothingNew(waiting.run)
    ingested(launch(slow, service(at = Now.plusSeconds(8 * 60)))).counts.rowsWritten shouldBe Some(
      1
    )
    assertLedger(orders)
    assertLedger(slow)
  }

  test("while another run holds the lease of the source, a run is skipped and does nothing") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    val holder = LeasedRun.open(database, starting(orders), versionOf(orders), timing).value

    val skipped = launch(orders)

    skipped match {
      case Outcome.Skipped(_, held, _) => held shouldBe holder.start.id
      case other => fail(s"the run was not skipped: $other")
    }
    val recorded = runOf(skipped.run)
    recorded.status shouldBe RunStatus.SkippedLocked
    recorded.epoch shouldBe None
    recorded.batch shouldBe None
    batchesOf(orders) shouldBe empty
    tableIsThere(orders) shouldBe false
    holderOf(orders) shouldBe Some(holder.start.id)

    // The lease given up, the next run does the work.
    holder.finish(RunEnd.Failed(FailureClass.Transient, "stopped by the test")) shouldBe true
    ingested(launch(orders)).batch shouldBe batchId(orders, 1)
    assertLedger(orders)
  }

  test("a source of another kind is run the same way, from where its last batch left it") {
    // The engine names no kind (RNF-04): this one counts on from its watermark, which the
    // commit of each batch records and the plan of the next is given.
    val counting = new CountingConnector
    val id = newSourceId().value
    val numbers = Source
      .load(
        s"$id.yaml",
        s"""id: $id
           |connector: { type: counting, per_batch: 3 }
           |load: { mode: full }
           |targets: { bronze: { table: bronze/${id.replace('.', '/')} } }
           |""".stripMargin,
        ConnectorRegistry(counting).kinds
      )
      .value
      .value
    val counts = new IngestionService(
      database,
      ConnectorRegistry(counting),
      spark,
      lakeRoot,
      timing,
      retries,
      Clock.fixed(Now, ZoneOffset.UTC)
    )
    def ids: Seq[Long] =
      spark.read.format("delta").load(bronze(numbers).uri).collect().map(_.getLong(0)).sorted.toSeq

    val first = ingested(counts.run(numbers, versionOf(numbers)))
    ids shouldBe Seq(0L, 1L, 2L)
    stateOf(numbers).value.watermark shouldBe Some(Json.fromLong(3))
    // No files, and no size that its plan says: neither is counted.
    first.counts.bytesRead shouldBe None
    takenOf(numbers) shouldBe empty

    ingested(counts.run(numbers, versionOf(numbers))).batch shouldBe batchId(numbers, 2)
    ids shouldBe Seq(0L, 1L, 2L, 3L, 4L, 5L)
    stateOf(numbers).value.watermark shouldBe Some(Json.fromLong(6))
  }

  // ---------------------------------------------------------------------------------------------
  // The fault points of the engine
  // ---------------------------------------------------------------------------------------------

  test("a run reaches the fault points in the order of the protocol, each after its step") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    val first = batchId(orders, 1)
    val seen = mutable.Buffer.empty[(FaultPoint, String, Option[Long], Int)]
    val recording: FaultHooks = (point, batch) => {
      batch.id shouldBe first
      // How the batch stands, what its table has of it, and how many of its files are taken.
      seen += ((
        point,
        batchOf(batch.id).status.name,
        delta.lastVersion(bronze(orders), batch.id.app),
        takenOf(orders).size
      ))
      ()
    }

    ingested(launch(orders, service(recording)))

    seen.toSeq shouldBe Seq(
      (FaultPoint.AfterPlan, "PLANNED", None, 0),
      (FaultPoint.AfterRead, "PLANNED", None, 0),
      (FaultPoint.AfterCounts, "PLANNED", None, 0),
      (FaultPoint.AfterBronzeCommit, "PLANNED", Some(1L), 0),
      (FaultPoint.BeforeStateCommit, "PLANNED", Some(1L), 0)
    )
    // Every point but the one after the quarantine, which a batch with nothing rejected has not.
    seen.map(_._1).toSeq shouldBe FaultPoint.All.filterNot(_ == FaultPoint.AfterQuarantineWrite)
    isThere(quarantine(orders)) shouldBe false
    batchOf(first).status shouldBe BatchStatus.Committed
    takenOf(orders) should have size 1
    assertLedger(orders)
  }

  test("a run that stops at a fault point is left as a process that died leaves it") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    val first = batchId(orders, 1)
    val dying = service(hookAt(FaultPoint.BeforeStateCommit)(_ => throw new Died))

    val stopped = intercept[Died](launch(orders, dying))

    stopped.getMessage shouldBe "the run died here"
    // The rows are in the table, and the control plane was told nothing: not that the run
    // ended, nor that the batch was written. The lease is held until it runs out.
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"))
    delta.lastVersion(bronze(orders), first.app) shouldBe Some(1L)
    val left = batchOf(first)
    left.status shouldBe BatchStatus.Planned
    takenOf(orders) shouldBe empty
    stateOf(orders) shouldBe None
    runOf(left.plannedBy).status shouldBe RunStatus.Running
    holderOf(orders) shouldBe Some(left.plannedBy)

    // While the lease is held, the source is its holder's: a run is skipped, and touches nothing.
    launch(orders) shouldBe a[Outcome.Skipped]
    batchOf(first).status shouldBe BatchStatus.Planned

    // The run that takes the lease over says what the dead one could not, of itself and of
    // its batch.
    expire(orders)
    val next = launch(orders)
    next shouldBe a[Outcome.RolledForward]
    runOf(left.plannedBy).status shouldBe RunStatus.Abandoned
    runOf(next.run).status shouldBe RunStatus.Succeeded
    batchOf(first).status shouldBe BatchStatus.Committed
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"))
    holderOf(orders) shouldBe None
    assertLedger(orders)
  }

  // ---------------------------------------------------------------------------------------------
  // A batch that a run left planned is finished by the next, as its table says (P5.10)
  // ---------------------------------------------------------------------------------------------

  /** A run of the source that dies at `point`, and whose lease then runs out. */
  private def dies(of: Source, at: FaultPoint): RunId = {
    intercept[Died](launch(of, service(hookAt(at)(_ => throw new Died))))
    val dead = holderOf(of).value
    expire(of)
    dead
  }

  test("a batch whose data is written is rolled forward: nothing is read, nothing written again") {
    val orders = fileSource()
    val file = drop(orders, "a.csv", "id\n1\n2\n")
    val size = Files.size(file)
    val first = batchId(orders, 1)
    val dead = dies(orders, FaultPoint.AfterBronzeCommit)
    batchOf(first).target shouldBe Some(bronze(orders).uri)
    // The file of the plan is gone by now: what a roll-forward records is said by the plan.
    Files.delete(file)

    val outcome = launch(orders)

    outcome shouldBe Outcome.RolledForward(
      outcome.run,
      first,
      Seq(BatchOutput(orders.targets.bronze.table, 0)),
      // It read nothing, which it knows; what the batch wrote was the dead run's to count.
      RunCounts(rowsRead = Some(0), bytesRead = Some(0))
    )
    // The table is as the dead run left it: the same rows, written by that run, at one version.
    tableVersion(orders) shouldBe 0L
    spark.read
      .format("delta")
      .load(bronze(orders).uri)
      .select("id", "_run_id", "_batch_seq")
      .collect()
      .map(row => (row.getString(0), row.getString(1), row.getLong(2)))
      .sorted
      .toSeq shouldBe Seq(("1", dead.toString, 1L), ("2", dead.toString, 1L))

    // And the control plane says what the dead run could not.
    val committed = batchOf(first)
    committed.status shouldBe BatchStatus.Committed
    committed.plannedBy shouldBe dead
    committed.committedBy shouldBe Some(outcome.run)
    committed.outputs shouldBe Some(Vector(BatchOutput(orders.targets.bronze.table, 0)))
    takenOf(orders) shouldBe Seq(("a.csv", size, 1L))
    val state = stateOf(orders).value
    state.lastCommitted shouldBe first.seq
    state.updatedBy shouldBe outcome.run
    runOf(dead).status shouldBe RunStatus.Abandoned
    val ended = runOf(outcome.run)
    ended.status shouldBe RunStatus.Succeeded
    ended.batch shouldBe Some(first.seq)
    ended.counts shouldBe RunCounts(rowsRead = Some(0), bytesRead = Some(0))
    holderOf(orders) shouldBe None

    // The source goes on from there: nothing new now, and a new file is the next batch.
    launch(orders) shouldBe a[Outcome.NothingNew]
    drop(orders, "b.csv", "id\n3\n")
    ingested(launch(orders)).batch shouldBe batchId(orders, 2)
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"), ("2", 1L, "a.csv"), ("3", 2L, "b.csv"))
    assertLedger(orders)
  }

  test("a batch whose data is not written is run again as it was planned, under its number") {
    Seq(FaultPoint.AfterPlan, FaultPoint.AfterRead, FaultPoint.AfterCounts).foreach { point =>
      withClue(s"a run that died at $point: ") {
        val orders = fileSource()
        val a = drop(orders, "a.csv", "id\n1\n2\n")
        val first = batchId(orders, 1)
        val dead = dies(orders, point)
        tableIsThere(orders) shouldBe false
        // A file that came since is not of the plan: it is the next batch's.
        drop(orders, "b.csv", "id\n3\n")

        val done = ingested(launch(orders))

        done.batch shouldBe first
        done.outputs shouldBe Seq(BatchOutput(orders.targets.bronze.table, 0))
        done.counts shouldBe RunCounts(
          rowsRead = Some(2),
          rowsWritten = Some(2),
          rowsRejected = Some(0),
          bytesRead = Some(Files.size(a)),
          writeConflicts = Some(0),
          commitContention = Some(0)
        )
        // The rows are those of the plan, written by the run that ran it again.
        spark.read
          .format("delta")
          .load(bronze(orders).uri)
          .select("id", "_run_id", "_batch_seq")
          .collect()
          .map(row => (row.getString(0), row.getString(1), row.getLong(2)))
          .sorted
          .toSeq shouldBe Seq(("1", done.run.toString, 1L), ("2", done.run.toString, 1L))
        val committed = batchOf(first)
        committed.status shouldBe BatchStatus.Committed
        committed.plannedBy shouldBe dead
        committed.committedBy shouldBe Some(done.run)
        takenOf(orders) shouldBe Seq(("a.csv", Files.size(a), 1L))
        runOf(dead).status shouldBe RunStatus.Abandoned
        runOf(done.run).batch shouldBe Some(first.seq)
        batchesOf(orders) shouldBe Seq((1L, "COMMITTED"))

        ingested(launch(orders)).batch shouldBe batchId(orders, 2)
        rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"), ("2", 1L, "a.csv"), ("3", 2L, "b.csv"))
        assertLedger(orders)
      }
    }
  }

  test("a batch whose quarantine is written and whose data is not: the rest of it is written") {
    val orders = fileSource(rejected = Some("0.5"))
    val a = drop(orders, "a.csv", "id,amount\n1,2\n3\n")
    val first = batchId(orders, 1)
    val dead = dies(orders, FaultPoint.AfterQuarantineWrite)
    quarantinedOf(orders) shouldBe Seq((1L, "a.csv", "3"))
    tableIsThere(orders) shouldBe false

    val done = ingested(launch(orders))

    // The data table says "run it again", and the quarantine, which has the batch, takes its
    // records once: Delta skips them, and the batch records where they are.
    done.batch shouldBe first
    done.outputs shouldBe Seq(
      BatchOutput(quarantineTable(orders), 0),
      BatchOutput(orders.targets.bronze.table, 0)
    )
    quarantinedOf(orders) shouldBe Seq((1L, "a.csv", "3"))
    DeltaLog.forTable(spark, quarantine(orders).uri).update().version shouldBe 0L
    spark.read
      .format("delta")
      .load(quarantine(orders).uri)
      .select("run_id")
      .collect()
      .map(_.getString(0))
      .toSeq shouldBe Seq(dead.toString)
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"))
    done.counts shouldBe RunCounts(
      rowsRead = Some(2),
      rowsWritten = Some(1),
      rowsRejected = Some(1),
      bytesRead = Some(Files.size(a)),
      writeConflicts = Some(0),
      commitContention = Some(0)
    )
    batchOf(first).outputs shouldBe Some(done.outputs.toVector)
    assertLedger(orders)
  }

  test("a batch that is rolled forward records its quarantine too, each table at its version") {
    val orders = fileSource(rejected = Some("0.5"))
    drop(orders, "a.csv", "id,amount\n1,2\n3\n")
    ingested(launch(orders)).batch shouldBe batchId(orders, 1)
    drop(orders, "b.csv", "id,amount\n4,5\n6\n")
    val second = batchId(orders, 2)
    dies(orders, FaultPoint.BeforeStateCommit)

    val outcome = launch(orders)

    val outputs = Seq(
      BatchOutput(quarantineTable(orders), 1),
      BatchOutput(orders.targets.bronze.table, 1)
    )
    outcome shouldBe Outcome.RolledForward(
      outcome.run,
      second,
      outputs,
      RunCounts(rowsRead = Some(0), bytesRead = Some(0))
    )
    batchOf(second).outputs shouldBe Some(outputs.toVector)
    quarantinedOf(orders) shouldBe Seq((1L, "a.csv", "3"), (2L, "b.csv", "6"))
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"), ("4", 2L, "b.csv"))
    assertLedger(orders)
  }

  test("a batch with no rows that a run left is run again, to the same nothing") {
    val orders = fileSource()
    val blank = drop(orders, "empty.csv", "")
    val first = batchId(orders, 1)
    dies(orders, FaultPoint.BeforeStateCommit)
    // It is in no table, so no table says that it is written.
    tableIsThere(orders) shouldBe false

    val done = ingested(launch(orders))

    done.batch shouldBe first
    done.outputs shouldBe empty
    tableIsThere(orders) shouldBe false
    takenOf(orders) shouldBe Seq(("empty.csv", Files.size(blank), 1L))
    batchOf(first).status shouldBe BatchStatus.Committed
    assertLedger(orders)
  }

  test("a file that changed since a run left its batch gives the batch up, and the next plans") {
    val orders = fileSource()
    val a = drop(orders, "a.csv", "id\n1\n")
    drop(orders, "b.csv", "id\n2\n")
    val first = batchId(orders, 1)
    val dead = dies(orders, FaultPoint.AfterRead)
    // The producer writes one of the files again, after the run that planned them died.
    drop(orders, "a.csv", "id\n10\n11\n", at = Now.minusSeconds(1800))

    val outcome = launch(orders)

    outcome match {
      case Outcome.GivenUp(_, batch, why) =>
        batch shouldBe first
        why should include("'a.csv' is of 9 bytes, and was of 5")
      case other => fail(s"the batch was not given up: $other")
    }
    val abandoned = batchOf(first)
    abandoned.status shouldBe BatchStatus.Abandoned
    abandoned.plannedBy shouldBe dead
    abandoned.abandoned.value.by shouldBe Some(outcome.run)
    tableIsThere(orders) shouldBe false
    takenOf(orders) shouldBe empty
    holderOf(orders) shouldBe None

    // The next run plans anew: the file that changed as a new file, and the other again.
    val next = ingested(launch(orders))
    next.batch shouldBe batchId(orders, 2)
    rowsOf(orders) shouldBe Seq(("10", 2L, "a.csv"), ("11", 2L, "a.csv"), ("2", 2L, "b.csv"))
    takenOf(orders) shouldBe Seq(("a.csv", Files.size(a), 2L), ("b.csv", 5L, 2L))
    assertLedger(orders)
  }

  test("a batch is finished in the table that it was planned for, and in no other") {
    val id = newSourceId().value
    val orders = fileSource(id = id)
    drop(orders, "a.csv", "id\n1\n")
    val first = batchId(orders, 1)
    dies(orders, FaultPoint.AfterBronzeCommit)
    // The source is given another table before the run that finishes the batch.
    val moved = fileSource(id = id, table = Some(s"bronze/moved/${id.replace('.', '/')}"))

    val outcome = launch(moved)

    outcome match {
      case Outcome.Failed(_, failure, message) =>
        failure shouldBe FailureClass.Permanent
        message should include(s"the batch $first was planned by the run")
        message should include(s"to be written to ${bronze(orders).uri}")
        message should include(s"has its table at ${bronze(moved).uri} now")
        message should include("Nothing was read or written")
      case other => fail(s"the run did not fail: $other")
    }
    // The other table would have said that the batch is not written, and been given its rows:
    // it is not made, the batch stays planned, and nothing is accounted for.
    isThere(bronze(moved)) shouldBe false
    batchOf(first).status shouldBe BatchStatus.Planned
    takenOf(orders) shouldBe empty
    stateOf(orders) shouldBe None
    runOf(outcome.run).status shouldBe RunStatus.Failed
    holderOf(orders) shouldBe None

    // With the table that it had, the batch is found written, and rolled forward.
    launch(orders) shouldBe a[Outcome.RolledForward]
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"))
    // From then on the source has a state, which says what was written to that table: another
    // table is a change that it cannot follow, and a run with it is refused before it plans
    // (ADR-0007, `BreakingChangeSpec`).
    drop(orders, "b.csv", "id\n2\n")
    launch(moved) match {
      case Outcome.Failed(_, failure, message) =>
        failure shouldBe FailureClass.Permanent
        message should include("targets.bronze.table of the source was")
      case other => fail(s"the run did not fail: $other")
    }
    isThere(bronze(moved)) shouldBe false
    ingested(launch(orders)).batch shouldBe batchId(orders, 2)
    batchOf(batchId(orders, 2)).target shouldBe Some(bronze(orders).uri)
    assertLedger(orders)
  }

  test("a batch that was planned before a batch said its table is finished where the source says") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    val first = batchId(orders, 1)
    dies(orders, FaultPoint.AfterBronzeCommit)
    // As a batch that was planned under version 3 of the schema is, after the migration.
    asSuperuser { c =>
      update(
        c,
        "UPDATE control.ingestion_batch SET target_table = NULL WHERE txn_app_id = ?",
        first.app.value
      ) shouldBe 1
    }
    batchOf(first).target shouldBe None

    launch(orders) shouldBe a[Outcome.RolledForward]
    batchOf(first).status shouldBe BatchStatus.Committed
    assertLedger(orders)
  }

  test("a batch whose commit the log no longer keeps is rolled forward at a version not known") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    val first = batchId(orders, 1)
    dies(orders, FaultPoint.AfterBronzeCommit)
    // The source is not run for forty days, in which its table is written by others: an
    // operator, here. Delta keeps a commit for thirty, and cleans its log up when it writes a
    // checkpoint.
    val deltaLog = DeltaLog.forTable(spark, bronze(orders).uri)
    deltaLog.startTransaction(None).commit(Nil, DeltaOperations.ManualUpdate) shouldBe 1L
    val commitOfTheBatch =
      Paths.get(URI.create(bronze(orders).uri)).resolve("_delta_log").resolve(f"${0L}%020d.json")
    Files.setLastModifiedTime(
      commitOfTheBatch,
      FileTime.from(Instant.now().minus(40, ChronoUnit.DAYS))
    )
    deltaLog.checkpoint(deltaLog.update())
    Files.exists(commitOfTheBatch) shouldBe false
    // A run that starts remembers nothing of the table: its state comes from the checkpoint.
    DeltaLog.clearCache()

    val outcome = launch(orders)

    // The table knows the batch, since its transactions are in the checkpoint, and no longer
    // at which version: that is said, and no version is made up.
    val unknown = Seq(BatchOutput(orders.targets.bronze.table, None))
    outcome shouldBe Outcome.RolledForward(
      outcome.run,
      first,
      unknown,
      RunCounts(rowsRead = Some(0), bytesRead = Some(0))
    )
    batchOf(first).outputs shouldBe Some(unknown.toVector)
    asSuperuser { c =>
      query(
        c,
        "SELECT outputs::text FROM control.ingestion_batch WHERE txn_app_id = ?",
        first.app.value
      ) shouldBe Seq(
        Seq(s"""[{"table": "${orders.targets.bronze.table.value}", "version": null}]""")
      )
    }
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"))
    takenOf(orders).map(_._1) shouldBe Seq("a.csv")
    assertLedger(orders)
  }

  // ---------------------------------------------------------------------------------------------
  // A run that lost its lease, and goes on (ADR-0019)
  // ---------------------------------------------------------------------------------------------

  /**
   * A run that is taken over between its read and its writes: at that point its lease runs out,
   * `meanwhile` happens, and another run of the source is made, whose outcome is kept. Then the
   * first goes on, with what it read in its hands and a lease that is another's.
   */
  private def takenOver(of: Source, by: => IngestionService = service())(
      meanwhile: => Unit
  ): (IngestionService, () => Outcome) = {
    var successor = Option.empty[Outcome]
    val zombie = service(hookAt(FaultPoint.AfterRead) { _ =>
      expire(of)
      meanwhile
      successor = Some(launch(of, by))
    })
    (zombie, () => successor.value)
  }

  private def lostItsLease(outcome: Outcome, of: Source): String = outcome match {
    case Outcome.Failed(_, failure, message) =>
      failure shouldBe FailureClass.Transient
      message should include(s"lost the lease of ${LeaseKey.of(of.id, Layer.Bronze)}")
      message
    case other => fail(s"the run did not lose its lease: $other")
  }

  test("a run that lost its lease writes nothing of a batch that was given up meanwhile") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    drop(orders, "b.csv", "id\n2\n")
    val first = batchId(orders, 1)
    // The run has read both files when its lease is taken. One of them is written again, and
    // the run that took the lease gives the batch up: no table has it, and none is there.
    val (zombie, successor) = takenOver(orders) {
      drop(orders, "a.csv", "id\n10\n11\n", at = Now.minusSeconds(1800)): Unit
    }

    val outcome = launch(orders, zombie)

    successor() shouldBe a[Outcome.GivenUp]
    batchOf(first).status shouldBe BatchStatus.Abandoned
    // The first run goes on to write the rows that it read, to a table that is not there yet,
    // and its commit is refused for its epoch: it is told that its lease is lost, and stops.
    val message = lostItsLease(outcome, orders)
    message should include("the writer commits under epoch 1 of")
    message should include("the epoch in force is 2")
    // What it leaves is what any write that fails leaves: files that no commit names. There is
    // no commit, so there is no table, and no batch in it.
    DeltaLog.forTable(spark, bronze(orders).uri).update().version shouldBe -1L
    delta.lastVersion(bronze(orders), first.app) shouldBe None
    runOf(outcome.run).status shouldBe RunStatus.Abandoned
    takenOf(orders) shouldBe empty

    // So the next batch has the rows of the files once: those that did not change are not in
    // Bronze twice, under the number of a batch that was given up and under its own.
    val next = ingested(launch(orders))
    next.batch shouldBe batchId(orders, 2)
    rowsOf(orders) shouldBe Seq(("10", 2L, "a.csv"), ("11", 2L, "a.csv"), ("2", 2L, "b.csv"))
    tableVersion(orders) shouldBe 0L
    assertLedger(orders)
  }

  test("a run that lost its lease adds nothing to a table that is there, nor to its quarantine") {
    val orders = fileSource(rejected = Some("0.5"))
    drop(orders, "a.csv", "id,amount\n1,2\n3\n")
    ingested(launch(orders)).batch shouldBe batchId(orders, 1)
    drop(orders, "b.csv", "id,amount\n4,5\n6\n")
    val second = batchId(orders, 2)
    val (zombie, successor) = takenOver(orders) {
      drop(orders, "b.csv", "id,amount\n7,8\n9,9\n0\n", at = Now.minusSeconds(1800)): Unit
    }

    val outcome = launch(orders, zombie)

    successor() shouldBe a[Outcome.GivenUp]
    lostItsLease(outcome, orders)
    // The first of its writes is the quarantine's: neither table has anything of the batch.
    batchOf(second).status shouldBe BatchStatus.Abandoned
    quarantinedOf(orders) shouldBe Seq((1L, "a.csv", "3"))
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"))
    delta.lastVersion(bronze(orders), second.app) shouldBe Some(1L)
    delta.lastVersion(quarantine(orders), second.app) shouldBe Some(1L)

    ingested(launch(orders)).batch shouldBe batchId(orders, 3)
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"), ("7", 3L, "b.csv"), ("9", 3L, "b.csv"))
    quarantinedOf(orders) shouldBe Seq((1L, "a.csv", "3"), (3L, "b.csv", "0"))
    assertLedger(orders)
  }

  test("a run that lost its lease finds its batch written by the run that took over, once") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    val first = batchId(orders, 1)
    // Nothing changes in the source: the run that took the lease runs the plan again.
    val (zombie, successor) = takenOver(orders)(())

    val outcome = launch(orders, zombie)

    val winner = successor() match {
      case done: Outcome.Ingested => done
      case other => fail(s"the run that took over did not finish the batch: $other")
    }
    winner.batch shouldBe first
    // The first run goes on: its table has the batch by now, so it writes nothing, and its
    // commit of the state is refused, since the lease is another's.
    lostItsLease(outcome, orders)
    spark.read
      .format("delta")
      .load(bronze(orders).uri)
      .select("id", "_run_id")
      .collect()
      .map(row => (row.getString(0), row.getString(1)))
      .toSeq shouldBe Seq(("1", winner.run.toString))
    tableVersion(orders) shouldBe 0L
    batchOf(first).committedBy shouldBe Some(winner.run)
    runOf(outcome.run).status shouldBe RunStatus.Abandoned
    assertLedger(orders)
  }

  test("a run that is taken over after its data is written: the other rolls its batch forward") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    val first = batchId(orders, 1)
    var successor = Option.empty[Outcome]
    // Its rows are in the table when its lease is taken: the run that takes it asks the table,
    // which says so, and commits the batch. This one then finds its commit refused.
    val zombie = service(hookAt(FaultPoint.AfterBronzeCommit) { _ =>
      expire(orders)
      successor = Some(launch(orders))
    })

    val outcome = launch(orders, zombie)

    successor.value shouldBe a[Outcome.RolledForward]
    lostItsLease(outcome, orders)
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"))
    tableVersion(orders) shouldBe 0L
    val committed = batchOf(first)
    committed.plannedBy shouldBe outcome.run
    committed.committedBy shouldBe Some(successor.value.run)
    takenOf(orders).map(_._1) shouldBe Seq("a.csv")
    assertLedger(orders)
  }

  test("a run that cannot announce its epoch reads nothing, and fails in a way that passes") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    // A session whose LogStore has its coordination database where nothing answers: the
    // control plane is there, and the store that takes the versions of the tables is not.
    val cutOff = spark.newSession()
    cutOff.conf.set(CoordinationSettings.JdbcUrl, "jdbc:postgresql://localhost:1/nothing")
    cutOff.conf.set(CoordinationSettings.ConnectTimeoutMs, "1000")
    val unannounced = new IngestionService(
      database,
      ConnectorRegistry(new FileSourceConnector(new Landing(landingRoot))),
      cutOff,
      lakeRoot,
      timing,
      retries,
      Clock.fixed(Now, ZoneOffset.UTC)
    )

    val outcome = launch(orders, unannounced)

    outcome match {
      case Outcome.Failed(_, failure, message) =>
        failure shouldBe FailureClass.Transient
        message should include(
          s"the epoch of the lease of ${LeaseKey.of(orders.id, Layer.Bronze)} could not be announced"
        )
        message should include("nothing was read or written")
      case other => fail(s"the run did not fail: $other")
    }
    // Nothing was planned, and the lease is given up: the next run starts from nothing.
    batchesOf(orders) shouldBe empty
    runOf(outcome.run).status shouldBe RunStatus.Failed
    holderOf(orders) shouldBe None
    ingested(launch(orders)).batch shouldBe batchId(orders, 1)
    assertLedger(orders)
  }

  test("a run told of an epoch that no run of its control plane announced fails for good") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    val key = LeaseKey.of(orders.id, Layer.Bronze)
    // Epochs of this key that another control plane announced, and that nobody forgot when
    // this one was made anew: the lease of the run is at 1, and 7 is in force.
    WriterEpochs.announce(
      spark.sessionState.newHadoopConf(),
      WriterEpoch(key.value, 7L)
    ) shouldBe 7L

    val outcome = launch(orders)

    // Nobody took the lease: running again does not mend it, and the run says what does.
    outcome match {
      case Outcome.Failed(_, failure, message) =>
        failure shouldBe FailureClass.Permanent
        message should include(s"has the epoch 7 in force for $key")
        message should include(
          s"DELETE FROM delta_coordination.writer_epoch WHERE epoch_key = '$key'"
        )
      case other => fail(s"the run did not fail: $other")
    }
    // It is recorded as failed, and its lease is given up: the next run is not kept waiting for
    // it to expire, and is told the same.
    val ended = runOf(outcome.run)
    ended.status shouldBe RunStatus.Failed
    ended.failure.value.failure shouldBe FailureClass.Permanent
    holderOf(orders) shouldBe None
    batchesOf(orders) shouldBe empty
    tableIsThere(orders) shouldBe false
    launch(orders) shouldBe a[Outcome.Failed]
    assertLedger(orders)

    // Once the epochs are forgotten, the source goes on.
    Using.resource(pg.connectAsSuperuser(CoordinationSchema.Database)) { c =>
      update(c, "DELETE FROM delta_coordination.writer_epoch WHERE epoch_key = ?", key.value)
    } shouldBe 1
    ingested(launch(orders)).batch shouldBe batchId(orders, 1)
    assertLedger(orders)
  }

  test("a run whose lease is taken over while it announces its epoch has lost its lease") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    val key = LeaseKey.of(orders.id, Layer.Bronze)
    // The announcement of the run that takes over, in flight: the run's own waits for it.
    val announcer = pg.connect(CoordinationSchema.CommitPathRole, CoordinationSchema.Database)
    try {
      announcer.setAutoCommit(false)
      update(
        announcer,
        "INSERT INTO delta_coordination.writer_epoch (epoch_key, epoch) VALUES (?, 2)",
        key.value
      ) shouldBe 1
      val running = Future(launch(orders))(ExecutionContext.global)
      eventually(announcementsWaiting() == 1)

      // The lease runs out and is taken, in the control plane, as a run that took over takes it.
      expire(orders)
      val taker = LeasedRun.open(database, starting(orders), versionOf(orders), timing).value
      taker.epoch.value shouldBe 2L
      announcer.commit()

      val outcome = Await.result(running, 1.minute)
      lostItsLease(outcome, orders)
      runOf(outcome.run).status shouldBe RunStatus.Abandoned
      holderOf(orders) shouldBe Some(taker.start.id)
      taker.finish(RunEnd.Failed(FailureClass.Transient, "stopped by the test")) shouldBe true
    } finally announcer.close()

    batchesOf(orders) shouldBe empty
    ingested(launch(orders)).batch shouldBe batchId(orders, 1)
    assertLedger(orders)
  }

  /** How many announcements of the LogStore are waiting for a lock, seen by the superuser. */
  private def announcementsWaiting(): Int =
    Using.resource(pg.connectAsSuperuser(CoordinationSchema.Database)) { c =>
      query(
        c,
        "SELECT count(*)::text FROM pg_stat_activity " +
          "WHERE application_name = ? AND wait_event_type = 'Lock'",
        PostgresCoordinationStore.ApplicationName
      ).head.head.toInt
    }

  private def eventually(condition: => Boolean): Unit = {
    val deadline = 30.seconds.fromNow
    while (!condition) {
      if (deadline.isOverdue()) fail("the condition did not become true in time")
      Thread.sleep(25)
    }
  }

  // ---------------------------------------------------------------------------------------------
  // A source that changes under the plan of a batch (ADR-0018)
  // ---------------------------------------------------------------------------------------------

  test("files that change after they were planned: the batch is given up, with why and which") {
    val orders = fileSource()
    val a = drop(orders, "a.csv", "id\n1\n")
    val b = drop(orders, "b.csv", "id\n2\n")
    drop(orders, "c.csv", "id\n3\n")
    val first = batchId(orders, 1)
    // The producer, between the plan and the read: one file written again, and one taken back.
    val changing = service(hookAt(FaultPoint.AfterPlan) { _ =>
      drop(orders, "a.csv", "id\n10\n11\n", at = Now.minusSeconds(1800))
      Files.delete(b)
    })

    val outcome = launch(orders, changing)

    val reason = outcome match {
      case Outcome.GivenUp(_, batch, why) =>
        batch shouldBe first
        why
      case other => fail(s"the batch was not given up: $other")
    }
    reason should include("2 files of it changed since it was planned")
    reason should include("'a.csv' is of 9 bytes, and was of 5")
    reason should include("'b.csv' is not there any more")

    // The batch says who gave it up, why, and which files; its plan is as it was written.
    val abandoned = batchOf(first)
    abandoned.status shouldBe BatchStatus.Abandoned
    val said = abandoned.abandoned.value
    said.by shouldBe Some(outcome.run)
    said.reason shouldBe reason
    val changed = said.detail.value.hcursor
    changed.get[Int]("total").value shouldBe 2
    changed.downField("changed").downN(0).get[String]("item").value shouldBe "a.csv"
    changed.downField("changed").downN(0).get[String]("change").value should startWith(
      "is of 9 bytes, and was of 5, was changed at 2026-11-19T11:30:00Z"
    )
    changed.downField("changed").downN(1).as[Json].value shouldBe Json.obj(
      "item" -> Json.fromString("b.csv"),
      "change" -> Json.fromString("is not there any more, or is a directory now")
    )
    val planned = FilePlan.fromJson(abandoned.plan).value
    planned.names.map(_.value) shouldBe Vector("a.csv", "b.csv", "c.csv")
    planned.files.head.size shouldBe 5L
    abandoned.committedBy shouldBe None

    // The run failed in a way that passes, and gave its lease up with the batch.
    val ended = runOf(outcome.run)
    ended.status shouldBe RunStatus.Failed
    ended.failure.value.failure shouldBe FailureClass.Transient
    ended.failure.value.message should startWith(
      s"$first was given up, and the next run plans anew: the batch of"
    )
    ended.batch shouldBe Some(first.seq)
    ended.counts shouldBe RunCounts(rowsWritten = Some(0))
    ended.finishedAt shouldBe Some(said.at)
    holderOf(orders) shouldBe None

    // Nothing of the batch is anywhere else.
    tableIsThere(orders) shouldBe false
    takenOf(orders) shouldBe empty
    stateOf(orders) shouldBe None
    Files.size(a) shouldBe 9L
    assertLedger(orders)
  }

  test("the run after a batch that was given up takes the changed file as a new file") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    drop(orders, "b.csv", "id\n2\n")
    val rewriting = service(hookAt(FaultPoint.AfterPlan) { _ =>
      drop(orders, "a.csv", "id\n10\n11\n", at = Now.minusSeconds(1800)): Unit
    })
    launch(orders, rewriting) shouldBe a[Outcome.GivenUp]

    // No operator, and no command: the next run of the source plans anew, by itself.
    val next = ingested(launch(orders))

    val second = batchId(orders, 2)
    next.batch shouldBe second
    next.outputs shouldBe Seq(BatchOutput(orders.targets.bronze.table, 0))
    next.counts.rowsWritten shouldBe Some(3)
    // The file as it was written again, whole, and the one that never changed: each once.
    rowsOf(orders) shouldBe Seq(("10", 2L, "a.csv"), ("11", 2L, "a.csv"), ("2", 2L, "b.csv"))
    takenOf(orders) shouldBe Seq(("a.csv", 9L, 2L), ("b.csv", 5L, 2L))
    FilePlan.fromJson(batchOf(second).plan).value.names.map(_.value) shouldBe
      Vector("b.csv", "a.csv")
    batchesOf(orders) shouldBe Seq(1L -> "ABANDONED", 2L -> "COMMITTED")
    // The number of the batch that was given up is passed over, by the state and by the table.
    stateOf(orders).value.lastCommitted shouldBe second.seq
    delta.lastVersion(bronze(orders), second.app) shouldBe Some(2L)
    delta.committedAt(bronze(orders), batchId(orders, 1)) shouldBe None

    val idle = launch(orders)
    idle shouldBe Outcome.NothingNew(idle.run)
    assertLedger(orders)
  }

  test("a file that changes while it is read gives the batch up as well, after the read") {
    val orders = fileSource()
    val a = drop(orders, "a.csv", "id\n1\n")
    var read = false
    val changing = service(afterRead = () => {
      read = true
      Files.write(a, "id\n1\n2\n".getBytes(UTF_8))
      Files.setLastModifiedTime(a, FileTime.from(Now.minusSeconds(1800)))
      ()
    })

    val outcome = launch(orders, changing)

    read shouldBe true
    outcome shouldBe an[Outcome.GivenUp]
    val said = batchOf(batchId(orders, 1)).abandoned.value
    said.reason should include("1 file of it changed while it was read")
    tableIsThere(orders) shouldBe false
    takenOf(orders) shouldBe empty

    // The next run takes the file as it is by then.
    ingested(launch(orders)).batch shouldBe batchId(orders, 2)
    rowsOf(orders) shouldBe Seq(("1", 2L, "a.csv"), ("2", 2L, "a.csv"))
    assertLedger(orders)
  }

  test("a batch that its table has is never given up: the run stops, and changes nothing") {
    val orders = fileSource()
    val a = drop(orders, "a.csv", "id\n1\n")
    val first = batchId(orders, 1)
    val other = RunId.generate()
    // Another writer of the sequence, which read the file as it was planned, writes the batch;
    // then the file is written again. A run that lost its lease and went on does the first.
    val overtaken = service(hookAt(FaultPoint.AfterPlan) { batch =>
      val session = spark
      import session.implicits._
      new BronzeWriter(
        new Lake(lakeRoot, spark.sessionState.newHadoopConf()),
        delta,
        CommitContext.forRun(other.uuid)
      ).write(
        bronze(orders),
        BronzeBatch(other, orders.id, batch.id, Now),
        Seq("1").toDF("id")
      ) shouldBe BatchAppend.Appended(0, conflicts = 0, rows = Some(1))
      drop(orders, "a.csv", "id\n10\n11\n", at = Now.minusSeconds(1800)): Unit
    })

    // What the run stops with is a mistake of the application by its own word, which nothing
    // classifies: the run ends as `internal`, saying its type and where it was thrown, and the
    // log has what it said.
    val (outcome, logged) = CapturedLogs.of(classOf[IngestionService])(launch(orders, overtaken))
    val recorded = outcome match {
      case Outcome.Failed(_, FailureClass.Internal, message) => message
      case ended => fail(s"the run did not end as internal: $ended")
    }
    recorded should startWith(
      "nothing classified the failure, a mistake of the application: " +
        "java.lang.IllegalStateException at "
    )
    recorded should not include "changed after the batch was planned"
    val stopped = logged.flatMap(_.thrown).collectFirst { case e: IllegalStateException => e }.value
    stopped.getMessage should include(s"the source of $first changed after the batch was planned")
    stopped.getMessage should include("has the batch")
    stopped.getMessage should include("Nothing was given up, and nothing was changed")
    stopped.getMessage should include("'a.csv' is of 9 bytes, and was of 5")

    // The batch is planned as it was, and nothing says that it was given up.
    val left = batchOf(first)
    left.status shouldBe BatchStatus.Planned
    left.abandoned shouldBe None
    FilePlan.fromJson(left.plan).value.files.map(_.size) shouldBe Vector(5L)
    // The run says that it failed as `internal`, with what it recorded, and gives its lease up.
    val run1 = runOf(left.plannedBy)
    run1.status shouldBe RunStatus.Failed
    run1.failure.value shouldBe RunFailure(FailureClass.Internal, recorded)
    holderOf(orders) shouldBe None
    // Nothing is accounted for, and the table has what the other writer wrote, and no more.
    takenOf(orders) shouldBe empty
    stateOf(orders) shouldBe None
    tableVersion(orders) shouldBe 0L
    spark.read
      .format("delta")
      .load(bronze(orders).uri)
      .select("id", "_run_id")
      .collect()
      .map(row => row.getString(0) -> row.getString(1))
      .toSeq shouldBe Seq("1" -> other.toString)
    Files.size(a) shouldBe 9L
  }

  test("a batch whose table is away is not given up for a file that changed: the table is asked") {
    // The run that planned the batch wrote it, and died. Then its table is on a volume of its
    // own that is not mounted, which leaves the directory that it is mounted on, and a file of
    // the batch is written again. An empty directory is no table to a file system, and the
    // batch would be given up with its rows written; a run asks Delta, through the LogStore,
    // whose registry has the commits of the table.
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    val first = batchId(orders, 1)
    dies(orders, FaultPoint.AfterBronzeCommit)
    val table = Paths.get(URI.create(bronze(orders).uri))
    val away = table.resolveSibling(s"${table.getFileName}.unmounted")
    Files.move(table, away)
    Files.createDirectory(table)
    drop(orders, "a.csv", "id\n10\n11\n", at = Now.minusSeconds(1800))

    // A table that the LogStore refuses is a permanent failure (ADR-0022 §2): the run ends with
    // it, and gives its lease up.
    val refused = launch(orders)
    refused match {
      case Outcome.Failed(_, FailureClass.Permanent, message) =>
        message should include("is committed and acknowledged, and its file is not in the log")
      case other => fail(s"the run did not fail for good: $other")
    }
    runOf(refused.run).failure.value.failure shouldBe FailureClass.Permanent
    holderOf(orders) shouldBe None
    val left = batchOf(first)
    left.status shouldBe BatchStatus.Planned
    left.abandoned shouldBe None
    takenOf(orders) shouldBe empty

    // Once the table is there again it has the batch, which is committed as it was planned;
    // the file that changed is a new file, of the batch after.
    Files.delete(table)
    Files.move(away, table)
    launch(orders) shouldBe a[Outcome.RolledForward]
    ingested(launch(orders)).batch shouldBe batchId(orders, 2)
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"), ("10", 2L, "a.csv"), ("11", 2L, "a.csv"))
    assertLedger(orders)
  }

  test("a run that lost its lease gives no batch up: the batch is the other run's") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    val first = batchId(orders, 1)
    var successor = Option.empty[LeasedRun]
    // The lease runs out and another run takes it, while this one is between its plan and its
    // read; and the file is written again.
    val fenced = service(hookAt(FaultPoint.AfterPlan) { _ =>
      expire(orders)
      successor = Some(LeasedRun.open(database, starting(orders), versionOf(orders), timing).value)
      drop(orders, "a.csv", "id\n10\n11\n", at = Now.minusSeconds(1800)): Unit
    })

    val outcome = launch(orders, fenced)

    outcome match {
      case Outcome.Failed(_, failure, message) =>
        failure shouldBe FailureClass.Transient
        message should include(s"lost the lease of ${LeaseKey.of(orders.id, Layer.Bronze)}")
      case other => fail(s"the run did not fail: $other")
    }
    val taker = successor.value
    batchOf(first).status shouldBe BatchStatus.Planned
    batchOf(first).abandoned shouldBe None
    runOf(outcome.run).status shouldBe RunStatus.Abandoned
    holderOf(orders) shouldBe Some(taker.start.id)
    taker.planned().value.id shouldBe first
    taker.finish(RunEnd.Failed(FailureClass.Transient, "stopped by the test")) shouldBe true
    assertLedger(orders)
  }

  test("a file that changes once it was read changes nothing: what was read is what is written") {
    val orders = fileSource()
    val a = drop(orders, "a.csv", "id\n1\n")
    val late = service(hookAt(FaultPoint.AfterRead) { _ =>
      drop(orders, "a.csv", "id\n10\n11\n", at = Now.minusSeconds(1800)): Unit
    })

    ingested(launch(orders, late)).counts.rowsWritten shouldBe Some(1)

    // The file that was planned is the one that was read, and the one that is recorded.
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"))
    takenOf(orders) shouldBe Seq(("a.csv", 5L, 1L))
    Files.size(a) shouldBe 9L

    // What it is now is another file: the next batch takes it, whole (ADR-0008).
    ingested(launch(orders)).batch shouldBe batchId(orders, 2)
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"), ("10", 2L, "a.csv"), ("11", 2L, "a.csv"))
    takenOf(orders) shouldBe Seq(("a.csv", 5L, 1L), ("a.csv", 9L, 2L))
    assertLedger(orders)
  }

  // ---------------------------------------------------------------------------------------------
  // A batch with no rows
  // ---------------------------------------------------------------------------------------------

  test("a batch of empty files records its files, and neither makes its table nor changes it") {
    val orders = fileSource()
    drop(orders, "empty-1.csv", "")
    drop(orders, "empty-2.csv", "")
    val points = mutable.Buffer.empty[FaultPoint]
    val recording: FaultHooks = (point, _) => { points += point; () }

    val nothing = ingested(launch(orders, service(recording)))

    // The batch is committed, with its files and with nothing written.
    nothing.batch shouldBe batchId(orders, 1)
    nothing.outputs shouldBe empty
    nothing.counts shouldBe NothingCounted
    batchOf(nothing.batch).status shouldBe BatchStatus.Committed
    batchOf(nothing.batch).outputs shouldBe Some(Vector.empty)
    takenOf(orders) shouldBe Seq(("empty-1.csv", 0L, 1L), ("empty-2.csv", 0L, 1L))
    stateOf(orders).value.lastCommitted.value shouldBe 1L
    runOf(nothing.run).status shouldBe RunStatus.Succeeded
    // No table: not one made of the columns of the engine alone, which no batch could follow.
    tableIsThere(orders) shouldBe false
    delta.lastVersion(bronze(orders), nothing.batch.app) shouldBe None
    // It wrote nothing to Bronze, so it never was after a commit there.
    points.toSeq shouldBe Seq(
      FaultPoint.AfterPlan,
      FaultPoint.AfterRead,
      FaultPoint.AfterCounts,
      FaultPoint.BeforeStateCommit
    )
    val idle = launch(orders)
    idle shouldBe Outcome.NothingNew(idle.run)

    // The table is made by the first batch that has rows, with the columns of its source.
    drop(orders, "a.csv", "id,amount\n1,10.50\n")
    val first = ingested(launch(orders))
    first.batch shouldBe batchId(orders, 2)
    first.outputs shouldBe Seq(BatchOutput(orders.targets.bronze.table, 0))
    spark.read.format("delta").load(bronze(orders).uri).columns.take(2).toSeq shouldBe
      Seq("id", "amount")
    rowsOf(orders) shouldBe Seq(("1", 2L, "a.csv"))

    // An empty file after it, and one with a header and no record: the table is as it was.
    drop(orders, "empty-3.csv", "")
    drop(orders, "header.csv", "id,amount\n")
    val after = ingested(launch(orders))
    after.batch shouldBe batchId(orders, 3)
    after.outputs shouldBe empty
    after.counts shouldBe NothingCounted.copy(bytesRead = Some(10))
    tableVersion(orders) shouldBe 0L
    rowsOf(orders) shouldBe Seq(("1", 2L, "a.csv"))
    delta.lastVersion(bronze(orders), after.batch.app) shouldBe Some(2L)
    takenOf(orders).map(_._1) shouldBe
      Seq("a.csv", "empty-1.csv", "empty-2.csv", "empty-3.csv", "header.csv")

    // And the batch after those is appended, under its own number.
    drop(orders, "b.csv", "id,amount\n2,7\n")
    ingested(launch(orders)).outputs shouldBe Seq(BatchOutput(orders.targets.bronze.table, 1))
    rowsOf(orders) shouldBe Seq(("1", 2L, "a.csv"), ("2", 4L, "b.csv"))
    batchesOf(orders).map(_._2).distinct shouldBe Seq("COMMITTED")
    assertLedger(orders)
  }

  // ---------------------------------------------------------------------------------------------
  // A run that fails
  // ---------------------------------------------------------------------------------------------

  test("a failure that says its class ends the run with it, and gives the lease up") {
    val orders = fileSource()
    drop(orders, "a.csv", "id,amount\n1,2\n")
    drop(orders, "b.csv", "amount,id\n3,4\n")

    val outcome = launch(orders)

    outcome match {
      case Outcome.Failed(_, failure, message) =>
        failure shouldBe FailureClass.Permanent
        message should include("do not all have one header")
      case other => fail(s"the run did not fail: $other")
    }
    val ended = runOf(outcome.run)
    ended.status shouldBe RunStatus.Failed
    ended.failure.value.failure shouldBe FailureClass.Permanent
    holderOf(orders) shouldBe None
    // The batch stays planned: what gives it up, short of a source that changed, is P7's.
    batchesOf(orders) shouldBe Seq(1L -> "PLANNED")
    tableIsThere(orders) shouldBe false
    takenOf(orders) shouldBe empty
    assertLedger(orders)
  }

  // ---------------------------------------------------------------------------------------------
  // Records that are rejected: the quarantine, and how many are too many
  // ---------------------------------------------------------------------------------------------

  test("records that cannot be parsed are set apart in quarantine, before the data is written") {
    val orders = fileSource(rejected = Some("0.5"))
    val a = drop(orders, "a.csv", "id,amount\n1,10\n2\n3,30\n")
    val first = batchId(orders, 1)
    val seen = mutable.Buffer.empty[(FaultPoint, Option[Long], Option[Long])]
    // What each table has of the batch, at each point of the run.
    val recording: FaultHooks = (point, batch) => {
      seen += ((
        point,
        delta.lastVersion(quarantine(orders), batch.id.app),
        delta.lastVersion(bronze(orders), batch.id.app)
      ))
      ()
    }

    val done = ingested(launch(orders, service(recording)))

    // The quarantine first, and then the data: a batch whose data is written has both.
    seen.toSeq shouldBe Seq(
      (FaultPoint.AfterPlan, None, None),
      (FaultPoint.AfterRead, None, None),
      (FaultPoint.AfterCounts, None, None),
      (FaultPoint.AfterQuarantineWrite, Some(1L), None),
      (FaultPoint.AfterBronzeCommit, Some(1L), Some(1L)),
      (FaultPoint.BeforeStateCommit, Some(1L), Some(1L))
    )
    seen.map(_._1).toSeq shouldBe FaultPoint.All

    // What the batch wrote, in the order it wrote it, and what it counted.
    done.outputs shouldBe Seq(
      BatchOutput(quarantineTable(orders), 0),
      BatchOutput(orders.targets.bronze.table, 0)
    )
    batchOf(first).outputs shouldBe Some(done.outputs.toVector)
    done.counts shouldBe RunCounts(
      rowsRead = Some(3),
      rowsWritten = Some(2),
      rowsRejected = Some(1),
      bytesRead = Some(Files.size(a)),
      writeConflicts = Some(0),
      commitContention = Some(0)
    )
    runOf(done.run).counts shouldBe done.counts
    runOf(done.run).status shouldBe RunStatus.Succeeded

    // The record as it was, with where it came from; and the rows that were parsed, in Bronze.
    quarantine(orders).uri shouldBe s"${lakeRoot.value}/_quarantine/bronze/${orders.id}"
    spark.read.format("delta").load(quarantine(orders).uri).collect().toSeq shouldBe Seq(
      Row(
        done.run.toString,
        1L,
        s"${orders.id.value.replace('.', '_')}/a.csv",
        "unparsed",
        null,
        "2",
        Timestamp.from(Now)
      )
    )
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"), ("3", 1L, "a.csv"))
    takenOf(orders) shouldBe Seq(("a.csv", Files.size(a), 1L))
    assertLedger(orders)
  }

  test("more rejected records than the source allows: the batch is not committed") {
    // By default a source lets none be rejected (`quality.max_rejected_ratio`).
    val orders = fileSource()
    val a = drop(orders, "a.csv", "id,amount\n1,2\n3\n")
    val first = batchId(orders, 1)

    val outcome = launch(orders)

    outcome match {
      case Outcome.Failed(_, failure, message) =>
        failure shouldBe FailureClass.Data
        message should include(s"1 of the 2 records of $first could not be parsed")
        message should include("0.0 (quality.max_rejected_ratio)")
        message should include("nothing of it was written to Bronze")
        message should include(s"in quarantine, at ${quarantine(orders).uri}")
      case other => fail(s"the run did not fail: $other")
    }
    val ended = runOf(outcome.run)
    ended.status shouldBe RunStatus.Failed
    ended.failure.value.failure shouldBe FailureClass.Data
    ended.counts shouldBe RunCounts(
      rowsRead = Some(2),
      rowsWritten = Some(0),
      rowsRejected = Some(1),
      bytesRead = Some(Files.size(a)),
      writeConflicts = Some(0),
      commitContention = Some(0)
    )
    holderOf(orders) shouldBe None

    // The rejected record is where whoever owns the source finds it, and nothing else is
    // anywhere: no row in Bronze, no file taken, no state moved, and the batch still planned.
    quarantinedOf(orders) shouldBe Seq((1L, "a.csv", "3"))
    tableIsThere(orders) shouldBe false
    delta.lastVersion(bronze(orders), first.app) shouldBe None
    takenOf(orders) shouldBe empty
    stateOf(orders) shouldBe None
    batchOf(first).status shouldBe BatchStatus.Planned
    batchOf(first).outputs shouldBe None
    assertLedger(orders)
  }

  test("the share that a source allows is allowed whole, and one record more is too many") {
    // One record of two: half of the batch.
    val half = fileSource(rejected = Some("0.5"))
    drop(half, "a.csv", "id,amount\n1,2\n3\n")
    ingested(launch(half)).counts.rowsRejected shouldBe Some(1)
    rowsOf(half) shouldBe Seq(("1", 1L, "a.csv"))

    val less = fileSource(rejected = Some("0.49"))
    drop(less, "a.csv", "id,amount\n1,2\n3\n")
    launch(less) match {
      case Outcome.Failed(_, failure, message) =>
        failure shouldBe FailureClass.Data
        message should include("0.49 (quality.max_rejected_ratio)")
      case other => fail(s"the run did not fail: $other")
    }
    tableIsThere(less) shouldBe false
    quarantinedOf(less) shouldBe Seq((1L, "a.csv", "3"))
    assertLedger(half)
  }

  test("a batch whose records were all rejected, as its source allows, writes no data") {
    val events = fileSource(format = "json", rejected = Some("1"))
    val a = drop(events, "a.json", "not json\n{\"id\": 1\n")
    val points = mutable.Buffer.empty[FaultPoint]
    val recording: FaultHooks = (point, _) => { points += point; () }

    val done = ingested(launch(events, service(recording)))

    // Its quarantine is all that it wrote; its files are taken, as those of any batch.
    done.outputs shouldBe Seq(BatchOutput(quarantineTable(events), 0))
    done.counts shouldBe RunCounts(
      rowsRead = Some(2),
      rowsWritten = Some(0),
      rowsRejected = Some(2),
      bytesRead = Some(Files.size(a)),
      writeConflicts = Some(0),
      commitContention = Some(0)
    )
    quarantinedOf(events) shouldBe Seq((1L, "a.json", "not json"), (1L, "a.json", "{\"id\": 1"))
    tableIsThere(events) shouldBe false
    takenOf(events) shouldBe Seq(("a.json", Files.size(a), 1L))
    batchOf(done.batch).status shouldBe BatchStatus.Committed
    points.toSeq shouldBe Seq(
      FaultPoint.AfterPlan,
      FaultPoint.AfterRead,
      FaultPoint.AfterCounts,
      FaultPoint.AfterQuarantineWrite,
      FaultPoint.BeforeStateCommit
    )
    assertLedger(events)
  }

  test("a quarantine grows with the batches that reject, and the others leave it as it was") {
    val orders = fileSource(rejected = Some("0.5"))
    drop(orders, "a.csv", "id,amount\n1,2\n3\n")
    ingested(launch(orders)).outputs.map(_.version) shouldBe Seq(Some(0L), Some(0L))

    // A batch with nothing rejected writes its data alone.
    drop(orders, "b.csv", "id,amount\n4,5\n")
    ingested(launch(orders)).outputs shouldBe Seq(BatchOutput(orders.targets.bronze.table, 1))
    DeltaLog.forTable(spark, quarantine(orders).uri).update().version shouldBe 0L
    delta.lastVersion(quarantine(orders), batchId(orders, 2).app) shouldBe Some(1L)

    // The next one that rejects is appended, under its own number.
    drop(orders, "c.csv", "id,amount\n6,7\n8,9,10\n")
    ingested(launch(orders)).outputs shouldBe Seq(
      BatchOutput(quarantineTable(orders), 1),
      BatchOutput(orders.targets.bronze.table, 2)
    )
    quarantinedOf(orders) shouldBe Seq((1L, "a.csv", "3"), (3L, "c.csv", "8,9,10"))
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"), ("4", 2L, "b.csv"), ("6", 3L, "c.csv"))
    assertLedger(orders)
  }

  test("the records of a batch that was given up are left out of what the quarantine is read as") {
    val orders = fileSource(rejected = Some("0.5"))
    val reader = new QuarantineReader(database, spark, lakeRoot)
    def read(): Seq[(Long, String, String)] =
      reader
        .records(orders.id)
        .value
        .select("batch_seq", "source_file", "raw_record")
        .collect()
        .map(row => (row.getLong(0), row.getString(1).split('/').last, row.getString(2)))
        .sorted
        .toSeq
    // A source that never rejected a record has no quarantine to read.
    reader.records(orders.id) shouldBe None
    reader.table(orders.id) shouldBe quarantine(orders)

    // The first batch sets a record apart, and its run dies before its data is written. One
    // of its files is then written again, so the run after it gives the batch up.
    drop(orders, "a.csv", "id,amount\n1,2\n3\n")
    drop(orders, "b.csv", "id,amount\n4,5\n")
    val dead = dies(orders, FaultPoint.AfterQuarantineWrite)
    drop(orders, "b.csv", "id,amount\n6,7\n8,9\n", at = Now.minusSeconds(1800))
    val givingUp = launch(orders) match {
      case givenUp: Outcome.GivenUp => givenUp.run
      case other => fail(s"the batch was not given up: $other")
    }
    // The runs that took the batch: the one that planned it and died, and the one that gave it up.
    reader.leftOut(orders.id) shouldBe Vector(dead, givingUp)
    // Its record is in the table, and counts for nothing.
    quarantinedOf(orders) shouldBe Seq((1L, "a.csv", "3"))
    read() shouldBe empty

    // The next batch reads the file that did not change again, and sets the same record apart
    // under its own number: the table has it twice, and whoever reads it as it counts, once.
    ingested(launch(orders)).batch shouldBe batchId(orders, 2)
    quarantinedOf(orders) shouldBe Seq((1L, "a.csv", "3"), (2L, "a.csv", "3"))
    read() shouldBe Seq((2L, "a.csv", "3"))
    reader.records(orders.id).value.columns.toSeq shouldBe Quarantine.Columns

    // The records of a batch that is planned still are kept: of one that is not committed
    // because too many were rejected, they are what there is to look at.
    drop(orders, "c.csv", "id,amount\n10\n11\n12,13\n")
    launch(orders) match {
      case Outcome.Failed(_, failure, _) => failure shouldBe FailureClass.Data
      case other => fail(s"the run did not fail: $other")
    }
    batchesOf(orders) shouldBe Seq((1L, "ABANDONED"), (2L, "COMMITTED"), (3L, "PLANNED"))
    read() shouldBe Seq((2L, "a.csv", "3"), (3L, "c.csv", "10"), (3L, "c.csv", "11"))
    assertLedger(orders)
  }

  test("a run that dies between the quarantine and the data leaves a batch to be replayed") {
    val orders = fileSource(rejected = Some("0.5"))
    drop(orders, "a.csv", "id,amount\n1,2\n3\n")
    val first = batchId(orders, 1)
    val dying = service(hookAt(FaultPoint.AfterQuarantineWrite)(_ => throw new Died))

    intercept[Died](launch(orders, dying))

    // The batch is written in part: its quarantine has it, and its data does not.
    quarantinedOf(orders) shouldBe Seq((1L, "a.csv", "3"))
    tableIsThere(orders) shouldBe false
    val left = batchOf(first)
    left.status shouldBe BatchStatus.Planned
    takenOf(orders) shouldBe empty
    val planned = PlannedBatch(left.id, left.plan, left.plannedBy, left.plannedAt)
    // The table that is asked is the one that a batch writes last, its data: it says "replay",
    // and the quarantine, which has the batch, would have said that all of it is written.
    Recovery.of(Some(planned), bronze(orders), delta) shouldBe Recovery.Replay(planned)
    Recovery.of(Some(planned), quarantine(orders), delta) shouldBe Recovery.RollForward(planned)
  }
}
