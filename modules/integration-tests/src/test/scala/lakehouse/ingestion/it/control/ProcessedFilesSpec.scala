package lakehouse.ingestion.it.control

import java.time.Instant

import scala.concurrent.duration._

import lakehouse.ingestion.core.config.{FileConnectorConfig, RelativePath, Source}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.core.files.FilePlanner.{NothingNew, Planned}
import lakehouse.ingestion.core.files.{FileName, FilePlan, FilePlanner, LandedFile, Listed}
import lakehouse.ingestion.engine.control._
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The files that batches take, on a real PostgreSQL (P5.3): a file source plans its batch from a
 * listing and what was taken, writes the plan down, and the commit of the batch records its
 * files in the transaction that moves its state. A file is taken once, and one written again
 * under its name is taken again.
 */
final class ProcessedFilesSpec extends AnyFunSuite with Matchers with ControlPlaneSuite {

  private val timing = LeaseTiming(2.minutes)
  private val Now = Instant.parse("2026-11-18T12:00:00Z")

  private def fileSource(): Source = {
    val id = newSourceId().value
    Source
      .load(
        s"$id.yaml",
        s"""id: $id
           |connector: { type: file, path: landing/${id.replace('.', '_')}, format: csv }
           |load: { mode: new_files }
           |targets: { bronze: { table: bronze/${id.replace('.', '/')} } }
           |""".stripMargin
      )
      .value
      .value
  }

  private def connector(of: Source): FileConnectorConfig =
    of.connector.asInstanceOf[FileConnectorConfig]

  private def open(of: Source): LeasedRun =
    LeasedRun.open(database, starting(of), versionOf(of), timing).value

  private def listed(name: String, size: Long = 10, at: String = "2026-11-18T10:00:00.123Z") =
    Listed(name, size, Instant.parse(at))

  /** Steps 2 and 3 of a file source: what is new, from the listing and what was taken. */
  private def plan(leased: LeasedRun, of: Source, listing: Seq[Listed]): FilePlanner.Planning = {
    val config = connector(of)
    val taken = leased.processed(config.path, listing.map(_.name))
    FilePlanner.plan(config, listing, taken, Now).value
  }

  private def planned(planning: FilePlanner.Planning): FilePlan = planning match {
    case Planned(filePlan, _, _) => filePlan
    case other => fail(s"no batch was planned: $other")
  }

  /** Plans a batch of the listing, writes the plan down, and commits it with its files. */
  private def ingest(of: Source, listing: Seq[Listed]): (PlannedBatch, FilePlan) = {
    val leased = open(of)
    val filePlan = planned(plan(leased, of, listing))
    val batch = leased.plan(filePlan.toJson)
    leased.commit(
      batch.id,
      None,
      Seq(BatchOutput(of.targets.bronze.table, 0)),
      files = Some(filePlan)
    ) shouldBe true
    (batch, filePlan)
  }

  private def rows(of: Source): Seq[Seq[String]] = asSuperuser { c =>
    query(
      c,
      """SELECT path, size::text, to_char(modified_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS'),
        |       txn_app_id, batch_seq::text
        |  FROM control.processed_file WHERE source_id = ? ORDER BY path, size, modified_at""".stripMargin,
      of.id.value
    )
  }

  test("the commit of a batch records its files, and the next run takes none of them again") {
    val returns = fileSource()
    val directory = connector(returns).path.value
    val listing = Seq(
      listed("returns 1.csv", size = 0, at = "2026-11-18T09:00:00Z"),
      listed("devoluciones-año.csv", size = 123456789012L),
      // What the producer names a file is its own: it is stored, and read back, as it is.
      listed("数据-\uFFFD.json", at = "2026-11-18T11:00:00.999Z")
    )

    val (batch, filePlan) = ingest(returns, listing)

    filePlan.names.map(_.value) shouldBe listing.map(_.name).toVector
    rows(returns) shouldBe Seq(
      Seq(
        s"$directory/devoluciones-año.csv",
        "123456789012",
        "2026-11-18T10:00:00.123",
        s"${batch.id.app}",
        "1"
      ),
      Seq(s"$directory/returns 1.csv", "0", "2026-11-18T09:00:00.000", s"${batch.id.app}", "1"),
      Seq(s"$directory/数据-\uFFFD.json", "10", "2026-11-18T11:00:00.999", s"${batch.id.app}", "1")
    )

    val next = open(returns)
    next.processed(connector(returns).path, listing.map(_.name)) shouldBe filePlan.files.toSet
    plan(next, returns, listing) shouldBe NothingNew(waiting = 0)
    next.finish(RunEnd.Succeeded(RunCounts.Nothing)) shouldBe true
  }

  test("the plan of a batch is read back from the control plane as the plan it was") {
    val returns = fileSource()
    val leased = open(returns)
    val filePlan = planned(
      plan(
        leased,
        returns,
        Seq(
          listed("año 2026.csv", size = Long.MaxValue),
          listed("x\uFFFEy.csv", at = "2026-11-18T10:00:00Z")
        )
      )
    )

    val batch = leased.plan(filePlan.toJson)

    // jsonb keeps neither the order of keys nor the spaces: what is compared is the plan.
    FilePlan.fromJson(leased.planned().value.plan) shouldBe Right(filePlan)
    FilePlan.fromJson(database.run(Batches.find(_, batch.id)).value.plan) shouldBe Right(filePlan)
    leased.finish(RunEnd.Failed(FailureClass.Transient, "stopped by the test")) shouldBe true
  }

  test("a file written again under its name is another file, and is taken again") {
    val returns = fileSource()
    val first = Seq(listed("a.csv"), listed("b.csv"))
    ingest(returns, first)

    val rewritten = Seq(listed("a.csv", size = 11, at = "2026-11-18T11:30:00Z"), listed("b.csv"))
    val (second, again) = ingest(returns, rewritten)

    again.names.map(_.value) shouldBe Vector("a.csv")
    second.id.seq shouldBe BatchSeq.of(2).value
    rows(returns).map(row => (row(0).split('/').last, row(1), row(4))) shouldBe Seq(
      ("a.csv", "10", "1"),
      ("a.csv", "11", "2"),
      ("b.csv", "10", "1")
    )
  }

  test("only the files of the plan that was written down are recorded, or nothing is") {
    val returns = fileSource()
    val leased = open(returns)
    val listing = Seq(listed("a.csv"), listed("b.csv"))
    val filePlan = planned(plan(leased, returns, listing))
    val batch = leased.plan(filePlan.toJson)

    // A caller that says the batch read other files than its plan names.
    val other = FilePlan(
      filePlan.directory,
      Vector(LandedFile(FileName.parse("c.csv").value, 1, Instant.parse("2026-11-18T10:00:00Z")))
    )
    the[IllegalStateException] thrownBy
      leased.commit(batch.id, None, Nil, files = Some(other)) should have message
      s"the batch ${batch.id} was not committed: the files given are not those of its plan"

    rows(returns) shouldBe empty
    database.run(SourceStates.find(_, returns.id, Layer.Bronze)) shouldBe None
    database.run(Batches.find(_, batch.id)).value.status shouldBe BatchStatus.Planned
  }

  test("a file that a batch took already is not recorded twice, and the commit changes nothing") {
    val returns = fileSource()
    val (_, firstPlan) = ingest(returns, Seq(listed("a.csv")))

    // A plan that names it again, as a planner that did not ask what was taken would make.
    val leased = open(returns)
    val again = FilePlan(firstPlan.directory, firstPlan.files :+ landedOf(listed("b.csv")))
    val batch = leased.plan(again.toJson)
    the[IllegalStateException] thrownBy
      leased.commit(batch.id, None, Nil, files = Some(again)) should have message
      s"the batch ${batch.id} was not committed: a file of its plan was taken by a batch already"

    rows(returns).map(_.head.split('/').last) shouldBe Seq("a.csv")
    database.run(SourceStates.find(_, returns.id, Layer.Bronze)).value.lastCommitted shouldBe
      BatchSeq.First
    leased.finish(RunEnd.Failed(FailureClass.Permanent, "stopped by the test")) shouldBe true
  }

  test("a zombie records no file") {
    val returns = fileSource()
    val zombie = open(returns)
    val filePlan = planned(plan(zombie, returns, Seq(listed("a.csv"))))
    val batch = zombie.plan(filePlan.toJson)
    zombie.close()
    asSuperuser { c =>
      update(
        c,
        "UPDATE control.source_lease SET expires_at = now() - interval '1 second' WHERE lease_key = ?",
        zombie.start.leaseKey.value
      ) shouldBe 1: Unit
    }
    val successor = open(returns)

    zombie.commit(batch.id, None, Nil, files = Some(filePlan)) shouldBe false
    rows(returns) shouldBe empty

    // The run that took over finds the plan, and records its files when it commits it.
    val found = successor.planned().value
    FilePlan.fromJson(found.plan) shouldBe Right(filePlan)
    successor.commit(found.id, None, Nil, files = Some(filePlan)) shouldBe true
    rows(returns).map(_.head.split('/').last) shouldBe Seq("a.csv")
  }

  test("what was taken is asked by name, and a name that is not there is not found") {
    val returns = fileSource()
    val (_, filePlan) = ingest(returns, Seq(listed("a.csv"), listed("b.csv")))
    val directory = connector(returns).path
    val leased = open(returns)

    leased.processed(directory, Nil) shouldBe empty
    leased.processed(directory, Seq("b.csv", "missing.csv", "bad[1].csv", "a/../b.csv")) shouldBe
      Set(filePlan.files(1))
    // Another directory, as a source that moved, has its own files.
    leased.processed(RelativePath.parse("elsewhere").value, Seq("a.csv", "b.csv")) shouldBe empty
    // Another source has its own, in the same directory.
    val other = open(fileSource())
    other.processed(directory, Seq("a.csv", "b.csv")) shouldBe empty

    leased.finish(RunEnd.Succeeded(RunCounts.Nothing)) shouldBe true
    other.finish(RunEnd.Succeeded(RunCounts.Nothing)) shouldBe true
  }

  private def landedOf(file: Listed): LandedFile =
    LandedFile(FileName.parse(file.name).value, file.size, file.modifiedAt)
}
