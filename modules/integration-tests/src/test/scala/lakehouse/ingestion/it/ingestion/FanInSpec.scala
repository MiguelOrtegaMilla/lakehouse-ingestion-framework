package lakehouse.ingestion.it.ingestion

import java.nio.file.Path

import scala.annotation.tailrec

import io.delta.tables.DeltaTable
import lakehouse.ingestion.cli.Main
import lakehouse.ingestion.core.config.{RootUri, Source}
import lakehouse.ingestion.core.control.{BatchRecord, BatchStatus, RunRecord, RunStatus}
import lakehouse.ingestion.engine.delta.CommitMetadata
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.it.support.FileGenerator._
import lakehouse.ingestion.it.support.{CommandProcess, DeploymentFiles}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Two sources that write one table of Bronze at the same time, each in a process of its own
 * (discovery §6.2, the second type of concurrency; ADR-0010): the command `run`, twice at once,
 * as an orchestrator that triggers two sources starts it.
 *
 * Nothing is done to make one process meet the other at a commit, and nothing here depends on
 * which gets there first: a table made by two writers at one moment, with one held at its
 * commit, is `BronzeWriterSpec`'s, and the commits of several processes at one version are the
 * stress runs'. What is asked is what holds however the two interleave: every row of each
 * source is in the table once, the log has a commit for each batch and no other, each batch
 * records the version that the log has under its name, and each source has a quarantine of its
 * own.
 *
 * '''The two runs of a round are to have been running at one time''', by the clock of the
 * control plane: with one run after the other this would show nothing. Two processes that are
 * started together are not promised to be, on a machine that is busy with something else. So a
 * round whose runs were not is made again, up to three times, and the test fails only if none
 * of the three had them at one time. A round that is made again is still held to everything
 * else: its rows are in the table, once.
 *
 * The files are `FileGenerator`'s, and so is what the table is to have. One source is of CSV and
 * the other of JSON, with the same columns: a column that a table does not have yet is the
 * policy of a schema (P9).
 */
final class FanInSpec extends AnyFunSuite with Matchers with GeneratedDeliveries {

  import FanInSpec._

  // The lake and the landing zone that the suite reads are those that the processes run on.
  private lazy val files = new DeploymentFiles(pg, workDir)
  override protected lazy val lakeRoot: RootUri = RootUri.parse(files.lake.toUri.toString).value
  override protected lazy val landingDirectory: Path = files.landing

  /** Two sources of one table, with nothing left for either yet. */
  private def newFanIn(): FanIn = {
    val table = Some(s"bronze/fan_in/${newSourceId().value.split('.').last}")
    val shop = files.fileSource(newSourceId(), Some("0.5"), format = "csv", table = table)
    val web = files.fileSource(newSourceId(), Some("0.5"), format = "json", table = table)
    bronze(shop) shouldBe bronze(web)
    FanIn(shop, web, Vector.empty, Vector.empty)
  }

  /**
   * One round: a delivery is left for each source, and a run of each is started, both before
   * either is awaited. Both are to end in 0, having committed their batch.
   *
   * @return the two sources with what they have by now, and whether the two runs were running
   *   at one time
   */
  private def round(before: FanIn): (FanIn, Boolean) = {
    val number = before.rounds + 1
    // The first delivery of each has files with every column: either may be the one that
    // makes the table.
    val ofShop =
      if (number == 1) Mix(files = 3, corrupt = 2, empty = 1, repeated = 1)
      else Mix(files = 2, corrupt = 1)
    val ofWeb =
      if (number == 1) Mix(files = 3, missing = 1, corrupt = 2, empty = 1)
      else Mix(files = 2, corrupt = 1, repeated = 1)
    val after = before.copy(
      ofShop = before.ofShop :+ deliver(41, Format.Csv, ofShop, number),
      ofWeb = before.ofWeb :+ deliver(42, Format.Json, ofWeb, number)
    )
    leave(after.shop, after.ofShop.last)
    leave(after.web, after.ofWeb.last)

    val ended = Seq(after.shop, after.web)
      .map(of => CommandProcess.start(files.arguments(of), files.newLog(), Heap))
      .map(_.await())

    ended.foreach(process => withClue(process.log)(process.exit shouldBe Main.Exit.Ok))
    val one = committer(batch(after.shop, number.toLong))
    val other = committer(batch(after.web, number.toLong))
    Seq(one, other).foreach(_.status shouldBe RunStatus.Succeeded)
    val together = one.startedAt.isBefore(other.finishedAt.value) &&
      other.startedAt.isBefore(one.finishedAt.value)
    // Said, and not asked: whether the two met at a commit is not this test's to decide.
    Seq("CSV" -> one, "JSON" -> other).foreach { case (source, run) =>
      info(
        s"round $number, the source of $source: ${run.counts.writeConflicts.value} write " +
          s"conflicts, ${run.counts.commitContention.value} commits that met another" +
          (if (together) "" else "; the two runs were not running at one time")
      )
    }
    (after, together)
  }

  /**
   * A round in which both sources make their table, with the two runs at one time. A table is
   * made once, so each try is of two sources of their own, and of a table of their own.
   */
  @tailrec
  private def made(tries: Int = Tries): FanIn = {
    val (fanIn, together) = round(newFanIn())
    asWritten(fanIn)
    if (together) fanIn
    else if (tries > 1) made(tries - 1)
    else fail(s"in none of $Tries rounds were the two runs that make a table running at one time")
  }

  /** A round more of two sources whose table is there, with the two runs at one time. */
  @tailrec
  private def appended(before: FanIn, tries: Int = Tries): FanIn = {
    val (after, together) = round(before)
    if (together) after
    else if (tries > 1) appended(after, tries - 1)
    else
      fail(s"in none of $Tries rounds were the two runs that append to a table running at one time")
  }

  private def batch(of: Source, seq: Long): BatchRecord = batchOf(batchId(of, seq))

  /** The run that committed a batch. */
  private def committer(batch: BatchRecord): RunRecord = runOf(batch.committedBy.value)

  /** The version of the table of Bronze of a source at which a batch says its rows are. */
  private def versionIn(of: Source, batch: BatchRecord): Long =
    batch.outputs.value.find(_.table == of.targets.bronze.table).value.version.value

  private def runsOf(of: Source): Seq[String] = asSuperuser { c =>
    query(
      c,
      "SELECT status FROM control.ingestion_run WHERE source_id = ? ORDER BY started_at",
      of.id.value
    ).map(_.head)
  }

  /** What holds of two sources of one table after their rounds, however their runs interleaved. */
  private def asWritten(fanIn: FanIn): Unit = {
    import fanIn._

    // Every row of each source, once, as its files have it; and no other row.
    written(shop, Columns) shouldBe expected(Columns, ofShop: _*)
    written(web, Columns) shouldBe expected(Columns, ofWeb: _*)
    spark.read.format("delta").load(bronze(shop).uri).count() shouldBe
      (ofShop ++ ofWeb).map(_.parsed).sum

    // The log has one commit for each batch and no other, and each batch records the version
    // that the log has under its name.
    val batches =
      for (of <- Seq(shop, web); seq <- 1L to rounds.toLong) yield of -> batch(of, seq)
    batches.foreach(_._2.status shouldBe BatchStatus.Committed)
    val history = DeltaTable
      .forPath(spark, bronze(shop).uri)
      .history()
      .select("version", "userMetadata")
      .collect()
      .map(row => row.getLong(0) -> row.getString(1))
      .toMap
    history.keySet shouldBe (0L until 2L * rounds).toSet
    batches.map { case (of, batch) => versionIn(of, batch) }.toSet shouldBe history.keySet
    batches.foreach { case (of, batch) =>
      history(versionIn(of, batch)) shouldBe
        CommitMetadata(batch.committedBy.value, of.id, batch.id).text
    }

    // The records that cannot be parsed are in the quarantine of their own source.
    def setApartOf(deliveries: Vector[Delivery]): Seq[(Long, String, String)] =
      deliveries.zipWithIndex.flatMap { case (delivery, i) => setApart(i + 1L, delivery) }.sorted
    quarantine(shop) should not be quarantine(web)
    quarantinedOf(shop) shouldBe setApartOf(ofShop)
    quarantinedOf(web) shouldBe setApartOf(ofWeb)

    // And nothing ran but one run of each source in each round.
    runsOf(shop) shouldBe Seq.fill(rounds)("SUCCEEDED")
    runsOf(web) shouldBe Seq.fill(rounds)("SUCCEEDED")
    assertLedger(shop)
    assertLedger(web)
  }

  test("two sources, in two processes at once, write one table: every row of each is there once") {
    // In the first round the table is not there, and both runs are to make it; in the second
    // it is, and both append to it.
    val both = appended(made())

    both.rounds should be >= 2
    asWritten(both)
  }
}

object FanInSpec {

  /** A heap as the writers of the stress runs have: two processes, beside the JVM of the tests. */
  private val Heap = "512m"

  /** How many times a round is made before the test says that two runs were never at one time. */
  private val Tries = 3

  /**
   * Two sources of one table of Bronze, and the deliveries that each was left, one for each
   * round.
   */
  final private case class FanIn(
      shop: Source,
      web: Source,
      ofShop: Vector[Delivery],
      ofWeb: Vector[Delivery]
  ) {
    def rounds: Int = ofShop.size
  }
}
