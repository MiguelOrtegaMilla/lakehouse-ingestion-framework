package lakehouse.ingestion.it.ingestion

import java.nio.file.Path

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
 * own. And that the two runs of a round were running at one time, by the clock of the control
 * plane, so that this does not pass with one run after the other.
 *
 * The files are `FileGenerator`'s, and so is what the table is to have. One source is of CSV and
 * the other of JSON, with the same columns: a column that a table does not have yet is the
 * policy of a schema (P9).
 */
final class FanInSpec extends AnyFunSuite with Matchers with GeneratedDeliveries {

  // The lake and the landing zone that the suite reads are those that the processes run on.
  private lazy val files = new DeploymentFiles(pg, workDir)
  override protected lazy val lakeRoot: RootUri = RootUri.parse(files.lake.toUri.toString).value
  override protected lazy val landingDirectory: Path = files.landing

  /** A heap as the writers of the stress runs have: two processes, beside the JVM of the tests. */
  private val Heap = "512m"

  /** One run of each source, all started before any is awaited: how each ended. */
  private def together(sources: Source*): Seq[CommandProcess.Ended] =
    sources
      .map(of => CommandProcess.start(files.arguments(of), files.newLog(), Heap))
      .map(_.await())

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

  test("two sources, in two processes at once, write one table: every row of each is there once") {
    val table = s"bronze/fan_in/${newSourceId().value.split('.').last}"
    val shop = files.fileSource(newSourceId(), Some("0.5"), format = "csv", table = Some(table))
    val web = files.fileSource(newSourceId(), Some("0.5"), format = "json", table = Some(table))
    bronze(shop) shouldBe bronze(web)

    val ofShop = Seq(
      deliver(41, Format.Csv, Mix(files = 3, corrupt = 2, empty = 1, repeated = 1), number = 1),
      deliver(41, Format.Csv, Mix(files = 2, corrupt = 1), number = 2)
    )
    val ofWeb = Seq(
      deliver(42, Format.Json, Mix(files = 3, missing = 1, corrupt = 2, empty = 1), number = 1),
      deliver(42, Format.Json, Mix(files = 2, corrupt = 1, repeated = 1), number = 2)
    )

    // Two rounds: in the first the table is not there, and both runs are to make it; in the
    // second it is, and both append to it.
    for (round <- 1 to 2) withClue(s"round $round: ") {
      leave(shop, ofShop(round - 1))
      leave(web, ofWeb(round - 1))

      val ended = together(shop, web)

      ended.foreach(process => withClue(process.log)(process.exit shouldBe Main.Exit.Ok))
      val (one, other) = (committer(batch(shop, round.toLong)), committer(batch(web, round.toLong)))
      Seq(one, other).foreach(_.status shouldBe RunStatus.Succeeded)
      // The two runs were running at one time, by the clock of the database.
      withClue(
        s"${one.startedAt} to ${one.finishedAt}, ${other.startedAt} to ${other.finishedAt}"
      ) {
        one.startedAt.isBefore(other.finishedAt.value) shouldBe true
        other.startedAt.isBefore(one.finishedAt.value) shouldBe true
      }
      // Said, and not asked: whether the two met at a commit is not this test's to decide.
      Seq("CSV" -> one, "JSON" -> other).foreach { case (source, run) =>
        info(
          s"round $round, the source of $source: ${run.counts.writeConflicts.value} write " +
            s"conflicts, ${run.counts.commitContention.value} commits that met another"
        )
      }
    }

    // Every row of each source, once, as its files have it; and no other row.
    written(shop, Columns) shouldBe expected(Columns, ofShop: _*)
    written(web, Columns) shouldBe expected(Columns, ofWeb: _*)
    spark.read.format("delta").load(bronze(shop).uri).count() shouldBe
      (ofShop ++ ofWeb).map(_.parsed).sum

    // The log has one commit for each batch and no other, and each batch records the version
    // that the log has under its name.
    val batches =
      for (of <- Seq(shop, web); seq <- Seq(1L, 2L)) yield of -> batch(of, seq)
    batches.foreach(_._2.status shouldBe BatchStatus.Committed)
    val history = DeltaTable
      .forPath(spark, bronze(shop).uri)
      .history()
      .select("version", "userMetadata")
      .collect()
      .map(row => row.getLong(0) -> row.getString(1))
      .toMap
    history.keySet shouldBe Set(0L, 1L, 2L, 3L)
    batches.map { case (of, batch) => versionIn(of, batch) }.toSet shouldBe history.keySet
    batches.foreach { case (of, batch) =>
      history(versionIn(of, batch)) shouldBe
        CommitMetadata(batch.committedBy.value, of.id, batch.id).text
    }

    // The records that cannot be parsed are in the quarantine of their own source.
    quarantine(shop) should not be quarantine(web)
    quarantinedOf(shop) shouldBe (setApart(1, ofShop(0)) ++ setApart(2, ofShop(1))).sorted
    quarantinedOf(web) shouldBe (setApart(1, ofWeb(0)) ++ setApart(2, ofWeb(1))).sorted

    // And nothing ran but the two runs of each source.
    runsOf(shop) shouldBe Seq("SUCCEEDED", "SUCCEEDED")
    runsOf(web) shouldBe Seq("SUCCEEDED", "SUCCEEDED")
  }
}
