package lakehouse.ingestion.it.cli

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, StandardOpenOption}

import scala.concurrent.duration._

import lakehouse.ingestion.cli.Main
import lakehouse.ingestion.core.config.Source
import lakehouse.ingestion.core.control.{LeaseTiming, OperatorRequest}
import lakehouse.ingestion.engine.control.LeasedRun
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.it.support.{DeploymentFiles, TakenAtItsWord}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The command `reprocess` from end to end, as an operator runs it: the configuration of a
 * deployment, the real control plane and LogStore, what it says, and the exit code of each of its
 * outcomes (discovery §14.1). `ReprocessSpec` has what it does to a source.
 */
final class ReprocessCommandSpec
    extends AnyFunSuite
    with Matchers
    with RunCommandQueries
    with CommandLedger {

  private lazy val files = new DeploymentFiles(pg, Files.createTempDirectory("reprocess-command-"))
  override protected def deployment: DeploymentFiles = files
  import files.{arguments, drop, validProfile}

  override def afterAll(): Unit =
    try files.close()
    finally super.afterAll()

  private def run(of: Source): Ran = command(Map.empty)(arguments(of): _*)

  private def reprocess(of: Source, batch: String, reason: String = "the parser was fixed"): Ran = {
    val ran = command(Map.empty)(
      "reprocess",
      "--profile",
      validProfile.toString,
      "--sources",
      files.sources.toString,
      "--source",
      of.id.value,
      "--batch",
      batch,
      "--reason",
      reason
    )
    // The command leaves the JVM as it found it.
    SparkSession.getDefaultSession shouldBe empty
    ran
  }

  /** The reprocesses of a source: the status of the run, who asked, and why. */
  private def reprocessesOf(of: Source): Seq[Seq[String]] = asSuperuser { c =>
    query(
      c,
      "SELECT status, requested_by, reason FROM control.ingestion_run " +
        "WHERE source_id = ? AND run_kind = 'reprocess' ORDER BY started_at",
      of.id.value
    )
  }

  private val operator = OperatorRequest.of(sys.props.getOrElse("user.name", ""), "x").toOption.get

  /** A source of files with one batch committed. */
  private def ingested(): Source = {
    val returns = files.fileSource(newSourceId())
    drop(returns, "a.csv", "id\n1\n")
    run(returns).exit shouldBe Main.Exit.Ok
    returns
  }

  test("the files of a committed batch are read again as the next batch: 0") {
    val returns = ingested()

    val ran = reprocess(returns, "1")

    withClue(ran.err)(ran.exit shouldBe Main.Exit.Ok)
    ran.out should include(s"committed the batch ${returns.id}/bronze#2: 1 rows read, 1 written")
    batchesOf(returns) shouldBe Seq("1" -> "COMMITTED", "2" -> "COMMITTED")
    reprocessesOf(returns) shouldBe
      Seq(Seq("SUCCEEDED", operator.requestedBy, "the parser was fixed"))
    assertLedger(returns)
  }

  test("an operator reprocesses a source that is disabled") {
    val returns = ingested()
    Files.write(
      files.sources.resolve(s"${returns.id}.yaml"),
      "enabled: false\n".getBytes(UTF_8),
      StandardOpenOption.APPEND
    )

    reprocess(returns, "1").exit shouldBe Main.Exit.Ok
  }

  test("a batch that was not committed is refused in 4, and nothing is planned") {
    val returns = ingested()
    val ran = reprocess(returns, "5")
    ran.exit shouldBe Main.Exit.Permanent
    said(ran).head should include("there is no batch")
    batchesOf(returns) shouldBe Seq("1" -> "COMMITTED")
    reprocessesOf(returns).map(_.head) shouldBe Seq("FAILED")
  }

  test("more records rejected than the source allows: 5, and nothing committed") {
    val returns = files.fileSource(newSourceId(), rejected = Some("0.5"))
    drop(returns, "a.csv", "id,amount\n1,10\n2,20,extra\n")
    run(returns).exit shouldBe Main.Exit.Ok
    files.fileSource(returns.id)

    reprocess(returns, "1").exit shouldBe Main.Exit.DataThreshold
    batchesOf(returns) shouldBe Seq("1" -> "COMMITTED", "2" -> "PLANNED")
    assertLedger(returns)
  }

  test("while a run holds the lease of the source, the command ends in 2 and plans nothing") {
    val returns = ingested()
    val holder =
      LeasedRun
        .open(
          database,
          starting(returns),
          versionOf(returns),
          LeaseTiming(2.minutes),
          TakenAtItsWord
        )
        .value

    val ran =
      try reprocess(returns, "1")
      finally holder.close()

    withClue(ran.err)(ran.exit shouldBe Main.Exit.SkippedLocked)
    ran.out should include(s"the run ${holder.start.id} holds the lease of the source")
    batchesOf(returns) shouldBe Seq("1" -> "COMMITTED")
  }

  test("a command line without a reason, or with a batch that is no number, is not one: 64") {
    val returns = ingested()
    reprocess(returns, "1", reason = " ").exit shouldBe Main.Exit.Usage
    reprocess(returns, "0").exit shouldBe Main.Exit.Usage
    reprocess(returns, "one").exit shouldBe Main.Exit.Usage
    reprocessesOf(returns) shouldBe empty
  }
}
