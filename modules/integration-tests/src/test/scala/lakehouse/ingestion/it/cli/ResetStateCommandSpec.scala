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
 * The command `reset-state` from end to end, as an operator runs it: the configuration of a
 * deployment, the real control plane and LogStore, what it says, and the exit code of each of its
 * outcomes (discovery §14.1). `ResetStateSpec` has what the action does to a source.
 */
final class ResetStateCommandSpec
    extends AnyFunSuite
    with Matchers
    with RunCommandQueries
    with CommandLedger {

  private lazy val files = new DeploymentFiles(pg, Files.createTempDirectory("reset-command-"))
  override protected def deployment: DeploymentFiles = files
  import files.{arguments, drop, validProfile}

  override def afterAll(): Unit =
    try files.close()
    finally super.afterAll()

  private def run(of: Source): Ran = command(Map.empty)(arguments(of): _*)

  private def reset(of: Source, mode: String*): Ran = {
    val ran = command(Map.empty)(
      Seq(
        "reset-state",
        "--profile",
        validProfile.toString,
        "--sources",
        files.sources.toString,
        "--source",
        of.id.value,
        "--reason",
        "the table was moved"
      ) ++ mode: _*
    )
    // The command leaves the JVM as it found it.
    SparkSession.getDefaultSession shouldBe empty
    ran
  }

  /** The resets of a source: the kind and status of the run, who asked, and why. */
  private def resetsOf(of: Source): Seq[Seq[String]] = asSuperuser { c =>
    query(
      c,
      "SELECT run_kind, status, requested_by, reason FROM control.ingestion_run " +
        "WHERE source_id = ? AND run_kind = 'reset' ORDER BY started_at",
      of.id.value
    )
  }

  private def changesOf(of: Source): Int = asSuperuser { c =>
    query(c, "SELECT count(*)::text FROM control.state_change_log WHERE source_id = ?", of.id.value)
  }.head.head.toInt

  private val operator = OperatorRequest.of(sys.props.getOrElse("user.name", ""), "x").toOption.get

  /** A source of files with one batch committed into its table of Bronze. */
  private def ingested(): Source = {
    val returns = files.fileSource(newSourceId())
    drop(returns, "a.csv", "id\n1\n")
    run(returns).exit shouldBe Main.Exit.Ok
    returns
  }

  test("a source whose table was moved is refused by run, and goes on once it is reset: 0") {
    val returns = ingested()
    val moved = files.fileSource(
      returns.id,
      table = Some(s"bronze/moved/${returns.id.value.replace('.', '_')}")
    )
    val refused = run(moved)
    refused.exit shouldBe Main.Exit.Permanent
    said(refused).head should include("an operator resets the state of the source with reset-state")

    val ran = reset(moved, "--keep-watermark")

    withClue(ran.err)(ran.exit shouldBe Main.Exit.Ok)
    ran.out should include(s"was reset by the run")
    ran.out should include("it lets through: targets.bronze.table of the source was")
    // The risk of the watermark kept is warned of.
    ran.errors.filter(_.startsWith("warning: the watermark was kept")) should have size 1
    ran.errors.mkString should include("the new one receives only what is ingested from now on")
    resetsOf(moved) shouldBe Seq(
      Seq("reset", "SUCCEEDED", operator.requestedBy, "the table was moved")
    )
    changesOf(moved) shouldBe 1
    drop(moved, "b.csv", "id\n2\n")
    run(moved).exit shouldBe Main.Exit.Ok
    batchesOf(moved) shouldBe Seq("1" -> "COMMITTED", "2" -> "COMMITTED")
    // The first batch was planned for the table that the source had before.
    assertLedger(moved, returns)
  }

  test("an operator resets a source that is disabled") {
    val returns = ingested()
    Files.write(
      files.sources.resolve(s"${returns.id}.yaml"),
      "enabled: false\n".getBytes(UTF_8),
      StandardOpenOption.APPEND
    )

    reset(returns, "--keep-watermark").exit shouldBe Main.Exit.Ok
    changesOf(returns) shouldBe 1
  }

  test("a reset that is refused ends in 4, and changes nothing") {
    val nothing = files.fileSource(newSourceId())
    val none = reset(nothing, "--keep-watermark")
    none.exit shouldBe Main.Exit.Permanent
    said(none).head should include("has no state in bronze")

    val returns = ingested()
    val moment = reset(returns, "--watermark", "2026-03-01T00:00:00Z")
    moment.exit shouldBe Main.Exit.Permanent
    said(moment).head should include("keeps no watermark")
    resetsOf(returns).map(_.take(2)) shouldBe Seq(Seq("reset", "FAILED"))
    changesOf(returns) shouldBe 0
  }

  test("while a run holds the lease of the source, the command ends in 2 and changes nothing") {
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
      try reset(returns, "--keep-watermark")
      finally holder.close()

    withClue(ran.err)(ran.exit shouldBe Main.Exit.SkippedLocked)
    ran.out should include(s"the run ${holder.start.id} holds the lease of the source")
    resetsOf(returns).map(_.take(2)) shouldBe Seq(Seq("reset", "SKIPPED_LOCKED"))
    changesOf(returns) shouldBe 0
  }

  test("a command line without one way of the watermark is not one: 64, nothing recorded") {
    val returns = ingested()
    reset(returns).exit shouldBe Main.Exit.Usage
    reset(returns, "--keep-watermark", "--forget-watermark").exit shouldBe Main.Exit.Usage
    reset(returns, "--watermark", "yesterday").exit shouldBe Main.Exit.Usage
    resetsOf(returns) shouldBe empty
  }
}
