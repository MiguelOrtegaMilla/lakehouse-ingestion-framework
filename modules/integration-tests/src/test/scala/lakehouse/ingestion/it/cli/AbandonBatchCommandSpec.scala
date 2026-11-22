package lakehouse.ingestion.it.cli

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, StandardOpenOption}

import scala.concurrent.duration._

import lakehouse.ingestion.cli.Main
import lakehouse.ingestion.core.config.Source
import lakehouse.ingestion.core.control.{LeaseTiming, OperatorRequest}
import lakehouse.ingestion.engine.control.LeasedRun
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.it.support.DeploymentFiles
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The command `abandon-batch` from end to end, as an operator runs it: the configuration of a
 * deployment, the real control plane and LogStore, and the exit code of each of its outcomes
 * (discovery §14.1). `AbandonBatchSpec` has what the action does to a source.
 */
final class AbandonBatchCommandSpec extends AnyFunSuite with Matchers with RunCommandQueries {

  private lazy val files = new DeploymentFiles(pg, Files.createTempDirectory("abandon-command-"))
  import files.{arguments, drop, validProfile}

  override def afterAll(): Unit =
    try files.close()
    finally super.afterAll()

  private def fileSource(): Source = files.fileSource(newSourceId())

  private def run(of: Source): Ran = command(Map.empty)(arguments(of): _*)

  private def abandon(of: Source, batch: String, reason: String = "its file is malformed"): Ran = {
    val ran = command(Map.empty)(
      "abandon-batch",
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

  /** Who asked for an action, and why, as the control plane keeps them. */
  private def askedOf(of: Source): Seq[Seq[String]] = asSuperuser { c =>
    query(
      c,
      "SELECT run_kind, status, requested_by, reason FROM control.ingestion_run " +
        "WHERE source_id = ? AND run_kind <> 'regular' ORDER BY started_at",
      of.id.value
    )
  }

  /** A source whose first batch is planned and stays so: more records rejected than allowed. */
  private def stuck(): Source = {
    val orders = fileSource()
    drop(orders, "a.csv", "id,amount\n1,10\n2,20,extra\n")
    run(orders).exit shouldBe Main.Exit.DataThreshold
    batchesOf(orders) shouldBe Seq("1" -> "PLANNED")
    orders
  }

  private val operator = OperatorRequest.of(sys.props.getOrElse("user.name", ""), "x").toOption.get

  test("a batch that its table does not have is given up, and the command ends in 0") {
    val orders = stuck()

    val ran = abandon(orders, "1")

    withClue(ran.err)(ran.exit shouldBe Main.Exit.Ok)
    ran.out should include(s"was given up by the run")
    ran.out should include("its file is malformed")
    batchesOf(orders) shouldBe Seq("1" -> "ABANDONED")
    askedOf(orders) shouldBe Seq(
      Seq("abandon", "SUCCEEDED", operator.requestedBy, "its file is malformed")
    )
  }

  test("an operator acts on a source that is disabled: it is how a paused source is mended") {
    val orders = stuck()
    Files.write(
      files.sources.resolve(s"${orders.id}.yaml"),
      "enabled: false\n".getBytes(UTF_8),
      StandardOpenOption.APPEND
    )
    run(orders).out should include("is disabled")

    abandon(orders, "1").exit shouldBe Main.Exit.Ok
    batchesOf(orders) shouldBe Seq("1" -> "ABANDONED")
  }

  test("a batch that is not the planned one is refused in 4, and nothing is changed") {
    val orders = stuck()

    val other = abandon(orders, "2")

    other.exit shouldBe Main.Exit.Permanent
    said(other).head should include("is 1, and not 2")
    batchesOf(orders) shouldBe Seq("1" -> "PLANNED")
    askedOf(orders).map(_.take(2)) shouldBe Seq(Seq("abandon", "FAILED"))

    val none = abandon(fileSource(), "1")
    none.exit shouldBe Main.Exit.Permanent
    said(none).head should include("no batch of")
  }

  test("while a run holds the lease of the source, the command ends in 2 and changes nothing") {
    val orders = stuck()
    val holder =
      LeasedRun.open(database, starting(orders), versionOf(orders), LeaseTiming(2.minutes)).value

    val ran =
      try abandon(orders, "1")
      finally holder.close()

    withClue(ran.err)(ran.exit shouldBe Main.Exit.SkippedLocked)
    ran.out should include(s"the run ${holder.start.id} holds the lease of the source")
    batchesOf(orders) shouldBe Seq("1" -> "PLANNED")
    askedOf(orders).map(_.take(2)) shouldBe Seq(Seq("abandon", "SKIPPED_LOCKED"))
  }

  test("a command line without a reason, or with a batch that is no number, is not one: 64") {
    val orders = stuck()
    abandon(orders, "1", reason = "  ").exit shouldBe Main.Exit.Usage
    abandon(orders, "0").exit shouldBe Main.Exit.Usage
    abandon(orders, "1a").exit shouldBe Main.Exit.Usage
    abandon(orders, "01").exit shouldBe Main.Exit.Usage
    abandon(orders, "1", reason = "two\nlines").exit shouldBe Main.Exit.Usage
    batchesOf(orders) shouldBe Seq("1" -> "PLANNED")
    askedOf(orders) shouldBe empty
  }
}
