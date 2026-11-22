package lakehouse.ingestion.it.ingestion

import java.time.{Clock, ZoneOffset}

import cats.syntax.all._
import io.circe.Json
import lakehouse.ingestion.core.config.Read.{Obj, Result}
import lakehouse.ingestion.core.config._
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.connector._
import lakehouse.ingestion.engine.{FaultHooks, FaultPoint, IngestionService, RunFailedException}
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * A source whose configuration changed in a way that what was ingested of it cannot follow
 * (ADR-0007, P6.7a): its run is refused, under its lease and before a batch that a run left
 * planned is looked for, and the source goes on once it is given the configuration that it had.
 *
 * A change of where the rows come from breaks a source that is loaded by a watermark, so the
 * source here is one of the test's own that is: it counts on from its watermark, on a counter
 * that its configuration names, which is its origin. The table of Bronze is changed under a
 * source of files, as any source has one. The control plane, the tables and the LogStore are
 * real.
 */
final class BreakingChangeSpec
    extends AnyFunSuite
    with Matchers
    with OptionValues
    with IngestionSuite {

  import BreakingChangeSpec._

  /**
   * A source that counts on the counter `of`, loaded by a watermark.
   *
   * @param silver the key that it is merged into Silver by, and the table of Silver; none, for
   *   a source that goes no further than Bronze
   */
  private def counting(
      id: String,
      of: String = "a",
      silver: Option[(String, String)] = None
  ): Source =
    Source
      .load(
        s"$id.yaml",
        s"""id: $id
           |connector: { type: counter, of: $of }
           |load: { mode: incremental, watermark: { column: counted_at, safety_margin: 1m } }
           |${silver.fold("") { case (key, _) => s"keys: [$key]" }}
           |targets:
           |  bronze: { table: bronze/${id.replace('.', '/')} }
           |  ${silver.fold("") { case (_, table) => s"silver: { table: $table }" }}
           |""".stripMargin,
        ConnectorRegistry(new CounterConnector).kinds
      )
      .value
      .value

  private def counter(hooks: FaultHooks = FaultHooks.Off): IngestionService =
    new IngestionService(
      database,
      ConnectorRegistry(new CounterConnector),
      spark,
      lakeRoot,
      timing,
      Clock.fixed(Now, ZoneOffset.UTC),
      hooks
    )

  /** The numbers in the table of a source, each with the batch that wrote it. */
  private def counted(of: Source): Seq[(Long, Long)] =
    spark.read
      .format("delta")
      .load(bronze(of).uri)
      .select("id", "_batch_seq")
      .collect()
      .map(row => row.getLong(0) -> row.getLong(1))
      .sorted
      .toSeq

  private def refused(outcome: Outcome): Outcome.Failed = outcome match {
    case failure: Outcome.Failed =>
      failure.failure shouldBe FailureClass.Permanent
      failure
    case other => fail(s"the run was not refused: $other")
  }

  private def dies(of: Source, at: FaultPoint): Unit = {
    intercept[Died](launch(of, counter(hookAt(at)(_ => throw new Died))))
    expire(of)
  }

  test("a source that reads from somewhere else is refused, until it reads what it read") {
    val id = newSourceId().value
    val fromA = counting(id)
    val fromB = counting(id, of = "b")
    ingested(launch(fromA, counter())).batch shouldBe batchId(fromA, 1)

    val stopped = refused(launch(fromB, counter()))

    stopped.message shouldBe
      s"the configuration of $id changed in a way that what was ingested of it cannot " +
      "follow: connector.of of the source was a and is b: the watermark that is stored is of " +
      "what was read before, and going on from it would skip rows of what is read now " +
      "without a word. Nothing was planned, read or written, and a batch that a run left " +
      "planned is as it was. The configuration that the state of the source was last " +
      s"advanced with is in control.config_snapshot, under the hash ${versionOf(fromA).hash}: " +
      "give the source that one, and run it again. To go on under the new one, an operator " +
      "resets the state of the source with reset-state and a reason, setting its watermark " +
      "(--watermark), forgetting it (--forget-watermark) or keeping it (--keep-watermark); " +
      "its configuration is then the one that later runs are compared with"
    // The run is recorded as it ended, with the configuration that it came with, and gave its
    // lease up. Nothing of the source moved.
    val run = runOf(stopped.run)
    run.status shouldBe RunStatus.Failed
    run.failure.value.failure shouldBe FailureClass.Permanent
    run.start.configHash shouldBe versionOf(fromB).hash
    holderOf(fromA) shouldBe None
    batchesOf(fromA) shouldBe Seq((1L, "COMMITTED"))
    stateOf(fromA).value.watermark shouldBe Some(Json.fromLong(PerBatch))
    counted(fromA) shouldBe Seq((0L, 1L), (1L, 1L))

    // The run that was refused advanced nothing, and is not what the next is compared with.
    refused(launch(fromB, counter())).message shouldBe stopped.message
    // With the configuration that it had, the source goes on from where it was.
    ingested(launch(fromA, counter())).batch shouldBe batchId(fromA, 2)
    counted(fromA) shouldBe Seq((0L, 1L), (1L, 1L), (2L, 2L), (3L, 2L))
  }

  test("it is refused before the batch that a run left written is rolled forward") {
    val id = newSourceId().value
    val fromA = counting(id)
    val fromB = counting(id, of = "b")
    ingested(launch(fromA, counter()))
    // The second batch is in its table, and the control plane was not told.
    dies(fromA, FaultPoint.AfterBronzeCommit)
    batchesOf(fromA) shouldBe Seq((1L, "COMMITTED"), (2L, "PLANNED"))

    // Rolled forward by this run, the state would be advanced under its configuration, and
    // the run after it would find nothing changed.
    refused(launch(fromB, counter())).message should include(
      "connector.of of the source was a and is b"
    )

    batchesOf(fromA) shouldBe Seq((1L, "COMMITTED"), (2L, "PLANNED"))
    stateOf(fromA).value.lastCommitted shouldBe BatchSeq.of(1).value
    holderOf(fromA) shouldBe None
    refused(launch(fromB, counter()))

    // The batch is finished under the configuration that it was planned under.
    launch(fromA, counter()) match {
      case forward: Outcome.RolledForward => forward.batch shouldBe batchId(fromA, 2)
      case other => fail(s"the batch was not rolled forward: $other")
    }
    counted(fromA) shouldBe Seq((0L, 1L), (1L, 1L), (2L, 2L), (3L, 2L))
    // And the change is still one: the state was advanced under the configuration of before.
    refused(launch(fromB, counter()))
  }

  test("another table of Bronze is refused, and nothing is written to it") {
    val id = newSourceId().value
    val orders = fileSource(id = id)
    val moved = fileSource(id = id, table = Some(s"bronze/moved/${id.replace('.', '/')}"))
    drop(orders, "a.csv", "id\n1\n")
    ingested(launch(orders))
    drop(orders, "b.csv", "id\n2\n")

    val stopped = refused(launch(moved))

    stopped.message should include(
      s"targets.bronze.table of the source was ${orders.targets.bronze.table} and is " +
        s"${moved.targets.bronze.table}: the Bronze table is another, and the state of the " +
        "source says what was written to the old one"
    )
    // The new file was not planned, and the other table was not made.
    isThere(bronze(moved)) shouldBe false
    batchesOf(orders) shouldBe Seq((1L, "COMMITTED"))
    takenOf(orders).map(_._1) shouldBe Seq("a.csv")

    // In the table that it had, the source takes the file.
    ingested(launch(orders)).batch shouldBe batchId(orders, 2)
    rowsOf(orders) shouldBe Seq(("1", 1L, "a.csv"), ("2", 2L, "b.csv"))
  }

  test("a change that Bronze follows does not stop a run into Bronze") {
    val id = newSourceId().value
    val withSilver = counting(id, silver = Some("id" -> s"silver/${id.replace('.', '/')}"))
    val otherSilver =
      counting(id, silver = Some("number" -> s"silver/again/${id.replace('.', '/')}"))
    ingested(launch(withSilver, counter()))

    // Another key and another table of Silver: what was merged cannot follow them, and what
    // was ingested is as it was.
    ConfigVersion
      .breakingChanges(versionOf(withSilver).content, versionOf(otherSilver))
      .map(change => change.field -> change.breaksBronze) shouldBe
      Seq("targets.silver.table" -> false, "keys" -> false)

    ingested(launch(otherSilver, counter())).batch shouldBe batchId(withSilver, 2)
    counted(withSilver) shouldBe Seq((0L, 1L), (1L, 1L), (2L, 2L), (3L, 2L))
  }

  test("a source that has committed nothing has nothing that a change could break") {
    val id = newSourceId().value
    val fromA = counting(id)
    val fromB = counting(id, of = "b")
    // Its first batch is planned, and nothing of it is committed: the source has no state.
    dies(fromA, FaultPoint.AfterPlan)
    stateOf(fromA) shouldBe None

    ingested(launch(fromB, counter())).batch shouldBe batchId(fromA, 1)

    // From here on it has, and it is the configuration that committed that is compared with.
    refused(launch(fromA, counter())).message should include(
      "connector.of of the source was b and is a"
    )
  }
}

object BreakingChangeSpec {

  final class Died extends RuntimeException("the run died here")

  /** How many numbers a batch of a counter has. */
  val PerBatch = 2L

  /** @param of the counter that is read: where the rows of the source come from */
  final case class Counter(of: String) extends ConnectorConfig {
    override def kind: String = CounterKind.name
    override def canonical: Json = Json.obj("of" -> Json.fromString(of))
    override def origin: Seq[String] = Seq("of")
  }

  object CounterKind extends ConnectorKind {
    override val name = "counter"
    override val loads: Set[Load.Mode] = Set(Load.Mode.Incremental)
    override def read(section: Obj): Result[ConnectorConfig] =
      section.required("of")(_.string).map(Counter(_))
  }

  /**
   * Counts on from the watermark of its source, `PerBatch` numbers a batch. Its plan names the
   * numbers and not the counter, so nothing of the connector tells a batch of one counter from
   * a batch of another: what does is the configuration that the state was advanced with.
   */
  final class CounterConnector extends SourceConnector[Counter] {

    override val kind: ConnectorKind = CounterKind

    override def plan(source: Source, config: Counter, context: PlanContext): Option[Json] = {
      val from = context.watermark.flatMap(_.asNumber).flatMap(_.toLong).getOrElse(0L)
      Some(
        Json.obj(
          "version" -> Json.fromInt(1),
          "from" -> Json.fromLong(from),
          "to" -> Json.fromLong(from + PerBatch)
        )
      )
    }

    override def read(
        source: Source,
        config: Counter,
        plan: Json,
        context: ReadContext
    ): ReadResult = {
      val (from, to) = rangeOf(source, plan)
      ReadResult.Kept(context.spark.range(from, to).toDF("id"), unparsed = None)
    }

    override def progress(source: Source, config: Counter, plan: Json): Progress =
      Progress(watermark = Some(Json.fromLong(rangeOf(source, plan)._2)), files = None)

    private def rangeOf(source: Source, plan: Json): (Long, Long) = {
      val at = plan.hcursor
      (
        at.get[Int]("version").toOption.filter(_ == 1),
        at.get[Long]("from").toOption,
        at.get[Long]("to").toOption
      ).mapN((_, from, to) => (from, to))
        .getOrElse(
          throw RunFailedException.permanent(s"the plan of ${source.id} is not of version 1")
        )
    }
  }
}
