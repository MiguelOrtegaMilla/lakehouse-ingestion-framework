package lakehouse.ingestion.it.connector

import java.time.Instant

import cats.syntax.all._
import io.circe.Json
import lakehouse.ingestion.core.config.Read.{Obj, Result}
import lakehouse.ingestion.core.config._
import lakehouse.ingestion.core.control.FailureClass
import lakehouse.ingestion.core.files.LandedFile
import lakehouse.ingestion.engine.failure.RunFailedException
import lakehouse.ingestion.engine.connector._
import lakehouse.ingestion.it.support.LocalSparkSuite
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{EitherValues, OptionValues}

/**
 * A kind of source that the framework does not have, added from outside the `engine` module: its
 * configuration, its connector, and one registration (RNF-04). Nothing of the engine changes for
 * it, and its sources are read like any other's: a configuration read with the kinds of the
 * registry, and each source planned and read through what the registry binds it to.
 */
final class AConnectorOfItsOwnSpec
    extends AnyFunSuite
    with Matchers
    with EitherValues
    with OptionValues
    with LocalSparkSuite {

  import AConnectorOfItsOwnSpec._

  private val registry = ConnectorRegistry(new CountingConnector)

  private val nothingTaken = new FileRegistry {
    override def processed(directory: RelativePath, names: Seq[String]): Set[LandedFile] =
      Set.empty
  }

  private def planContext(watermark: Option[Json]) =
    PlanContext(Instant.parse("2026-11-19T09:00:00Z"), watermark, nothingTaken)

  private val numbers: Source = Source
    .load(
      "lab.numbers.yaml",
      """id: lab.numbers
        |connector: { type: counting, per_batch: 3 }
        |load: { mode: full }
        |targets:
        |  bronze: { table: bronze/numbers }
        |""".stripMargin,
      registry.kinds
    )
    .value
    .value

  private def ids(result: ReadResult): Seq[Long] =
    result.rows.collect().map(_.getLong(0)).toSeq.sorted

  test("a configuration with the kind is read with the registry's kinds, and not without") {
    numbers.connector shouldBe Counting(3)

    val withoutIt = Source.load(
      "lab.numbers.yaml",
      """id: lab.numbers
        |connector: { type: counting, per_batch: 3 }
        |load: { mode: full }
        |targets:
        |  bronze: { table: bronze/numbers }
        |""".stripMargin
    )
    withoutIt.left.value.map(_.field).toList shouldBe List(Some("connector.type"))
  }

  test("its sources are planned and read through the registry") {
    val bound = registry.bind(numbers)

    val first = bound.plan(planContext(watermark = None)).value
    ids(bound.read(first, ReadContext(spark))) shouldBe Seq(0L, 1L, 2L)

    // From where the last batch left the source: the engine keeps the watermark, and gives it.
    val second = bound.plan(planContext(watermark = Some(Json.fromLong(3)))).value
    ids(bound.read(second, ReadContext(spark))) shouldBe Seq(3L, 4L, 5L)
  }

  test("a plan read again reads the same rows") {
    val bound = registry.bind(numbers)
    val plan = bound.plan(planContext(watermark = Some(Json.fromLong(10)))).value

    ids(bound.read(plan, ReadContext(spark))) shouldBe ids(bound.read(plan, ReadContext(spark)))
  }

  test("a plan that is not of its form is refused, as a permanent failure") {
    val failure = the[RunFailedException] thrownBy
      registry.bind(numbers).read(Json.obj("version" -> Json.fromInt(2)), ReadContext(spark))

    failure.failure shouldBe FailureClass.Permanent
  }

  test("what the commit of a plan records is said by its connector, from the plan alone") {
    val bound = registry.bind(numbers)
    val plan = bound.plan(planContext(watermark = Some(Json.fromLong(3)))).value

    // Where the next batch starts from, which the engine keeps and gives back (`PlanContext`).
    bound.progress(plan) shouldBe Progress(watermark = Some(Json.fromLong(6)), files = None)
    bound.progress(plan).bytes shouldBe None
    val next = bound.plan(planContext(watermark = bound.progress(plan).watermark)).value
    ids(bound.read(next, ReadContext(spark))) shouldBe Seq(6L, 7L, 8L)

    (the[RunFailedException] thrownBy bound.progress(Json.obj())).failure shouldBe
      FailureClass.Permanent
  }
}

object AConnectorOfItsOwnSpec {

  /** Counts on from where the last batch left off, `per_batch` at a time. */
  final case class Counting(perBatch: Int) extends ConnectorConfig {
    override def kind: String = CountingKind.name
    override def canonical: Json = Json.obj("per_batch" -> Json.fromInt(perBatch))
    override def origin: Seq[String] = Nil
  }

  object CountingKind extends ConnectorKind {
    override val name = "counting"
    override val loads: Set[Load.Mode] = Set(Load.Mode.Full)
    override def read(section: Obj): Result[ConnectorConfig] =
      section.orElse("per_batch", 1)(_.integer(1, 1000)).map(Counting(_))
  }

  /** The connector: a plan is a range of numbers, and a read is the rows of that range. */
  final class CountingConnector extends SourceConnector[Counting] {

    override val kind: ConnectorKind = CountingKind

    override def plan(source: Source, config: Counting, context: PlanContext): Option[Json] = {
      val from = context.watermark.flatMap(_.asNumber).flatMap(_.toLong).getOrElse(0L)
      Some(
        Json.obj(
          "version" -> Json.fromInt(1),
          "from" -> Json.fromLong(from),
          "to" -> Json.fromLong(from + config.perBatch)
        )
      )
    }

    override def read(
        source: Source,
        config: Counting,
        plan: Json,
        context: ReadContext
    ): ReadResult = {
      val (from, to) = rangeOf(source, plan)
      ReadResult.Kept(context.spark.range(from, to).toDF("id"), unparsed = None)
    }

    /** The next batch counts on from where this one ends. */
    override def progress(source: Source, config: Counting, plan: Json): Progress =
      Progress(watermark = Some(Json.fromLong(rangeOf(source, plan)._2)), files = None)

    private def rangeOf(source: Source, plan: Json): (Long, Long) = {
      val at = plan.hcursor
      val range = (
        at.get[Int]("version").toOption.filter(_ == 1),
        at.get[Long]("from").toOption,
        at.get[Long]("to").toOption
      ).mapN((_, from, to) => (from, to))
      range.getOrElse(
        throw RunFailedException.permanent(s"the plan of ${source.id} is not of version 1")
      )
    }
  }
}
