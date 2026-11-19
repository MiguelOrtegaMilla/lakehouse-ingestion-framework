package lakehouse.ingestion.engine.connector

import java.time.Instant

import io.circe.Json
import lakehouse.ingestion.core.config.Read.{Obj, Result}
import lakehouse.ingestion.core.config._
import lakehouse.ingestion.core.files.LandedFile
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{EitherValues, OptionValues}

/** Which connector a source is planned and read with, by the kind of its configuration. */
final class ConnectorRegistrySpec
    extends AnyFunSuite
    with Matchers
    with EitherValues
    with OptionValues {

  import ConnectorRegistrySpec._

  private val nothingTaken = new FileRegistry {
    override def processed(directory: RelativePath, names: Seq[String]): Set[LandedFile] =
      Set.empty
  }
  private val context = PlanContext(Instant.parse("2026-11-19T09:00:00Z"), None, nothingTaken)

  private def source(id: String, kind: String): Source =
    Source
      .load(
        s"$id.yaml",
        s"""id: $id
           |connector: { type: $kind, says: "of $id" }
           |load: { mode: full }
           |targets:
           |  bronze: { table: bronze/x }
           |""".stripMargin,
        Seq(Told, Asked)
      )
      .value
      .value

  test("a source is planned by the connector of its kind, with its own configuration") {
    val registry = ConnectorRegistry(new Telling(Told), new Telling(Asked))

    registry.bind(source("shop.orders", "told")).plan(context).value shouldBe
      Json.obj("kind" -> Json.fromString("told"), "says" -> Json.fromString("of shop.orders"))
    registry.bind(source("shop.returns", "asked")).plan(context).value shouldBe
      Json.obj("kind" -> Json.fromString("asked"), "says" -> Json.fromString("of shop.returns"))
  }

  test("what a source is bound to is that source") {
    val orders = source("shop.orders", "told")

    ConnectorRegistry(new Telling(Told)).bind(orders).source shouldBe orders
  }

  test("the kinds, for a configuration to be read with, in the order they were registered") {
    ConnectorRegistry(new Telling(Asked), new Telling(Told)).kinds shouldBe Seq(Asked, Told)
  }

  test("two connectors of one kind are refused") {
    val failure = the[IllegalArgumentException] thrownBy
      ConnectorRegistry(new Telling(Told), new Telling(Asked), new Telling(Told))

    failure.getMessage should include("two connectors of one kind: 'told'")
  }

  test("a source of a kind that has no connector is refused, with the kinds there are") {
    val failure = the[IllegalArgumentException] thrownBy
      ConnectorRegistry(new Telling(Asked)).bind(source("shop.orders", "told"))

    failure.getMessage should include("of kind 'told', and this application has no connector")
    failure.getMessage should include("it has 'asked'")
  }

  test("a configuration of another class under the name of the kind is refused") {
    // A kind named `told` that reads into another class: the source was read with a kind that
    // is not the connector's.
    val impostor = new ConnectorKind {
      override val name = "told"
      override val loads: Set[Load.Mode] = Set(Load.Mode.Full)
      override def read(section: Obj): Result[ConnectorConfig] =
        section.required("says")(_.string).map(Other(_))
    }
    val orders = Source
      .load(
        "shop.orders.yaml",
        """id: shop.orders
          |connector: { type: told, says: "x" }
          |load: { mode: full }
          |targets:
          |  bronze: { table: bronze/x }
          |""".stripMargin,
        Seq(impostor)
      )
      .value
      .value

    val failure = the[IllegalArgumentException] thrownBy
      ConnectorRegistry(new Telling(Told)).bind(orders)
    failure.getMessage should include("is not what the connector of kind 'told' reads")
  }
}

object ConnectorRegistrySpec {

  /** A configuration that says something, and the kind whose it is. */
  final case class Says(kind: String, says: String) extends ConnectorConfig {
    override def canonical: Json = Json.obj("says" -> Json.fromString(says))
    override def origin: Seq[String] = Seq("says")
  }

  /** Another class of configuration. */
  final case class Other(says: String) extends ConnectorConfig {
    override val kind = "told"
    override def canonical: Json = Json.obj("says" -> Json.fromString(says))
    override def origin: Seq[String] = Seq("says")
  }

  final class SaysKind(override val name: String) extends ConnectorKind {
    override val loads: Set[Load.Mode] = Set(Load.Mode.Full)
    override def read(section: Obj): Result[ConnectorConfig] =
      section.required("says")(_.string).map(Says(name, _))
  }

  val Told = new SaysKind("told")
  val Asked = new SaysKind("asked")

  /** Plans what its source says, and reads nothing: there is no Spark here. */
  final class Telling(override val kind: ConnectorKind) extends SourceConnector[Says] {
    override def plan(source: Source, config: Says, context: PlanContext): Option[Json] =
      Some(Json.obj("kind" -> Json.fromString(config.kind), "says" -> Json.fromString(config.says)))

    override def read(source: Source, config: Says, plan: Json, context: ReadContext): ReadResult =
      throw new UnsupportedOperationException("not read here")
  }
}
