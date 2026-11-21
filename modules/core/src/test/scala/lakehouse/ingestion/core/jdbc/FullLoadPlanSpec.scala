package lakehouse.ingestion.core.jdbc

import io.circe.Json
import lakehouse.ingestion.core.config.{QualifiedName, SafeYaml}
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class FullLoadPlanSpec extends AnyFunSuite with Matchers with EitherValues {

  private def table(text: String): QualifiedName = QualifiedName.parse(text).value

  private def read(text: String): Either[String, FullLoadPlan] =
    FullLoadPlan.fromJson(SafeYaml.parse(text).value)

  test("a plan is written in one way, and is read back as itself") {
    val plan = FullLoadPlan(table("public.orders"))

    plan.toJson.noSpaces shouldBe """{"version":1,"table":"public.orders"}"""
    FullLoadPlan.fromJson(plan.toJson).value shouldBe plan
    // A table without its schema is named so, and is another table than one with it.
    FullLoadPlan(table("orders")).toJson.noSpaces shouldBe """{"version":1,"table":"orders"}"""
    read("""{"version":1,"table":"orders"}""").value shouldBe FullLoadPlan(table("orders"))
  }

  test("a document that is not a plan of this form is refused, with what is wrong with it") {
    read("""{"version":2,"table":"orders"}""").left.value shouldBe
      "is of version 2, and this application reads version 1"
    read("""{"version":"1","table":"orders"}""").left.value shouldBe
      "version is not a whole number"
    read("""{"table":"orders"}""").left.value shouldBe "has no version"
    read("""{"version":1}""").left.value shouldBe "has no table"
    read("""{"version":1,"table":"orders","where":"1=1"}""").left.value shouldBe
      "has a field where"
    read("""{"version":1,"table":7}""").left.value shouldBe "table is not text"
    read("""[1]""").left.value shouldBe "is not an object"
    FullLoadPlan.fromJson(Json.Null).left.value shouldBe "is not an object"
  }

  test("a table that is no name is refused: what a plan names reaches a statement") {
    read("""{"version":1,"table":"orders; DROP TABLE canary"}""").left.value shouldBe
      "table has ';' (U+003B) at position 7: a name is letters of ASCII, digits and underscores"
    read("""{"version":1,"table":"a.b.c"}""").left.value shouldBe
      "table has 3 parts: a table is a name, or schema.name"
    read("""{"version":1,"table":""}""").left.value shouldBe "table is empty"
  }

  test("the plan of a batch of a watermark is no plan of a full load, nor the other way") {
    read(
      """{"version":1,"column":"updated_at","lower":null,"upper":"2026-11-20T10:00:00Z",
        |"horizon":"2026-11-20T10:00:00Z"}""".stripMargin
    ).left.value shouldBe
      "has no table, has a field column, has a field horizon, has a field lower, has a field upper"
  }
}
