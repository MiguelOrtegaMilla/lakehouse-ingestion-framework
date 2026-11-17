package lakehouse.ingestion.core.config

import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The grammar of names, with the corpus of ADR-0013 §1: every one of them is refused. */
final class SqlIdentifierSpec extends AnyFunSuite with Matchers with EitherValues {

  test("a name of letters, digits and underscores is accepted, as it was written") {
    Seq("orders", "order_id", "_ingested_at", "Orders", "t1", "a", "x" * 63).foreach { name =>
      withClue(name)(SqlIdentifier.parse(name).value.value shouldBe name)
    }
    SqlIdentifier.parse("Orders").value should not be SqlIdentifier.parse("orders").value
    SqlIdentifier.parse("updated_at").value.toString shouldBe "updated_at"
  }

  test("the malicious corpus is refused, every name of it") {
    val corpus = Seq(
      "orders; DROP TABLE orders",
      "orders--",
      "orders\" OR \"1\"=\"1",
      "public.orders.extra",
      "",
      " ",
      "\t",
      "orders ",
      " orders",
      "x" * 64,
      "orders\u0000",
      "\u0000",
      // A Cyrillic o: the name reads as `orders`, and is another.
      "\u043Erders",
      "orders'",
      "orders`",
      "orders/*",
      "orders)",
      "my-table",
      "my table",
      "1st",
      "naïve",
      "orders\n",
      "public.orders"
    )

    corpus.foreach(name => withClue(s"[$name]")(SqlIdentifier.parse(name).isLeft shouldBe true))
  }

  test("a refusal says what is wrong, and names a character by its code point") {
    SqlIdentifier.parse("").left.value shouldBe "is empty"
    SqlIdentifier.parse("orders; DROP TABLE orders").left.value shouldBe
      "has ';' (U+003B) at position 7: a name is letters of ASCII, digits and underscores"
    SqlIdentifier.parse("\u043Erders").left.value shouldBe
      "has U+043E CYRILLIC SMALL LETTER O at position 1: a name is letters of ASCII, digits " +
      "and underscores"
    SqlIdentifier.parse("orders\u0000").left.value shouldBe
      "has U+0000 NULL at position 7: a name is letters of ASCII, digits and underscores"
    SqlIdentifier.parse("my table").left.value should startWith("has U+0020 SPACE at position 3")
    SqlIdentifier.parse("1st").left.value shouldBe
      "starts with a digit: a name starts with a letter or an underscore"
    SqlIdentifier.parse("x" * 64).left.value shouldBe "is 64 bytes long, and a name is 63 at most"
  }

  test("a refusal does not repeat the text it refuses") {
    // What a terminal would obey, or a log would be split by, never reaches a message.
    val reason = SqlIdentifier.parse("orders\u001B[2J\nERROR forged line").left.value

    reason should not include "\u001B"
    reason should not include "forged"
  }

  test("a table is a name, or a schema and a name") {
    QualifiedName.parse("orders").value shouldBe
      QualifiedName(None, SqlIdentifier.parse("orders").value)
    val qualified = QualifiedName.parse("public.orders").value
    qualified.schema.map(_.value) shouldBe Some("public")
    qualified.name.value shouldBe "orders"
    qualified.toString shouldBe "public.orders"
  }

  test("a table with more than a schema, or with a part that is no name, is refused") {
    QualifiedName.parse("public.orders.extra").left.value shouldBe
      "has 3 parts: a table is a name, or schema.name"
    QualifiedName.parse("sales.public.orders.extra").isLeft shouldBe true
    QualifiedName.parse("public.").left.value shouldBe "has a table name that is empty"
    QualifiedName.parse(".orders").left.value shouldBe "has a schema that is empty"
    QualifiedName.parse("public.orders; DROP TABLE orders").left.value should
      startWith("has a table name that has ';' (U+003B) at position 7")
    QualifiedName.parse("pub lic.orders").left.value should startWith("has a schema that has")
    QualifiedName.parse("").left.value shouldBe "is empty"
  }
}
