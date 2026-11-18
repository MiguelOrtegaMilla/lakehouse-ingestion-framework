package lakehouse.ingestion.engine.control

import io.circe.{Json, JsonObject, Printer}
import lakehouse.ingestion.core.config.SafeYaml
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The JSON that the control plane stores, read back by the safe reader of the configuration.
 * Whatever a `Json` is, written as JSON in any of its ways, it is read as the same `Json`.
 */
final class StoredJsonSpec extends AnyFunSuite with Matchers with EitherValues {

  /** The ways a document is written: compact, indented, as PostgreSQL does, all in ASCII. */
  private val printers: Seq[(String, Printer)] = Seq(
    "compact" -> Printer.noSpaces,
    "indented" -> Printer.spaces2,
    "sorted" -> Printer.noSpacesSortKeys,
    "as jsonb" -> Printer.noSpaces
      .copy(colonRight = " ", objectCommaRight = " ", arrayCommaRight = " "),
    "ascii" -> Printer.noSpaces.copy(escapeNonAscii = true)
  )

  private def sameReadBack(document: Json): Unit =
    printers.foreach { case (way, printer) =>
      withClue(s"$way: ${printer.print(document).take(200)}\n") {
        StoredJson.parse(printer.print(document)).value shouldBe document
      }
    }

  private def text(value: String): Json = Json.fromString(value)

  test("objects, arrays and every kind of value are read as they were written") {
    sameReadBack(
      Json.obj(
        "schema_version" -> Json.fromInt(1),
        "enabled" -> Json.True,
        "disabled" -> Json.False,
        "nothing" -> Json.Null,
        "empty_object" -> Json.obj(),
        "empty_array" -> Json.arr(),
        "empty_text" -> text(""),
        "keys" -> Json.arr(text("order_id"), text("line")),
        "nested" -> Json.obj("a" -> Json.obj("b" -> Json.arr(Json.obj("c" -> Json.fromInt(3))))),
        "mixed" -> Json.arr(
          Json.fromInt(1),
          text("two"),
          Json.Null,
          Json.True,
          Json.arr(),
          Json.obj()
        )
      )
    )
    Seq(Json.obj(), Json.arr(), text("alone"), Json.fromInt(7), Json.True, Json.Null)
      .foreach(sameReadBack)
  }

  test("numbers are read with every digit they have") {
    val numbers = Seq(
      Json.fromInt(0),
      Json.fromInt(-1),
      Json.fromLong(Long.MaxValue),
      Json.fromLong(Long.MinValue),
      Json.fromBigInt(BigInt("123456789012345678901234567890")),
      Json.fromBigDecimal(BigDecimal("0.01")),
      Json.fromBigDecimal(BigDecimal("-12345.678900")),
      Json.fromBigDecimal(BigDecimal("0.000000000000000000000000000001")),
      Json.fromBigDecimal(BigDecimal("1E+30")),
      Json.fromDoubleOrNull(1.5e-7),
      Json.fromDoubleOrNull(6.02214076e23)
    )
    numbers.foreach(sameReadBack)
    sameReadBack(Json.fromValues(numbers))
    // A number, not the text of one, and the other way round.
    StoredJson.parse("""{"a": 10, "b": "10"}""").value shouldBe
      Json.obj("a" -> Json.fromInt(10), "b" -> text("10"))
  }

  test("text that YAML would read as something else is read as the text it is") {
    val looksLike = Seq(
      "yes",
      "no",
      "on",
      "off",
      "true",
      "false",
      "null",
      "~",
      "Null",
      "TRUE",
      "10",
      "0x1F",
      "0o17",
      "1e3",
      ".5",
      "-.inf",
      ".nan",
      "2026-11-18",
      "2026-11-18T09:10:25Z",
      "*alias",
      "&anchor",
      "!tag",
      "!!str x",
      "- item",
      "? key",
      "key: value",
      "a # comment",
      "#",
      "{",
      "}",
      "[",
      "]",
      ",",
      "{a: 1}",
      "[1, 2]",
      "|",
      ">",
      "%YAML 1.2",
      "---",
      "...",
      "'single'",
      "\"double\"",
      "back\\slash",
      "a/b",
      " leading",
      "trailing ",
      "  ",
      "@at",
      "`tick"
    )
    looksLike.foreach(value => sameReadBack(text(value)))
    sameReadBack(Json.fromValues(looksLike.map(text)))
    // And as keys.
    sameReadBack(
      Json.fromJsonObject(JsonObject.fromIterable(looksLike.map(key => key -> text(key))))
    )
  }

  test("every character that JSON has in a string is read back, raw or escaped") {
    val controls = (0x01 to 0x1f).map(_.toChar).mkString
    val strings = Seq(
      controls,
      "tab\there, line\nbreak, return\rhere, quote\" and \\ and /",
      // What YAML does not take raw, and PostgreSQL writes raw: DEL, C1, its line breaks, a BOM.
      "del\u007f",
      (0x80 to 0x9f).map(_.toChar).mkString("c1[", "", "]"),
      "nel\u0085 ls  ps  bom﻿ nbsp ",
      "￾ and ￿",
      "áéíóú ñ ç ß — € 你好 мир עברית العربية",
      "beyond the first plane: 😀 𝄞 𠜎",
      "\u007f",
      "ends in a backslash \\",
      "\\u0041 is six characters, not A"
    )
    strings.foreach(value => sameReadBack(text(value)))
    sameReadBack(Json.fromJsonObject(JsonObject.fromIterable(strings.map(key => key -> text(key)))))
  }

  test("a snapshot of configuration, as PostgreSQL writes it, is read back") {
    val asJsonb =
      """{"source": {"id": "sales.orders", "keys": ["order_id"], "load": {"mode": "incremental", "watermark": {"column": "updated_at", "lookback": "10m"}}, "quality": {"max_rejected_ratio": 0.01}, "enabled": true}, "profile": {"spark": {"settings": {"spark.sql.shuffle.partitions": "4"}}}, "schema_version": 1}"""

    val read = StoredJson.parse(asJsonb).value
    read.hcursor
      .downField("source")
      .downField("load")
      .downField("watermark")
      .get[String]("column") shouldBe
      Right("updated_at")
    read.hcursor
      .downField("source")
      .downField("quality")
      .get[BigDecimal]("max_rejected_ratio") shouldBe
      Right(BigDecimal("0.01"))
    read.hcursor
      .downField("profile")
      .downField("spark")
      .downField("settings")
      .get[String]("spark.sql.shuffle.partitions") shouldBe
      Right("4")
    read.hcursor.get[Int]("schema_version") shouldBe Right(1)
  }

  test("a document is read as deep as its limit, and no deeper: refused, with no overflow") {
    def nested(depth: Int): String = "[" * depth + "]" * depth
    def objects(depth: Int): String = """{"a":""" * depth + "1" + "}" * depth

    StoredJson.parse(nested(StoredJson.Limits.maxDepth)).isRight shouldBe true
    StoredJson.parse(nested(StoredJson.Limits.maxDepth + 1)).left.value should include("deep")
    StoredJson.parse(objects(StoredJson.Limits.maxDepth + 1)).left.value should include("deep")
    // What a parser with no limit on depth dies of.
    StoredJson.parse(nested(200000)).left.value should include("deep")
    StoredJson.parse(objects(200000)).left.value should include("deep")
  }

  test("a document is read up to its size, and one beyond it is refused before it is read") {
    val small = SafeYaml.Limits(maxCodePoints = 64, maxDepth = 8, maxAliases = 0)

    StoredJson.parse(s""""${"x" * 60}"""", small).isRight shouldBe true
    StoredJson.parse(s""""${"x" * 70}"""", small).left.value should include("72")
    // The limit of a stored document is above that of a file of configuration.
    StoredJson.Limits.maxCodePoints should be > SafeYaml.Limits.Default.maxCodePoints
    val large =
      Json.fromValues((1 to 20000).map(i => text(s"landing/sales/returns/returns-$i.csv")))
    large.noSpaces.length should be > SafeYaml.Limits.Default.maxCodePoints
    StoredJson.parse(large.noSpaces).value shouldBe large
  }

  test("a key of an object is 1,022 characters at most, as PostgreSQL writes it") {
    // As a jsonb is written: on one line, a space after the colon.
    def stored(length: Int): String = s"""{"${"k" * length}": 1}"""
    def keyed(length: Int): Json = Json.obj(("k" * length) -> Json.fromInt(1))

    StoredJson.parse(stored(1022)).value shouldBe keyed(1022)
    // YAML takes no longer key written as JSON writes it: nothing is read, and nothing wrong is.
    Seq(1023, 1024, 5000).foreach { length =>
      withClue(s"a key of $length: ")(StoredJson.parse(stored(length)).isLeft shouldBe true)
    }
    StoredJson.readable(keyed(1022)) shouldBe Right(())
    StoredJson.readable(keyed(1023)).isLeft shouldBe true
    // Well within it, a key is read however the document is written; a value has no such limit.
    sameReadBack(keyed(1000))
    sameReadBack(Json.obj("k" -> text("v" * 100000)))
  }

  test("what could not be read back is known before it is stored") {
    StoredJson.readable(
      Json.obj("source" -> Json.obj("keys" -> Json.arr(text("order_id"))))
    ) shouldBe
      Right(())
    StoredJson.readable(text("a\ufffe\u007f\u2028b")) shouldBe Right(())

    StoredJson.readable(Json.obj(("k" * 2000) -> Json.Null)).isLeft shouldBe true
    val deep =
      (1 to StoredJson.Limits.maxDepth + 1).foldLeft(Json.Null)((inner, _) => Json.arr(inner))
    StoredJson.readable(deep).left.value should include("deep")
  }

  test("what is not a document is refused, and the reason does not repeat it") {
    val notDocuments = Seq(
      "",
      "{",
      """{"a": 1""",
      """{"a": 1}}""",
      """{"a": 1, "a": 2}""",
      """["unterminated]""",
      """{"a": 1} {"b": 2}""",
      // YAML's own, which JSON has not: nothing stored has them, and none is taken.
      """{"a": &x 1, "b": *x}""",
      """{"a": !!binary "AAAA"}"""
    )
    notDocuments.foreach { document =>
      withClue(s"[$document] ")(StoredJson.parse(document).isLeft shouldBe true)
    }
    val secret = "s3cr3t-in-a-document"
    StoredJson.parse(s"""{"password": "$secret", """).left.value should not include secret
  }
}
