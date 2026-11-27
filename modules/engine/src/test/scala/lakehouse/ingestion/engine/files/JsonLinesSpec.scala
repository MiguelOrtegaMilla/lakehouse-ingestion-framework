package lakehouse.ingestion.engine.files

import com.fasterxml.jackson.core.json.JsonReadFeature
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * A line of a file of JSON as the read reads it (P8.8): what is a record, with which values,
 * what is set apart and why. That the values are those of Spark's reader of files is held by
 * `FileSourceConnectorSpec`, against the reader itself.
 */
final class JsonLinesSpec extends AnyFunSuite with Matchers with OptionValues {

  import JsonLines.Line

  private def fields(line: String): Vector[(String, String)] = JsonLines.of(line) match {
    case Line.Record(fields) => fields
    case other => fail(s"not a record: $other")
  }

  test("one whole object with each field of its first level once is a record, with its fields") {
    fields("""{"id":"1","name":"ann"}""") shouldBe Vector("id" -> "1", "name" -> "ann")
    // In the order written; spaces around it and between its tokens are nothing.
    fields("""  { "b" : 1 , "a" : "x" }  """) shouldBe Vector("b" -> "1", "a" -> "x")
    fields("{}") shouldBe Vector.empty
    // A name escaped is the name that it stands for, as Spark reads it.
    fields("""{"ab":1}""") shouldBe Vector("ab" -> "1")
  }

  test("a value is what a string says, none for null, and otherwise the text it was written as") {
    fields("""{"s":"a\"bé","n":null,"e":""}""") shouldBe
      Vector("s" -> "a\"bé", "n" -> null, "e" -> "")
    // Numbers, as written: neither a zero nor an exponent is taken off.
    fields("""{"a":10.50,"b":1e2,"c":-0,"d": -1.0e-3 ,"e":12345678901234567890123}""") shouldBe
      Vector(
        "a" -> "10.50",
        "b" -> "1e2",
        "c" -> "-0",
        "d" -> "-1.0e-3",
        "e" -> "12345678901234567890123"
      )
    // An object and a list, whole, with their spaces.
    fields("""{"o": { "a" : 1.50 , "b":[1, 2] } ,"l":[ 1.50 ]}""") shouldBe
      Vector("o" -> """{ "a" : 1.50 , "b":[1, 2] }""", "l" -> "[ 1.50 ]")
    fields("""{"t":true,"f":false,"x":NaN}""") shouldBe
      Vector("t" -> "true", "f" -> "false", "x" -> "NaN")
  }

  test("a blank line is nothing, as Spark's reader passes over it") {
    JsonLines.of("") shouldBe Line.Blank
    JsonLines.of(" \t ") shouldBe Line.Blank
  }

  test("text after a whole object is set apart, with the column where it starts") {
    JsonLines.of("""{"id":"4"} trailing""") shouldBe Line.TrailingText(12)
    JsonLines.of("""{"id":"4"} {"id":"5"}""") shouldBe Line.TrailingText(12)
    JsonLines.of("""{"id":"4"},""") shouldBe Line.TrailingText(11)
    JsonLines.of("""{"id":"4"}{""") shouldBe Line.TrailingText(11)
    // A comment is no part of JSON as Spark reads it: what Spark would drop without a word.
    JsonLines.of("""{"id":"4"} // a note""") shouldBe Line.TrailingText(12)
    JsonLines.detail(Line.TrailingText(12)) shouldBe Some("text after the object, from column 12")
  }

  test("a field twice at the first level is set apart, by its name and no value") {
    JsonLines.of("""{"id":"1","id":"2"}""") shouldBe Line.DuplicateField("id")
    // The first name written twice, and the field twice before the text after it.
    JsonLines.of("""{"a":1,"b":2,"b":3,"a":4} x""") shouldBe Line.DuplicateField("b")
    JsonLines.detail(Line.DuplicateField("id")) shouldBe
      Some("the field 'id' twice, at the first level of the record")
    // A long name is cut in the detail; the record keeps it whole.
    JsonLines.detail(Line.DuplicateField("n" * 300)).getOrElse("").length should be < 300
  }

  test("a long name is cut by its characters in the detail, and never inside one") {
    // 201 characters, the 200th of them outside the first plane: a pair of surrogates.
    val pair = new String(Character.toChars(0x1f600))
    val detail = JsonLines.detail(Line.DuplicateField("a" * 199 + pair + "b")).value
    detail shouldBe s"the field '${"a" * 199}$pair…' twice, at the first level of the record"
    detail.codePoints().toArray.exists(Character.getType(_) == Character.SURROGATE) shouldBe false
    // A name of 200 characters is kept whole.
    JsonLines.detail(Line.DuplicateField("a" * 199 + pair)).value should include(s"$pair'")
  }

  test("a field twice inside an object of the record is kept: Bronze keeps the object as text") {
    fields("""{"id":"1","customer":{"name":"a","name":"b"}}""") shouldBe
      Vector("id" -> "1", "customer" -> """{"name":"a","name":"b"}""")
    // Fields that differ only in their case are two names here: the read refuses them (P9).
    fields("""{"id":"1","ID":"2"}""").map(_._1) shouldBe Vector("id", "ID")
  }

  test("a line that is no whole object is no record, and is set apart as one") {
    Seq(
      "not json",
      "5",
      "null",
      "\"a string\"",
      """[{"id":"1"},{"id":"2"}]""",
      "[]",
      """[{"id":"1"}] x""",
      // The lines of a record written over several lines.
      "{",
      """  "id": "1",""",
      "}",
      // A broken object, a field twice in it or not.
      """{"id":"1",""",
      """{"id":"1","id":"2",""",
      """{"id": }"""
    ).foreach(line => withClue(line)(JsonLines.of(line) shouldBe Line.NotARecord))
  }

  test("the parser is Spark's own: what Spark reads as a record is one here") {
    // Quotes of either kind, and numbers that are no number, as Spark's reader takes them.
    fields("""{'id':'1'}""") shouldBe Vector("id" -> "1")
    JsonLines.of("""{'id':'1'} x""") shouldBe Line.TrailingText(12)
    fields("""{"n":NaN}""") shouldBe Vector("n" -> "NaN")
    // And what it does not take, is not: names without quotes, leading zeros.
    JsonLines.of("""{id:"1"}""") shouldBe Line.NotARecord
    JsonLines.of("""{"n":01}""") shouldBe Line.NotARecord

    // Each feature of the parser as Spark's reader has it, with no option of a source set.
    val spark = new JSONOptions(Map.empty[String, String], "UTC", "").buildJsonFactory()
    def enabled(feature: JsonReadFeature) = spark.isEnabled(feature.mappedFeature())
    enabled(JsonReadFeature.ALLOW_SINGLE_QUOTES) shouldBe true
    enabled(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS) shouldBe true
    enabled(JsonReadFeature.ALLOW_JAVA_COMMENTS) shouldBe false
    enabled(JsonReadFeature.ALLOW_UNQUOTED_FIELD_NAMES) shouldBe false
  }
}
