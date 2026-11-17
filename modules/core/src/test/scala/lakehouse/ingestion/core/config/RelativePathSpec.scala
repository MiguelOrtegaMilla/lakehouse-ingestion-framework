package lakehouse.ingestion.core.config

import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The grammar of relative paths, with the corpus of ADR-0013 §2: every one is refused. */
final class RelativePathSpec extends AnyFunSuite with Matchers with EitherValues {

  test("a path of plain segments is accepted, as it was written") {
    Seq(
      "bronze/sales/orders",
      "silver/sales/orders",
      "orders",
      "landing/sales/2026/11/17",
      "raw/dt=2026-11-17/part-0001.csv",
      "a.b/c_d/e-f"
    ).foreach(path => withClue(path)(RelativePath.parse(path).value.value shouldBe path))

    RelativePath.parse("bronze/sales/orders").value.segments shouldBe
      Vector("bronze", "sales", "orders")
  }

  test("the malicious corpus is refused, every path of it") {
    val corpus = Seq(
      // ADR-0013 §2
      "../",
      "a/../../b",
      "/etc/passwd",
      "file:///etc",
      "s3a://other-bucket/x",
      "%2e%2e/",
      "..\\",
      "bronze/../../x",
      // and the rest of the family
      "..",
      ".",
      "./bronze",
      "bronze/./orders",
      "bronze//orders",
      "bronze/orders/",
      "",
      " ",
      "bronze/sales orders",
      "bronze\\sales",
      "C:\\lake\\bronze",
      "C:/lake/bronze",
      "~/lake",
      "bronze/orders\u0000",
      "bronze/%2e%2e/x",
      "bronze/..%2fx",
      "bronze/\u2025/x",
      "br\u043Enze/orders",
      "bronze/orders\n",
      "bronze/*/orders",
      "bronze/orders?x=1",
      "bronze/orders#x",
      "bronze/$HOME",
      // hidden to Spark and Delta, and somebody else's
      "bronze/sales/orders/_delta_log",
      "_quarantine/bronze/sales.orders",
      "bronze/.tmp/x",
      "bronze/...",
      "-rf/x",
      "=x"
    )

    corpus.foreach(path => withClue(s"[$path]")(RelativePath.parse(path).isLeft shouldBe true))
  }

  test("a refusal says what is wrong") {
    RelativePath.parse("").left.value shouldBe "is empty"
    RelativePath.parse("/etc/passwd").left.value shouldBe
      "is absolute: it must be relative to its root"
    RelativePath.parse("a/../../b").left.value shouldBe
      "has the segment '..': a path does not climb out of its root"
    RelativePath.parse("s3a://other-bucket/x").left.value shouldBe
      "has ':' (U+003A) at position 4: a path has no scheme and no drive, it is relative to its root"
    RelativePath.parse("%2e%2e/").left.value shouldBe
      "has '%' (U+0025) at position 1: a path is not URL-encoded"
    RelativePath.parse("..\\").left.value shouldBe
      "has '\\' (U+005C) at position 3: segments are separated by /"
    RelativePath.parse("bronze/orders/").left.value shouldBe
      "has an empty segment: no // and no / at the end"
    RelativePath.parse("bronze/./orders").left.value shouldBe
      "has the segment '.': a path is written one way"
    RelativePath.parse("bronze/orders\u0000").left.value shouldBe
      "has U+0000 NULL at position 14: a segment is letters of ASCII, digits, and _ - . ="
    RelativePath.parse("bronze/sales/orders/_delta_log").left.value should
      startWith("has the segment '_delta_log', which starts with '_'")
  }

  test("a segment and a path have a longest length") {
    RelativePath.parse("a" * 255).isRight shouldBe true
    RelativePath.parse("a" * 256).left.value shouldBe
      "has a segment of 256 bytes, and a segment is 255 at most"
    val long = Seq.fill(8)("a" * 127).mkString("/")
    long.length shouldBe 1023
    RelativePath.parse(long).isRight shouldBe true
    RelativePath.parse(long + "/b").left.value shouldBe
      "is 1025 bytes long, and a path is 1024 at most"
  }
}
