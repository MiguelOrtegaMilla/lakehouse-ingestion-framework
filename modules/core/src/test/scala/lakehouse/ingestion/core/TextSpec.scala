package lakehouse.ingestion.core

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class TextSpec extends AnyFunSuite with Matchers {

  /** A character outside the first plane: two units of UTF-16, a pair of surrogates. */
  private val Pair = new String(Character.toChars(0x1f600))

  private def halves(text: String): Seq[Int] =
    text
      .codePoints()
      .toArray
      .toSeq
      .filter(codePoint => Character.getType(codePoint) == Character.SURROGATE)

  test("a text no longer than the bound is kept whole") {
    Text.upTo("", 3) shouldBe ""
    Text.upTo("abc", 3) shouldBe "abc"
    Text.upTo("abc", 10) shouldBe "abc"
    // Three characters, four units of UTF-16: no longer than three.
    Text.upTo(s"ab$Pair", 3) shouldBe s"ab$Pair"
  }

  test("a longer text is cut to its first characters, counted as Unicode counts them") {
    Text.upTo("abcdef", 3) shouldBe "abc"
    Text.upTo(s"$Pair$Pair$Pair$Pair", 2) shouldBe s"$Pair$Pair"
    Text.upTo("abc", 0) shouldBe ""
  }

  test("a cut where a pair of surrogates is keeps the pair whole, or leaves it out whole") {
    // `take(3)` keeps "ab" and half of the pair.
    halves(s"ab${Pair}c".take(3)) should have size 1

    Text.upTo(s"ab${Pair}c", 3) shouldBe s"ab$Pair"
    Text.upTo(s"ab${Pair}c", 2) shouldBe "ab"
    halves(Text.upTo(s"ab${Pair}c", 3)) shouldBe empty
  }

  test("a bound below 0 is a mistake of the caller") {
    an[IllegalArgumentException] should be thrownBy Text.upTo("abc", -1)
  }
}
