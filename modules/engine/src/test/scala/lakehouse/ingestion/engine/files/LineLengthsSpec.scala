package lakehouse.ingestion.engine.files

import java.io.{ByteArrayInputStream, InputStream, SequenceInputStream}
import java.nio.charset.StandardCharsets.UTF_8

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class LineLengthsSpec extends AnyFunSuite with Matchers {

  private def of(text: String): InputStream = new ByteArrayInputStream(text.getBytes(UTF_8))

  /** Every size of buffer that a look may have, down to one byte. */
  private val Sizes = Seq(1, 2, 3, 7, 8, 64, FileBytes.BufferSize)

  private def first(text: String, bound: Int): Seq[Option[Long]] =
    Sizes.map(size => LineLengths.firstTooLong(of(text), bound, size)).distinct

  test("a line as long as the bound is within it, and one character more is the line found") {
    first("abc\nabcd\n", 4) shouldBe Seq(None)
    first("abc\nabcde\nab\n", 4) shouldBe Seq(Some(2L))
    first("abcde", 4) shouldBe Seq(Some(1L))
    first("", 0) shouldBe Seq(None)
    first("\n\n\n", 0) shouldBe Seq(None)
  }

  test("a line ends where Spark's reader of text ends it: at LF, at CRLF and at CR alone") {
    // Each line of these is 3 characters: none is longer than 3, and the fourth is the first
    // longer than 2, whatever ends the ones before it.
    Seq("aaa\nbbb\nccc\nddd", "aaa\r\nbbb\r\nccc\r\nddd", "aaa\rbbb\rccc\rddd").foreach { text =>
      first(text, 3) shouldBe Seq(None)
      first(text.replace("ddd", "dddd"), 3) shouldBe Seq(Some(4L))
    }
    // CR CR is two ends, with an empty line between them; CRLF is one.
    first("a\r\rbbbb", 3) shouldBe Seq(Some(3L))
    first("a\r\nbbbb", 3) shouldBe Seq(Some(2L))
    first("a\n\r\nbbbb", 3) shouldBe Seq(Some(3L))
  }

  test("a character is counted as the units of UTF-16 that it is, from its bytes of UTF-8") {
    // ñ is two bytes, € three: one unit each. 😀 is four bytes, a pair of surrogates: two.
    first("ñ€ñ€", 4) shouldBe Seq(None)
    first("ñ€ñ€ñ", 4) shouldBe Seq(Some(1L))
    first("😀😀", 4) shouldBe Seq(None)
    first("😀😀a", 4) shouldBe Seq(Some(1L))
    // As Java counts them, for every line of a text.
    val text = "a😀ñ€\nxyz😀😀\n€€€€€"
    text.split('\n').map(_.length).toSeq shouldBe Seq(5, 7, 5)
    first(text, 7) shouldBe Seq(None)
    first(text, 6) shouldBe Seq(Some(2L))
  }

  test("the look stops at the first line too long, and reads no further") {
    // What follows the line too long is a stream that fails if it is read.
    val poisoned = new InputStream {
      override def read(): Int = throw new IllegalStateException("read past the line")
      override def read(b: Array[Byte], off: Int, len: Int): Int = read()
    }
    def stream: InputStream = new SequenceInputStream(of("ok\n" + "x" * 100), poisoned)
    LineLengths.firstTooLong(stream, bound = 10, size = 8) shouldBe Some(2L)
  }

  test("what is said of a line too long is its number, and what the file looks like") {
    LineLengths.why(1, 1048576) shouldBe
      "line_too_long: its first line is longer than 1048576 characters (units of UTF-16), the " +
      "longest that a line of JSON may be: it looks like one document of JSON, as a list " +
      "written on one line, and not JSON Lines, one record to a line"
    LineLengths.why(3, 1048576) shouldBe
      "line_too_long: its line 3 is longer than 1048576 characters (units of UTF-16), the " +
      "longest that a line of JSON may be: a record of JSON is one line (JSON Lines)"
  }
}
