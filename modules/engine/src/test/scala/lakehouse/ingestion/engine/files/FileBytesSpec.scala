package lakehouse.ingestion.engine.files

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets.UTF_8

import lakehouse.ingestion.core.config.CsvDialect.Encoding
import lakehouse.ingestion.engine.files.FileBytes.{Found, Mark, What}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers

/**
 * The look at the bytes of a file for the first that is not of its encoding (P8.9c, ADR-0024 §4
 * and §5), on bytes written one by one: what is found, where, and what is not. That the bytes
 * found are those that Spark's readers replace is held by `SparkCsvDialectContractSpec`, and
 * that the look is of the files of a batch, by `FileSourceConnectorSpec`.
 */
final class FileBytesSpec extends AnyFunSuite with Matchers with OptionValues {

  /** Bytes: a string as UTF-8, and a number as the byte that it is. */
  private def bytes(parts: Any*): Array[Byte] = parts.flatMap {
    case text: String => text.getBytes(UTF_8).toSeq
    case byte: Int => Seq(byte.toByte)
    case other => throw new IllegalArgumentException(s"$other is not bytes")
  }.toArray

  /** What the look finds, which is the same whatever the size of its buffer. */
  private def look(encoding: Encoding)(parts: Any*): Option[Found] = {
    val content = bytes(parts: _*)
    val found = Seq(8, 9, 16, 4096, FileBytes.BufferSize).map { size =>
      FileBytes.look(new ByteArrayInputStream(content), encoding.charset, size)
    }
    withClue(s"by the size of the buffer: $found") {
      found.distinct should have size 1
    }
    found.head
  }

  private def notOf(offset: Long, line: Long, mark: Option[Mark] = None): Option[Found] =
    Some(Found(What.NotOfEncoding, offset, line, mark))

  test("a file of its encoding has nothing found, whatever it holds") {
    look(Encoding.Utf8)() shouldBe None
    look(Encoding.Utf8)("id,name\n1,Peña €\n2,日本語 😀\n") shouldBe None
    // The mark of UTF-8, under UTF-8, and a U+FFFD that the source wrote (EF BF BD).
    look(Encoding.Utf8)(0xef, 0xbb, 0xbf, "id\n1\n") shouldBe None
    look(Encoding.Utf8)("id\n", 0xef, 0xbf, 0xbd, "\n") shouldBe None
    // A NUL is of every encoding.
    look(Encoding.Utf8)("i", 0x00, "d\n") shouldBe None
    look(Encoding.Iso88591)("id,a", 0xf1, "o\n1,ni", 0xf1, "o ", 0xa0, 0xff, "\n") shouldBe None
    look(Encoding.Windows1252)("id\n10 ", 0x80, " ", 0x93, "x", 0x94, 0xf1, "\n") shouldBe None
    // Text long enough for a character to be cut by the end of every buffer.
    look(Encoding.Utf8)((1 to 3000).map(i => s"$i,ñ€😀\r\n").mkString) shouldBe None
  }

  test("a byte that is not UTF-8 is found, by its offset and its line") {
    // A byte of ISO-8859-1, in the third line.
    look(Encoding.Utf8)("id,name\n1,ann\n2,ni", 0xf1, "o\n") shouldBe notOf(18, 3)
    look(Encoding.Utf8)(0xf1) shouldBe notOf(0, 1)
    // A continuation with nothing before it, and one that does not come.
    look(Encoding.Utf8)("ab", 0xb1, "\n") shouldBe notOf(2, 1)
    look(Encoding.Utf8)("ab", 0xc3, "x\n") shouldBe notOf(2, 1)
    // A character longer than it needs to be, and half of a pair of UTF-16.
    look(Encoding.Utf8)("a", 0xc0, 0xaf) shouldBe notOf(1, 1)
    look(Encoding.Utf8)("a\n", 0xed, 0xa0, 0x80) shouldBe notOf(2, 2)
    // A character that the file ends in the middle of.
    look(Encoding.Utf8)("id\n1\n", 0xe2, 0x82) shouldBe notOf(5, 3)
    // The first is the one that is said.
    look(Encoding.Utf8)("a\n", 0xf1, "\n", 0xf1, "\n") shouldBe notOf(2, 2)
  }

  test("a byte far into a file is found where it is, across the buffers") {
    val lines = (1 to 5000).map(i => s"$i,ñandú €\n").mkString
    val before = lines.getBytes(UTF_8).length.toLong
    look(Encoding.Utf8)(lines, "x,", 0xff, "\n") shouldBe notOf(before + 2, 5001)
  }

  test("a line ends in LF, in CR, or in both, which are one end") {
    look(Encoding.Utf8)("a\r\nb\r\n", 0xf1) shouldBe notOf(6, 3)
    look(Encoding.Utf8)("a\rb\r", 0xf1) shouldBe notOf(4, 3)
    look(Encoding.Utf8)("a\n\nb\n", 0xf1) shouldBe notOf(5, 4)
    look(Encoding.Utf8)("a\r\rb\n\r", 0xf1) shouldBe notOf(6, 5)
    // The byte is in the line that it ends, if it is just before its end.
    look(Encoding.Utf8)("a\nb", 0xf1, "\r\n") shouldBe notOf(3, 2)
    // An end of line between the quotes of a value is one as well: the line is the file's.
    look(Encoding.Utf8)("id,v\n1,\"a\nb", 0xf1, "\"\n") shouldBe notOf(11, 3)
  }

  test("the five bytes that windows-1252 does not define are found, and no other") {
    val undefined = Seq(0x81, 0x8d, 0x8f, 0x90, 0x9d)
    (0x00 to 0xff).foreach { byte =>
      withClue(f"0x$byte%02X: ") {
        look(Encoding.Windows1252)("a\n", byte) shouldBe
          (if (undefined.contains(byte)) notOf(2, 2) else None)
      }
    }
  }

  test("under ISO-8859-1, a byte of 0x80 to 0x9F is found, and no other") {
    (0x00 to 0xff).foreach { byte =>
      withClue(f"0x$byte%02X: ") {
        look(Encoding.Iso88591)("a\nb", byte) shouldBe
          (if (byte >= 0x80 && byte <= 0x9f) Some(Found(What.Control, 3, 2, None)) else None)
      }
    }
    // The euro of a spreadsheet, in its second record.
    look(Encoding.Iso88591)("id;importe\r\n1;10\r\n2;20 ", 0x80, "\r\n") shouldBe
      Some(Found(What.Control, 23, 3, None))
  }

  test("the mark of UTF-8 under an encoding of one byte is found, at the start of a file alone") {
    Seq(Encoding.Iso88591, Encoding.Windows1252).foreach { encoding =>
      look(encoding)(0xef, 0xbb, 0xbf, "id\n1\n") shouldBe
        Some(Found(What.MarkOfUtf8, 0, 1, Some(Mark.Utf8)))
      look(encoding)(0xef, 0xbb, 0xbf) shouldBe Some(Found(What.MarkOfUtf8, 0, 1, Some(Mark.Utf8)))
      // Those three characters after the start are characters, and so are two of them.
      look(encoding)("id\n", 0xef, 0xbb, 0xbf, "\n") shouldBe None
      look(encoding)(0xef, 0xbb, "id\n") shouldBe None
    }
  }

  test("the mark that a file starts with is said with what is found") {
    // A file of UTF-16 said to be UTF-8: its mark is not of UTF-8.
    look(Encoding.Utf8)(0xff, 0xfe, "i", 0x00, "d", 0x00) shouldBe notOf(0, 1, Some(Mark.Utf16))
    look(Encoding.Utf8)(0xfe, 0xff, 0x00, "i", 0x00, "d") shouldBe notOf(0, 1, Some(Mark.Utf16))
    look(Encoding.Utf8)(0xef, 0xbb, 0xbf, "id\n", 0xf1) shouldBe notOf(6, 2, Some(Mark.Utf8))
    // With no mark, a file of UTF-16 of such characters is of UTF-8, with NULs.
    look(Encoding.Utf8)("i", 0x00, "d", 0x00) shouldBe None
    // Under an encoding of one byte, the mark of UTF-16 is two characters.
    look(Encoding.Windows1252)(0xff, 0xfe, "i", 0x00) shouldBe None
  }

  test("why says where, the encoding and what to try, and never a byte of the file") {
    val utf8 = look(Encoding.Utf8)("id\n1,ni", 0xf1, "o\n").value
    FileBytes.why(utf8, Encoding.Utf8, compressed = false) shouldBe
      "not_of_its_encoding: the byte at offset 7 of the file, in line 2, is not of UTF-8"
    FileBytes.why(utf8, Encoding.Utf8, compressed = true) shouldBe
      "not_of_its_encoding: the byte at offset 7 of what the file holds, decompressed, in " +
      "line 2, is not of UTF-8"

    val utf16 = look(Encoding.Utf8)(0xff, 0xfe, "i", 0x00).value
    FileBytes.why(utf16, Encoding.Utf8, compressed = false) shouldBe
      "not_of_its_encoding: the byte at offset 0 of the file, in line 1, is not of UTF-8: the " +
      "file starts with the mark of the order of bytes of UTF-16"
    // Its own mark says nothing of why a file of UTF-8 has a byte that is not.
    val marked = look(Encoding.Utf8)(0xef, 0xbb, 0xbf, "id\n", 0xf1).value
    FileBytes.why(marked, Encoding.Utf8, compressed = false) shouldBe
      "not_of_its_encoding: the byte at offset 6 of the file, in line 2, is not of UTF-8"

    val control = look(Encoding.Iso88591)("id\n10 ", 0x80, "\n").value
    FileBytes.why(control, Encoding.Iso88591, compressed = false) shouldBe
      "not_of_its_encoding: the byte at offset 6 of the file, in line 2, is a control " +
      "character of ISO-8859-1 (0x80 to 0x9F), which a text does not have: the file may be of " +
      "windows-1252, which has the euro and its quotes there"

    val mark = look(Encoding.Windows1252)(0xef, 0xbb, 0xbf, "id\n").value
    FileBytes.why(mark, Encoding.Windows1252, compressed = false) shouldBe
      "not_of_its_encoding: it starts with the mark of the order of bytes of UTF-8, and its " +
      "source says windows-1252: it is a file of UTF-8"
  }

  test("a buffer too small for a character is refused") {
    an[IllegalArgumentException] should be thrownBy
      FileBytes.look(new ByteArrayInputStream(Array.emptyByteArray), UTF_8, 4)
  }
}
