package lakehouse.ingestion.engine.files

import java.io.InputStream
import java.nio.charset.StandardCharsets.{ISO_8859_1, UTF_8}
import java.nio.charset.{Charset, CodingErrorAction}
import java.nio.{ByteBuffer, CharBuffer}

import scala.util.control.NonFatal

import lakehouse.ingestion.core.config.CsvDialect
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration

/**
 * The bytes of a file, looked at for the first that is not of the encoding that its source says
 * (ADR-0024 §4 and §5).
 *
 * Spark's readers give U+FFFD for a byte that is not of the encoding that they read by, and say
 * nothing: no record is set apart, and nothing tells that character from one that the source
 * wrote (`SparkCsvDialectContractSpec`). So every byte of a file is decoded once more, by a
 * decoder of the JDK that reports what it cannot decode, which are the bytes that Spark's
 * reader, which decodes with the JDK too, replaces.
 *
 * Two things that no decoder refuses are found by the bytes themselves (§5):
 *
 *  - under ISO-8859-1, a byte of 0x80 to 0x9F: a control character there, which a text does
 *    not have, and the euro or a quote of windows-1252, which a spreadsheet writes;
 *  - the mark of the order of bytes of UTF-8 at the start of a file whose source says an
 *    encoding of one byte to a character: it is a file of UTF-8.
 *
 * '''Nothing of what a file holds is said''': where the byte is, by its offset and its line,
 * and never the byte, which may be part of a value.
 */
object FileBytes {

  /** What is wrong with a byte, or with the start of a file. */
  sealed trait What
  object What {

    /** A decoder of the encoding refuses it: it is malformed, or the encoding has no such byte. */
    case object NotOfEncoding extends What

    /** One of 0x80 to 0x9F, under ISO-8859-1. */
    case object Control extends What

    /** The file starts with the mark of UTF-8, under an encoding of one byte to a character. */
    case object MarkOfUtf8 extends What
  }

  /** The mark of the order of bytes that a file starts with. */
  sealed abstract class Mark(val encoding: String)
  object Mark {
    case object Utf8 extends Mark("UTF-8")
    case object Utf16 extends Mark("UTF-16")
  }

  /**
   * The first thing of a file that is not of its encoding.
   *
   * @param offset where it is in what the file holds, once decompressed, from 0
   * @param line the line that it is in, from 1, as an editor counts them: a line ends in LF,
   *   in CR, or in both, whether or not it is between the quotes of a value
   * @param mark the mark of the order of bytes that the file starts with, if it has one
   */
  final case class Found(what: What, offset: Long, line: Long, mark: Option[Mark])

  /** What a file whose bytes are not of its encoding is recorded as, before why. */
  val NotOfItsEncoding = "not_of_its_encoding"

  /** How many bytes are read at a time. */
  val BufferSize: Int = 64 * 1024

  /** The longest sequence of bytes of a character is 4, and a buffer holds one and more. */
  private val SmallestBuffer = 8

  /**
   * The first byte of a stream that is not of an encoding, if it has one. Every byte is read,
   * `size` at a time; the stream is left open.
   *
   * @param charset one of those of `CsvDialect.Encoding`, in all of which the bytes of CR and
   *   of LF are those characters and no part of another
   */
  def look(in: InputStream, charset: Charset, size: Int = BufferSize): Option[Found] = {
    require(size >= SmallestBuffer, s"a buffer of $SmallestBuffer bytes at the least")
    val decoder = charset
      .newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
    val controls = charset == ISO_8859_1
    val array = new Array[Byte](size)
    val bytes = ByteBuffer.wrap(array)
    val chars = CharBuffer.allocate(size)

    // Where the first byte of the buffer is in the stream, and the ends of lines before the
    // byte that is looked at: a CR and the LF after it are one.
    var base = 0L
    var ends = 0L
    var afterCr = false
    var mark: Option[Mark] = None
    var found: Option[Found] = None
    var ended = false

    while (found.isEmpty && !ended) {
      // After the bytes that the decoder left, of a character that is not whole yet.
      val left = bytes.position()
      val read = in.readNBytes(array, left, size - left)
      ended = read < size - left
      bytes.position(left + read)
      bytes.flip()
      if (base == 0L && left == 0) {
        mark = markOf(array, read)
        if (mark.contains(Mark.Utf8) && charset != UTF_8)
          found = Some(Found(What.MarkOfUtf8, 0L, 1L, mark))
      }

      var more = found.isEmpty
      while (more) {
        chars.clear()
        val from = bytes.position()
        val result = decoder.decode(bytes, chars, ended)
        // On an error, the buffer is at the first byte of what the decoder refuses.
        val to = bytes.position()
        var i = from
        while (i < to && found.isEmpty) {
          val byte = array(i) & 0xff
          if (controls && byte >= 0x80 && byte <= 0x9f)
            found = Some(Found(What.Control, base + i, ends + 1, mark))
          else if (byte == '\r') {
            ends += 1
            afterCr = true
          } else {
            if (byte == '\n' && !afterCr) ends += 1
            afterCr = false
          }
          i += 1
        }
        if (found.isEmpty && result.isError)
          found = Some(Found(What.NotOfEncoding, base + to, ends + 1, mark))
        more = found.isEmpty && result.isOverflow
      }
      base += bytes.position()
      bytes.compact()
    }
    found
  }

  private def markOf(bytes: Array[Byte], length: Int): Option[Mark] = {
    def starts(mark: Int*): Boolean =
      length >= mark.size && mark.indices.forall(i => (bytes(i) & 0xff) == mark(i))
    if (starts(0xef, 0xbb, 0xbf)) Some(Mark.Utf8)
    else if (starts(0xff, 0xfe) || starts(0xfe, 0xff)) Some(Mark.Utf16)
    else None
  }

  /**
   * Why a file is not of its encoding, as a record of it keeps it: where, and never what.
   *
   * @param compressed whether the offset is of what the file holds once decompressed
   */
  def why(found: Found, encoding: CsvDialect.Encoding, compressed: Boolean): String = {
    val of = if (compressed) "what the file holds, decompressed" else "the file"
    val where = s"the byte at offset ${found.offset} of $of, in line ${found.line},"
    val said = found.what match {
      case What.MarkOfUtf8 =>
        s"it starts with the mark of the order of bytes of UTF-8, and its source says " +
          s"${encoding.name}: it is a file of UTF-8"
      case What.Control =>
        s"$where is a control character of ${encoding.name} (0x80 to 0x9F), which a text does " +
          s"not have: the file may be of ${CsvDialect.Encoding.Windows1252.name}, which has " +
          "the euro and its quotes there"
      case What.NotOfEncoding =>
        val marked = found.mark
          .filter(_.encoding != encoding.name)
          .fold("")(mark =>
            s": the file starts with the mark of the order of bytes of ${mark.encoding}"
          )
        s"$where is not of ${encoding.name}$marked"
    }
    s"$NotOfItsEncoding: $said"
  }

  /**
   * Each of some files, with why it is not of an encoding, if it is not: a job of Spark, a task
   * for each file, which reads it with the codec that Spark's readers read it with, by the
   * ending of its name. The job is one of the thread that asks, and is stopped with its others
   * (`RunJobs`).
   *
   * A failure of a task is the job's: a file that cannot be opened or read fails the look, and
   * is none that is not of its encoding.
   *
   * @param locations each file, as Spark is given it to read
   * @return what was found of each, in their order
   */
  def notOfEncoding(
      spark: SparkSession,
      locations: Vector[String],
      encoding: CsvDialect.Encoding
  ): Vector[Option[String]] =
    if (locations.isEmpty) Vector.empty
    else {
      val hadoop = new SerializableConfiguration(spark.sessionState.newHadoopConf())
      val name = encoding.name
      val found = spark.sparkContext
        .parallelize(locations.zipWithIndex, locations.size)
        .map { case (location, index) => index -> ofFile(location, name, hadoop.value) }
        .collect()
        .toMap
      locations.indices.toVector.map(found)
    }

  private def ofFile(location: String, name: String, hadoop: Configuration): Option[String] = {
    val encoding = CsvDialect.Encoding.All
      .find(_.name == name)
      .getOrElse(throw new IllegalArgumentException(s"$name is no encoding of a source"))
    val (in, compressed) = open(location, hadoop)
    try look(in, encoding.charset).map(why(_, encoding, compressed))
    finally in.close()
  }

  /**
   * A file as Spark's readers read it: with the codec of the ending of its name, if it has one,
   * which gives what the file holds, decompressed. The stream is the caller's to close.
   *
   * @return the stream, and whether the file is compressed
   */
  private[files] def open(location: String, hadoop: Configuration): (InputStream, Boolean) = {
    val path = new HadoopPath(location)
    val codec = Option(new CompressionCodecFactory(hadoop).getCodec(path))
    val opened = path.getFileSystem(hadoop).open(path)
    val in: InputStream =
      try codec.fold[InputStream](opened)(_.createInputStream(opened))
      catch {
        case NonFatal(failed) =>
          opened.close()
          throw failed
      }
    (in, codec.nonEmpty)
  }
}
