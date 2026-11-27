package lakehouse.ingestion.it.spark

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.charset.{
  Charset,
  CodingErrorAction,
  MalformedInputException,
  UnmappableCharacterException
}
import java.nio.file.{Files, Path}
import java.util.zip.GZIPOutputStream

import scala.util.{Try, Using}

import lakehouse.ingestion.it.support.LocalSparkSuite
import org.apache.spark.SparkThrowable
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrameReader, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What Spark 4.1's reader of CSV does with a dialect and an encoding, and what the decoders of
 * the JDK say of the bytes of a file: the facts that the dialect and the encoding of a source of
 * CSV are built on (discovery §17, P8.9; §20; ADR-0024).
 *
 * A read of the file connector has Spark read the files of a batch twice: it infers the columns
 * from the header of the first file (''the inference''), and then reads the rows with them and
 * keeps them apart from the files (`localCheckpoint`, ''the checkpoint''). The reads here are
 * made with the options of `FileSourceConnector` (a header, values over several lines, every
 * file read), with the delimiter, the quote, the escape and the encoding of each case, and no
 * code of the engine is in them: characterisation tests, as those of
 * `SparkUnreadableFileContractSpec` are. An upgrade that changes one of these facts fails here,
 * with the name of the assumption that it breaks (ADR-0014, upgrade procedure).
 *
 * What they show, and what ADR-0024 takes from them:
 *
 *  - Spark reads with seven charsets, and refuses windows-1252 unless its session may read with
 *    any charset of the JDK (`spark.sql.legacy.javaCharsets`, internal, off by default): the
 *    session of the application is to have it on, and a refusal of a charset is permanent;
 *  - a byte that is not of the encoding declared is replaced by U+FFFD, in a value or in a name,
 *    and nothing says so; a U+FFFD that the source wrote is read as itself. So the read cannot
 *    tell a replacement, and the bytes of each file are to be looked at by a decoder of the JDK
 *    that reports what it cannot decode;
 *  - every byte is a character of ISO-8859-1: a file of windows-1252 read as ISO-8859-1 has its
 *    euro and its quotes as control characters (U+0080 to U+009F), which no decoder refuses;
 *  - the mark of the order of bytes of UTF-8 is taken off, but a second one is kept, and in a
 *    file read in another encoding it is three characters at the start of the first name;
 *  - inside a quoted value, the end of line of the file is read as LF: a value of a file whose
 *    lines end in CRLF has LF where it had CRLF. A file of a spreadsheet, whose records end in
 *    CRLF and whose cells have LF, reads as written; one that mixes its ends of lines changes a
 *    value, or makes two records one, without a word;
 *  - a file read with values over several lines is read whole, by one task.
 */
final class SparkCsvDialectContractSpec extends AnyFunSuite with Matchers with LocalSparkSuite {

  import SparkCsvDialectContractSpec._

  /** A session that may read with any charset of the JDK, as the application's session does. */
  private lazy val javaCharsets: SparkSession = {
    val session = spark.newSession()
    session.conf.set(SQLConf.LEGACY_JAVA_CHARSETS.key, "true")
    session
  }

  // ---------------------------------------------------------------------------------------------
  // The reads, as the file connector makes them
  // ---------------------------------------------------------------------------------------------

  private def reader(session: SparkSession, dialect: Dialect): DataFrameReader = {
    val reader = session.read
      .option("ignoreCorruptFiles", "false")
      .option("ignoreMissingFiles", "false")
      .option("header", "true")
      .option("multiLine", "true")
      .option("sep", dialect.delimiter)
      .option("quote", dialect.quote)
      .option("escape", dialect.escape)
    dialect.encoding.fold(reader)(reader.option("encoding", _))
  }

  /** The columns of the files, as the connector infers them. */
  private def inferred(session: SparkSession, dialect: Dialect, files: Seq[Path]): Seq[String] =
    reader(session, dialect).csv(locations(files): _*).schema.fieldNames.toSeq

  /** The rows read with `columns`, each with the column of what could not be parsed, kept. */
  private def kept(
      session: SparkSession,
      dialect: Dialect,
      files: Seq[Path],
      columns: Seq[String]
  ): Seq[Seq[String]] =
    reader(session, dialect)
      .option("enforceSchema", "false")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", Unparsed)
      .schema(StructType((columns :+ Unparsed).map(StructField(_, StringType))))
      .csv(locations(files): _*)
      .localCheckpoint(eager = true)
      .collect()
      .toSeq
      .map(row => (0 until row.length).map(row.getString))

  /** A read at both of its moments, or the failure of the moment that failed. */
  private def attempt(dialect: Dialect, session: SparkSession)(
      files: Path*
  ): Either[Failure, Read] =
    caught(Stage.Inference)(inferred(session, dialect, files)).flatMap { columns =>
      caught(Stage.Checkpoint)(kept(session, dialect, files, columns)).map { rows =>
        Read(
          columns,
          rows.filter(_.last == null).map(_.init),
          rows.map(_.last).filter(_ != null)
        )
      }
    }

  /** A read that is to read. */
  private def read(dialect: Dialect = Rfc4180, session: SparkSession = spark)(
      files: Path*
  ): Read =
    attempt(dialect, session)(files: _*).fold(
      failure => fail(s"the read failed at the ${failure.stage}: ${failure.messages.head}"),
      identity
    )

  /** A read that is to fail. */
  private def failure(dialect: Dialect, session: SparkSession = spark)(
      files: Path*
  ): Failure =
    attempt(dialect, session)(files: _*).fold(identity, read => fail(s"the read read $read"))

  // ---------------------------------------------------------------------------------------------
  // Encodings
  // ---------------------------------------------------------------------------------------------

  test("a file of ISO-8859-1 is read with its letters, in its header and in a value over lines") {
    val latin1 = file(bytes("id,a", 0xf1, "o\n1,ni", 0xf1, "o\n2,\"a", 0xf1, "\no\"\n"))
    read(Rfc4180.in("ISO-8859-1"))(latin1) shouldBe Read(
      Seq("id", "año"),
      Seq(Seq("1", "niño"), Seq("2", "añ\no"))
    )
  }

  test("Spark refuses windows-1252 unless its session may read any charset of the JDK") {
    val cp1252 = Rfc4180.in("windows-1252")
    val euro = file(bytes("id,v\n1,", 0x80, "\n"))

    val refused = failure(cp1252)(euro)
    refused.stage shouldBe Stage.Inference
    refused.conditions should contain(CharsetCondition)
    refused.messages.head should include(
      "expects one of the iso-8859-1, us-ascii, utf-16, utf-16be, utf-16le, utf-32, utf-8"
    )
    read(cp1252, javaCharsets)(euro) shouldBe Read(Seq("id", "v"), Seq(Seq("1", "€")))
    // The setting is the session's: the session that the other was made from refuses still.
    failure(cp1252)(euro).conditions should contain(CharsetCondition)

    // The names of the list are taken whatever their case; another name of one of them is not.
    read(Rfc4180.in("utf-8"))(file(bytes("id\n1\n"))).columns shouldBe Seq("id")
    read(Rfc4180.in("WINDOWS-1252"), javaCharsets)(euro).records shouldBe Seq(Seq("1", "€"))
    failure(Rfc4180.in("latin1"))(euro).conditions should contain(CharsetCondition)

    // An internal setting, off by default, since Spark 4.0.0: what an upgrade may take away.
    val setting = SQLConf.LEGACY_JAVA_CHARSETS
    setting.key shouldBe "spark.sql.legacy.javaCharsets"
    setting.defaultValue shouldBe Some(false)
    setting.version shouldBe "4.0.0"
    setting.isPublic shouldBe false
  }

  test(
    "windows-1252: the euro and the quotes of 0x80 to 0x9F, in gzip, and in a value over lines"
  ) {
    val cp1252 = Rfc4180.in("windows-1252")
    read(cp1252, javaCharsets)(
      file(bytes("id,a", 0xf1, "o\n1,10 ", 0x80, " ", 0x93, "x", 0x94, "\n"))
    ) shouldBe Read(Seq("id", "año"), Seq(Seq("1", "10 € “x”")))
    read(cp1252, javaCharsets)(
      file(bytes("id,a", 0xf1, "o\n1,", 0x80, "\n"), gzip = true)
    ) shouldBe Read(Seq("id", "año"), Seq(Seq("1", "€")))
    read(cp1252, javaCharsets)(
      file(bytes("id,v\n1,\"", 0x80, "\n", 0xf1, "\"\n"))
    ).records shouldBe Seq(Seq("1", "€\nñ"))
    // With a delimiter and an escape of their own.
    read(Dialect(";", "\"", "\\", Some("windows-1252")), javaCharsets)(
      file(bytes("id;v\n1;\"10 ", 0x80, "; \\\"a", 0xf1, "o\\\"\"\n"))
    ).records shouldBe Seq(Seq("1", "10 €; \"año\""))
  }

  test("the bytes 0x80 to 0x9F, read as ISO-8859-1, are control characters, and nothing says so") {
    val c1 = (0x80 to 0x9f).map(_.toByte).toArray
    val read1 = read(Rfc4180.in("ISO-8859-1"))(file(bytes("id,v\n1,a", c1, "b\n")))
    read1.records shouldBe Seq(Seq("1", "a" + (0x80 to 0x9f).map(_.toChar).mkString + "b"))
    read1.unparsed shouldBe empty
    // A file of windows-1252, as a spreadsheet writes one, read so: its euro is U+0080.
    read(Rfc4180.in("ISO-8859-1"))(
      file(bytes("id,v\n1,", 0x80, " ", 0x93, "x", 0x94, "\n"))
    ).records shouldBe Seq(Seq("1", "\u0080 \u0093x\u0094"))
  }

  test("a byte that is not of the encoding is replaced by U+FFFD, and nothing says so") {
    val utf8 = Rfc4180.in("UTF-8")
    // One U+FFFD for each sequence that is not of the encoding: a byte, two, one cut short.
    read(utf8)(file(bytes("id,v\n1,ni", 0xf1, "o\n2,a", 0xff, 0xfe, "b\n3,a", 0xe2, 0x82, "b\n")))
      .shouldBe(
        Read(
          Seq("id", "v"),
          Seq(Seq("1", "ni�o"), Seq("2", "a��b"), Seq("3", "a�b"))
        )
      )
    // In a name of the header.
    read(utf8)(file(bytes("id,a", 0xf1, "o\n1,x\n"))).columns shouldBe Seq("id", "a�o")
    // Between quotes, before a comma that is still the value's.
    read(utf8)(file(bytes("id,v\n1,\"a", 0xf1, ",b\"\n"))).records shouldBe
      Seq(Seq("1", "a�,b"))
    // At the end of a file with no end of line, and before an end of line.
    read(utf8)(file(bytes("id,v\n1,caf", 0xc3))).records shouldBe Seq(Seq("1", "caf�"))
    read(utf8)(file(bytes("id,v\n1,caf", 0xc3, "\n2,x\n"))).records shouldBe
      Seq(Seq("1", "caf�"), Seq("2", "x"))
    // In a file after the first of its batch.
    read(utf8)(file(bytes("id,v\n1,x\n")), file(bytes("id,v\n2,y", 0xf1, "\n"))).records should
      contain theSameElementsAs Seq(Seq("1", "x"), Seq("2", "y�"))
    // With no encoding given, the read is of UTF-8.
    val none = read()(file(bytes("id,v\n1,ni", 0xf1, "o\n")))
    none.records shouldBe Seq(Seq("1", "ni�o"))
    none.unparsed shouldBe empty
  }

  test("the five bytes that windows-1252 does not define are replaced by U+FFFD") {
    read(Rfc4180.in("windows-1252"), javaCharsets)(
      file(bytes("id,v\n1,a", 0x81, "b", 0x8d, "c", 0x8f, "d", 0x90, "e", 0x9d, "f\n"))
    ).records shouldBe Seq(Seq("1", "a�b�c�d�e�f"))
  }

  test("a U+FFFD that the source wrote is read as itself: the read cannot tell a replacement") {
    read(Rfc4180.in("UTF-8"))(file(bytes("id,v\n1,a", 0xef, 0xbf, 0xbd, "b\n"))).records shouldBe
      Seq(Seq("1", "a�b"))
  }

  test("a line of a file read as text keeps its bytes, and is a string with U+FFFD for a byte") {
    // As the lines of a file of JSON are read (P8.8b).
    val json = file(bytes("{\"v\":\"ni", 0xf1, "o\"}\n"), extension = "json")
    spark.read.text(s"file:$json").collect().map(_.getString(0)).toSeq shouldBe
      Seq("{\"v\":\"ni�o\"}")
    spark.read
      .text(s"file:$json")
      .selectExpr("hex(value)")
      .collect()
      .map(_.getString(0))
      .toSeq
      .shouldBe(Seq("7B2276223A226E69F16F227D"))
  }

  test("the decoders of the JDK, reporting what they cannot decode, refuse those bytes") {
    def decoded(charset: String, bytes: Int*): Try[String] = Try(
      Charset
        .forName(charset)
        .newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
        .decode(ByteBuffer.wrap(bytes.map(_.toByte).toArray))
        .toString
    )
    decoded("UTF-8", 0xf1).failed.get shouldBe a[MalformedInputException]
    decoded("UTF-8", 0xc3, 0xb1).get shouldBe "ñ"

    val undefined = Seq(0x81, 0x8d, 0x8f, 0x90, 0x9d)
    undefined.foreach { byte =>
      decoded("windows-1252", byte).failed.get shouldBe an[UnmappableCharacterException]
    }
    (0x80 to 0x9f).filterNot(undefined.contains).foreach { byte =>
      val char = decoded("windows-1252", byte).get.head
      withClue(f"0x$byte%02X: ") {
        Character.getType(char) should not be Character.CONTROL
      }
    }
    decoded("windows-1252", 0x80).get shouldBe "€"
    // Every byte is a character of ISO-8859-1, those of 0x80 to 0x9F control characters.
    (0x00 to 0xff).foreach(byte => decoded("ISO-8859-1", byte).get shouldBe byte.toChar.toString)
    (0x80 to 0x9f).foreach { byte =>
      Character.getType(byte.toChar) shouldBe Character.CONTROL
    }

    Seq("UTF-8", "ISO-8859-1", "windows-1252").foreach { name =>
      Charset.forName(name).name shouldBe name
      Charset.forName(name).getClass.getModule.getName shouldBe "java.base"
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The mark of the order of bytes
  // ---------------------------------------------------------------------------------------------

  test("the mark of UTF-8 is taken off: of the first file and of the others, and in gzip") {
    val utf8 = Rfc4180.in("UTF-8")
    val plain = Read(Seq("id", "v"), Seq(Seq("1", "x")))
    read(utf8)(file(bytes(Bom, "id,v\n1,x\n"))) shouldBe plain
    read()(file(bytes(Bom, "id,v\n1,x\n"))) shouldBe plain
    read(utf8)(file(bytes(Bom, "id,v\n1,x\n"), gzip = true)) shouldBe plain
    read(utf8)(file(bytes(Bom, "\"id\",v\n1,x\n"))) shouldBe plain
    val both = Seq(Seq("1", "x"), Seq("2", "y"))
    read(utf8)(file(bytes("id,v\n1,x\n")), file(bytes(Bom, "id,v\n2,y\n"))).records should
      contain theSameElementsAs both
    read(utf8)(file(bytes(Bom, "id,v\n1,x\n")), file(bytes("id,v\n2,y\n"))).records should
      contain theSameElementsAs both
  }

  test("a second mark is kept, at the start of the first name of its header") {
    val utf8 = Rfc4180.in("UTF-8")
    read(utf8)(file(bytes(Bom, Bom, "id,v\n1,x\n"))) shouldBe
      Read(Seq("﻿id", "v"), Seq(Seq("1", "x")))
    // In a file after the first, the header is not the batch's.
    val other = failure(utf8)(file(bytes("id,v\n1,x\n")), file(bytes(Bom, Bom, "id,v\n2,y\n")))
    other.stage shouldBe Stage.Checkpoint
    other.conditions should contain(HeaderCondition)
    // A U+FEFF inside a value is a character of it.
    read(utf8)(file(bytes("id,v\n1,a", Bom, "b\n"))).records shouldBe Seq(Seq("1", "a﻿b"))
  }

  test(
    "the mark of UTF-8, read in an encoding of one byte, is three characters of the first name"
  ) {
    read(Rfc4180.in("ISO-8859-1"))(file(bytes(Bom, "id,v\n1,x\n"))).columns shouldBe
      Seq("ï»¿id", "v")
    read(Rfc4180.in("windows-1252"), javaCharsets)(file(bytes(Bom, "id,v\n1,x\n"))).columns shouldBe
      Seq("ï»¿id", "v")
  }

  test("a file of UTF-16 read as UTF-8: its mark is two U+FFFD, and its characters have NULs") {
    val utf16 = "id,v\n1,x\n".getBytes("UTF-16LE")
    read(Rfc4180.in("UTF-8"))(file(bytes(0xff, 0xfe, utf16))).columns shouldBe
      Seq("��i\u0000d\u0000", "\u0000v\u0000")
    // With no mark, its bytes are of UTF-8 as well.
    read(Rfc4180.in("UTF-8"))(file(utf16)).columns shouldBe Seq("i\u0000d\u0000", "\u0000v\u0000")
  }

  test("a file with nothing but a mark has a header of one column, named U+0000") {
    val utf8 = Rfc4180.in("UTF-8")
    inferred(spark, utf8, Seq(file(Bom))) shouldBe Seq("\u0000")
    inferred(spark, Rfc4180.in("ISO-8859-1"), Seq(file(Bom))) shouldBe Seq("ï»¿")
    // An empty file, or a mark and an end of line, has no header, and is passed over.
    inferred(spark, utf8, Seq(file(Array.emptyByteArray))) shouldBe empty
    inferred(spark, utf8, Seq(file(bytes(Bom, "\n")))) shouldBe empty
    read(utf8)(file(Array.emptyByteArray), file(bytes("id,v\n1,x\n"))) shouldBe
      Read(Seq("id", "v"), Seq(Seq("1", "x")))
    // Beside files with a header, it fails the batch: first or after them.
    val first = failure(utf8)(file(Bom), file(bytes("id,v\n1,x\n")))
    first.stage shouldBe Stage.Checkpoint
    first.conditions should contain(HeaderCondition)
    val after = failure(utf8)(file(bytes("id,v\n1,x\n")), file(Bom))
    after.stage shouldBe Stage.Checkpoint
    after.conditions should contain(HeaderCondition)
  }

  // ---------------------------------------------------------------------------------------------
  // The dialect
  // ---------------------------------------------------------------------------------------------

  test("a delimiter of one character: between quotes, a character of the value") {
    read(Rfc4180.by(";"))(file(bytes("id;v\n1;\"a;b\"\n2;c,d\n"))).records shouldBe
      Seq(Seq("1", "a;b"), Seq("2", "c,d"))
    read(Rfc4180.by("\t"))(file(bytes("id\tv\n1\t\"a\tb\"\n"))).records shouldBe
      Seq(Seq("1", "a\tb"))
    read(Rfc4180.by("|"))(file(bytes("id|v\n1|a,b\n"))).records shouldBe Seq(Seq("1", "a,b"))
    read(Rfc4180.by("\u0001"))(file(bytes("id", 0x01, "v\n1", 0x01, "a,b\n"))).records shouldBe
      Seq(Seq("1", "a,b"))
    // One that is two bytes of UTF-8, and one of ISO-8859-1.
    read(Rfc4180.by("§"))(file(bytes("id§v\n1§a,b\n"))).records shouldBe
      Seq(Seq("1", "a,b"))
    read(Rfc4180.by("§").in("ISO-8859-1"))(
      file(bytes("id", 0xa7, "v\n1", 0xa7, "a,b\n"))
    ).records shouldBe Seq(Seq("1", "a,b"))
  }

  test("a quote that is its own escape is doubled inside a quoted value, whichever it is") {
    read(Dialect(",", "'", "'"))(file(bytes("id,v\n1,'it''s, here'\n2,\"x\"\n"))).records shouldBe
      Seq(Seq("1", "it's, here"), Seq("2", "\"x\""))
  }

  test("under the escape \\: \\\" is a quote, \\\\ a backslash, and a backslash else is itself") {
    val backslash = Rfc4180.escapedBy("\\")
    read(backslash)(file(bytes("id,v\n1,\"a\\\"b\"\n"))).records shouldBe Seq(Seq("1", "a\"b"))
    read(backslash)(file(bytes("id,v\n1,\"a\\\\b\"\n"))).records shouldBe Seq(Seq("1", "a\\b"))
    read(backslash)(file(bytes("id,v,w\n1,\"a\\\\\",z\n"))).records shouldBe
      Seq(Seq("1", "a\\", "z"))
    read(backslash)(file(bytes("id,v\n1,\"a\\nb\"\n"))).records shouldBe Seq(Seq("1", "a\\nb"))
    // Outside quotes every backslash is itself, before a quote as well.
    read(backslash)(file(bytes("id,v\n1,a\\b\n2,a\\\"b\n"))).records shouldBe
      Seq(Seq("1", "a\\b"), Seq("2", "a\\\"b"))
  }

  test("under the escape \\, a quote doubled as RFC 4180 doubles it is no quote") {
    val backslash = Rfc4180.escapedBy("\\")
    // The value keeps every quote that it was written with...
    read(backslash)(file(bytes("id,v\n1,\"a\"\"b\"\n"))).records shouldBe
      Seq(Seq("1", "\"a\"\"b\""))
    // ...or is cut at the comma after them, and its record has a value too many.
    val cut = read(backslash)(file(bytes("id,v,w\n1,\"say \"\"hi\"\", then\",z\n")))
    cut.records shouldBe empty
    cut.unparsed shouldBe Seq("1,\"say \"\"hi\"\", then\",z")
  }

  test("an escape of its own, ': '' is ' and '\" is \"") {
    read(Rfc4180.escapedBy("'"))(file(bytes("id,v\n1,\"a'\"b\"\n2,\"a''b\"\n"))).records shouldBe
      Seq(Seq("1", "a\"b"), Seq("2", "a'b"))
  }

  test("a NUL as the quote turns quoting off: a comma between quotes separates two values") {
    read(Dialect(",", "\u0000", "\""))(file(bytes("id,v,w\n1,\"a,b\"\n"))).records shouldBe
      Seq(Seq("1", "\"a", "b\""))
  }

  test("a quote inside a value that is not quoted is a character of it") {
    read()(file(bytes("id,v,w\n1,a\"b,c\n2,x,y\n"))).records shouldBe
      Seq(Seq("1", "a\"b", "c"), Seq("2", "x", "y"))
  }

  // ---------------------------------------------------------------------------------------------
  // Ends of lines
  // ---------------------------------------------------------------------------------------------

  test("a record ends at the end of line of its file, which is the first that it has") {
    val two = Seq(Seq("1", "x"), Seq("2", "y"))
    read()(file(bytes("id,v\n1,x\n2,y\n"))).records shouldBe two
    read()(file(bytes("id,v\r\n1,x\r\n2,y\r\n"))).records shouldBe two
    read()(file(bytes("id,v\r1,x\r2,y\r"))).records shouldBe two
  }

  test("between quotes, the end of line of the file is read as LF, and every other as written") {
    def value(content: Any*): String = read()(file(bytes(content: _*))).records.head(1)
    // A file whose lines end in CRLF, or in CR.
    value("id,v\r\n1,\"a\r\nb\"\r\n") shouldBe "a\nb"
    value("id,v\r1,\"a\rb\"\r") shouldBe "a\nb"
    value("id,v\r1,\"a\r\nb\"\r") shouldBe "a\n\nb"
    value("id,v\r\n1,\"a\rb\"\r\n") shouldBe "a\rb"
    value("id,v\r\n1,\"a\nb\"\r\n") shouldBe "a\nb"
    // A file whose lines end in LF.
    value("id,v\n1,\"a\r\nb\"\n") shouldBe "a\r\nb"
    value("id,v\n1,\"a\rb\"\n") shouldBe "a\rb"
    // The end of line of a file is that of its first line, that of the header.
    value("id,v\n1,\"a\r\nb\"\r\n") shouldBe "a\r\nb"
    // Each file has its own.
    read()(file(bytes("id,v\r\n1,\"a\r\nb\"\r\n")), file(bytes("id,v\n2,\"c\r\nd\"\n"))).records
      .should(contain theSameElementsAs Seq(Seq("1", "a\nb"), Seq("2", "c\r\nd")))
  }

  test("a file that mixes its ends of lines") {
    // After a first CRLF, an LF ends no record: two records are one, with a value too many.
    val glued = read()(file(bytes("id,v\r\n1,x\n2,y\r\n")))
    glued.records shouldBe empty
    glued.unparsed shouldBe Seq("1,x\n2,y")
    // A record of one value has no value too many: two are one, and nothing is set apart.
    val one = read()(file(bytes("id\r\n1\n2\r\n3\r\n")))
    one.records shouldBe Seq(Seq("1\n2"), Seq("3"))
    one.unparsed shouldBe empty
    // After a first LF, a CRLF leaves its CR at the end of the last value, and nothing says so.
    val cr = read()(file(bytes("id,v\n1,x\r\n2,y\n")))
    cr.records shouldBe Seq(Seq("1", "x\r"), Seq("2", "y"))
    cr.unparsed shouldBe empty
    // Which a value that ends in CR between quotes, in a file of LF, is too.
    read()(file(bytes("id,v\n1,\"x\r\"\n2,y\n"))).records shouldBe
      Seq(Seq("1", "x\r"), Seq("2", "y"))
  }

  test("a file of a spreadsheet: records that end in CRLF, and LF inside a cell, as written") {
    // A spreadsheet ends each record in CRLF, and a line inside a cell in LF alone. In Spain it
    // separates values by `;`, and writes "CSV" in windows-1252 and "CSV UTF-8" with a mark.
    val excel = Dialect(";", "\"", "\"")
    val expected = Read(
      Seq("id", "nombre", "nota"),
      Seq(
        Seq("1", "Peña", "línea uno\nlínea dos"),
        Seq("2", "Ruiz; Ana", "dice \"hola\"\n10 €"),
        // An empty value is none.
        Seq("3", null, null)
      )
    )
    val utf8 = read(excel.in("UTF-8"))(
      file(
        bytes(
          Bom,
          "id;nombre;nota\r\n",
          "1;Peña;\"línea uno\nlínea dos\"\r\n",
          "2;\"Ruiz; Ana\";\"dice \"\"hola\"\"\n10 €\"\r\n",
          "3;;\r\n"
        )
      )
    )
    utf8 shouldBe expected
    val cp1252 = read(excel.in("windows-1252"), javaCharsets)(
      file(
        bytes(
          "id;nombre;nota\r\n",
          "1;Pe",
          0xf1,
          "a;\"l",
          0xed,
          "nea uno\nl",
          0xed,
          "nea dos\"\r\n",
          "2;\"Ruiz; Ana\";\"dice \"\"hola\"\"\n10 ",
          0x80,
          "\"\r\n",
          "3;;\r\n"
        )
      )
    )
    cp1252 shouldBe expected
  }

  // ---------------------------------------------------------------------------------------------
  // How a file is read
  // ---------------------------------------------------------------------------------------------

  test("a file is read whole by one task, whatever its size, and several files may share one") {
    val big = file(bytes("id,v\n" + (1 to 20000).map(i => s"$i,value $i").mkString("\n") + "\n"))
    val small = spark.newSession()
    small.conf.set("spark.sql.files.maxPartitionBytes", "4096")
    small.conf.set("spark.sql.files.openCostInBytes", "0")
    Files.size(big) should be > 50L * 4096
    val whole = small.read.option("header", "true").option("multiLine", "true")
    whole.csv(s"file:$big").rdd.getNumPartitions shouldBe 1
    whole.csv(s"file:$big").count() shouldBe 20000
    // A read by lines would split it.
    small.read.option("header", "true").csv(s"file:$big").rdd.getNumPartitions should be > 1

    val three = (1 to 3).map(i => file(bytes(s"id,v\n$i,x\n")))
    spark.read
      .option("header", "true")
      .option("multiLine", "true")
      .csv(locations(three): _*)
      .rdd
      .getNumPartitions should be < 3
  }

  // ---------------------------------------------------------------------------------------------
  // The files
  // ---------------------------------------------------------------------------------------------

  private var count = 0

  /** A file of its own, with these bytes, in gzip if it says so. */
  private def file(content: Array[Byte], gzip: Boolean = false, extension: String = "csv"): Path = {
    count += 1
    val path = workDir.resolve(s"dialect-$count.$extension${if (gzip) ".gz" else ""}")
    if (gzip)
      Using.resource(new GZIPOutputStream(Files.newOutputStream(path)))(_.write(content))
    else Files.write(path, content)
    path
  }

  private def caught[A](stage: Stage)(body: => A): Either[Failure, A] =
    try Right(body)
    catch {
      case e: Exception =>
        val chain = Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).take(16).toSeq
        Left(
          Failure(
            stage,
            chain.collect {
              case spark: SparkThrowable if spark.getCondition != null => spark.getCondition
            },
            chain.map(cause => Option(cause.getMessage).getOrElse(""))
          )
        )
    }
}

object SparkCsvDialectContractSpec {

  /** The column of a record that cannot be parsed, as the connector names it. */
  private val Unparsed = "_lakehouse_unparsed"

  /** Spark's condition for a charset that its reader does not take. */
  private val CharsetCondition = "INVALID_PARAMETER_VALUE.CHARSET"

  /**
   * Spark's condition for a header of CSV that is not the schema's (`CSVHeaderChecker`), as
   * `FileSourceConnector` knows it.
   */
  private val HeaderCondition = "_LEGACY_ERROR_TEMP_3241"

  /** The mark of the order of bytes of UTF-8. */
  private val Bom: Array[Byte] = Array(0xef, 0xbb, 0xbf).map(_.toByte)

  /** How a read separates, quotes and decodes; `encoding` none, for Spark's own (UTF-8). */
  final private case class Dialect(
      delimiter: String,
      quote: String,
      escape: String,
      encoding: Option[String] = None
  ) {
    def in(charset: String): Dialect = copy(encoding = Some(charset))
    def by(separator: String): Dialect = copy(delimiter = separator)
    def escapedBy(character: String): Dialect = copy(escape = character)
  }

  /** The dialect of the engine up to P8.9: a quote inside a quoted value is the quote twice. */
  private val Rfc4180 = Dialect(",", "\"", "\"")

  /**
   * What a read kept: the columns of its inference, the rows that were parsed, each without the
   * column of what could not be, and the text of the records that could not be.
   */
  final private case class Read(
      columns: Seq[String],
      records: Seq[Seq[String]],
      unparsed: Seq[String] = Nil
  )

  sealed private trait Stage
  private object Stage {
    case object Inference extends Stage { override def toString = "inference" }
    case object Checkpoint extends Stage { override def toString = "checkpoint" }
  }

  /** A failure, by the conditions of Spark and the messages along its chain of causes. */
  final private case class Failure(stage: Stage, conditions: Seq[String], messages: Seq[String])

  private def locations(files: Seq[Path]): Seq[String] = files.map(file => s"file:$file")

  /** Bytes: a string as UTF-8, a number as the byte that it is, and bytes as they are. */
  private def bytes(parts: Any*): Array[Byte] = parts.flatMap {
    case text: String => text.getBytes(UTF_8).toSeq
    case byte: Int =>
      require(byte >= 0 && byte <= 0xff, s"$byte is no byte")
      Seq(byte.toByte)
    case many: Array[Byte] => many.toSeq
    case other => throw new IllegalArgumentException(s"$other is not bytes")
  }.toArray
}
