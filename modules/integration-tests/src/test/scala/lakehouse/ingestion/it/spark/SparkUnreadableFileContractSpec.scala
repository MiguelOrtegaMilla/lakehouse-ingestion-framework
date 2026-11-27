package lakehouse.ingestion.it.spark

import java.io.{EOFException, FileNotFoundException, IOException}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{Files, Path}
import java.util.zip.GZIPOutputStream

import scala.jdk.CollectionConverters._
import scala.util.Using

import lakehouse.ingestion.it.support.LocalSparkSuite
import org.apache.spark.SparkThrowable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader}
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What Spark 4.1 raises when a file of a batch cannot be read, at each of the two moments that a
 * read of the file connector has one read it, and what the same file does when it is read alone:
 * the facts that the policy for a file that cannot be read is built on (ADR-0023, discovery §20).
 *
 * A read of the connector has Spark read the files twice. First it infers what the columns are,
 * from the header of CSV, from every record of JSON, from the footer of every file of Parquet
 * (''the inference''); then it reads the rows with those columns and keeps them apart from the
 * files (`localCheckpoint`, ''the checkpoint''). The reads here are made with the options of
 * `FileSourceConnector`, as Spark is asked by it, and no code of the engine is in them: these are
 * characterisation tests, as those of `DeltaAppendContractSpec` are. A Spark upgrade that changes
 * one of these facts fails here, with the name of the assumption that it breaks (ADR-0014,
 * upgrade procedure).
 *
 * What they show, and what ADR-0023 takes from it:
 *
 *  - what is raised is of no one type nor condition, and comes at either moment, by the format,
 *    by the place of the file in the batch, and by how much of it is left. A file is not known to
 *    be the one that cannot be read by what Spark raises;
 *  - Spark says that a file that it may not open does not exist (`FAILED_READ_FILE.FILE_NOT_EXIST`)
 *    while the file is there: what is gone is known by a look at the file, and not by Spark;
 *  - a file that cannot be read, read alone at both moments, fails, and the same way every time;
 *    a sound file of the same batch, read alone, reads whole. So reading each file of a batch
 *    alone finds those that cannot be read, and finds the same ones each time;
 *  - a batch may fail with no file that fails alone: two files of Parquet with one column of two
 *    types. Such a failure is not a file's.
 *
 * The files are broken for real: cut short, a page of Parquet written over, the permissions of a
 * file taken away. A test of a file that cannot be opened is cancelled for a user that may open
 * any file (root).
 */
final class SparkUnreadableFileContractSpec extends AnyFunSuite with Matchers with LocalSparkSuite {

  import SparkUnreadableFileContractSpec._

  // ---------------------------------------------------------------------------------------------
  // The reads, as the file connector makes them
  // ---------------------------------------------------------------------------------------------

  /** A reader that passes over no file, as every reader of the connector is. */
  private def everyFile(reader: DataFrameReader): DataFrameReader =
    reader.option("ignoreCorruptFiles", "false").option("ignoreMissingFiles", "false")

  private def csvReader: DataFrameReader =
    everyFile(spark.read)
      .option("header", "true")
      .option("multiLine", "true")
      .option("quote", "\"")
      .option("escape", "\"")

  /** The inference of a format: the columns of the files, as the connector asks for them. */
  private def infer(format: Format, files: Seq[Path]): Seq[String] = format match {
    case Format.Csv => csvReader.csv(locations(files): _*).schema.fieldNames.toSeq
    case Format.Json =>
      val exact = spark.newSession()
      exact.conf.set("spark.sql.caseSensitive", "true")
      everyFile(exact.read)
        .option("columnNameOfCorruptRecord", Unparsed)
        .option("primitivesAsString", "true")
        .json(locations(files): _*)
        .schema
        .fieldNames
        .toSeq
        .filterNot(_ == Unparsed)
    case Format.Parquet => parquet(files).schema.fieldNames.toSeq
  }

  private def parquet(files: Seq[Path]): DataFrame =
    everyFile(spark.read).option("mergeSchema", "true").parquet(locations(files): _*)

  /** The checkpoint of a read with the columns that its inference found: the rows it kept. */
  private def checkpoint(format: Format, columns: Seq[String], files: Seq[Path]): Long = {
    val text = StructType((columns :+ Unparsed).map(StructField(_, StringType)))
    val read = format match {
      case Format.Csv =>
        csvReader
          .option("enforceSchema", "false")
          .option("mode", "PERMISSIVE")
          .option("columnNameOfCorruptRecord", Unparsed)
          .schema(text)
          .csv(locations(files): _*)
      case Format.Json =>
        everyFile(spark.read)
          .option("columnNameOfCorruptRecord", Unparsed)
          .option("mode", "PERMISSIVE")
          .schema(text)
          .json(locations(files): _*)
      case Format.Parquet => parquet(files)
    }
    read
      .select(col("*"), read.metadataColumn("_metadata").getField("file_path").as("file"))
      .localCheckpoint(eager = true)
      .count()
  }

  /** A read of some files at both of its moments, as the connector makes one: what it kept. */
  private def readOf(format: Format, files: Path*): Either[Failure, Long] =
    caught(Stage.Inference)(infer(format, files)).flatMap { columns =>
      caught(Stage.Checkpoint)(checkpoint(format, columns, files))
    }

  // ---------------------------------------------------------------------------------------------
  // The files
  // ---------------------------------------------------------------------------------------------

  /** A directory of its own for each case. */
  private def directory(name: String): Path = Files.createDirectories(workDir.resolve(name))

  /** A file of Parquet of `rows` rows, `id` and `name`, written by Spark as one file. */
  private def parquetFile(dir: Path, name: String, rows: Int, idAsText: Boolean = false): Path = {
    val session = spark
    import session.implicits._
    val frame =
      if (idAsText) (1 to rows).map(i => (i.toString, s"name $i")).toDF("id", "name")
      else (1 to rows).map(i => (i.toLong, s"name $i")).toDF("id", "name")
    val written = workDir.resolve(s"written-${dir.getFileName}-$name")
    frame.coalesce(1).write.parquet(written.toString)
    val part = Using.resource(Files.list(written)) {
      _.iterator().asScala.find(_.getFileName.toString.endsWith(".parquet")).get
    }
    Files.move(part, dir.resolve(name))
  }

  private def csvFile(dir: Path, name: String, rows: Int): Path =
    Files.write(dir.resolve(name), csvText(rows).getBytes(UTF_8))

  private def jsonFile(dir: Path, name: String, rows: Int): Path =
    Files.write(dir.resolve(name), jsonText(rows).getBytes(UTF_8))

  private def gzipped(dir: Path, name: String, text: String): Path = {
    val file = dir.resolve(name)
    Using.resource(new GZIPOutputStream(Files.newOutputStream(file)))(_.write(text.getBytes(UTF_8)))
    file
  }

  /** The file with its second half gone, as a copy that stopped halfway leaves it. */
  private def cutShort(file: Path): Path = {
    val bytes = Files.readAllBytes(file)
    Files.write(file, bytes.take(bytes.length / 2))
  }

  /**
   * A file of Parquet with its first pages written over and its footer whole: its columns can be
   * read, and its rows cannot.
   */
  private def pagesWrittenOver(file: Path): Path = {
    val bytes = Files.readAllBytes(file)
    // After the magic number at its head, and up to the half: the footer is at the tail.
    java.util.Arrays.fill(bytes, 4, bytes.length / 2, 0.toByte)
    Files.write(file, bytes)
  }

  /** `body` with the file made one that this user may not open, and then as it was. */
  private def unopenable[A](file: Path)(body: => A): A = {
    val permissions = Files.getPosixFilePermissions(file)
    Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("---------"))
    try {
      assume(!Files.isReadable(file), "the tests run as a user that may open any file")
      body
    } finally Files.setPosixFilePermissions(file, permissions): Unit
  }

  // ---------------------------------------------------------------------------------------------
  // What a batch with a file that cannot be read raises, at each moment
  // ---------------------------------------------------------------------------------------------

  test("a file of CSV in gzip is read as the text that it holds, by its name") {
    val dir = directory("gzip-whole")
    val file = gzipped(dir, "a.csv.gz", csvText(2000))

    readOf(Format.Csv, file) shouldBe Right(2000L)
  }

  test("a file of Parquet cut short fails the inference of its batch, which reads every footer") {
    val dir = directory("parquet-cut")
    val sound = parquetFile(dir, "a.parquet", rows = 100)
    val cut = cutShort(parquetFile(dir, "b.parquet", rows = 1000))

    // Whatever its place: the footer of each file is read.
    for (files <- Seq(Seq(sound, cut), Seq(cut, sound))) {
      val failure = readOf(Format.Parquet, files: _*).left.toOption.get
      failure.stage shouldBe Stage.Inference
      // Under a job that was aborted, with no condition, and a wait for a result of the job.
      failure.conditions shouldBe Seq(FooterNotRead)
      failure.cause shouldBe classOf[RuntimeException]
      failure.message should include("is not a Parquet file")
    }
  }

  test("a file of Parquet whose footer is whole and whose pages are not fails the checkpoint") {
    val dir = directory("parquet-pages")
    val sound = parquetFile(dir, "a.parquet", rows = 100)
    val broken = pagesWrittenOver(parquetFile(dir, "b.parquet", rows = 1000))

    caught(Stage.Inference)(infer(Format.Parquet, Seq(sound, broken))) shouldBe
      Right(Seq("id", "name"))
    val failure = readOf(Format.Parquet, sound, broken).left.toOption.get
    failure.stage shouldBe Stage.Checkpoint
    failure.conditions shouldBe Seq(NoHint)
    failure.causes should contain(classOf[IOException])
  }

  test("a file of JSON in gzip cut short fails the inference of its batch, which reads it all") {
    val dir = directory("json-gzip")
    val sound = jsonFile(dir, "a.json", rows = 10)
    val cut = cutShort(gzipped(dir, "b.json.gz", jsonText(20000)))

    val failure = readOf(Format.Json, sound, cut).left.toOption.get
    failure.stage shouldBe Stage.Inference
    failure.conditions shouldBe Seq(NoHint)
    failure.cause shouldBe classOf[EOFException]
  }

  test("a file of CSV in gzip cut short after the first file of its batch fails the checkpoint") {
    val dir = directory("csv-gzip")
    val sound = csvFile(dir, "a.csv", rows = 10)
    val cut = cutShort(gzipped(dir, "b.csv.gz", csvText(20000)))

    // The header is read from the first file of the read alone.
    caught(Stage.Inference)(infer(Format.Csv, Seq(sound, cut))) shouldBe
      Right(Seq("id", "name", "note"))
    val failure = readOf(Format.Csv, sound, cut).left.toOption.get
    failure.stage shouldBe Stage.Checkpoint
    failure.conditions shouldBe Seq(NoHint)
    failure.cause shouldBe classOf[EOFException]
  }

  test("the inference of a batch of CSV reads its first file, and no other") {
    val dir = directory("csv-first")
    val sound = csvFile(dir, "a.csv", rows = 10)
    val closed = csvFile(dir, "b.csv", rows = 10)

    unopenable(closed) {
      caught(Stage.Inference)(infer(Format.Csv, Seq(sound, closed))) shouldBe
        Right(Seq("id", "name", "note"))
      val first = caught(Stage.Inference)(infer(Format.Csv, Seq(closed, sound))).left.toOption.get
      // Under a job that was aborted, with no condition of Spark's at all.
      first.conditions shouldBe empty
      first.cause shouldBe classOf[FileNotFoundException]
    }
  }

  test("a file that may not be opened is said by Spark not to exist, while it is there") {
    val dir = directory("unopenable")
    val csv = (csvFile(dir, "a.csv", rows = 10), csvFile(dir, "b.csv", rows = 10))
    val json = (jsonFile(dir, "a.json", rows = 10), jsonFile(dir, "b.json", rows = 10))

    unopenable(csv._2) {
      val failure = readOf(Format.Csv, csv._1, csv._2).left.toOption.get
      failure.stage shouldBe Stage.Checkpoint
      failure.conditions shouldBe Seq(NotThere)
      failure.cause shouldBe classOf[FileNotFoundException]
      Files.exists(csv._2) shouldBe true
    }
    unopenable(json._2) {
      val failure = readOf(Format.Json, json._1, json._2).left.toOption.get
      failure.stage shouldBe Stage.Inference
      failure.conditions shouldBe Seq(NotThere)
      failure.cause shouldBe classOf[FileNotFoundException]
    }
  }

  test("a file of Parquet that may not be opened fails the inference, with no condition") {
    val dir = directory("parquet-unopenable")
    val sound = parquetFile(dir, "a.parquet", rows = 10)
    val closed = parquetFile(dir, "b.parquet", rows = 10)

    unopenable(closed) {
      val failure = readOf(Format.Parquet, sound, closed).left.toOption.get
      failure.stage shouldBe Stage.Inference
      failure.conditions shouldBe empty
      failure.cause shouldBe classOf[FileNotFoundException]
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Each file read alone
  // ---------------------------------------------------------------------------------------------

  private val cases = Seq(
    Case(
      "a file of Parquet cut short",
      Format.Parquet,
      dir => parquetFile(dir, "a.parquet", rows = 100) -> 100L,
      dir => cutShort(parquetFile(dir, "b.parquet", rows = 1000))
    ),
    Case(
      "a file of Parquet whose pages are written over",
      Format.Parquet,
      dir => parquetFile(dir, "a.parquet", rows = 100) -> 100L,
      dir => pagesWrittenOver(parquetFile(dir, "b.parquet", rows = 1000))
    ),
    Case(
      "a file of CSV in gzip cut short",
      Format.Csv,
      dir => csvFile(dir, "a.csv", rows = 10) -> 10L,
      dir => cutShort(gzipped(dir, "b.csv.gz", csvText(20000)))
    ),
    Case(
      "a file of JSON in gzip cut short",
      Format.Json,
      dir => jsonFile(dir, "a.json", rows = 10) -> 10L,
      dir => cutShort(gzipped(dir, "b.json.gz", jsonText(20000)))
    ),
    Case(
      "a file of CSV that may not be opened",
      Format.Csv,
      dir => csvFile(dir, "a.csv", rows = 10) -> 10L,
      dir => csvFile(dir, "b.csv", rows = 10),
      closed = true
    ),
    Case(
      "a file of JSON that may not be opened",
      Format.Json,
      dir => jsonFile(dir, "a.json", rows = 10) -> 10L,
      dir => jsonFile(dir, "b.json", rows = 10),
      closed = true
    ),
    Case(
      "a file of Parquet that may not be opened",
      Format.Parquet,
      dir => parquetFile(dir, "a.parquet", rows = 10) -> 10L,
      dir => parquetFile(dir, "b.parquet", rows = 10),
      closed = true
    )
  )

  cases.zipWithIndex.foreach { case (c, index) =>
    test(s"${c.name}, read alone, fails the same way each time; the sound file beside it reads") {
      val dir = directory(s"alone-$index")
      val (sound, rows) = c.sound(dir)
      val broken = c.broken(dir)

      def alone(): Assertion = {
        readOf(c.format, sound, broken) shouldBe a[Left[_, _]]
        val reads = (1 to 3).map(_ => readOf(c.format, broken))
        reads.head shouldBe a[Left[_, _]]
        // The same moment, the same conditions and the same cause, at every read.
        reads.map(_.left.map(_.signature)).distinct should have size 1
        readOf(c.format, sound) shouldBe Right(rows)
      }

      if (c.closed) unopenable(broken)(alone()) else alone()
    }
  }

  test("two files of Parquet may fail together, and each read alone reads whole") {
    val dir = directory("parquet-types")
    val number = parquetFile(dir, "a.parquet", rows = 3)
    val text = parquetFile(dir, "b.parquet", rows = 2, idAsText = true)

    val together = readOf(Format.Parquet, number, text).left.toOption.get
    together.stage shouldBe Stage.Inference
    together.conditions shouldBe Seq("CANNOT_MERGE_SCHEMAS", "CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE")
    readOf(Format.Parquet, number) shouldBe Right(3L)
    readOf(Format.Parquet, text) shouldBe Right(2L)
  }
}

object SparkUnreadableFileContractSpec {

  /** The column of a record that cannot be parsed, as the connector names it. */
  private val Unparsed = "_lakehouse_unparsed"

  /** Spark's condition for a file that it failed to read, with no more said of why. */
  private val NoHint = "FAILED_READ_FILE.NO_HINT"

  /** Spark's condition for a file that it says is not there. */
  private val NotThere = "FAILED_READ_FILE.FILE_NOT_EXIST"

  /** Spark's condition for a file of Parquet whose footer it could not read. */
  private val FooterNotRead = "FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER"

  sealed private trait Format
  private object Format {
    case object Csv extends Format
    case object Json extends Format
    case object Parquet extends Format
  }

  /**
   * A batch of two files, a sound one and one that cannot be read, each made in the directory that
   * it is given.
   *
   * @param sound the sound file, and how many rows it has
   * @param broken the file that cannot be read
   * @param closed whether the broken file is one that may not be opened: it is made so only while
   *   it is read, so that the suite can remove it
   */
  final private case class Case(
      name: String,
      format: Format,
      sound: Path => (Path, Long),
      broken: Path => Path,
      closed: Boolean = false
  )

  /** The moment of a read at which Spark raised. */
  sealed private trait Stage
  private object Stage {
    case object Inference extends Stage
    case object Checkpoint extends Stage
  }

  /**
   * What a read raised: at which moment, the conditions of Spark in its chain of causes from the
   * outside in, and the types of the causes, of which the last is where it began.
   */
  final private case class Failure(
      stage: Stage,
      conditions: Seq[String],
      causes: Seq[Class[_]],
      message: String
  ) {
    def cause: Class[_] = causes.last

    /** What is the same for two reads of one file: all but the message, which has a path. */
    def signature: (Stage, Seq[String], Seq[Class[_]]) = (stage, conditions, causes)
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
            chain.map(_.getClass),
            Option(chain.last.getMessage).getOrElse("")
          )
        )
    }

  private def locations(files: Seq[Path]): Seq[String] = files.map(file => s"file:$file")

  private def csvText(rows: Int): String =
    ("id,name,note" +: (1 to rows).map(i => s"""$i,name $i,"a note, with a comma: $i""""))
      .mkString("", "\n", "\n")

  private def jsonText(rows: Int): String =
    (1 to rows).map(i => s"""{"id":"$i","name":"name $i"}""").mkString("", "\n", "\n")
}
