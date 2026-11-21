package lakehouse.ingestion.engine.files

import java.net.URI
import java.nio.file.{Path, Paths}
import java.util.Locale

import scala.annotation.tailrec

import io.circe.Json
import lakehouse.ingestion.core.config.{
  ConnectorKind,
  EngineColumns,
  FileConnector,
  FileConnectorConfig,
  Source
}
import lakehouse.ingestion.core.files.FilePlanner.{NothingNew, Planned}
import lakehouse.ingestion.core.files.{FilePlan, FilePlanner}
import lakehouse.ingestion.engine.RunFailedException
import lakehouse.ingestion.engine.connector.{
  PlanContext,
  Progress,
  ReadContext,
  ReadResult,
  SourceChangedException,
  SourceConnector
}
import org.apache.spark.SparkThrowable
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, DataFrame, DataFrameReader, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Files that are left in a directory of the landing zone (`type: file`).
 *
 * '''A plan''' is the files of the directory that are new, complete and of the source
 * (`FilePlanner`), each by its name, its size and its time, as `processed_file` records it.
 *
 * '''A read''' reads the files of a plan and no other, as the source wrote them:
 *
 *  - '''CSV and JSON as text''' (ADR-0008): every column is a string, as it was written, and
 *    `_rescued_data` is there for what does not fit the columns, which the policy of a
 *    source's schema fills (P9). The columns of CSV are those of the header, which every file
 *    of the batch must have: Spark reads a record by the place of its values, and a file with
 *    other columns would put values under another column's name. The dialect is RFC 4180's:
 *    a value is quoted if it has a comma, a quote or the end of a line in it, a quote inside
 *    it is written twice, and a backslash is a character like any other. A source that writes
 *    another dialect has no way to say so yet (P8). The columns of JSON are those of the
 *    objects of the batch, each under its name as it was written; a value that is an object or
 *    a list is kept as the JSON it was written as. JSON is one object to a line.
 *  - '''Parquet with its own types''', and the columns of every file of the batch: a column
 *    that only some files have is null in the rows of the others. Two files with one column
 *    of two types fail the read.
 *  - '''A record that cannot be parsed''' is set apart, with its text as it was (`unparsed`):
 *    a line that is not JSON, a record of CSV with more values or fewer than the header has. A
 *    quote of CSV that is not closed is not one: the parser takes the rest of the file into
 *    that value, which is then kept as it was read, for Silver and its checks to see.
 *  - Every row says the file that it was read from (`_source_file`), as `<directory>/<name>`
 *    under the landing root: where the source left it, which is how `processed_file` records
 *    it.
 *
 * '''The names of the columns are the source's, and no value is lost for its name.''' Spark
 * and Delta tell the names of columns apart without their case, and a read that left that to
 * them would lose what a source sent, without a word:
 *
 *  - ''fields of JSON that differ only in their case'', as `id` in one record and `ID` in
 *    another, would be read as one column, with the values under one of the two names left
 *    out. They fail the batch, as permanent, naming both: which of the two a column is to be
 *    is the policy of a schema (P9). So does a record with a field twice, which Spark refuses
 *    with an error of no class. Inside an object nothing is told apart: it is kept as its text;
 *  - ''a header of CSV with a name twice, case aside, or with an empty name'' is read by Spark
 *    under names of its own making, which no file then has: the batch fails, as permanent, and
 *    says so, as it does for files whose headers differ;
 *  - ''a column named `_metadata`'' is a column of the source like any other. It is the name
 *    under which Spark gives the file of a row, which is asked for in the way that a column of
 *    the source does not hide (`Dataset.metadataColumn`).
 *
 * '''A file that cannot be read fails its batch, whatever the session is set to.''' With
 * `spark.sql.files.ignoreCorruptFiles`, Spark passes over a file that it cannot read, or keeps
 * what it had read of it, and says nothing: the file would be recorded as taken with its rows
 * not in Bronze. Every read says that no file is passed over, which a setting of a session
 * does not change. The policy that may skip a file, and says that it did, is P8's.
 *
 * '''The files are the planned ones while they are read.''' A name may be given to another file
 * at any moment. So, just before Spark reads the files, each name must still lead to a file of
 * the size and the time that were planned, and which file of the file system it is, its device
 * and its inode, is taken then (`Landing.confirm`); Spark then reads them all, and what it read
 * is kept, by Spark, apart from the files (`Dataset.localCheckpoint`); and just after, each name
 * must still lead to that same file, of that size and that time (`Landing.confirmAgain`). If one
 * does not, the read fails, before anything of it is written. What is kept is what everything
 * after the read is made from: the data, the records set apart, and their counts, which are
 * then of one read. A copy that Spark loses is not read again from the files: the job that needs
 * it fails, and the batch with it.
 *
 * The device and the inode are not in the plan. A plan outlives its process, and a restart of the
 * machine or of its container may give the same disk another device number: a plan that named
 * one would fail, as permanent, a batch whose files never changed, and stop its source.
 *
 * '''What that leaves open.''' Between the plan and the look before the read, a file is known by
 * its size and its time alone: another file put in its place with the same size and time, or the
 * file changed in its place with them put back, is read, and not seen to have changed. While it
 * is read, a file put in its place is seen; a file changed in its place with its size and time
 * put back, or changed and put back as it was between the two looks, is not. Telling those needs
 * what the file holds, read twice, and it takes a producer that means to hide a change. The way
 * to close it is to take a file out of the producer's reach before it is read, by moving it where
 * only the engine writes, or to read from storage where an object never changes once written
 * (E2).
 *
 * '''A plan whose files changed is not read''', and the read says so, with each file and what
 * changed of it (`SourceChangedException`): a run that read the plan again would find the same.
 * The engine then gives the batch up, if nothing of it was written, and the next run plans the
 * files as they are by then (ADR-0018). A file that was written again is another file, and is
 * planned as a new one.
 *
 * @param afterRead what runs once Spark has read the files and before they are confirmed again:
 *   nothing, but in the tests that change a file at that moment
 */
final class FileSourceConnector(landing: Landing, afterRead: () => Unit = () => ())
    extends SourceConnector[FileConnectorConfig] {

  import FileSourceConnector._

  override val kind: ConnectorKind = FileConnector

  override def plan(
      source: Source,
      config: FileConnectorConfig,
      context: PlanContext
  ): Option[Json] = {
    val listed = landing.list(config)
    val taken = context.files.processed(config.path, listed.map(_.name))
    FilePlanner.plan(config, listed, taken, context.now) match {
      case Left(problems) =>
        throw RunFailedException.permanent(
          s"the directory ${config.path} of the landing zone has files whose names no run may " +
            s"read, and nothing was planned: ${problems.toChain.toList.mkString("; ")}"
        )
      case Right(NothingNew(waiting)) =>
        log.info(s"${source.id}: no new file is complete in ${config.path}; $waiting not yet")
        None
      case Right(Planned(plan, waiting, leftOver)) =>
        log.info(
          s"${source.id}: planned ${plan.files.size} files of ${config.path}, " +
            s"${plan.bytes} bytes; $waiting not complete yet, $leftOver for the next batch"
        )
        Some(plan.toJson)
    }
  }

  override def read(
      source: Source,
      config: FileConnectorConfig,
      plan: Json,
      context: ReadContext
  ): ReadResult.Kept = {
    val files = planOf(source, config, plan)
    val before = confirmed(config, "since it was planned")(landing.confirm(config, files))
    val read =
      readable(context.spark, config.format, before.paths, sourceFiles(files, before.paths))
    // What is read is kept by Spark apart from the files, and everything after is made from it.
    val kept =
      try read.rows.localCheckpoint(eager = true)
      catch {
        case e: Exception if otherHeader(e) =>
          throw RunFailedException.permanent(
            s"a file of CSV of the batch of ${config.path} does not have the header that the " +
              "batch is read by. Either the files do not all have one header, or a header has " +
              "a name twice, case aside, or a name that is empty, which Spark reads under a " +
              "name of its own making. Spark reads a record by the place of its values, and " +
              "would put them under another column's name. Nothing was written",
            e
          )
      }
    afterRead()
    confirmed(config, "while it was read")(landing.confirmAgain(config, files, before))

    if (config.format == FileConnectorConfig.Format.Parquet)
      ReadResult.Kept(kept, unparsed = None)
    else
      ReadResult.Kept(
        // The column that stood for the columns of a batch that has none goes with it. A
        // column of the source is kept, whatever it is named.
        rows = kept.filter(col(Unparsed).isNull).drop(Unparsed +: read.placeholder.toSeq: _*),
        unparsed = Some(
          kept
            .filter(col(Unparsed).isNotNull)
            .select(col(EngineColumns.SourceFile), col(Unparsed).as(ReadResult.Record))
        )
      )
  }

  /** The files of a plan are what its commit records: each is taken, and never again. */
  override def progress(source: Source, config: FileConnectorConfig, plan: Json): Progress =
    Progress(watermark = None, files = Some(planOf(source, config, plan)))

  /** A plan of this source, as this application reads it. */
  private def planOf(source: Source, config: FileConnectorConfig, plan: Json): FilePlan = {
    val files = FilePlan
      .fromJson(plan)
      .fold(
        problem =>
          throw RunFailedException.permanent(
            s"the plan of the batch of ${source.id} is not one that this application reads: it " +
              problem
          ),
        identity
      )
    if (files.directory != config.path)
      throw RunFailedException.permanent(
        s"the plan of the batch of ${source.id} is of the directory ${files.directory}, and the " +
          s"source reads ${config.path}: it was planned under another configuration"
      )
    files
  }

  /**
   * What a look at the files of a plan found, if each is still as it was.
   *
   * @throws SourceChangedException if one is not: the plan is not read
   */
  private def confirmed[A](config: FileConnectorConfig, when: String)(
      look: Either[Vector[Landing.NotAsPlanned], A]
  ): A =
    look match {
      case Right(found) => found
      case Left(problems) =>
        throw new SourceChangedException(
          s"the batch of ${config.path} cannot be read as it was planned: " +
            s"${Landing.files(problems.size)} of it changed $when, and nothing of it was " +
            s"written: ${Landing.some(problems.map(_.toString))}",
          problems.map(problem => SourceChangedException.Changed(problem.name, problem.how))
        )
    }

  /**
   * The rows of the files, lazily, with the column of the records that cannot be parsed for CSV
   * and JSON, and the columns that a read adds.
   */
  private def readable(
      spark: SparkSession,
      format: FileConnectorConfig.Format,
      paths: Vector[Path],
      sourceFileOf: Map[String, String]
  ): Reading = {
    paths.map(_.toString).find(_.exists(Globbed.contains(_))).foreach { _ =>
      throw RunFailedException.permanent(
        "the directory of the files, with its symbolic links resolved, has a character that " +
          s"Spark reads as a pattern (one of $Globbed): it would read other files"
      )
    }
    val locations = paths.map(path => s"file:$path")
    val ofItsFile = udf { (uri: String) =>
      val path = Paths.get(new URI(uri)).toString
      sourceFileOf.getOrElse(
        path,
        throw new IllegalStateException("a file that is not of the plan was read")
      )
    }

    val (read, columns) = format match {
      case FileConnectorConfig.Format.Csv =>
        // The dialect is RFC 4180's: a quote inside a quoted value is the quote twice. Spark's
        // own escape is a backslash, under which such a value keeps every quote it was written
        // with, and a comma after the doubled quote ends it.
        val header = everyFile(spark.read)
          .option("header", "true")
          .option("multiLine", "true")
          .option("quote", CsvQuote)
          .option("escape", CsvQuote)
        val columns = refused(header.csv(locations: _*).schema.fieldNames.toSeq)
        val csv = header
          .option("enforceSchema", "false")
          .option("mode", "PERMISSIVE")
          .option("columnNameOfCorruptRecord", Unparsed)
          .schema(asText(columns))
          .csv(locations: _*)
        (csv, columns)

      case FileConnectorConfig.Format.Json =>
        val columns = refused(fieldsOfJson(spark, locations))
        val json = everyFile(spark.read)
          .option("columnNameOfCorruptRecord", Unparsed)
          .option("mode", "PERMISSIVE")
          .schema(asText(columns))
          .json(locations: _*)
        (json, columns)

      case FileConnectorConfig.Format.Parquet =>
        val parquet = oneNameEach("a file of Parquet") {
          everyFile(spark.read).option("mergeSchema", "true").parquet(locations: _*)
        }
        (parquet, refused(parquet.schema.fieldNames.toSeq))
    }

    // The file of a row, asked for by what it is and not by its name: a source may have a
    // column `_metadata` of its own, which a name would find in place of Spark's.
    val file = read.metadataColumn(FileMetadata).getField("file_path")
    val withFile = read.select(col("*"), ofItsFile(file).as(SourceFile))
    val text = format != FileConnectorConfig.Format.Parquet
    Reading(
      if (text) withFile.withColumn(EngineColumns.RescuedData, lit(null).cast(StringType))
      else withFile,
      placeholder = Option.when(text && columns.isEmpty)(NoColumn)
    )
  }

  /**
   * The fields of the objects of a batch of JSON, each as it was written.
   *
   * They are asked with the case of a name counting, in a session of Spark of this question's
   * own. Asked as the session of a run tells names apart, without their case, two fields that
   * differ only in it come back as one column, under one of the two names, and the read that
   * follows finds the values under that name alone: those under the other are left out of the
   * row, of what is set apart, and of every count. So the two are found here, and refused.
   *
   * @throws RunFailedException a permanent failure, for fields that differ only in their case,
   *   and for a record with a field twice
   */
  private def fieldsOfJson(spark: SparkSession, locations: Seq[String]): Seq[String] = {
    // The settings of the session of the run are not touched: this one is made from it, has
    // its own, and is left to be collected. The jobs that it starts are the thread's, and so
    // the run's, to stop.
    val exact = spark.newSession()
    exact.conf.set("spark.sql.caseSensitive", "true")
    val fields = oneNameEach("a record of JSON") {
      everyFile(exact.read)
        .option("columnNameOfCorruptRecord", Unparsed)
        .option("primitivesAsString", "true")
        .json(locations: _*)
        .schema
        .fieldNames
        .toSeq
        .filterNot(_ == Unparsed)
    }
    val alike = fields
      .groupBy(_.toLowerCase(Locale.ROOT))
      .values
      .collect { case names if names.size > 1 => names.sorted.map(n => s"'$n'").mkString(" and ") }
      .toSeq
      .sorted
    if (alike.nonEmpty)
      throw RunFailedException.permanent(
        "the records of JSON of the batch have fields that differ only in their case: " +
          s"${Landing.some(alike)}. Spark and Delta take such names for one column, and the " +
          "values under one of them would be left out without a word. Nothing was written: " +
          "the fields are to have one name before they get here"
      )
    fields
  }
}

object FileSourceConnector {

  private val log = LoggerFactory.getLogger(classOf[FileSourceConnector])

  private val SourceFile = EngineColumns.SourceFile

  /**
   * The column in which Spark puts a record that it cannot parse, until the record is set apart.
   * A source may not have a column of this name, case aside, as it may not have one of the
   * engine's.
   */
  val Unparsed = "_lakehouse_unparsed"

  /** The quote of CSV, which is its own escape (RFC 4180). */
  private val CsvQuote = "\""

  /** What makes Spark read a path as a pattern. */
  private val Globbed = "*?[]{}\\"

  /**
   * A column of a read whose files have none, as a batch of empty files: Spark does not read a
   * file for the column of what cannot be parsed alone. It is dropped once the files are read.
   */
  private val NoColumn = "_lakehouse_no_column"

  /** Spark's own column of a file source, which has the file of each row. */
  private val FileMetadata = "_metadata"

  /**
   * What is read of a batch, before it is kept.
   *
   * @param placeholder the column that the read made for a batch whose files have none, to be
   *   dropped once the files are read; none, if the files have columns, whatever they are named
   */
  final private case class Reading(rows: DataFrame, placeholder: Option[String])

  /**
   * A reader that passes over no file, whatever its session is set to: one that is corrupt, or
   * that is gone, fails the read (`spark.sql.files.ignoreCorruptFiles`, `ignoreMissingFiles`).
   * A file that a read skipped without a word would be recorded as taken, with its rows not
   * in Bronze. A file that is gone is found by the look after the read as well.
   */
  private def everyFile(reader: DataFrameReader): DataFrameReader =
    reader.option("ignoreCorruptFiles", "false").option("ignoreMissingFiles", "false")

  /**
   * What Spark made of the columns of some files, unless two of them have one name: Spark says
   * so with an error of no class, which here is a failure of the data, and permanent.
   */
  private def oneNameEach[A](what: String)(columns: => A): A =
    try columns
    catch {
      case e: AnalysisException if Option(e.getCondition).contains(SameName) =>
        throw RunFailedException.permanent(
          s"$what of the batch has a field twice, or two that differ only in their case: " +
            s"${firstLine(e.getMessage)} Nothing was written: a field is to have one name, and " +
            "one field that name, before it gets here",
          e
        )
    }

  /** The condition of Spark's error for two columns of one name. */
  private val SameName = "COLUMN_ALREADY_EXISTS"

  private def firstLine(message: String): String =
    Option(message).flatMap(_.linesIterator.map(_.trim).find(_.nonEmpty)).getOrElse("")

  /** The columns as text, and the one for what cannot be parsed. */
  private def asText(columns: Seq[String]): StructType = {
    val some = if (columns.isEmpty) Seq(NoColumn) else columns
    StructType((some :+ Unparsed).map(StructField(_, StringType)))
  }

  /**
   * The columns of the files, if none of them is one that the read or the writer adds: a column
   * of the source under such a name would be taken for the engine's, or the engine's for it
   * (discovery §17, P5). The way out is to rename it (P9).
   */
  private def refused(columns: Seq[String]): Seq[String] = {
    val engine = columns.flatMap { name =>
      EngineColumns
        .reserved(name)
        .orElse(Option.when(name.toLowerCase(Locale.ROOT) == Unparsed)(Unparsed))
        .map(reserved => s"'$name', which is the engine's $reserved")
    }
    if (engine.nonEmpty)
      throw RunFailedException.permanent(
        s"the files have columns that the engine adds to every row, case aside: " +
          s"${engine.mkString(", ")}. Nothing was written"
      )
    columns
  }

  /** `_source_file` of each path that is read, by the path as Spark gives it, decoded. */
  private def sourceFiles(plan: FilePlan, paths: Vector[Path]): Map[String, String] =
    paths
      .zip(plan.files)
      .map { case (path, file) => path.toString -> s"${plan.directory}/${file.name}" }
      .toMap

  /** Whether a failure of a read is that of a file of CSV whose header is not the others'. */
  @tailrec
  private[files] def otherHeader(e: Throwable): Boolean = e match {
    case null => false
    case spark: SparkThrowable if Option(spark.getCondition).contains(HeaderCondition) => true
    case other => otherHeader(other.getCause)
  }

  /**
   * The condition of Spark's error for a header of CSV that is not the schema's
   * (`CSVHeaderChecker`, with `enforceSchema` off). A legacy one, which a version of Spark may
   * give a name: the test of files with two headers fails then, and says so.
   */
  private val HeaderCondition = "_LEGACY_ERROR_TEMP_3241"
}
