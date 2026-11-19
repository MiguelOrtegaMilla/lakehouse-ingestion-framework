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
import lakehouse.ingestion.engine.connector.{PlanContext, ReadContext, ReadResult, SourceConnector}
import org.apache.spark.SparkThrowable
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Files that are left in a directory of the landing zone (`type: file`).
 *
 * '''A plan''' is the files of the directory that are new, complete and of the source
 * (`FilePlanner`), each with which file of the file system it was when the directory was listed
 * (`Landing`).
 *
 * '''A read''' reads the files of a plan and no other, as the source wrote them:
 *
 *  - '''CSV and JSON as text''' (ADR-0008): every column is a string, as it was written, and
 *    `_rescued_data` is there for what does not fit the columns, which the policy of a
 *    source's schema fills (P9). The columns of CSV are those of the header, which every file
 *    of the batch must have: Spark reads a record by the place of its values, and a file with
 *    other columns would put values under another column's name. Values may span lines when
 *    they are quoted (RFC 4180). The columns of JSON are those of the objects of the batch; a
 *    value that is an object or a list is kept as the JSON it was written as. JSON is one object
 *    to a line.
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
 * '''The files are the planned ones while they are read.''' A name may be given to another file
 * at any moment. So, just before Spark reads the files, each name must still lead to the file
 * that was planned, of the same size and the same time (`Landing.confirm`); Spark then reads
 * them all, and what it read is kept, by Spark, apart from the files
 * (`Dataset.localCheckpoint`); and just after, each name must still lead to that file. If one
 * does not, the read fails, before anything of it is written. What is kept is what everything
 * after the read is made from: the data, the records set apart, and their counts, which are
 * then of one read. A copy that Spark loses is not read again from the files: the job that needs
 * it fails, and the batch with it.
 *
 * '''What that leaves open.''' A file that is changed and then put back as it was between the
 * two looks, or changed in its place with its size and its time put back, is read as it was
 * while it was read, and not seen to have changed: telling that needs what the file holds,
 * read twice. It takes a producer that means to do it. The way to close it is to take a file
 * out of the producer's reach before it is read, by moving it where only the engine writes, or
 * to read from storage where an object never changes once written (E2). The device of a file
 * is that of the machine that planned it, so a plan is read on the machine whose landing zone
 * it is (one machine, in the deployments of the MVP).
 *
 * A plan that cannot be read as it was planned fails as permanent (4): a run that reads it again
 * finds the same. The batch stays planned, for the operator (P7).
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
          s"${source.id}: planned ${plan.planned.size} files of ${config.path}, " +
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
  ): ReadResult = {
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

    val paths = confirmed(config, files, "since it was planned")
    val read = readable(context.spark, config.format, paths, sourceFiles(files, paths))
    // What is read is kept by Spark apart from the files, and everything after is made from it.
    val kept =
      try read.localCheckpoint(eager = true)
      catch {
        case e: Exception if otherHeader(e) =>
          throw RunFailedException.permanent(
            s"the files of CSV of the batch of ${config.path} do not all have one header: Spark " +
              "reads a record by the place of its values, and would put them under another " +
              "column's name. Nothing was written",
            e
          )
      }
    afterRead()
    confirmed(config, files, "while it was read")

    if (config.format == FileConnectorConfig.Format.Parquet) ReadResult(kept, unparsed = None)
    else
      ReadResult(
        rows = kept.filter(col(Unparsed).isNull).drop(Unparsed, NoColumn),
        unparsed = Some(
          kept
            .filter(col(Unparsed).isNotNull)
            .select(col(EngineColumns.SourceFile), col(Unparsed).as(ReadResult.Record))
        )
      )
  }

  /** The paths of the files of a plan, if each is still the file that was planned. */
  private def confirmed(config: FileConnectorConfig, plan: FilePlan, when: String): Vector[Path] =
    landing.confirm(config, plan) match {
      case Right(paths) => paths
      case Left(problems) =>
        throw RunFailedException.permanent(
          s"the batch of ${config.path} cannot be read as it was planned: " +
            s"${Landing.files(problems.size)} of it changed $when, and nothing of it was " +
            s"written: ${Landing.some(problems)}"
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
  ): DataFrame = {
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

    val read = format match {
      case FileConnectorConfig.Format.Csv =>
        val header = spark.read.option("header", "true").option("multiLine", "true")
        val columns = refused(header.csv(locations: _*).schema.fieldNames.toSeq)
        header
          .option("enforceSchema", "false")
          .option("mode", "PERMISSIVE")
          .option("columnNameOfCorruptRecord", Unparsed)
          .schema(asText(columns))
          .csv(locations: _*)

      case FileConnectorConfig.Format.Json =>
        val unparsed = spark.read.option("columnNameOfCorruptRecord", Unparsed)
        val columns = refused(
          unparsed
            .option("primitivesAsString", "true")
            .json(locations: _*)
            .schema
            .fieldNames
            .toSeq
            .filterNot(_ == Unparsed)
        )
        unparsed.option("mode", "PERMISSIVE").schema(asText(columns)).json(locations: _*)

      case FileConnectorConfig.Format.Parquet =>
        val parquet = spark.read.option("mergeSchema", "true").parquet(locations: _*)
        refused(parquet.schema.fieldNames.toSeq)
        parquet
    }

    val withFile = read.select(col("*"), ofItsFile(col("_metadata.file_path")).as(SourceFile))
    if (format == FileConnectorConfig.Format.Parquet) withFile
    else withFile.withColumn(EngineColumns.RescuedData, lit(null).cast(StringType))
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

  /** What makes Spark read a path as a pattern. */
  private val Globbed = "*?[]{}\\"

  /**
   * A column of a read whose files have none, as a batch of empty files: Spark does not read a
   * file for the column of what cannot be parsed alone. It is dropped once the files are read.
   */
  private val NoColumn = "_lakehouse_no_column"

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
      .zip(plan.planned)
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
