package lakehouse.ingestion.engine.files

import java.net.URI
import java.nio.file.{Path, Paths}
import java.util.Locale

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.control.NonFatal

import io.circe.Json
import lakehouse.ingestion.core.config.{
  ConnectorKind,
  CsvDialect,
  EngineColumns,
  FileConnector,
  FileConnectorConfig,
  Profile,
  RelativePath,
  Source
}
import lakehouse.ingestion.core.control.Quarantine
import lakehouse.ingestion.core.files.FilePlanner.{NothingNew, Planned}
import lakehouse.ingestion.core.files.{FileHorizon, FilePlan, FilePlanner, LandedFile, ReadAs}
import lakehouse.ingestion.engine.failure.RunFailedException
import lakehouse.ingestion.engine.connector.{
  PlanContext,
  Progress,
  ReadContext,
  ReadResult,
  SourceChangedException,
  SourceConnector,
  UnreadableFilesException
}
import org.apache.spark.SparkThrowable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, explode, lit, udf}
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
 *    other columns would put values under another column's name. The dialect is the plan's
 *    (below), RFC 4180's in UTF-8 unless the source says its own (`connector.csv`): a value is
 *    quoted if it has the delimiter, the quote or the end of a line in it, and a quote inside
 *    it is written twice, or after the escape of the source. Values over several lines are
 *    always read. The columns of JSON are those of the first level of the records of the
 *    batch, each under its name as it was written; a value that is an object or a list is kept
 *    as the JSON it was written as.
 *  - '''JSON is one record to a line''', read as text, and each line read by Jackson as
 *    Spark's reader of files reads a record, each value as it was written (`JsonLines`, P8.8):
 *    a record is one whole object, each field of its first level once, and nothing after it
 *    but spaces. A blank line is passed over, as Spark passes over it. A record with text
 *    after its object (`trailing_text`), and one with a field twice at its first level
 *    (`duplicate_field`), are set apart with why: of the first, Spark would take the object and
 *    drop the rest, and of the second keep one of the two values, both without a word. A line
 *    that is no whole object is set apart as one that cannot be parsed: a line of a record
 *    written over several lines, and a list, of which Spark's reader made rows up to P8.8.
 *  - '''Parquet with its own types''', and the columns of every file of the batch: a column
 *    that only some files have is null in the rows of the others. Two files with one column
 *    of two types fail the read.
 *  - '''A record that cannot be parsed''' is set apart, with its text as it was (`unparsed`):
 *    a line that is not a record of JSON, a record of CSV with more values or fewer than the
 *    header has. A
 *    quote of CSV that is not closed is not one: the parser takes the rest of the file into
 *    that value, which is then kept as it was read, for Silver and its checks to see.
 *  - Every row says the file that it was read from (`_source_file`), as `<directory>/<name>`
 *    under the landing root: where the source left it, which is how `processed_file` records
 *    it.
 *
 * '''A plan says how its files are read''' (ADR-0024 §3): their format and, for CSV, the
 * dialect of the source when it was planned (`ReadAs`). Every read of the plan reads so, the reads
 * of a file alone among them, whatever the configuration says by then, and the run says so when
 * the two differ: a plan is replayed exactly, and a batch whose records set apart were written
 * before its process died is read again as they were. A plan written before plans said it is
 * read in the format of the source, and for CSV in RFC 4180 and UTF-8, which was then the one
 * dialect. A reprocess reads the files of a batch as the source says now (`again`).
 *
 * '''A charset that Spark refuses is the session's, and permanent''': windows-1252 is read only in a
 * session that may read any charset of the JDK, which every session of a run is (ADR-0024 §2).
 * Such a refusal is no file's, and is never taken for a file that cannot be read.
 *
 * '''The names of the columns are the source's, and no value is lost for its name.''' Spark
 * and Delta tell the names of columns apart without their case, and a read that left that to
 * them would lose what a source sent, without a word:
 *
 *  - ''fields of JSON that differ only in their case'', as `id` in one record and `ID` in
 *    another, would be read as one column, with the values under one of the two names left
 *    out. They fail the batch, as permanent, naming both: which of the two a column is to be
 *    is the policy of a schema (P9). A record with a field twice is set apart (above). Inside
 *    an object nothing is told apart: it is kept as its text;
 *  - ''a header of CSV with a name twice, case aside, or with an empty name'' is read by Spark
 *    under names of its own making, which no file then has: the batch fails, as permanent, and
 *    says so, as it does for files whose headers differ;
 *  - ''a column named `_metadata`'' is a column of the source like any other. It is the name
 *    under which Spark gives the file of a row, which is asked for in the way that a column of
 *    the source does not hide (`Dataset.metadataColumn`).
 *
 * '''No file is passed over without a word, whatever the session is set to.''' With
 * `spark.sql.files.ignoreCorruptFiles`, Spark passes over a file that it cannot read, or keeps
 * what it had read of it, and says nothing: the file would be recorded as taken with its rows
 * not in Bronze. Every read says that no file is passed over, which a setting of a session
 * does not change.
 *
 * '''A file that cannot be read stops its source, or is skipped, as the source says'''
 * (`connector.on_unreadable_file`, ADR-0023). What Spark raises does not say which file of a
 * batch it is, nor whether one is: its conditions are several or none, at the inference of the
 * columns or at the checkpoint, and it says that a file that it may not open does not exist
 * (`SparkUnreadableFileContractSpec`). What a file does alone says it, the same way at every
 * read. So once a read of a batch fails with what the read does not know:
 *
 *  - the files are looked at again, as after every read: one that changed makes it a batch
 *    whose source changed (ADR-0018), whatever Spark said;
 *  - each file is read alone, as the batch is read, up to `AloneReads` times, those that failed
 *    again after a wait that grows (`AloneWaits`), within the budget of the run. One that fails
 *    at every read, with what the read does not know, cannot be read. One that fails and then
 *    reads is sound, and the failure of the batch is one that passed: the run tries again. One
 *    that fails alone with a failure that the read knows (a header, a field twice) is not one
 *    that cannot be read: that failure is the file's, and is met by the read of a batch;
 *  - the files are looked at again, after the reads alone.
 *
 * Under `fail` the files that cannot be read are a permanent failure that names each, and why;
 * under `skip` the read says them (`UnreadableFilesException`), and the engine gives the batch
 * up and records them. If none cannot be read and none passed, the failure of the batch is let
 * through as it was. A file that the landing zone refuses (outside the root, a link to nothing,
 * not a file) is one that cannot be read too, under `skip`, found just before the read: none of
 * the files of its plan is then read, and it is never opened (`Landing`).
 *
 * '''A file whose bytes are not of its encoding cannot be read either''' (ADR-0024 §4 and §5).
 * Spark reads such a byte as U+FFFD and says nothing. So once it has read a batch of CSV or of
 * JSON, every byte of each of its files is decoded once more, by a decoder that reports what
 * it cannot decode (`FileBytes`): CSV in the encoding of its plan, JSON in UTF-8. A file with a
 * byte that is not of it, with a byte of 0x80 to 0x9F under ISO-8859-1, or with the mark of
 * UTF-8 under an encoding of one byte to a character, is one that cannot be read, under the
 * same policy; it is said with where the byte is, and never with the byte. A read that fails
 * for the names of its batch (a header that is not the others', a name that no producer gave)
 * has the bytes looked at before it says so: a file that is not of its encoding is why, and
 * is what is said.
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
 * @param afterRead what runs once Spark has read the files and before they are confirmed again
 *   and their bytes looked at, whether the read went well or Spark stopped it with a failure of
 *   the run: nothing, but in the tests that change a file at that moment
 * @param beforeAlone what runs before each read of a file alone, given its name and which read
 *   it is, from 1: nothing, but in the tests that mend a file between two reads
 */
final class FileSourceConnector(
    landing: Landing,
    afterRead: () => Unit = () => (),
    beforeAlone: (String, Int) => Unit = (_, _) => ()
) extends SourceConnector[FileConnectorConfig] {

  import FileSourceConnector._

  override val kind: ConnectorKind = FileConnector

  override def plan(
      source: Source,
      config: FileConnectorConfig,
      context: PlanContext
  ): Option[Json] = {
    val listed = landing.list(config)
    val taken = context.files.processed(config.path, listed.map(_.name))
    val horizon = context.horizon.map { document =>
      FileHorizon
        .fromJson(document)
        .fold(
          problem =>
            throw RunFailedException.permanent(
              s"the horizon that ${source.id} was given to plan against is not one that this " +
                s"application wrote, and nothing was planned: it $problem"
            ),
          identity
        )
    }
    FilePlanner.plan(config, listed, taken, context.now, horizon) match {
      case Left(problems) =>
        throw RunFailedException.permanent(
          s"the directory ${config.path} of the landing zone has files whose names no run may " +
            s"read, and nothing was planned: ${problems.toChain.toList.mkString("; ")}"
        )
      case Right(NothingNew(waiting)) =>
        log.info(s"${source.id}: no new file is complete in ${config.path}; $waiting not yet")
        None
      case Right(Planned(plan, waiting, leftOver)) =>
        val chain = plan.horizon.fold("")(at =>
          s"; the invocation takes ${at.remaining} more of those changed by ${at.completeBy}"
        )
        log.info(
          s"${source.id}: planned ${plan.files.size} files of ${config.path}, " +
            s"${plan.bytes} bytes; $waiting not complete yet, $leftOver complete beyond it$chain"
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
    val as = readAsOf(source, config, files)
    val before = confirmed(config, "since it was planned")(landing.confirm(config, files))
    // A file that no run may open is never opened, and nor is any other of its plan.
    if (before.refused.nonEmpty)
      throw unreadable(config, before.refused.map { case (file, why) => file -> why.toString })
    val each = files.files.zip(before.paths)
    val read =
      try readOf(context.spark, config, as, files.directory, each)
      catch {
        // A file with a line too long cannot be read, unless it changed since it was planned.
        case long: LinesTooLong =>
          confirmed(config, "while its lines were looked at")(
            landing.confirmAgain(config, files, before)
          )
          throw unreadable(config, long.files)
        // What a read refuses of the names of a batch may be what a file that is not of its
        // encoding was read as: a header that is not the others', for one. Such a file is
        // what is said, and it is the source's to say what becomes of it.
        case known: RunFailedException =>
          afterRead()
          throw looked(config, as, files, before, each, context) match {
            case Right(notOf) if notOf.nonEmpty => unreadableAs(config, as, notOf, known)
            case Right(_) => known
            // A look that could not be made says nothing of the files, and the failure of the
            // read is what is known of the batch: it keeps its class, and what the look met
            // goes with it. A file that cannot be read is no reason to try a refusal again.
            case Left(failed) =>
              log.warn(
                s"the read of the batch of ${config.path} failed as ${known.failure} " +
                  s"(${said(known)}), and the bytes of its files could not be looked at after " +
                  s"it (${said(failed)}): the failure of the read is reported, as it was"
              )
              known.addSuppressed(failed)
              known
          }
        case refused: Exception if charsetRefused(refused) =>
          throw RunFailedException.permanent(
            s"Spark refuses to read the files of ${config.path} in ${as.describe}: the session " +
              s"of the run does not read that encoding (${Profile.EngineSpark.JavaCharsets} is " +
              "not on), which the engine has every session of a run do (ADR-0024 §2). Nothing " +
              "was read",
            refused
          )
        case NonFatal(failed) => throw alone(config, as, files, before, each, failed, context)
      }
    afterRead()
    val cannot = notOfItsEncoding(config, as, files, before, each, context)
    if (cannot.nonEmpty) throw unreadableAs(config, as, cannot)

    if (as.format == FileConnectorConfig.Format.Parquet)
      ReadResult.Kept(read.rows, unparsed = None)
    else
      ReadResult.Kept(
        // The column that stood for the columns of a batch that has none goes with it. A
        // column of the source is kept, whatever it is named.
        rows = read.rows.filter(col(Unparsed).isNull).drop(Unparsed +: read.placeholder.toSeq: _*),
        unparsed = Some(unparsedOf(read))
      )
  }

  /**
   * The records that a read set apart: those that Spark could not parse, and those that the
   * look at the lines of JSON set apart before Spark read them, each with why.
   */
  private def unparsedOf(read: Reading): DataFrame = {
    val cannot = read.rows
      .filter(col(Unparsed).isNotNull)
      .select(
        col(EngineColumns.SourceFile),
        col(Unparsed).as(ReadResult.Record),
        lit(Quarantine.ErrorType.Unparsed).as(ReadResult.ErrorType),
        lit(null).cast(StringType).as(ReadResult.ErrorDetail)
      )
    read.apart.fold(cannot)(cannot.unionByName(_))
  }

  /**
   * How the files of a plan are read: as the plan says, and as the source said when a plan of
   * before P8.9 was written. A configuration that says otherwise by now is said, and not
   * followed: a plan is replayed exactly (ADR-0024 §3).
   */
  private def readAsOf(source: Source, config: FileConnectorConfig, files: FilePlan): ReadAs = {
    val as = files.readAs.getOrElse(ReadAs.before(config.format))
    val now = ReadAs.of(config)
    // Said at the read, which does not know whether the run will commit the batch: the run
    // that reads it commits it, unless it fails, and a batch committed is not one to give up.
    if (as != now)
      log.warn(
        s"${source.id}: the batch is read as it was planned, as ${as.describe}, and the source " +
          s"now says ${now.describe}. A plan is read as it was written. To have its files read " +
          "as the source says now: once the batch is committed, as the run that reads it " +
          "commits it, reprocess it (reprocess), whose plan takes them so; while it is planned, " +
          "if the run fails before its commit, give it up (abandon-batch), and the next plan " +
          "takes them so"
      )
    as
  }

  /** The same files, read as the source says now: the plan of a reprocess (ADR-0024 §3). */
  override def again(source: Source, config: FileConnectorConfig, plan: Json): Json = {
    val files = planOf(source, config, plan)
    FilePlan(files.directory, files.files, files.horizon, Some(ReadAs.of(config))).toJson
  }

  /** The files of a plan are what its commit records: each is taken, and never again. */
  override def progress(source: Source, config: FileConnectorConfig, plan: Json): Progress =
    Progress(watermark = None, files = Some(planOf(source, config, plan)))

  /** More while the chain has files left of those complete at its first plan; none for a plan of version 1. */
  override def more(source: Source, config: FileConnectorConfig, plan: Json): Option[Json] =
    planOf(source, config, plan).next.map(_.toJson)

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
   * The files read, at both moments: their columns are inferred, and what is read of them is
   * kept by Spark apart from the files (`Dataset.localCheckpoint`), which everything after is
   * made from. For CSV and Parquet that is their rows; for JSON, its lines, which are read once
   * and looked at before their fields are (`jsonOf`).
   *
   * @param each the files, each with where it is, as the look before the read found it
   * @return what was read, made from what was kept
   */
  private def readOf(
      spark: SparkSession,
      config: FileConnectorConfig,
      as: ReadAs,
      directory: RelativePath,
      each: Vector[(LandedFile, Path)]
  ): Reading = {
    val paths = each.map(_._2)
    paths.map(_.toString).find(_.exists(Globbed.contains(_))).foreach { _ =>
      throw RunFailedException.permanent(
        "the directory of the files, with its symbolic links resolved, has a character that " +
          s"Spark reads as a pattern (one of $Globbed): it would read other files"
      )
    }
    val sourceFileOf = each.map { case (file, path) =>
      path.toString -> s"${directory.value}/${file.name.value}"
    }.toMap
    if (as.format == FileConnectorConfig.Format.Json) {
      val locations = paths.map(path => s"file:$path")
      linesWithin(spark, each, locations)
      jsonOf(spark, locations, sourceFileOf)
    } else kept(config, readable(spark, as, paths, sourceFileOf))
  }

  /**
   * The lines of the files of JSON of a read, each no longer than the bound, looked at before
   * Spark reads them (`LineLengths`): Spark holds a line whole, and a file that is one document
   * of JSON on one line, or a line of some hundreds of megabytes, would end the process for
   * want of memory. A look that fails is a read that fails, of a file that cannot be read.
   *
   * @throws LinesTooLong the files with a line too long, each with why
   */
  private def linesWithin(
      spark: SparkSession,
      each: Vector[(LandedFile, Path)],
      locations: Vector[String]
  ): Unit = {
    val long = each.map(_._1).zip(LineLengths.tooLong(spark, locations, MaxValueLength)).collect {
      case (file, Some(line)) => file -> LineLengths.why(line, MaxValueLength)
    }
    if (long.nonEmpty) throw new LinesTooLong(long)
  }

  /** The rows of a read of CSV or Parquet, kept by Spark apart from the files. */
  private def kept(config: FileConnectorConfig, read: Reading): Reading = {
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
    read.copy(rows = kept)
  }

  /**
   * The lines of the files of a batch of JSON, read as text once and kept, each read as a record
   * by Jackson, as Spark's reader of files would read it (`JsonLines`): a blank line is passed
   * over, as Spark passes over it; a record is a row; and every other line is set apart, with
   * why. The columns are the names of the first level of the records, each as it was written.
   */
  private def jsonOf(
      spark: SparkSession,
      locations: Seq[String],
      sourceFileOf: Map[String, String]
  ): Reading = {
    val text = everyFile(spark.read).text(locations: _*)
    val file = text.metadataColumn(FileMetadata).getField("file_path")
    val lines = text
      .select(col("value").as(Line), ofItsFile(sourceFileOf)(file).as(SourceFile))
      .withColumn(Look, lookedAt(col(Line)))
      .filter(col(s"$Look.kind") =!= Kind.Blank)
      .localCheckpoint(eager = true)

    val columns = refused(fieldsOfJson(lines))
    val kind = col(s"$Look.kind")
    val values = col(s"$Look.values")
    val rows = lines
      .filter(kind === Kind.Record)
      .select(
        columns.map(name => values.getItem(name).as(name)) ++ Seq(
          lit(null).cast(StringType).as(Unparsed),
          col(SourceFile),
          lit(null).cast(StringType).as(EngineColumns.RescuedData)
        ): _*
      )
    val apart = lines
      .filter(kind =!= Kind.Record)
      .select(
        col(SourceFile),
        col(Line).as(ReadResult.Record),
        kind.as(ReadResult.ErrorType),
        col(s"$Look.detail").as(ReadResult.ErrorDetail)
      )
    Reading(rows, placeholder = None, apart = Some(apart))
  }

  /**
   * The files of a plan whose bytes are not of the encoding that they were read by, each with
   * why, once Spark has read them whole (`looked`). A look at the bytes that could not be made,
   * with no file changed, is a failure that passes: the read went well, and nothing else is
   * known of the batch.
   *
   * @throws SourceChangedException if a file of the plan changed by the look after the read
   * @throws RunFailedException a transient failure, if the bytes could not be looked at
   */
  private def notOfItsEncoding(
      config: FileConnectorConfig,
      as: ReadAs,
      files: FilePlan,
      before: Landing.Confirmed,
      each: Vector[(LandedFile, Path)],
      context: ReadContext
  ): Vector[(LandedFile, String)] =
    looked(config, as, files, before, each, context) match {
      case Right(cannot) => cannot
      case Left(failed) =>
        throw RunFailedException.transient(
          s"the files of the batch of ${config.path} were read, and their bytes could not be " +
            s"looked at for those that are not of their encoding (${said(failed)}), with no " +
            "file changed: a failure that passed. Nothing was written",
          failed
        )
    }

  /**
   * The files of a plan whose bytes are not of the encoding that they were read by, each with
   * why (ADR-0024 §4 and §5), and the look at the files after the read (`Landing.confirmAgain`).
   * CSV is of the encoding of its plan, and JSON of UTF-8; Parquet has no text to decode.
   *
   * It is asked once Spark has read the files, so that those it opens are files that Spark
   * read whole, and before the look at the files, so that the bytes looked at are those of the
   * files that were planned, as the bytes read are. A look at the bytes that fails is of no
   * file: a file that changed makes it a batch whose source changed (ADR-0018), and otherwise
   * it is what the look met, for the caller to say what it comes to.
   *
   * @return the files that are not of their encoding, or the failure of a look that could not
   *   be made
   * @throws SourceChangedException if a file of the plan changed by the look after the read
   */
  private def looked(
      config: FileConnectorConfig,
      as: ReadAs,
      files: FilePlan,
      before: Landing.Confirmed,
      each: Vector[(LandedFile, Path)],
      context: ReadContext
  ): Either[Throwable, Vector[(LandedFile, String)]] = {
    val found =
      if (as.format == FileConnectorConfig.Format.Parquet) Right(Vector.empty)
      else {
        // No job is started by a run that has lost its lease.
        context.verify()
        val encoding = as.csv.fold[CsvDialect.Encoding](CsvDialect.Encoding.Utf8)(_.encoding)
        try
          Right(
            FileBytes.notOfEncoding(context.spark, each.map(file => s"file:${file._2}"), encoding)
          )
        catch { case NonFatal(failed) => Left(failed) }
      }
    confirmed(config, "while it was read")(landing.confirmAgain(config, files, before))
    found.map { whys =>
      each.map(_._1).zip(whys).collect { case (file, Some(why)) => file -> why }
    }
  }

  /**
   * Files that are not of their encoding, as the policy of the source has files that cannot
   * be read (ADR-0024 §4), with what to do about them: which is not the same for a source that
   * may say its encoding as for one of JSON, nor for a batch that stays planned as for one
   * that is given up.
   *
   * Under `fail` it is said once, after the files. Under `skip` it is said of each file, with
   * why: that is what its record keeps (`processed_file`), and what the warning for it says.
   */
  private def unreadableAs(
      config: FileConnectorConfig,
      as: ReadAs,
      cannot: Seq[(LandedFile, String)],
      cause: Throwable = null
  ): Exception = {
    val csv = as.format == FileConnectorConfig.Format.Csv
    val encoding = s"connector.${FileConnector.Csv}.${CsvDialect.EncodingField}"
    config.onUnreadableFile match {
      case FileConnectorConfig.OnUnreadableFile.Fail =>
        val toDo =
          if (csv)
            s"If it is the encoding of the source that is wrong, correct $encoding and give " +
              "the batch up (abandon-batch): a batch is read as it was planned, and the next " +
              "plan takes its files as the source says by then"
          else
            "JSON is read as UTF-8, and a source has no field to say another encoding: such a " +
              "file is to be written again, in UTF-8"
        unreadable(config, cannot, cause, Some(toDo))
      case FileConnectorConfig.OnUnreadableFile.Skip =>
        val toDo =
          if (csv)
            s"To have it taken again, correct $encoding, if it is the encoding of the source " +
              "that is wrong, and touch the file (touch)"
          else "JSON is read as UTF-8: the file is to be written again, in UTF-8"
        unreadable(config, cannot.map { case (file, why) => file -> s"$why. $toDo" }, cause)
    }
  }

  /**
   * What a read of a batch that failed with `failed` comes to, once each of its files is read
   * alone (ADR-0023 §3): the files that cannot be read, under the policy of the source; a
   * failure that passed, if a file failed and then read; or `failed` as it was.
   *
   * @throws SourceChangedException if a file of the plan changed by the look after the read, or
   *   after the reads alone: whatever Spark said, the plan is what is wrong (ADR-0018)
   */
  private def alone(
      config: FileConnectorConfig,
      as: ReadAs,
      files: FilePlan,
      before: Landing.Confirmed,
      each: Vector[(LandedFile, Path)],
      failed: Throwable,
      context: ReadContext
  ): Throwable = {
    // Spark says that a file which it may not open does not exist: what is gone is known by a
    // look at the files, and not by what Spark raised.
    confirmed(config, "while it was read")(landing.confirmAgain(config, files, before))
    log.warn(
      s"the batch of ${config.path} could not be read (${said(failed)}): each of its " +
        s"${Landing.files(each.size)} is read alone, to find those that cannot be read"
    )
    val found = readAlone(config, as, files.directory, each, context)
    confirmed(config, "while its files were read alone")(
      landing.confirmAgain(config, files, before)
    )

    val cannot = found.collect { case (file, Alone.Unreadable(why)) => file -> why }
    val passed = found.collect { case (file, Alone.Passed(failures)) =>
      s"'${file.name}' failed ${times(failures)} and then read"
    }
    if (cannot.nonEmpty) unreadable(config, cannot, failed)
    else if (passed.nonEmpty)
      RunFailedException.transient(
        s"the batch of ${config.path} could not be read (${said(failed)}), and read alone, " +
          s"${Landing.some(passed)}: a failure that passed. Nothing was written",
        failed
      )
    else {
      log.warn(
        s"the batch of ${config.path} could not be read, and each of its files reads alone: " +
          "the failure is not a file's"
      )
      failed
    }
  }

  /**
   * What each file comes to, read alone up to `AloneReads` times, in rounds: each file once, and
   * those that failed again after a wait, longer each time (`AloneWaits`), which the run waits
   * within its budget (`ReadContext.pause`). A wait is for a round, and not for each file: a
   * plan whose files all fail waits no longer than one with a file that fails.
   *
   * @return each file, with what it came to, in the order of `each`
   */
  private def readAlone(
      config: FileConnectorConfig,
      as: ReadAs,
      directory: RelativePath,
      each: Vector[(LandedFile, Path)],
      context: ReadContext
  ): Vector[(LandedFile, Alone)] = {
    // What one read of a file alone found, at the read `n`: none, for a file to be read again.
    def once(file: LandedFile, path: Path, n: Int): Option[Alone] = {
      // No job is started by a run that has lost its lease.
      context.verify()
      beforeAlone(file.name.value, n)
      try {
        readOf(context.spark, config, as, directory, Vector(file -> path))
        Some(if (n == 1) Alone.Read else Alone.Passed(n - 1))
      } catch {
        // A value, or a line, too long is so at every read: the file cannot be read, from its
        // first.
        case long: LinesTooLong => Some(Alone.Unreadable(long.files.head._2))
        case _: RunFailedException => Some(Alone.Known)
        case NonFatal(failed) if tooLong(failed) =>
          Some(Alone.Unreadable(s"$Unreadable: $TooLong"))
        case NonFatal(failed) if n == AloneReads =>
          Some(
            Alone.Unreadable(
              s"$Unreadable: it failed each of $AloneReads reads of it alone: ${said(failed)}"
            )
          )
        case NonFatal(_) => None
      }
    }

    @tailrec
    def round(n: Int, left: Vector[Int], found: Map[Int, Alone]): Map[Int, Alone] =
      if (left.isEmpty) found
      else {
        if (n > 1) context.pause(AloneWaits(n - 2))
        val read = left.map { index =>
          val (file, path) = each(index)
          index -> once(file, path, n)
        }
        round(
          n + 1,
          read.collect { case (index, None) => index },
          found ++ read.collect { case (index, Some(alone)) => index -> alone }
        )
      }

    val found = round(1, each.indices.toVector, Map.empty)
    each.indices.toVector.map(index => each(index)._1 -> found(index))
  }

  /**
   * Files of a plan that cannot be read, as the policy of the source has them: a permanent
   * failure that names each, under `fail`; under `skip`, what the engine gives the batch up by.
   *
   * @param cannot each file, with why: never the text of an exception
   * @param toDo what more there is to do about such files than what the policy says, if anything
   */
  private def unreadable(
      config: FileConnectorConfig,
      cannot: Seq[(LandedFile, String)],
      cause: Throwable = null,
      toDo: Option[String] = None
  ): Exception = {
    val named = cannot.map { case (file, why) => s"'${file.name}' $why" }
    val more = toDo.fold("")(what => s". $what")
    config.onUnreadableFile match {
      case FileConnectorConfig.OnUnreadableFile.Fail =>
        RunFailedException.permanent(
          s"the batch of ${config.path} has ${Landing.files(cannot.size)} that cannot be read, " +
            s"and nothing of it was written: ${Landing.some(named)}. The source fails for such " +
            s"a file (connector.${FileConnector.OnUnreadableFile}: fail), until it is written " +
            "again, which the next run takes as a new file; or until the source skips such " +
            s"files$more",
          cause
        )
      case FileConnectorConfig.OnUnreadableFile.Skip =>
        new UnreadableFilesException(
          s"${Landing.files(cannot.size)} of the batch of ${config.path} cannot be read, and " +
            s"the source skips such files (connector.${FileConnector.OnUnreadableFile}: skip): " +
            s"${Landing.some(named)}. Nothing of the batch was written$more",
          cannot.map { case (file, why) =>
            UnreadableFilesException.Unreadable(file.name.value, why)
          }
        )
    }
  }

  /**
   * The rows of the files, lazily, with the column of the records that cannot be parsed for CSV
   * and JSON, and the columns that a read adds.
   */
  private def readable(
      spark: SparkSession,
      as: ReadAs,
      paths: Vector[Path],
      sourceFileOf: Map[String, String]
  ): Reading = {
    val locations = paths.map(path => s"file:$path")

    val (read, columns) = as.format match {
      case FileConnectorConfig.Format.Csv =>
        // The dialect of the plan, each of its fields given to Spark, its defaults included:
        // Spark's own escape is a backslash, under which a quote doubled, as RFC 4180 writes
        // one, is no quote (SparkCsvDialectContractSpec). Values over several lines are always
        // read: without them a quoted value with the end of a line in it is two records.
        val dialect = as.csv.getOrElse(CsvDialect.Default)
        val header = everyFile(spark.read)
          .option("header", "true")
          .option("multiLine", "true")
          .option("sep", dialect.delimiter.toString)
          .option("quote", dialect.quote.toString)
          .option("escape", dialect.escape.toString)
          .option("encoding", dialect.encoding.name)
          .option("maxCharsPerColumn", MaxValueLength.toString)
        val columns = refused(ofAProducer(header.csv(locations: _*).schema.fieldNames.toSeq))
        val csv = header
          .option("enforceSchema", "false")
          .option("mode", "PERMISSIVE")
          .option("columnNameOfCorruptRecord", Unparsed)
          .schema(asText(columns))
          .csv(locations: _*)
        (csv, columns)

      case FileConnectorConfig.Format.Json =>
        throw new IllegalArgumentException("JSON is read by its lines (jsonOf)")

      case FileConnectorConfig.Format.Parquet =>
        val parquet = oneNameEach("a file of Parquet") {
          oneTypeEach {
            everyFile(spark.read).option("mergeSchema", "true").parquet(locations: _*)
          }
        }
        (parquet, refused(parquet.schema.fieldNames.toSeq))
    }

    // The file of a row, asked for by what it is and not by its name: a source may have a
    // column `_metadata` of its own, which a name would find in place of Spark's.
    val file = read.metadataColumn(FileMetadata).getField("file_path")
    val withFile = read.select(col("*"), ofItsFile(sourceFileOf)(file).as(SourceFile))
    val text = as.format != FileConnectorConfig.Format.Parquet
    Reading(
      if (text) withFile.withColumn(EngineColumns.RescuedData, lit(null).cast(StringType))
      else withFile,
      placeholder = Option.when(text && columns.isEmpty)(NoColumn)
    )
  }

  /**
   * The fields of the records of a batch of JSON, each as it was written: the names of the
   * first level of each record, as the look at its line found them, and in the order of their
   * names, as Spark's inference orders them.
   *
   * Spark and Delta tell the names of columns apart without their case, and Spark's reader
   * finds a field of a line by its name, with its case: two fields that differ only in it would
   * be one column, under one of the two names, and the values under the other would be left out
   * of the row, of what is set apart, and of every count. So the two are found here, and refused.
   *
   * @throws RunFailedException a permanent failure, for fields that differ only in their case
   */
  private def fieldsOfJson(lines: DataFrame): Seq[String] = {
    val fields = lines
      .filter(col(s"$Look.kind") === Kind.Record)
      .select(explode(col(s"$Look.names")).as(Name))
      .distinct()
      .collect()
      .map(_.getString(0))
      .toSeq
      .sorted
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
   * What is read of a batch.
   *
   * @param rows its rows, with the column of the records that Spark could not parse
   * @param placeholder the column that the read made for a batch whose files have none, to be
   *   dropped once the files are read; none, if the files have columns, whatever they are named
   * @param apart the records set apart before Spark read them, each with why, as
   *   `ReadResult.unparsed` has them; none, for a format whose records are not looked at first
   */
  final private case class Reading(
      rows: DataFrame,
      placeholder: Option[String],
      apart: Option[DataFrame] = None
  )

  /** The `_source_file` of each path that a read reads, by the path as Spark gives it, decoded. */
  private def ofItsFile(sourceFileOf: Map[String, String]): UserDefinedFunction = udf {
    (uri: String) =>
      val path = Paths.get(new URI(uri)).toString
      sourceFileOf.getOrElse(
        path,
        throw new IllegalStateException("a file that is not of the plan was read")
      )
  }

  /**
   * What a line of JSON is, as a row: its kind, which for a line set apart is its type in the
   * quarantine (`Quarantine.ErrorType`); the detail of one set apart; and for a record, the
   * names of its first level and the value of each, as text.
   */
  final case class Looked(
      kind: String,
      detail: String,
      names: Seq[String],
      values: Map[String, String]
  )

  private object Kind {
    val Blank = "blank"
    val Record = "record"
  }

  private val lookedAt: UserDefinedFunction = udf { (line: String) =>
    val looked = JsonLines.of(line)
    val detail = JsonLines.detail(looked).orNull
    def apart(kind: String) = Looked(kind, detail, Nil, Map.empty)
    looked match {
      case JsonLines.Line.Blank => apart(Kind.Blank)
      case record: JsonLines.Line.Record =>
        Looked(Kind.Record, detail, record.names, record.fields.toMap)
      case _: JsonLines.Line.TrailingText => apart(Quarantine.ErrorType.TrailingText)
      case _: JsonLines.Line.DuplicateField => apart(Quarantine.ErrorType.DuplicateField)
      case JsonLines.Line.NotARecord => apart(Quarantine.ErrorType.Unparsed)
    }
  }

  /** The columns of the lines of JSON while they are read: the line, and what it is. */
  private val Line = "_lakehouse_line"
  private val Look = "_lakehouse_look"
  private val Name = "_lakehouse_name"

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

  /**
   * What Spark made of the columns of some files of Parquet, unless a column has one type in a
   * file and another in another. Each such file reads alone (ADR-0023 §3), so it is the batch
   * that cannot be read, and none of its files is one to skip: the failure is permanent,
   * whatever the policy of the source, and no file is read alone for it.
   */
  private def oneTypeEach[A](columns: => A): A =
    try columns
    catch {
      case e: Exception if hasCondition(e, TwoTypes) =>
        // Spark says the two schemas, of names and types alone, over several lines: they are
        // said last, on one, so that what a record of the run cuts of a wide one is of them.
        throw RunFailedException.permanent(
          "the files of Parquet of the batch have a column of one type in one file and of " +
            "another in another, and a column is to have one type in every file of a batch. " +
            "Nothing was written. Once the file of the other type is written again, or taken " +
            "out of the landing zone, the next run gives the batch up, as its files are not " +
            "those that it planned (ADR-0018), and plans them as they are. Spark says: " +
            oneLine(e.getMessage),
          e
        )
    }

  /**
   * The condition of Spark's error for files of Parquet whose schemas do not merge: a column of
   * two types (`SparkUnreadableFileContractSpec`).
   */
  private val TwoTypes = "CANNOT_MERGE_SCHEMAS"

  private def firstLine(message: String): String =
    Option(message).flatMap(_.linesIterator.map(_.trim).find(_.nonEmpty)).getOrElse("")

  private def oneLine(message: String): String =
    Option(message).fold("")(_.linesIterator.map(_.trim).filter(_.nonEmpty).mkString(" "))

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

  /**
   * The names of a header of CSV, if each is one that a producer may have given (ADR-0024 §6).
   * Two are not, and Spark reads both without a word (`SparkCsvDialectContractSpec`):
   *
   *  - one with a NUL: the header of a file of UTF-16, which is not read (ADR-0024 §2), as it
   *    reads in an encoding of one byte or in UTF-8, and that of a file with nothing but the
   *    mark of the order of bytes;
   *  - one that starts with that mark: Spark takes one mark off a file, and a file that has it
   *    twice, as some tools leave it, keeps the second at the start of its first name.
   *
   * The names are not said: they are no names.
   */
  private def ofAProducer(columns: Seq[String]): Seq[String] = {
    if (columns.exists(_.contains('\u0000')))
      throw RunFailedException.permanent(
        "the header of the files of CSV of the batch has a name with a NUL (U+0000), which no " +
          "text has: a file of the batch is of UTF-16, which is not read, or has nothing but " +
          "the mark of the order of bytes. Nothing was written: the file is to be written " +
          "again, in an encoding that a source may say and with a header"
      )
    if (columns.exists(_.startsWith(ByteOrderMark)))
      throw RunFailedException.permanent(
        "the header of the files of CSV of the batch has a name that starts with the mark of " +
          "the order of bytes (U+FEFF): a file of the batch has the mark twice, and one is " +
          "taken off. Nothing was written: the file is to be written again, with one mark or none"
      )
    columns
  }

  /** The mark of the order of bytes, as a character of a text that was decoded. */
  private val ByteOrderMark = "\uFEFF"

  /**
   * The longest value of CSV that a read takes, and the longest name of a header, in units of
   * UTF-16 as univocity counts them (`maxCharsPerColumn`): about a million characters, half as
   * many outside the first plane. A file with a longer one cannot be read (ADR-0023).
   *
   * Spark reads with no bound by default, and a quote that is not closed takes the rest of its
   * file into one value, which a file of some hundreds of megabytes ends its process for want
   * of memory with. univocity reserves the bound once for each parser, whatever the number of
   * columns: 2 MiB for each file that a task reads (`SparkCsvValueBoundContractSpec`).
   */
  val MaxValueLength: Int = 1 << 20

  /** Why a file with a value longer than the bound cannot be read: never what the value holds. */
  private val TooLong =
    s"a value of it, or a name of its header, is longer than $MaxValueLength characters " +
      "(units of UTF-16), the longest that a value of CSV may be: most often a quote that is " +
      "not closed, which takes the rest of its file into one value"

  /**
   * Whether a failure of a read is univocity's for a value longer than the bound. It is known by
   * the name of its class and the words of its message, and univocity is not a dependency of
   * the engine: `SparkCsvValueBoundContractSpec` pins both. Its message is never repeated: it
   * has the end of what it parsed, a value of the source.
   */
  private[files] def tooLong(failure: Throwable): Boolean =
    Iterator
      .iterate(failure)(_.getCause)
      .takeWhile(_ != null)
      .take(MaxCauses)
      .exists { cause =>
        cause.getClass.getName == TextParsing &&
        Option(cause.getMessage).exists(_.contains(ExceedsTheBound))
      }

  private val TextParsing = "com.univocity.parsers.common.TextParsingException"
  private val ExceedsTheBound = "exceeds the maximum number of characters defined in your parser"

  /** How many reads of a file alone it fails at, at most, before it cannot be read (ADR-0023). */
  val AloneReads = 3

  /**
   * The waits before the second read of a file alone and before the third: short, and longer
   * the second time, so that a disk or a mount that is away for a few seconds does not leave a
   * sound file as one that cannot be read. The run waits them within its budget of retries.
   */
  val AloneWaits: Vector[FiniteDuration] = Vector(1.second, 2.seconds)
  require(AloneWaits.size == AloneReads - 1, "a wait before each read alone but the first")

  /** What a record of a file that Spark cannot read says it is, as a word. */
  val Unreadable = "unreadable"

  /** Files of a read of JSON with a line longer than the bound, each with why. */
  final private class LinesTooLong(val files: Vector[(LandedFile, String)])
      extends Exception(s"${files.size} files with a line too long")

  /** What one file came to, read alone. */
  sealed private trait Alone
  private object Alone {

    /** It read, at its first read. */
    case object Read extends Alone

    /** It failed, and then read: it is sound, and what it failed with passed. */
    final case class Passed(failures: Int) extends Alone

    /** It failed with a failure that the read knows: not one that cannot be read. */
    case object Known extends Alone

    /**
     * It cannot be read, for `why`, as its record says it: it failed at every read with what the
     * read does not know, or once with a value or a line too long.
     */
    final case class Unreadable(why: String) extends Alone
  }

  private def times(count: Int): String = if (count == 1) "once" else s"$count times"

  /** How far the causes of a failure are followed: further than any chain of Spark's. */
  private val MaxCauses = 16

  /**
   * What Spark said of a failure, as a record may keep it: the conditions of its errors, from
   * the outside in, and the type of the cause where it began. Never the text of an exception,
   * which may hold what the file holds (ADR-0022, as built in P8.3).
   */
  private[files] def said(failure: Throwable): String = {
    val chain =
      Iterator.iterate(failure)(_.getCause).takeWhile(_ != null).take(MaxCauses).toVector
    val conditions = chain.collect {
      case spark: SparkThrowable if spark.getCondition != null => spark.getCondition
    }.distinct
    (conditions :+ s"from ${chain.last.getClass.getName}").mkString(", ")
  }

  /** Whether a failure of a read is that of a file of CSV whose header is not the others'. */
  private[files] def otherHeader(e: Throwable): Boolean = hasCondition(e, HeaderCondition)

  /** Whether a failure, or one of its causes, is Spark's with this condition. */
  @tailrec
  private def hasCondition(e: Throwable, condition: String): Boolean = e match {
    case null => false
    case spark: SparkThrowable if Option(spark.getCondition).contains(condition) => true
    case other => hasCondition(other.getCause, condition)
  }

  /** Whether a failure of a read is Spark's refusal of the charset that it was given. */
  private[files] def charsetRefused(e: Throwable): Boolean = hasCondition(e, CharsetCondition)

  /** The condition of Spark's error for a charset that its reader does not take. */
  private val CharsetCondition = "INVALID_PARAMETER_VALUE.CHARSET"

  /**
   * The condition of Spark's error for a header of CSV that is not the schema's
   * (`CSVHeaderChecker`, with `enforceSchema` off). A legacy one, which a version of Spark may
   * give a name: the test of files with two headers fails then, and says so.
   */
  private val HeaderCondition = "_LEGACY_ERROR_TEMP_3241"
}
