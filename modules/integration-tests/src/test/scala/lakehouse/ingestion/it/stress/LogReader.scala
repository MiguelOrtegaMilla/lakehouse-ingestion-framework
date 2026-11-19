package lakehouse.ingestion.it.stress

import java.io.{IOException, OutputStream}
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}

import scala.jdk.CollectionConverters._
import scala.util.Using
import scala.util.control.NonFatal

import lakehouse.ingestion.it.stress.LogModel._
import org.apache.hadoop.fs.{LocalFileSystem, Path => HadoopPath, RawLocalFileSystem}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.{col, count, countDistinct, from_json}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Builds the verifier's views of a table (`LogModel`). This is the only I/O of the verifier.
 *
 * The commit files are read as bytes with `java.nio` and parsed with Spark's JSON parser, not
 * read through Delta or Hadoop: the log view must not depend on the log replay it is meant to
 * check, nor on Hadoop's checksum layer, which rejects a commit file whose `.crc` sidecar went
 * stale and would leave the verifier blind exactly when a commit was overwritten. Sidecars are
 * checked separately, as an observation. Only `TableView` goes through Delta, because it is what
 * a reader of the table sees.
 */
object LogReader {

  /** Commit files only: checkpoints, checksums and compacted logs have other names. */
  private val CommitFile = """^(\d{20})\.json$""".r

  private val CorruptRecord = "_corrupt_record"
  private val Version = "version"
  private val Line = "line"
  private val Action = "action"

  private def pathStruct = StructType(Seq(StructField("path", StringType)))

  /** The actions the verifier uses; all others are ignored, even if present. */
  private val actionSchema = StructType(
    Seq(
      StructField(
        "txn",
        StructType(Seq(StructField("appId", StringType), StructField("version", LongType)))
      ),
      StructField("add", pathStruct),
      StructField("remove", pathStruct),
      StructField(
        "commitInfo",
        StructType(
          Seq(StructField("operation", StringType), StructField("userMetadata", StringType))
        )
      ),
      StructField(CorruptRecord, StringType)
    )
  )

  /**
   * Reads the views of a finished run, through every `Reader`, and verifies them.
   *
   * The readers read first. On `PostgresLogStore` a read publishes a commit that a writer left
   * taken and not published, and what is verified is the table that the next reader gets. The
   * commit files and the registry are looked at after that.
   *
   * @param registry reads the registry of the table, for a run on `PostgresLogStore`
   * @param epochs reads the epochs of the run, for one whose writers commit under them
   */
  def verify(
      spark: SparkSession,
      run: StressRun,
      expected: Verifier.Expected,
      writers: Seq[StressHarness.WriterOutcome],
      registry: Option[() => RegistryView] = None,
      epochs: Option[() => EpochsView] = None
  ): Verifier.Report = {
    val tables = Reader.all.map(r => r -> tableView(spark, run.tableUri, r))
    Verifier.verify(
      expected,
      writers,
      logView(spark, run.tableDir),
      tables,
      diskView(run.tableDir),
      registry.map(_.apply()),
      epochs.map(_.apply())
    )
  }

  def logView(spark: SparkSession, tableDir: Path): LogView = {
    val logDir = tableDir.resolve("_delta_log")
    val commitFiles = listCommitFiles(logDir)
    if (commitFiles.isEmpty) LogView(Nil, Nil, Nil)
    else {
      import spark.implicits._
      val lines = commitFiles.toSeq.flatMap { case (v, p) =>
        Files.readAllLines(p, UTF_8).asScala.filterNot(_.trim.isEmpty).map(v -> _)
      }
      val rows = lines
        .toDF(Version, Line)
        .select(
          col(Version),
          from_json(
            col(Line),
            actionSchema,
            Map("mode" -> "PERMISSIVE", "columnNameOfCorruptRecord" -> CorruptRecord)
          ).as(Action)
        )
        .collect()
        .toSeq
      val byVersion = rows.groupMap(_.getLong(0))(_.getStruct(1))
      val parsed = commitFiles.keys.toSeq.sorted.map(v => parse(v, byVersion.getOrElse(v, Nil)))
      LogView(
        versions = commitFiles.keys.toSeq.sorted,
        commits = parsed.collect { case Right(c) => c },
        unreadable = parsed.collect { case Left(u) => u },
        checksumMismatches = checksumMismatches(spark, commitFiles),
        commitSidecars = commitFiles.toSeq.collect {
          case (v, p) if Files.exists(sidecarOf(p)) => v
        }.sorted,
        staleSidecars = staleSidecars(spark, logDir, commitFiles.values.toSet)
      )
    }
  }

  /** Where Hadoop's checksummed file system keeps the checksums of `file`. */
  private def sidecarOf(file: Path): Path = file.resolveSibling(s".${file.getFileName}.crc")

  /** Whether `file` is the sidecar of a file that is there. A sidecar has none of its own. */
  private def isSidecar(file: Path): Boolean = {
    val name = file.getFileName.toString
    name.startsWith(".") && name.endsWith(".crc") &&
    Files.exists(file.resolveSibling(name.substring(1, name.length - ".crc".length)))
  }

  /**
   * The files of the log, commits apart, whose sidecar does not match them, at any depth. A
   * file without a sidecar is not looked at: nothing checks it, so nothing can refuse it.
   */
  private def staleSidecars(
      spark: SparkSession,
      logDir: Path,
      commitFiles: Set[Path]
  ): Seq[String] =
    Using.resource(new LocalFileSystem()) { fs =>
      fs.initialize(URI.create("file:///"), spark.sparkContext.hadoopConfiguration)
      Using.resource(Files.walk(logDir)) { paths =>
        paths.iterator.asScala
          .filter(p => Files.isRegularFile(p) && !commitFiles(p) && !isSidecar(p))
          .filter(p => Files.exists(sidecarOf(p)) && !readsCleanly(fs, new HadoopPath(p.toUri)))
          .map(p => relative(logDir, p))
          .toList
          .sorted
      }
    }

  /**
   * The versions whose commit file Hadoop's `LocalFileSystem` refuses to read because its `.crc`
   * sidecar does not match. A file without a sidecar is read unchecked, and passes.
   * On the local file system Hadoop reports a checksum error without moving the file aside (its
   * `bad_files` handling needs a second replica to retry on), so this read changes nothing.
   */
  private def checksumMismatches(spark: SparkSession, commitFiles: Map[Long, Path]): Seq[Long] =
    Using.resource(new LocalFileSystem()) { fs =>
      fs.initialize(URI.create("file:///"), spark.sparkContext.hadoopConfiguration)
      commitFiles.toSeq.sortBy(_._1).collect {
        case (v, p) if !readsCleanly(fs, new HadoopPath(p.toUri)) => v
      }
    }

  private def readsCleanly(fs: LocalFileSystem, file: HadoopPath): Boolean =
    try {
      Using.resource(fs.open(file))(in =>
        IOUtils.copyBytes(in, OutputStream.nullOutputStream(), 4096, false)
      )
      true
    } catch {
      // A checksum error, or a sidecar too short for the file: Hadoop refuses it either way.
      case _: IOException => false
    }

  /**
   * The latest snapshot as a fresh Delta reader sees it. A `Left` holds the reader's error: an
   * unreadable table is an outcome of the run, not a failure of the verifier.
   */
  def tableView(spark: SparkSession, tableUri: String, reader: Reader): Either[String, TableView] =
    try {
      // The orchestrator created the table at version 0; do not reuse that cached DeltaLog.
      DeltaLog.clearCache()
      val df = readerOptions(reader)
        .foldLeft(spark.read.format("delta")) { case (r, (k, v)) => r.option(k, v) }
        .load(tableUri)
      val root = Paths.get(URI.create(tableUri))
      val files = df.inputFiles.map(f => relative(root, Paths.get(URI.create(f)))).toSet
      val rows = df
        .groupBy("writer_id", "txn_version")
        .agg(count("*"), countDistinct(col("row_seq")))
        .collect()
        .map(r => (r.getString(0), r.getLong(1)) -> RowCount(r.getLong(2), r.getLong(3)))
        .toMap
      Right(TableView(files, rows))
    } catch {
      case NonFatal(e) => Left(describe(e))
    }

  /** The error and, if different, its root cause, which is usually the one that explains it. */
  private def describe(e: Throwable): String = {
    def line(t: Throwable) = s"${t.getClass.getName}: ${t.getMessage}"
    val root = Iterator.iterate(e)(_.getCause).takeWhile(_ != null).toSeq.last
    if (root eq e) line(e) else s"${line(e)} (root cause: ${line(root)})"
  }

  /**
   * Delta passes `fs.*` reader options into the Hadoop configuration of the table's `DeltaLog`,
   * for log and data files alike. The file system cache is keyed by scheme, not by
   * configuration, so it is bypassed: otherwise the cached `LocalFileSystem` would be reused.
   */
  private def readerOptions(reader: Reader): Map[String, String] = reader match {
    case Reader.Default => Map.empty
    case Reader.SidecarFree =>
      Map(
        "fs.file.impl" -> classOf[RawLocalFileSystem].getName,
        "fs.file.impl.disable.cache" -> "true"
      )
  }

  /** Data files on disk: every regular file outside `_delta_log`, hidden files excluded. */
  def diskView(tableDir: Path): DiskView =
    Using.resource(Files.walk(tableDir)) { paths =>
      val files = paths.iterator.asScala
        .filter(Files.isRegularFile(_))
        .map(p => relative(tableDir, p))
        .filterNot(p => p.startsWith("_delta_log/") || p.split('/').exists(_.startsWith(".")))
        .toSet
      DiskView(files)
    }

  private def listCommitFiles(logDir: Path): Map[Long, Path] =
    if (!Files.isDirectory(logDir)) Map.empty
    else
      Using.resource(Files.list(logDir)) {
        _.iterator.asScala
          .flatMap { p =>
            p.getFileName.toString match {
              case CommitFile(v) => Some(v.toLong -> p)
              case _ => None
            }
          }
          .toMap
      }

  /** `rows` are the parsed actions of one commit file, one per non-blank line. */
  private def parse(version: Long, rows: Seq[Row]): Either[UnreadableCommit, Commit] = {
    // A line `from_json` cannot parse at all comes back as null rather than as a corrupt record.
    val corrupt = rows.flatMap(r =>
      if (r == null) Some("(unparseable)") else Option(r.getAs[String](CorruptRecord))
    )
    if (rows.isEmpty) Left(UnreadableCommit(version, "no actions"))
    else if (corrupt.nonEmpty)
      Left(UnreadableCommit(version, s"malformed line: ${corrupt.head.take(200)}"))
    else {
      def struct(r: Row, name: String): Option[Row] = Option(r.getAs[Row](name))
      def paths(name: String): Seq[String] =
        rows.flatMap(struct(_, name)).map(s => decode(s.getAs[String]("path")))
      Right(
        Commit(
          version = version,
          txns = rows
            .flatMap(struct(_, "txn"))
            .map(t => Txn(t.getAs[String]("appId"), t.getAs[Long]("version"))),
          added = paths("add"),
          removed = paths("remove"),
          operation = rows.flatMap(struct(_, "commitInfo")).headOption.map(_.getAs[String](0)),
          // What the writer of the commit said of its epoch, in the commit itself.
          epoch = rows
            .flatMap(struct(_, "commitInfo"))
            .headOption
            .flatMap(info => Option(info.getAs[String]("userMetadata")))
            .flatMap(Turn.epochIn)
        )
      )
    }
  }

  /** Delta stores data file paths as relative, URI-encoded strings. */
  private def decode(path: String): String = new URI(path).getPath

  private def relative(root: Path, file: Path): String =
    root.toAbsolutePath.normalize().relativize(file.toAbsolutePath.normalize()).toString
}
