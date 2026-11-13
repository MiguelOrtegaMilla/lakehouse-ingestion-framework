package lakehouse.ingestion.it.stress

import java.net.URI
import java.nio.file.{Files, Path, Paths}

import scala.jdk.CollectionConverters._
import scala.util.Using
import scala.util.control.NonFatal

import lakehouse.ingestion.it.stress.LogModel._
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.{col, count, countDistinct, input_file_name}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Builds the verifier's views of a table (`LogModel`). This is the only I/O of the verifier.
 *
 * The commit files are read with Spark's plain JSON reader, not with Delta: the log view must not
 * depend on the log replay it is meant to check. Only `TableView` goes through Delta, because it
 * is what a reader of the table sees.
 */
object LogReader {

  /** Commit files only: checkpoints, checksums and compacted logs have other names. */
  private val CommitFile = """^(\d{20})\.json$""".r

  private val CorruptRecord = "_corrupt_record"
  private val SourceFile = "_source_file"

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
      StructField("commitInfo", StructType(Seq(StructField("operation", StringType)))),
      StructField(CorruptRecord, StringType)
    )
  )

  /** Reads the three views of a finished run and verifies them. */
  def verify(
      spark: SparkSession,
      run: StressRun,
      expected: Verifier.Expected,
      writers: Seq[StressHarness.WriterOutcome]
  ): Verifier.Report =
    Verifier.verify(
      expected,
      writers,
      logView(spark, run.tableDir),
      tableView(spark, run.tableUri),
      diskView(run.tableDir)
    )

  def logView(spark: SparkSession, tableDir: Path): LogView = {
    val commitFiles = listCommitFiles(tableDir.resolve("_delta_log"))
    if (commitFiles.isEmpty) LogView(Nil, Nil, Nil)
    else {
      val rows = spark.read
        .schema(actionSchema)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CorruptRecord)
        .json(commitFiles.values.map(_.toUri.toString).toSeq: _*)
        .withColumn(SourceFile, input_file_name())
        .collect()
        .toSeq
      val versionOf = commitFiles.map { case (v, p) => p.toUri.normalize().getPath -> v }
      val byVersion = rows.groupBy(r => versionOf(URI.create(r.getAs[String](SourceFile)).getPath))
      val parsed = commitFiles.keys.toSeq.sorted.map(v => parse(v, byVersion.getOrElse(v, Nil)))
      LogView(
        versions = commitFiles.keys.toSeq.sorted,
        commits = parsed.collect { case Right(c) => c },
        unreadable = parsed.collect { case Left(u) => u }
      )
    }
  }

  /**
   * The latest snapshot as a fresh Delta reader sees it. A `Left` holds the reader's error: an
   * unreadable table is an outcome of the run, not a failure of the verifier.
   */
  def tableView(spark: SparkSession, tableUri: String): Either[String, TableView] =
    try {
      // The orchestrator created the table at version 0; do not reuse that cached DeltaLog.
      DeltaLog.clearCache()
      val df = spark.read.format("delta").load(tableUri)
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
      case NonFatal(e) => Left(s"${e.getClass.getName}: ${e.getMessage}")
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

  private def parse(version: Long, rows: Seq[Row]): Either[UnreadableCommit, Commit] = {
    val corrupt = rows.flatMap(r => Option(r.getAs[String](CorruptRecord)))
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
          operation = rows.flatMap(struct(_, "commitInfo")).headOption.map(_.getAs[String](0))
        )
      )
    }
  }

  /** Delta stores data file paths as relative, URI-encoded strings. */
  private def decode(path: String): String = new URI(path).getPath

  private def relative(root: Path, file: Path): String =
    root.toAbsolutePath.normalize().relativize(file.toAbsolutePath.normalize()).toString
}
