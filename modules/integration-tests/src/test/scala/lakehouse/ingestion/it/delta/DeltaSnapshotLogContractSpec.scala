package lakehouse.ingestion.it.delta

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.util.Base64

import scala.jdk.CollectionConverters._
import scala.util.Using

import com.fasterxml.jackson.databind.ObjectMapper
import lakehouse.ingestion.it.support.{CapturedLogs, LocalSparkSuite}
import org.apache.logging.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What Delta 4.4 logs of the rows of a table, and the setting that keeps them out of a log: the
 * facts that the sessions of the engine are built on (discovery §20, "Delta logs values of the
 * rows of a table of Bronze").
 *
 * These are characterisation tests, as those of `DeltaSetTransactionContractSpec` are. They test
 * Delta, and no code of the engine is in them. What they pin:
 *
 *  - Delta keeps, in the log of a table, the least and the greatest value of each column of
 *    each file, a text by its first 32 characters; it has the files of a small table, with
 *    those statistics, in the checksum of each version (`.crc`); and it logs each snapshot that
 *    it makes with its checksum, at INFO, which is the level that Spark logs at unless it is
 *    told otherwise. So values of the rows of a table are in the log of every process that
 *    reads or writes it;
 *  - with `spark.databricks.delta.allFilesInCrc.enabled` off, a setting of the session that is
 *    internal to Delta, a checksum is written without the files, and no line of Delta's or of
 *    Spark's has a value of a row, at any level, as it is or encoded. The statistics stay in
 *    the commits of the table, where a read passes over files by them;
 *  - the setting is of what a session writes, and not of what it reads: a table whose last
 *    checksum was written with its files is logged so by whoever reads it, until its next
 *    commit by a session that has the setting off;
 *  - what is given up with the files of a checksum is Delta's check of the files of a snapshot
 *    against them, which it makes only in its verification mode, off by default.
 *
 * A Delta upgrade that changes one of these fails here, before a run logs what its source holds
 * (ADR-0014, upgrade procedure).
 *
 * '''What is not Delta's, and is left out of what is asked here.''' Parquet's writer of records
 * logs each value that it writes, at DEBUG (`org.apache.parquet.io`), whatever Delta is set to,
 * and takes its level once, when its class is loaded: whether it logs in this JVM is of what
 * ran before. A process of its own pins it (`RunCommandLogsSpec`).
 */
final class DeltaSnapshotLogContractSpec extends AnyFunSuite with Matchers with LocalSparkSuite {

  import DeltaSnapshotLogContractSpec._

  /** As Delta has the setting, whatever the sessions of the tests are given, which have it off. */
  override protected def sparkConf: Map[String, String] =
    Map(FilesInChecksum.key -> FilesInChecksum.defaultValueString)

  /** A session that writes its checksums without the files of the table. */
  private lazy val without: SparkSession = {
    val session = spark.newSession()
    session.conf.set(FilesInChecksum.key, "false")
    session
  }

  test("the setting is internal to Delta, on by default, and of incremental commits") {
    FilesInChecksum.key shouldBe "spark.databricks.delta.allFilesInCrc.enabled"
    FilesInChecksum.defaultValue shouldBe Some(true)
    FilesInChecksum.isPublic shouldBe false
    // It says so itself: it has an effect only while a commit computes its checksum from the
    // one before, which is Delta's default too.
    FilesInChecksum.doc should include("only takes effect when incremental commits are enabled")
    DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.defaultValue shouldBe Some(true)
  }

  test("as Delta is, each snapshot of a table is logged at INFO with values of its rows") {
    val path = tablePath("as-delta-is")
    val heard = hearing {
      append(spark, path, from = 0)
      append(spark, path, from = 10)
      aProcessThatStarts(spark)(_.read.format("delta").load(path).count() shouldBe 6L)
    }

    val withValues = ofARow(heard)
    withValues should not be empty
    // By the two loggers of a snapshot, and at the level that a deployment logs at by default.
    withValues.map(_.logger).toSet shouldBe Set(SnapshotLogger, DeltaLogLogger)
    withValues.map(_.level).toSet shouldBe Set(Level.INFO)
    withValues.foreach(line => Snapshots.exists(line.message.contains) shouldBe true)
    // The least and the greatest value of each column of each file; a text by its first 32
    // characters, and no more of it.
    val logged = withValues.map(_.message).mkString("\n")
    logged should include(
      s""""minValues":{"id":0,"v":"${text(0)}","long":"${long(0).take(32)}"}"""
    )
    logged should include(s""""v":"${text(12)}"""")
    logged should not include long(0).take(33)

    // They are logged from the checksum of a version, which has the files of the table.
    checksums(path) should have size 2
    checksums(path).foreach { checksum =>
      checksum should include(""""allFiles"""")
      checksum should include(text(0))
    }
  }

  test(
    "with the files out of the checksum, no line of Delta or of Spark has a value, at any level"
  ) {
    val path = tablePath("without")
    val heard = hearing {
      append(without, path, from = 0)
      append(without, path, from = 10)
      in(without)(DeltaLog.forTable(without, path).checkpoint())
      aProcessThatStarts(without)(_.read.format("delta").load(path).count() shouldBe 6L)
      aProcessThatStarts(without) {
        _.read.format("delta").load(path).where("id >= 0").collect() should have length 6
      }
    }

    val withValues = ofARow(heard).map(line => line.logger -> valueIn(line.message))
    withClue(withValues.mkString("\n"))(withValues shouldBe empty)
    // The snapshots were logged, and say the columns of the table, by their names.
    // (And that of the table before it was made, which has none: `DummySnapshot`.)
    val snapshots = heard.kept.filter(line => Snapshots.exists(line.message.contains))
    snapshots.map(_.logger).toSet should contain allOf (SnapshotLogger, DeltaLogLogger)
    snapshots.exists(_.message.contains(""""name":"long"""")) shouldBe true
    // And every logger was heard, as low as it logs: those of Delta, of Spark and of Parquet,
    // which wrote the files, the checkpoint among them, and read them.
    heard.events should be > 5000L
    Seq("org.apache.spark.sql.delta", "org.apache.spark.sql.execution", "org.apache.parquet")
      .foreach(prefix => withClue(prefix)(heard.loggers.exists(_.startsWith(prefix)) shouldBe true))

    checksums(path) should have size 2
    checksums(path).foreach { checksum =>
      checksum should not include """"allFiles""""
      Forms.foreach(form => checksum should not include form)
    }
  }

  test("the statistics stay in the commits of the table, and a read passes over files by them") {
    val path = tablePath("skipping")
    append(without, path, from = 0)
    append(without, path, from = 10)

    commits(path) should have size 2
    // The statistics of a file are a text of JSON, inside the JSON of its commit.
    commits(path).head.replace("\\\"", "\"") should include(
      s""""minValues":{"id":0,"v":"${text(0)}""""
    )
    // Two files, and one of them has no row of the condition.
    val read = in(without)(without.read.format("delta").load(path))
    in(without) {
      read.inputFiles should have length 2
      read.where("id = 11").inputFiles should have length 1
      read.where(s"v = '${text(11)}'").inputFiles should have length 1
    }
  }

  test("a table whose last checksum has its files is logged so until its next commit without") {
    val path = tablePath("written-before")
    append(spark, path, from = 0)
    append(spark, path, from = 10)

    // The setting is of what a session writes: a session that has it off reads the checksum
    // that another wrote, and logs it.
    val reading = hearing {
      aProcessThatStarts(without)(_.read.format("delta").load(path).count() shouldBe 6L)
    }
    ofARow(reading).map(_.logger).toSet shouldBe Set(SnapshotLogger, DeltaLogLogger)

    // So does the snapshot that its first write starts from. The commit of that write has a
    // checksum without the files, and from it on nothing is logged.
    val writing = hearing(aProcessThatStarts(without)(_ => append(without, path, from = 20)))
    ofARow(writing) should not be empty
    val after = hearing {
      append(without, path, from = 30)
      aProcessThatStarts(without)(_.read.format("delta").load(path).count() shouldBe 12L)
    }
    ofARow(after) shouldBe empty
    after.kept.filter(line => Snapshots.exists(line.message.contains)) should not be empty

    // The checksums of before are as they were written: nothing rewrites one.
    checksums(path).map(_.contains(""""allFiles"""")) shouldBe Seq(true, true, false, false)
  }

  test("what is given up: the check of the files of a snapshot against those of its checksum") {
    val path = tablePath("given-up")
    append(spark, path, from = 0)
    val snapshot = in(spark)(DeltaLog.forTable(spark, path).update())
    // Whether Delta checks a snapshot so, as it asks itself after a commit and at a checkpoint:
    // a member of Delta's own package, reached as the JVM has it.
    def checks(session: SparkSession): Boolean =
      Snapshot.getClass
        .getMethod("allFilesInCrcVerificationEnabled", classOf[SparkSession], classOf[Snapshot])
        .invoke(Snapshot, session, snapshot)
        .asInstanceOf[Boolean]
    // A session of its own for each: one made from another has none of what that one was set.
    def having(files: Boolean)(setting: (String, String)): SparkSession = {
      val session = spark.newSession()
      session.conf.set(FilesInChecksum.key, files.toString)
      session.conf.set(setting._1, setting._2)
      session
    }
    val verifying = VerificationMode.key -> "true"
    val notInUtc = "spark.sql.session.timeZone" -> "Europe/Madrid"

    // Delta makes the check only in its verification mode: off by default, and on by itself
    // for a session that is not in UTC, which no session of the engine is.
    VerificationMode.defaultValue shouldBe Some(false)
    VerificationOutOfUtc.defaultValue shouldBe Some(true)
    checks(spark) shouldBe false
    checks(having(files = true)(verifying)) shouldBe true
    checks(having(files = true)(notInUtc)) shouldBe true
    // With the files out of the checksum there is nothing to check against, in that mode or
    // out of it.
    checks(without) shouldBe false
    checks(having(files = false)(verifying)) shouldBe false
    checks(having(files = false)(notInUtc)) shouldBe false

    // The rest of a checksum is written as before: all that it has but the files.
    val other = tablePath("given-up-without")
    append(without, other, from = 0)
    val (withFiles, withoutFiles) =
      (fieldsOf(checksums(path).head), fieldsOf(checksums(other).head))
    withFiles should contain allOf (
      "allFiles",
      "numFiles",
      "tableSizeBytes",
      "metadata",
      "protocol"
    )
    withoutFiles shouldBe withFiles - "allFiles"
  }

  /** The names of the fields of a checksum, which is one object of JSON. */
  private def fieldsOf(checksum: String): Set[String] =
    new ObjectMapper().readTree(checksum).fieldNames().asScala.toSet

  /** Three rows from `from`, in one file. */
  private def append(session: SparkSession, path: String, from: Int): Unit = {
    import session.implicits._
    val rows = (from until from + 3).map(i => (i.toLong, text(i), long(i))).toDF("id", "v", "long")
    in(session)(rows.coalesce(1).write.format("delta").mode("append").save(path))
  }

  /** `body`, with `session` the one that Delta takes its settings from. */
  private def in[T](session: SparkSession)(body: => T): T = {
    val before = SparkSession.getActiveSession
    SparkSession.setActiveSession(session)
    try body
    finally before.fold(SparkSession.clearActiveSession())(SparkSession.setActiveSession)
  }

  /** `body`, as a process that starts runs it: with no table that Delta has in memory. */
  private def aProcessThatStarts[T](session: SparkSession)(body: SparkSession => T): T = {
    DeltaLog.clearCache()
    in(session)(body(session))
  }

  /**
   * What every logger logs while `body` runs, at its lowest level: the lines with a value of a
   * row, in any of its forms, and those of a snapshot are kept.
   */
  private def hearing(body: => Any): CapturedLogs.Heard =
    CapturedLogs
      .everything(Level.ALL) { line =>
        valueIn(line).nonEmpty || Snapshots.exists(line.contains)
      }(body)
      ._2

  /** The lines that were heard with a value of a row, but those of Parquet's writer of records. */
  private def ofARow(heard: CapturedLogs.Heard): Seq[CapturedLogs.Logged] =
    heard.kept.filter { line =>
      valueIn(line.message).nonEmpty && !line.logger.startsWith(ParquetRecords)
    }

  /** The files of the log of a table whose names end so, each as its text, in their order. */
  private def ofTheLog(path: String, ending: String): Seq[String] = {
    val log = Paths.get(new URI(path)).resolve("_delta_log")
    Using
      .resource(Files.list(log))(_.iterator().asScala.toVector)
      .filter { file =>
        val name = file.getFileName.toString
        Files.isRegularFile(file) && !name.startsWith(".") && name.endsWith(ending)
      }
      .sortBy(_.getFileName.toString)
      .map(file => new String(Files.readAllBytes(file), UTF_8))
  }

  private def checksums(path: String): Seq[String] = ofTheLog(path, ".crc")

  private def commits(path: String): Seq[String] = ofTheLog(path, ".json")
}

object DeltaSnapshotLogContractSpec {

  private val FilesInChecksum = DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_ENABLED

  /** Delta's check of the files of a snapshot against those of its checksum, and when it is on. */
  private val VerificationMode = DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_VERIFICATION_MODE_ENABLED
  private val VerificationOutOfUtc =
    DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_FORCE_VERIFICATION_MODE_FOR_NON_UTC_ENABLED

  private val SnapshotLogger = "org.apache.spark.sql.delta.Snapshot"

  /** `SnapshotManagement` logs as the `DeltaLog` that it is part of. */
  private val DeltaLogLogger = "org.apache.spark.sql.delta.DeltaLog"

  /** The loggers of Parquet's writer of records, which logs each value that it writes. */
  private val ParquetRecords = "org.apache.parquet.io."

  /** What a line that logs a snapshot says first, after the id of its table if it says it. */
  private val Snapshots = Seq("Created snapshot ", "Updated snapshot to ")

  /** What every value of a text of the rows starts with: no line of a log has it otherwise. */
  private val InAText = "Xq7Zv9Kp"
  private val InALongText = "Wm3Lt8Rd"

  private def text(i: Int): String = f"$InAText-$i%04d"

  /** More than the 32 characters that Delta keeps of a text. */
  private def long(i: Int): String = f"$InALongText-$i%04d-" + "x" * 60

  /**
   * How a value may be in a line: as it is, or as its bytes in base64, in hexadecimal, or as
   * the list of them that Java writes, which is how a line that says what a file of Parquet
   * holds may have it. The bytes are those that every value starts with, six of them, which
   * are eight characters of base64 whatever follows.
   */
  private val Forms: Seq[String] = Seq(InAText, InALongText).flatMap { start =>
    val bytes = start.getBytes(UTF_8).take(6)
    Seq(
      start,
      Base64.getEncoder.encodeToString(bytes),
      bytes.map(byte => f"$byte%02x").mkString,
      bytes.map(byte => f"$byte%02X").mkString,
      bytes.mkString(", ")
    )
  }

  /** The form of a value that a line has, if it has one. */
  private def valueIn(line: String): Option[String] = Forms.find(line.contains)
}
