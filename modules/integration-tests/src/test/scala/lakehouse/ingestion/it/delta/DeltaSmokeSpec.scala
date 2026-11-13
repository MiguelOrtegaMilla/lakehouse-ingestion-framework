package lakehouse.ingestion.it.delta

import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.sql.Timestamp
import java.time.Instant

import scala.jdk.CollectionConverters._

import io.delta.tables.DeltaTable
import lakehouse.ingestion.it.support.LocalSparkSuite
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Smoke test of the pinned stack (ADR-0014): Spark 4.1 and Delta 4.4 on Java 17, writing
 * path-addressed tables (AD-10) on the local filesystem with Delta's default LogStore.
 *
 * Besides "it runs", it checks the Delta behaviour later phases build on: one commit file per
 * version in `_delta_log/` (the LogStore, ADR-0004), and writes made idempotent by
 * `txnAppId`/`txnVersion` (recovery, ADR-0005).
 */
final class DeltaSmokeSpec extends AnyFunSuite with Matchers with LocalSparkSuite {

  private val schema = StructType(
    Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType),
      StructField("updated_at", TimestampType)
    )
  )

  private def row(id: Long, name: String, updatedAt: String): Row =
    Row(id, name, Timestamp.from(Instant.parse(updatedAt)))

  private val batchA = Seq(
    row(1, "alpha", "2026-01-01T00:00:00Z"),
    row(2, "beta", "2026-01-01T12:30:00.123456Z")
  )
  private val batchB = Seq(row(3, "gamma", "2026-01-02T08:15:00Z"))

  private def frame(rows: Seq[Row]): DataFrame = spark.createDataFrame(rows.asJava, schema)

  private def rowsOf(df: DataFrame): Seq[Row] = df.orderBy("id").collect().toSeq

  private def readTable(path: String): DataFrame = spark.read.format("delta").load(path)

  private def latestVersion(path: String): Long =
    DeltaTable.forPath(spark, path).history(1).head().getAs[Long]("version")

  private def logDir(path: String): Path = Paths.get(URI.create(path)).resolve("_delta_log")

  private def commitFile(version: Long): String = f"$version%020d.json"

  test("writes a Delta table on the local filesystem and reads the same rows back") {
    val path = tablePath("roundtrip")

    frame(batchA).write.format("delta").save(path)

    DeltaTable.isDeltaTable(spark, path) shouldBe true
    val read = readTable(path)
    read.schema.map(f => f.name -> f.dataType) shouldBe schema.map(f => f.name -> f.dataType)
    // Microseconds and the UTC instant survive the round trip.
    rowsOf(read) shouldBe batchA
  }

  test("each write is one new version: one commit file, one history entry, time travel") {
    val path = tablePath("history")

    frame(batchA).write.format("delta").save(path)
    frame(batchB).write.format("delta").mode("append").save(path)

    // The files the LogStore will coordinate (ADR-0004): exactly N.json for N = 0, 1.
    val commits = Files.list(logDir(path)).iterator.asScala.map(_.getFileName.toString)
    commits.filter(_.endsWith(".json")).toSeq.sorted shouldBe Seq(commitFile(0), commitFile(1))

    val history = DeltaTable
      .forPath(spark, path)
      .history()
      .select("version", "operation")
      .orderBy("version")
      .collect()
      .map(r => r.getLong(0) -> r.getString(1))
      .toSeq
    history shouldBe Seq(0L -> "WRITE", 1L -> "WRITE")

    rowsOf(spark.read.format("delta").option("versionAsOf", 0L).load(path)) shouldBe batchA
    rowsOf(readTable(path)) shouldBe batchA ++ batchB
  }

  test("a write with an already committed txnAppId/txnVersion is skipped (ADR-0005)") {
    val path = tablePath("idempotent")
    val appId = "smoke.source/bronze"

    def append(rows: Seq[Row], txnVersion: Long): Unit =
      frame(rows).write
        .format("delta")
        .mode("append")
        .option("txnAppId", appId)
        .option("txnVersion", txnVersion)
        .save(path)

    append(batchA, txnVersion = 1)
    latestVersion(path) shouldBe 0

    // A replayed batch, e.g. after a crash between the Delta commit and the state commit.
    append(batchA, txnVersion = 1)
    latestVersion(path) shouldBe 0
    rowsOf(readTable(path)) shouldBe batchA

    append(batchB, txnVersion = 2)
    latestVersion(path) shouldBe 1

    // A stale batch, e.g. from a zombie writer that lost its lease: also skipped.
    append(batchA, txnVersion = 1)
    latestVersion(path) shouldBe 1
    rowsOf(readTable(path)) shouldBe batchA ++ batchB

    // Delta records the application's progress as a `txn` action in the commit itself.
    val txns = spark.read
      .json(logDir(path).resolve(commitFile(1)).toString)
      .where("txn IS NOT NULL")
      .select("txn.appId", "txn.version")
      .collect()
      .map(r => r.getString(0) -> r.getLong(1))
      .toSeq
    txns shouldBe Seq(appId -> 2L)
  }
}
