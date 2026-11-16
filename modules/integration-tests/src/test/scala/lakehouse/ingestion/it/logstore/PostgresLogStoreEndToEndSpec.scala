package lakehouse.ingestion.it.logstore

import java.nio.file.attribute.{FileTime, PosixFilePermissions}
import java.nio.file.Files
import java.time.Instant
import java.time.temporal.ChronoUnit

import scala.jdk.CollectionConverters._
import scala.util.Using

import io.delta.tables.DeltaTable
import lakehouse.ingestion.it.support.LocalSparkSuite
import lakehouse.ingestion.logstore._
import lakehouse.ingestion.logstore.coordination.Registration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, OptimisticTransaction}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The operations an ingestion makes, by Delta on `PostgresLogStore` against a real PostgreSQL:
 * an idempotent append, `MERGE`, `UPDATE`, `DELETE`, `OPTIMIZE`, `VACUUM`, checkpoints and the
 * log cleanup that comes with them, and the creation of a table, by one writer, by two, and
 * again after it was dropped.
 *
 * Each test looks at what the LogStore adds to the operation: that every commit has its entry,
 * that a commit left half way by a writer is in the table for whoever comes next, and that
 * what Delta removes from a table directory is never something the LogStore still needs.
 */
final class PostgresLogStoreEndToEndSpec
    extends AnyFunSuite
    with Matchers
    with CoordinatedDeltaSuite {

  import CoordinatedDeltaSuite.EmptyCommit
  import PostgresLogStoreEndToEndSpec._

  private def table(path: String): DeltaTable = DeltaTable.forPath(spark, path)

  /** Every version up to the table's latest has a complete entry: no commit of another kind. */
  private def everyCommitIsCoordinated(path: String): Unit = {
    val latest = versionOf(path)
    store.latestEntry(keyOf(path)).map(_.version) shouldBe Some(latest)
    (0L to latest).map(entry(path, _).complete) shouldBe Seq.fill(latest.toInt + 1)(true): Unit
  }

  /** The table's data files on disk, whether its current version uses them or not. */
  private def dataFilesOf(path: String): Set[String] =
    Using.resource(Files.list(directoryOf(path))) {
      _.iterator.asScala.map(_.getFileName.toString).filter(_.endsWith(".parquet")).toSet
    }

  // ---------------------------------------------------------------------------------------------
  // An idempotent append (ADR-0005)
  // ---------------------------------------------------------------------------------------------

  test("F16: a batch whose commit could not be published is in the table once, after its retry") {
    val path = create("exactly-once")

    def batch(sequence: Long, from: Long): Unit =
      rows(from, from + 10L).write
        .format("delta")
        .mode("append")
        .option("txnAppId", "orders-source")
        .option("txnVersion", sequence)
        .save(path)

    // The log directory accepts no new name, and its directory of temporary files still does:
    // the commit's bytes are written, its version is taken, and it cannot be published.
    val log = logOf(path)
    Files.setPosixFilePermissions(log, PosixFilePermissions.fromString("r-xr-xr-x"))
    val failure =
      try {
        assume(!Files.isWritable(log), "this user may write to any directory")
        intercept[Exception](batch(sequence = 1L, from = 10L))
      } finally
        Files.setPosixFilePermissions(log, PosixFilePermissions.fromString("rwxr-xr-x")): Unit

    // Delta is told that the commit failed, and how: it exists, and will appear.
    causesOf(failure).collectFirst { case pending: CommitPublicationPendingException =>
      pending.version
    } shouldBe Some(1L)
    Files.exists(commitFile(path, 1)) shouldBe false
    entry(path, 1).complete shouldBe false

    // What the engine does with a batch that failed: the same batch again. Delta reads the
    // table first, which publishes the commit, finds the batch in it, and writes nothing.
    batch(sequence = 1L, from = 10L)

    versionOf(path) shouldBe 1L
    idsOf(path) shouldBe (0L until 20L)
    entry(path, 1).complete shouldBe true

    // The batch after it is a commit like any other.
    batch(sequence = 2L, from = 20L)
    versionOf(path) shouldBe 2L
    idsOf(path) shouldBe (0L until 30L)
    everyCommitIsCoordinated(path)
  }

  // ---------------------------------------------------------------------------------------------
  // Row-level changes
  // ---------------------------------------------------------------------------------------------

  test("MERGE, UPDATE and DELETE commit through the LogStore, and a new reader gets the result") {
    val path = create("upserts") // ids 0 to 9

    table(path)
      .as("target")
      .merge(rows(5L, 15L).as("source"), "target.id = source.id")
      .whenMatched()
      .updateExpr(Map("id" -> "source.id + 100"))
      .whenNotMatched()
      .insertAll()
      .execute()
    table(path).updateExpr("id = 0", Map("id" -> "1000"))
    table(path).delete("id BETWEEN 105 AND 109")

    // A process that has never seen the table builds it from the log.
    DeltaLog.clearCache()
    idsOf(path) shouldBe ((1L to 4L) ++ (10L to 14L) :+ 1000L)
    versionOf(path) shouldBe 3L
    everyCommitIsCoordinated(path)

    // The history and an old version are read through the same listing.
    val operations = table(path).history().select("version", "operation").collect()
    operations.map(row => row.getLong(0) -> row.getString(1)).sortBy(_._1).map(_._2) shouldBe
      Seq("WRITE", "MERGE", "UPDATE", "DELETE")
    spark.read.format("delta").option("versionAsOf", 1L).load(path).count() shouldBe 15L

    // Each commit has its temporary file, and nothing else is left there: the other files
    // Delta writes through the LogStore, such as checksums, leave none behind.
    tempsOf(path) shouldBe (0L to 3L).map(entry(path, _).tempPath.stripPrefix(".tmp/")).toSet
  }

  // ---------------------------------------------------------------------------------------------
  // Maintenance of the table by Delta
  // ---------------------------------------------------------------------------------------------

  test("OPTIMIZE and VACUUM keep the table's rows and everything the LogStore has in the log") {
    val path = create("compacted")
    (1L to 3L).foreach(batch => append(path, batch * 10L, batch * 10L + 10L))
    val smallFiles = dataFilesOf(path)

    table(path).optimize().executeCompaction(): Unit

    versionOf(path) shouldBe 4L
    DeltaLog.forTable(spark, path).update().numOfFiles shouldBe 1L
    dataFilesOf(path).size shouldBe smallFiles.size + 1

    // A writer leaves version 5 taken and not published, and the table is vacuumed.
    dieAfterTaking(path, version = 5, Seq(EmptyCommit))
    val temps = tempsOf(path)
    temps should have size 6
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    try table(path).vacuum(0.0): Unit
    finally spark.conf.unset("spark.databricks.delta.retentionDurationCheck.enabled")

    // The files that compaction replaced are gone, and nothing of the log is.
    dataFilesOf(path).intersect(smallFiles) shouldBe empty
    tempsOf(path) should contain allElementsOf temps
    DeltaLog.clearCache()
    idsOf(path) shouldBe (0L until 40L)
    versionOf(path) should be >= 5L
    everyCommitIsCoordinated(path)
  }

  test("a reader that starts from a checkpoint finds the commit a dead writer left after it") {
    // Delta writes a checkpoint by itself every third commit of this table.
    spark.conf.set(CheckpointIntervalDefault, "3")
    val path =
      try create("checkpointed")
      finally spark.conf.unset(CheckpointIntervalDefault)
    (1L to 4L).foreach(batch => append(path, batch * 10L, batch * 10L + 10L))
    val pointer = new String(Files.readAllBytes(logOf(path).resolve("_last_checkpoint")))
    pointer should include(""""version":3""")

    dieAfterTaking(path, version = 5, Seq(EmptyCommit))
    Files.exists(commitFile(path, 5)) shouldBe false

    // A new process: it reads the pointer, then lists the log from the checkpoint on.
    DeltaLog.clearCache()
    versionOf(path) shouldBe 5L
    idsOf(path) shouldBe (0L until 50L)
    everyCommitIsCoordinated(path)
    // The pointer is replaced whole, and has no sidecar that could go stale.
    Files.exists(logOf(path).resolve("._last_checkpoint.crc")) shouldBe false
  }

  test("Delta's log cleanup removes old commits and never the latest, and the table goes on") {
    val path = create("expired")
    (1L to 4L).foreach(batch => append(path, batch * 10L, batch * 10L + 10L))
    // Forty days pass, which is more than the thirty a commit is kept after a checkpoint.
    val fortyDaysAgo = FileTime.from(Instant.now().minus(40, ChronoUnit.DAYS))
    (0L to 4L).foreach(version =>
      Files.setLastModifiedTime(commitFile(path, version), fortyDaysAgo)
    )

    // The cleanup comes with a checkpoint.
    val log = DeltaLog.forTable(spark, path)
    log.checkpoint(log.update())

    (0L to 3L).map(version => Files.exists(commitFile(path, version))) shouldBe Seq.fill(4)(false)
    Files.exists(commitFile(path, 4)) shouldBe true

    // A reader is asked for the latest commit only, and a writer for the one before its own.
    DeltaLog.clearCache()
    idsOf(path) shouldBe (0L until 50L)
    append(path, 50L, 60L)
    versionOf(path) shouldBe 5L
    idsOf(path) shouldBe (0L until 60L)
    (0L to 5L).map(entry(path, _).complete) shouldBe Seq.fill(6)(true)

    // Collecting temporary files after the cleanup: a known limit. The file of a commit that
    // Delta expired looks like the only copy of a commit that was lost, and is kept.
    val fates = maintenance.collectTemps(new Path(path), fs)
    def fateOf(version: Long): TempFate = fates(entry(path, version).tempPath.stripPrefix(".tmp/"))
    (0L to 3L).map(fateOf) shouldBe Seq.fill(4)(TempFate.KeptOnlyCopy)
    (4L to 5L).map(fateOf) shouldBe Seq.fill(2)(TempFate.RemovedPublished)
    idsOf(path) shouldBe (0L until 60L)
  }

  // ---------------------------------------------------------------------------------------------
  // Creating a table
  // ---------------------------------------------------------------------------------------------

  test("of two transactions that create one table, the second is told by Delta of the first") {
    val path = tablePath("created-twice")
    val log = DeltaLog.forTable(spark, path)

    def creation(): OptimisticTransaction = {
      val transaction = log.startTransaction(None)
      transaction.updateMetadataForNewTable(Metadata(schemaString = rows(0L, 1L).schema.json))
      transaction
    }
    val (first, second) = (creation(), creation())

    first.commit(Nil, DeltaOperations.ManualUpdate) shouldBe 0L
    // The LogStore answers "version taken"; what the second creator gets is Delta's verdict on
    // the two transactions, which an engine retries, and not an error for an operator.
    a[java.util.ConcurrentModificationException] should be thrownBy
      second.commit(Nil, DeltaOperations.ManualUpdate)

    versionOf(path) shouldBe 0L
    everyCommitIsCoordinated(path)
    store.registration(keyOf(path)).map(_.registeredVersion) shouldBe
      Some(Registration.NotYetCreated)
  }

  test("F9: a table dropped by deleting its directory is created again after a registry reset") {
    val path = create("dropped")
    append(path, 10L, 20L)
    LocalSparkSuite.deleteRecursively(directoryOf(path))
    DeltaLog.clearCache()

    // The registry still has the commits of the table that was there: nothing is created under
    // them, and the refusal says what to do.
    val refused = intercept[Exception](rows(100L, 105L).write.format("delta").save(path))
    causesOf(refused).map(_.getMessage).mkString should include("reset its registry")
    Files.exists(commitFile(path, 0)) shouldBe false

    maintenance.reset(new Path(path), fs)

    DeltaLog.clearCache()
    rows(100L, 105L).write.format("delta").save(path)
    idsOf(path) shouldBe (100L until 105L)
    versionOf(path) shouldBe 0L
    everyCommitIsCoordinated(path)
  }
}

object PostgresLogStoreEndToEndSpec {

  /** The session setting that gives new tables their `delta.checkpointInterval`. */
  private val CheckpointIntervalDefault =
    "spark.databricks.delta.properties.defaults.checkpointInterval"
}
