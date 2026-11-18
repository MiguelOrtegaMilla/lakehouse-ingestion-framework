package lakehouse.ingestion.it.delta

import java.nio.file.Files
import java.nio.file.attribute.FileTime
import java.time.Instant
import java.time.temporal.ChronoUnit

import scala.jdk.CollectionConverters._
import scala.util.Using

import io.circe.Json
import lakehouse.ingestion.core.config.{
  PathTableLocator,
  RelativePath,
  RootUri,
  SourceId,
  TableLocation
}
import lakehouse.ingestion.core.control.{BatchId, Layer, RunId, TableTransactions, TxnAppId}
import lakehouse.ingestion.engine.delta.{CommitMetadata, DeltaTableTransactions}
import lakehouse.ingestion.it.logstore.CoordinatedDeltaSuite
import lakehouse.ingestion.logstore.LogStores
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.DeltaLog

/**
 * What a real Delta table says of its batches, read from its log through the coordinated
 * LogStore (P5.4): the contract that the simulation of P4 passes too, and what only a real
 * table can get wrong.
 *
 * What is assumed of Delta itself in the answers is pinned apart, with no code of the engine
 * (`DeltaSetTransactionContractSpec`).
 */
final class DeltaTableTransactionsSpec
    extends TableTransactionsContract
    with CoordinatedDeltaSuite {

  private lazy val locator =
    new PathTableLocator(RootUri.parse(workDir.resolve("tables").toUri.toString).value)

  private lazy val delta = new DeltaTableTransactions(spark)

  override protected def tables: TableTransactions = delta

  override protected def newTable(name: String): TableLocation =
    locator.locate(RelativePath.parse(name).value)

  /** An idempotent append that says its batch in its commit, as the engine writes one. */
  override protected def write(
      table: TableLocation,
      source: SourceId,
      batch: BatchId,
      rows: Seq[String]
  ): Option[Long] = {
    val before = versionOf(table.uri)
    val after =
      append(table, batch, rows, Some(CommitMetadata(RunId.generate(), source, batch).text))
    Option.when(after > before)(after)
  }

  /**
   * An idempotent append, with whatever its commit is to say of itself.
   *
   * @return the version of the table after it
   */
  private def append(
      table: TableLocation,
      batch: BatchId,
      rows: Seq[String],
      userMetadata: Option[String]
  ): Long = {
    val writer = frame(rows).write
      .format("delta")
      .mode("append")
      .option("txnAppId", batch.app.value)
      .option("txnVersion", batch.seq.value)
    userMetadata.fold(writer)(writer.option(CommitMetadata.WriteOption, _)).save(table.uri)
    versionOf(table.uri)
  }

  override protected def rowsOf(table: TableLocation): Seq[String] =
    spark.read.format("delta").load(table.uri).collect().map(_.getString(0)).sorted.toSeq

  private def frame(rows: Seq[String]) = {
    val session = spark
    import session.implicits._
    rows.toDF("value")
  }

  /** A commit of some other writer: no transaction, and whatever it says of itself. */
  private def foreign(table: TableLocation, userMetadata: Option[String]): Long = {
    val writer = frame(Seq("not a batch")).write.format("delta").mode("append")
    userMetadata.fold(writer)(writer.option(CommitMetadata.WriteOption, _)).save(table.uri)
    versionOf(table.uri)
  }

  test("asking for a table that is not there makes nothing: no directory, no registration") {
    val table = newTable("never-made")

    delta.lastVersion(table, TxnAppId.of(returns, Layer.Bronze)) shouldBe None
    delta.committedAt(table, batch(returns, 1)) shouldBe None

    Files.exists(directoryOf(table.uri)) shouldBe false
    store.registration(keyOf(table.uri)) shouldBe None
  }

  test("a directory that is no table has no batch, and is left as it is") {
    val table = newTable("just-files")
    val directory = Files.createDirectories(directoryOf(table.uri))
    Files.write(directory.resolve("data.csv"), "a,b\n1,2\n".getBytes("UTF-8"))

    delta.lastVersion(table, TxnAppId.of(returns, Layer.Bronze)) shouldBe None
    delta.committedAt(table, batch(returns, 1)) shouldBe None

    Using.resource(Files.list(directory)) { files =>
      files.iterator.asScala.map(_.getFileName.toString).toSeq shouldBe Seq("data.csv")
    }
  }

  test("the log is read, and not what this process remembers of the table") {
    val table = newTable("another-process")
    writeBatch(table, returns, 1).value shouldBe 0
    // This process has looked at the table, and has it in its cache at version 0.
    delta.lastVersion(table, TxnAppId.of(returns, Layer.Bronze)) shouldBe Some(1)

    // Another process commits batch 2: through a LogStore of its own, on the same
    // coordination, with nothing of this session in between.
    val metadata = CommitMetadata(RunId.generate(), returns, batch(returns, 2))
    val commit = Seq(
      Json.obj(
        "commitInfo" -> Json.obj(
          "timestamp" -> Json.fromLong(System.currentTimeMillis()),
          "operation" -> Json.fromString("WRITE"),
          "operationParameters" -> Json.obj(),
          "userMetadata" -> Json.fromString(metadata.text)
        )
      ),
      Json.obj(
        "txn" -> Json.obj(
          "appId" -> Json.fromString(metadata.batch.app.value),
          "version" -> Json.fromLong(2),
          "lastUpdated" -> Json.fromLong(System.currentTimeMillis())
        )
      )
    ).map(_.noSpaces)
    LogStores
      .over(store)
      .write(
        new Path(commitFile(table.uri, 1).toUri),
        commit.iterator.asJava,
        false,
        spark.sessionState.newHadoopConf()
      )

    delta.lastVersion(table, TxnAppId.of(returns, Layer.Bronze)) shouldBe Some(2)
    delta.committedAt(table, batch(returns, 2)) shouldBe Some(1)
    // And Delta, asked to write that batch again, skips it: the two agree on what is there.
    writeBatch(table, returns, 2) shouldBe None
  }

  test("a log longer than a page is read further back, across checkpoints") {
    val table = newTable("long-log")
    val one = new DeltaTableTransactions(spark, firstPage = 1)
    val two = new DeltaTableTransactions(spark, firstPage = 2)
    // A checkpoint every three commits: the state of the table is then read from one, and the
    // commits before it are still in the log.
    val interval = "spark.databricks.delta.properties.defaults.checkpointInterval"
    spark.conf.set(interval, "3")
    val versions =
      try (1L to 10L).map(seq => writeBatch(table, returns, seq).value)
      finally spark.conf.unset(interval)

    Files.exists(logOf(table.uri).resolve("_last_checkpoint")) shouldBe true
    versions shouldBe (0L to 9L)
    one.lastVersion(table, TxnAppId.of(returns, Layer.Bronze)) shouldBe Some(10)
    // Three pages: the last commit, the eight before it, and the first of the table.
    (1L to 10L).map(seq => one.committedAt(table, batch(returns, seq)).value) shouldBe versions
    // Two: the last two commits, and the other eight in a page of sixteen.
    (1L to 10L).map(seq => two.committedAt(table, batch(returns, seq)).value) shouldBe versions
    (1L to 10L).map(seq => delta.committedAt(table, batch(returns, seq)).value) shouldBe versions
  }

  test("a log that ends on the edge of a page is read to its beginning, and no further") {
    // A number that was passed over is looked for in every commit: two commits and pages of
    // one, then of eight, and then nothing is left to read.
    val table = newTable("page-edge")
    val one = new DeltaTableTransactions(spark, firstPage = 1)
    writeBatch(table, returns, 1).value shouldBe 0
    writeBatch(table, returns, 3).value shouldBe 1

    one.committedAt(table, batch(returns, 2)) shouldBe None
    one.committedAt(table, batch(returns, 1)) shouldBe Some(0)
    one.committedAt(table, batch(returns, 3)) shouldBe Some(1)
    // Exactly a page of commits, with the batch in none of them.
    new DeltaTableTransactions(spark, firstPage = 2)
      .committedAt(table, batch(returns, 2)) shouldBe None
  }

  test("commits that are of no batch are passed over, whatever they say of themselves") {
    val table = newTable("other-writers")
    val small = new DeltaTableTransactions(spark, firstPage = 2)

    writeBatch(table, returns, 1).value shouldBe 0
    foreign(table, None) shouldBe 1
    foreign(table, Some("nightly load, by hand")) shouldBe 2
    foreign(table, Some("""{"job":"backfill","batch_seq":2}""")) shouldBe 3
    writeBatch(table, returns, 2).value shouldBe 4
    foreign(table, Some("[1,2,3]")) shouldBe 5
    foreign(table, Some("x" * 5000)) shouldBe 6

    small.committedAt(table, batch(returns, 1)) shouldBe Some(0)
    small.committedAt(table, batch(returns, 2)) shouldBe Some(4)
    small.lastVersion(table, TxnAppId.of(returns, Layer.Bronze)) shouldBe Some(2)
    rowsOf(table).count(_ == "not a batch") shouldBe 5
  }

  test("a commit that names a batch which the table does not have is not believed") {
    val table = newTable("forged")
    writeBatch(table, returns, 1).value shouldBe 0
    // The words of a batch, on a write that is not one: no `txn`, so Delta knows no batch 2.
    val claimed = CommitMetadata(RunId.generate(), returns, batch(returns, 2))
    foreign(table, Some(claimed.text)) shouldBe 1

    delta.lastVersion(table, TxnAppId.of(returns, Layer.Bronze)) shouldBe Some(1)
    delta.committedAt(table, batch(returns, 2)) shouldBe None
    // The batch is still to be written, and is: its rows get in, at a version of their own.
    writeBatch(table, returns, 2).value shouldBe 2
    delta.committedAt(table, batch(returns, 2)) shouldBe Some(2)
  }

  test("a batch is where its transaction is, whatever any commit says of itself") {
    val table = newTable("exact")
    def saying(source: SourceId, seq: Long): Option[String] =
      Some(CommitMetadata(RunId.generate(), source, batch(source, seq)).text)

    // Batch 1 says nothing of itself, and batch 2 says that it is a batch of another source.
    append(table, batch(returns, 1), Seq("returns-1"), None) shouldBe 0
    append(table, batch(returns, 2), Seq("returns-2"), saying(orders, 7)) shouldBe 1
    // Then two commits that are of no batch say that they are the two, after them.
    foreign(table, saying(returns, 1)) shouldBe 2
    foreign(table, saying(returns, 2)) shouldBe 3

    delta.committedAt(table, batch(returns, 1)) shouldBe Some(0)
    delta.committedAt(table, batch(returns, 2)) shouldBe Some(1)
    delta.lastVersion(table, TxnAppId.of(orders, Layer.Bronze)) shouldBe None
    delta.committedAt(table, batch(orders, 7)) shouldBe None
  }

  test("a batch whose commit the log no longer keeps is known to be written, and not where") {
    val table = newTable("cleaned-up")
    val two = new DeltaTableTransactions(spark, firstPage = 2)
    (1L to 5L).map(seq => writeBatch(table, returns, seq).value) shouldBe (0L to 4L)

    // Forty days pass, which is more than the thirty that Delta keeps a commit for, and a
    // checkpoint is written, which is when it cleans a log up.
    val fortyDaysAgo = FileTime.from(Instant.now().minus(40, ChronoUnit.DAYS))
    (0L to 4L).foreach(version =>
      Files.setLastModifiedTime(commitFile(table.uri, version), fortyDaysAgo)
    )
    val log = DeltaLog.forTable(spark, table.uri)
    log.checkpoint(log.update())
    (0L to 4L).filter(version => Files.exists(commitFile(table.uri, version))) shouldBe Seq(4L)
    // A run that starts remembers nothing of the table: its state comes from the checkpoint.
    DeltaLog.clearCache()

    Seq(delta, two).foreach { asked =>
      asked.lastVersion(table, TxnAppId.of(returns, Layer.Bronze)) shouldBe Some(5)
      asked.committedAt(table, batch(returns, 5)) shouldBe Some(4)
      (1L to 4L).map(seq => asked.committedAt(table, batch(returns, seq))) shouldBe
        Seq.fill(4)(None)
    }
    // And none of them is written again: Delta has their transaction, and skips them.
    (1L to 5L).map(seq => writeBatch(table, returns, seq)) shouldBe Seq.fill(5)(None)
    rowsOf(table) should have size 15
  }
}
