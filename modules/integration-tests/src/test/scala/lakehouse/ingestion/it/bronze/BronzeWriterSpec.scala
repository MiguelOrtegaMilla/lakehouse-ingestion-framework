package lakehouse.ingestion.it.bronze

import java.nio.file.{Files, Path => LocalPath}
import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.Using

import io.delta.tables.DeltaTable
import lakehouse.ingestion.core.config.{
  PathTableLocator,
  RelativePath,
  RootUri,
  SourceId,
  TableLocation
}
import lakehouse.ingestion.core.control.{BatchId, BatchSeq, FailureClass, Layer, RunId, TxnAppId}
import lakehouse.ingestion.engine.RunFailedException
import lakehouse.ingestion.engine.bronze.BronzeWriter.{AlreadyThere, Appended}
import lakehouse.ingestion.engine.bronze.{BronzeBatch, BronzeWriter}
import lakehouse.ingestion.engine.delta.{CommitMetadata, DeltaTableTransactions, Lake}
import lakehouse.ingestion.it.logstore.CoordinatedDeltaSuite
import lakehouse.ingestion.logstore.CommitContext
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{LongType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{EitherValues, OptionValues}

/**
 * The writer of Bronze on real tables, through the coordinated LogStore (P5.5): what it
 * writes of a batch, that it writes it once, what it refuses before it writes anything, and
 * what it does when another writer changes the table under it.
 */
final class BronzeWriterSpec
    extends AnyFunSuite
    with Matchers
    with OptionValues
    with EitherValues
    with CoordinatedDeltaSuite {

  import BronzeWriterSpec._

  private lazy val lakeDirectory: LocalPath = Files.createDirectories(workDir.resolve("lake"))
  private lazy val root = RootUri.parse(s"file://$lakeDirectory").value
  private lazy val locator = new PathTableLocator(root)
  private lazy val lake = new Lake(root, spark.sessionState.newHadoopConf())
  private lazy val delta = new DeltaTableTransactions(spark)

  private val returns = SourceId.parse("shop.returns").value
  private val orders = SourceId.parse("shop.orders").value
  private val readAt = Instant.parse("2026-11-18T10:15:30.123456Z")

  private def table(name: String): TableLocation = locator.locate(RelativePath.parse(name).value)

  private def batch(source: SourceId, seq: Long, run: RunId = RunId.generate()): BronzeBatch =
    BronzeBatch(
      run,
      source,
      BatchId(TxnAppId.of(source, Layer.Bronze), BatchSeq.of(seq).value),
      readAt
    )

  /** A writer of the run of a batch, as a run has one. */
  private def writerOf(batch: BronzeBatch): BronzeWriter =
    new BronzeWriter(lake, delta, CommitContext.forRun(batch.run.uuid))

  private def write(
      table: TableLocation,
      batch: BronzeBatch,
      rows: DataFrame
  ): BronzeWriter.Result =
    writerOf(batch).write(table, batch, rows)

  /** Rows as a read of text gives them: every column a string. */
  private def rows(values: (String, String)*): DataFrame = {
    val session = spark
    import session.implicits._
    values.toDF("id", "amount")
  }

  private def read(table: TableLocation): DataFrame = spark.read.format("delta").load(table.uri)

  private def idsOf(table: TableLocation): Seq[String] =
    read(table).select("id").collect().map(_.getString(0)).sorted.toSeq

  private def refusal(body: => Any): RunFailedException = intercept[RunFailedException](body)

  private def filesUnder(directory: LocalPath): Seq[String] =
    if (!Files.exists(directory)) Nil
    else
      Using.resource(Files.list(directory))(_.iterator.asScala.map(_.getFileName.toString).toList)

  // ---------------------------------------------------------------------------------------------
  // What is written
  // ---------------------------------------------------------------------------------------------

  test(
    "the rows of a batch are written with what the engine says of them, in a commit of the run"
  ) {
    val target = table("bronze/shop/written")
    val first = batch(returns, 1)

    write(target, first, rows("1" -> "10.50", "2" -> "7")) shouldBe Appended(0, conflicts = 0)

    val written = read(target)
    written.schema.fields.map(field => field.name -> field.dataType).toSeq shouldBe Seq(
      "id" -> StringType,
      "amount" -> StringType,
      "_run_id" -> StringType,
      "_batch_seq" -> LongType,
      "_ingested_at" -> TimestampType,
      "_source_id" -> StringType
    )
    written.orderBy("id").collect().toSeq shouldBe Seq(
      Row("1", "10.50", first.run.toString, 1L, Timestamp.from(readAt), "shop.returns"),
      Row("2", "7", first.run.toString, 1L, Timestamp.from(readAt), "shop.returns")
    )
    // The commit says the same of itself, the table has the transaction of the batch, and the
    // LogStore has the run in the entry of the commit.
    DeltaTable
      .forPath(spark, target.uri)
      .history(1)
      .select("userMetadata")
      .head()
      .getString(0) shouldBe
      CommitMetadata(first.run, returns, first.id).text
    delta.lastVersion(target, first.id.app) shouldBe Some(1)
    delta.committedAt(target, first.id) shouldBe Some(0)
    entry(target.uri, 0).runId shouldBe Some(first.run.uuid)
  }

  test("what a read gives of a row is written as it comes, before what the writer adds") {
    val target = table("bronze/shop/from-a-read")
    val session = spark
    import session.implicits._
    val read = Seq(
      ("1", "landing/returns/a.csv", None: Option[String]),
      ("2", "landing/returns/b.csv", Some("""{"extra":"x"}"""))
    )
      .toDF("id", "_source_file", "_rescued_data")

    write(target, batch(returns, 1), read) shouldBe Appended(0, conflicts = 0)

    this.read(target).columns.toSeq shouldBe
      Seq(
        "id",
        "_source_file",
        "_rescued_data",
        "_run_id",
        "_batch_seq",
        "_ingested_at",
        "_source_id"
      )
    this
      .read(target)
      .select("id", "_source_file", "_rescued_data")
      .orderBy("id")
      .collect()
      .toSeq shouldBe
      Seq(
        Row("1", "landing/returns/a.csv", null),
        Row("2", "landing/returns/b.csv", """{"extra":"x"}""")
      )
  }

  test("a batch that is written again adds nothing, whoever writes it and whatever it brings") {
    val target = table("bronze/shop/again")
    val first = batch(returns, 1)
    write(target, first, rows("1" -> "10")) shouldBe Appended(0, conflicts = 0)

    // The run again, and then another run that recovers the batch, with rows that are not
    // those of its plan: nothing of either gets in.
    write(target, first, rows("1" -> "10")) shouldBe AlreadyThere(Some(0))
    write(target, batch(returns, 1), rows("9" -> "99")) shouldBe AlreadyThere(Some(0))
    versionOf(target.uri) shouldBe 0

    write(target, batch(returns, 2), rows("2" -> "20")) shouldBe Appended(1, conflicts = 0)
    // A run that should have stopped comes back with the batch before.
    write(target, batch(returns, 1), rows("9" -> "99")) shouldBe AlreadyThere(Some(0))
    idsOf(target) shouldBe Seq("1", "2")
    versionOf(target.uri) shouldBe 1
  }

  test("two sources of one table have each its batches, under numbers of their own") {
    val target = table("bronze/shop/fan-in")

    write(target, batch(returns, 1), rows("r1" -> "1")) shouldBe Appended(0, conflicts = 0)
    write(target, batch(orders, 1), rows("o1" -> "1")) shouldBe Appended(1, conflicts = 0)
    write(target, batch(orders, 2), rows("o2" -> "1")) shouldBe Appended(2, conflicts = 0)

    read(target).groupBy("_source_id", "_batch_seq").count().collect().toSet shouldBe Set(
      Row("shop.returns", 1L, 1L),
      Row("shop.orders", 1L, 1L),
      Row("shop.orders", 2L, 1L)
    )
  }

  test("a batch of no rows is committed all the same, and the table has it") {
    val target = table("bronze/shop/no-rows")
    write(target, batch(returns, 1), rows("1" -> "10")) shouldBe Appended(0, conflicts = 0)

    val empty = batch(returns, 2)
    write(target, empty, rows()) shouldBe Appended(1, conflicts = 0)

    delta.lastVersion(target, empty.id.app) shouldBe Some(2)
    idsOf(target) shouldBe Seq("1")
    write(target, empty, rows("9" -> "99")) shouldBe AlreadyThere(Some(1))
  }

  test("a batch is written by the run whose context the writer has, and by no other") {
    val writer = writerOf(batch(returns, 1))
    an[IllegalArgumentException] should be thrownBy
      writer.write(table("bronze/shop/another-run"), batch(returns, 1), rows("1" -> "10"))
  }

  // ---------------------------------------------------------------------------------------------
  // What is refused, before anything is written
  // ---------------------------------------------------------------------------------------------

  test("rows with a column under a name of the engine's are refused, and nothing is written") {
    val target = table("bronze/shop/reserved")
    val session = spark
    import session.implicits._

    Seq(
      "_run_id",
      "_RUN_ID",
      "_Batch_Seq",
      "_ingested_at",
      "_source_id",
      "_SOURCE_FILE",
      "_Rescued_Data"
    )
      .foreach { name =>
        withClue(name) {
          val refused = refusal(write(target, batch(returns, 1), Seq("1" -> "x").toDF("id", name)))
          refused.failure shouldBe FailureClass.Permanent
          refused.getMessage should include(s"a column $name,")
          refused.getMessage should include(name.toLowerCase)
        }
      }
    Files.exists(directoryOf(target.uri)) shouldBe false
    store.registration(keyOf(target.uri)) shouldBe None
  }

  test("a table that a link takes out of the lake is refused, and nothing is written there") {
    val elsewhere = Files.createDirectories(workDir.resolve("elsewhere"))
    val bronze = Files.createDirectories(lakeDirectory.resolve("bronze/linked"))
    Files.createSymbolicLink(bronze.resolve("out"), elsewhere)
    val target = table("bronze/linked/out/orders")

    val refused = refusal(write(target, batch(orders, 1), rows("1" -> "10")))

    refused.failure shouldBe FailureClass.Permanent
    refused.getMessage should include("is not in the lake")
    refused.getMessage should include(elsewhere.toString)
    filesUnder(elsewhere) shouldBe empty
    // Asked alone, as a run asks when it starts.
    refusal(lake.confine(target)).failure shouldBe FailureClass.Permanent
    // A link to a file, where a directory of the lake should be, leads nowhere that is in it.
    Files.createSymbolicLink(bronze.resolve("passwd"), java.nio.file.Paths.get("/etc/passwd"))
    refusal(lake.confine(table("bronze/linked/passwd"))).failure shouldBe FailureClass.Permanent
  }

  test("a link that stays in the lake is written through, and so is a lake that is a link") {
    val bronze = Files.createDirectories(lakeDirectory.resolve("bronze/aliased"))
    val real = Files.createDirectories(bronze.resolve("real"))
    Files.createSymbolicLink(bronze.resolve("alias"), real)

    write(table("bronze/aliased/alias/orders"), batch(orders, 1), rows("1" -> "10")) shouldBe
      Appended(0, conflicts = 0)
    Files.isDirectory(real.resolve("orders/_delta_log")) shouldBe true

    // The lake of a profile may be a link itself, as to a disk that is mounted elsewhere.
    val linked = Files.createSymbolicLink(workDir.resolve("lake-by-a-link"), lakeDirectory)
    val linkedRoot = RootUri.parse(s"file://$linked").value
    val throughLink = new PathTableLocator(linkedRoot).locate(
      RelativePath.parse("bronze/aliased/real/orders").value
    )
    val second = batch(orders, 2)
    new BronzeWriter(
      new Lake(linkedRoot, spark.sessionState.newHadoopConf()),
      delta,
      CommitContext.forRun(second.run.uuid)
    ).write(throughLink, second, rows("2" -> "20")) shouldBe Appended(1, conflicts = 0)
    idsOf(table("bronze/aliased/real/orders")) shouldBe Seq("1", "2")
  }

  test("a table that forgets its transactions is not written to until it keeps them again") {
    val target = table("bronze/shop/forgetful")
    write(target, batch(returns, 1), rows("1" -> "10")) shouldBe Appended(0, conflicts = 0)
    spark.sql(
      s"ALTER TABLE delta.`${target.uri}` SET TBLPROPERTIES " +
        "('delta.setTransactionRetentionDuration' = 'interval 30 days')"
    ): Unit

    val refused = refusal(write(target, batch(returns, 2), rows("2" -> "20")))

    refused.failure shouldBe FailureClass.Permanent
    refused.getMessage should include("delta.setTransactionRetentionDuration")
    versionOf(target.uri) shouldBe 1
    idsOf(target) shouldBe Seq("1")

    spark.sql(
      s"ALTER TABLE delta.`${target.uri}` UNSET TBLPROPERTIES ('delta.setTransactionRetentionDuration')"
    ): Unit
    write(target, batch(returns, 2), rows("2" -> "20")) shouldBe Appended(3, conflicts = 0)
    idsOf(target) shouldBe Seq("1", "2")
  }

  test("the schema of a table is not changed by a write, whatever the session is set to") {
    val target = table("bronze/shop/schema")
    val session = spark
    import session.implicits._
    write(target, batch(returns, 1), rows("1" -> "10")) shouldBe Appended(0, conflicts = 0)

    val autoMerge = "spark.databricks.delta.schema.autoMerge.enabled"
    spark.conf.set(autoMerge, "true")
    try {
      val more = Seq(("2", "20", "a note")).toDF("id", "amount", "note")
      val refused = refusal(write(target, batch(returns, 2), more))
      refused.failure shouldBe FailureClass.Permanent
      refused.getMessage should include("does not take the rows")

      val anotherType = Seq(("2", 20L)).toDF("id", "amount")
      refusal(write(target, batch(returns, 2), anotherType)).failure shouldBe FailureClass.Permanent
    } finally spark.conf.unset(autoMerge)

    read(target).columns.toSeq shouldBe
      Seq("id", "amount", "_run_id", "_batch_seq", "_ingested_at", "_source_id")
    versionOf(target.uri) shouldBe 0
    delta.lastVersion(target, TxnAppId.of(returns, Layer.Bronze)) shouldBe Some(1)

    // Rows that lack a column of the table are taken, with nothing in it.
    write(target, batch(returns, 2), Seq("2").toDF("id")) shouldBe Appended(1, conflicts = 0)
    read(target).select("id", "amount").orderBy("id").collect().toSeq shouldBe
      Seq(Row("1", "10"), Row("2", null))
  }

  test(
    "a column under a name that a table cannot have is a permanent failure, with no table made"
  ) {
    val target = table("bronze/shop/bad-name")
    val session = spark
    import session.implicits._

    val refused = refusal(write(target, batch(returns, 1), Seq("1" -> "x").toDF("id", "order id")))

    refused.failure shouldBe FailureClass.Permanent
    delta.lastVersion(target, TxnAppId.of(returns, Layer.Bronze)) shouldBe None
  }

  // ---------------------------------------------------------------------------------------------
  // Another writer, while a batch is written
  // ---------------------------------------------------------------------------------------------

  /**
   * Writes `held` up to the point where its rows are computed, which is after Delta has looked
   * at the table for it, lets `meanwhile` run whole, and then lets the first go on to its
   * commit.
   */
  private def whileWriting(target: TableLocation, held: BronzeBatch, heldRows: DataFrame)(
      meanwhile: => Any
  ): Either[Throwable, BronzeWriter.Result] = {
    val gate = Gate.close()
    val thread = Executors.newSingleThreadExecutor()
    try {
      val writing = Future(
        write(target, held, heldRows.select(Gate.passing(col("id")).as("id"), col("amount")))
      )(
        ExecutionContext.fromExecutor(thread)
      )
      gate.awaitArrival()
      try meanwhile
      finally gate.open()
      Await.ready(writing, 2.minutes).value.value.toEither
    } finally {
      gate.open()
      thread.shutdownNow(): Unit
    }
  }

  test("two sources that make one table at the same moment are both in it") {
    val target = table("bronze/shop/made-twice")

    val loser = whileWriting(target, batch(orders, 1), rows("o1" -> "1")) {
      write(target, batch(returns, 1), rows("r1" -> "1")) shouldBe Appended(0, conflicts = 0)
    }

    // The one that lost the making of the table wrote again, on the table that was there.
    loser.value shouldBe Appended(1, conflicts = 1)
    idsOf(target) shouldBe Seq("o1", "r1")
    (0L to 1L).map(entry(target.uri, _).complete) shouldBe Seq(true, true)
  }

  test("a batch that another writer of its sequence commits first is there, and is there once") {
    val target = table("bronze/shop/zombie")
    write(target, batch(returns, 1), rows("1" -> "10")) shouldBe Appended(0, conflicts = 0)

    // Two runs with batch 2: one that lost its lease and went on, and the one that took it.
    val late = whileWriting(target, batch(returns, 2), rows("2" -> "20")) {
      write(target, batch(returns, 2), rows("2" -> "20")) shouldBe Appended(1, conflicts = 0)
    }

    late.value shouldBe AlreadyThere(Some(1))
    idsOf(target) shouldBe Seq("1", "2")
    versionOf(target.uri) shouldBe 1
  }

  test("two runs that make a table with the same batch leave it there once") {
    val target = table("bronze/shop/zombie-at-the-start")

    val late = whileWriting(target, batch(returns, 1), rows("1" -> "10")) {
      write(target, batch(returns, 1), rows("1" -> "10")) shouldBe Appended(0, conflicts = 0)
    }

    late.value shouldBe AlreadyThere(Some(0))
    idsOf(target) shouldBe Seq("1")
    versionOf(target.uri) shouldBe 0
  }

  test("a sequence that another writer moves under a batch, without writing it, fails for good") {
    val target = table("bronze/shop/two-writers")
    write(target, batch(returns, 1), rows("1" -> "10")) shouldBe Appended(0, conflicts = 0)

    // Batch 3 is being written when batch 2 of the same sequence is committed: two writers of
    // one sequence, which is what a lease is there to prevent.
    val failed = whileWriting(target, batch(returns, 3), rows("3" -> "30")) {
      write(target, batch(returns, 2), rows("2" -> "20")) shouldBe Appended(1, conflicts = 0)
    }

    val refused = failed.left.value
    refused shouldBe a[RunFailedException]
    refused.asInstanceOf[RunFailedException].failure shouldBe FailureClass.Permanent
    refused.getMessage should include("two writers of one sequence")
    idsOf(target) shouldBe Seq("1", "2")
    delta.lastVersion(target, TxnAppId.of(returns, Layer.Bronze)) shouldBe Some(2)
  }
}

object BronzeWriterSpec {

  /**
   * A point that rows pass as they are computed, where a test holds a write: after Delta has
   * started its transaction, and before it commits.
   */
  final class Gate {
    private val arrived = new CountDownLatch(1)
    private val opened = new CountDownLatch(1)

    def pass(): Unit = {
      arrived.countDown()
      if (!opened.await(2, TimeUnit.MINUTES))
        throw new IllegalStateException("the gate was never opened")
    }

    def awaitArrival(): Unit =
      if (!arrived.await(2, TimeUnit.MINUTES))
        throw new IllegalStateException("no row got to the gate")

    def open(): Unit = opened.countDown()
  }

  object Gate {
    @volatile private var current = new Gate

    /** A new gate, closed, which the rows computed from now on pass. */
    def close(): Gate = {
      current = new Gate
      current
    }

    // Not folded into the plan when it is optimised: the rows are held where they are computed.
    val passing = udf { (value: String) =>
      current.pass()
      value
    }.asNondeterministic()
  }
}
