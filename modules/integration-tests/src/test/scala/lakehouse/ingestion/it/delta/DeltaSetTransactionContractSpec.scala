package lakehouse.ingestion.it.delta

import java.net.URI
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Path, Paths}
import java.time.Instant
import java.time.temporal.ChronoUnit

import scala.concurrent.duration._

import lakehouse.ingestion.it.logstore.RecordingLogStore
import lakehouse.ingestion.it.logstore.RecordingLogStore.{commitVersion, kindOf, FileKind, Read}
import lakehouse.ingestion.it.support.LocalSparkSuite
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{Action, AddFile, SetTransaction}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What Delta 4.4 records of a write that names its transaction, and how it is read back from a
 * log: the facts `DeltaTableTransactions` is built on (ADR-0005 §3; discovery §5, R3, and §20).
 *
 * These are characterisation tests, as those of `DeltaLogStoreContractSpec` are. They test
 * Delta, and no code of the engine is in them. Each pins a behaviour of a class that is not
 * Delta's API and that the engine asks: `Snapshot.transactions`, `DeltaLog.getChanges`,
 * `SetTransaction`. A Delta upgrade that changes one of these facts fails here, with the name
 * of the assumption it breaks, before it can make the engine take a batch that is written for
 * one that is not, or record it at the version of another commit (ADR-0014, upgrade procedure).
 *
 * The store under Delta is its own for `file://` with a journal (`RecordingLogStore`), which
 * says which commits a call read.
 */
final class DeltaSetTransactionContractSpec
    extends AnyFunSuite
    with Matchers
    with LocalSparkSuite
    with BeforeAndAfterEach {

  import DeltaSetTransactionContractSpec._

  override protected def sparkConf: Map[String, String] = Map(
    "spark.delta.logStore.file.impl" -> classOf[RecordingLogStore].getName
  )

  override def beforeEach(): Unit = {
    super.beforeEach()
    RecordingLogStore.reset()
  }

  override def afterEach(): Unit =
    try RecordingLogStore.reset()
    finally super.afterEach()

  // ---------------------------------------------------------------------------------------------
  // What a write records
  // ---------------------------------------------------------------------------------------------

  test("a write that names its transaction has it recorded in the commit of its rows") {
    val path = tablePath("recorded")
    write(path, Seq("orders-1"), Some(Orders -> 1L)) // version 0, and the table with it
    write(path, Seq("no batch"), None) // 1
    write(path, Seq("orders-2"), Some(Orders -> 2L)) // 2
    write(path, Seq("returns-1"), Some(Returns -> 1L)) // 3

    val commits = DeltaLog.forTable(spark, path).getChanges(0L).toSeq
    commits.map(_._1) shouldBe Seq(0L, 1L, 2L, 3L)
    commits.map { case (_, actions) => transactionsIn(actions) } shouldBe Seq(
      Seq(Orders -> 1L),
      Nil,
      Seq(Orders -> 2L),
      Seq(Returns -> 1L)
    )
    // The commit that has the transaction is the one that adds the rows: they are not in the
    // table at the version before it, and are there from it on.
    commits.foreach { case (_, actions) =>
      actions.collect { case added: AddFile => added } should not be empty
    }
    rowsAt(path, 1) shouldBe Seq("no batch", "orders-1")
    rowsAt(path, 2) shouldBe Seq("no batch", "orders-1", "orders-2")
    rowsAt(path, 3) shouldBe Seq("no batch", "orders-1", "orders-2", "returns-1")
  }

  test("a write whose transaction a table has is not committed: one commit holds a transaction") {
    val path = tablePath("skipped")
    write(path, Seq("orders-1"), Some(Orders -> 1L))
    write(path, Seq("orders-2"), Some(Orders -> 2L))
    write(path, Seq("returns-1"), Some(Returns -> 1L))

    // The last of an application, one before it, and the last of another.
    write(path, Seq("orders-2, again"), Some(Orders -> 2L))
    write(path, Seq("orders-1, again"), Some(Orders -> 1L))
    write(path, Seq("returns-1, again"), Some(Returns -> 1L))

    val log = DeltaLog.forTable(spark, path)
    log.update().version shouldBe 2L
    log.getChanges(0L).flatMap { case (_, actions) => transactionsIn(actions) }.toSeq shouldBe
      Seq(Orders -> 1L, Orders -> 2L, Returns -> 1L)
    rowsAt(path, 2) shouldBe Seq("orders-1", "orders-2", "returns-1")
  }

  // ---------------------------------------------------------------------------------------------
  // What a table knows of its transactions
  // ---------------------------------------------------------------------------------------------

  test("the last transaction of each application is in the state of a table") {
    val path = tablePath("state")
    val log = DeltaLog.forTable(spark, path)

    // No table there: no version, no transaction, and asking has made nothing.
    val absent = log.update()
    absent.version shouldBe -1L
    absent.transactions shouldBe empty
    Files.exists(directoryOf(path)) shouldBe false

    write(path, Seq("orders-1"), Some(Orders -> 1L))
    write(path, Seq("returns-1"), Some(Returns -> 1L))
    write(path, Seq("orders-2"), Some(Orders -> 2L))
    write(path, Seq("no batch"), None)

    val transactions: Map[String, Long] = log.update().transactions
    transactions shouldBe Map(Orders -> 2L, Returns -> 1L)
  }

  test("a table whose log is cleaned up keeps its transactions, and not the commits of them") {
    val path = tablePath("cleaned-up")
    (1L to 4L).foreach(version => write(path, Seq(s"orders-$version"), Some(Orders -> version)))
    write(path, Seq("returns-1"), Some(Returns -> 1L)) // version 4
    cleanUp(path, 0L to 4L)

    // Delta removes the commits before a checkpoint that are older than what the log keeps,
    // and never the last one.
    commitsOnDisk(path) shouldBe Seq(4L)
    // What this process knew of the table is let go: the state is read from the checkpoint.
    DeltaLog.clearCache()
    val log = DeltaLog.forTable(spark, path)
    log.update().transactions shouldBe Map(Orders -> 4L, Returns -> 1L)

    // Asked from a version that it does not keep, a log starts at the first that it keeps, and
    // says nothing of the others.
    log.getChanges(0L).map(_._1).toSeq shouldBe Seq(4L)
    log.getChanges(2L).map(_._1).toSeq shouldBe Seq(4L)

    // And a write of then is skipped still.
    write(path, Seq("orders-1, again"), Some(Orders -> 1L))
    log.update().version shouldBe 4L
  }

  test("a table that sets a retention for its transactions forgets them, and takes a write again") {
    // Why no table of the engine sets `delta.setTransactionRetentionDuration`, and none that it
    // writes to may have it.
    val path = tablePath("forgetful")
    write(path, Seq("orders-1"), Some(Orders -> 1L))
    DeltaLog.forTable(spark, path).update().transactions shouldBe Map(Orders -> 1L)

    spark.sql(
      s"ALTER TABLE delta.`$path` SET TBLPROPERTIES " +
        "('delta.setTransactionRetentionDuration' = 'interval 1 second')"
    ): Unit
    // The transaction is dropped when the state of the table is computed after its second is
    // over, by the clock of this machine: asked until it is.
    eventually(30.seconds.fromNow) {
      DeltaLog.clearCache()
      DeltaLog.forTable(spark, path).update().transactions.isEmpty
    }

    val before = DeltaLog.forTable(spark, path).update().version
    write(path, Seq("orders-1"), Some(Orders -> 1L))
    DeltaLog.forTable(spark, path).update().version shouldBe before + 1
    rowsAt(path, before + 1) shouldBe Seq("orders-1", "orders-1")
  }

  // ---------------------------------------------------------------------------------------------
  // How a log is read
  // ---------------------------------------------------------------------------------------------

  test("the commits of a log are given from a version on, in order, each read when it is taken") {
    val path = tablePath("read")
    (1L to 5L).foreach(version => write(path, Seq(s"orders-$version"), Some(Orders -> version)))
    val log = DeltaLog.forTable(spark, path)
    log.update().version shouldBe 4L
    RecordingLogStore.clearJournal()

    // Asking for the commits reads none of them, and neither does asking whether there is one.
    val changes = log.getChanges(2L)
    changes.hasNext shouldBe true
    commitsRead() shouldBe empty

    val (first, actions) = changes.next()
    first shouldBe 2L
    transactionsIn(actions) shouldBe Seq(Orders -> 3L)
    commitsRead() shouldBe Seq(2L)

    changes.hasNext shouldBe true
    changes.next()._1 shouldBe 3L
    commitsRead() shouldBe Seq(2L, 3L)

    changes.map(_._1).toSeq shouldBe Seq(4L)
    commitsRead() shouldBe Seq(2L, 3L, 4L)

    // Beyond the last commit there is none, and no error.
    log.getChanges(5L).toSeq shouldBe empty
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  /** An append, under the name of an application and a version if it has a transaction. */
  private def write(path: String, rows: Seq[String], transaction: Option[(String, Long)]): Unit = {
    val session = spark
    import session.implicits._
    val writer = rows.toDF("value").write.format("delta").mode("append")
    transaction
      .fold(writer) { case (app, version) =>
        writer.option("txnAppId", app).option("txnVersion", version)
      }
      .save(path)
  }

  /** Every row of a table as it was at a version, sorted. */
  private def rowsAt(path: String, version: Long): Seq[String] =
    spark.read
      .format("delta")
      .option("versionAsOf", version)
      .load(path)
      .collect()
      .map(_.getString(0))
      .sorted
      .toSeq

  private def transactionsIn(actions: Seq[Action]): Seq[(String, Long)] =
    actions.collect { case SetTransaction(app, version, _) => app -> version }

  /** The commits that Delta has read through its LogStore since the journal was cleared. */
  private def commitsRead(): Seq[Long] =
    RecordingLogStore.events.collect {
      case read: Read if kindOf(read.path) == FileKind.Commit => commitVersion(read.path)
    }

  private def directoryOf(path: String): Path = Paths.get(URI.create(path))

  private def commitFile(path: String, version: Long): Path =
    directoryOf(path).resolve("_delta_log").resolve(f"$version%020d.json")

  private def commitsOnDisk(path: String): Seq[Long] =
    (0L to DeltaLog.forTable(spark, path).update().version)
      .filter(version => Files.exists(commitFile(path, version)))

  /**
   * Forty days pass for the commits, which is more than the thirty that a log keeps them for,
   * and a checkpoint is written, which is when Delta cleans a log up.
   */
  private def cleanUp(path: String, versions: Seq[Long]): Unit = {
    val fortyDaysAgo = FileTime.from(Instant.now().minus(40, ChronoUnit.DAYS))
    versions.foreach(version => Files.setLastModifiedTime(commitFile(path, version), fortyDaysAgo))
    val log = DeltaLog.forTable(spark, path)
    log.checkpoint(log.update())
  }

  @scala.annotation.tailrec
  private def eventually(deadline: Deadline)(condition: => Boolean): Unit =
    if (!condition) {
      if (deadline.isOverdue()) fail("the condition did not come to hold in time")
      Thread.sleep(200)
      eventually(deadline)(condition)
    }
}

object DeltaSetTransactionContractSpec {

  /** Two applications, named as the engine names the sequences of two sources. */
  private val Orders = "shop.orders/bronze"
  private val Returns = "shop.returns/bronze"
}
