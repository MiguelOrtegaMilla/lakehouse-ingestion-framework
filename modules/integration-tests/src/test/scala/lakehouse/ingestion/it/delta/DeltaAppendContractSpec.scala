package lakehouse.ingestion.it.delta

import java.net.URI
import java.nio.file.{Files, Paths}

import lakehouse.ingestion.it.support.LocalSparkSuite
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo, SetTransaction}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.scalatest.LoneElement
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What Delta 4.4 says of an append in its commit: the count of its rows, and what is committed
 * for an append that has none (discovery §17, P6; §15).
 *
 * A batch of a JDBC source is read once, while it is written, and is not counted before the
 * write (discovery §17, P6). What it wrote is what its commit says, `numOutputRows` of the
 * metrics of the operation. These are characterisation tests, as those of
 * `DeltaSetTransactionContractSpec` are, with no code of the engine in them.
 */
final class DeltaAppendContractSpec
    extends AnyFunSuite
    with Matchers
    with LoneElement
    with LocalSparkSuite {

  import DeltaAppendContractSpec._

  test("the commit of an append counts its rows, by default") {
    // Asked of Delta's own setting: a session knows the name only once Delta has read it.
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED) shouldBe true
    val path = tablePath("counted")
    write(path, Seq("a", "b", "c"), Some(Orders -> 1L))
    write(path, Seq("d"), Some(Orders -> 2L))

    commits(path).map(c => metricsOf(c).flatMap(_.get("numOutputRows"))) shouldBe
      Seq(Some("3"), Some("1"))
  }

  test("a session that does not record the metrics of a commit counts nothing") {
    val path = tablePath("uncounted")
    spark.conf.set(MetricsEnabled, "false")
    try write(path, Seq("a", "b"), Some(Orders -> 1L))
    finally spark.conf.unset(MetricsEnabled)

    commits(path).map(metricsOf) shouldBe Seq(None)
  }

  test("an append of no rows to a table is a commit, with its transaction and no file") {
    val path = tablePath("empty-append")
    write(path, Seq("a"), Some(Orders -> 1L))
    write(path, Nil, Some(Orders -> 2L))

    val last = commits(path).last
    last.version shouldBe 1L
    last.transactions shouldBe Seq(Orders -> 2L)
    last.files shouldBe 0
    metricsOf(last).flatMap(_.get("numOutputRows")) shouldBe Some("0")
    // And it is skipped, written again, as any other.
    write(path, Nil, Some(Orders -> 2L))
    DeltaLog.forTable(spark, path).update().version shouldBe 1L
  }

  test("an append of no rows where there is no table makes one") {
    val path = tablePath("empty-create")
    write(path, Nil, Some(Orders -> 1L))

    Files.isDirectory(Paths.get(URI.create(path)).resolve("_delta_log")) shouldBe true
    val only = commits(path).loneElement
    only.transactions shouldBe Seq(Orders -> 1L)
    only.files shouldBe 0
    spark.read.format("delta").load(path).columns.toSeq shouldBe Seq("value")
  }

  /** An append of `rows` as a column `value`, under a transaction if it names one. */
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

  private def commits(path: String): Seq[Commit] =
    DeltaLog.forTable(spark, path).getChanges(0L).toSeq.map { case (version, actions) =>
      Commit(version, actions)
    }
}

object DeltaAppendContractSpec {

  private val MetricsEnabled = DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key

  private val Orders = "shop.orders/bronze"

  final private case class Commit(version: Long, actions: Seq[Action]) {
    def info: CommitInfo = actions.collectFirst { case c: CommitInfo => c }.getOrElse {
      throw new IllegalStateException(s"commit $version has no commitInfo")
    }
    def transactions: Seq[(String, Long)] =
      actions.collect { case SetTransaction(app, version, _) => app -> version }
    def files: Int = actions.count(_.isInstanceOf[AddFile])
  }

  /** The metrics of the operation of a commit; none, if it recorded none. */
  private def metricsOf(commit: Commit): Option[Map[String, String]] =
    commit.info.operationMetrics.filter(_.nonEmpty)
}
