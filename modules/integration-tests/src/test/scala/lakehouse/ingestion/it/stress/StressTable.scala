package lakehouse.ingestion.it.stress

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * The table every stress writer appends to (ADR-0004 §7.2).
 *
 * Each commit of writer `i` carries `txnAppId = writer-<i>` and `txnVersion = k`, and writes
 * `rowsPerCommit` rows tagged with the same `(writer_id, txn_version)`. The tags make every row
 * attributable to the commit that wrote it, so the verifier can check row counts per commit
 * without trusting the Delta log it is checking.
 */
object StressTable {

  val schema: StructType = StructType(
    Seq(
      StructField("writer_id", StringType, nullable = false),
      StructField("txn_version", LongType, nullable = false),
      StructField("row_seq", IntegerType, nullable = false)
    )
  )

  /** Both the `writer_id` column value and the writer's Delta `txnAppId`. */
  def writerId(writer: Int): String = s"writer-$writer"

  def rows(writer: Int, txnVersion: Long, rowsPerCommit: Int): Seq[Row] =
    (0 until rowsPerCommit).map(seq => Row(writerId(writer), txnVersion, seq))

  /**
   * Creates the table at version 0, empty, before any writer starts. Writers then only append,
   * so they never race to create the table, which is a different problem from the one measured.
   */
  def create(spark: SparkSession, tableUri: String): Unit =
    spark
      .createDataFrame(Seq.empty[Row].asJava, schema)
      .write
      .format("delta")
      .mode("errorifexists")
      .save(tableUri)
}
