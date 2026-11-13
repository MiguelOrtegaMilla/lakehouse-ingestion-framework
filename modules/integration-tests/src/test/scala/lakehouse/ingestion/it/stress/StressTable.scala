package lakehouse.ingestion.it.stress

import java.net.URI

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
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
   *
   * The table is created from its schema alone. Saving an empty DataFrame instead would leave
   * a data file on disk that no commit references, the same trace a lost commit leaves.
   *
   * The location is passed in Hadoop's qualified form (`file:/...`). Given `file:///...`,
   * `CREATE TABLE` fails with `DELTA_AMBIGUOUS_PATHS_IN_CREATE_TABLE`: Delta sees two spellings
   * of one location, the canonical-identity problem of ADR-0004 §3.6.
   */
  def create(spark: SparkSession, tableUri: String): Unit = {
    val location = new Path(URI.create(tableUri)).toString
    DeltaTable.create(spark).location(location).addColumns(schema).execute(): Unit
  }
}
