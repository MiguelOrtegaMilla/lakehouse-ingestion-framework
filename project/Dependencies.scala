import sbt._

/**
 * Single source of truth for library versions (ADR-0002, versions pinned by ADR-0014).
 *
 * A library is added here when the first module that uses it is written, not before.
 */
object Dependencies {

  object Versions {
    // Must equal the Scala patch Apache Spark is built with (spark-parent `scala.version`).
    val scala = "2.13.17"

    val spark = "4.1.4"
    val delta = "4.4.1"
    // The Hadoop line bundled with Spark 4.1.4 (spark-parent `hadoop.version`).
    val hadoop = "3.4.2"
  }

  // Spark and Delta are `provided`: the Spark runtime (local or Standalone) supplies them.
  val sparkSql = "org.apache.spark" %% "spark-sql" % Versions.spark % Provided
  val deltaSpark = "io.delta" % "delta-spark_4.1_2.13" % Versions.delta % Provided

  // The LogStore API only; `delta-pg-logstore` must not see Spark or delta-spark (ADR-0012).
  val deltaStorage = "io.delta" % "delta-storage" % Versions.delta
  val hadoopClientApi = "org.apache.hadoop" % "hadoop-client-api" % Versions.hadoop % Provided
}
