package lakehouse.ingestion.cli

import scala.collection.immutable.SortedMap

import lakehouse.ingestion.core.config.{Profile, SourceId}
import lakehouse.ingestion.engine.control.LogStoreSettings
import lakehouse.ingestion.logstore.{PostgresLogStore, StartupGuard}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

/**
 * The Spark session of a run: what its profile gives it, and what the engine gives every
 * session, which a profile may not set (`Profile.EngineSpark`).
 *
 *  - the master of the profile, and the session named after its source;
 *  - Delta's extension and catalog;
 *  - `PostgresLogStore` for the scheme of the lake, so that every commit to a table of the lake
 *    is coordinated (ADR-0004);
 *  - the settings of the LogStore, made from `databases.coordination`, under `spark.hadoop.`:
 *    Spark copies them into the Hadoop configuration that Delta hands a LogStore, on the driver
 *    and in the tasks.
 */
object RunSession {

  /** What a session is called: the application, and the source that it runs. */
  def appName(source: SourceId): String = s"lakehouse-ingestion ${source.value}"

  /** Every setting of the session of a run of `source`, the profile's and the engine's. */
  def settings(profile: Profile, source: SourceId): SortedMap[String, String] =
    profile.spark.settings ++ engine(profile, source)

  /** The settings that the engine gives the session, whatever the profile says. */
  def engine(profile: Profile, source: SourceId): SortedMap[String, String] = {
    val scheme = profile.lake.uri.scheme.name
    SortedMap(
      "spark.master" -> profile.spark.master.value,
      Profile.EngineSpark.AppName -> appName(source),
      Profile.EngineSpark.Extensions -> "io.delta.sql.DeltaSparkSessionExtension",
      Profile.EngineSpark.Catalog -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
      StartupGuard.logStoreKey(scheme) -> classOf[PostgresLogStore].getName
    ) ++ LogStoreSettings.hadoop(profile.databases.coordination).map { case (key, value) =>
      s"spark.hadoop.$key" -> value
    }
  }

  /**
   * The settings that a session has and that neither its profile nor the engine gave it, and
   * that a profile would be refused for: from `spark-defaults.conf`, from a system property of
   * the JVM, or from `spark-submit --conf`. Each with why it is refused.
   *
   * The engine's own keys are overridden by what the engine gives a session; what is under a
   * prefix of the engine's, or what would make Delta forget the transactions of a table, is
   * not, and would be used.
   *
   * @param engine the settings that the engine gave the session
   */
  def foreign(
      effective: Map[String, String],
      engine: Map[String, String]
  ): Seq[(String, String)] =
    effective.toSeq.sortBy(_._1).flatMap { case (key, value) =>
      Profile
        .refusedSparkSetting(key)
        .filterNot(_ => engine.get(key).contains(value))
        .map(key -> _)
    }

  /** Opens the session of a run. A JVM has one: there must be none open. */
  def open(settings: Map[String, String]): SparkSession = {
    require(
      SparkSession.getActiveSession.isEmpty && SparkSession.getDefaultSession.isEmpty,
      "a Spark session is open in this JVM already: a run opens its own"
    )
    settings
      .foldLeft(SparkSession.builder()) { case (builder, (key, value)) =>
        builder.config(key, value)
      }
      .getOrCreate()
  }

  /**
   * Stops the session, and leaves the JVM as the run found it: no session, and no table cached
   * with the LogStore of this one. A process ends after it; a test that runs the command in its
   * own JVM runs the next command after it.
   */
  def close(spark: SparkSession): Unit =
    try spark.stop()
    finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      DeltaLog.clearCache()
    }
}
