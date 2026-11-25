package lakehouse.ingestion.cli

import java.time.{ZoneId, ZoneOffset}
import java.util.TimeZone

import scala.collection.immutable.SortedMap
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import lakehouse.ingestion.core.config.{Profile, SourceId}
import lakehouse.ingestion.engine.control.LogStoreSettings
import lakehouse.ingestion.logstore.{FaultHooks, FaultPoint, PostgresLogStore, StartupGuard}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

/**
 * The Spark session of a run: what its profile gives it, and what the engine gives every
 * session, which a profile may not set (`Profile.EngineSpark`).
 *
 *  - the master of the profile, and the session named after its source;
 *  - Delta's extension and catalog;
 *  - the metrics of a commit recorded by Delta, which is where the engine reads the rows that
 *    a batch wrote;
 *  - UTC as the time zone of the session, which is that of the JVM as well (`jvmToUtc`);
 *  - `PostgresLogStore` for the scheme of the lake, so that every commit to a table of the lake
 *    is coordinated (ADR-0004);
 *  - the settings of the LogStore, made from `databases.coordination`, under `spark.hadoop.`:
 *    Spark copies them into the Hadoop configuration that Delta hands a LogStore, on the driver
 *    and in the tasks.
 */
object RunSession {

  /** The time zone that everything of a run is in (discovery §4, §17). */
  val Zone = "UTC"

  /**
   * Sets the time zone of the JVM to UTC, before anything of a run is opened: a connection to
   * a database takes the zone of the JVM when it is made, and Spark writes and reads a time
   * with no zone in it (pinned by `SparkJdbcContractSpec`). A process that is started in
   * another zone, as a machine has it, is then in the one zone that a run is correct in.
   *
   * It is for the JVM of the command. Where the tasks of Spark run in a JVM of their own, on
   * another machine, that JVM is whoever starts it's to set (E1).
   *
   * @param set what sets the zone of the JVM: `TimeZone.setDefault`, outside tests
   * @return why the JVM is not in UTC, if it could not be set so
   */
  def jvmToUtc(set: TimeZone => Unit = TimeZone.setDefault): Either[String, Unit] = {
    val refused =
      try {
        set(TimeZone.getTimeZone(Zone))
        None
      } catch { case NonFatal(failure) => Some(failure) }
    val now = ZoneId.systemDefault()
    Either.cond(
      now.getRules == ZoneOffset.UTC.getRules,
      (),
      s"the time zone of the JVM is ${now.getId}, and could not be set to $Zone" +
        refused.fold("")(failure => s" (${failure.getMessage})") +
        ". A time with no zone would mean one thing to the engine and another to a source, " +
        s"and rows would be read twice, or not at all. Start the JVM with -Duser.timezone=$Zone"
    )
  }

  /** What a session is called: the application, and the source that it runs. */
  def appName(source: SourceId): String = s"lakehouse-ingestion ${source.value}"

  /** Every setting of the session of a run of `source`, the profile's and the engine's. */
  def settings(
      profile: Profile,
      source: SourceId,
      waits: LogStoreWaits = NoWaits
  ): SortedMap[String, String] =
    profile.spark.settings ++ engine(profile, source, waits)

  /**
   * The settings that the engine gives the session, whatever the profile says.
   *
   * @param waits how long a writer of the LogStore waits at each of its points: none, but in a
   *   launcher of the tests that holds a process inside a commit to kill it there (P7.10). They
   *   are the engine's to give, and only code gives them: a profile, a system property of the
   *   JVM, `spark-defaults.conf` or `--conf` that gives one is refused (`foreign`)
   */
  def engine(
      profile: Profile,
      source: SourceId,
      waits: LogStoreWaits = NoWaits
  ): SortedMap[String, String] = {
    val scheme = profile.lake.uri.scheme.name
    SortedMap(
      "spark.master" -> profile.spark.master.value,
      Profile.EngineSpark.AppName -> appName(source),
      Profile.EngineSpark.Extensions -> "io.delta.sql.DeltaSparkSessionExtension",
      Profile.EngineSpark.Catalog -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
      Profile.EngineSpark.CommitMetrics -> "true",
      Profile.EngineSpark.SessionTimeZone -> Zone,
      StartupGuard.logStoreKey(scheme) -> classOf[PostgresLogStore].getName
    ) ++ LogStoreSettings.hadoop(profile.databases.coordination).map { case (key, value) =>
      s"spark.hadoop.$key" -> value
    } ++ waits.map { case (point, wait) =>
      s"spark.hadoop.${FaultHooks.delayKey(point)}" -> wait.toMillis.toString
    }
  }

  /** How long a writer of the LogStore waits at each of its points that it waits at. */
  type LogStoreWaits = Map[FaultPoint, FiniteDuration]

  /** No wait at any point: what every process but a launcher of the tests has. */
  val NoWaits: LogStoreWaits = Map.empty

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
