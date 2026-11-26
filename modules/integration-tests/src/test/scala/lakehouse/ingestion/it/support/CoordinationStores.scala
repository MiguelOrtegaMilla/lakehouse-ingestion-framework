package lakehouse.ingestion.it.support

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import scala.concurrent.duration._

import lakehouse.ingestion.logstore.PostgresLogStore
import lakehouse.ingestion.logstore.coordination.{CoordinationSettings, SecretRef}

/** Settings with which tests open a `PostgresCoordinationStore` on a `BootstrappedPostgres`. */
object CoordinationStores {

  /**
   * What makes a Spark application commit through `PostgresLogStore` with `settings`, as a
   * deployment sets it: the LogStore by name for `file:`, and its settings under
   * `spark.hadoop.`. The password travels as its reference, never as its value.
   */
  def sparkSettings(settings: CoordinationSettings): Map[String, String] = Map(
    "spark.delta.logStore.file.impl" -> classOf[PostgresLogStore].getName,
    s"spark.hadoop.${CoordinationSettings.JdbcUrl}" -> settings.jdbcUrl,
    s"spark.hadoop.${CoordinationSettings.User}" -> settings.user,
    s"spark.hadoop.${CoordinationSettings.PasswordRef}" -> settings.password.toString,
    s"spark.hadoop.${CoordinationSettings.ConnectTimeoutMs}" ->
      settings.connectTimeout.toMillis.toString,
    s"spark.hadoop.${CoordinationSettings.StatementTimeoutMs}" ->
      settings.statementTimeout.toMillis.toString
  )

  /**
   * Settings for a runtime role, the commit path's unless another is named, with its password
   * in a file, the way a deployment mounts a secret. The file is deleted when the JVM exits.
   */
  def settings(
      jdbcUrl: String,
      pg: BootstrappedPostgres,
      poolMaxSize: Int = CoordinationSettings.DefaultPoolMaxSize,
      connectTimeout: FiniteDuration = CoordinationSettings.DefaultConnectTimeout,
      statementTimeout: FiniteDuration = CoordinationSettings.DefaultStatementTimeout,
      role: String = CoordinationSchema.CommitPathRole
  ): CoordinationSettings = {
    val secret = Files.createTempFile("coordination-secret-", ".txt")
    secret.toFile.deleteOnExit()
    Files.write(secret, pg.password(role).getBytes(UTF_8))
    CoordinationSettings(
      jdbcUrl,
      role,
      SecretRef.File(secret),
      poolMaxSize,
      connectTimeout,
      statementTimeout
    )
  }
}
