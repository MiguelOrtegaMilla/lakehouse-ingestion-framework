package lakehouse.ingestion.it.support

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import scala.concurrent.duration._

import lakehouse.ingestion.logstore.coordination.{CoordinationSettings, SecretRef}

/** Settings with which tests open a `PostgresCoordinationStore` on a `BootstrappedPostgres`. */
object CoordinationStores {

  /**
   * Settings for the commit path's role, with its password in a file, the way a deployment
   * mounts a secret. The file is deleted when the JVM exits.
   */
  def settings(
      jdbcUrl: String,
      pg: BootstrappedPostgres,
      poolMaxSize: Int = CoordinationSettings.DefaultPoolMaxSize,
      connectTimeout: FiniteDuration = CoordinationSettings.DefaultConnectTimeout,
      statementTimeout: FiniteDuration = CoordinationSettings.DefaultStatementTimeout
  ): CoordinationSettings = {
    val secret = Files.createTempFile("coordination-secret-", ".txt")
    secret.toFile.deleteOnExit()
    Files.write(secret, pg.password(CoordinationSchema.CommitPathRole).getBytes(UTF_8))
    CoordinationSettings(
      jdbcUrl,
      CoordinationSchema.CommitPathRole,
      SecretRef.File(secret),
      poolMaxSize,
      connectTimeout,
      statementTimeout
    )
  }
}
