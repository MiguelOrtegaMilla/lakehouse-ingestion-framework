package lakehouse.ingestion.engine.control

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8

import lakehouse.ingestion.core.config.{DatabaseConnection, Tls}
import lakehouse.ingestion.logstore.coordination.CoordinationSettings

/**
 * From the coordination database of a profile to the settings of the LogStore (ADR-0004 §3.9).
 *
 * The LogStore is a library that knows nothing of profiles: Delta hands it a Hadoop
 * configuration, and that is all it reads. So the connection of the profile is written there
 * for it, with the same TLS and the same timeouts as the control plane has, and the password
 * as the reference that the profile has: the LogStore reads the secret itself, in the process
 * that opens the connection, and the secret is never a setting.
 */
object LogStoreSettings {

  /**
   * The settings, by the names the LogStore reads them under. A Spark application is given
   * them with `spark.hadoop.` before each name.
   */
  def hadoop(coordination: DatabaseConnection): Map[String, String] = {
    // The LogStore takes TLS in its URL. The mode is always said: the driver's own default
    // tries TLS and goes on without it, which is neither of the two modes of a profile.
    val tls = coordination.tls match {
      case Tls.Disabled => "sslmode=disable"
      case Tls.VerifyFull(rootCertificate) =>
        s"sslmode=verify-full&sslrootcert=${URLEncoder.encode(rootCertificate, UTF_8)}"
    }
    Map(
      CoordinationSettings.JdbcUrl -> s"${coordination.url.value}?$tls",
      CoordinationSettings.User -> coordination.user.value,
      CoordinationSettings.PasswordRef -> coordination.password.toString,
      CoordinationSettings.ConnectTimeoutMs -> coordination.timeouts.connect.toMillis.toString,
      CoordinationSettings.StatementTimeoutMs -> coordination.timeouts.statement.toMillis.toString
    )
  }
}
