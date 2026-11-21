package lakehouse.ingestion.engine.jdbc

import java.sql.{Connection, Driver, SQLException}
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import lakehouse.ingestion.core.config.{SecretRef, SecretResolver}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.jdbc.JdbcConnectionProvider

/**
 * How Spark opens a connection to a source: with the password read where the configuration
 * points at it, when the connection is opened, so that Spark is given the reference and never
 * the password.
 *
 * Spark's own provider of a connection (`BasicConnectionProvider`, Spark 4.1) logs at `DEBUG`
 * every property of a connection that it opens, the password among them; and an option of a
 * read is held by its plan. With this provider the options of a read have where the password is
 * (`env:NAME` or `file:/path`) and no password, and the provider reads it as it connects.
 *
 * '''The one file that knows how Spark opens a connection.''' `JdbcConnectionProvider` is a
 * developer API of Spark's, found by `ServiceLoader`
 * (`META-INF/services/org.apache.spark.sql.jdbc.JdbcConnectionProvider`) and chosen by the
 * option `connectionProvider`; `JDBCOptions` is not Spark's API at all, and is what Spark's
 * own provider makes the properties of a connection with. What the engine stands on of both is
 * pinned by `JdbcConnectionProviderContractSpec`: a Spark that changes them fails there.
 *
 * '''Where the password is read from.''' Spark makes the provider itself, with no argument, so
 * what reads a secret is named by an option: the `SecretResolver` that a connector was given,
 * registered in this JVM (`register`). A JVM that does not have it, as an executor of a cluster
 * would not, reads the reference from its own environment and files: each executor is then to
 * be given the secret (discovery §20, E1).
 *
 * A read whose options carry a password is refused: a password goes by this provider, or by
 * nothing.
 */
final class SecretRefConnectionProvider extends JdbcConnectionProvider {
  import SecretRefConnectionProvider._

  override val name: String = Name

  override def canHandle(driver: Driver, options: Map[String, String]): Boolean =
    options.contains(PasswordRef)

  override def getConnection(driver: Driver, options: Map[String, String]): Connection = {
    if (options.contains("password"))
      throw new SQLException(
        "a read of a source was given its password as an option; it is to be given where the " +
          "password is, and nothing else",
        "28000"
      )
    val reference = SecretRef
      .parse(options(PasswordRef))
      .fold(
        problem => throw new SQLException(s"the reference to the password $problem", "28000"),
        identity
      )
    val resolver =
      options.get(ResolverKey).flatMap(id => Option(resolvers.get(id))).getOrElse(SecretResolver())
    val password = resolver
      .resolve(reference)
      .fold(
        problem =>
          throw new SQLException(
            s"the password at $reference could not be read: $problem",
            "28000"
          ),
        identity
      )
    val jdbc = new JDBCOptions(options)
    val properties = jdbc.asConnectionProperties
    properties.remove(PasswordRef)
    properties.remove(ResolverKey)
    properties.setProperty("password", password.reveal)
    driver.connect(jdbc.url, properties)
  }

  override def modifiesSecurityContext(driver: Driver, options: Map[String, String]): Boolean =
    false
}

object SecretRefConnectionProvider {

  /** The name that a read chooses this provider by. */
  val Name = "lakehouse-secret-ref"

  // In lower case, so that they are found whatever the case of the names a provider is given.
  private val PasswordRef = "lakehouse.passwordref"
  private val ResolverKey = "lakehouse.secretresolver"

  private val resolvers = new ConcurrentHashMap[String, SecretResolver]

  /** A resolver, for the reads that `options` names it in; what names it, in this JVM. */
  def register(secrets: SecretResolver): String = {
    val id = UUID.randomUUID().toString
    resolvers.put(id, secrets)
    id
  }

  /**
   * The options of a read that has its connections opened by this provider.
   *
   * @param password where the password of the role is
   * @param resolver what reads it, as `register` named it
   */
  def options(password: SecretRef, resolver: String): Map[String, String] =
    Map(
      "connectionProvider" -> Name,
      PasswordRef -> password.toString,
      ResolverKey -> resolver
    )
}
