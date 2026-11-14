package lakehouse.ingestion.logstore.coordination

import scala.concurrent.duration._

import org.apache.hadoop.conf.Configuration

/**
 * How the LogStore reaches its coordination database (ADR-0004 §3.9).
 *
 * The settings travel in the Hadoop configuration, which is the only thing Delta hands to a
 * LogStore, on the driver and in tasks. Under Spark they are set with the `spark.hadoop.` prefix
 * when the application starts.
 *
 * @param jdbcUrl a `jdbc:postgresql:` URL. TLS options (`sslmode`, `sslrootcert`) go here.
 * @param password a reference; the secret is read when the pool opens
 * @param poolMaxSize connections this JVM may hold at once
 * @param connectTimeout how long to wait for a connection, new or from the pool
 * @param statementTimeout how long one statement may take, including waiting for a lock
 */
final case class CoordinationSettings(
    jdbcUrl: String,
    user: String,
    password: SecretRef,
    poolMaxSize: Int,
    connectTimeout: FiniteDuration,
    statementTimeout: FiniteDuration
)

object CoordinationSettings {

  val JdbcUrl = "lakehouse.logstore.jdbc.url"
  val User = "lakehouse.logstore.jdbc.user"
  val PasswordRef = "lakehouse.logstore.jdbc.passwordRef"
  val PoolMaxSize = "lakehouse.logstore.pool.maxSize"
  val ConnectTimeoutMs = "lakehouse.logstore.timeout.connectMs"
  val StatementTimeoutMs = "lakehouse.logstore.timeout.statementMs"

  /**
   * A thread holds a connection for one short statement, so a JVM needs as many as the tables it
   * commits to or refreshes at the same moment (ADR-0004 §3.9).
   */
  val DefaultPoolMaxSize = 4
  val DefaultConnectTimeout: FiniteDuration = 5.seconds
  val DefaultStatementTimeout: FiniteDuration = 10.seconds

  /** The connection pool does not accept a shorter wait. */
  val MinConnectTimeout: FiniteDuration = 250.millis

  /**
   * URL parameters that the LogStore sets itself. In a JDBC URL they would win over its own
   * values: a literal password would defeat the reference, and a timeout of zero would turn an
   * unreachable database into a hang instead of a failure.
   */
  val ReservedUrlParameters: Seq[String] =
    Seq("password", "socketTimeout", "connectTimeout", "loginTimeout")

  def from(conf: Configuration): Either[Seq[String], CoordinationSettings] =
    from(key => Option(conf.get(key)))

  /**
   * Reads the settings through `lookup`.
   *
   * @return the settings, or everything that is wrong with them. No message repeats a value.
   */
  def from(lookup: String => Option[String]): Either[Seq[String], CoordinationSettings] = {
    def required(key: String): Either[String, String] =
      lookup(key).map(_.trim).filter(_.nonEmpty).toRight(s"$key is not set")

    def positive(key: String, default: Long, min: Long = 1): Either[String, Long] =
      lookup(key).map(_.trim).filter(_.nonEmpty) match {
        case None => Right(default)
        case Some(text) =>
          text.toLongOption.filter(_ >= min).toRight(s"$key must be an integer of at least $min")
      }

    val url = required(JdbcUrl).flatMap { value =>
      val parameters = value.split("[?&]").drop(1).map(_.takeWhile(_ != '='))
      val reserved = ReservedUrlParameters.filter(r => parameters.exists(_.equalsIgnoreCase(r)))
      if (!value.startsWith("jdbc:postgresql:")) Left(s"$JdbcUrl must be a jdbc:postgresql: URL")
      else if (reserved.nonEmpty)
        Left(s"$JdbcUrl must not set ${reserved.mkString(", ")}: the LogStore sets them itself")
      else Right(value)
    }
    val user = required(User)
    val password =
      required(PasswordRef).flatMap(SecretRef.parse(_).left.map(e => s"$PasswordRef $e"))
    val poolMaxSize = positive(PoolMaxSize, DefaultPoolMaxSize.toLong).map(_.toInt)
    val connect =
      positive(ConnectTimeoutMs, DefaultConnectTimeout.toMillis, min = MinConnectTimeout.toMillis)
    val statement = positive(StatementTimeoutMs, DefaultStatementTimeout.toMillis)

    val problems = Seq(url, user, password, poolMaxSize, connect, statement).collect {
      case Left(problem) => problem
    }
    if (problems.nonEmpty) Left(problems)
    else
      for {
        u <- url.left.map(Seq(_))
        usr <- user.left.map(Seq(_))
        pw <- password.left.map(Seq(_))
        size <- poolMaxSize.left.map(Seq(_))
        c <- connect.left.map(Seq(_))
        s <- statement.left.map(Seq(_))
      } yield CoordinationSettings(u, usr, pw, size, c.millis, s.millis)
  }
}
