package lakehouse.ingestion.core.config

import scala.concurrent.duration._

import cats.syntax.all._
import lakehouse.ingestion.core.config.Read.{Obj, Problem, Result}

/**
 * How a PostgreSQL database is reached: the databases of a profile, and the one a JDBC source
 * reads from. One shape for all of them, so that TLS and timeouts are said the same way
 * everywhere (ADR-0013 §5).
 *
 * @param user the role to connect as; a name like any other that configuration gives
 */
final case class DatabaseConnection(
    url: PostgresUrl,
    user: SqlIdentifier,
    password: SecretRef,
    tls: Tls,
    timeouts: Timeouts
)

object DatabaseConnection {

  /** Reads the fields `url`, `user`, `password`, `tls` and `timeouts` of a mapping. */
  def read(fields: Obj): Result[DatabaseConnection] =
    (
      fields.required("url")(_.text(PostgresUrl.parse)),
      fields.required("user")(_.text(SqlIdentifier.parse)),
      fields.required("password")(_.secret),
      fields.required("tls")(_.obj(tls)),
      fields.orElse("timeouts", Timeouts.Default)(_.obj(timeouts))
    ).mapN(DatabaseConnection.apply)

  /** What is worth a second look in a connection that is valid; `field` is where it is written. */
  def warnings(field: String, connection: DatabaseConnection): Option[Problem] =
    Option.when(connection.tls == Tls.Disabled && !connection.url.isLoopback)(
      Problem(
        s"$field.tls.mode",
        "is disable, and the database is on another machine: its traffic, the password " +
          "included, is not encrypted"
      )
    )

  private def tls(fields: Obj): Result[Tls] = {
    val certificate = fields.optional("root_certificate")(_.text(absolutePath))
    val mode = fields.required("mode")(_.oneOf("disable" -> false, "verify-full" -> true))
    (mode, certificate).tupled.andThen {
      case (true, Some(path)) => Read.valid(Tls.VerifyFull(path))
      case (true, None) =>
        Read.invalid(
          fields.path("root_certificate"),
          "is missing: verify-full needs the certificate of the authority that signed the server's"
        )
      case (false, Some(_)) =>
        Read.invalid(
          fields.path("root_certificate"),
          "is set, and mode is disable: nothing would be verified with it"
        )
      case (false, None) => Read.valid(Tls.Disabled)
    }
  }

  private def timeouts(fields: Obj): Result[Timeouts] =
    (
      // The connection pool takes no shorter wait; a longer one is a hang, not a timeout.
      fields.orElse("connect", Timeouts.Default.connect)(_.duration(250.millis, 5.minutes)),
      fields.orElse("statement", Timeouts.Default.statement)(_.duration(1.second, 1.hour))
    ).mapN(Timeouts.apply)

  private def absolutePath(text: String): Either[String, String] =
    if (!text.startsWith("/")) Left("must be an absolute path")
    else RelativePath.parse(text.drop(1)).map(_ => text)
}

/** TLS to a database (ADR-0013 §5). Two modes, and none in between that encrypts and trusts anyone. */
sealed trait Tls

object Tls {

  /** No TLS: for a database on this machine. */
  case object Disabled extends Tls

  /** `sslmode=verify-full`: the server proves it is the host that was named, to this root. */
  final case class VerifyFull(rootCertificate: String) extends Tls
}

/**
 * @param connect how long a connection is waited for. It is how long a call takes to fail when
 *   the database is down
 * @param statement how long one statement may run
 */
final case class Timeouts(connect: FiniteDuration, statement: FiniteDuration)

object Timeouts {

  /** The LogStore's own defaults (`CoordinationSettings`); a test holds the two to each other. */
  val Default: Timeouts = Timeouts(connect = 5.seconds, statement = 10.seconds)
}
