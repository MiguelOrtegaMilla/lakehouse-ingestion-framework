package lakehouse.ingestion.core.config

/**
 * A root that a profile declares: where the lake is, or where files land (ADR-0011, ADR-0013
 * §2). Everything else in a configuration is a path relative to one of these.
 *
 * Two forms, and no other: `file:///absolute/path` and `s3a://bucket/prefix`. One scheme for
 * object storage, since `s3://` and `s3a://` would be two keys for one table. No user and no
 * password in it, no query, and segments of the same grammar as a relative path.
 *
 * This is the text alone. What the file system knows, a symbolic link, is resolved where the
 * root is used, by the LogStore's `TableIdentity` for a table (ADR-0011, note of 2026-11-17).
 *
 * @param bucket the bucket of an `s3a` root; `None` for a local one
 * @param segments the path under the scheme, or under the bucket
 */
final case class RootUri private (
    scheme: RootUri.Scheme,
    bucket: Option[String],
    segments: Vector[String]
) {

  /** The one way this root is written: no `/` at its end. */
  def value: String = scheme match {
    case RootUri.Scheme.File => segments.mkString("file:///", "/", "")
    case RootUri.Scheme.S3a => (bucket.toVector ++ segments).mkString("s3a://", "/", "")
  }

  def isLocal: Boolean = scheme == RootUri.Scheme.File

  /** Whether `other` is this root or a place under it. */
  def contains(other: RootUri): Boolean =
    scheme == other.scheme && bucket == other.bucket && other.segments.startsWith(segments)

  override def toString: String = value
}

object RootUri {

  sealed abstract class Scheme(val name: String)

  object Scheme {
    case object File extends Scheme("file")
    case object S3a extends Scheme("s3a")
  }

  private val Bucket = """[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]""".r

  /** `Left` says what is wrong, to follow the name of the field. */
  def parse(text: String): Either[String, RootUri] =
    if (text.startsWith("file:///")) under(text.stripPrefix("file:///")).flatMap {
      case Vector() =>
        Left("is the root of the file system: a lake or a landing zone is a directory of its own")
      case segments => Right(new RootUri(Scheme.File, None, segments))
    }
    else if (text.startsWith("s3a://")) {
      val (bucket, rest) = text.stripPrefix("s3a://").span(_ != '/')
      if (!Bucket.matches(bucket))
        Left("has no bucket name after s3a://, of lower-case letters, digits, dots and hyphens")
      else under(rest.stripPrefix("/")).map(new RootUri(Scheme.S3a, Some(bucket), _))
    } else if (text.startsWith("s3://") || text.startsWith("s3n://"))
      Left(
        "must be written with s3a://: another scheme for the same bucket would be another key for the same table"
      )
    else if (text.startsWith("file:"))
      Left("must be written as file:///absolute/path, with three slashes")
    else Left("must be file:///absolute/path or s3a://bucket/prefix")

  /** The segments of what follows the scheme, or the bucket; a `/` at the end is let go. */
  private def under(path: String): Either[String, Vector[String]] = {
    val trimmed = path.stripSuffix("/")
    if (trimmed.isEmpty) Right(Vector.empty)
    else RelativePath.parse(trimmed).map(_.segments)
  }
}

/**
 * A PostgreSQL database as a profile names it: `jdbc:postgresql://host:port/database`, and
 * nothing after it.
 *
 * No parameters. What they would set has fields of its own in the profile, the TLS mode and the
 * timeouts, and a parameter in the URL would win over those without a word. Some parameters of
 * the driver also name a class for it to load (`socketFactory`, `sslfactory`: CVE-2022-21724),
 * and a password in a URL is a password in a file. No user or password before the host either.
 */
final case class PostgresUrl private (host: String, port: Int, database: String) {

  def value: String = s"jdbc:postgresql://$host:$port/$database"

  /** Whether the host is this machine, by its name: no name is looked up. */
  def isLoopback: Boolean = host == "localhost" || host == "127.0.0.1"

  override def toString: String = value
}

object PostgresUrl {

  val DefaultPort = 5432

  private val Written =
    """jdbc:postgresql://([A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?)(?::([0-9]{1,5}))?/([A-Za-z0-9_][A-Za-z0-9_-]{0,62})""".r

  /** `Left` says what is wrong, to follow the name of the field. It does not repeat the text. */
  def parse(text: String): Either[String, PostgresUrl] = text match {
    case Written(host, port, database) =>
      Option(port).map(_.toInt).getOrElse(DefaultPort) match {
        case valid if valid >= 1 && valid <= 65535 => Right(new PostgresUrl(host, valid, database))
        case _ => Left("has a port that is not between 1 and 65535")
      }
    case _ if text.contains("?") || text.contains("&") =>
      Left(
        "has parameters: a URL is jdbc:postgresql://host:port/database and nothing more. TLS " +
          "and timeouts are fields of their own, and a password is a reference"
      )
    case _ if text.contains("@") =>
      Left(
        "has a user or a password in it: the user is a field of its own, and a password is a reference"
      )
    case _ => Left("must be jdbc:postgresql://host:port/database")
  }
}
