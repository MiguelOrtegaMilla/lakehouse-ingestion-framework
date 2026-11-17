package lakehouse.ingestion.core.config

import java.io.IOException
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

/**
 * Where a secret is, never the secret itself: `env:NAME` or `file:/absolute/path` (ADR-0007).
 * A configuration holds references alone, so it can be read, reviewed, stored and hashed.
 *
 * The syntax is the one the LogStore reads for its own password
 * (`lakehouse.ingestion.logstore.coordination.SecretRef`). The two modules may not depend on
 * each other (ADR-0012), so there are two types, and a test holds them to one syntax.
 */
sealed trait SecretRef

object SecretRef {

  /** The value of an environment variable of the process that resolves it. */
  final case class Env(name: String) extends SecretRef {
    override def toString: String = s"env:$name"
  }

  /** The content of a file, such as a mounted Docker secret. */
  final case class File(path: String) extends SecretRef {
    override def toString: String = s"file:$path"
  }

  private val EnvName = """[A-Za-z_][A-Za-z0-9_]*""".r
  // An absolute path, written one way: no empty segment, and none that is `.` or `..`.
  private val FilePath = """(/[A-Za-z0-9_][A-Za-z0-9_.=-]*)+""".r

  /**
   * `Left` says what is wrong, to follow the name of the field. It never repeats the text: what
   * was written where a reference belongs may be the secret.
   */
  def parse(text: String): Either[String, SecretRef] = text.split(":", 2) match {
    case Array("env", name) if EnvName.matches(name) => Right(Env(name))
    case Array("env", _) => Left("has env: without the name of an environment variable after it")
    case Array("file", path) if FilePath.matches(path) => Right(File(path))
    case Array("file", _) =>
      Left("has file: without an absolute path after it, of plain segments and with no '..'")
    case _ => Left("is not a reference: write env:NAME or file:/absolute/path, never the secret")
  }
}

/**
 * A secret that was resolved. It prints as nothing, in a log, in an error, in a test failure:
 * the value is read through `reveal`, by the one call that needs it.
 */
final class Secret(private val value: String) {

  def reveal: String = value

  override def toString: String = "Secret(***)"

  override def equals(other: Any): Boolean = other match {
    case that: Secret => that.value == value
    case _ => false
  }

  override def hashCode: Int = value.hashCode
}

/** Reads the secret that a reference points at, in this process and when it is needed. */
trait SecretResolver {

  /** `Left` names the reference and the reason, and never has the secret in it. */
  def resolve(reference: SecretRef): Either[String, Secret]
}

object SecretResolver {

  /**
   * @param environment how a variable is looked up; the environment of the process, unless a
   *   test says otherwise
   */
  def apply(environment: String => Option[String] = sys.env.get): SecretResolver = {
    case reference @ SecretRef.Env(name) =>
      environment(name)
        .filter(_.nonEmpty)
        .map(new Secret(_))
        .toRight(s"cannot read the secret $reference: the variable is not set")

    case reference @ SecretRef.File(path) =>
      // The exception's own message is the path, which the reference already shows.
      val content =
        try Right(new String(Files.readAllBytes(Paths.get(path)), UTF_8))
        catch {
          case e: IOException =>
            Left(s"cannot read the secret $reference: ${e.getClass.getSimpleName}")
        }
      // A file written by hand, or by `echo`, ends in a line break that is not the secret's.
      content.map(_.stripSuffix("\n").stripSuffix("\r")).flatMap { secret =>
        if (secret.isEmpty) Left(s"cannot read the secret $reference: the file is empty")
        else Right(new Secret(secret))
      }
  }
}
