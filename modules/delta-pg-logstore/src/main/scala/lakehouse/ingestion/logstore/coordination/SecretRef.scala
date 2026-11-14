package lakehouse.ingestion.logstore.coordination

import java.io.IOException
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}

/**
 * Where a secret is, never the secret itself: `env:NAME` or `file:/absolute/path`, the syntax of
 * ADR-0007.
 *
 * A reference can travel in a configuration that Spark copies to executors and shows in its UI.
 * Each JVM resolves it for itself, when it opens its connection pool (ADR-0004 §3.9).
 */
sealed trait SecretRef {

  /**
   * Reads the secret in this JVM.
   *
   * @param environment how a variable is looked up; the process environment unless a test says
   *   otherwise
   * @throws java.io.IOException if the secret cannot be read. The message names the reference
   *   and never contains the secret.
   */
  @throws[IOException]
  def resolve(environment: String => Option[String] = sys.env.get): String
}

object SecretRef {

  /** The value of an environment variable of the JVM that resolves it. */
  final case class Env(name: String) extends SecretRef {
    override def resolve(environment: String => Option[String]): String =
      environment(name).filter(_.nonEmpty).getOrElse {
        throw new IOException(s"cannot read the secret $this: the variable is not set")
      }

    override def toString: String = s"env:$name"
  }

  /** The content of a file, such as a mounted Docker secret, without its final line break. */
  final case class File(path: Path) extends SecretRef {
    override def resolve(environment: String => Option[String]): String = {
      val content =
        try new String(Files.readAllBytes(path), UTF_8)
        catch {
          // The exception's own message is the path, which the reference already shows.
          case e: IOException =>
            throw new IOException(s"cannot read the secret $this: ${e.getClass.getSimpleName}")
        }
      val secret = content.stripSuffix("\n").stripSuffix("\r")
      if (secret.isEmpty) throw new IOException(s"cannot read the secret $this: the file is empty")
      secret
    }

    override def toString: String = s"file:$path"
  }

  private val EnvName = """[A-Za-z_][A-Za-z0-9_]*""".r

  /** Parses a reference; `Left` says what is wrong, without repeating a value that may be a secret. */
  def parse(text: String): Either[String, SecretRef] = text.split(":", 2) match {
    case Array("env", name) if EnvName.matches(name) => Right(Env(name))
    case Array("env", _) => Left("env: must be followed by the name of an environment variable")
    case Array("file", path) if path.startsWith("/") => Right(File(Paths.get(path)))
    case Array("file", _) => Left("file: must be followed by an absolute path")
    case _ => Left("must be a reference, env:NAME or file:/absolute/path, not the secret itself")
  }
}
