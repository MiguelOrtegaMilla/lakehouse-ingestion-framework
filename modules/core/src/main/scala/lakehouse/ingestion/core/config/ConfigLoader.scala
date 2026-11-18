package lakehouse.ingestion.core.config

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.charset.{CharacterCodingException, CodingErrorAction}
import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._
import scala.util.Using

import cats.data.{Chain, NonEmptyChain}

/**
 * Reads a configuration from its files: the file of a profile, and a directory with one file
 * for each source (ADR-0007).
 *
 * It reads files and nothing else. No connection is opened and no name is looked up, so a
 * configuration is validated in milliseconds, with every database it names down.
 *
 * Everything that is wrong is said at once, a file that cannot be read with what is wrong in
 * the files that can. Nothing in the directory of the sources is passed over in silence, since
 * a source that is not read is a source that is not ingested, and nobody is told:
 *
 *  - a file that ends in `.yaml` is a source;
 *  - a file that ends in `.yml` is an error: it would be one that is left out for its name;
 *  - a directory is an error: no source is looked for below the directory of the sources;
 *  - anything else, a `README.md`, a file whose name starts with a dot, is not configuration.
 */
object ConfigLoader {

  /**
   * The largest file that is read at all. `SafeYaml` takes 256 KiB of characters, which UTF-8
   * writes in at most this many bytes; a file beyond it is refused before it is in memory.
   */
  val MaxBytes: Long = 1024L * 1024L

  /**
   * @param profile the file of the profile
   * @param sources the directory of the sources
   * @param kinds the kinds of connector that the application has
   */
  def load(
      profile: Path,
      sources: Path,
      kinds: Seq[ConnectorKind] = ConnectorKind.BuiltIn
  ): Either[NonEmptyChain[ConfigError], Loaded[Configuration]] = {
    val readProfile = read(profile)
    val listed = list(sources)
    val readSources = listed.toSeq.flatten.map(_.flatMap(read))
    val unread = Chain.fromSeq(
      readProfile.left.toSeq ++ listed.left.toSeq ++ readSources.flatMap(_.left.toSeq)
    )

    Configuration.load(readProfile.toOption, readSources.flatMap(_.toSeq), unread, kinds)
  }

  /** The files of the directory that are sources, or why an entry of it is in the way. */
  private def list(directory: Path): Either[ConfigError, Seq[Either[ConfigError, Path]]] =
    if (!Files.isDirectory(directory))
      Left(
        ConfigError(
          directory.toString,
          None,
          "is not a directory: it is where the sources are, one file for each"
        )
      )
    else
      try
        Right(
          Using
            .resource(Files.list(directory))(_.iterator.asScala.toList)
            .sortBy(_.getFileName.toString)
            .filterNot(_.getFileName.toString.startsWith("."))
            .flatMap { entry =>
              val name = entry.getFileName.toString
              if (Files.isDirectory(entry))
                Some(
                  Left(
                    ConfigError(
                      entry.toString,
                      None,
                      "is a directory: sources are the .yaml files of this directory, and none is looked for below it"
                    )
                  )
                )
              else if (name.endsWith(".yaml")) Some(Right(entry))
              else if (name.endsWith(".yml"))
                Some(
                  Left(
                    ConfigError(
                      entry.toString,
                      None,
                      "is named .yml: a source is a .yaml file, so that none is left out for its name"
                    )
                  )
                )
              else None
            }
        )
      catch {
        case e: IOException =>
          Left(
            ConfigError(directory.toString, None, s"cannot be listed: ${e.getClass.getSimpleName}")
          )
      }

  private def read(file: Path): Either[ConfigError, Configuration.File] = {
    def refused(problem: String): Left[ConfigError, Configuration.File] =
      Left(ConfigError(file.toString, None, problem))

    if (!Files.exists(file)) refused("does not exist")
    else if (!Files.isRegularFile(file)) refused("is not a file")
    else
      try
        if (Files.size(file) > MaxBytes)
          refused(s"is ${Files.size(file)} bytes, and a file of configuration is $MaxBytes at most")
        else {
          // Strictly: bytes that are not UTF-8 are a mistake of the file, not characters to guess.
          val decoder = UTF_8
            .newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT)
          Right(
            Configuration.File(
              file.toString,
              decoder.decode(ByteBuffer.wrap(Files.readAllBytes(file))).toString
            )
          )
        }
      catch {
        case _: CharacterCodingException => refused("is not UTF-8 text")
        // The exception's own message is the path, which the error already has.
        case e: IOException => refused(s"cannot be read: ${e.getClass.getSimpleName}")
      }
  }
}
