package lakehouse.ingestion.core.config

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}

import scala.jdk.CollectionConverters._
import scala.util.Using

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The corpus of configurations (`src/test/resources/configurations`): whole configurations, a
 * profile and its sources, each a directory.
 *
 *  - `valid/<case>` must be valid;
 *  - `invalid/<case>` must give the errors of its `errors.txt`, all of them and no other, in
 *    that order: one error on each line, as the application prints it.
 *
 * A case has its sources in `sources/`, and takes the profile of the corpus unless it has a
 * `profile.yaml` of its own. A new case is a new directory: no code.
 */
final class ConfigurationCorpusSpec extends AnyFunSuite with Matchers {

  private val corpus: Path =
    Paths.get(getClass.getResource("/configurations/profile.yaml").toURI).getParent

  private def read(file: Path): String = new String(Files.readAllBytes(file), UTF_8)

  private def directories(of: Path): Seq[Path] =
    Using
      .resource(Files.list(of))(_.iterator.asScala.filter(Files.isDirectory(_)).toList)
      .sortBy(_.toString)

  private def load(directory: Path) = {
    val own = directory.resolve("profile.yaml")
    val profile = Configuration.File(
      "profile.yaml",
      read(if (Files.exists(own)) own else corpus.resolve("profile.yaml"))
    )
    val sources = directory.resolve("sources")
    val files =
      if (!Files.isDirectory(sources)) Nil
      else
        Using
          .resource(Files.list(sources))(_.iterator.asScala.toList)
          .map(file => Configuration.File(s"sources/${file.getFileName}", read(file)))
    Configuration.load(profile, files)
  }

  private val valid = directories(corpus.resolve("valid"))
  private val invalid = directories(corpus.resolve("invalid"))

  test("the corpus has its cases") {
    valid should not be empty
    invalid should not be empty
  }

  valid.foreach { directory =>
    test(s"valid: ${directory.getFileName}") {
      load(directory).left.map(_.toChain.toVector.map(_.message)) shouldBe a[Right[_, _]]
    }
  }

  invalid.foreach { directory =>
    test(s"invalid: ${directory.getFileName}") {
      val expected = read(directory.resolve("errors.txt")).linesIterator.filter(_.nonEmpty).toVector
      val errors = load(directory).left.map(_.toChain.toVector.map(_.message))

      errors shouldBe Left(expected)
    }
  }

  test("the examples of the repository are one valid configuration") {
    val repository = Iterator
      .iterate(Paths.get("").toAbsolutePath)(_.getParent)
      .takeWhile(_ != null)
      .find(dir => Files.isDirectory(dir.resolve("conf/sources")))
      .getOrElse(fail("conf/sources was not found above the working directory"))
    def file(name: String): Configuration.File =
      Configuration.File(name, read(repository.resolve(name)))
    val sources = Using
      .resource(Files.list(repository.resolve("conf/sources")))(_.iterator.asScala.toList)
      .map(path => file(s"conf/sources/${path.getFileName}"))

    val loaded = Configuration.load(file("conf/app/local.example.yaml"), sources)

    loaded.left.map(_.toChain.toVector.map(_.message)) shouldBe a[Right[_, _]]
    loaded.foreach { configuration =>
      configuration.warnings shouldBe empty
      configuration.value.sources.map(_.id.value) shouldBe Vector("sales.orders", "sales.returns")
    }
  }

  test("no error of the corpus repeats a password that was written in a file") {
    invalid.foreach { directory =>
      load(directory).left.foreach(
        _.toChain.toVector.foreach(_.message should not include "Tr0ub4dor")
      )
    }
  }
}
