package lakehouse.ingestion.core.config

import java.nio.charset.StandardCharsets.{ISO_8859_1, UTF_8}
import java.nio.file.{Files, Path, Paths}

import scala.concurrent.duration._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, EitherValues}

/** A configuration read from its files: what is a source, and what is said of what is not. */
final class ConfigLoaderSpec
    extends AnyFunSuite
    with Matchers
    with EitherValues
    with BeforeAndAfterAll {

  private val work: Path = Files.createTempDirectory("config-loader-spec")

  override def afterAll(): Unit = {
    def delete(path: Path): Unit = {
      if (Files.isDirectory(path))
        Option(path.toFile.listFiles()).toSeq.flatten.foreach(f => delete(f.toPath))
      Files.deleteIfExists(path): Unit
    }
    delete(work)
  }

  private val corpus: Path =
    Paths.get(getClass.getResource("/configurations/profile.yaml").toURI).getParent
  private val ProfileText: String =
    new String(Files.readAllBytes(corpus.resolve("profile.yaml")), UTF_8)

  private def source(id: String): String =
    s"id: $id\nconnector: { type: file, path: ${id.replace('.', '/')}, format: csv }\n" +
      s"load: { mode: new_files }\ntargets:\n  bronze: { table: bronze/${id.replace('.', '/')} }\n"

  /** A configuration in a directory of its own: a profile, and the files of its sources. */
  private def configuration(name: String)(files: (String, String)*): (Path, Path) = {
    val directory = Files.createDirectory(work.resolve(name))
    val sources = Files.createDirectory(directory.resolve("sources"))
    files.foreach { case (file, text) => Files.write(sources.resolve(file), text.getBytes(UTF_8)) }
    (Files.write(directory.resolve("profile.yaml"), ProfileText.getBytes(UTF_8)), sources)
  }

  private def errorsOf(profile: Path, sources: Path): Seq[String] =
    ConfigLoader.load(profile, sources).left.value.toChain.toVector.map(_.message)

  test("a profile and the .yaml files of a directory are a configuration") {
    val (profile, sources) = configuration("valid")(
      "shop.returns.yaml" -> source("shop.returns"),
      "shop.orders.yaml" -> source("shop.orders")
    )

    val loaded = ConfigLoader.load(profile, sources).value
    loaded.value.sources.map(_.id.value) shouldBe Vector("shop.orders", "shop.returns")
    loaded.warnings shouldBe empty
  }

  test("the examples of the repository are read from where they are") {
    val repository = Iterator
      .iterate(Paths.get("").toAbsolutePath)(_.getParent)
      .takeWhile(_ != null)
      .find(dir => Files.isDirectory(dir.resolve("conf/sources")))
      .getOrElse(fail("conf/sources was not found above the working directory"))

    val loaded = ConfigLoader.load(
      repository.resolve("conf/app/local.example.yaml"),
      repository.resolve("conf/sources")
    )

    loaded.value.value.sources.map(_.id.value) shouldBe Vector("sales.orders", "sales.returns")
  }

  test("what is not configuration is let be, and a directory without sources is valid") {
    val (profile, sources) = configuration("not-configuration")(
      "README.md" -> "How the sources of this team are named.\n",
      ".gitkeep" -> "",
      ".hidden.yaml" -> "not: even looked at",
      "notes.txt" -> "id: not.a.source\n"
    )

    ConfigLoader.load(profile, sources).value.value.sources shouldBe empty
  }

  test("nothing that could be a source is passed over in silence") {
    val (profile, sources) = configuration("passed-over")(
      "shop.orders.yaml" -> source("shop.orders"),
      "shop.returns.yml" -> source("shop.returns")
    )
    Files.createDirectory(sources.resolve("archive"))
    Files.write(
      sources.resolve("archive").resolve("shop.old.yaml"),
      source("shop.old").getBytes(UTF_8)
    )

    errorsOf(profile, sources) shouldBe Seq(
      s"${sources.resolve("archive")}: is a directory: sources are the .yaml files of this directory, and none is looked for below it",
      s"${sources.resolve("shop.returns.yml")}: is named .yml: a source is a .yaml file, so that none is left out for its name"
    )
  }

  test("a file that cannot be read is said with what is wrong in those that can") {
    val (profile, sources) = configuration("unread")(
      "shop.orders.yaml" -> source("shop.orders").replace("format: csv", "format: xls"),
      "shop.returns.yaml" -> source("shop.returns")
    )
    // Latin-1, as an editor of another time would save it.
    Files.write(sources.resolve("shop.cafes.yaml"), "id: shop.cafés\n".getBytes(ISO_8859_1))
    Files.write(sources.resolve("shop.huge.yaml"), new Array[Byte](ConfigLoader.MaxBytes.toInt + 1))
    val missing = profile.resolveSibling("no-such-profile.yaml")

    errorsOf(missing, sources) shouldBe Seq(
      s"$missing: does not exist",
      s"${sources.resolve("shop.cafes.yaml")}: is not UTF-8 text",
      s"${sources.resolve("shop.huge.yaml")}: is 1048577 bytes, and a file of configuration is 1048576 at most",
      s"${sources.resolve("shop.orders.yaml")}: connector.format must be one of csv, json, parquet"
    )
  }

  test("a profile that is a directory, and sources that are a file, are errors of their own") {
    val (profile, sources) = configuration("swapped")("shop.orders.yaml" -> source("shop.orders"))

    errorsOf(sources, profile) shouldBe Seq(
      s"$sources: is not a file",
      s"$profile: is not a directory: it is where the sources are, one file for each"
    )
    errorsOf(profile, sources.resolve("nowhere")).head should include("is not a directory")
  }

  test("the rules across sources apply to what the files say, by the names of the files") {
    val (profile, sources) = configuration("across")(
      "shop.orders.yaml" -> source("shop.orders"),
      "copy.yaml" -> source("shop.orders")
    )

    errorsOf(profile, sources) shouldBe Seq(
      s"${sources.resolve("copy.yaml")}: id is shop.orders, and a source is in the file of its name, shop.orders.yaml",
      s"${sources.resolve("copy.yaml")}: id is shop.orders, which is the id of ${sources.resolve("shop.orders.yaml")} as well",
      s"${sources.resolve("shop.orders.yaml")}: id is shop.orders, which is the id of ${sources.resolve("copy.yaml")} as well"
    )
  }

  test("a configuration is read without a connection, whatever it names is down") {
    val (profile, sources) =
      configuration("unreachable")("shop.orders.yaml" -> source("shop.orders"))
    Files.write(
      profile,
      ProfileText.replace("localhost:5432", "db.unreachable.invalid:5432").getBytes(UTF_8)
    )
    ConfigLoader.load(profile, sources).isRight shouldBe true

    val started = System.nanoTime()
    (1 to 10).foreach(_ => ConfigLoader.load(profile, sources).isRight shouldBe true)

    ((System.nanoTime() - started).nanos / 10) should be < 200.millis
  }
}
