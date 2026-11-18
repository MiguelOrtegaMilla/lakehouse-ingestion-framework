package lakehouse.ingestion.cli

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The command line: what `validate` prints, and the code each way of ending gives the process. */
final class MainSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  import MainSpec.Ran

  private def run(args: String*): Ran = {
    val (out, err) = (new ByteArrayOutputStream, new ByteArrayOutputStream)
    val exit =
      Main.run(args, new PrintStream(out, true, "UTF-8"), new PrintStream(err, true, "UTF-8"))
    Ran(exit, out.toString("UTF-8"), err.toString("UTF-8"))
  }

  private val repository: Path =
    Iterator
      .iterate(Paths.get("").toAbsolutePath)(_.getParent)
      .takeWhile(_ != null)
      .find(dir => Files.isDirectory(dir.resolve("conf/sources")))
      .getOrElse(fail("conf/sources was not found above the working directory"))

  private val exampleProfile = repository.resolve("conf/app/local.example.yaml").toString
  private val exampleSources = repository.resolve("conf/sources").toString

  private val work: Path = Files.createTempDirectory("main-spec")

  override def afterAll(): Unit = {
    def delete(path: Path): Unit = {
      if (Files.isDirectory(path))
        Option(path.toFile.listFiles()).toSeq.flatten.foreach(f => delete(f.toPath))
      Files.deleteIfExists(path): Unit
    }
    delete(work)
  }

  private def write(file: Path, text: String): Path = {
    Files.createDirectories(file.getParent)
    Files.write(file, text.getBytes(UTF_8))
  }

  private def example(file: String): String =
    new String(Files.readAllBytes(repository.resolve(file)), UTF_8)

  test("validate: the examples of the repository are valid, and each source has its version") {
    val ran = run("validate", "--profile", exampleProfile, "--sources", exampleSources)

    ran.exit shouldBe Main.Exit.Ok
    ran.err shouldBe empty
    val lines = ran.out.linesIterator.toVector
    lines.head shouldBe s"The configuration is valid: $exampleProfile, and 2 sources."
    lines(1) shouldBe "The lake is file:///home/CHANGE_ME/lakehouse/lake."
    lines(2) should fullyMatch regex
      """  sales\.orders   enabled  jdbc  incremental  bronze/sales/orders, silver/sales/orders  [0-9a-f]{12}"""
    lines(3) should fullyMatch regex
      """  sales\.returns  enabled  file  new_files    bronze/sales/returns                      [0-9a-f]{12}"""
    lines should have size 4
  }

  test("validate: the options are taken in any order") {
    run(
      "validate",
      "--sources",
      exampleSources,
      "--profile",
      exampleProfile
    ).exit shouldBe Main.Exit.Ok
  }

  test(
    "validate: a configuration that is not valid ends in 4, with every error on a line of its own"
  ) {
    val profile = write(
      work.resolve("invalid/profile.yaml"),
      example("conf/app/local.example.yaml")
        .replace("deployment: single-machine", "deployment: laptop")
    )
    val sources = work.resolve("invalid/sources")
    val orders = write(
      sources.resolve("sales.orders.yaml"),
      example("conf/sources/sales.orders.yaml").replace("column: updated_at", "colum: updated_at")
    )
    write(sources.resolve("sales.returns.yaml"), example("conf/sources/sales.returns.yaml"))

    val ran = run("validate", "--profile", profile.toString, "--sources", sources.toString)

    ran.exit shouldBe Main.Exit.Permanent
    ran.out shouldBe empty
    ran.errors shouldBe Seq(
      s"$profile: lake.deployment must be one of single-machine, cluster",
      s"$orders: load.watermark.column is missing",
      s"$orders: load.watermark.colum is not a field that is known here: did you mean 'column'?",
      "3 errors in 2 files. The configuration is not valid."
    )
  }

  test("validate: files that are not there end in 4 as well, and say which") {
    val ran = run("validate", "--profile", "no/such/profile.yaml", "--sources", "no/such/sources")

    ran.exit shouldBe Main.Exit.Permanent
    ran.errors shouldBe Seq(
      "no/such/profile.yaml: does not exist",
      "no/such/sources: is not a directory: it is where the sources are, one file for each",
      "2 errors in 2 files. The configuration is not valid."
    )
  }

  test("validate: a warning is said, and the configuration is valid all the same") {
    val profile = write(
      work.resolve("warned/profile.yaml"),
      example("conf/app/local.example.yaml")
        .replace("file:///home/CHANGE_ME/lakehouse/lake", "file:///tmp/lake")
    )

    val ran = run("validate", "--profile", profile.toString, "--sources", exampleSources)

    ran.exit shouldBe Main.Exit.Ok
    ran.errors should have size 1
    ran.errors.head should startWith(s"warning: $profile: lake.uri is under /tmp")
    ran.out should include("The configuration is valid")
  }

  test("validate: one error in one file is counted as one") {
    val profile = write(work.resolve("one/profile.yaml"), "lake: [unclosed")

    run("validate", "--profile", profile.toString, "--sources", exampleSources).errors.last shouldBe
      "1 error in 1 file. The configuration is not valid."
  }

  test("a command line that is not one ends in 2, with what is wrong and how it is written") {
    val wrong = Seq(
      Seq.empty[String] -> "error: no command",
      Seq("run") -> "error: 'run' is not a command",
      Seq("validate") -> "error: --profile and --sources must be given",
      Seq("validate", "--profile", exampleProfile) -> "error: --sources must be given",
      Seq("validate", "--profile") -> "error: --profile has no value after it",
      Seq(
        "validate",
        "--profile",
        "a",
        "--profile",
        "b",
        "--sources",
        "c"
      ) -> "error: --profile is given twice",
      Seq(
        "validate",
        "--profile",
        "a",
        "--sources",
        "c",
        "--verbose"
      ) -> "error: '--verbose' is not an option of this command",
      Seq(
        "validate",
        "conf/app/local.yaml"
      ) -> "error: 'conf/app/local.yaml' is not an option of this command"
    )

    wrong.foreach { case (args, problem) =>
      withClue(args.mkString(" ")) {
        val ran = run(args: _*)
        ran.exit shouldBe Main.Exit.Usage
        ran.out shouldBe empty
        ran.errors.head shouldBe problem
        ran.err should include(
          "usage: lakehouse-ingestion validate --profile <file> --sources <directory>"
        )
      }
    }
  }

  test("what was typed is shown without what a terminal would obey") {
    val ran = run("\u001B[2Jvalidate\u0007")

    ran.exit shouldBe Main.Exit.Usage
    ran.errors.head shouldBe "error: '[2Jvalidate' is not a command"
  }

  test("help is asked for, and is not an error") {
    Seq("--help", "-h", "help").foreach { word =>
      val ran = run(word)
      ran.exit shouldBe Main.Exit.Ok
      ran.err shouldBe empty
      ran.out should startWith("usage: lakehouse-ingestion validate")
    }
  }
}

object MainSpec {

  /** What a command printed, and how it ended. */
  final private case class Ran(exit: Int, out: String, err: String) {
    def errors: Seq[String] = err.linesIterator.toSeq
  }
}
