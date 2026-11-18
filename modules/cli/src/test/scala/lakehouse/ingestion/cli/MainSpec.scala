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

  private def run(args: String*): Ran = runWith(Map.empty)(args: _*)

  /** With an environment of the test's own: no command of a test reads that of the build. */
  private def runWith(environment: Map[String, String])(args: String*): Ran = {
    val (out, err) = (new ByteArrayOutputStream, new ByteArrayOutputStream)
    val exit = Main.run(
      args,
      new PrintStream(out, true, "UTF-8"),
      new PrintStream(err, true, "UTF-8"),
      environment.get
    )
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

  test("validate: what the README and the guide show for the examples is what the command prints") {
    // The documentation shows the output for the examples of the repository. If the examples
    // change, or the way a configuration is hashed, this fails and says what to show instead.
    val printed = run("validate", "--profile", exampleProfile, "--sources", exampleSources).out
      .replace(s"$repository/", "")
      .trim

    Seq("README.md", "docs/configuration/adding-a-source.md").foreach { document =>
      withClue(s"$document should show:\n$printed\n") {
        example(document) should include(s"```text\n$printed\n```")
      }
    }
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

    ran.exit shouldBe 4
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

  test("the exit codes are those of the contract with the orchestrator, one meaning each") {
    // Discovery §14.1. The numbers are what an orchestrator is configured with: they are
    // pinned here by their value, and not by the name that the code calls them.
    Main.Exit.All shouldBe Seq(
      0 -> "ok",
      2 -> "skipped: locked",
      3 -> "transient failure",
      4 -> "permanent failure",
      5 -> "data threshold exceeded",
      64 -> "usage error",
      70 -> "internal error"
    )
    Main.Exit.All.map(_._1).distinct should have size Main.Exit.All.size.toLong
    // A command line that is wrong must not look like a lease that is taken, which an
    // orchestrator answers by trying again at its next trigger.
    Main.Exit.Usage should not be Main.Exit.SkippedLocked
    (Main.Exit.Ok, Main.Exit.SkippedLocked, Main.Exit.Transient) shouldBe ((0, 2, 3))
    (Main.Exit.Permanent, Main.Exit.DataThreshold, Main.Exit.Usage) shouldBe ((4, 5, 64))
    // Decided for P8, and here already so that nothing else takes the number.
    Main.Exit.Internal shouldBe 70
  }

  test("the usage says which code is which") {
    Main.Usage should include("exit codes: 0 the configuration is valid, or the schemas are at")
    Main.Usage should include("3 a database could not be reached, and running again may do;")
    Main.Usage should include("4 the configuration is not valid, or a database refused;")
    Main.Usage should include("64 the command line is not one")
  }

  test("a command line that is not one ends in 64, with what is wrong and how it is written") {
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
        ran.exit shouldBe 64
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

  // What `migrate` does to a database is tested against one (MigrateCommandSpec, in the
  // integration tests). Here is what it refuses before it opens a connection.

  test("migrate: a command line that is not one ends in 64, before anything is read") {
    val wrong = Seq(
      Seq("migrate") -> "error: --password and --profile must be given",
      Seq("migrate", "--profile", exampleProfile) -> "error: --password must be given",
      Seq("migrate", "--password", "env:P") -> "error: --profile must be given",
      Seq("migrate", "--profile", exampleProfile, "--password", "env:P", "--sources", "x") ->
        "error: '--sources' is not an option of this command",
      Seq("migrate", "--profile", exampleProfile, "--password", "env:P", "--user") ->
        "error: --user has no value after it",
      Seq("migrate", "--profile", exampleProfile, "--password", "env:") ->
        "error: --password has env: without the name of an environment variable after it"
    )

    wrong.foreach { case (args, problem) =>
      withClue(args.mkString(" ")) {
        val ran = run(args: _*)
        ran.exit shouldBe Main.Exit.Usage
        ran.errors.head shouldBe problem
        ran.out shouldBe empty
      }
    }
  }

  test("migrate: a password that is written out is refused, and is not said again") {
    val ran = run("migrate", "--profile", exampleProfile, "--password", "s3cr3t-Pa55word")

    ran.exit shouldBe Main.Exit.Usage
    ran.errors.head shouldBe
      "error: --password is not a reference: write env:NAME or file:/absolute/path, never the secret"
    ran.err should not include "s3cr3t"
    ran.out shouldBe empty
  }

  test("migrate: a role that is not the name of one is refused, and is not said again") {
    val ran =
      run("migrate", "--profile", exampleProfile, "--password", "env:P", "--user", "x; DROP ROLE y")

    ran.exit shouldBe Main.Exit.Usage
    ran.errors.head should startWith("error: --user ")
    ran.errors.head should not include "DROP"
  }

  test("migrate: a profile that is not valid ends in 4, and no connection is opened") {
    val missing =
      run("migrate", "--profile", work.resolve("none.yaml").toString, "--password", "env:P")
    missing.exit shouldBe Main.Exit.Permanent
    missing.errors.head should endWith("none.yaml: does not exist")
    missing.errors.last shouldBe "The profile is not valid, and nothing was migrated."

    val profile = write(
      work.resolve("migrate/profile.yaml"),
      example("conf/app/local.example.yaml").replace("tls: { mode: disable }", "tls: { mode: off }")
    )
    val wrong = run("migrate", "--profile", profile.toString, "--password", "env:P")
    wrong.exit shouldBe Main.Exit.Permanent
    wrong.errors.count(_.contains(".tls.mode")) shouldBe 2
    wrong.errors.last shouldBe "The profile is not valid, and nothing was migrated."
    wrong.out shouldBe empty
  }

  test("migrate: a password that is not where the command was told ends in 4") {
    val unset = run("migrate", "--profile", exampleProfile, "--password", "env:MIGRATOR_PASSWORD")
    unset.exit shouldBe Main.Exit.Permanent
    unset.errors.last shouldBe
      "error: --password cannot read the secret env:MIGRATOR_PASSWORD: the variable is not set"

    val noFile = run("migrate", "--profile", exampleProfile, "--password", "file:/run/secrets/none")
    noFile.exit shouldBe Main.Exit.Permanent
    noFile.errors.last should startWith(
      "error: --password cannot read the secret file:/run/secrets/none: "
    )
    noFile.out shouldBe empty
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
