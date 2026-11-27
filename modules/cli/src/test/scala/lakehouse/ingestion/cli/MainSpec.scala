package lakehouse.ingestion.cli

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}

import java.io.IOException
import java.sql.SQLException
import java.time.Instant
import java.util.TimeZone

import scala.concurrent.duration._

import lakehouse.ingestion.core.config.{
  ConfigLoader,
  ConnectorKind,
  Profile,
  RelativePath,
  SecretResolver,
  SourceId
}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.connector.ConnectorRegistry
import lakehouse.ingestion.engine.files.{FileSourceConnector, Landing}
import lakehouse.ingestion.logstore.coordination.CoordinationSettings
import lakehouse.ingestion.logstore.{
  FaultHooks,
  FaultPoint,
  PostgresLogStore,
  StartupGuard,
  StartupRefusedException
}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, EitherValues, OptionValues}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The command line: what `validate` prints, and the code each way of ending gives the process. */
final class MainSpec
    extends AnyFunSuite
    with Matchers
    with EitherValues
    with OptionValues
    with BeforeAndAfterAll {

  import MainSpec.Ran

  /** The usage as words: what it says, whatever lines its sentences are broken into. */
  private lazy val usage: String = Main.Usage.replaceAll("\\s+", " ")

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
    lines(1) shouldBe "The lake is file:///home/CHANGE_ME/lakehouse-data/lake."
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
        .replace("file:///home/CHANGE_ME/lakehouse-data/lake", "file:///tmp/lake")
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
    usage should include("exit codes: 0 the configuration is valid, the schemas are at")
    usage should include("2 another run holds the lease of the source")
    usage should include("3 a database could not be reached, the run lost its lease")
    usage should include("4 the configuration is not valid, a database refused")
    usage should include("5 more records were rejected than the source allows")
    usage should include("64 the command line is not one")
  }

  test("a command line that is not one ends in 64, with what is wrong and how it is written") {
    val wrong = Seq(
      Seq.empty[String] -> "error: no command",
      Seq("ingest") -> "error: 'ingest' is not a command",
      Seq("run") -> "error: --profile, --source and --sources must be given",
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

  test(
    "what nothing classified, in any command, ends in 70: its type and where, not what it said"
  ) {
    // A lookup of the environment that fails as nothing expects: the command reads the password
    // of migrate through it, before it opens a connection.
    val (out, err) = (new ByteArrayOutputStream, new ByteArrayOutputStream)
    val exit = Main.run(
      Seq("migrate", "--profile", exampleProfile, "--password", "env:MIGRATOR_PASSWORD"),
      new PrintStream(out, true, "UTF-8"),
      new PrintStream(err, true, "UTF-8"),
      _ => throw new IllegalStateException("a mistake that holds what it was handed: s3cr3t")
    )
    val errors = err.toString("UTF-8")

    exit shouldBe Main.Exit.Internal
    errors.linesIterator.toSeq.last should startWith(
      "error: nothing classified the failure, a mistake of the application: " +
        "java.lang.IllegalStateException at lakehouse.ingestion.cli.MainSpec"
    )
    errors should endWith("The log of the instance has it whole\n")
    errors should not include "s3cr3t"
    out.toString("UTF-8") shouldBe empty
    usage should include("70 a failure that nothing classified, a mistake of the application")
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

  // What `maintain-table` does is tested against a database and a lake (MaintainTableCommandSpec,
  // in the integration tests). Here is what it refuses before it opens a connection.

  private def maintain(operation: String*)(more: String*): Ran =
    run(
      Seq("maintain-table") ++ operation ++ Seq(
        "--profile",
        exampleProfile,
        "--password",
        "env:MAINTENANCE_PASSWORD"
      ) ++ more: _*
    )

  test("maintain-table: a command line that is not one ends in 64, before anything is read") {
    val table = Seq("--table", "file:/data/lake/bronze/sales_orders")
    val reason = Seq("--reason", "its log was looked at")
    val wrong = Seq(
      run("maintain-table") ->
        "error: maintain-table needs its operation first: adopt, readopt, reset, collect-temps",
      maintain()(table ++ reason: _*) ->
        "error: maintain-table needs its operation first: adopt, readopt, reset, collect-temps",
      maintain("drop")(table ++ reason: _*) ->
        "error: 'drop' is not an operation on a table: adopt, readopt, reset, collect-temps",
      maintain("readopt")(table: _*) -> "error: --reason must be given",
      maintain("readopt")(reason: _*) -> "error: --table must be given",
      maintain("readopt")(table ++ reason ++ Seq("--source", "sales.orders"): _*) ->
        "error: '--source' is not an option of this command",
      maintain("readopt")(Seq("--table", "bronze/sales_orders") ++ reason: _*) ->
        ("error: --table 'bronze/sales_orders' is not the location of a table: an absolute " +
          "URI with a path, as the error of a run names it"),
      maintain("readopt")(table ++ Seq("--reason", "  "): _*) ->
        "error: --reason says nothing: an operator's action says why it was asked for"
    )

    wrong.foreach { case (ran, problem) =>
      withClue(problem) {
        ran.exit shouldBe Main.Exit.Usage
        ran.errors.head shouldBe problem
        ran.out shouldBe empty
      }
    }
  }

  test("maintain-table: a password that is not where the command was told ends in 4") {
    val unset =
      maintain("reset")("--table", "file:/data/lake/bronze/sales_orders", "--reason", "dropped")

    unset.exit shouldBe Main.Exit.Permanent
    unset.errors.last shouldBe
      "error: --password cannot read the secret env:MAINTENANCE_PASSWORD: the variable is not set"
    unset.out shouldBe empty
  }

  test("maintain-table: a failure ends in the code of its class, as a run does") {
    MaintainTableCommand.exitOf(FailureClass.Transient) shouldBe Main.Exit.Transient
    MaintainTableCommand.exitOf(FailureClass.Conflict) shouldBe Main.Exit.Transient
    MaintainTableCommand.exitOf(FailureClass.Permanent) shouldBe Main.Exit.Permanent
    MaintainTableCommand.exitOf(FailureClass.Internal) shouldBe Main.Exit.Internal
    usage should include("maintain-table (adopt | readopt | reset | collect-temps)")
  }

  // What `run` does is tested against a database and a lake (RunCommandSpec, in the integration
  // tests). Here is what it refuses, or ends, before it opens a connection or a session.

  /** A profile whose roots are under `work`, made unless said, and whose databases are nowhere. */
  private def runProfile(name: String, makeRoots: Boolean = true): Path = {
    val lake = work.resolve(s"$name/lake")
    val landing = work.resolve(s"$name/landing")
    if (makeRoots) Seq(lake, landing).foreach(Files.createDirectories(_))
    write(
      work.resolve(s"$name/profile.yaml"),
      example("conf/app/local.example.yaml")
        .replace("file:///home/CHANGE_ME/lakehouse-data/lake", lake.toUri.toString)
        .replace("file:///home/CHANGE_ME/lakehouse-data/landing", landing.toUri.toString)
        // `.invalid` never resolves (RFC 6761): a command that connected would end in 3.
        .replace("localhost:5432", "db.unreachable.invalid:5432")
    )
  }

  /** The example sources, with what a test changes in the text of one of them. */
  private def runSources(name: String, change: (String, String => String)*): Path = {
    val directory = work.resolve(s"$name/sources")
    Seq("sales.orders", "sales.returns").foreach { id =>
      val text = change.foldLeft(example(s"conf/sources/$id.yaml")) {
        case (text, (`id`, edit)) => edit(text)
        case (text, _) => text
      }
      write(directory.resolve(s"$id.yaml"), text)
    }
    directory
  }

  /**
   * `run`, with the warnings of the profile left out of what it said: its lake is under /tmp and
   * its databases on another machine without TLS, which is warned of and valid.
   */
  private def runOf(profile: Path, sources: Path, source: String): Ran = {
    val ran =
      run("run", "--profile", profile.toString, "--sources", sources.toString, "--source", source)
    ran.copy(err = ran.errors.filterNot(_.startsWith("warning: ")).map(_ + "\n").mkString)
  }

  test("run: a command line that is not one ends in 64, before anything is read") {
    val noSource = run("run", "--profile", "p.yaml", "--sources", "conf/sources")
    noSource.exit shouldBe Main.Exit.Usage
    noSource.errors.head shouldBe "error: --source must be given"

    val notAnId =
      run("run", "--profile", "p.yaml", "--sources", "conf/sources", "--source", "Sales.Orders")
    notAnId.exit shouldBe Main.Exit.Usage
    notAnId.errors.head shouldBe "error: --source must be words of lower-case letters, digits " +
      "and underscores, each starting with a letter and joined by dots, as sales.orders"
    notAnId.out shouldBe empty
  }

  test("run: --max-batches is a number of batches from 1, given once") {
    def refused(value: String): Unit = {
      val ran = run(
        "run",
        "--profile",
        "p.yaml",
        "--sources",
        "conf/sources",
        "--source",
        "sales.orders",
        "--max-batches",
        value
      )
      ran.exit shouldBe Main.Exit.Usage
      ran.errors.head shouldBe s"error: --max-batches '$value' is not a number of batches, from 1": Unit
    }
    Seq("0", "-1", "two", "01", "+2", "2147483648").foreach(refused)
    run(
      "run",
      "--profile",
      "p.yaml",
      "--sources",
      "conf/sources",
      "--source",
      "sales.orders",
      "--max-batches",
      "1",
      "--max-batches",
      "2"
    ).errors.head shouldBe "error: --max-batches is given twice"
  }

  test("run: a configuration that is not valid ends in 4, and nothing is run") {
    val sources =
      runSources("invalid", "sales.returns" -> (_.replace("format: csv", "format: xls")))

    val ran = runOf(runProfile("invalid"), sources, "sales.returns")

    ran.exit shouldBe Main.Exit.Permanent
    ran.errors.head should include("connector.format")
    ran.errors.last shouldBe "The configuration is not valid, and nothing was run."
    ran.out shouldBe empty
  }

  test("run: a source that is not in the configuration ends in 4, and says which are") {
    val sources = runSources("unknown")

    val ran = runOf(runProfile("unknown"), sources, "sales.refunds")

    ran.exit shouldBe Main.Exit.Permanent
    ran.errors shouldBe Seq(
      s"error: there is no source sales.refunds in $sources, and nothing was run. The sources " +
        "are: sales.orders, sales.returns"
    )
  }

  test("run: a source that is disabled ends well, and nothing is opened") {
    val sources = runSources(
      "disabled",
      "sales.returns" -> (_.replace("id: sales.returns\n", "id: sales.returns\nenabled: false\n"))
    )
    // Its roots are not there and its databases are nowhere: a pause needs none of them.
    val ran = runOf(runProfile("disabled", makeRoots = false), sources, "sales.returns")

    withClue(ran.err)(ran.exit shouldBe Main.Exit.Ok)
    ran.err shouldBe empty
    ran.out.trim shouldBe "The source sales.returns is disabled (enabled: false), and was not " +
      "run: nothing was opened, read or recorded. Set enabled to true to run it again."
    SparkSession.getDefaultSession shouldBe empty
  }

  test("run: the application has a connector for every kind of source that it reads") {
    val loaded = ConfigLoader.load(runProfile("kinds"), runSources("kinds")).value.value
    val profile = loaded.profile
    val registry = RunCommand.connectors(profile, SecretResolver(_ => None))

    registry.kinds.map(_.name).toSet shouldBe ConnectorKind.BuiltIn.map(_.name).toSet
    loaded.sources.foreach(RunCommand.withoutConnector(_, registry) shouldBe None)
    // One that was put together with a kind left out says so of a source of that kind.
    val orders = loaded.sources.find(_.id.value == "sales.orders").value
    val filesOnly = ConnectorRegistry(new FileSourceConnector(new Landing(profile.landing.root)))
    RunCommand.withoutConnector(orders, filesOnly).value shouldBe
      "the source sales.orders is of kind 'jdbc', and this version of the application has no " +
      "connector for it: it has 'file'"
  }

  test("run: the JVM is set to UTC, and one that cannot be set so is refused") {
    val before = TimeZone.getDefault
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("Europe/Madrid"))
      // A JVM that does not take the zone, as one would not that has a rule against it.
      RunSession.jvmToUtc(set = _ => ()).left.value shouldBe
        "the time zone of the JVM is Europe/Madrid, and could not be set to UTC. A time with " +
        "no zone would mean one thing to the engine and another to a source, and rows would " +
        "be read twice, or not at all. Start the JVM with -Duser.timezone=UTC"
      RunSession
        .jvmToUtc(set = _ => throw new SecurityException("setDefault is not allowed"))
        .left
        .value should include("could not be set to UTC (setDefault is not allowed). A time")
      TimeZone.getDefault.getID shouldBe "Europe/Madrid"

      RunSession.jvmToUtc() shouldBe Right(())
      TimeZone.getDefault.getID shouldBe "UTC"
      java.time.ZoneId.systemDefault().getId shouldBe "UTC"
      // A JVM that is in UTC already is left as it is.
      RunSession.jvmToUtc(set = _ => ()) shouldBe Right(())
    } finally TimeZone.setDefault(before)
  }

  test("run: roots that are not there end in 4, each said, before any database") {
    val profile = runProfile("roots", makeRoots = false)
    val ran = runOf(profile, runSources("roots"), "sales.returns")

    ran.exit shouldBe Main.Exit.Permanent
    ran.errors should have size 3
    ran.errors.head should startWith(
      s"error: the root of lake.uri, ${work.resolve("roots/lake").toUri.toString.stripSuffix("/")}, is not a directory"
    )
    ran.errors(1) should startWith("error: the root of landing.root, ")
    ran.errors(1) should endWith(
      "The engine does not make it, since a root that is missing is more often a disk that is " +
        "not mounted: make it once, by hand"
    )
    ran.errors.last shouldBe "Nothing was run."

    // A file where a root is to be is not a root either.
    Files.createDirectories(work.resolve("roots/landing"))
    write(work.resolve("roots/lake"), "not a directory")
    val file = runOf(profile, runSources("roots"), "sales.returns")
    file.exit shouldBe Main.Exit.Permanent
    file.errors should have size 2
    file.errors.head should include("roots/lake, is not a directory")
  }

  test("run: each way a run ends has its code") {
    val run = RunId.generate()
    val batch =
      BatchId(TxnAppId.of(SourceId.parse("sales.returns").value, Layer.Bronze), BatchSeq.First)
    def failed(failure: FailureClass): Outcome = Outcome.Failed(run, failure, "why")

    RunCommand.exitOf(Outcome.NothingNew(run)) shouldBe 0
    RunCommand.exitOf(Outcome.Ingested(run, batch, Nil, RunCounts.Nothing)) shouldBe 0
    RunCommand.exitOf(Outcome.RolledForward(run, batch, Nil, RunCounts.Nothing)) shouldBe 0
    RunCommand.exitOf(Outcome.Skipped(run, RunId.generate(), Instant.EPOCH)) shouldBe 2
    RunCommand.exitOf(Outcome.GivenUp(run, batch, "a file changed")) shouldBe 3
    RunCommand.exitOf(failed(FailureClass.Transient)) shouldBe 3
    // A conflict that outlasted the attempts of the write (discovery §14).
    RunCommand.exitOf(failed(FailureClass.Conflict)) shouldBe 3
    RunCommand.exitOf(failed(FailureClass.Permanent)) shouldBe 4
    RunCommand.exitOf(failed(FailureClass.Data)) shouldBe 5
    RunCommand.exitOf(failed(FailureClass.Internal)) shouldBe 70
  }

  test("abandon-batch: each way an operator's action ends has its code, and the usage says it") {
    import lakehouse.ingestion.engine.operator.OperatorActions.{Outcome => Action}
    val run = RunId.generate()
    OperatorCommand.exitOf(Action.Done(run, "given up")) shouldBe 0
    OperatorCommand.exitOf(Action.Done(run, "reset", Seq("a risk"))) shouldBe 0
    OperatorCommand.exitOf(Action.Skipped(run, RunId.generate(), Instant.EPOCH)) shouldBe 2
    OperatorCommand.exitOf(Action.Failed(run, FailureClass.Transient, "why")) shouldBe 3
    OperatorCommand.exitOf(Action.Failed(run, FailureClass.Permanent, "why")) shouldBe 4
    OperatorCommand.exitOf(Action.Failed(run, FailureClass.Internal, "why")) shouldBe 70

    usage should include("lakehouse-ingestion abandon-batch --profile <file>")
    usage should include("--batch <n> --reason <text>")
    usage should include("the batch was given up or reprocessed, the state was reset,")
    usage should include("a table could not be read")
    usage should include("none is planned, another is, or its table has it")

    // The chunk of a backfill, by the id of the backfill: one out of its grammar is no id.
    val misnamed = this.run(
      "abandon-batch",
      "--profile",
      "p.yaml",
      "--sources",
      "conf/sources",
      "--source",
      "s.a",
      "--batch",
      "1",
      "--reason",
      "its plan cannot be run",
      "--backfill",
      "History"
    )
    misnamed.exit shouldBe Main.Exit.Usage
    misnamed.errors.head should startWith("error: --backfill 'History' must be lower-case")
    usage should include("--batch <n> --reason <text> [--backfill <id>]")
    usage should include("With --backfill, the chunk of that backfill that is planned, under")
    usage should include("or the backfill named is not there")
  }

  test(
    "quarantine: what is asked of it is read before anything is opened, or it is no command line"
  ) {
    def quarantine(more: String*): Ran =
      run(
        Seq("quarantine", "--profile", "p.yaml", "--sources", "conf/sources", "--source", "s.a") ++
          more: _*
      )
    def refused(more: String*): String = {
      val ran = quarantine(more: _*)
      ran.exit shouldBe Main.Exit.Usage
      ran.out shouldBe empty
      ran.errors.head
    }
    run("quarantine", "--profile", "p.yaml", "--sources", "conf/sources").errors.head shouldBe
      "error: --source must be given"
    // How many records, written as a number is: from 1 to those that are shown at most.
    Seq("0", "-1", "1001", "01", "ten", "1.5", "").foreach { records =>
      withClue(records) {
        refused("--records", records) should include("is not a number of records, from 1 to 1000")
      }
    }
    refused("--records") shouldBe "error: --records has no value after it"
    refused("--type", "broken") shouldBe
      "error: --type 'broken' is not why a record is set apart: one of unparsed, " +
      "trailing_text, duplicate_field"
    refused("--run", "the-first") should startWith("error: --run 'the-first' is not a run id")
    refused("--batch", "0") shouldBe "error: --batch '0' is not the number of a batch, from 1"
    refused("--backfill", "history") shouldBe
      "error: --backfill says the sequence of a batch: --batch must be given with it"
    refused("--batch", "1", "--backfill", "History") should
      startWith("error: --backfill 'History' must be lower-case")
    refused("--reason", "to look") shouldBe "error: '--reason' is not an option of this command"
    // What is asked rightly gets as far as the configuration, which is not there (4).
    quarantine(
      "--run",
      RunId.generate().toString,
      "--batch",
      "2",
      "--backfill",
      "history",
      "--file",
      "s_a/a.csv",
      "--type",
      "unparsed",
      "--records",
      "1000"
    ).exit shouldBe Main.Exit.Permanent

    usage should include("lakehouse-ingestion quarantine --profile <file> --sources <directory>")
    usage should include("[--records <n>]")
    usage should include("It reads, and writes nothing: it is no run")
    usage should include("to the standard output and nowhere else")
    usage should include("they may hold personal data")
    Quarantine.ErrorType.All.foreach(why => usage should include(why))
    usage should include("from 1 to 1000; each has its first")
    usage should include("or what was asked of a quarantine was shown")
    usage should include("or the quarantine was not read: the source has no such")
  }

  test("reprocess: a batch by its number and a reason, or the command line is not one: 64") {
    def reprocess(more: String*): Ran =
      run(
        Seq("reprocess", "--profile", "p.yaml", "--sources", "conf/sources", "--source", "s.a") ++
          more: _*
      )
    reprocess("--reason", "fixed").errors.head shouldBe "error: --batch must be given"
    reprocess("--batch", "1").errors.head shouldBe "error: --reason must be given"
    reprocess("--batch", "01", "--reason", "fixed").errors.head shouldBe
      "error: --batch '01' is not the number of a batch, from 1"
    reprocess("--batch", "1", "--reason", "fixed", "--keep-watermark").exit shouldBe Main.Exit.Usage

    usage should include("lakehouse-ingestion reprocess --profile <file>")
    usage should include("the batch was given up or reprocessed")
    usage should include("or the batch was not reprocessed: it was not committed")
  }

  test("reset-state: one way of the watermark, a moment as start is written, and a reason: 64") {
    def reset(more: String*): Ran =
      run(
        Seq(
          "reset-state",
          "--profile",
          "p.yaml",
          "--sources",
          "conf/sources",
          "--source",
          "sales.orders"
        ) ++ more: _*
      )
    def refused(ran: Ran, problem: String): Unit = {
      ran.exit shouldBe Main.Exit.Usage
      ran.errors.head shouldBe s"error: $problem"
      ran.out shouldBe empty: Unit
    }

    refused(
      reset("--reason", "moved"),
      "one of --forget-watermark, --keep-watermark, --watermark must be given"
    )
    refused(
      reset("--reason", "moved", "--keep-watermark", "--forget-watermark"),
      "--forget-watermark and --keep-watermark are given: one of them is"
    )
    refused(
      reset("--reason", "moved", "--keep-watermark", "--watermark", "2026-03-01T00:00:00Z"),
      "--keep-watermark and --watermark are given: one of them is"
    )
    refused(
      reset("--keep-watermark", "--keep-watermark", "--reason", "x"),
      "--keep-watermark is given twice"
    )
    refused(reset("--keep-watermark"), "--reason must be given")
    reset("--keep-watermark", "--reason", " ").errors.head should startWith(
      "error: --reason says nothing"
    )
    reset("--reason", "moved", "--watermark", "2026-03-01").exit shouldBe Main.Exit.Usage
    reset("--reason", "moved", "--watermark", "2026-03-01T00:00:00.0000001Z").errors.head should
      include("more than six digits after the second")
    refused(reset("--reason", "moved", "--watermark"), "--watermark has no value after it")

    usage should include("lakehouse-ingestion reset-state --profile <file>")
    usage should include("(--watermark <moment> | --forget-watermark")
    usage should include("the state was reset, the table was maintained, or what was asked")
    usage should include("reset: a batch is planned, the source has no state, or no such")
  }

  test(
    "backfill: an id, a range (from, to], a chunk and a reason, or the command line is not one"
  ) {
    val whole = Seq(
      "--backfill",
      "history",
      "--from",
      "2025-01-01T00:00:00Z",
      "--to",
      "2025-02-01T00:00:00Z",
      "--reason",
      "the history before go-live"
    )
    def backfill(options: Seq[String]): Ran =
      run(
        Seq("backfill", "--profile", "p.yaml", "--sources", "conf/sources", "--source", "s.a") ++
          options: _*
      )
    def without(option: String): Seq[String] = whole.patch(whole.indexOf(option), Nil, 2)
    def replaced(option: String, value: String): Seq[String] =
      whole.updated(whole.indexOf(option) + 1, value)
    def refused(ran: Ran, problem: String): Unit = {
      ran.exit shouldBe Main.Exit.Usage
      ran.errors.head shouldBe s"error: $problem"
      ran.out shouldBe empty: Unit
    }

    Seq("--backfill", "--from", "--to", "--reason").foreach { option =>
      refused(backfill(without(option)), s"$option must be given")
    }
    refused(
      backfill(replaced("--backfill", "History")),
      "--backfill 'History' must be lower-case letters, digits, '.', '_' and '-', starting with " +
        "a letter or a digit, as history-2024"
    )
    backfill(replaced("--from", "2025-01-01")).errors.head should startWith(
      "error: --from '2025-01-01' "
    )
    backfill(replaced("--to", "yesterday")).errors.head should startWith("error: --to 'yesterday' ")
    // A range with no moment in it is no range: from must be before to.
    refused(
      backfill(replaced("--to", "2024-12-01T00:00:00Z")),
      "--from and --to: from, 2025-01-01T00:00:00Z, is not before to, 2024-12-01T00:00:00Z: " +
        "the range (from, to] holds no moment"
    )
    backfill(replaced("--to", "2025-01-01T00:00:00Z")).exit shouldBe Main.Exit.Usage
    refused(
      backfill(whole ++ Seq("--chunk", "0s")),
      "--chunk '0s' is no stretch of time: a chunk is 1ms at least"
    )
    refused(
      backfill(whole ++ Seq("--chunk", "7")),
      "--chunk '7' has no unit: write it as 500ms, 30s, 10m, 2h or 7d"
    )
    refused(
      backfill(whole ++ Seq("--max-batches", "0")),
      "--max-batches '0' is not a number of batches, from 1"
    )
    val unreasoned = backfill(replaced("--reason", " "))
    unreasoned.exit shouldBe Main.Exit.Usage
    unreasoned.errors.head should startWith("error: --reason says nothing")

    usage should include("lakehouse-ingestion backfill --profile <file>")
    usage should include("--to <moment> [--chunk <duration>] --reason <text>")
    usage should include("backfill Reads the range (from, to] of the watermark column")
    usage should include("whose watermark is from exactly are not read, and those at to are")
    usage should include("and a row whose watermark is from exactly is not read")
    usage should include("the backfill ended well (chunks committed, or it is complete)")
    usage should include("2 another run holds the lease of the source, or of the backfill")
    usage should include("or the backfill was refused: its source")
  }

  test("run: a count that a run did not make is said as not known, and not as zero") {
    val run = RunId.generate()
    val source = SourceId.parse("sales.orders").value
    val batch = BatchId(TxnAppId.of(source, Layer.Bronze), BatchSeq.First)
    val table = RelativePath.parse("bronze/sales/orders").value
    def said(counts: RunCounts): String =
      RunCommand
        .report(source, Outcome.Ingested(run, batch, Seq(BatchOutput(table, Some(7L))), counts))
        ._2

    said(RunCounts(rowsRead = Some(3), rowsWritten = Some(3), rowsRejected = Some(0))) shouldBe
      s"The run $run of sales.orders committed the batch $batch: 3 rows read, 3 written, " +
      "0 rejected, into bronze/sales/orders at version 7."
    // A batch that another writer of its sequence wrote: it has rows, which this run did not
    // count, and the control plane has no count of them either.
    said(RunCounts(rowsRejected = Some(0))) shouldBe
      s"The run $run of sales.orders committed the batch $batch: an unknown number of rows " +
      "read, an unknown number written, 0 rejected, into bronze/sales/orders at version 7."
  }

  test(
    "run: a refusal of the startup guard is transient only for a database that was not reached"
  ) {
    def down(state: String): IOException =
      new IOException("coordination store: cannot read", new SQLException("no", state))
    val notReached = "the coordination database cannot be used: no"

    RunCommand.exitOfRefusal(
      new StartupRefusedException(Seq(notReached), Some(down("08001")))
    ) shouldBe Main.Exit.Transient
    RunCommand.exitOfRefusal(
      new StartupRefusedException(Seq(notReached), Some(down("28P01")))
    ) shouldBe Main.Exit.Permanent
    // Running again does not mend the other problem, whatever the database does.
    RunCommand.exitOfRefusal(
      new StartupRefusedException(
        Seq("spark.delta.logStore.file.impl is not set", notReached),
        Some(down("08001"))
      )
    ) shouldBe Main.Exit.Permanent
    RunCommand.exitOfRefusal(
      new StartupRefusedException(Seq("spark.delta.logStore.file.impl is not set"))
    ) shouldBe Main.Exit.Permanent
  }

  test("run: the session has the settings of the engine, and the profile could set none of them") {
    val profile = ConfigLoader.loadProfile(runProfile("session")).value.value
    val id = SourceId.parse("sales.returns").value
    val engine = RunSession.engine(profile, id)

    engine("spark.master") shouldBe "local[2]"
    engine("spark.app.name") shouldBe "lakehouse-ingestion sales.returns"
    engine("spark.sql.extensions") shouldBe "io.delta.sql.DeltaSparkSessionExtension"
    engine("spark.sql.catalog.spark_catalog") shouldBe
      "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    engine("spark.databricks.delta.history.metricsEnabled") shouldBe "true"
    engine("spark.sql.session.timeZone") shouldBe "UTC"
    engine("spark.sql.legacy.javaCharsets") shouldBe "true"
    engine("spark.databricks.delta.allFilesInCrc.enabled") shouldBe "false"
    engine(StartupGuard.logStoreKey("file")) shouldBe classOf[PostgresLogStore].getName
    engine(s"spark.hadoop.${CoordinationSettings.User}") shouldBe "ingest_coordination"
    engine(s"spark.hadoop.${CoordinationSettings.PasswordRef}") shouldBe
      "env:INGEST_COORDINATION_PASSWORD"
    // Every key that the engine sets is one that a profile is refused for.
    (engine.keySet - "spark.master").foreach { key =>
      withClue(key)(Profile.refusedSparkSetting(key) should not be empty)
    }
    // The fault hooks of the LogStore as well, which the engine sets only when code asks it to.
    Profile.refusedSparkSetting(
      s"spark.hadoop.${FaultHooks.delayKey(FaultPoint.AfterInsert)}"
    ) should not be empty
    // The profile's own are kept, under the engine's.
    RunSession.settings(profile, id) shouldBe (profile.spark.settings ++ engine)
    profile.spark.settings should contain("spark.sql.shuffle.partitions" -> "4")
  }

  test(
    "run: a wait of the LogStore is the engine's to give, by code alone, and refused from outside"
  ) {
    val profile = ConfigLoader.loadProfile(runProfile("waits")).value.value
    val id = SourceId.parse("sales.returns").value
    val key = s"spark.hadoop.${FaultHooks.delayKey(FaultPoint.AfterInsert)}"
    RunSession.engine(profile, id).keySet should not contain key

    val waiting = RunSession.engine(profile, id, Map(FaultPoint.AfterInsert -> 5.seconds))
    waiting(key) shouldBe "5000"
    // Given by code, it is the engine's, and the session that has it has nothing from outside.
    RunSession.foreign(waiting, waiting) shouldBe empty
    // The same setting from anywhere else is refused, as it was: the guard of a profile holds.
    RunSession.foreign(waiting, RunSession.engine(profile, id)).map(_._1) shouldBe Seq(key)
  }

  test("run: a setting from outside the profile that a profile could not set is found") {
    val engine = Map("spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension")
    val effective = engine ++ Map(
      "spark.sql.shuffle.partitions" -> "4",
      "spark.app.id" -> "local-1",
      Profile.TransactionRetentionDefault -> "interval 30 days",
      "spark.hadoop.lakehouse.logstore.fault.afterInsert.delayMs" -> "50"
    )

    RunSession.foreign(effective, engine).map(_._1) shouldBe Seq(
      "spark.databricks.delta.properties.defaults.setTransactionRetentionDuration",
      "spark.hadoop.lakehouse.logstore.fault.afterInsert.delayMs"
    )
    // A key of the engine's with another value than the engine gave it.
    RunSession.foreign(Map("spark.sql.extensions" -> "x.Other"), engine).map(_._1) shouldBe
      Seq("spark.sql.extensions")
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
