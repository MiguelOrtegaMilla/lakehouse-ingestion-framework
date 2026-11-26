package lakehouse.ingestion.it.cli

import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._
import scala.util.Using

import io.delta.storage.HDFSLogStore
import lakehouse.ingestion.cli.Main
import lakehouse.ingestion.core.config.Source
import lakehouse.ingestion.core.control.OperatorRequest
import lakehouse.ingestion.it.logstore.CoordinatedDeltaSuite.EmptyCommit
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.it.support.{CoordinationSchema, DeploymentFiles, LocalSparkSuite}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The command `maintain-table` from end to end, as an operator runs it: the configuration of a
 * deployment, the real coordination database reached as the role of maintenance, real tables
 * in the lake, and the exit code of each of its outcomes (discovery §14.1). A tainted table is
 * tainted by a commit that Delta's own LogStore makes, and its run names the command that mends
 * it. `TableMaintenanceSpec` has what each operation does to a table.
 */
final class MaintainTableCommandSpec
    extends AnyFunSuite
    with Matchers
    with RunCommandQueries
    with CommandLedger {

  private lazy val files = new DeploymentFiles(pg, Files.createTempDirectory("maintain-command-"))
  override protected def deployment: DeploymentFiles = files
  import files.{arguments, drop, validProfile}

  override def afterAll(): Unit =
    try files.close()
    finally super.afterAll()

  private val operator =
    OperatorRequest.of(sys.props.getOrElse("user.name", ""), "x").toOption.get.requestedBy

  /** Where the command is told the password of the role of maintenance is. */
  private lazy val maintenancePassword =
    Map("MAINTENANCE_PASSWORD" -> pg.password(CoordinationSchema.MaintenanceRole))

  private def fileSource(): Source = files.fileSource(newSourceId())

  private def run(of: Source): Ran = command(Map.empty)(arguments(of): _*)

  private def maintain(
      operation: String,
      table: String,
      reason: String = "its log was looked at, and it is right",
      profile: Path = validProfile,
      environment: Map[String, String] = maintenancePassword
  ): Ran =
    command(environment)(
      "maintain-table",
      operation,
      "--profile",
      profile.toString,
      "--password",
      "env:MAINTENANCE_PASSWORD",
      "--table",
      table,
      "--reason",
      reason
    )

  private def bronzeOf(of: Source): Path =
    files.lake.resolve(s"bronze/${of.id.value.replace('.', '/')}")

  /** The highest version in the log of a table. */
  private def versionOf(table: Path): Long =
    Using.resource(Files.list(table.resolve("_delta_log")))(
      _.iterator.asScala
        .map(_.getFileName.toString)
        .collect { case name if name.matches("\\d{20}\\.json") => name.take(20).toLong }
        .max
    )

  /** A commit made by a process with Delta's own LogStore, which coordinates nothing. */
  private def foreignCommit(table: Path, version: Long, actions: String*): Unit = {
    val conf = new Configuration()
    new HDFSLogStore(conf).write(
      new HadoopPath(table.resolve(f"_delta_log/$version%020d.json").toUri),
      actions.iterator.asJava,
      false,
      conf
    )
  }

  /** The rows of the maintenance log of the table at `uri`: operation, who, why, and a field. */
  private def loggedOf(uri: String, field: String): Seq[Seq[String]] = asSuperuser { c =>
    query(
      c,
      "SELECT operation, requested_by, reason, detail ->> ? " +
        "FROM delta_coordination.maintenance_log WHERE table_uri = ? ORDER BY entry_id",
      field,
      uri
    )
  }

  /** The key of a table, as the LogStore and the error of a run name it. */
  private def keyOf(table: Path): String = s"file:${table.toRealPath()}"

  // ---------------------------------------------------------------------------------------------

  test("a tainted table is adopted again by the command its run names, and its runs go on") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    run(orders).exit shouldBe Main.Exit.Ok

    val bronze = bronzeOf(orders)
    foreignCommit(bronze, versionOf(bronze) + 1, EmptyCommit)
    drop(orders, "b.csv", "id\n2\n")
    val refused = run(orders)
    refused.exit shouldBe Main.Exit.Permanent
    val error = said(refused).mkString("\n")
    error should include("is tainted")

    // The command is said whole, with the table: an operator copies it.
    val named = """maintain-table readopt --table (\S+) --profile""".r
      .findFirstMatchIn(error)
      .map(_.group(1))
      .getOrElse(fail(s"the error names no command: $error"))
    named shouldBe keyOf(bronze)

    val mended = maintain("readopt", named, reason = "the empty commit was an operator's test")

    withClue(mended.err)(mended.exit shouldBe Main.Exit.Ok)
    mended.out should include(s"$named was adopted again at version ${versionOf(bronze)}")
    mended.out should include("its taint was cleared: ")
    mended.out should include(
      s"recorded in the maintenance log, as asked by $operator: the empty commit was an " +
        "operator's test"
    )
    val logged = loggedOf(named, "cleared_taint")
    logged.map(_.take(3)) shouldBe
      Seq(Seq("readopt", operator, "the empty commit was an operator's test"))
    logged.head(3) should not be null

    // The batch that the refused run planned is committed by the next one.
    run(orders).exit shouldBe Main.Exit.Ok
    batchesOf(orders) shouldBe Seq("1" -> "COMMITTED", "2" -> "COMMITTED")
    assertLedger(orders)
  }

  test("a table made without the LogStore is adopted with the id of its first commit, once") {
    val elsewhere = files.lake.resolve("bronze/made_elsewhere")
    Files.createDirectories(elsewhere.resolve("_delta_log"))
    foreignCommit(
      elsewhere,
      0L,
      """{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}""",
      """{"metaData":{"id":"5f3c2a1e-made-elsewhere","format":{"provider":"parquet",""" +
        """"options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}",""" +
        """"partitionColumns":[],"configuration":{},"createdTime":1763200000000}}"""
    )
    val uri = keyOf(elsewhere)

    val adopted = maintain("adopt", uri, reason = "made by the team before go-live")

    withClue(adopted.err)(adopted.exit shouldBe Main.Exit.Ok)
    adopted.out should include(
      s"$uri was adopted at version 0, with the id 5f3c2a1e-made-elsewhere"
    )
    loggedOf(uri, "delta_table_id") shouldBe
      Seq(Seq("adopt", operator, "made by the team before go-live", "5f3c2a1e-made-elsewhere"))

    // Registered by then: refused, and nothing more is recorded.
    val again = maintain("adopt", uri)
    again.exit shouldBe Main.Exit.Permanent
    said(again).head should include("is registered already")
    said(again).head should endWith("Nothing was changed")
    loggedOf(uri, "version") should have size 1
  }

  test("the registry of a table is reset once it is dropped, and not while it has its log") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    run(orders).exit shouldBe Main.Exit.Ok
    val bronze = bronzeOf(orders)
    val uri = keyOf(bronze)

    val live = maintain("reset", uri, reason = "the table was dropped")
    live.exit shouldBe Main.Exit.Permanent
    said(live).head should include("still has a log")
    loggedOf(uri, "version") shouldBe empty

    LocalSparkSuite.deleteRecursively(bronze)
    val reset = maintain("reset", uri, reason = "the table was dropped")
    withClue(reset.err)(reset.exit shouldBe Main.Exit.Ok)
    reset.out should include(s"The registry of the table at $uri was reset")
    loggedOf(uri, "entries").map(_.take(3)) shouldBe
      Seq(Seq("reset", operator, "the table was dropped"))

    // Nothing is left to reset.
    val nothing = maintain("reset", uri, reason = "the table was dropped")
    nothing.exit shouldBe Main.Exit.Permanent
    said(nothing).head should include("not registered")
  }

  test("collect-temps removes the temporary files that nothing needs, and records each") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    run(orders).exit shouldBe Main.Exit.Ok
    val bronze = bronzeOf(orders)
    val temps = bronze.resolve("_delta_log/.tmp")
    val before = Using.resource(Files.list(temps))(_.iterator.asScala.toVector)
    before should not be empty

    val collected = maintain("collect-temps", keyOf(bronze), reason = "the weekly clean")

    withClue(collected.err)(collected.exit shouldBe Main.Exit.Ok)
    collected.out should include(s"${before.size} temporary files of the table at ")
    collected.out should include("were removed, and 0 kept")
    Using.resource(Files.list(temps))(_.iterator.asScala.toVector) shouldBe empty
    val logged = loggedOf(keyOf(bronze), "temps")
    logged.map(_.take(3)) shouldBe Seq(Seq("collect_temps", operator, "the weekly clean"))
    before.foreach(file => logged.head(3) should include(file.getFileName.toString))
    // The table is read and written as before.
    drop(orders, "b.csv", "id\n2\n")
    run(orders).exit shouldBe Main.Exit.Ok
    assertLedger(orders)
  }

  test("a table that is not in the lake is refused in 4, and nothing is recorded") {
    val outside = Files.createTempDirectory("not-in-the-lake-")
    try {
      val uri = keyOf(outside)
      val refused = maintain("reset", uri)
      refused.exit shouldBe Main.Exit.Permanent
      said(refused).head should include(s"is not in the lake: it is at $uri, and the lake is")
      loggedOf(uri, "version") shouldBe empty
    } finally LocalSparkSuite.deleteRecursively(outside)
  }

  test("a coordination database that cannot be reached ends in 3, and one that refuses in 4") {
    val dropped = files.lake.resolve("bronze/never_there")
    val unreachable = files.profileFile(coordination =
      files.connection(
        CoordinationSchema.CommitPathRole,
        pg.password(CoordinationSchema.CommitPathRole),
        url = "jdbc:postgresql://127.0.0.1:1/ingestion_control",
        connect = "1s"
      )
    )
    val away = maintain("reset", dropped.toUri.toString, profile = unreachable)
    away.exit shouldBe Main.Exit.Transient
    said(away).head should startWith("error: the coordination database, as ingest_maintenance: ")

    val wrong = maintain(
      "reset",
      dropped.toUri.toString,
      environment = Map("MAINTENANCE_PASSWORD" -> "not-its-password")
    )
    wrong.exit shouldBe Main.Exit.Permanent
    said(wrong).head should include("password authentication failed")
    wrong.err should not include "not-its-password"
  }

  test("collect-temps as a role that cannot write the log ends in 4, and removes nothing") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    run(orders).exit shouldBe Main.Exit.Ok
    val bronze = bronzeOf(orders)
    val uri = keyOf(bronze)
    val temps = bronze.resolve("_delta_log/.tmp")
    val before = Using.resource(Files.list(temps))(_.iterator.asScala.toVector.sorted)
    before should not be empty

    // The role of the commit path reads the registry, and may not write the maintenance log.
    val refused = command(
      Map("COMMIT_PASSWORD" -> pg.password(CoordinationSchema.CommitPathRole))
    )(
      "maintain-table",
      "collect-temps",
      "--profile",
      validProfile.toString,
      "--password",
      "env:COMMIT_PASSWORD",
      "--user",
      CoordinationSchema.CommitPathRole,
      "--table",
      uri,
      "--reason",
      "the weekly clean"
    )

    refused.exit shouldBe Main.Exit.Permanent
    said(refused).head should include("cannot write the maintenance log")
    said(refused).head should include("Nothing was removed")
    Using.resource(Files.list(temps))(_.iterator.asScala.toVector.sorted) shouldBe before
    loggedOf(uri, "temps") shouldBe empty
  }

  test("the command acts as the role of maintenance, and the commit path's role cannot") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    run(orders).exit shouldBe Main.Exit.Ok
    val uri = keyOf(bronzeOf(orders))

    val asCommitPath = command(
      Map("COMMIT_PASSWORD" -> pg.password(CoordinationSchema.CommitPathRole))
    )(
      "maintain-table",
      "readopt",
      "--profile",
      validProfile.toString,
      "--password",
      "env:COMMIT_PASSWORD",
      "--user",
      CoordinationSchema.CommitPathRole,
      "--table",
      uri,
      "--reason",
      "not its role"
    )

    asCommitPath.exit shouldBe Main.Exit.Permanent
    said(asCommitPath).head should include("permission denied")
    loggedOf(uri, "version") shouldBe empty
  }
}
