package lakehouse.ingestion.core.config

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}

import scala.concurrent.duration._

import lakehouse.ingestion.core.config.Profile._
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The profile: its fields, its defaults, the rules about more than one field (ADR-0011 §2), and
 * that everything wrong in a file is said at once (ADR-0007).
 */
final class ProfileSpec extends AnyFunSuite with Matchers with EitherValues {

  private val File = "conf/app/test.yaml"

  /** The directory of the repository: the tests run from it, or from somewhere inside it. */
  private val repository: Path =
    Iterator
      .iterate(Paths.get("").toAbsolutePath)(_.getParent)
      .takeWhile(_ != null)
      .find(dir => Files.isDirectory(dir.resolve("conf/app")))
      .getOrElse(fail("conf/app was not found above the working directory"))

  private val Valid =
    """lake:
      |  uri: "file:///data/lake"
      |  deployment: single-machine
      |landing:
      |  root: "file:///data/landing"
      |databases:
      |  control:
      |    url: "jdbc:postgresql://localhost:5432/ingestion_control"
      |    user: ingest_control
      |    password: { secretRef: "env:INGEST_CONTROL_PASSWORD" }
      |    tls: { mode: disable }
      |  coordination:
      |    url: "jdbc:postgresql://localhost:5432/ingestion_control"
      |    user: ingest_coordination
      |    password: { secretRef: "file:/run/secrets/ingest_coordination_password" }
      |    tls: { mode: disable }
      |spark:
      |  master: "local[2]"
      |""".stripMargin

  /** The valid profile with one line of it written another way. */
  private def changed(line: String, to: String): String = {
    Valid should include(line)
    Valid.replace(line, to)
  }

  private def errorsOf(text: String): Seq[String] =
    Profile.load(File, text).left.value.toChain.toVector.map(_.message)

  private def warningsOf(text: String): Seq[String] =
    Profile.load(File, text).value.warnings.map(_.message)

  test("the example of a local profile is valid, with nothing to warn of") {
    val file = repository.resolve("conf/app/local.example.yaml")
    val loaded =
      Profile.load("conf/app/local.example.yaml", new String(Files.readAllBytes(file), UTF_8))

    loaded.left.map(_.toChain.toVector.map(_.message)) shouldBe a[Right[_, _]]
    val profile = loaded.value.value
    loaded.value.warnings shouldBe empty
    profile.lake.uri.value shouldBe "file:///home/CHANGE_ME/lakehouse-data/lake"
    profile.lake.deployment shouldBe Deployment.SingleMachine
    profile.databases.control.user.value shouldBe "ingest_control"
    profile.databases.coordination.password shouldBe SecretRef.Env("INGEST_COORDINATION_PASSWORD")
    profile.databases.coordination.timeouts shouldBe Timeouts(5.seconds, 10.seconds)
    profile.retries shouldBe Retries(2.minutes, 1.second, 30.seconds)
    profile.spark.master shouldBe Master.Local("local[2]")
    // Settings are passed on as text, whatever YAML read them as.
    profile.spark.settings.toMap shouldBe
      Map("spark.sql.shuffle.partitions" -> "4", "spark.ui.enabled" -> "false")
  }

  test("what a profile leaves out has the defaults, which are the LogStore's own") {
    val profile = Profile.load(File, Valid).value.value

    profile.lake.sharedFilesystem shouldBe false
    profile.databases.control.timeouts shouldBe Timeouts(
      connect = 5.seconds,
      statement = 10.seconds
    )
    profile.retries shouldBe Retries(
      budget = 2.minutes,
      firstBackoff = 1.second,
      maxBackoff = 30.seconds
    )
    profile.spark.settings shouldBe empty
    profile.databases.control.tls shouldBe Tls.Disabled
    // ADR-0006: two minutes, renewed every forty seconds.
    profile.leases shouldBe Profile.Leases(ttl = 2.minutes)
  }

  test("everything wrong in a file is said at once, each with its field") {
    val text =
      """lake:
        |  uri: "s3://lake/"
        |  deployement: cluster
        |landing:
        |  root: "/data/landing"
        |databases:
        |  control:
        |    url: "jdbc:postgresql://localhost:5432/control?sslmode=disable"
        |    user: "ingest control"
        |    password: hunter2
        |    tls: { mode: require }
        |  coordination:
        |    url: "jdbc:postgresql://localhost:5432/control"
        |    user: ingest_coordination
        |    password: { secretRef: "hunter2" }
        |    tls: { mode: verify-full }
        |    timeouts: { connect: 5, statement: 2h }
        |retries: { budget: 0s }
        |spark:
        |  master: yarn
        |  settings:
        |    spark.hadoop.fs.s3a.secret.key: abc
        |    shuffle.partitions: 4
        |pool: 4
        |""".stripMargin

    errorsOf(text) shouldBe Seq(
      s"$File: lake.uri must be written with s3a://: another scheme for the same bucket would be another key for the same table",
      s"$File: lake.deployment is missing",
      s"$File: lake.deployement is not a field that is known here: did you mean 'deployment'?",
      s"$File: landing.root must be file:///absolute/path or s3a://bucket/prefix",
      s"$File: databases.control.url has parameters: a URL is jdbc:postgresql://host:port/database and nothing more. TLS and timeouts are fields of their own, and a password is a reference",
      s"$File: databases.control.user has U+0020 SPACE at position 7: a name is letters of ASCII, digits and underscores",
      s"""$File: databases.control.password must be a reference to a secret, as { secretRef: "env:NAME" } or { secretRef: "file:/run/secrets/name" }, and never the secret itself""",
      s"$File: databases.control.tls.mode must be one of disable, verify-full",
      s"$File: databases.coordination.password.secretRef is not a reference: write env:NAME or file:/absolute/path, never the secret",
      s"$File: databases.coordination.tls.root_certificate is missing: verify-full needs the certificate of the authority that signed the server's",
      s"$File: databases.coordination.timeouts.connect has no unit: write it as 500ms, 30s, 10m, 2h or 7d",
      s"$File: databases.coordination.timeouts.statement is 2h, and the longest it may be is 1h",
      s"$File: retries.budget is 0s, and the shortest it may be is 1s",
      s"$File: spark.master must be local, local[N], local[*] or spark://host:port",
      s"$File: spark.settings.spark.hadoop.fs.s3a.secret.key is a credential, and no credential is written in a profile: object storage takes its own from the environment or from a Hadoop credential provider (ADR-0011)",
      s"$File: spark.settings.shuffle.partitions is not a Spark setting: its key must start with spark.",
      s"$File: pool is not a field that is known here"
    )
    // A password that was written where a reference belongs is in no error.
    errorsOf(text).foreach(_ should not include "hunter2")
  }

  test("a file that is not YAML, or not a mapping, is one error of the file") {
    errorsOf("lake: [unclosed") should have size 1
    errorsOf("lake: [unclosed").head should startWith(s"$File: while parsing a flow sequence")
    errorsOf("- a\n- b\n") shouldBe Seq(s"$File: must be a mapping of fields, and is a list")
    errorsOf("") shouldBe Seq(s"$File: the file has no document")
    errorsOf("lake: 1\nlake: 2\n") shouldBe
      Seq(s"$File: the key 'lake' appears twice in one mapping (line 2, column 1)")
  }

  test("a lake on a local disk is for one machine, unless every machine mounts it") {
    val cluster = changed("deployment: single-machine", "deployment: cluster")
    errorsOf(cluster) shouldBe Seq(
      s"$File: lake.uri is on a local disk, and lake.deployment is cluster: processes on other " +
        "machines would each write to a disk of their own. Use shared storage, or set " +
        "lake.shared_filesystem to true if every machine mounts this path"
    )

    val mounted =
      cluster.replace("deployment: cluster", "deployment: cluster\n  shared_filesystem: true")
    warningsOf(mounted) shouldBe Seq(
      s"$File: lake.shared_filesystem is true: the lake is a path that every machine mounts. " +
        "Renames and listings on such storage are not those of a local disk, and need a " +
        "verification of their own"
    )

    val pointless =
      changed("deployment: single-machine", "deployment: single-machine\n  shared_filesystem: true")
    errorsOf(pointless) shouldBe Seq(
      s"$File: lake.shared_filesystem is true, and it only says something of a file:// lake in a cluster deployment"
    )
  }

  test("a lake under /tmp is warned of: the registry would outlive it") {
    warningsOf(Valid.replace("file:///data/lake", "file:///tmp/lakehouse/lake")) shouldBe Seq(
      s"$File: lake.uri is under /tmp, which is emptied when the machine restarts. The " +
        "coordination database would still have the commits of the lake's tables, and the " +
        "LogStore would find them missing and refuse to write"
    )
    // Only the lake: a landing zone that is emptied loses files that were yet to be read.
    warningsOf(Valid.replace("file:///data/landing", "file:///tmp/landing")) shouldBe empty
    warningsOf(Valid.replace("file:///data/lake", "file:///data/tmp/lake")) shouldBe empty
  }

  test("a cluster of Spark with a single-machine lake on a local disk is refused") {
    errorsOf(changed("master: \"local[2]\"", "master: \"spark://master:7077\"")) shouldBe Seq(
      s"$File: spark.master is a cluster, and lake.deployment is single-machine with a lake on " +
        "a local disk: every worker would write data files to its own disk"
    )
    // A local master in a cluster deployment is how tests run: allowed.
    val objectStorage = Valid
      .replace("file:///data/lake", "s3a://lake/")
      .replace("deployment: single-machine", "deployment: cluster")
    Profile.load(File, objectStorage).isRight shouldBe true
    Profile
      .load(File, objectStorage.replace("local[2]", "spark://master:7077"))
      .isRight shouldBe true
  }

  test("the landing zone and the lake are apart") {
    val apart =
      s"$File: landing.root is the lake, or inside it, or around it: the two must be apart"

    errorsOf(changed("root: \"file:///data/landing\"", "root: \"file:///data/lake\"")) shouldBe Seq(
      apart
    )
    errorsOf(
      changed("root: \"file:///data/landing\"", "root: \"file:///data/lake/landing\"")
    ) shouldBe Seq(apart)
    errorsOf(changed("root: \"file:///data/landing\"", "root: \"file:///data\"")) shouldBe Seq(
      apart
    )
    Profile
      .load(File, changed("root: \"file:///data/landing\"", "root: \"file:///data/lake2\""))
      .isRight shouldBe true
  }

  test("the two databases have a role each") {
    errorsOf(changed("user: ingest_coordination", "user: ingest_control")) shouldBe Seq(
      s"$File: databases.coordination.user is the role of databases.control as well: each has " +
        "the privileges of its own work and no more"
    )
  }

  test("the retry budget must be longer than the LogStore itself waits for its database") {
    // Three attempts of five seconds: a batch without the database fails after that (P2b.5).
    errorsOf(Valid + "retries: { budget: 15s, max_backoff: 10s }\n") shouldBe Seq(
      s"$File: retries.budget is 15s, and must be longer than 15s: the LogStore itself waits " +
        "that long for the coordination database before a commit fails " +
        "(databases.coordination.timeouts.connect, 5s, 3 times), and a shorter budget gives up " +
        "on outages that would have passed"
    )
    Profile.load(File, Valid + "retries: { budget: 16s, max_backoff: 10s }\n").isRight shouldBe true

    // It follows the timeout of the profile, not a number of its own.
    val patient = changed(
      "password: { secretRef: \"file:/run/secrets/ingest_coordination_password\" }",
      "password: { secretRef: \"file:/run/secrets/ingest_coordination_password\" }\n    timeouts: { connect: 1m }"
    )
    errorsOf(patient).head should include("retries.budget is 2m, and must be longer than 3m")
    Profile.load(File, patient + "retries: { budget: 5m }\n").isRight shouldBe true
  }

  test("a lease outlasts a renewal that takes as long as it may") {
    // Renewed every third of it; a renewal waits 5s for a connection and 10s for its answer.
    Profile.load(File, Valid + "leases: { ttl: 45s }\n").value.value.leases.ttl shouldBe 45.seconds
    errorsOf(Valid + "leases: { ttl: 44s }\n") shouldBe Seq(
      s"$File: leases.ttl is 44s, and must be at least 45s: a lease is renewed every third of " +
        "it, and a renewal can take 15s (databases.control.timeouts: connect, 5s, and statement, " +
        "10s). A shorter lease would be lost to a renewal that was still waiting for its answer"
    )

    // It follows the timeouts of the control plane, and of no other database.
    val quick = changed(
      "password: { secretRef: \"env:INGEST_CONTROL_PASSWORD\" }",
      "password: { secretRef: \"env:INGEST_CONTROL_PASSWORD\" }\n    timeouts: { connect: 1s, statement: 3s }"
    )
    Profile.load(File, quick + "leases: { ttl: 12s }\n").isRight shouldBe true
    errorsOf(quick + "leases: { ttl: 11s }\n").head should include(
      "leases.ttl is 11s, and must be at least 12s"
    )

    val patient = changed(
      "password: { secretRef: \"env:INGEST_CONTROL_PASSWORD\" }",
      "password: { secretRef: \"env:INGEST_CONTROL_PASSWORD\" }\n    timeouts: { statement: 1m }"
    )
    // The default is refused as well, when the timeouts are made longer than it can bear.
    errorsOf(patient).head should include("leases.ttl is 2m, and must be at least 195s")
    Profile.load(File, patient + "leases: { ttl: 5m }\n").isRight shouldBe true
  }

  test("a lease is ten seconds to a day, and nothing else is said of leases") {
    errorsOf(Valid + "leases: { ttl: 9s }\n") shouldBe
      Seq(s"$File: leases.ttl is 9s, and the shortest it may be is 10s")
    errorsOf(Valid + "leases: { ttl: 2d }\n") shouldBe
      Seq(s"$File: leases.ttl is 2d, and the longest it may be is 1d")
    errorsOf(Valid + "leases: { ttl: 120 }\n").head should startWith(s"$File: leases.ttl ")
    errorsOf(Valid + "leases: { ttl: 2m, heartbeat: 40s }\n") shouldBe
      Seq(s"$File: leases.heartbeat is not a field that is known here")
    // A section with nothing in it has the default.
    Profile.load(File, Valid + "leases: {}\n").value.value.leases shouldBe Profile.Leases.Default
  }

  test("the backoffs fit in each other and in the budget") {
    errorsOf(Valid + "retries: { first_backoff: 1m }\n") shouldBe
      Seq(s"$File: retries.first_backoff is 1m, longer than retries.max_backoff, 30s")
    errorsOf(Valid + "retries: { budget: 20s }\n") shouldBe
      Seq(s"$File: retries.max_backoff is 30s, longer than retries.budget, 20s")
  }

  test("TLS is off or verify-full, and off to another machine is warned of") {
    val verified = changed(
      "user: ingest_control\n    password: { secretRef: \"env:INGEST_CONTROL_PASSWORD\" }\n    tls: { mode: disable }",
      "user: ingest_control\n    password: { secretRef: \"env:INGEST_CONTROL_PASSWORD\" }\n    tls: { mode: verify-full, root_certificate: /etc/ssl/certs/lake-ca.pem }"
    )
    Profile.load(File, verified).value.value.databases.control.tls shouldBe
      Tls.VerifyFull("/etc/ssl/certs/lake-ca.pem")

    errorsOf(verified.replace("mode: verify-full", "mode: disable")) shouldBe Seq(
      s"$File: databases.control.tls.root_certificate is set, and mode is disable: nothing would be verified with it"
    )
    errorsOf(verified.replace("/etc/ssl/certs/lake-ca.pem", "certs/lake-ca.pem")) shouldBe
      Seq(s"$File: databases.control.tls.root_certificate must be an absolute path")

    val remote = Valid.replace(
      "localhost:5432/ingestion_control\"\n    user: ingest_coordination",
      "db.internal:5432/ingestion_control\"\n    user: ingest_coordination"
    )
    warningsOf(remote) shouldBe Seq(
      s"$File: databases.coordination.tls.mode is disable, and the database is on another " +
        "machine: its traffic, the password included, is not encrypted"
    )
  }

  test("a profile sets no LogStore, no master and no credential through the settings of Spark") {
    def settings(key: String): Seq[String] = errorsOf(Valid + s"  settings:\n    $key: x\n")

    settings("spark.delta.logStore.file.impl") shouldBe Seq(
      s"$File: spark.settings.spark.delta.logStore.file.impl is set by the engine: which " +
        "LogStore commits to a table is not a setting of a profile (ADR-0004)"
    )
    settings("spark.master") shouldBe
      Seq(s"$File: spark.settings.spark.master is set by the field master of this profile")
    Seq(
      "spark.hadoop.fs.s3a.access.key",
      "spark.hadoop.fs.s3a.secret.key",
      "spark.hadoop.fs.s3a.session.token",
      "spark.ssl.keyStorePassword",
      "spark.authenticate.secret"
    ).foreach(key => withClue(key)(settings(key).head should include("is a credential")))
    // The provider of credentials is a class to load them with, not one of them.
    Profile
      .load(
        File,
        Valid + "  settings:\n    spark.hadoop.fs.s3a.aws.credentials.provider: x.Provider\n"
      )
      .isRight shouldBe true
  }

  test("a profile sets none of the Spark settings that the engine sets, whatever their case") {
    def settings(key: String): Seq[String] = errorsOf(Valid + s"  settings:\n    $key: x\n")

    Seq(
      "spark.sql.extensions",
      "spark.sql.catalog.spark_catalog",
      "spark.SQL.Extensions"
    ).foreach { key =>
      withClue(key)(
        settings(key) shouldBe Seq(
          s"$File: spark.settings.$key is set by the engine: every session of a run has the " +
            "extension and the catalog of Delta"
        )
      )
    }
    settings("spark.app.name") shouldBe Seq(
      s"$File: spark.settings.spark.app.name is set by the engine: a session is named after " +
        "the source that it runs"
    )
    settings("spark.databricks.delta.history.metricsEnabled") shouldBe Seq(
      s"$File: spark.settings.spark.databricks.delta.history.metricsEnabled is set by the " +
        "engine: the rows that a run wrote are read from the commits of its batch, which " +
        "Delta counts them in only while this is on"
    )
    settings("spark.sql.session.timeZone") shouldBe Seq(
      s"$File: spark.settings.spark.sql.session.timeZone is set by the engine: every session " +
        "of a run is in UTC, as its JVM is, so that a time means the same to the engine, to " +
        "Spark and to a source"
    )
    settings("spark.delta.logstore.class") shouldBe Seq(
      s"$File: spark.settings.spark.delta.logstore.class is set by the engine: which " +
        "LogStore commits to a table is not a setting of a profile (ADR-0004)"
    )
    // Before it is taken for a credential: it is a reference, and the engine's to write.
    Seq(
      "spark.hadoop.lakehouse.logstore.jdbc.url",
      "spark.hadoop.lakehouse.logstore.jdbc.passwordRef",
      "spark.hadoop.lakehouse.logstore.fault.afterInsert.delayMs"
    ).foreach { key =>
      withClue(key)(
        settings(key) shouldBe Seq(
          s"$File: spark.settings.$key is set by the engine, from databases.coordination: how " +
            "the LogStore reaches its database is said there, and its fault hooks are for " +
            "tests alone (ADR-0004)"
        )
      )
    }
    settings("spark.Master") shouldBe
      Seq(s"$File: spark.settings.spark.Master is set by the field master of this profile")
  }

  test("a profile does not give new tables a retention of their transactions") {
    Seq(
      "spark.databricks.delta.properties.defaults.setTransactionRetentionDuration",
      "spark.databricks.delta.properties.defaults.settransactionretentionduration"
    ).foreach { key =>
      withClue(key)(
        errorsOf(Valid + s"  settings:\n    $key: \"interval 30 days\"\n") shouldBe Seq(
          s"$File: spark.settings.$key would give every table that Delta makes the property " +
            "delta.setTransactionRetentionDuration. Delta would then forget the batches " +
            "written to a table before that time, and a replay would write such a batch " +
            "again: no table of the engine may have it"
        )
      )
    }
    // Other defaults of the properties of a table are a profile's to give.
    Profile
      .load(
        File,
        Valid + "  settings:\n    spark.databricks.delta.properties.defaults.logRetentionDuration: \"interval 60 days\"\n"
      )
      .isRight shouldBe true
  }

  test("a profile whose databases cannot be reached is validated all the same, and at once") {
    // Nothing is connected to and no name is looked up: `.invalid` never resolves (RFC 6761).
    val unreachable = Valid.replace("localhost:5432", "db.unreachable.invalid:5432")
    Profile.load(File, unreachable).isRight shouldBe true

    val started = System.nanoTime()
    (1 to 20).foreach(_ => Profile.load(File, unreachable).isRight shouldBe true)
    val each = (System.nanoTime() - started).nanos / 20

    each should be < 100.millis
  }
}
