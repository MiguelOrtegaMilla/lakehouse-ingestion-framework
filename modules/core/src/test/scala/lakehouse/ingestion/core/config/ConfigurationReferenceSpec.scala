package lakehouse.ingestion.core.config

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}

import io.circe.Json
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Holds the documentation of the configuration to the code (`docs/configuration`).
 *
 * A reference that nobody checks says, after a while, what the model was. So:
 *
 *  - '''the fields of the reference are the fields of the model''', no more and no fewer. The
 *    fields of the model are taken from what a configuration with every field looks like when
 *    it is written down to be stored (`ConfigVersion`), which has the names of the fields of
 *    the files and is read back by the model itself. A field that is added to the model and
 *    not to the reference fails here, and so does one that the reference keeps after it is
 *    gone;
 *  - '''an example that the documentation marks as valid is valid'''.
 *
 * In the documents, `<!-- fields: NAME -->` is before a table whose first column has the
 * fields, and `<!-- valid: profile -->` or `<!-- valid: source -->` before a block of YAML.
 */
final class ConfigurationReferenceSpec extends AnyFunSuite with Matchers with EitherValues {

  private val repository: Path =
    Iterator
      .iterate(Paths.get("").toAbsolutePath)(_.getParent)
      .takeWhile(_ != null)
      .find(dir => Files.isDirectory(dir.resolve("docs/configuration")))
      .getOrElse(fail("docs/configuration was not found above the working directory"))

  private def document(name: String): Vector[String] =
    new String(
      Files.readAllBytes(repository.resolve(s"docs/configuration/$name")),
      UTF_8
    ).linesIterator.toVector

  private val reference = document("reference.md")

  /** The fields in the first column of the table that follows `<!-- fields: name -->`. */
  private def documented(name: String): Set[String] = {
    val marker = reference.indexOf(s"<!-- fields: $name -->")
    withClue(s"no table of fields '$name' in the reference: ")(marker should be >= 0)
    val Field = """\| `([^`]+)` \|.*""".r
    reference
      .drop(marker + 1)
      .takeWhile(_.startsWith("|"))
      .collect { case Field(field) => field }
      .toSet
  }

  /** The blocks of YAML that follow `<!-- valid: kind -->` in a document. */
  private def examples(lines: Vector[String], kind: String): Seq[String] =
    lines.zipWithIndex
      .collect { case (line, index) if line == s"<!-- valid: $kind -->" => index }
      .map { index =>
        lines(index + 1) shouldBe "```yaml"
        lines.drop(index + 2).takeWhile(_ != "```").mkString("\n") + "\n"
      }

  /**
   * The paths of the values of a document: a mapping is gone into, and nothing else is. A
   * mapping that is empty has no field to show: the sources of this test have one that is not.
   */
  private def leaves(json: Json, path: String = ""): Set[String] =
    json.asObject match {
      case Some(fields) =>
        fields.toList.flatMap { case (name, value) =>
          leaves(value, if (path.isEmpty) name else s"$path.$name")
        }.toSet
      case None => Set(path)
    }

  /** Says which fields are on one side alone, so that a failure names them. */
  private def sameFields(documented: Set[String], model: Set[String]): org.scalatest.Assertion = {
    withClue("in the reference, and not in the model: ")(
      (documented -- model).toSeq.sorted shouldBe empty
    )
    withClue("in the model, and not in the reference: ")(
      (model -- documented).toSeq.sorted shouldBe empty
    )
  }

  // A profile and two sources that leave no field out, between them.
  private val FullProfile =
    """lake: { uri: "file:///srv/lake", deployment: cluster, shared_filesystem: true }
      |landing: { root: "file:///srv/landing" }
      |databases:
      |  control:
      |    url: "jdbc:postgresql://db.internal:5432/ingestion_control"
      |    user: ingest_control
      |    password: { secretRef: "env:INGEST_CONTROL_PASSWORD" }
      |    tls: { mode: verify-full, root_certificate: /etc/ssl/certs/ca.pem }
      |    timeouts: { connect: 5s, statement: 10s }
      |  coordination:
      |    url: "jdbc:postgresql://db.internal:5432/ingestion_control"
      |    user: ingest_coordination
      |    password: { secretRef: "file:/run/secrets/coordination" }
      |    tls: { mode: verify-full, root_certificate: /etc/ssl/certs/ca.pem }
      |retries: { budget: 2m, first_backoff: 1s, max_backoff: 30s }
      |spark:
      |  master: "local[2]"
      |  settings: { spark.sql.shuffle.partitions: 4 }
      |""".stripMargin

  private val FullJdbcSource =
    """id: sales.orders
      |enabled: true
      |owner: team-sales
      |connector:
      |  type: jdbc
      |  connection:
      |    url: "jdbc:postgresql://db.internal:5432/sales"
      |    user: sales_ro
      |    password: { secretRef: "env:SALES_RO_PASSWORD" }
      |    tls: { mode: verify-full, root_certificate: /etc/ssl/certs/ca.pem }
      |    timeouts: { connect: 5s, statement: 10s }
      |  table: public.orders
      |load:
      |  mode: incremental
      |  watermark: { column: updated_at, lookback: 10m, safety_margin: 1m }
      |  max_batch_range: 7d
      |keys: [order_id]
      |deletes: { strategy: deleted_flag, column: is_deleted }
      |schema:
      |  on_new_column: add
      |  on_missing_column: "null"
      |  on_type_change: fail
      |  renames: { cust_mail: customer_email }
      |columns:
      |  customer_email: { pii: email, silver: { mask: hmac } }
      |quality: { max_rejected_ratio: 0.01 }
      |targets:
      |  bronze: { table: bronze/sales/orders }
      |  silver: { table: silver/sales/orders, partition_by: [order_day] }
      |freshness_sla: 2h
      |""".stripMargin

  private val FullFileSource =
    """id: sales.returns
      |connector: { type: file, path: sales/returns, format: csv, pattern: "returns-*.csv" }
      |load: { mode: new_files }
      |targets:
      |  bronze: { table: bronze/sales/returns }
      |""".stripMargin

  private val profile: Profile = Profile.load("profile.yaml", FullProfile).value.value
  private def source(text: String): Source = Source.load("source.yaml", text).value.value

  /** As it is stored: a field that a configuration does not have is not there as a null. */
  private def stored(content: Json): Set[String] = leaves(content.deepDropNullValues)

  private val profileFields: Set[String] = stored(ConfigVersion.canonical(profile))
  private val sourceFields: Set[String] =
    stored(ConfigVersion.canonical(source(FullJdbcSource))) ++
      stored(ConfigVersion.canonical(source(FullFileSource)))

  /** The places where a connection is, and the name that a writer chose as a key of a mapping. */
  private val Connections =
    Seq("databases.control", "databases.coordination", "connector.connection")
  private val Named = Seq("spark.settings", "schema.renames")
  private val Column = """columns\.[^.]+\.(.+)""".r

  /** A field of the model as the reference writes it. */
  private def asDocumented(field: String): String =
    (Connections ++ Named).find(place => field.startsWith(s"$place.")).getOrElse {
      field match {
        case Column(rest) => s"columns.<name>.$rest"
        case other => other
      }
    }

  private def ofConnection(fields: Set[String]): Set[String] =
    for {
      field <- fields
      place <- Connections
      if field.startsWith(s"$place.")
    } yield field.stripPrefix(s"$place.")

  test("the profile and the sources of this test leave no field of the model out") {
    // Otherwise the comparisons below would be with less than the model.
    profileFields should contain allOf (
      "lake.shared_filesystem",
      "spark.settings.spark.sql.shuffle.partitions"
    )
    profileFields should contain("databases.control.tls.root_certificate")
    sourceFields should contain allOf (
      "owner",
      "load.max_batch_range",
      "deletes.column",
      "schema.renames.cust_mail",
      "columns.customer_email.silver.mask",
      "targets.silver.partition_by",
      "freshness_sla",
      "connector.pattern"
    )
  }

  test("the fields of a profile in the reference are those of the model") {
    sameFields(documented("profile"), profileFields.map(asDocumented))
  }

  test("the fields of a connection in the reference are those of the model") {
    sameFields(documented("connection"), ofConnection(profileFields))
    // A connection is one shape, in a profile and in a source.
    ofConnection(sourceFields) shouldBe ofConnection(profileFields)
  }

  test("the fields of a source in the reference are those of the model") {
    val ofKinds = documented("connector jdbc") ++ documented("connector file")

    ofKinds.foreach(_ should startWith("connector."))
    sameFields(documented("source") ++ ofKinds, sourceFields.map(asDocumented))
    documented("source") intersect ofKinds shouldBe empty
  }

  test("every kind of connector that the framework has is in the reference") {
    ConnectorKind.BuiltIn.foreach { kind =>
      withClue(kind.name)(documented(s"connector ${kind.name}") should not be empty)
    }
  }

  test("the examples that the documentation marks as valid are valid") {
    val documents = Seq("reference.md", "adding-a-source.md").map(name => name -> document(name))
    val profiles = documents.flatMap { case (name, lines) =>
      examples(lines, "profile").map(name -> _)
    }
    val sources = documents.flatMap { case (name, lines) =>
      examples(lines, "source").map(name -> _)
    }

    profiles should not be empty
    sources.size should be >= 2
    profiles.foreach { case (name, text) =>
      withClue(s"$name:\n$text")(
        Profile.load(name, text).left.map(_.toChain.toList) shouldBe a[Right[_, _]]
      )
    }
    sources.foreach { case (name, text) =>
      withClue(s"$name:\n$text")(
        Source.load(name, text).left.map(_.toChain.toList) shouldBe a[Right[_, _]]
      )
    }
  }
}
