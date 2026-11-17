package lakehouse.ingestion.core.config

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest

import scala.jdk.CollectionConverters._
import scala.util.Using

import io.circe.Json
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The version of a configuration (ADR-0007): one hash for one configuration, however it was
 * written; a content that is the configuration, and no secret; and the changes that what is
 * already ingested cannot follow.
 */
final class ConfigVersionSpec extends AnyFunSuite with Matchers with EitherValues {

  private val ProfileText =
    """lake: { uri: "file:///data/lake", deployment: single-machine }
      |landing: { root: "file:///data/landing" }
      |databases:
      |  control:
      |    url: "jdbc:postgresql://localhost:5432/ingestion_control"
      |    user: ingest_control
      |    password: { secretRef: "env:INGEST_CONTROL_PASSWORD" }
      |    tls: { mode: disable }
      |  coordination:
      |    url: "jdbc:postgresql://localhost:5432/ingestion_control"
      |    user: ingest_coordination
      |    password: { secretRef: "env:INGEST_COORDINATION_PASSWORD" }
      |    tls: { mode: disable }
      |spark: { master: "local[2]" }
      |""".stripMargin

  private val SourceText =
    """id: sales.orders
      |connector:
      |  type: jdbc
      |  connection:
      |    url: "jdbc:postgresql://localhost:5432/sales"
      |    user: sales_ro
      |    password: { secretRef: "env:SALES_RO_PASSWORD" }
      |    tls: { mode: disable }
      |  table: public.orders
      |load:
      |  mode: incremental
      |  watermark: { column: updated_at, lookback: 10m }
      |keys: [order_id]
      |targets:
      |  bronze: { table: bronze/sales/orders }
      |  silver: { table: silver/sales/orders, partition_by: [order_day] }
      |""".stripMargin

  private def profile(text: String): Profile = Profile.load("profile.yaml", text).value.value
  private def source(text: String): Source = Source.load("source.yaml", text).value.value
  private def version(source: String = SourceText, profile: String = ProfileText): ConfigVersion =
    ConfigVersion.of(this.profile(profile), this.source(source))

  /** The text with one piece of it written another way. */
  private def changed(text: String, piece: String, to: String): String = {
    text should include(piece)
    text.replace(piece, to)
  }

  test("the hash is the SHA-256 of the content, written one way") {
    val v = version()

    v.hash should fullyMatch regex "[0-9a-f]{64}"
    v.hash shouldBe MessageDigest
      .getInstance("SHA-256")
      .digest(v.text.getBytes(UTF_8))
      .map(byte => f"$byte%02x")
      .mkString
    // Keys in order, no space, and nothing that is null.
    v.text should startWith("""{"profile":{"databases":{"control":{"password":{"secretRef":""")
    v.text should not include "null,"
    v.text should not include ": "
    v.content.hcursor.downField("schema_version").as[Int] shouldBe Right(
      ConfigVersion.SchemaVersion
    )
  }

  test("this configuration has this hash, until the way it is written down changes") {
    // The hash of every snapshot that is stored depends on how a configuration is written
    // down here. If this fails for a change that was meant, SchemaVersion goes up with it.
    version().hash shouldBe "9faee5e5282dec48de9856af119acdc466ff946c726a50d474bca797888f6108"
  }

  test("two files that say the same have one hash, however they say it") {
    val reordered =
      """targets:
        |  silver: { partition_by: [order_day], table: silver/sales/orders }
        |  bronze: { table: bronze/sales/orders }
        |
        |# What tells one order from another.
        |keys:
        |  - order_id
        |load:
        |  watermark:
        |    lookback: 600s          # ten minutes
        |    column: "updated_at"
        |  mode: incremental
        |connector:
        |  table: public.orders
        |  connection:
        |    tls: { mode: disable }
        |    password: { secretRef: "env:SALES_RO_PASSWORD" }
        |    user: sales_ro
        |    url: "jdbc:postgresql://localhost:5432/sales"
        |  type: jdbc
        |id: sales.orders
        |""".stripMargin
    version(reordered).hash shouldBe version().hash

    // What a file leaves out is what it would say by default.
    val spelledOut = SourceText +
      """enabled: true
        |schema: { on_new_column: add, on_missing_column: "null", on_type_change: fail, renames: {} }
        |quality: { max_rejected_ratio: 0 }
        |columns: {}
        |""".stripMargin
    version(spelledOut).hash shouldBe version().hash
    version(
      changed(SourceText, "lookback: 10m", "lookback: 10m, safety_margin: 0s")
    ).hash shouldBe version().hash

    val profileSpelledOut = changed(
      ProfileText,
      "deployment: single-machine }",
      "deployment: single-machine, shared_filesystem: false }"
    ) +
      "retries: { budget: 120s, first_backoff: 1000ms, max_backoff: 30s }\n"
    version(profile = profileSpelledOut).hash shouldBe version().hash
    version(profile =
      changed(ProfileText, "file:///data/lake", "file:///data/lake/")
    ).hash shouldBe version().hash
  }

  test("a configuration that says something else has another hash") {
    val base = version().hash
    val other = Seq(
      version(changed(SourceText, "keys: [order_id]", "keys: [order_id, line]")),
      version(changed(SourceText, "column: updated_at", "column: modified_at")),
      version(changed(SourceText, "lookback: 10m", "lookback: 11m")),
      version(changed(SourceText, "table: public.orders", "table: public.orders_v2")),
      version(changed(SourceText, "env:SALES_RO_PASSWORD", "env:SALES_RO_PASSWORD_2")),
      version(SourceText + "enabled: false\n"),
      version(SourceText + "owner: team-sales\n"),
      version(SourceText + "quality: { max_rejected_ratio: 0.01 }\n"),
      version(profile = changed(ProfileText, "file:///data/lake", "file:///data/lake2")),
      version(profile = ProfileText + "retries: { budget: 3m }\n"),
      version(profile = changed(ProfileText, "local[2]", "local[4]"))
    ).map(_.hash)

    other.foreach(_ should not be base)
    other.distinct should have size other.size.toLong
  }

  test("the content is the configuration: read back, it is the same profile and the same source") {
    val v = version(
      SourceText + "owner: team-sales\ncolumns:\n  email: { pii: email, silver: { mask: hmac } }\ndeletes: { strategy: deleted_flag, column: is_deleted }\nfreshness_sla: 2h\n"
    )
    def part(name: String): String =
      v.content.hcursor.downField(name).focus.map(_.noSpaces).getOrElse(fail(name))

    profile(part("profile")) shouldBe profile(ProfileText)
    val read = source(part("source"))
    ConfigVersion.of(profile(part("profile")), read).hash shouldBe v.hash
    read.columns.toMap shouldBe Map(
      "email" -> ColumnSpec(Some("email"), Some(ColumnSpec.Mask.Hmac))
    )
  }

  test("every configuration of the repository and of the corpus reads back as itself") {
    val repository = Iterator
      .iterate(Paths.get("").toAbsolutePath)(_.getParent)
      .takeWhile(_ != null)
      .find(dir => Files.isDirectory(dir.resolve("conf/sources")))
      .getOrElse(fail("conf/sources was not found above the working directory"))
    val corpus = Paths.get(getClass.getResource("/configurations/profile.yaml").toURI).getParent
    def read(file: Path): String = new String(Files.readAllBytes(file), UTF_8)
    def yamlIn(directory: Path): Seq[Path] =
      Using.resource(Files.walk(directory))(
        _.iterator.asScala.filter(_.toString.endsWith(".yaml")).toList
      )

    val example = profile(read(repository.resolve("conf/app/local.example.yaml")))
    val sources = (yamlIn(repository.resolve("conf/sources")) ++ yamlIn(corpus.resolve("valid")))
      .map(file => source(read(file)))
    sources.size should be >= 9

    sources.foreach { original =>
      withClue(original.id) {
        val v = ConfigVersion.of(example, original)
        val content = v.content.hcursor
        val again =
          Source.load("snapshot", content.downField("source").focus.get.noSpaces).value.value

        again shouldBe original
        Profile
          .load("snapshot", content.downField("profile").focus.get.noSpaces)
          .value
          .value shouldBe example
      }
    }
  }

  test("the content has where each secret is, and never a secret") {
    val v = version()
    // The secrets as a running application would have them.
    val resolver = SecretResolver(
      Map(
        "SALES_RO_PASSWORD" -> "hunter2-sales",
        "INGEST_CONTROL_PASSWORD" -> "hunter2-control",
        "INGEST_COORDINATION_PASSWORD" -> "hunter2-coordination"
      ).get
    )
    val secrets =
      Seq("SALES_RO_PASSWORD", "INGEST_CONTROL_PASSWORD", "INGEST_COORDINATION_PASSWORD")
        .map(name => resolver.resolve(SecretRef.Env(name)).value.reveal)

    secrets.foreach(secret => v.text should not include secret)
    v.text should include(""""password":{"secretRef":"env:SALES_RO_PASSWORD"}""")
    v.text should include(""""password":{"secretRef":"env:INGEST_COORDINATION_PASSWORD"}""")
  }

  private def breaking(now: ConfigVersion, last: ConfigVersion = version()): Seq[String] =
    ConfigVersion.breakingChanges(last.content, now).map(_.message)

  test("a source that did not change, or changed what can change, can run on") {
    breaking(version()) shouldBe empty
    Seq(
      version(
        SourceText + "owner: team-sales\nenabled: false\nquality: { max_rejected_ratio: 0.05 }\nfreshness_sla: 1h\n"
      ),
      version(changed(SourceText, "lookback: 10m", "lookback: 1h")),
      // How the database is reached, not which database it is.
      version(changed(SourceText, "env:SALES_RO_PASSWORD", "file:/run/secrets/sales_ro")),
      version(changed(SourceText, "user: sales_ro", "user: sales_reader")),
      version(
        changed(
          SourceText,
          "tls: { mode: disable }",
          "tls: { mode: disable }\n    timeouts: { connect: 2s, statement: 1m }"
        )
      ),
      version(
        SourceText + "schema: { on_new_column: rescue }\ncolumns:\n  email: { pii: email }\n"
      ),
      version(profile = ProfileText + "retries: { budget: 5m }\n"),
      version(profile = changed(ProfileText, "local[2]", "local[4]")),
      version(profile = changed(ProfileText, "file:///data/landing", "file:///data/landing2"))
    ).foreach(now => breaking(now) shouldBe empty)
  }

  test("another key, another watermark column or another table is a breaking change") {
    breaking(
      version(changed(SourceText, "keys: [order_id]", "keys: [order_id, line]"))
    ) shouldBe Seq(
      """keys of the source was ["order_id"] and is ["order_id","line"]: the rows already in Silver were merged by the old key, and would not be found by the new one"""
    )
    breaking(
      version(changed(SourceText, "column: updated_at", "column: modified_at"))
    ) shouldBe Seq(
      "load.watermark.column of the source was updated_at and is modified_at: the watermark that is stored is a value of the old column"
    )
    breaking(
      version(changed(SourceText, "bronze/sales/orders", "bronze/sales/orders_v2"))
    ) shouldBe Seq(
      "targets.bronze.table of the source was bronze/sales/orders and is bronze/sales/orders_v2: the Bronze table is another, and the state of the source says what was written to the old one"
    )
    breaking(
      version(changed(SourceText, "silver/sales/orders", "silver/sales/orders_v2"))
    ) shouldBe Seq(
      "targets.silver.table of the source was silver/sales/orders and is silver/sales/orders_v2: the Silver table is another, and what was merged is in the old one"
    )
    breaking(
      version(changed(SourceText, "partition_by: [order_day]", "partition_by: [order_month]"))
    ) shouldBe Seq(
      """targets.silver.partition_by of the source was ["order_day"] and is ["order_month"]: a table is partitioned one way, and this one already is"""
    )
    breaking(
      version(profile = changed(ProfileText, "file:///data/lake", "file:///data/lake2"))
    ) shouldBe Seq(
      "lake.uri of the profile was file:///data/lake and is file:///data/lake2: every table of the source is in another place, and its state says what was written to the old one"
    )
  }

  private val Full = SourceText.replace(
    "  mode: incremental\n  watermark: { column: updated_at, lookback: 10m }\n",
    "  mode: full\n"
  )

  private val elsewhere = "the watermark that is stored is of what was read before, and going on " +
    "from it would skip rows of what is read now without a word"

  test("a source loaded by a watermark that reads another table or database is a breaking change") {
    breaking(
      version(changed(SourceText, "table: public.orders", "table: public.orders_v2"))
    ) shouldBe
      Seq(s"connector.table of the source was public.orders and is public.orders_v2: $elsewhere")
    breaking(
      version(changed(SourceText, "localhost:5432/sales", "localhost:5432/sales_replica"))
    ) shouldBe Seq(
      "connector.connection.url of the source was jdbc:postgresql://localhost:5432/sales and is " +
        s"jdbc:postgresql://localhost:5432/sales_replica: $elsewhere"
    )
    // Another host or another port is another database, as far as anybody can tell from here.
    breaking(
      version(changed(SourceText, "localhost:5432/sales", "db.internal:5432/sales"))
    ) should have size 1
    breaking(
      version(changed(SourceText, "localhost:5432/sales", "localhost:6432/sales"))
    ) should have size 1
    // Written another way, it is the same database: the port that is left out is 5432.
    breaking(version(changed(SourceText, "localhost:5432/sales", "localhost/sales"))) shouldBe empty

    val both = changed(
      changed(SourceText, "table: public.orders", "table: archive.orders"),
      "5432/sales",
      "5432/archive"
    )
    ConfigVersion.breakingChanges(version().content, version(both)).map(_.field) shouldBe
      Seq("connector.connection.url", "connector.table")
  }

  test("a source loaded in full reads another table or another database as it likes") {
    val elsewhereInFull = changed(
      changed(Full, "table: public.orders", "table: public.orders_v2"),
      "5432/sales",
      "5432/archive"
    )

    // There is no watermark to go on from: a full load takes everything, whatever it is.
    breaking(version(elsewhereInFull), last = version(Full)) shouldBe empty
    // From a watermark to full loads, and from full loads to a watermark that starts anew.
    breaking(version(elsewhereInFull)) shouldBe empty
    breaking(
      version(changed(SourceText, "table: public.orders", "table: public.orders_v2")),
      last = version(Full)
    ) shouldBe empty
  }

  test("what the origin of a source is, is its connector's to say") {
    version().origin shouldBe
      Vector("source.connector.type", "source.connector.connection.url", "source.connector.table")
    // The origin is not content: it is in no hash.
    version().text should not include "origin"

    // A kind of connector that loads by a watermark names its own, and is held to it.
    final case class Feed(endpoint: String, token: SecretRef) extends ConnectorConfig {
      override def kind: String = "feed"
      override def canonical: Json = Json.obj(
        "endpoint" -> Json.fromString(endpoint),
        "token" -> Json.obj("secretRef" -> Json.fromString(token.toString))
      )
      override def origin: Seq[String] = Seq("endpoint")
    }
    def feed(endpoint: String, token: String): ConfigVersion = ConfigVersion.of(
      profile(ProfileText),
      source(SourceText).copy(connector = Feed(endpoint, SecretRef.Env(token)))
    )
    def fields(last: ConfigVersion, now: ConfigVersion): Seq[String] =
      ConfigVersion.breakingChanges(last.content, now).map(_.field)

    fields(feed("https://a.example/orders", "T1"), feed("https://b.example/orders", "T1")) shouldBe
      Seq("connector.endpoint")
    fields(
      feed("https://a.example/orders", "T1"),
      feed("https://a.example/orders", "T2")
    ) shouldBe empty
    // And another kind of connector altogether reads from somewhere else.
    fields(version(), feed("https://a.example/orders", "T1")) shouldBe Seq("connector.type")
  }

  test("every breaking change of a configuration is said, not the first alone") {
    val now = version(
      changed(
        changed(SourceText, "keys: [order_id]", "keys: [id]"),
        "column: updated_at",
        "column: modified_at"
      ),
      changed(ProfileText, "file:///data/lake", "file:///data/lake2")
    )

    ConfigVersion.breakingChanges(version().content, now).map(c => c.of -> c.field) shouldBe
      Seq("profile" -> "lake.uri", "source" -> "keys", "source" -> "load.watermark.column")
  }

  test("what a source stops doing, or starts doing, breaks nothing that was done") {
    val bronzeOnly = SourceText.replace(
      "  silver: { table: silver/sales/orders, partition_by: [order_day] }\n",
      ""
    )
    val full = Full

    // Silver given up, and taken up again later with another key: nothing merged is in the way.
    breaking(version(bronzeOnly.replace("keys: [order_id]", "keys: [id]"))) shouldBe empty
    breaking(
      version(changed(SourceText, "keys: [order_id]", "keys: [id]")),
      last = version(bronzeOnly)
    ) shouldBe empty
    // From a watermark to full loads and back: the watermark is another matter of the state.
    breaking(version(full)) shouldBe empty
    breaking(
      version(changed(SourceText, "column: updated_at", "column: modified_at")),
      last = version(full)
    ) shouldBe empty
  }

  test("a snapshot that an older application wrote is compared by what it has") {
    val older = Json.obj(
      "schema_version" -> Json.fromInt(0),
      "source" -> Json.obj(
        "id" -> Json.fromString("sales.orders"),
        "keys" -> Json.arr(Json.fromString("order_id")),
        "targets" -> Json.obj(
          "bronze" -> Json.obj("table" -> Json.fromString("bronze/sales/orders_old"))
        ),
        "a_field_of_then" -> Json.fromString("whatever")
      )
    )

    ConfigVersion.breakingChanges(older, version()).map(_.field) shouldBe Seq(
      "targets.bronze.table"
    )
    ConfigVersion.breakingChanges(Json.obj(), version()) shouldBe empty
  }
}
