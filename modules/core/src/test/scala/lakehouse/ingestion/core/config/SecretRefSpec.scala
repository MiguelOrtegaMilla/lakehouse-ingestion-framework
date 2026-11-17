package lakehouse.ingestion.core.config

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, EitherValues}

final class SecretRefSpec
    extends AnyFunSuite
    with Matchers
    with EitherValues
    with BeforeAndAfterAll {

  private val dir: Path = Files.createTempDirectory("secret-ref-spec")

  override def afterAll(): Unit = {
    Option(dir.toFile.listFiles()).toSeq.flatten.foreach(_.delete())
    dir.toFile.delete(): Unit
  }

  private def file(name: String, content: String): SecretRef =
    SecretRef.File(Files.write(dir.resolve(name), content.getBytes(UTF_8)).toString)

  test("a reference is env:NAME or file:/absolute/path, and prints as it was written") {
    SecretRef.parse("env:SALES_DB_PASSWORD").value shouldBe SecretRef.Env("SALES_DB_PASSWORD")
    SecretRef.parse("env:_x1").value shouldBe SecretRef.Env("_x1")
    SecretRef.parse("file:/run/secrets/sales_db").value shouldBe SecretRef.File(
      "/run/secrets/sales_db"
    )

    SecretRef.Env("SALES_DB_PASSWORD").toString shouldBe "env:SALES_DB_PASSWORD"
    SecretRef.File("/run/secrets/sales_db").toString shouldBe "file:/run/secrets/sales_db"
  }

  test("what is not a reference is refused, and the refusal does not repeat it") {
    val literal = "s3cr3t-P4ssw0rd!"
    val notReferences = Seq(
      literal,
      "",
      "env:",
      "env:1NAME",
      "env:MY-VAR",
      "env:MY VAR",
      "env:A=B",
      "env:$(whoami)",
      "ENV:NAME",
      "file:",
      "file:relative/path",
      "file:./secret",
      "file:/run/secrets/../../etc/shadow",
      "file:/run//secrets",
      "file:/run/secrets/",
      "file:///run/secrets/x",
      "file:/run/secrets/x y",
      "file:/run/secrets/x\u0000",
      "file:/run/secrets/.hidden",
      "vault:secret/data/sales",
      "https://example.com/secret",
      "env:NAME\nenv:OTHER",
      s"password=$literal"
    )

    notReferences.foreach { text =>
      withClue(s"[$text]") {
        val reason = SecretRef.parse(text).left.value
        if (!Set("", "env:", "file:")(text)) reason should not include text
        reason should not include literal
      }
    }
    SecretRef.parse(literal).left.value shouldBe
      "is not a reference: write env:NAME or file:/absolute/path, never the secret"
  }

  test("a variable is resolved from the environment it is given") {
    val resolver = SecretResolver(Map("SALES_DB_PASSWORD" -> "hunter2", "EMPTY" -> "").get)

    resolver.resolve(SecretRef.Env("SALES_DB_PASSWORD")).value.reveal shouldBe "hunter2"
    resolver.resolve(SecretRef.Env("MISSING")).left.value shouldBe
      "cannot read the secret env:MISSING: the variable is not set"
    // An empty variable is a mistake in the deployment, not an empty password.
    resolver.resolve(SecretRef.Env("EMPTY")).left.value shouldBe
      "cannot read the secret env:EMPTY: the variable is not set"
  }

  test("a file is resolved to its content, without the line break at its end") {
    val resolver = SecretResolver(_ => None)

    resolver.resolve(file("plain", "hunter2")).value.reveal shouldBe "hunter2"
    resolver.resolve(file("unix", "hunter2\n")).value.reveal shouldBe "hunter2"
    resolver.resolve(file("windows", "hunter2\r\n")).value.reveal shouldBe "hunter2"
    // Only the last one: a secret may have line breaks and spaces of its own.
    resolver.resolve(file("inner", " hun\nter2 \n")).value.reveal shouldBe " hun\nter2 "
  }

  test("a file that cannot be read, or is empty, is an error that names the reference alone") {
    val resolver = SecretResolver(_ => None)
    val missing = SecretRef.File(dir.resolve("missing").toString)

    resolver.resolve(missing).left.value shouldBe
      s"cannot read the secret $missing: NoSuchFileException"
    resolver.resolve(file("empty", "")).left.value should endWith("the file is empty")
    resolver.resolve(file("only-a-line-break", "\n")).left.value should endWith("the file is empty")
  }

  test("a secret does not print itself") {
    val secret = new Secret("hunter2")

    secret.toString shouldBe "Secret(***)"
    s"connecting with $secret" should not include "hunter2"
    Seq(secret).toString should not include "hunter2"
    secret shouldBe new Secret("hunter2")
    secret should not be new Secret("hunter3")
    // A failed assertion on a secret shows nothing of it either.
    intercept[org.scalatest.exceptions.TestFailedException](
      secret shouldBe new Secret("x")
    ).getMessage should
      not include "hunter2"
  }
}
