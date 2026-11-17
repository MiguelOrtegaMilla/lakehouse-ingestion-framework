package lakehouse.ingestion.it.config

import lakehouse.ingestion.core.config.{SecretRef => ConfiguredRef}
import lakehouse.ingestion.logstore.coordination.{SecretRef => LogStoreRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * `core` and the LogStore each read a reference to a secret, `env:NAME` or `file:/path`
 * (ADR-0007), and neither module may depend on the other (ADR-0012). The configuration is
 * validated by `core`, and the reference of the coordination database is then handed to the
 * LogStore as text: this holds the two to one syntax.
 *
 * `core` is the stricter of the two, on purpose. What it accepts, the LogStore must read as the
 * same reference. What the LogStore would accept and `core` refuses never reaches the LogStore.
 */
final class SecretRefAgreementSpec extends AnyFunSuite with Matchers {

  private val accepted = Seq(
    "env:COORDINATION_DB_PASSWORD",
    "env:_x",
    "env:A1",
    "file:/run/secrets/coordination_password",
    "file:/etc/lakehouse/secrets/db.password",
    "file:/a",
    "file:/var/run/secrets/dt=2026/key-1"
  )

  private val refused = Seq(
    "s3cr3t",
    "",
    "env:",
    "env:1NAME",
    "env:MY-VAR",
    "file:",
    "file:relative/path",
    "vault:secret/data/x"
  )

  test("a reference that core accepts is the same reference to the LogStore") {
    accepted.foreach { text =>
      withClue(text) {
        val configured = ConfiguredRef.parse(text)
        configured.isRight shouldBe true
        // Each prints as the text it was read from, so the text can travel between them.
        configured.map(_.toString) shouldBe Right(text)
        LogStoreRef.parse(text).map(_.toString) shouldBe Right(text)
      }
    }
  }

  test("what is not a reference is refused by both") {
    refused.foreach { text =>
      withClue(s"[$text]") {
        ConfiguredRef.parse(text).isLeft shouldBe true
        LogStoreRef.parse(text).isLeft shouldBe true
      }
    }
  }

  test("core refuses paths that the LogStore would take: it is the one that validates") {
    Seq("file:/run/secrets/../../etc/shadow", "file:/run//secrets", "file:/run/secrets/x y")
      .foreach { text =>
        withClue(text) {
          ConfiguredRef.parse(text).isLeft shouldBe true
          LogStoreRef.parse(text).isRight shouldBe true
        }
      }
  }
}
