package lakehouse.ingestion.logstore.coordination

import java.io.IOException
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class SecretRefSpec extends AnyFunSuite with Matchers {

  private val Secret = "s3cr3t-value"

  private def secretFile(content: String): Path = {
    val file = Files.createTempFile("secret-ref-", ".txt")
    file.toFile.deleteOnExit()
    Files.write(file, content.getBytes(UTF_8))
  }

  test("a reference is env:NAME or file:/absolute/path") {
    SecretRef.parse("env:PG_PASSWORD") shouldBe Right(SecretRef.Env("PG_PASSWORD"))
    SecretRef.parse("file:/run/secrets/pg").map(_.toString) shouldBe Right("file:/run/secrets/pg")
  }

  test("anything else is rejected, without repeating what may be the secret itself") {
    val rejected = Seq(Secret, s"env:$Secret-not-a-name", s"file:relative/$Secret", "", "env:")
    rejected.foreach { text =>
      val problem = SecretRef.parse(text)
      withClue(s"'$text': ") {
        problem.isLeft shouldBe true
        problem.left.getOrElse("") should not include Secret
      }
    }
  }

  test("an environment reference reads the variable of the JVM that resolves it") {
    val reference = SecretRef.Env("PG_PASSWORD")

    reference.resolve(Map("PG_PASSWORD" -> Secret).get) shouldBe Secret

    val failure = the[IOException] thrownBy reference.resolve(Map.empty[String, String].get)
    failure.getMessage should include("env:PG_PASSWORD")
  }

  test("a file reference reads the file, without its final line break") {
    SecretRef.File(secretFile(Secret)).resolve() shouldBe Secret
    SecretRef.File(secretFile(s"$Secret\n")).resolve() shouldBe Secret
    SecretRef.File(secretFile(s"$Secret\r\n")).resolve() shouldBe Secret
    // Only the last line break is not part of the secret.
    SecretRef.File(secretFile(s" $Secret \n\n")).resolve() shouldBe s" $Secret \n"
  }

  test("a file that is missing or empty fails with the reference in the message") {
    val missing = SecretRef.File(Files.createTempDirectory("secret-ref-").resolve("absent"))
    (the[IOException] thrownBy missing.resolve()).getMessage should include(missing.toString)

    val empty = SecretRef.File(secretFile("\n"))
    (the[IOException] thrownBy empty.resolve()).getMessage should include("is empty")
  }

  test("a reference never prints the secret") {
    val file = secretFile(Secret)
    SecretRef.File(file).toString shouldBe s"file:$file"
    SecretRef.Env("PG_PASSWORD").toString shouldBe "env:PG_PASSWORD"
  }
}
