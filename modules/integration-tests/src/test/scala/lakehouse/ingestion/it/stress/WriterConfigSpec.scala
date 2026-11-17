package lakehouse.ingestion.it.stress

import java.nio.file.Paths

import scala.concurrent.duration._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class WriterConfigSpec extends AnyFunSuite with Matchers {

  private val config = WriterConfig(Paths.get("/tmp/run"), 2, 200, 10, 150.seconds, guard = true)

  test("survives the round trip through the command line") {
    WriterConfig.fromArgs(config.toArgs) shouldBe Right(config)
    val unguarded = config.copy(guard = false)
    WriterConfig.fromArgs(unguarded.toArgs) shouldBe Right(unguarded)
    val inPlaceOfAKilledOne = config.copy(firstCommit = 7, restarted = true)
    WriterConfig.fromArgs(inPlaceOfAKilledOne.toArgs) shouldBe Right(inPlaceOfAKilledOne)
    val repeating = config.copy(repeatFor = 90.seconds)
    WriterConfig.fromArgs(repeating.toArgs) shouldBe Right(repeating)
  }

  test("rejects malformed, unknown, missing and out-of-range arguments") {
    def without(key: String) = config.toArgs.filterNot(_.startsWith(s"$key="))
    def replaced(key: String, value: String) = without(key) :+ s"$key=$value"

    val cases = Seq(
      (config.toArgs :+ "verbose") -> "expected key=value",
      (config.toArgs :+ "colour=red") -> "unknown arguments: colour",
      without("commits") -> "missing arguments: commits",
      replaced("commits", "0") -> "commits must be a positive integer",
      replaced("writer", "-1") -> "writer must be a non-negative integer",
      replaced("rows-per-commit", "ten") -> "rows-per-commit must be a positive integer",
      replaced("guard", "maybe") -> "guard must be true or false",
      replaced("first-commit", "0") -> "first-commit must be a positive integer",
      replaced("restarted", "again") -> "restarted must be true or false",
      replaced("repeat-seconds", "-1") -> "repeat-seconds must be a non-negative integer"
    )
    cases.foreach { case (args, expected) =>
      WriterConfig.fromArgs(args).left.map(_.startsWith(expected)) shouldBe Left(true)
    }
  }
}
