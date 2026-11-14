package lakehouse.ingestion.it.containers

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.testcontainers.utility.TestcontainersConfiguration

/**
 * Testcontainers starts Ryuk from the image pinned in `testcontainers.properties` (ADR-0013 §5).
 * `scripts/check-pins.sh` checks that the file pins it; this checks that Testcontainers reads it.
 * A misspelt key or a file outside the test classpath would silently fall back to the tag.
 */
final class RyukImageSpec extends AnyFunSuite with Matchers {

  test("Testcontainers resolves Ryuk to the image pinned by digest") {
    val image = TestcontainersConfiguration.getInstance().getRyukImage
    image should fullyMatch regex """testcontainers/ryuk:[\w.-]+@sha256:[0-9a-f]{64}"""
  }
}
