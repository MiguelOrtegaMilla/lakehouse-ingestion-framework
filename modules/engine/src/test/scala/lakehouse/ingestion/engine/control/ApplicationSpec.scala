package lakehouse.ingestion.engine.control

import java.time.Instant

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** What this process says it is, in the audit trail. */
final class ApplicationSpec extends AnyFunSuite with Matchers {

  test("the version is that of the build, with the commit it was built from") {
    // The build writes it down; here it was built from a repository, so the commit is there.
    Application.version should fullyMatch regex
      """\d+\.\d+\.\d+(-SNAPSHOT)?\+[0-9a-f]{12}(\.dirty)?"""
  }

  test("the instance is this process: its host, its number, and when it started") {
    val parts = Application.instance.value.split(":").toSeq
    parts should have size 3
    val (host, pid, started) = (parts(0), parts(1), parts(2))

    host should not be empty
    pid.toLong shouldBe ProcessHandle.current().pid()
    val start = Instant.ofEpochMilli(started.toLong)
    start should be <= Instant.now()
    start should be > Instant.now().minusSeconds(3600)
    // The same for the life of the process.
    Application.instance shouldBe Application.instance
  }
}
