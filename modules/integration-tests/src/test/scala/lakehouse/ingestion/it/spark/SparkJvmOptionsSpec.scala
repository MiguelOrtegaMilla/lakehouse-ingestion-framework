package lakehouse.ingestion.it.spark

import java.lang.management.ManagementFactory

import scala.jdk.CollectionConverters._

import org.apache.spark.launcher.JavaModuleOptions
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Drift check for `project/SparkTestJvm.scala`, which copies Spark's JVM module options because
 * the build cannot call Spark.
 *
 * This is the only test that catches a missing option. Spark starts without them, and the other
 * suites only fail once a code path needs one, if they ever reach that path. Removing a single
 * option makes this test fail and name it (checked in P1.3).
 */
final class SparkJvmOptionsSpec extends AnyFunSuite with Matchers {

  test("the forked test JVM carries every module option the Spark on the classpath requires") {
    val jvmArgs = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala.toSet
    val missing = JavaModuleOptions.defaultModuleOptionArray().toSeq.filterNot(jvmArgs)

    withClue("Copy these into project/SparkTestJvm.scala (moduleOptions): ") {
      missing shouldBe empty
    }
  }

  test("the forked test JVM runs in UTC") {
    java.util.TimeZone.getDefault.getID shouldBe "UTC"
  }
}
