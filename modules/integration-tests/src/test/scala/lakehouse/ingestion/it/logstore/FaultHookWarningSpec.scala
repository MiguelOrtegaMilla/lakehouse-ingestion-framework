package lakehouse.ingestion.it.logstore

import lakehouse.ingestion.it.support.CapturedLogs
import lakehouse.ingestion.logstore.{FaultHooks, FaultPoint, PostgresLogStore}
import org.apache.hadoop.conf.Configuration
import org.apache.logging.log4j.Level
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * A fault hook left on in a configuration cannot go unnoticed: every LogStore says so when it
 * is created. The warnings are read from the logging backend the application runs with, log4j
 * (`CapturedLogs`).
 */
final class FaultHookWarningSpec extends AnyFunSuite with Matchers {

  /** What `PostgresLogStore` logs while `body` runs: the level and the text of each event. */
  private def logged(body: => Unit): Seq[(Level, String)] =
    CapturedLogs.of(classOf[PostgresLogStore])(body)._2.map(line => line.level -> line.message)

  test("a LogStore created with fault hooks configured warns of each: its point and its wait") {
    val conf = new Configuration()
    conf.setLong(FaultHooks.delayKey(FaultPoint.AfterInsert), 150L)
    conf.setLong(FaultHooks.delayKey(FaultPoint.BeforeInsert), 20L)

    val warnings = logged(new PostgresLogStore(conf): Unit)

    warnings.map(_._1) shouldBe Seq(Level.WARN, Level.WARN)
    val texts = warnings.map(_._2)
    texts.head should include("20 ms at beforeInsert")
    texts.head should include("lakehouse.logstore.fault.beforeInsert.delayMs")
    texts.last should include("150 ms at afterInsert")
    texts.last should include("lakehouse.logstore.fault.afterInsert.delayMs")

    // Every LogStore says it again: Delta creates one for each table, and more in tasks.
    logged(new PostgresLogStore(conf): Unit) shouldBe warnings
  }

  test("a LogStore created without fault settings says nothing") {
    logged(new PostgresLogStore(new Configuration()): Unit) shouldBe empty
  }

  test("a fault setting that cannot be read is announced too, and does not fail the creation") {
    val conf = new Configuration()
    conf.set(FaultHooks.delayKey(FaultPoint.AfterPublish), "soon")

    val warnings = logged(new PostgresLogStore(conf): Unit)

    warnings.map(_._1) shouldBe Seq(Level.WARN)
    warnings.head._2 should include("lakehouse.logstore.fault.afterPublish.delayMs")
  }
}
