package lakehouse.ingestion.it.support

import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.{Configurator, Property}
import org.apache.logging.log4j.core.{LogEvent, LoggerContext}
import org.apache.logging.log4j.{Level, LogManager}

/**
 * What a class logs while a test runs, read from the logging backend that the application runs
 * with, log4j, through an appender of the test's own. Only what the configuration of the tests
 * lets through is heard, warnings and errors, unless a test sets the level of what it hears
 * (`under`).
 */
object CapturedLogs {

  /** @param thrown what the line was logged with, whole: its causes are those of the failure */
  final case class Logged(level: Level, message: String, thrown: Option[Throwable])

  /** What `by` logs while `body` runs, with what `body` gave. */
  def of[T](by: Class[_])(body: => T): (T, Seq[Logged]) =
    capture(_ == by.getName)(body)

  /**
   * What every logger whose name starts with `prefix` logs at `level` and above while `body`
   * runs, whatever the configuration of the tests lets through; the level is put back after.
   */
  def under[T](prefix: String, level: Level)(body: => T): (T, Seq[Logged]) = {
    val before = LogManager.getLogger(prefix).getLevel
    Configurator.setLevel(prefix, level)
    try capture(_.startsWith(prefix))(body)
    finally Configurator.setLevel(prefix, before)
  }

  private def capture[T](by: String => Boolean)(body: => T): (T, Seq[Logged]) = {
    val events = new ConcurrentLinkedQueue[Logged]
    val appender =
      new AbstractAppender("captured-by-test", null, null, false, Property.EMPTY_ARRAY) {
        // An event may be used again by log4j once this returns: what is kept is read here.
        override def append(event: LogEvent): Unit =
          if (by(event.getLoggerName))
            events.add(
              Logged(event.getLevel, event.getMessage.getFormattedMessage, Option(event.getThrown))
            ): Unit
      }
    appender.start()
    val root =
      LogManager.getContext(false).asInstanceOf[LoggerContext].getConfiguration.getRootLogger
    root.addAppender(appender, null, null)
    val result =
      try body
      finally {
        root.removeAppender(appender.getName)
        appender.stop()
      }
    (result, events.asScala.toSeq)
  }
}
