package lakehouse.ingestion.it.support

import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.{LogEvent, LoggerContext}
import org.apache.logging.log4j.{Level, LogManager}

/**
 * What a class logs while a test runs, read from the logging backend that the application runs
 * with, log4j, through an appender of the test's own. Only what the configuration of the tests
 * lets through is heard: warnings and errors.
 */
object CapturedLogs {

  /** @param thrown what the line was logged with, whole: its causes are those of the failure */
  final case class Logged(level: Level, message: String, thrown: Option[Throwable])

  /** What `by` logs while `body` runs, with what `body` gave. */
  def of[T](by: Class[_])(body: => T): (T, Seq[Logged]) = {
    val events = new ConcurrentLinkedQueue[Logged]
    val appender =
      new AbstractAppender("captured-by-test", null, null, false, Property.EMPTY_ARRAY) {
        // An event may be used again by log4j once this returns: what is kept is read here.
        override def append(event: LogEvent): Unit =
          if (event.getLoggerName == by.getName)
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
