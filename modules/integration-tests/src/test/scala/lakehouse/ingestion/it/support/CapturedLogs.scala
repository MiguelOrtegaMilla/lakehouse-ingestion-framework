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

  /**
   * @param thrown what the line was logged with, whole: its causes are those of the failure
   * @param logger the name of the logger that logged it
   */
  final case class Logged(level: Level, message: String, thrown: Option[Throwable], logger: String)

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

  /**
   * What `everything` heard.
   *
   * @param events how many lines were logged, by every logger
   * @param loggers the loggers that logged them
   * @param kept those that were asked to be kept
   */
  final case class Heard(events: Long, loggers: Set[String], kept: Seq[Logged])

  /**
   * Hears every logger at `level` and above while `body` runs, whatever the configuration of
   * the tests lets through, and keeps the lines that `keep` says, by all that a line says: its
   * message, and each message of what it was logged with. The lines are not all kept: at a low
   * level they are very many. Nor are they printed: the appenders of the configuration hear
   * nothing meanwhile, and both they and the level are put back after.
   */
  def everything[T](level: Level)(keep: String => Boolean)(body: => T): (T, Heard) = {
    val context = LogManager.getContext(false).asInstanceOf[LoggerContext]
    val root = context.getConfiguration.getRootLogger
    val before = root.getLevel
    val theirs = root.getAppenders.asScala.values.toVector
    val events = new java.util.concurrent.atomic.AtomicLong
    val loggers = java.util.concurrent.ConcurrentHashMap.newKeySet[String]()
    val kept = new ConcurrentLinkedQueue[Logged]
    val appender =
      new AbstractAppender("heard-by-test", null, null, false, Property.EMPTY_ARRAY) {
        override def append(event: LogEvent): Unit = {
          events.incrementAndGet()
          loggers.add(event.getLoggerName)
          val message = event.getMessage.getFormattedMessage
          val causes = Iterator
            .iterate(event.getThrown)(_.getCause)
            .takeWhile(_ != null)
            .take(32)
            .map(cause => String.valueOf(cause.getMessage))
          if ((Iterator(message) ++ causes).exists(keep))
            kept.add(
              Logged(event.getLevel, message, Option(event.getThrown), event.getLoggerName)
            ): Unit
        }
      }
    appender.start()
    theirs.foreach(their => root.removeAppender(their.getName))
    root.addAppender(appender, null, null)
    root.setLevel(level)
    context.updateLoggers()
    val result =
      try body
      finally {
        root.removeAppender(appender.getName)
        theirs.foreach(root.addAppender(_, null, null))
        root.setLevel(before)
        context.updateLoggers()
        appender.stop()
      }
    (result, Heard(events.get, loggers.asScala.toSet, kept.asScala.toSeq))
  }

  private def capture[T](by: String => Boolean)(body: => T): (T, Seq[Logged]) = {
    val events = new ConcurrentLinkedQueue[Logged]
    val appender =
      new AbstractAppender("captured-by-test", null, null, false, Property.EMPTY_ARRAY) {
        // An event may be used again by log4j once this returns: what is kept is read here.
        override def append(event: LogEvent): Unit =
          if (by(event.getLoggerName))
            events.add(
              Logged(
                event.getLevel,
                event.getMessage.getFormattedMessage,
                Option(event.getThrown),
                event.getLoggerName
              )
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
