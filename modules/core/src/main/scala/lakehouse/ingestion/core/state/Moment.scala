package lakehouse.ingestion.core.state

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import cats.syntax.all._
import io.circe.{ACursor, Json}

/**
 * A moment that a plan or a watermark holds: a time of the clock of a source.
 *
 *  - '''To the microsecond''', which is what PostgreSQL keeps. A bound that is finer would be
 *    rounded by the source, and would not be the bound that was written down.
 *  - '''From 1900 to the end of 9999''', as a moment of configuration is (`Timestamps`): Spark
 *    writes no later timestamp into a statement, and writes one before 1582 as another day.
 *  - '''Written in UTC, in one way''': `2026-01-01T00:00:00Z`, with the digits after the second
 *    that it has. A document with a moment written otherwise is not one that this application
 *    wrote.
 */
private[core] object Moment {

  /** The earliest moment that a plan, a watermark or a field of configuration may be. */
  val Min: Instant = Instant.parse("1900-01-01T00:00:00Z")

  /** The latest moment that a plan, a watermark or a field of configuration may be. */
  val Max: Instant = Instant.parse("9999-12-31T23:59:59.999999Z")

  def toTheMicrosecond(at: Instant): Boolean = at.getNano % 1000 == 0

  def inRange(at: Instant): Boolean =
    !at.isBefore(Min) && !at.isAfter(Max)

  /** What is wrong with a moment, to follow its name: none, if a plan can hold it. */
  def problem(at: Instant): Option[String] =
    if (!toTheMicrosecond(at)) Some(s"is finer than a microsecond ($at)")
    else if (!inRange(at))
      Some(s"is $at, outside $Min to $Max")
    else None

  def write(at: Instant): Json = Json.fromString(at.toString)

  /** `Left` says what is wrong, starting with the name of the field. */
  def read(at: ACursor, field: String): Either[String, Instant] =
    for {
      text <- at.downField(field).as[String].leftMap(_ => s"$field is not a moment")
      moment <- Either.catchNonFatal(Instant.parse(text)).leftMap(_ => s"$field is not a moment")
      _ <- Either.cond(
        moment.toString == text,
        (),
        s"$field is not written as a moment is, in UTC and in one way: ${moment.toString}"
      )
      _ <- problem(moment).map(what => s"$field $what").toLeft(())
    } yield moment

  /** A duration of configuration, which is whole milliseconds (`Durations`). */
  def millis(duration: FiniteDuration): Long = {
    require(duration.toNanos % 1000000 == 0, s"$duration is not whole milliseconds")
    duration.toMillis
  }

  def earlier(a: Instant, b: Instant): Instant = if (a.isBefore(b)) a else b
  def later(a: Instant, b: Instant): Instant = if (a.isAfter(b)) a else b
}
