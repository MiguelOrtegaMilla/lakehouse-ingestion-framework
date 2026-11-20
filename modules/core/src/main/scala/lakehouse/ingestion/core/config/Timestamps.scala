package lakehouse.ingestion.core.config

import java.time.format.DateTimeParseException
import java.time.{Instant, OffsetDateTime}

/**
 * A moment as configuration writes it: a date, a time of day to the second or finer, and its
 * zone, as `2026-01-01T00:00:00Z` or `2026-01-01T01:00:00+01:00` (ISO 8601).
 *
 * '''The zone is not optional.''' A moment without one is read in whatever zone a machine is
 * in, and a bound written so is off by the difference between two machines (discovery §17,
 * P6.2). A date alone is refused for the same reason: midnight of where?
 *
 * '''Microseconds at most.''' PostgreSQL keeps a timestamp to the microsecond, and rounds what
 * is finer: a bound of a window with nanoseconds would not be the bound that was written.
 *
 * '''From 1900 to the end of 9999.''' Spark writes no timestamp after 9999, and writes one
 * before the change to the Gregorian calendar, in 1582, as another day. A watermark is when a
 * row changed: 1900 is earlier than any of them, and far from both edges.
 */
object Timestamps {

  /** The earliest moment that a field may be. */
  val Min: Instant = Instant.parse("1900-01-01T00:00:00Z")

  /** The latest moment that a field may be: the last microsecond of 9999. */
  val Max: Instant = Instant.parse("9999-12-31T23:59:59.999999Z")

  private val Example = "2026-01-01T00:00:00Z"

  private val Written =
    """[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.([0-9]+))?(?:Z|[+-][0-9]{2}:[0-9]{2})""".r
  private val WithoutZone =
    """[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}(?::[0-9]{2}(?:\.[0-9]+)?)?""".r
  private val DateAlone = """[0-9]{4}-[0-9]{2}-[0-9]{2}""".r

  /** `Left` says what is wrong, to follow the name of the field: "has no zone…". */
  def parse(text: String): Either[String, Instant] = text match {
    case Written(fraction) =>
      if (Option(fraction).exists(_.length > 6))
        Left(
          "has more than six digits after the second: PostgreSQL keeps a timestamp to the " +
            "microsecond, and rounds the rest"
        )
      else
        exists(text).flatMap { instant =>
          if (instant.isBefore(Min))
            Left(s"is before ${render(Min)}, the earliest a moment may be")
          else if (instant.isAfter(Max))
            Left(s"is after ${render(Max)}, the latest a moment may be")
          else Right(instant)
        }
    case WithoutZone() =>
      Left(
        s"has no zone: write it with Z or an offset, as $Example. Without one it would be " +
          "read in the zone of whichever machine reads it"
      )
    case DateAlone() => Left(s"is a date alone: write a moment, with its zone, as $Example")
    case _ =>
      Left(
        s"is not a moment: write a date, a time and a zone, as $Example or " +
          "2026-01-01T01:00:00+01:00"
      )
  }

  /** The one way a moment is written: in UTC, with as many digits after the second as it has. */
  def render(instant: Instant): String = instant.toString

  /** The moment, if the date and the time of day are ones that a calendar has. */
  private def exists(text: String): Either[String, Instant] =
    try Right(OffsetDateTime.parse(text).toInstant)
    catch {
      case _: DateTimeParseException =>
        Left(
          "is not a moment that a calendar has: a month, a day, an hour or an offset is out of range"
        )
    }
}
