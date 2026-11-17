package lakehouse.ingestion.core.config

import scala.concurrent.duration._

/**
 * A length of time as configuration writes it: a whole number and a unit, `500ms`, `30s`, `10m`,
 * `2h`, `7d`.
 *
 * The unit is not optional. A bare `30` would be seconds to one reader and milliseconds to
 * another, and a timeout that is a thousand times what was meant is found in production. The
 * units are in lower case alone, because `M` is a month elsewhere. There is one way to write
 * a number: no sign, no fraction, no zeros in front.
 */
object Durations {

  /** The longest a duration may be: ten years, far beyond any timeout, lookback or retention. */
  val Max: FiniteDuration = 3650.days

  private val Written = """(0|[1-9][0-9]{0,8})(ms|s|m|h|d)""".r
  private val BareNumber = """[0-9]+""".r

  private val Units: Seq[(String, TimeUnit)] =
    Seq("d" -> DAYS, "h" -> HOURS, "m" -> MINUTES, "s" -> SECONDS, "ms" -> MILLISECONDS)

  /** `Left` says what is wrong, to follow the name of the field: "has no unit…". */
  def parse(text: String): Either[String, FiniteDuration] = text match {
    case Written(number, unit) =>
      // Nine digits of days are within a Long of milliseconds; FiniteDuration holds 292 years.
      val millis = Units.toMap.apply(unit).toMillis(number.toLong)
      if (millis > Max.toMillis)
        Left(s"is longer than ${render(Max)}, the longest a duration may be")
      else Right(millis.millis.toCoarsest)
    case BareNumber() => Left("has no unit: write it as 500ms, 30s, 10m, 2h or 7d")
    case _ =>
      Left("is not a duration: a whole number and a unit, as in 500ms, 30s, 10m, 2h or 7d")
  }

  /** The one way a duration is written: with the largest unit that holds it in whole numbers. */
  def render(duration: FiniteDuration): String = {
    val millis = duration.toMillis
    if (millis == 0) "0s"
    else
      Units
        .collectFirst {
          case (name, unit) if millis % unit.toMillis(1) == 0 =>
            s"${millis / unit.toMillis(1)}$name"
        }
        .getOrElse(s"${millis}ms")
  }
}
