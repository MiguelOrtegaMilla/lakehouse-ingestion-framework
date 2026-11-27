package lakehouse.ingestion.core

/**
 * A text cut to a length for a person to read, counted in characters as Unicode counts them
 * (code points), as PostgreSQL's `char_length` counts them and Spark counts the characters of a
 * record of a quarantine.
 *
 * `String.take` counts the units of UTF-16, and a character outside the first plane is two of
 * them, a pair of surrogates: a cut between the two leaves half of one, which UTF-8 has no bytes
 * for. The driver of PostgreSQL writes `?` in its place, and Delta writes the bytes of a
 * replacement.
 */
object Text {

  /**
   * The first `max` characters of `text`, or all of it: never half of one.
   *
   * @param max how many characters, as code points, from 0
   */
  def upTo(text: String, max: Int): String = {
    require(max >= 0, s"a text is cut to 0 characters at the least, not $max")
    // A text of no more units than `max` has no more characters than that.
    if (text.length <= max || text.codePointCount(0, text.length) <= max) text
    else text.substring(0, text.offsetByCodePoints(0, max))
  }
}
