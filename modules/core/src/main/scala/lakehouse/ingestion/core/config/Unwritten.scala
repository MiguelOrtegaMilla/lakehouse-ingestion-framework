package lakehouse.ingestion.core.config

/**
 * How a character that a grammar does not allow is named in an error. The text it came from is
 * not repeated: it may hold characters that are not what they look like, or that a terminal
 * obeys, and the code point says which one it is.
 */
private[config] object Unwritten {

  /** `'x' (U+0078)` for what ASCII prints, and the code point with its Unicode name otherwise. */
  def describe(codePoint: Int): String = {
    val hex = f"U+$codePoint%04X"
    if (codePoint > 0x20 && codePoint < 0x7f)
      s"'${new String(Character.toChars(codePoint))}' ($hex)"
    else Option(Character.getName(codePoint)).fold(hex)(name => s"$hex $name")
  }

  /** The first code point of `text` that `allowed` does not hold, with its index in characters. */
  def firstOutside(text: String, allowed: Int => Boolean): Option[(Int, Int)] = {
    val codePoints = text.codePoints().toArray
    val at = codePoints.indexWhere(codePoint => !allowed(codePoint))
    Option.when(at >= 0)((codePoints(at), at + 1))
  }
}
