package lakehouse.ingestion.core.config

/**
 * A path that configuration gave, relative to a root that the profile declares: a table under
 * the lake, a directory under the landing root (ADR-0013 §2, ADR-0011).
 *
 * This is the static half of the confinement: what can be told from the text. A path is
 * segments separated by `/`. A segment starts with a letter or a digit of ASCII and goes on
 * with letters, digits, `_`, `-`, `.` and `=`. That is an allow-list, and it leaves out
 * everything that takes a path somewhere else: a `/` in front, a scheme or a drive (no `:`),
 * `..` and `.` (a segment does not start with a dot), a backslash, a percent-encoding, a NUL.
 *
 * It also leaves out the names that Spark and Delta treat as hidden, the ones that start with
 * `_` or `.`. A target that ended in `_delta_log` would be the log of another table.
 *
 * So one directory of a lake can be the engine's own, `_quarantine`: no path that a
 * configuration gives is under it, and the tables that the engine makes there for a source are
 * no source's to name (`RelativePath.quarantine`).
 *
 * There is one way to write a path: no `/` at the end and no empty segment, so that two texts
 * never name one place. What the text cannot tell, a symbolic link that leads out of the root,
 * is checked where the path is used (P5).
 */
final class RelativePath private (val value: String) extends AnyVal {

  def segments: Vector[String] = value.split('/').toVector

  override def toString: String = value
}

object RelativePath {

  val MaxSegmentBytes = 255
  val MaxBytes = 1024

  /**
   * The directory of a lake that is the engine's own: under it are the tables that the engine
   * makes for a source and that no source names, the quarantine of each (discovery §13.4). No
   * path that is written in a configuration is under it, since no segment of one starts with
   * `_`: a source cannot give the quarantine of another, nor its own, as one of its targets.
   */
  val Quarantine = "_quarantine"

  /**
   * A path under the engine's own directory. It is made by the engine, from a path that is one
   * by the grammar, and never from what a configuration says.
   */
  private[core] def quarantine(under: RelativePath): RelativePath =
    new RelativePath(s"$Quarantine/${under.value}")

  /**
   * A path as the control plane stored it, of a table that a batch wrote: one that a
   * configuration may give, or one of the engine's own. `Left` says what is wrong.
   */
  def stored(text: String): Either[String, RelativePath] =
    if (text.startsWith(s"$Quarantine/"))
      parse(text.drop(Quarantine.length + 1)).map(quarantine)
    else parse(text)

  private def startsSegment(c: Int): Boolean =
    (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')

  private def inSegment(c: Int): Boolean =
    startsSegment(c) || c == '_' || c == '-' || c == '.' || c == '='

  /** `Left` says what is wrong, to follow the name of the field: "is empty", "is absolute…". */
  def parse(text: String): Either[String, RelativePath] =
    if (text.isEmpty) Left("is empty")
    else if (text.startsWith("/")) Left("is absolute: it must be relative to its root")
    else
      Unwritten.firstOutside(text, c => inSegment(c) || c == '/') match {
        case Some((codePoint, position)) => Left(notAllowed(codePoint, position))
        case None if text.length > MaxBytes =>
          Left(s"is ${text.length} bytes long, and a path is $MaxBytes at most")
        case None =>
          text.split("/", -1).toList.flatMap(wrong).headOption.toLeft(new RelativePath(text))
      }

  private def notAllowed(codePoint: Int, position: Int): String = {
    val why = codePoint match {
      case '\\' => "segments are separated by /"
      case '%' => "a path is not URL-encoded"
      case ':' => "a path has no scheme and no drive, it is relative to its root"
      case _ => "a segment is letters of ASCII, digits, and _ - . ="
    }
    s"has ${Unwritten.describe(codePoint)} at position $position: $why"
  }

  /** What is wrong with one segment, if anything. */
  private def wrong(segment: String): Option[String] =
    if (segment.isEmpty) Some("has an empty segment: no // and no / at the end")
    else if (segment == "..") Some("has the segment '..': a path does not climb out of its root")
    else if (segment == ".") Some("has the segment '.': a path is written one way")
    else if (!startsSegment(segment.codePointAt(0)))
      Some(
        s"has the segment '$segment', which starts with '${segment.head}': a segment starts " +
          "with a letter or a digit, and names that start with _ or . are hidden to Spark and Delta"
      )
    else if (segment.length > MaxSegmentBytes)
      Some(s"has a segment of ${segment.length} bytes, and a segment is $MaxSegmentBytes at most")
    else None
}
