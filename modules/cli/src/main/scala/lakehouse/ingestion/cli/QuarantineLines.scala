package lakehouse.ingestion.cli

import lakehouse.ingestion.core.config.SourceId
import lakehouse.ingestion.engine.quarantine.QuarantineReader.{Counts, Group, MaxChars, Record}

/**
 * How what a quarantine has is written for whoever asked: the counts as a table of text, and a
 * record as a line of JSON (JSON Lines). Nothing here writes anywhere: it makes the lines, and
 * the command says where they go.
 *
 * '''A record is written as one line, with no character that a terminal acts on.''' Its text is
 * what a source sent, which may be anything: the end of a line, a tab, or the sequence that
 * moves the cursor of a terminal, clears its screen or sets its title. As a string of JSON it
 * is one line, and every character that is not seen as itself is written as its escape
 * (`\uXXXX`), which whatever reads the line as JSON gives back as it was:
 *
 *  - the control characters (Cc): those below U+0020, among them ESC; U+007F; and those of
 *    U+0080 to U+009F, among them U+009B, which some terminals take for the start of a control
 *    sequence as they take ESC and `[`. JSON asks for the escape of the first of those alone;
 *  - the separators of lines and of paragraphs (U+2028 and U+2029), which some readers take for
 *    the end of a line, and which JSON lets through as they are;
 *  - the format characters (Cf), which have no glyph and change how what is around them is
 *    shown: those that turn the direction of a text among them (U+202E), by which a line reads
 *    as another than it is;
 *  - half of a pair of surrogates, which is no character, and cannot be written as UTF-8.
 *
 * Every other character is written as it is: a letter with an accent is read as one.
 */
private[cli] object QuarantineLines {

  /** Whether a character is written as its escape: one that is not seen as itself. */
  private def unseen(codePoint: Int): Boolean =
    Character.getType(codePoint) match {
      case Character.CONTROL | Character.FORMAT | Character.LINE_SEPARATOR |
          Character.PARAGRAPH_SEPARATOR | Character.SURROGATE =>
        true
      case _ => false
    }

  /** A character as its escape, or as the two of its pair of surrogates. */
  private def escape(codePoint: Int, into: java.lang.StringBuilder): Unit =
    Character.toChars(codePoint).foreach(unit => into.append(f"\\u${unit.toInt}%04x"))

  private def rendered(text: String, json: Boolean): String = {
    val into = new java.lang.StringBuilder(text.length + 2)
    if (json) into.append('"')
    var i = 0
    while (i < text.length) {
      val codePoint = text.codePointAt(i)
      codePoint match {
        case '"' if json => into.append("\\\"")
        case '\\' if json => into.append("\\\\")
        case '\n' if json => into.append("\\n")
        case '\r' if json => into.append("\\r")
        case '\t' if json => into.append("\\t")
        case other if unseen(other) => escape(other, into)
        case other => into.appendCodePoint(other)
      }
      i += Character.charCount(codePoint)
    }
    if (json) into.append('"')
    into.toString
  }

  /** A text as a string of JSON, between its quotes. */
  def string(text: String): String = rendered(text, json = true)

  /** A text as a line of a terminal shows it: what is not seen as itself, as its escape. */
  def seen(text: String): String = rendered(text, json = false)

  private val Null = "null"

  /**
   * A record as one line of JSON, with its fields in this order. `backfill` is the backfill
   * whose sequence its batch is of, and null for the regular sequence; it and `batch_status`
   * are null for a run that the control plane does not have.
   */
  def line(record: Record): String =
    Seq(
      "run_id" -> string(record.run.toString),
      "backfill" -> record.taken.flatMap(_.batch.app.backfill).fold(Null)(id => string(id.value)),
      "batch_seq" -> record.batchSeq.toString,
      "batch_status" -> record.taken.fold(Null)(taken => string(taken.status.name)),
      "source_file" -> record.file.fold(Null)(string),
      "error_type" -> string(record.errorType),
      "error_detail" -> record.errorDetail.fold(Null)(string),
      "quarantined_at" -> string(record.quarantinedAt.toString),
      "raw_record" -> string(record.raw),
      "raw_record_chars" -> record.chars.toString,
      "truncated" -> record.truncated.toString
    ).map { case (name, value) => s"${string(name)}:$value" }.mkString("{", ",", "}")

  /** What is said of the records that were written, on the error output: no record is in it. */
  def written(source: SourceId, records: Seq[Record], asked: Int): String = {
    val cut = records.count(_.truncated)
    s"${counted(records.size.toLong, "record")} of the quarantine of ${source.value} " +
      s"${if (records.size == 1) "was" else "were"} written, the latest first" +
      (if (cut == 0) "" else s"; $cut of them cut at $MaxChars characters") +
      (if (records.size < asked) "."
       else
         ". It may have more: ask for the counts, or for less of it with --run, --batch, " +
           "--file or --type.")
  }

  /**
   * The counts of a quarantine, as the lines of a table: a line for each group that is shown,
   * and then what there is of each type, what is not shown, and what was left out.
   */
  def table(source: SourceId, counts: Counts): Seq[String] = {
    val leftOut = Option.when(counts.leftOut.records > 0)(
      s"Left out: ${counted(counts.leftOut.records, "record")} of " +
        s"${counted(counts.leftOut.runs, "run")} that took a batch that was given up. They " +
        "count for nothing: the batch after sets the same records apart again."
    )
    if (counts.records == 0)
      s"The quarantine of ${source.value} has no record of what was asked." +: leftOut.toSeq
    else {
      val header = Seq("RUN", "BATCH", "STATUS", "FILE", "TYPE", "RECORDS", "LATEST")
      val rows = counts.groups.map(row)
      val widths = (header +: rows).transpose.map(_.map(_.length).max)
      def padded(cells: Seq[String]): String =
        cells
          .zip(widths)
          .zipWithIndex
          .map {
            // The count of the records to the right, as numbers are read.
            case ((cell, width), Records) => " " * (width - cell.length) + cell
            case ((cell, width), _) => cell + " " * (width - cell.length)
          }
          .mkString("  ")
          .replaceAll("\\s+$", "")
      val notShown = counts.groupsInAll - counts.groups.size
      Seq(
        s"The quarantine of ${source.value} has ${counted(counts.records, "record")} of what " +
          s"was asked, in ${counted(counts.groupsInAll, "group")} by run, file and type" +
          (if (notShown > 0) s"; the ${counts.groups.size} with the latest records are shown:"
           else ":"),
        "",
        padded(header)
      ) ++ rows.map(padded) ++ Seq(
        "",
        "By type: " + counts.byType.map(of => s"${of.errorType} ${of.records}").mkString(", ") + "."
      ) ++ Option.when(notShown > 0)(
        s"${counted(notShown, "more group is", "more groups are")} not shown: ask for less " +
          "of it with --run, --batch, --file or --type."
      ) ++ leftOut
    }
  }

  /** The place of the count of the records in a row. */
  private val Records = 5

  private def row(group: Group): Seq[String] = Seq(
    group.run.toString,
    // The number of the batch, after its backfill if it is a chunk of one.
    group.taken
      .flatMap(_.batch.app.backfill)
      .fold(group.batchSeq.toString)(backfill => s"$backfill#${group.batchSeq}"),
    group.taken.fold("unknown")(_.status.name),
    group.file.fold("-")(seen),
    seen(group.errorType),
    group.records.toString,
    group.last.toString
  )

  private def counted(count: Long, one: String): String = counted(count, one, s"${one}s")

  private def counted(count: Long, one: String, many: String): String =
    s"$count ${if (count == 1) one else many}"
}
