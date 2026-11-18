package lakehouse.ingestion.engine.control

import io.circe.{Json, Printer}
import lakehouse.ingestion.core.config.SafeYaml

/**
 * Reads back the JSON that the control plane stores: the snapshot of a configuration, the plan
 * of a batch. It is read by the safe reader of the configuration (`SafeYaml`, ADR-0013 §5),
 * and not by a JSON parser of its own.
 *
 * YAML 1.2 has JSON in it, and that reader builds the same `Json` from the events of its
 * parser, with a limit on the size and on the depth of what it reads, no recursion, and nothing
 * that it constructs. So the engine has one reader of text into `Json`, with limits that are
 * its own, and no JSON parser among its libraries: the one that circe has (jawn) had no limit
 * on depth until a version that is built for a Scala newer than the build's.
 *
 * What it reads is what PostgreSQL writes for a `jsonb`, which is JSON by construction. It is
 * not a validator of JSON: it also takes YAML that is not JSON.
 *
 * It does not read every JSON there is. A document has its limits of size and of depth, and a
 * key of an object is 1,022 characters at most, as it is written between its quotes: YAML does
 * not take a longer one written as JSON writes it. So nothing is stored that could not be read back (`readable`), and a document that
 * is beyond a limit is refused when it is written, by the run that would write it, and not
 * found later by another.
 */
object StoredJson {

  /**
   * What a stored document may be. It is larger than a file of configuration may be: a plan
   * names every file of its batch. JSON has no aliases, and none is taken.
   */
  val Limits: SafeYaml.Limits =
    SafeYaml.Limits(maxCodePoints = 8 * 1024 * 1024, maxDepth = 32, maxAliases = 0)

  /** @return the document, or why it could not be read. The reason never repeats the text */
  def parse(text: String, limits: SafeYaml.Limits = Limits): Either[String, Json] =
    SafeYaml.parse(escaped(text), limits).left.map(_.message)

  // A document as PostgreSQL writes a jsonb: a space after each colon and each comma.
  private val AsStored: Printer =
    Printer.noSpaces.copy(colonRight = " ", objectCommaRight = " ", arrayCommaRight = " ")

  /**
   * Whether a document that is about to be stored will be read back as itself. What stores a
   * document asks first.
   *
   * @return why not: which limit it is beyond
   */
  def readable(document: Json): Either[String, Unit] =
    parse(AsStored.print(document)).flatMap { read =>
      Either.cond(read == document, (), "it is not read back as it was written")
    }

  /**
   * JSON takes any character raw in a string but the controls below a space, and PostgreSQL
   * writes them raw. YAML's printable characters are fewer: the specification leaves out DEL,
   * the controls of C1 and two that are no characters, U+FFFE and U+FFFF, and has line breaks
   * of its own. The parser refuses the last two today, and takes the others; what another
   * version of it does with them is not relied on.
   *
   * In a JSON text such a character can only be inside a string, where `\uXXXX` says the same,
   * to JSON and to YAML alike. So each is written that way before the text is read.
   */
  private def escaped(text: String): String =
    if (text.forall(raw)) text
    else {
      val out = new java.lang.StringBuilder(text.length + 16)
      text.foreach(c => if (raw(c)) out.append(c) else out.append(f"\\u${c.toInt}%04x"))
      out.toString
    }

  /** Whether a UTF-16 unit is read by YAML as itself, wherever it is in a string. */
  private def raw(c: Char): Boolean =
    (c >= 0x20 && c <= 0x7e) || c == '\n' || c == '\r' || c == '\t' ||
      (c >= 0xa0 && c <= 0xd7ff && c != 0x2028 && c != 0x2029) ||
      // Halves of a character beyond the first plane: PostgreSQL writes them in pairs.
      Character.isSurrogate(c) ||
      (c >= 0xe000 && c <= 0xfffd && c != 0xfeff)
}
