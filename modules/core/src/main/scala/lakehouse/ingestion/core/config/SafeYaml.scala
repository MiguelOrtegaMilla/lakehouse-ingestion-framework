package lakehouse.ingestion.core.config

import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.OptionConverters._

import io.circe.Json
import io.circe.yaml.v12.{Parser => YamlParser}
import org.snakeyaml.engine.v2.api.LoadSettings
import org.snakeyaml.engine.v2.api.lowlevel.Parse
import org.snakeyaml.engine.v2.events._
import org.snakeyaml.engine.v2.exceptions.{Mark, MarkedYamlEngineException, YamlEngineException}

/**
 * Reads one YAML document into JSON, for configuration that people write and the application
 * does not trust (ADR-0007, ADR-0013 §5).
 *
 * The document is YAML 1.2: `yes`, `no`, `on` and `off` are strings, and only `true` and `false`
 * are booleans. It is loaded by circe-yaml, which builds a tree of nodes and turns it into JSON.
 * No tag ever chooses a class to construct, which is what CVE-2022-1471 was in SnakeYAML.
 *
 * Before it is loaded, the document is looked at as the stream of its events, which the parser
 * yields one at a time without building anything. That look refuses what the loader would
 * accept, what it cannot be asked to refuse, and what it refuses badly (as measured on
 * circe-yaml-v12 0.16.1 with snakeyaml-engine 3.0.1):
 *
 *  - '''too much nesting'''. The loader has no limit on depth, and it builds and walks the tree
 *    by recursion: a hundred thousand `[` end in a `StackOverflowError`, which is no error of
 *    the document;
 *  - '''too many aliases''', which is how a short document becomes a huge tree (the "billion
 *    laughs"). The loader's own limit counts only aliases of collections, and by default
 *    allows fifty;
 *  - '''any tag''', `!!str` included. The loader turns a tag it does not know into an object
 *    with the tag's name as its key: `password: !secret x` becomes `{"secret": "x"}`, and
 *    the shape of the configuration changes without a word;
 *  - '''a key twice in one mapping'''. The loader refuses it too, by throwing an exception
 *    out of a method that returns its failures;
 *  - '''a key that is not a scalar''', which JSON cannot hold;
 *  - '''no document, or more than one''', which the loader refuses as well.
 *
 * A document that is too large is refused before it is read at all.
 *
 * What a writer of configuration should know of YAML 1.2 as it is loaded here, the JSON schema:
 * `null` or nothing is a null, and `~` is a string; `True` is a string; and `<<` is a key like
 * any other, not a merge.
 */
object SafeYaml {

  /**
   * @param maxCodePoints the length of a document. Configuration files are a few kilobytes.
   * @param maxDepth how deep collections may nest. The configuration model nests five or six.
   * @param maxAliases how many aliases a document may have, of any kind. An alias inside an
   *   aliased collection multiplies, so the tree can grow by a factor of two for every two.
   */
  final case class Limits(maxCodePoints: Int, maxDepth: Int, maxAliases: Int) {
    require(maxCodePoints > 0 && maxDepth > 0 && maxAliases >= 0, s"invalid limits: $this")
  }

  object Limits {
    val Default: Limits = Limits(maxCodePoints = 256 * 1024, maxDepth = 32, maxAliases = 8)
  }

  /** Where in the document, counted from 1 as an editor shows it. */
  final case class Position(line: Int, column: Int) {
    override def toString: String = s"line $line, column $column"
  }

  /** Why a document was refused. */
  sealed abstract class Refusal extends Product with Serializable {

    /** Where the problem is, if it has a place. */
    def at: Option[Position]

    /** What is wrong, without the place. */
    def problem: String

    final def message: String = at.fold(problem)(position => s"$problem ($position)")
  }

  object Refusal {

    final case class TooLarge(codePoints: Int, limit: Int) extends Refusal {
      override def at: Option[Position] = None
      override def problem: String =
        s"the document has $codePoints characters, and the limit is $limit"
    }

    final case class TooDeep(limit: Int, at: Option[Position]) extends Refusal {
      override def problem: String = s"collections are nested more than $limit deep"
    }

    final case class TooManyAliases(limit: Int, at: Option[Position]) extends Refusal {
      override def problem: String = s"the document has more than $limit aliases"
    }

    final case class DuplicateKey(key: String, at: Option[Position]) extends Refusal {
      override def problem: String = s"the key '$key' appears twice in one mapping"
    }

    final case class TagNotAllowed(tag: String, at: Option[Position]) extends Refusal {
      override def problem: String = s"the tag '$tag' is not allowed: no tag is"
    }

    final case class KeyNotScalar(at: Option[Position]) extends Refusal {
      override def problem: String = "a key is a collection or an alias, and must be a plain value"
    }

    final case class NotOneDocument(documents: Int) extends Refusal {
      override def at: Option[Position] = None
      override def problem: String =
        if (documents == 0) "the file has no document"
        else s"the file has $documents documents, and must have one"
    }

    /** The text is not YAML, or the loader could not turn it into JSON. */
    final case class Malformed(problem: String, at: Option[Position]) extends Refusal
  }

  /** Reads `text` as one YAML document, or says why it was refused. */
  def parse(text: String, limits: Limits = Limits.Default): Either[Refusal, Json] = {
    val codePoints = text.codePointCount(0, text.length)
    if (codePoints > limits.maxCodePoints) Left(Refusal.TooLarge(codePoints, limits.maxCodePoints))
    else examine(text, limits).toLeft(()).flatMap(_ => load(text, limits))
  }

  /** The loader, with its own limits set as well: a second line, behind the look at the events. */
  private def load(text: String, limits: Limits): Either[Refusal, Json] =
    // The loader returns most of its failures, and throws some: a duplicate key is one.
    try
      YamlParser
        .make(
          YamlParser.Config(
            allowDuplicateKeys = false,
            allowRecursiveKeys = false,
            codePointLimit = limits.maxCodePoints,
            maxAliasesForCollections = limits.maxAliases
          )
        )
        .parse(text)
        .left
        .map(failure => Refusal.Malformed(firstLine(failure.message), None))
    catch {
      case thrown: YamlEngineException =>
        Left(
          Refusal.Malformed(firstLine(Option(thrown.getMessage).getOrElse(thrown.toString)), None)
        )
    }

  /** A mapping that is open, with the keys seen in it and whether a key comes next. */
  final private class OpenMapping {
    val keys: mutable.Set[String] = mutable.Set.empty
    var keyNext: Boolean = true
  }

  /**
   * Goes through the events of the document and stops at the first thing that is refused. The
   * parser behind the events is a state machine, with no recursion: the depth of a document
   * costs it nothing, and nothing is built.
   */
  private def examine(text: String, limits: Limits): Option[Refusal] = {
    val settings = LoadSettings.builder().setCodePointLimit(limits.maxCodePoints).build()
    // `None` for a sequence, which has no keys.
    val open = mutable.Stack.empty[Option[OpenMapping]]
    var documents = 0
    var aliases = 0

    /** A value is complete: in a mapping, a key comes next. */
    def valueDone(): Unit = open.headOption.flatten.foreach(_.keyNext = true)

    /** The node that `event` starts is a key if a mapping is open and a key comes next. */
    def asKey(event: Event): Option[Option[Refusal]] =
      open.headOption.flatten.filter(_.keyNext).map { mapping =>
        mapping.keyNext = false
        event match {
          case scalar: ScalarEvent =>
            Option.when(!mapping.keys.add(scalar.getValue))(
              Refusal.DuplicateKey(scalar.getValue, position(event))
            )
          case _ => Some(Refusal.KeyNotScalar(position(event)))
        }
      }

    def opened(mapping: Option[OpenMapping], event: Event): Option[Refusal] = {
      open.push(mapping)
      Option.when(open.size > limits.maxDepth)(Refusal.TooDeep(limits.maxDepth, position(event)))
    }

    def tagged(tag: java.util.Optional[String], event: Event): Option[Refusal] =
      tag.toScala.map(Refusal.TagNotAllowed(_, position(event)))

    def refused(event: Event): Option[Refusal] = event match {
      case _: DocumentStartEvent =>
        documents += 1
        None
      case scalar: ScalarEvent =>
        tagged(scalar.getTag, event).orElse {
          asKey(event).getOrElse {
            valueDone()
            None
          }
        }
      case _: AliasEvent =>
        aliases += 1
        if (aliases > limits.maxAliases)
          Some(Refusal.TooManyAliases(limits.maxAliases, position(event)))
        else
          asKey(event).getOrElse {
            valueDone()
            None
          }
      case mapping: MappingStartEvent =>
        tagged(mapping.getTag, event)
          .orElse(asKey(event).flatten)
          .orElse(opened(Some(new OpenMapping), event))
      case sequence: SequenceStartEvent =>
        tagged(sequence.getTag, event)
          .orElse(asKey(event).flatten)
          .orElse(opened(None, event))
      case _: MappingEndEvent | _: SequenceEndEvent =>
        open.pop(): Unit
        valueDone()
        None
      case _ => None
    }

    try {
      val events = new Parse(settings).parseString(text).iterator()

      @tailrec
      def first(): Option[Refusal] =
        if (!events.hasNext) None
        else
          refused(events.next()) match {
            case None => first()
            case found => found
          }

      first().orElse(Option.when(documents != 1)(Refusal.NotOneDocument(documents)))
    } catch {
      case marked: MarkedYamlEngineException =>
        Some(
          Refusal.Malformed(
            Seq(Option(marked.getContext), Option(marked.getProblem)).flatten.mkString(": "),
            marked.getProblemMark.toScala.map(position)
          )
        )
      case other: YamlEngineException =>
        Some(Refusal.Malformed(firstLine(Option(other.getMessage).getOrElse(other.toString)), None))
    }
  }

  private def position(event: Event): Option[Position] = event.getStartMark.toScala.map(position)

  private def position(mark: Mark): Position = Position(mark.getLine + 1, mark.getColumn + 1)

  /** The parser's messages go on with a snippet of the document; the first line is the problem. */
  private def firstLine(message: String): String =
    message.linesIterator.nextOption().getOrElse("").trim
}
