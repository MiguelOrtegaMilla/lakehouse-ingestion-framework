package lakehouse.ingestion.core.config

import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.OptionConverters._

import io.circe.{Json, JsonNumber}
import org.snakeyaml.engine.v2.api.LoadSettings
import org.snakeyaml.engine.v2.api.lowlevel.Parse
import org.snakeyaml.engine.v2.common.Anchor
import org.snakeyaml.engine.v2.events._
import org.snakeyaml.engine.v2.exceptions.{Mark, MarkedYamlEngineException, YamlEngineException}
import org.snakeyaml.engine.v2.nodes.Tag
import org.snakeyaml.engine.v2.resolver.JsonScalarResolver

/**
 * Reads one YAML document into JSON, for configuration that people write and the application
 * does not trust (ADR-0007, ADR-0013 §5).
 *
 * The JSON is built from the events of the document, which snakeyaml-engine's parser yields one
 * at a time. That parser is a state machine: it does not recurse, and it constructs nothing.
 * There is no tree of nodes and no loader behind this, so there is nothing that a tag could
 * make construct a class, which is what CVE-2022-1471 was in SnakeYAML, and no recursion for
 * a deep document to exhaust.
 *
 * What is refused, each with its line and column:
 *
 *  - '''a document that is too large''', before it is read at all;
 *  - '''too much nesting''';
 *  - '''too many aliases'''. An alias costs nothing here, since the value it stands for is
 *    shared, but whoever walks the JSON afterwards walks it once for every alias: it is how a
 *    short document becomes a huge one (the "billion laughs");
 *  - '''an alias without its anchor''', or inside the collection that its anchor names;
 *  - '''any tag''', `!!str` included: nothing in a configuration needs one;
 *  - '''a key twice in one mapping''';
 *  - '''a key that is not a scalar''', which JSON cannot hold;
 *  - '''no document, or more than one'''.
 *
 * A scalar that is not quoted is read by the JSON schema of YAML 1.2, with the engine's own
 * resolver. `null`, or nothing, is a null; `true` and `false` are booleans; a number is one
 * that JSON has too. Everything else is a string: `yes`, `no`, `on`, `off`, `True`, `~`, `007`,
 * `0x1F`, a date, `${HOME}`. And `<<` is a key like any other, not a merge.
 */
object SafeYaml {

  /**
   * @param maxCodePoints the length of a document. Configuration files are a few kilobytes.
   * @param maxDepth how deep collections may nest. The configuration model nests five or six.
   * @param maxAliases how many aliases a document may have. An alias inside an aliased
   *   collection multiplies: what is walked can double with every two.
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

    final case class UnknownAlias(name: String, at: Option[Position]) extends Refusal {
      override def problem: String =
        s"the alias '*$name' has no anchor before it, or is inside the collection it names"
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

    /** A value that YAML reads as a number and JSON cannot hold: `1.`, `.inf`, `.nan`. */
    final case class NotANumber(text: String, at: Option[Position]) extends Refusal {
      override def problem: String =
        s"'$text' is read as a number that JSON does not have: write it as JSON does, or quote it"
    }

    case object NoDocument extends Refusal {
      override def at: Option[Position] = None
      override def problem: String = "the file has no document"
    }

    /** @param at where the second document starts */
    final case class SeveralDocuments(at: Option[Position]) extends Refusal {
      override def problem: String = "the file has more than one document, and must have one"
    }

    /** The text is not YAML. */
    final case class Malformed(problem: String, at: Option[Position]) extends Refusal
  }

  /** Reads `text` as one YAML document, or says why it was refused. */
  def parse(text: String, limits: Limits = Limits.Default): Either[Refusal, Json] = {
    val codePoints = text.codePointCount(0, text.length)
    if (codePoints > limits.maxCodePoints) Left(Refusal.TooLarge(codePoints, limits.maxCodePoints))
    else new Reading(limits).of(text)
  }

  /** A collection whose end has not come yet. */
  sealed private trait Open {
    def anchor: Option[String]
  }

  final private class OpenMapping(val anchor: Option[String]) extends Open {
    val fields: mutable.Builder[(String, Json), Vector[(String, Json)]] = Vector.newBuilder
    val keys: mutable.Set[String] = mutable.Set.empty

    /** The key whose value comes next; `None` when a key does. */
    var key: Option[String] = None
  }

  final private class OpenSequence(val anchor: Option[String]) extends Open {
    val items: mutable.Builder[Json, Vector[Json]] = Vector.newBuilder
  }

  /** One reading of one document: the state between its events. */
  final private class Reading(limits: Limits) {

    private val resolver = new JsonScalarResolver()
    private val open = mutable.Stack.empty[Open]
    private val anchored = mutable.Map.empty[String, Json]
    private var documents = 0
    private var aliases = 0
    private var root = Option.empty[Json]

    def of(text: String): Either[Refusal, Json] = {
      val settings = LoadSettings.builder().setCodePointLimit(limits.maxCodePoints).build()
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

        first().toLeft(()).flatMap(_ => root.toRight(Refusal.NoDocument))
      } catch {
        case marked: MarkedYamlEngineException =>
          Left(
            Refusal.Malformed(
              Seq(Option(marked.getContext), Option(marked.getProblem)).flatten.mkString(": "),
              marked.getProblemMark.toScala.map(position)
            )
          )
        case other: YamlEngineException =>
          Left(
            Refusal.Malformed(firstLine(Option(other.getMessage).getOrElse(other.toString)), None)
          )
      }
    }

    /** Takes one event in, and says whether the document is refused at it. */
    private def refused(event: Event): Option[Refusal] = event match {
      case _: DocumentStartEvent =>
        documents += 1
        Option.when(documents > 1)(Refusal.SeveralDocuments(position(event)))

      case scalar: ScalarEvent =>
        tagged(scalar.getTag, event).orElse {
          keyExpected match {
            case Some(mapping) =>
              val key = scalar.getValue
              if (!mapping.keys.add(key)) Some(Refusal.DuplicateKey(key, position(event)))
              else {
                mapping.key = Some(key)
                remember(scalar.getAnchor, Json.fromString(key))
                None
              }
            case None =>
              valueOf(scalar) match {
                case Left(refusal) => Some(refusal)
                case Right(value) =>
                  remember(scalar.getAnchor, value)
                  place(value)
                  None
              }
          }
        }

      case alias: AliasEvent =>
        aliases += 1
        val name = alias.getAlias.getValue
        if (aliases > limits.maxAliases)
          Some(Refusal.TooManyAliases(limits.maxAliases, position(event)))
        else if (keyExpected.nonEmpty) Some(Refusal.KeyNotScalar(position(event)))
        else
          anchored.get(name) match {
            case Some(value) =>
              place(value)
              None
            case None => Some(Refusal.UnknownAlias(name, position(event)))
          }

      case start: MappingStartEvent =>
        tagged(start.getTag, event).orElse(opened(new OpenMapping(name(start.getAnchor)), event))

      case start: SequenceStartEvent =>
        tagged(start.getTag, event).orElse(opened(new OpenSequence(name(start.getAnchor)), event))

      case _: MappingEndEvent | _: SequenceEndEvent =>
        val closed = open.pop()
        val value = closed match {
          case mapping: OpenMapping => Json.fromFields(mapping.fields.result())
          case sequence: OpenSequence => Json.fromValues(sequence.items.result())
        }
        // Only now: an alias inside the collection finds no anchor, so nothing contains itself.
        closed.anchor.foreach(anchored.update(_, value))
        place(value)
        None

      case _ => None
    }

    /** The mapping that is open and waits for a key, if the next node is to be one. */
    private def keyExpected: Option[OpenMapping] =
      open.headOption.collect { case mapping: OpenMapping if mapping.key.isEmpty => mapping }

    private def opened(collection: Open, event: Event): Option[Refusal] =
      if (keyExpected.nonEmpty) Some(Refusal.KeyNotScalar(position(event)))
      else {
        open.push(collection)
        Option.when(open.size > limits.maxDepth)(Refusal.TooDeep(limits.maxDepth, position(event)))
      }

    /** Puts a value that is complete where it belongs: in the open collection, or at the top. */
    private def place(value: Json): Unit = open.headOption match {
      case Some(mapping: OpenMapping) =>
        mapping.key.foreach(key => mapping.fields += (key -> value))
        mapping.key = None
      case Some(sequence: OpenSequence) => sequence.items += value
      case None => root = Some(value)
    }

    private def remember(anchor: java.util.Optional[Anchor], value: Json): Unit =
      name(anchor).foreach(anchored.update(_, value))

    private def tagged(tag: java.util.Optional[String], event: Event): Option[Refusal] =
      tag.toScala.map(Refusal.TagNotAllowed(_, position(event)))

    /**
     * A scalar as JSON. One that is quoted, or written as a block, is a string. A plain one is
     * what the JSON schema of YAML 1.2 says, by the engine's own resolver.
     */
    private def valueOf(scalar: ScalarEvent): Either[Refusal, Json] = {
      val text = scalar.getValue
      if (!scalar.isPlain) Right(Json.fromString(text))
      else {
        val tag = resolver.resolve(text, java.lang.Boolean.TRUE)
        if (tag == Tag.NULL) Right(Json.Null)
        else if (tag == Tag.BOOL) Right(Json.fromBoolean(text == "true"))
        else if (tag == Tag.INT || tag == Tag.FLOAT)
          JsonNumber
            .fromString(text)
            .map(Json.fromJsonNumber)
            .toRight(Refusal.NotANumber(text, position(scalar)))
        else Right(Json.fromString(text))
      }
    }
  }

  private def name(anchor: java.util.Optional[Anchor]): Option[String] =
    anchor.toScala.map(_.getValue)

  private def position(event: Event): Option[Position] = event.getStartMark.toScala.map(position)

  private def position(mark: Mark): Position = Position(mark.getLine + 1, mark.getColumn + 1)

  /** The parser's messages go on with a snippet of the document; the first line is the problem. */
  private def firstLine(message: String): String =
    message.linesIterator.nextOption().getOrElse("").trim
}
