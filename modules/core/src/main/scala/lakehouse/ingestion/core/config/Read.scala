package lakehouse.ingestion.core.config

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

import cats.data.{Chain, NonEmptyChain, Validated, ValidatedNec}
import cats.syntax.all._
import io.circe.{Json, JsonObject}

/**
 * Reads a configuration out of the JSON of its file into the types of the model, and keeps
 * every mistake it meets on the way (ADR-0007: validation accumulates all errors).
 *
 * Two things the decoders of a library would not give:
 *
 *  - '''a mapping knows its fields'''. A field that was not asked for is an error, with the
 *    field that was probably meant. A misspelt `watermark` must not become a full load that
 *    nobody asked for;
 *  - '''an error has the path of its field''' and words that say what is expected, since the
 *    one who reads it wrote YAML, not Scala.
 */
private[config] object Read {

  /** A mistake at one field of the file. */
  final case class Problem(field: String, problem: String)

  type Result[A] = ValidatedNec[Problem, A]

  def valid[A](value: A): Result[A] = Validated.validNec(value)

  def invalid[A](field: String, problem: String): Result[A] =
    Validated.invalidNec(Problem(field, problem))

  /** The top of a file, which must be a mapping. */
  def document[A](json: Json)(body: Obj => Result[A]): Result[A] =
    json.asObject match {
      case Some(fields) => new Obj("", fields).read(body)
      case None => invalid("the file", s"must be a mapping of fields, and is ${kind(json)}")
    }

  /** A value somewhere in the file. */
  final class Node private[Read] (val field: String, json: Json) {

    def fail[A](problem: String): Result[A] = invalid(field, problem)

    def string: Result[String] =
      json.asString.fold(fail[String](s"must be text, and is ${kind(json)}"))(valid)

    /** Text that a grammar turns into a type of the model; the grammar says what is wrong. */
    def text[A](parse: String => Either[String, A]): Result[A] =
      string.andThen(text => parse(text).fold(fail, valid))

    def boolean: Result[Boolean] =
      json.asBoolean.fold(fail[Boolean](s"must be true or false, and is ${kind(json)}"))(valid)

    def duration: Result[FiniteDuration] = json.asString match {
      case Some(text) => Durations.parse(text).fold(fail, valid)
      case None if json.isNumber => fail("has no unit: write it as 500ms, 30s, 10m, 2h or 7d")
      case None => fail(s"must be a duration such as 30s, and is ${kind(json)}")
    }

    /** A duration within bounds, both of which a field of the configuration may reach. */
    def duration(min: FiniteDuration, max: FiniteDuration): Result[FiniteDuration] =
      duration.andThen { length =>
        if (length < min)
          fail(
            s"is ${Durations.render(length)}, and the shortest it may be is ${Durations.render(min)}"
          )
        else if (length > max)
          fail(
            s"is ${Durations.render(length)}, and the longest it may be is ${Durations.render(max)}"
          )
        else valid(length)
      }

    /** A setting that is passed on as text: text, or a number or a boolean as it was written. */
    def scalarText: Result[String] =
      json.asString
        .orElse(json.asNumber.map(_.toString))
        .orElse(json.asBoolean.map(_.toString))
        .fold(fail[String](s"must be a single value, and is ${kind(json)}"))(valid)

    /**
     * A reference to a secret, `{ secretRef: "env:NAME" }`. Text where the reference belongs is
     * not read at all: it is most likely the secret, and nothing here repeats it.
     */
    def secret: Result[SecretRef] =
      if (json.isObject) obj(_.required("secretRef")(_.text(SecretRef.parse)))
      else
        fail(
          "must be a reference to a secret, as { secretRef: \"env:NAME\" } or " +
            "{ secretRef: \"file:/run/secrets/name\" }, and never the secret itself"
        )

    /** One of a few words. */
    def oneOf[A](choices: (String, A)*): Result[A] = {
      val words = choices.map(_._1)
      val expected = s"must be one of ${words.mkString(", ")}"
      json.asString match {
        case Some(word) =>
          choices.toMap.get(word).fold(fail[A](expected + suggestion(word, words)))(valid)
        case None => fail(s"$expected, and is ${kind(json)}")
      }
    }

    def obj[A](body: Obj => Result[A]): Result[A] = json.asObject match {
      case Some(fields) => new Obj(field, fields).read(body)
      case None => fail(s"must be a mapping of fields, and is ${kind(json)}")
    }

    def list[A](item: Node => Result[A]): Result[Vector[A]] = json.asArray match {
      case Some(items) =>
        items.zipWithIndex.traverse { case (value, index) =>
          item(new Node(s"$field[$index]", value))
        }
      case None => fail(s"must be a list, and is ${kind(json)}")
    }

    /** A mapping whose keys are not fields of the model but names that the writer chose. */
    def entries[A](entry: (String, Node) => Result[A]): Result[Vector[A]] = json.asObject match {
      case Some(fields) =>
        fields.toVector.traverse { case (key, value) =>
          entry(key, new Node(child(field, key), value))
        }
      case None => fail(s"must be a mapping, and is ${kind(json)}")
    }
  }

  /** A mapping whose fields the model knows. */
  final class Obj private[Read] (val field: String, fields: JsonObject) {

    private val asked = mutable.LinkedHashSet.empty[String]

    /** The path of a field of this mapping, for a rule that is about more than one field. */
    def path(name: String): String = child(field, name)

    def required[A](name: String)(read: Node => Result[A]): Result[A] =
      present(name).fold(invalid[A](path(name), "is missing"))(read)

    /** A field that may be left out, or left empty. */
    def optional[A](name: String)(read: Node => Result[A]): Result[Option[A]] =
      present(name).traverse(read)

    def orElse[A](name: String, default: => A)(read: Node => Result[A]): Result[A] =
      optional(name)(read).map(_.getOrElse(default))

    private def present(name: String): Option[Node] = {
      asked += name
      fields(name).filterNot(_.isNull).map(new Node(path(name), _))
    }

    /** Runs `body`, and then refuses every field that `body` did not ask for. */
    private[Read] def read[A](body: Obj => Result[A]): Result[A] = {
      val result = body(this)
      val unknown = Chain.fromSeq(fields.keys.toSeq.filterNot(asked)).map { name =>
        Problem(path(name), s"is not a field that is known here${suggestion(name, asked.toSeq)}")
      }
      NonEmptyChain
        .fromChain(unknown)
        .fold(result)(more => result <* Validated.invalid[NonEmptyChain[Problem], Unit](more))
    }
  }

  private def child(field: String, name: String): String =
    if (field.isEmpty) name else s"$field.$name"

  private def kind(json: Json): String =
    json.fold(
      "nothing",
      _ => "true or false",
      _ => "a number",
      _ => "text",
      _ => "a list",
      _ => "a mapping"
    )

  /** The word that was probably meant: the closest, if it is one or two edits away. */
  private def suggestion(written: String, known: Seq[String]): String =
    known
      .map(word => word -> distance(written, word))
      .filter { case (_, edits) => edits <= 2 }
      .minByOption(_._2)
      .fold("")(closest => s": did you mean '${closest._1}'?")

  /** The edit distance of Levenshtein. */
  private def distance(a: String, b: String): Int = {
    val last = a.indices.foldLeft((0 to b.length).toVector) { (previous, i) =>
      b.indices.foldLeft(Vector(i + 1)) { (row, j) =>
        val substitution = previous(j) + (if (a(i) == b(j)) 0 else 1)
        row :+ math.min(math.min(row(j) + 1, previous(j + 1) + 1), substitution)
      }
    }
    last.last
  }
}
