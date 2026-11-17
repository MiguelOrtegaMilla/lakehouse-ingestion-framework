package lakehouse.ingestion.core.config

/**
 * The name of a column, a table or a schema that configuration gave (ADR-0013 §1).
 *
 * The grammar is an allow-list: a letter of ASCII or an underscore, then letters, digits and
 * underscores, 63 bytes at most. Every name that configuration gives is parsed into this type
 * when the configuration is validated, so no text that is not one reaches a connector.
 *
 * A name that needs quotes to be one, with a space or a hyphen, is refused. Identifiers are
 * always quoted when they are used, so a wider grammar would be safe as well: it is narrow
 * because nothing has needed more, and widening it is a change of validation, not of security.
 *
 * Case is kept as written. A quoted identifier is case-sensitive in PostgreSQL, so `Orders`
 * and `orders` are two names.
 */
final class SqlIdentifier private (val value: String) extends AnyVal {
  override def toString: String = value
}

object SqlIdentifier {

  /** PostgreSQL's `NAMEDATALEN - 1`. The grammar is ASCII, so bytes are characters. */
  val MaxBytes = 63

  private def isLetter(c: Int): Boolean =
    (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_'
  private def isDigit(c: Int): Boolean = c >= '0' && c <= '9'

  /** `Left` says what is wrong, to follow the name of the field: "is empty", "starts with…". */
  def parse(text: String): Either[String, SqlIdentifier] =
    if (text.isEmpty) Left("is empty")
    else
      Unwritten.firstOutside(text, c => isLetter(c) || isDigit(c)) match {
        case Some((codePoint, position)) =>
          Left(
            s"has ${Unwritten.describe(codePoint)} at position $position: a name is letters " +
              "of ASCII, digits and underscores"
          )
        case None if isDigit(text.codePointAt(0)) =>
          Left("starts with a digit: a name starts with a letter or an underscore")
        case None if text.length > MaxBytes =>
          Left(s"is ${text.length} bytes long, and a name is $MaxBytes at most")
        case None => Right(new SqlIdentifier(text))
      }
}

/**
 * A table as configuration names it: its name, or its schema and its name. No catalog, and no
 * more than one dot (ADR-0013 §1).
 */
final case class QualifiedName(schema: Option[SqlIdentifier], name: SqlIdentifier) {
  override def toString: String = (schema.toSeq :+ name).mkString(".")
}

object QualifiedName {

  def parse(text: String): Either[String, QualifiedName] =
    text.split("\\.", -1).toList match {
      case name :: Nil => SqlIdentifier.parse(name).map(QualifiedName(None, _))
      case schema :: name :: Nil =>
        for {
          schema <- SqlIdentifier.parse(schema).left.map(reason => s"has a schema that $reason")
          name <- SqlIdentifier.parse(name).left.map(reason => s"has a table name that $reason")
        } yield QualifiedName(Some(schema), name)
      case parts => Left(s"has ${parts.size} parts: a table is a name, or schema.name")
    }
}
