package lakehouse.ingestion.core

import io.circe.ACursor

/**
 * What reads back a document that the control plane stores, of a form that has a version: the
 * plan of a batch, the watermark of a source. Such a document outlives the process that wrote
 * it, and is read as it was written, or not at all: a field that is missing, one too many, or a
 * number written as text is a document of another form, and guessing at it would read a batch
 * that nobody planned.
 */
private[core] object StoredForm {

  /**
   * A whole number written as one. circe's decoders take a number written as text as well, and
   * a document is read as it was written.
   */
  def whole(at: ACursor): Option[Long] = at.focus.flatMap(_.asNumber).flatMap(_.toLong)

  /** The fields of an object, these and no other. `Left` says which is missing, or is more. */
  def exactly(at: ACursor, fields: String*): Either[String, Unit] =
    at.keys.map(_.toSet) match {
      case None => Left("is not an object")
      case Some(keys) if keys == fields.toSet => Right(())
      case Some(keys) =>
        val missing = fields.filterNot(keys)
        val more = keys.toSeq.filterNot(fields.contains).sorted
        Left(
          (missing.map(name => s"has no $name") ++ more.map(name => s"has a field $name"))
            .mkString(", ")
        )
    }

  /**
   * The version of a form, which is the one that this application reads.
   *
   * @param version the one form that is read
   */
  def version(at: ACursor, version: Int): Either[String, Unit] =
    whole(at.downField("version")).toRight("version is not a whole number").flatMap { written =>
      Either.cond(
        written == version,
        (),
        s"is of version $written, and this application reads version $version"
      )
    }
}
