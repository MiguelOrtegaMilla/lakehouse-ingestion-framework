package lakehouse.ingestion.core.state

import java.time.Instant

import io.circe.Json
import lakehouse.ingestion.core.StoredForm

/**
 * How far a source has been ingested by its watermark column, as the control plane stores it
 * (`source_state.watermark`): every row whose column is at or before `value`, and that was
 * there to be read when its window was, is in Bronze (ADR-0020).
 *
 * It is the upper bound of the last batch that was committed, and never a value that a batch
 * happened to read. It only moves forward.
 */
final case class StoredWatermark(value: Instant) {
  Moment.problem(value).foreach(what => throw new IllegalArgumentException(s"a watermark $what"))

  /** As the control plane stores it. */
  def toJson: Json = Json.obj(
    "version" -> Json.fromInt(StoredWatermark.Version),
    "value" -> Moment.write(value)
  )
}

object StoredWatermark {

  /**
   * The form of the JSON. A watermark outlives every process: an application that reads
   * another form refuses it, and does not go on from a moment that it guessed.
   */
  val Version = 1

  /** @return the watermark, or what is wrong with the document */
  def fromJson(json: Json): Either[String, StoredWatermark] = {
    val top = json.hcursor
    for {
      _ <- StoredForm.exactly(top, "version", "value")
      _ <- StoredForm.version(top, Version)
      value <- Moment.read(top, "value")
    } yield StoredWatermark(value)
  }

  /**
   * The watermark that an operator's reset sets a source to (ADR-0021 §3): every row whose
   * column is at or before `at` is taken as ingested, and the next batch counts from it.
   *
   * A moment before `start` is refused. The rows at or before `start` are not of the regular
   * sequence of the source, which starts there, and a backfill reads them (ADR-0021 §5). A
   * moment equal to it is where a source with no watermark starts. A moment that the source has
   * not reached is not refused: the clock of the source is not asked, and the runs after the
   * reset plan nothing until it is passed, and say how far behind it the clock is.
   *
   * @param start `load.watermark.start` of the source
   * @return the watermark, or why `at` cannot be one
   */
  def reset(at: Instant, start: Option[Instant]): Either[String, StoredWatermark] =
    Moment.problem(at) match {
      case Some(what) => Left(s"the moment $what")
      case None =>
        start.filter(at.isBefore) match {
          case Some(from) =>
            Left(
              s"$at is before load.watermark.start, $from: the rows at or before it are not " +
                "of the regular sequence of the source, which starts there, and a backfill " +
                "reads them"
            )
          case None => Right(StoredWatermark(at))
        }
    }
}
