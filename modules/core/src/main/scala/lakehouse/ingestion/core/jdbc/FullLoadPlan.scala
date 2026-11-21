package lakehouse.ingestion.core.jdbc

import cats.syntax.all._
import io.circe.Json
import lakehouse.ingestion.core.StoredForm
import lakehouse.ingestion.core.config.QualifiedName

/**
 * The plan of a batch of a full load of a table (ADR-0005 §3): the table, whole. It is what
 * the write-ahead log keeps of the batch, and what a run that replays it reads again.
 *
 * It names the table and nothing of what is in it: a full load is one statement, and so one
 * snapshot of its table, taken when the statement starts (discovery §17, P6). A replay reads
 * the table as it is by then, and not as the run that planned it would have: nothing of that
 * run is in Bronze, or the batch is rolled forward and nothing is read.
 *
 * The plan names the table so that a replay under a configuration with another table is
 * refused by its connector, as a plan of files is for its directory: the batch was planned to
 * be a snapshot of this one.
 *
 * @param table the table, as the configuration of the source names it
 */
final case class FullLoadPlan(table: QualifiedName) {

  def toJson: Json = Json.obj(
    "version" -> Json.fromInt(FullLoadPlan.Version),
    "table" -> Json.fromString(table.toString)
  )
}

object FullLoadPlan {

  /** The form of the document. A document of another version is not read (`fromJson`). */
  val Version = 1

  /**
   * Reads a plan back as it was written.
   *
   * @return the plan, or what is wrong with the document
   */
  def fromJson(json: Json): Either[String, FullLoadPlan] = {
    val top = json.hcursor
    for {
      _ <- StoredForm.exactly(top, "version", "table")
      _ <- StoredForm.version(top, Version)
      text <- top.get[String]("table").leftMap(_ => "table is not text")
      table <- QualifiedName.parse(text).leftMap(problem => s"table $problem")
    } yield FullLoadPlan(table)
  }
}
