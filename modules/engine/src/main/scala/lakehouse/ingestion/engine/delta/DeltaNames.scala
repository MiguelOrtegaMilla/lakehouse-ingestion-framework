package lakehouse.ingestion.engine.delta

/**
 * The names that a table of Delta can give a column.
 *
 * A table without column mapping, as every table of the engine is, refuses a column whose name
 * holds a space, a tab, a line break or any of `,;{}()=`, and takes any other name: quotes,
 * backticks, dots, dashes and letters beyond ASCII among them (pinned by `SqlInjectionSpec`,
 * against Delta itself). A connector that is given such a name by its source refuses it before
 * a batch is planned, as a permanent failure that says which, rather than leave the write of
 * the batch to fail as nothing the engine knows. Column mapping, which takes any name, is the
 * policy of a schema's to decide (P9).
 */
object DeltaNames {

  /** The characters that no name of a column of a table of the engine may hold. */
  val Refused: String = " ,;{}()\n\t="

  /** Whether a table of the engine can have a column of this name. */
  def fits(name: String): Boolean = !name.exists(Refused.contains(_))
}
