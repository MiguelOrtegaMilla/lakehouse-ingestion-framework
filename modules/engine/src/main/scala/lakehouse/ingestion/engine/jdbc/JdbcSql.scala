package lakehouse.ingestion.engine.jdbc

import lakehouse.ingestion.core.config.QualifiedName

/**
 * The text that the connector writes into a statement of a source (ADR-0013 §1).
 *
 * '''A statement is made of names, and of nothing else.''' A table is named by identifiers
 * that the grammar of the configuration let through (`SqlIdentifier`), and a column by the
 * name that the source itself gave for it; each is written quoted, with a quote in it doubled,
 * so that whatever a name holds it is one name. No value is ever written into a statement: the
 * bounds of a batch reach Spark as predicates on a `DataFrame`, and Spark writes them
 * (discovery §17, P6.2).
 *
 * Spark writes a table into its statements as it was given, and quotes none (pinned by
 * `SparkJdbcContractSpec`), so what it is given is what is made here.
 */
object JdbcSql {

  /** A name as one identifier, whatever it holds. */
  def quoted(name: String): String = "\"" + name.replace("\"", "\"\"") + "\""

  /** A table, with its schema if the configuration names one. */
  def table(name: QualifiedName): String =
    (name.schema.toSeq :+ name.name).map(part => quoted(part.value)).mkString(".")

  /**
   * What the source says the time is, which the bounds of a batch of a watermark are times of
   * (ADR-0020). A constant: nothing of a source, and nothing of its configuration, is in it.
   */
  val Clock = "SELECT CURRENT_TIMESTAMP"

  /**
   * A statement over a table that gives no row: what its columns are is in its metadata. It is
   * what Spark asks before a read, so the two are told the same.
   */
  def noRows(name: QualifiedName): String = s"SELECT * FROM ${table(name)} WHERE 1=0"

  /**
   * The rows of a table, each with one column more: the place in `mayBeInfinite`, from 1, of
   * the first of those columns that has `infinity` or `-infinity` in the row, and null if none
   * has. It is the source that says so, in the statement that gives the row: once Spark has
   * read the value, it is a date like any other.
   *
   * It is a query in the place of a table, as Spark takes one (`dbtable`).
   *
   * @param mayBeInfinite the columns to ask of, which are dates and timestamps; one at least
   * @param flag the name of the column that is added, which is no column of the table
   */
  def flagged(name: QualifiedName, mayBeInfinite: Seq[String], flag: String): String = {
    require(mayBeInfinite.nonEmpty, "no column to ask of")
    val source = quoted(SourceAlias)
    val cases = mayBeInfinite.zipWithIndex.map { case (column, index) =>
      s"WHEN NOT isfinite($source.${quoted(column)}) THEN ${index + 1}"
    }
    s"(SELECT $source.*, CASE ${cases.mkString(" ")} END AS ${quoted(flag)} " +
      s"FROM ${table(name)} AS $source) AS ${quoted(ReadAlias)}"
  }

  private val SourceAlias = "_lakehouse_source"
  private val ReadAlias = "_lakehouse_read"
}
