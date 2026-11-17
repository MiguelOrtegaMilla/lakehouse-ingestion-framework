package lakehouse.ingestion.core.config

/**
 * Where a table is: the one URI that every process of every instance opens it by (ADR-0011).
 *
 * It is made by a `TableLocator` and by nothing else. A source names its tables by a path
 * relative to the lake, and never by a URI of its own, so two sources that mean one table
 * cannot spell it two ways.
 */
final class TableLocation private[config] (val uri: String) {

  override def equals(other: Any): Boolean = other match {
    case that: TableLocation => that.uri == uri
    case _ => false
  }

  override def hashCode: Int = uri.hashCode

  override def toString: String = uri
}

/**
 * From the name a source gives a table to the place of the table.
 *
 * A trait, because a table need not be a path: one that a catalog manages would be found by its
 * name, with another locator and the same engine (ADR-0011 §1).
 */
trait TableLocator {
  def locate(table: RelativePath): TableLocation
}

/**
 * Tables under the lake of a profile: the root of the lake, and the path of the table after it.
 *
 * It works on text alone, and that is enough for what it promises:
 *
 *  - '''one table, one URI'''. The root is written one way (`RootUri`) and so is the path
 *    (`RelativePath`), so there is nothing left to normalise when the two are joined;
 *  - '''every table is under the lake'''. A relative path has no `..`, no scheme and no `/` in
 *    front: a location outside the root cannot be made, whatever a source says.
 *
 * What text cannot tell is a symbolic link, in the root or under it. That is the LogStore's to
 * resolve, when it makes the key that commits to the table are coordinated under
 * (`TableIdentity`; ADR-0011, note of 2026-11-17). A test holds this locator to that key for
 * a lake without links.
 */
final class PathTableLocator(lake: RootUri) extends TableLocator {

  override def locate(table: RelativePath): TableLocation =
    new TableLocation(s"${lake.value}/${table.value}")
}
