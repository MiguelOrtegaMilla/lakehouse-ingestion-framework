package lakehouse.ingestion.engine.delta

import lakehouse.ingestion.core.config.{RootUri, TableLocation}
import lakehouse.ingestion.engine.failure.RunFailedException
import lakehouse.ingestion.logstore.{TableIdentity, UnsupportedTableLocationException}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * The lake of a profile as it is on its storage, and the one rule about it: a table that the
 * engine writes to is under the root of the lake (ADR-0013 §2).
 *
 * The location of a table is made from text (`PathTableLocator`), and by its text it is under
 * the root: a source cannot name `..`, nor a path of its own. What text cannot tell is a
 * symbolic link. A directory of the lake that is a link to some other place would have the
 * engine write there, under a name that looks as one of the lake's. So the location is
 * resolved on the storage, by the one function that does so (`TableIdentity`, which gives the
 * key that the commits of a table are coordinated under), and what it resolves to must be
 * under what the root resolves to.
 *
 * The root itself may be a link, as a lake on a disk that is mounted elsewhere: it is where
 * the link leads that tables must be under. A link inside the lake to another place inside it
 * is let be, since the table has one key by either name.
 *
 * @param conf the Hadoop configuration of the session of the run
 */
final class Lake(root: RootUri, conf: Configuration) {

  /**
   * Refuses a table that is not under the lake, before anything is read for it or written.
   *
   * It is asked when a run starts, for every table that the run may write to, and again by
   * what writes: a link can be made at any time, and asking costs a few calls to the file
   * system.
   *
   * @throws RunFailedException a permanent failure, if the table is outside the lake or its
   *   location cannot be that of a coordinated table
   */
  def confine(table: TableLocation): Unit = {
    val (lake, real) =
      try (resolved(root.value), resolved(table.uri))
      catch {
        case e: UnsupportedTableLocationException =>
          throw RunFailedException.permanent(
            s"the table at ${table.uri} cannot be written to: ${e.getMessage}",
            e
          )
      }
    if (!real.startsWith(under(lake))) {
      // A location that a source names is under the lake by its text, and is out of it only
      // through a link; one that an operator gives may be anywhere (`maintain-table`).
      val named =
        new Path(table.uri).toUri.getPath.startsWith(under(new Path(root.value).toUri.getPath))
      throw RunFailedException.permanent(
        s"the table at ${table.uri} is not in the lake: it is at $real" +
          (if (named) ", through a symbolic link" else "") +
          s", and the lake is $lake. Nothing was written"
      )
    }
  }

  private def under(directory: String): String =
    if (directory.endsWith("/")) directory else s"$directory/"

  /** A location with its symbolic links resolved, as the LogStore keys a table by it. */
  private def resolved(uri: String): String = {
    val path = new Path(uri)
    TableIdentity.of(path, path.getFileSystem(conf)).value
  }
}
