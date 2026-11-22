package lakehouse.ingestion.logstore

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * What an application may ask of the registry of the tables that this LogStore coordinates,
 * through the store that its LogStores share (ADR-0004 §3.5).
 *
 * A table that is not registered has no commit that went through this LogStore: the first
 * commit of a table registers it before its version is taken. So the registry tells a table that
 * was never written from one whose directory cannot be read, which the file system alone cannot:
 * a directory that is not there may be of a table never made, or of a disk that is not mounted.
 */
object CoordinatedTables {

  /**
   * Whether the table at `table` is registered, by its canonical key (`TableIdentity`), whatever
   * spelling of its path is given.
   *
   * @throws IOException if the coordination store cannot be asked, or the location is not one
   *   that this LogStore coordinates
   */
  @throws[IOException]
  def registered(conf: Configuration, table: Path): Boolean = {
    val key = TableIdentity.of(table, table.getFileSystem(conf))
    PostgresLogStore.sharedCoordination(conf).registration(key).nonEmpty
  }
}
