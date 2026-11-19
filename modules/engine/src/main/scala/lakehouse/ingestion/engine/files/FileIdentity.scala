package lakehouse.ingestion.engine.files

/**
 * Which file of a file system a name led to: the device that holds it, and its number there
 * (its inode). A name may be given to another file at any moment, by a rename over it or by a
 * link that is changed; the file that it was keeps these two.
 *
 * They say which file it is on one machine, for as long as the file system stays mounted as it
 * is: the same disk mounted again, after a restart of the machine or of its container, may have
 * another device number. So they are never written down: they are taken just before a read and
 * compared just after it, within one process (`Landing.confirm`).
 */
final case class FileIdentity(device: Long, inode: Long) {
  override def toString: String = s"inode $inode of device $device"
}
