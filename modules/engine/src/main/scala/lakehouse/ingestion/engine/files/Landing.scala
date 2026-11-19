package lakehouse.ingestion.engine.files

import java.io.IOException
import java.net.URI
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, FileSystemException, LinkOption, NoSuchFileException, Path, Paths}
import java.time.Instant

import scala.jdk.CollectionConverters._
import scala.util.Using

import lakehouse.ingestion.core.config.{FileConnectorConfig, RootUri}
import lakehouse.ingestion.core.files.{FileIdentity, FileName, FilePlan, Listed, PlannedFile}
import lakehouse.ingestion.engine.RunFailedException

/**
 * The landing zone of a profile as it is on disk, and the one rule about it: what a file
 * source reads is under the root of the landing zone (ADR-0013 §2).
 *
 * The directory of a source is under the root by its text: a source cannot name `..`, nor a
 * path of its own (`RelativePath`). What text cannot tell is a symbolic link, which anyone who
 * may leave files in the landing zone may leave as well, and which may lead anywhere the
 * process can read: `/etc`, another source's directory under another root, the lake. So the
 * directory and every file of it that the source would take are resolved on the file system,
 * links and all, and what they resolve to must be under what the root resolves to.
 *
 * The root itself may be a link, as a landing zone on a disk that is mounted elsewhere: it is
 * where the link leads that files must be under. A link inside the landing zone to another place
 * inside it is let be, and the file is read through it.
 *
 * A name may lead to another file at any moment: a producer that writes a file again, renames
 * another over it, or changes a link. So each file is listed with which file of the file system
 * it is (`FileIdentity`), its size and its time, all from one look at it, and the plan keeps
 * them. A read of the plan confirms them just before Spark reads the files and again just after
 * (`confirm`, P5.7). What that leaves open is said where the read is (`FileSourceConnector`).
 * A hard link is a file like any other, and Linux lets a process make one only to a file that
 * it owns or may write (`fs.protected_hardlinks`).
 *
 * The root is `file://`. A landing zone on object storage has no links, and is read with the
 * deployment that has one (E2).
 */
final class Landing(root: RootUri) {

  /**
   * The files of the directory of a source that the source would take: not hidden, and of its
   * pattern. Directories are not listed, nor the links to them, and neither is what is under
   * them. A file reached through a link inside the landing zone is listed under its own name,
   * with the size and the time of the file that the link leads to: what a read reads.
   *
   * A name that no run can read (`FileName`) is listed as it is, unresolved, for the planner to
   * refuse, by its place and not by the name (`FilePlanner`).
   *
   * @return the files, in no order
   * @throws RunFailedException a permanent failure, for a root or a directory that is not there
   *   or is not a directory, a directory outside the root, and every file of the source that no
   *   run can read: one outside the root, a link to nothing, something that is not a file. A
   *   file is never passed over without a word: a source that skipped one would lose its data
   *   the same way (discovery §17, P5; the policy that may skip one is P8's)
   * @throws java.io.IOException if the file system cannot be read, as for lack of permission:
   *   the classification of errors is P8's
   */
  @throws[IOException]
  def list(config: FileConnectorConfig): Vector[Listed] = {
    val (rootReal, directory) = locate(config)

    val entries = Using.resource(Files.newDirectoryStream(directory))(_.asScala.toVector)
    val looked = entries.flatMap { entry =>
      val name = entry.getFileName.toString
      if (FileName.hidden(name) || !config.pattern.forall(_.matches(name))) None
      else look(entry, name, rootReal).map(name -> _)
    }

    val problems = looked.collect { case (name, Left(problem)) => s"'$name' $problem" }
    if (problems.nonEmpty)
      throw RunFailedException.permanent(
        s"the directory ${config.path} of the landing zone has ${Landing.files(problems.size)} " +
          s"that no run may read, and nothing was planned: ${Landing.some(problems.sorted)}"
      )
    looked.collect { case (_, Right(listed)) => listed }
  }

  /**
   * Where the files of a plan are, if each of its names still leads to the file that was planned:
   * the same file of the file system, of the same size and the same time, under the root.
   *
   * @return the path of each file of the plan, in its directory as it is resolved, in the order
   *   of the plan; or what is not as it was planned, for each name that is not
   * @throws RunFailedException a permanent failure, for a root or a directory that is not there,
   *   is not a directory, or is outside the root
   * @throws IllegalArgumentException if the plan is of another directory than the source's
   */
  @throws[IOException]
  def confirm(config: FileConnectorConfig, plan: FilePlan): Either[Vector[String], Vector[Path]] = {
    require(plan.directory == config.path, s"the plan is of ${plan.directory}, not ${config.path}")
    val (rootReal, directory) = locate(config)

    val found = plan.planned.map { planned =>
      val name = planned.name.value
      val entry = directory.resolve(name)
      val now = look(entry, name, rootReal)
        .getOrElse(Left("is not there any more, or is a directory now"))
        .flatMap(listed => changes(planned, listed).toLeft(entry))
      now.left.map(problem => s"'$name' $problem")
    }
    val problems = found.collect { case Left(problem) => problem }
    if (problems.nonEmpty) Left(problems) else Right(found.collect { case Right(path) => path })
  }

  /** How a file is not the one that was planned, if it is not. */
  private def changes(planned: PlannedFile, now: Listed): Option[String] = {
    val was = planned.file
    Seq(
      Option.when(now.identity != planned.identity)(
        s"is another file of the file system (${now.identity}, and was ${planned.identity})"
      ),
      Option.when(now.size != was.size)(s"is of ${now.size} bytes, and was of ${was.size}"),
      Option.when(now.modifiedAt != was.modifiedAt)(
        s"was changed at ${now.modifiedAt}, and was of ${was.modifiedAt}"
      )
    ).flatten match {
      case Seq() => None
      case some => Some(some.mkString(", "))
    }
  }

  /** The real root, and the real directory of the source, which is under it. */
  private def locate(config: FileConnectorConfig): (Path, Path) = {
    if (!root.isLocal)
      throw RunFailedException.permanent(
        s"the landing zone is at $root, and this deployment reads one on file:// alone"
      )
    val rootPath = Paths.get(new URI(root.value))
    val rootReal = real(rootPath, missing = "is not there").fold(
      problem =>
        throw RunFailedException.permanent(s"the root of the landing zone, $root, $problem"),
      identity
    )
    if (!Files.isDirectory(rootReal))
      throw RunFailedException.permanent(s"the root of the landing zone, $root, is not a directory")

    val named = config.path.segments.foldLeft(rootPath)(_.resolve(_))
    val directory = real(named, missing = "is not there, or is a symbolic link to nothing").fold(
      problem =>
        throw RunFailedException.permanent(
          s"the directory ${config.path} of the landing zone $problem"
        ),
      identity
    )
    if (!directory.startsWith(rootReal))
      throw RunFailedException.permanent(
        s"the directory ${config.path} of the landing zone leads out of it, through a symbolic " +
          "link: nothing was read"
      )
    if (!Files.isDirectory(directory))
      throw RunFailedException.permanent(
        s"the directory ${config.path} of the landing zone is not a directory"
      )
    (rootReal, directory)
  }

  /**
   * What an entry of the directory is: a file to list, nothing to list, or what is wrong.
   *
   * @return none for a directory, a link to one, and an entry that is gone since it was listed:
   *   its producer took it back, and it is not the source's any more
   */
  private def look(entry: Path, name: String, rootReal: Path): Option[Either[String, Listed]] =
    stat(entry, LinkOption.NOFOLLOW_LINKS).flatMap { own =>
      if (own.isDirectory) None
      else if (FileName.parse(name).isLeft) Some(Right(own.listed(name)))
      else if (!own.isSymbolicLink) Some(file(name, own))
      else
        real(entry, missing = "is a symbolic link to nothing") match {
          case Left(problem) => Some(Left(problem))
          case Right(target) =>
            stat(target).flatMap { theirs =>
              if (theirs.isDirectory) None
              else if (!target.startsWith(rootReal))
                Some(Left("leads out of the landing zone, through a symbolic link"))
              else Some(file(name, theirs))
            }
        }
    }

  /** What is read under a name: a file, and nothing else. */
  private def file(name: String, stat: Landing.Stat): Either[String, Listed] =
    if (stat.isRegularFile) Right(stat.listed(name))
    else
      Left(
        "is not a file but a pipe, a socket or a device: a read of it could wait for ever, or " +
          "never end"
      )

  /**
   * The path with every link in it resolved, or why it leads nowhere: to what is not there, or
   * round a loop of links. The JDK says the second with a `FileSystemException` of its own class
   * and no other, whose reason it writes itself and does not translate. A denial of access is
   * let through, as every failure of the file system is (P8).
   */
  private def real(path: Path, missing: String): Either[String, Path] =
    try Right(path.toRealPath())
    catch {
      case _: NoSuchFileException => Left(missing)
      case e: FileSystemException if e.getClass == classOf[FileSystemException] =>
        Left(s"is a symbolic link that cannot be followed: ${e.getReason}")
    }

  /**
   * What the file system says of a path, all of it from one `stat`, so that the size, the time
   * and the identity are of one moment: three looks could each see another file.
   */
  private def stat(path: Path, options: LinkOption*): Option[Landing.Stat] =
    try {
      val attrs = Files.readAttributes(path, Landing.Attributes, options: _*)
      Some(
        Landing.Stat(
          isDirectory = attrs.get("isDirectory").asInstanceOf[Boolean],
          isRegularFile = attrs.get("isRegularFile").asInstanceOf[Boolean],
          isSymbolicLink = attrs.get("isSymbolicLink").asInstanceOf[Boolean],
          size = attrs.get("size").asInstanceOf[Long],
          modifiedAt = attrs.get("lastModifiedTime").asInstanceOf[FileTime].toMillis,
          identity = FileIdentity(
            attrs.get("dev").asInstanceOf[Long],
            attrs.get("ino").asInstanceOf[Long]
          )
        )
      )
    } catch { case _: NoSuchFileException => None }
}

object Landing {

  final private[files] case class Stat(
      isDirectory: Boolean,
      isRegularFile: Boolean,
      isSymbolicLink: Boolean,
      size: Long,
      modifiedAt: Long,
      identity: FileIdentity
  ) {
    def listed(name: String): Listed =
      Listed(name, size, Instant.ofEpochMilli(modifiedAt), identity)
  }

  /** The attributes of the `unix` view that a look reads, which Linux and macOS have. */
  private val Attributes =
    "unix:isDirectory,isRegularFile,isSymbolicLink,size,lastModifiedTime,dev,ino"

  private[files] def files(count: Int): String = if (count == 1) "1 file" else s"$count files"

  /** How many problems a failure names: a directory may have thousands, and a log one line. */
  val Named = 10

  private[files] def some(problems: Seq[String]): String =
    if (problems.size <= Named) problems.mkString("; ")
    else s"${problems.take(Named).mkString("; ")}; and ${problems.size - Named} more"
}
