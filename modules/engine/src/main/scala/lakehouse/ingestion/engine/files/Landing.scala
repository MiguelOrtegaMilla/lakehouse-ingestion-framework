package lakehouse.ingestion.engine.files

import java.io.IOException
import java.net.URI
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, FileSystemException, LinkOption, NoSuchFileException, Path, Paths}
import java.time.Instant

import scala.jdk.CollectionConverters._
import scala.util.Using

import lakehouse.ingestion.core.config.{FileConnectorConfig, RootUri}
import lakehouse.ingestion.core.files.{FileName, FilePlan, LandedFile, Listed}
import lakehouse.ingestion.engine.failure.RunFailedException

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
 * another over it, or changes a link. A plan keeps each file by its size and its time, as
 * `processed_file` does. Just before Spark reads the files, each must still have them, and which
 * file of the file system it is (`FileIdentity`) is taken then, from the same look; just after,
 * each must still be that file, with that size and that time (`confirm`, `confirmAgain`, P5.7).
 * What that leaves open is said where the read is (`FileSourceConnector`).
 * A hard link is a file like any other, and Linux lets a process make one only to a file that
 * it owns or may write (`fs.protected_hardlinks`).
 *
 * '''What no run may open''' is a file that leads out of the root, a link to nothing or round a
 * loop, and what is not a regular file (a pipe, a socket, a device). Under `fail`, the default,
 * such a file fails the listing of its source, and nothing is planned. Under `skip` it is
 * listed, by what the file system says of the name itself and not of where it leads, planned as
 * any file, and found again just before the read, as refused, for its batch to be given up and
 * the file recorded (ADR-0023). Either way it is never opened, nor is anything looked at beyond
 * the root.
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
   * A file that no run may open is listed too, under `skip`, by the size and the time of the
   * name itself: a link by its own, and not by those of what it leads to.
   *
   * @return the files, in no order
   * @throws RunFailedException a permanent failure, for a root or a directory that is not there
   *   or is not a directory, a directory outside the root, and, under `fail`, every file of the
   *   source that no run may open: one outside the root, a link to nothing, something that is
   *   not a file. A file is never passed over without a word: a source that skipped one would
   *   lose its data the same way (discovery §17, P5)
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

    val problems = looked.collect { case (name, Landing.Look.Refused(why, _)) =>
      s"'$name' ${why.problem}"
    }
    if (problems.nonEmpty && config.onUnreadableFile == FileConnectorConfig.OnUnreadableFile.Fail)
      throw RunFailedException.permanent(
        s"the directory ${config.path} of the landing zone has ${Landing.files(problems.size)} " +
          "that no run may read, and nothing was planned, as the source fails for such a file " +
          s"(connector.on_unreadable_file): ${Landing.some(problems.sorted)}"
      )
    looked.map {
      case (name, Landing.Look.Readable(stat)) => stat.listed(name)
      case (name, Landing.Look.Refused(_, own)) => own.listed(name)
    }
  }

  /**
   * Where the files of a plan are, just before they are read, if each of its names still leads to
   * a file of the size and the time that were planned, under the root; and which file of the file
   * system each is, from the same look, for `confirmAgain` to compare.
   *
   * Which file it is was not planned: a plan outlives the process that made it, and a restart of
   * the machine or of its container may number the same disk otherwise (`FileIdentity`).
   *
   * A file that no run may open, and that has the size and the time that were planned for its
   * name, is as it was planned: it is found as refused, and is not to be read
   * (`Confirmed.refused`).
   *
   * @return the files of the plan, in its directory as it is resolved, in the order of the plan;
   *   or what is not as it was planned, for each name that is not, in that order
   * @throws RunFailedException a permanent failure, for a root or a directory that is not there,
   *   is not a directory, or is outside the root
   * @throws IllegalArgumentException if the plan is of another directory than the source's
   */
  @throws[IOException]
  def confirm(
      config: FileConnectorConfig,
      plan: FilePlan
  ): Either[Vector[Landing.NotAsPlanned], Landing.Confirmed] =
    looks(config, plan)((_, _) => None).map(found => new Landing.Confirmed(plan, found))

  /**
   * Whether the files of a plan are still, just after they were read, those that `confirm` found
   * just before: the same file of the file system, of the size and the time that were planned.
   *
   * @return what is not as it was, for each name that is not
   * @throws IllegalArgumentException if what was confirmed is of another plan
   */
  @throws[IOException]
  def confirmAgain(
      config: FileConnectorConfig,
      plan: FilePlan,
      before: Landing.Confirmed
  ): Either[Vector[Landing.NotAsPlanned], Unit] = {
    require(before.plan == plan, "what was confirmed is of another plan")
    looks(config, plan) { (index, now) =>
      val was = before.identities(index)
      Option.when(now != was)(
        s"is another file of the file system than the one that was read ($now, and was $was)"
      )
    }.map(_ => ())
  }

  /**
   * Each file of a plan as it is now, if every name leads to a file of the planned size and time,
   * which `otherwise` finds nothing against, given its place in the plan and its identity.
   */
  private def looks(config: FileConnectorConfig, plan: FilePlan)(
      otherwise: (Int, FileIdentity) => Option[String]
  ): Either[Vector[Landing.NotAsPlanned], Vector[Landing.Found]] = {
    require(plan.directory == config.path, s"the plan is of ${plan.directory}, not ${config.path}")
    val (rootReal, directory) = locate(config)

    val found = plan.files.zipWithIndex.map { case (file, index) =>
      val name = file.name.value
      val entry = directory.resolve(name)
      def asPlanned(stat: Landing.Stat, refused: Option[Landing.Refusal]) = {
        val problems = otherwise(index, stat.identity).toSeq ++ changes(file, stat)
        if (problems.isEmpty) Right(Landing.Found(entry, stat.identity, refused))
        else Left(problems.mkString(", "))
      }
      val now = look(entry, name, rootReal) match {
        case None => Left("is not there any more, or is a directory now")
        case Some(Landing.Look.Readable(stat)) => asPlanned(stat, refused = None)
        // Known by the name itself, as it was listed: what it leads to is not looked at. One
        // that was planned as another file says what it is now.
        case Some(Landing.Look.Refused(why, own)) =>
          asPlanned(own, refused = Some(why)).left.map(_ => why.problem)
      }
      now.left.map(Landing.NotAsPlanned(name, _))
    }
    val problems = found.collect { case Left(problem) => problem }
    if (problems.nonEmpty) Left(problems) else Right(found.collect { case Right(file) => file })
  }

  /** How a file is not of the size and the time that were planned, if it is not. */
  private def changes(planned: LandedFile, now: Landing.Stat): Seq[String] = {
    val modifiedAt = Instant.ofEpochMilli(now.modifiedAt)
    Seq(
      Option.when(now.size != planned.size)(s"is of ${now.size} bytes, and was of ${planned.size}"),
      Option.when(modifiedAt != planned.modifiedAt)(
        s"was changed at $modifiedAt, and was of ${planned.modifiedAt}"
      )
    ).flatten
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
   * What an entry of the directory is: a file to list, nothing to list, or one that no run may
   * open, with why and what the file system says of the name itself.
   *
   * @return none for a directory, a link to one, and an entry that is gone since it was listed:
   *   its producer took it back, and it is not the source's any more
   */
  private def look(entry: Path, name: String, rootReal: Path): Option[Landing.Look] =
    stat(entry, LinkOption.NOFOLLOW_LINKS).flatMap { own =>
      def refused(code: String, problem: String) =
        Some(Landing.Look.Refused(Landing.Refusal(code, problem), own))
      if (own.isDirectory) None
      else if (FileName.parse(name).isLeft) Some(Landing.Look.Readable(own))
      else if (!own.isSymbolicLink) file(own).orElse(Some(Landing.Look.Readable(own)))
      else
        real(entry, missing = "is a symbolic link to nothing") match {
          case Left(problem) => refused(Landing.Refusal.BrokenLink, problem)
          case Right(target) =>
            // Whether it leads to a directory, which is not listed, is asked of what the file
            // system says of the target. Nothing is opened, inside the root or out of it, and
            // what a link out of it is listed by is its own.
            stat(target).flatMap { theirs =>
              if (theirs.isDirectory) None
              else if (!target.startsWith(rootReal))
                refused(
                  Landing.Refusal.OutsideTheRoot,
                  "leads out of the landing zone, through a symbolic link"
                )
              else file(theirs).map(_.copy(own = own)).orElse(Some(Landing.Look.Readable(theirs)))
            }
        }
    }

  /** What is not read under a name: what is not a file. None, for a regular file. */
  private def file(stat: Landing.Stat): Option[Landing.Look.Refused] =
    Option.when(!stat.isRegularFile)(
      Landing.Look.Refused(
        Landing.Refusal(
          Landing.Refusal.NotAFile,
          "is not a file but a pipe, a socket or a device: a read of it could wait for ever, " +
            "or never end"
        ),
        stat
      )
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

  /**
   * The files of a plan as they were found just before they were read: where each is, and which
   * file of the file system it was. It lives as long as the read, and is never written down.
   */
  final class Confirmed private[files] (
      private[files] val plan: FilePlan,
      found: Vector[Found]
  ) {

    /** Where each file of the plan is, in its directory as it is resolved, in the plan's order. */
    val paths: Vector[Path] = found.map(_.path)

    /** Which file of the file system each was, in the plan's order. */
    val identities: Vector[FileIdentity] = found.map(_.identity)

    /**
     * The files of the plan that no run may open, each with why, in the plan's order. A plan
     * that has one is not read: none of its files is opened.
     */
    val refused: Vector[(LandedFile, Refusal)] =
      plan.files.zip(found).collect { case (file, Found(_, _, Some(why))) => file -> why }
  }

  /** A file of a plan, as a look found it as it was planned. */
  final private[files] case class Found(
      path: Path,
      identity: FileIdentity,
      refused: Option[Refusal]
  )

  /** What a look found under a name that it lists. */
  sealed private[files] trait Look
  private[files] object Look {

    /** A file to read: what the file system says of it, through a link if it is one. */
    final case class Readable(stat: Stat) extends Look

    /** A name that no run may open, with why, and what the file system says of the name. */
    final case class Refused(why: Refusal, own: Stat) extends Look
  }

  /**
   * Why no run may open a name of a directory of the landing zone (ADR-0013 §2, ADR-0023 §2).
   *
   * @param code what a record of the file says it is, as a word: `path_outside_root`,
   *   `broken_link` or `not_a_file`
   * @param problem for a person, to follow the name of the file. Never where a link leads
   */
  final case class Refusal(code: String, problem: String) {
    override def toString: String = s"$code: it $problem, and was not opened"
  }

  object Refusal {
    val OutsideTheRoot = "path_outside_root"
    val BrokenLink = "broken_link"
    val NotAFile = "not_a_file"
  }

  /**
   * A file of a plan that a look did not find as it was planned.
   *
   * @param name the file, by its name in the directory of its source
   * @param how what is not as it was planned: its size, its time, which file it is, or that it
   *   is not there. Never where a link leads
   */
  final case class NotAsPlanned(name: String, how: String) {
    override def toString: String = s"'$name' $how"
  }

  final private[files] case class Stat(
      isDirectory: Boolean,
      isRegularFile: Boolean,
      isSymbolicLink: Boolean,
      size: Long,
      modifiedAt: Long,
      identity: FileIdentity
  ) {
    def listed(name: String): Listed = Listed(name, size, Instant.ofEpochMilli(modifiedAt))
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
