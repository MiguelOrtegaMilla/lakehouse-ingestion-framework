package lakehouse.ingestion.core.files

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant

import cats.data.NonEmptyChain
import cats.syntax.all._
import io.circe.{ACursor, Json}
import lakehouse.ingestion.core.config.{FileConnectorConfig, RelativePath, Unwritten}

/**
 * A file as the listing of a directory gives it: its name in the directory, its size in bytes,
 * and when it was last changed. Nothing has been judged of it yet.
 *
 * @param modifiedAt to the millisecond, as file systems report it to Hadoop
 */
final case class Listed(name: String, size: Long, modifiedAt: Instant) {
  require(size >= 0, s"a file has no size $size")
  require(LandedFile.toTheMillisecond(modifiedAt), s"$modifiedAt is not to the millisecond")
}

/**
 * The name of a file that a source may take: one name in its directory, as the producer wrote
 * it, with what would be read as something else left out.
 *
 *  - '''No `/`''': a name is in its directory, and a source lists one directory.
 *  - '''No `* ? [ ] { } \`''': Spark reads a path that has one as a pattern, and would read
 *    the files that it matches instead of the file of that name.
 *  - '''No `:`''': Hadoop reads a name with one as a URI with a scheme.
 *  - '''No control, format or separator character''', and no half of a pair: a name is shown
 *    in logs and in errors, and such a character is one that a terminal obeys or that hides
 *    what is written.
 *  - '''Not hidden''': a name that starts with `.` or `_` is a file that its producer is still
 *    writing, or a marker of its own (`_SUCCESS`). Spark and Delta pass over such names, and so
 *    does a listing: they are never a source's.
 *  - '''255 bytes at most''' in UTF-8, as a name is on the file systems of Linux.
 *
 * Every other character is taken, spaces and letters of any script included: the names are the
 * producer's, and refusing what is merely unusual would stop a source for nothing.
 */
final class FileName private (val value: String) extends AnyVal {
  override def toString: String = value
}

object FileName {

  val MaxBytes = 255

  /** Whether a name is one that a listing passes over, without a word. */
  def hidden(name: String): Boolean = name.startsWith(".") || name.startsWith("_")

  /** `Left` says what is wrong, to follow "the name of the file". It does not repeat the name. */
  def parse(name: String): Either[String, FileName] =
    if (name.isEmpty) Left("is empty")
    else if (name == "." || name == "..") Left("is a directory, its own or the one above it")
    else if (hidden(name))
      Left(s"starts with '${name.head}': such names are hidden to Spark and Delta")
    else
      Unwritten.firstOutside(name, allowed) match {
        case Some((codePoint, position)) =>
          Left(s"has ${Unwritten.describe(codePoint)} at position $position: ${why(codePoint)}")
        case None if name.getBytes(UTF_8).length > MaxBytes =>
          Left(s"is ${name.getBytes(UTF_8).length} bytes long, and a name is $MaxBytes at most")
        case None => Right(new FileName(name))
      }

  private val Read = "*?[]{}\\"

  private def allowed(codePoint: Int): Boolean =
    codePoint != '/' && codePoint != ':' && Read.indexOf(codePoint) < 0 &&
      !(Character.getType(codePoint) match {
        case Character.CONTROL | Character.FORMAT | Character.LINE_SEPARATOR |
            Character.PARAGRAPH_SEPARATOR | Character.SURROGATE =>
          true
        case _ => false
      })

  private def why(codePoint: Int): String = codePoint match {
    case '/' => "a name is in its directory, and a source lists one directory"
    case ':' => "Hadoop reads a name with it as a URI with a scheme"
    case c if Read.indexOf(c) >= 0 =>
      "Spark reads a path with it as a pattern, and would read other files"
    case _ => "it is a character that a terminal obeys, or that hides what is written"
  }
}

/**
 * A file that a batch takes, and what tells it from every other: its name, its size and when it
 * was last changed (discovery §8). A file that is written again under the same name is another
 * file, and is taken again, whole: Bronze keeps what was received (ADR-0008).
 */
final case class LandedFile(name: FileName, size: Long, modifiedAt: Instant) {
  require(size >= 0, s"a file has no size $size")
  require(LandedFile.toTheMillisecond(modifiedAt), s"$modifiedAt is not to the millisecond")
}

object LandedFile {

  /** What a file system reports, and what is written down: a finer time would not read back. */
  private[files] def toTheMillisecond(at: Instant): Boolean = at.getNano % 1000000 == 0
}

/**
 * The plan of a batch of files (ADR-0005 §3): the directory, and the files of it that the batch
 * takes, in the order they are read. It is written down before any of them is read, and a run
 * that replays it reads these files and no other.
 *
 * It has a file at least: a run with nothing new plans no batch.
 *
 * @param directory relative to the landing root of the profile
 */
final case class FilePlan private (directory: RelativePath, files: Vector[LandedFile]) {

  /** The files that it takes, by their names: how they are found among those processed. */
  def names: Vector[FileName] = files.map(_.name)

  def bytes: Long = files.map(_.size).sum

  /** As the control plane stores it, in the write-ahead log of batches. */
  def toJson: Json = Json.obj(
    "version" -> Json.fromInt(FilePlan.Version),
    "directory" -> Json.fromString(directory.value),
    "files" -> Json.fromValues(files.map { file =>
      Json.obj(
        "name" -> Json.fromString(file.name.value),
        "size" -> Json.fromLong(file.size),
        "modified_at" -> Json.fromString(file.modifiedAt.toString)
      )
    })
  )
}

object FilePlan {

  /**
   * The form of the JSON. A plan outlives the process that wrote it, by a crash, and may be
   * replayed by another version of the application: one that reads another form refuses it.
   */
  val Version = 1

  /** @throws IllegalArgumentException if it has no file, or a name twice */
  def apply(directory: RelativePath, files: Vector[LandedFile]): FilePlan = {
    require(files.nonEmpty, "a plan has a file at least")
    require(files.map(_.name).distinct.size == files.size, "a plan has each name once")
    new FilePlan(directory, files)
  }

  /**
   * Reads a plan back as it was written.
   *
   * @return the plan, or what is wrong with the document. A name is never repeated
   */
  def fromJson(json: Json): Either[String, FilePlan] = {
    val top = json.hcursor
    for {
      _ <- exactly(top, "version", "directory", "files")
      version <- whole(top.downField("version")).toRight("version is not a whole number")
      _ <- Either.cond(
        version == Version,
        (),
        s"is of version $version, and this application reads version $Version"
      )
      directoryText <- top.get[String]("directory").leftMap(_ => "directory is not text")
      directory <- RelativePath.parse(directoryText).leftMap(problem => s"directory $problem")
      entries <- top.downField("files").values.toRight("files is not a list")
      files <- entries.toVector.zipWithIndex.traverse { case (entry, index) => file(entry, index) }
      plan <- Either
        .catchOnly[IllegalArgumentException](FilePlan(directory, files))
        .leftMap(_.getMessage.stripPrefix("requirement failed: "))
    } yield plan
  }

  private def file(entry: Json, index: Int): Either[String, LandedFile] = {
    val at = entry.hcursor
    def field[A](name: String, problem: String)(read: ACursor => Either[Any, A]) =
      read(at.downField(name)).leftMap(_ => s"files[$index].$name $problem")
    for {
      _ <- exactly(at, "name", "size", "modified_at").leftMap(problem => s"files[$index] $problem")
      text <- field("name", "is not text")(_.as[String])
      name <- FileName.parse(text).leftMap(problem => s"files[$index].name $problem")
      size <- whole(at.downField("size"))
        .filter(_ >= 0)
        .toRight(s"files[$index].size is not a size")
      modifiedAt <- field("modified_at", "is not a time")(_.as[String].flatMap { text =>
        Either.catchNonFatal(Instant.parse(text))
      }).filterOrElse(
        LandedFile.toTheMillisecond,
        s"files[$index].modified_at is not to the millisecond"
      )
    } yield LandedFile(name, size, modifiedAt)
  }

  /**
   * A whole number written as one. circe's decoders take a number written as text as well, and a
   * plan is read as it was written, or not at all.
   */
  private def whole(at: ACursor): Option[Long] = at.focus.flatMap(_.asNumber).flatMap(_.toLong)

  /** The fields of an object, these and no other: a plan is read as it was written, or not. */
  private def exactly(at: ACursor, fields: String*): Either[String, Unit] =
    at.keys.map(_.toSet) match {
      case None => Left("is not an object")
      case Some(keys) if keys == fields.toSet => Right(())
      case Some(keys) =>
        val missing = fields.filterNot(keys)
        val more = keys.toSeq.filterNot(fields.contains).sorted
        Left(
          (missing.map(name => s"has no $name") ++ more.map(name => s"has a field $name"))
            .mkString(", ")
        )
    }
}

/**
 * Which files a batch of a file source takes (ADR-0005 §3, step 2): a function of what the
 * directory lists, of what was processed, and of the time, with no I/O of its own. So it is the
 * same, file for file, whoever runs it with the same three, and is tested without a directory.
 */
object FilePlanner {

  /** What a run is to do. */
  sealed trait Planning {

    /** Files that are new and not complete yet: younger than `min_file_age`. */
    def waiting: Int
  }

  /** No new file is complete: no batch is planned, and the run ends well. */
  final case class NothingNew(waiting: Int) extends Planning

  /**
   * @param leftOver files that are new and complete, beyond `max_files_per_batch`: the next
   *   run's
   */
  final case class Planned(plan: FilePlan, waiting: Int, leftOver: Int) extends Planning

  /**
   * @param listed what the directory of the source lists, in any order
   * @param processed the files of the directory that batches have taken. Others may be there
   *   too, and are not looked at
   * @param now when the run plans: a file is complete once it is `min_file_age` older than this
   * @return what to do, or the names of files that the source would take and that no run can
   *   read. They are said, by their place in the listing and by the character that is in the
   *   way, and never passed over: a source that would skip a file without a word would lose
   *   its data the same way (discovery §17, P5)
   * @throws IllegalArgumentException if the listing has a name twice
   */
  def plan(
      config: FileConnectorConfig,
      listed: Seq[Listed],
      processed: Set[LandedFile],
      now: Instant
  ): Either[NonEmptyChain[String], Planning] = {
    require(listed.map(_.name).distinct.size == listed.size, "a listing has each name once")

    // Oldest first, and by name for files of one moment: an order that no listing changes.
    val ofTheSource = listed
      .filterNot(file => FileName.hidden(file.name))
      .filter(file => config.pattern.forall(_.matches(file.name)))
      .sortBy(file => (file.modifiedAt, file.name))

    val named = ofTheSource.zipWithIndex.traverse { case (file, index) =>
      FileName
        .parse(file.name)
        .map(LandedFile(_, file.size, file.modifiedAt))
        .leftMap { problem =>
          s"the file of ${config.path} that is number ${index + 1} of those it would take, " +
            s"oldest first (of ${file.size} bytes, changed at ${file.modifiedAt}), has a name " +
            s"that $problem"
        }
        .toValidatedNec
    }

    // Every name that is in the way, and not the first alone.
    named.toEither.map { files =>
      val fresh = files.filterNot(processed)
      val oldEnough = now.minusMillis(config.minFileAge.toMillis)
      val complete = fresh.filterNot(_.modifiedAt.isAfter(oldEnough))
      val waiting = fresh.size - complete.size
      val taken = complete.take(config.maxFilesPerBatch).toVector

      if (taken.isEmpty) NothingNew(waiting)
      else Planned(FilePlan(config.path, taken), waiting, complete.size - taken.size)
    }
  }
}
