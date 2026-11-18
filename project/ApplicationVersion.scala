import java.io.File

import scala.sys.process._
import scala.util.Try

/**
 * The version of the application as a run records it: the version of the build, and the commit
 * it was built from, as `0.1.0-SNAPSHOT+3f2a9c1d7e4b`. The version alone says little while it
 * is a snapshot: every build of months has the same one.
 *
 * `.dirty` is added when the build had changes that no commit has, so that a run of such a
 * build does not pass for one of the commit. A build with no git to ask, as one from an archive
 * of the sources, has the version of the build and nothing else.
 */
object ApplicationVersion {

  def of(version: String, repository: File): String = {
    val quiet = ProcessLogger(_ => ())
    def git(arguments: String*): Option[String] =
      Try(Process("git" +: arguments, repository).!!(quiet).trim).toOption

    git("rev-parse", "--short=12", "HEAD").filter(_.matches("[0-9a-f]{12}")) match {
      case Some(commit) =>
        val dirty = git("status", "--porcelain").exists(_.nonEmpty)
        s"$version+$commit${if (dirty) ".dirty" else ""}"
      case None => version
    }
  }
}
