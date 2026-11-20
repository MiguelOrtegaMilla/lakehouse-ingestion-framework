package lakehouse.ingestion.logstore

import java.io.IOException
import java.util.UUID

import lakehouse.ingestion.logstore.coordination.{CoordinationSettings, CoordinationStore}
import lakehouse.ingestion.logstore.storage.{LogStorage, PublishResult}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * A process may not start: its configuration would not coordinate the tables it writes.
 *
 * @param coordinationFailure what the coordination database failed with, when that is one of
 *   the problems. It is the cause of the exception as well. This library does not say whether
 *   a failure may pass: the application that starts it does, by its own rules, and tells a
 *   database that was not reached from one that answered and said no
 */
final class StartupRefusedException(
    val problems: Seq[String],
    val coordinationFailure: Option[IOException] = None
) extends IOException(
      problems.mkString("the LogStore is not set up for this process:\n  - ", "\n  - ", ""),
      coordinationFailure.orNull
    )

/**
 * The first safeguard against writing a coordinated table without coordination (ADR-0004 §3.5,
 * layer 1): what a process checks about itself before it touches a table. It catches the
 * realistic mistake, which is one of our own processes started with a wrong configuration.
 *
 * A library function: the entry point of an application calls it at startup, with the Hadoop
 * configuration Spark builds, and refuses to run on any problem. What it cannot see from here
 * is which LogStore Delta resolves for a path, since this module does not see Delta: a process
 * that has Delta asks it too.
 */
object StartupGuard {

  /** Delta's setting that names one LogStore for every scheme, overriding the ones by scheme. */
  val LogStoreClassKey = "spark.delta.logStore.class"

  /** Delta's setting that names the LogStore of `scheme`. */
  def logStoreKey(scheme: String): String = s"spark.delta.logStore.$scheme.impl"

  /**
   * Everything that is wrong with `conf` for a process that reads and writes tables under
   * `lakeRoot`. Empty if the process may start.
   *
   *  - Delta is told to use `PostgresLogStore` for the lake's scheme.
   *  - The LogStore has a storage for the lake: its scheme is one it implements, and `file:` is
   *    served by Hadoop's own local file system.
   *  - The lake root can hold a coordinated log: a file is written there durably and published
   *    under a second name, which on a local disk is a hard link (ADR-0015).
   *  - The coordination settings are usable, the database answers, and its schema is the
   *    version this code works with (scenario F13).
   *  - No fault hook setting is unreadable.
   *
   * @param conf the configuration a LogStore of this process would be given
   */
  def problems(conf: Configuration, lakeRoot: Path): Seq[String] =
    problems(conf, lakeRoot, PostgresLogStore.sharedCoordination, LogStorage.of)

  /**
   * Throws if `problems` finds any, with all of them, and with what the coordination database
   * failed with if it is one of them.
   */
  @throws[StartupRefusedException]
  def require(conf: Configuration, lakeRoot: Path): Unit =
    require(conf, lakeRoot, PostgresLogStore.sharedCoordination, LogStorage.of)

  private[logstore] def require(
      conf: Configuration,
      lakeRoot: Path,
      coordinationOf: Configuration => CoordinationStore,
      storageOf: FileSystem => LogStorage
  ): Unit = {
    val found = findings(conf, lakeRoot, coordinationOf, storageOf)
    if (found.nonEmpty)
      throw new StartupRefusedException(
        found.map(_.problem),
        found.collectFirst { case Finding(_, Some(failure)) => failure }
      )
  }

  private[logstore] def problems(
      conf: Configuration,
      lakeRoot: Path,
      coordinationOf: Configuration => CoordinationStore,
      storageOf: FileSystem => LogStorage
  ): Seq[String] = findings(conf, lakeRoot, coordinationOf, storageOf).map(_.problem)

  /** A problem, with what the coordination database failed with, if that is the problem. */
  final private case class Finding(problem: String, coordinationFailure: Option[IOException])

  private def problem(text: String): Finding = Finding(text, None)

  private def findings(
      conf: Configuration,
      lakeRoot: Path,
      coordinationOf: Configuration => CoordinationStore,
      storageOf: FileSystem => LogStorage
  ): Seq[Finding] =
    try {
      val fs = lakeRoot.getFileSystem(conf)
      val storage =
        try Right(storageOf(fs))
        catch { case e: UnsupportedTableLocationException => Left(e.getMessage) }

      (logStoreProblem(conf, fs.getUri.getScheme).toSeq ++
        storage.left.toSeq ++
        storage.toOption.flatMap(lakeRootProblem(_, fs, lakeRoot))).map(problem) ++
        coordinationProblems(conf, coordinationOf, storage.toOption.map(_ => (lakeRoot, fs))) ++
        faultHookProblem(conf).map(problem)
    } catch {
      // The lake root itself cannot be looked at: no file system for it, for instance.
      case e: IOException =>
        Seq(problem(s"the lake root $lakeRoot cannot be reached: ${e.getMessage}"))
    }

  private def logStoreProblem(conf: Configuration, scheme: String): Option[String] = {
    val expected = classOf[PostgresLogStore].getName
    val forEveryScheme = Option(conf.get(LogStoreClassKey))
    val forThisScheme = Option(conf.get(logStoreKey(scheme)))
    forEveryScheme.orElse(forThisScheme) match {
      case Some(`expected`) => None
      case Some(other) =>
        val key = if (forEveryScheme.isDefined) LogStoreClassKey else logStoreKey(scheme)
        Some(
          s"$key is $other: tables on $scheme: would be written without coordination. " +
            s"It must be $expected"
        )
      case None =>
        Some(
          s"${logStoreKey(scheme)} is not set: Delta would write tables on $scheme: with its " +
            s"default LogStore, without coordination. It must be $expected"
        )
    }
  }

  /** Writes a file under the lake root and publishes it, as a commit is; leaves nothing behind. */
  private def lakeRootProblem(storage: LogStorage, fs: FileSystem, root: Path): Option[String] =
    if (!fs.exists(root) || !fs.getFileStatus(root).isDirectory)
      Some(s"the lake root $root does not exist, or is not a directory")
    else {
      val published = new Path(root, s".logstore-probe.${UUID.randomUUID()}")
      val temp = published.suffix(".tmp")
      try {
        storage.writeTemp(temp, Iterator("a probe of the startup guard"))
        storage.publish(temp, published) match {
          case PublishResult.Published => None
          case other => Some(s"the lake root $root cannot hold a coordinated log: $other")
        }
      } catch {
        case e: IOException =>
          Some(
            s"the lake root $root cannot hold a coordinated log: a file could not be written " +
              s"there and published under a second name (${e.getMessage})"
          )
      } finally
        Seq(published, temp).foreach { file =>
          try storage.discard(file)
          catch { case _: IOException => () }
        }
    }

  /** The settings, then the database behind them: reached, and at the schema version expected. */
  private def coordinationProblems(
      conf: Configuration,
      coordinationOf: Configuration => CoordinationStore,
      lake: Option[(Path, FileSystem)]
  ): Seq[Finding] =
    CoordinationSettings.from(conf) match {
      case Left(problems) => problems.map(problem)
      case Right(_) =>
        // Any question does: the first one opens the pool and checks the schema version.
        try {
          lake.foreach { case (root, fs) =>
            coordinationOf(conf).registration(TableIdentity.of(root, fs))
          }
          Nil
        } catch {
          case e: IOException =>
            Seq(Finding(s"the coordination database cannot be used: ${e.getMessage}", Some(e)))
        }
    }

  private def faultHookProblem(conf: Configuration): Option[String] =
    try {
      FaultHooks.delaysIn(conf)
      None
    } catch { case e: IOException => Some(e.getMessage) }
}
