package lakehouse.ingestion.cli

import java.io.PrintStream
import java.nio.file.Path

import lakehouse.ingestion.core.config._
import lakehouse.ingestion.engine.control.{ControlDatabase, DatabaseFailure}
import lakehouse.ingestion.logstore.{StartupGuard, StartupRefusedException}
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.spark.sql.SparkSession

/**
 * What a command on one source stands on, as `run` and an operator's action each need it: the
 * configuration, the source, the control plane, a session of Spark on the coordinated LogStore.
 * What can be refused is refused before anything costly is started, the cheapest first, and each
 * refusal ends in the code that says what to do with it (discovery §14.1):
 *
 *  1. the whole configuration, validated as `validate` does (4);
 *  1. the source, which must be in it (4). One that is disabled is a pause that somebody chose:
 *     `run` ends well with nothing opened (0), and an operator may act on it, which is how a
 *     source is mended while it is paused;
 *  1. for a command that reads the source, a connector for its kind in this application (4);
 *  1. the roots that the command needs, which must be there: the engine makes none, since a root
 *     that is missing is more often a disk that is not mounted (4). The lake for every command;
 *     the landing zone for one that reads the source;
 *  1. the time zone of the JVM, which is set to UTC before a database is connected to or a
 *     session is made, and which only a JVM that cannot be set so is refused for (4);
 *  1. the control plane, reached and at its version: not reached (3), or refused (4);
 *  1. the session of Spark, with no setting from outside the profile that a profile would be
 *     refused for (4), and the startup guard of the LogStore: a coordination database that is
 *     not reached (3), anything else (4).
 *
 * The session and the control plane are closed however the command ends. An exception that
 * nothing classified is let through after that.
 */
private[cli] object SourceCommand {

  /** What a command has, once nothing was refused. */
  final case class Ready(
      profile: Profile,
      source: Source,
      config: ConfigVersion,
      database: ControlDatabase,
      spark: SparkSession
  )

  /**
   * @param reads whether the command reads the source. It needs the connector of the source and
   *   the landing zone
   * @param operator whether the command is an operator's action, which acts on a source that is
   *   disabled; `run` does nothing with one
   * @param done what the command would have done, as a refusal says that it did not: "run",
   *   "changed"
   * @param waits the waits of the LogStore that the session is given (`RunSession.engine`): none,
   *   but in a launcher of the tests
   */
  def apply(
      profilePath: Path,
      sourcesPath: Path,
      id: SourceId,
      secrets: SecretResolver,
      out: PrintStream,
      err: PrintStream,
      reads: Boolean,
      operator: Boolean,
      done: String,
      waits: RunSession.LogStoreWaits = RunSession.NoWaits
  )(body: Ready => Int): Int = {
    val nothing = s"Nothing was $done."
    ConfigLoader.load(profilePath, sourcesPath) match {
      case Left(errors) =>
        errors.toChain.toVector.foreach(error => err.println(error.message))
        err.println(s"The configuration is not valid, and nothing was $done.")
        Main.Exit.Permanent

      case Right(Loaded(configuration, warnings)) =>
        warnings.foreach(warning => err.println(s"warning: ${warning.message}"))
        val profile = configuration.profile
        configuration.sources.find(_.id == id) match {
          case None =>
            err.println(
              s"error: there is no source ${id.value} in $sourcesPath, and nothing was $done. " +
                s"The sources are: ${configuration.sources.map(_.id.value).sorted.mkString(", ")}"
            )
            Main.Exit.Permanent

          case Some(source) if !operator && !source.enabled =>
            out.println(
              s"The source ${id.value} is disabled (enabled: false), and was not run: nothing " +
                "was opened, read or recorded. Set enabled to true to run it again."
            )
            Main.Exit.Ok

          case Some(source) =>
            val refused =
              Option
                .when(reads)(
                  RunCommand.withoutConnector(source, RunCommand.connectors(profile, secrets))
                )
                .flatten
                .toSeq ++ RunCommand.missingRoots(profile, landing = reads) ++
                // Before a database is connected to: a connection takes the zone of the JVM.
                RunSession.jvmToUtc().left.toSeq
            if (refused.nonEmpty) {
              refused.foreach(problem => err.println(s"error: $problem"))
              err.println(nothing)
              Main.Exit.Permanent
            } else
              ControlDatabase.open(profile.databases.control, secrets) match {
                case Left(failure) =>
                  err.println(s"error: ${failure.message}")
                  err.println(nothing)
                  failure match {
                    case _: DatabaseFailure.Unreachable => Main.Exit.Transient
                    case _: DatabaseFailure.Refused => Main.Exit.Permanent
                  }
                case Right(database) =>
                  try withSession(profile, source, database, err, nothing, waits)(body)
                  finally database.close()
              }
        }
    }
  }

  /** The command, in a session of its own, which is stopped however it ends. */
  private def withSession(
      profile: Profile,
      source: Source,
      database: ControlDatabase,
      err: PrintStream,
      nothing: String,
      waits: RunSession.LogStoreWaits
  )(body: Ready => Int): Int = {
    val engine = RunSession.engine(profile, source.id, waits)
    val spark = RunSession.open(RunSession.settings(profile, source.id, waits))
    try {
      val effective = spark.sparkContext.getConf.getAll.toMap ++ spark.conf.getAll
      val foreign = RunSession.foreign(effective, engine)
      if (foreign.nonEmpty) {
        foreign.foreach { case (key, reason) =>
          err.println(
            s"error: the Spark session has $key from outside the profile (spark-defaults.conf, " +
              s"a system property of the JVM, or --conf), and it $reason"
          )
        }
        err.println(nothing)
        Main.Exit.Permanent
      } else
        guarded(spark, profile, err, nothing).getOrElse {
          body(Ready(profile, source, ConfigVersion.of(profile, source), database, spark))
        }
    } finally RunSession.close(spark)
  }

  /**
   * The startup guard of the LogStore, with the Hadoop configuration that Delta would hand it.
   *
   * @return the exit code, if the process may not start
   */
  private def guarded(
      spark: SparkSession,
      profile: Profile,
      err: PrintStream,
      nothing: String
  ): Option[Int] =
    try {
      StartupGuard.require(
        spark.sessionState.newHadoopConf(),
        new HadoopPath(profile.lake.uri.value)
      )
      None
    } catch {
      case refused: StartupRefusedException =>
        refused.problems.foreach(problem => err.println(s"error: $problem"))
        refused.coordinationFailure.foreach { failure =>
          err.println(s"error: the coordination database said: ${DatabaseFailure.said(failure)}")
        }
        err.println(nothing)
        Some(RunCommand.exitOfRefusal(refused))
    }
}
