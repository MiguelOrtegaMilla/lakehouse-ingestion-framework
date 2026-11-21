package lakehouse.ingestion.cli

import java.io.PrintStream
import java.net.URI
import java.nio.file.{Files, Path, Paths}

import lakehouse.ingestion.core.config._
import lakehouse.ingestion.core.control.{FailureClass, LeaseTiming}
import lakehouse.ingestion.engine.IngestionService
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.connector.ConnectorRegistry
import lakehouse.ingestion.engine.control.{ControlDatabase, DatabaseFailure}
import lakehouse.ingestion.engine.files.{FileSourceConnector, Landing}
import lakehouse.ingestion.engine.jdbc.JdbcSourceConnector
import lakehouse.ingestion.logstore.{StartupGuard, StartupRefusedException}
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.spark.sql.SparkSession

/**
 * The command `run`: one run of one source into Bronze, as an orchestrator starts it (a process
 * runs one source in P5). What can be refused is refused before anything costly is started,
 * the cheapest first, and each refusal ends in the code that says what to do with it
 * (discovery §14.1):
 *
 *  1. the whole configuration, validated as `validate` does (4);
 *  1. the source, which must be in it (4); one that is disabled is a pause that somebody chose,
 *     and the run ends well with nothing opened (0);
 *  1. a connector for its kind in this application (4);
 *  1. the roots of the lake and of the landing zone, which must be there: the engine makes
 *     neither, since a root that is missing is more often a disk that is not mounted (4);
 *  1. the time zone of the JVM, which is set to UTC before a database is connected to or a
 *     session is made, and which only a JVM that cannot be set so is refused for (4);
 *  1. the control plane, reached and at its version: not reached (3), or refused (4);
 *  1. the session of Spark, with no setting from outside the profile that a profile would be
 *     refused for (4), and the startup guard of the LogStore: a coordination database that is
 *     not reached (3), anything else (4);
 *  1. the run itself, whose outcome is its code (`exitOf`).
 *
 * An exception that nothing classified is let through, after the session and the control plane
 * are closed: the process ends with the JVM's 1 until `Main` catches it (P8, 70).
 */
object RunCommand {

  /**
   * The connectors of the application: the one place where a kind of source is added to it
   * (RNF-04).
   */
  def connectors(profile: Profile, secrets: SecretResolver): ConnectorRegistry =
    ConnectorRegistry(
      new FileSourceConnector(new Landing(profile.landing.root)),
      new JdbcSourceConnector(secrets)
    )

  def run(
      profilePath: Path,
      sourcesPath: Path,
      id: SourceId,
      secrets: SecretResolver,
      out: PrintStream,
      err: PrintStream
  ): Int =
    ConfigLoader.load(profilePath, sourcesPath) match {
      case Left(errors) =>
        errors.toChain.toVector.foreach(error => err.println(error.message))
        err.println("The configuration is not valid, and nothing was run.")
        Main.Exit.Permanent

      case Right(Loaded(configuration, warnings)) =>
        warnings.foreach(warning => err.println(s"warning: ${warning.message}"))
        val profile = configuration.profile
        configuration.sources.find(_.id == id) match {
          case None =>
            err.println(
              s"error: there is no source ${id.value} in $sourcesPath, and nothing was run. " +
                s"The sources are: ${configuration.sources.map(_.id.value).sorted.mkString(", ")}"
            )
            Main.Exit.Permanent

          case Some(source) if !source.enabled =>
            out.println(
              s"The source ${id.value} is disabled (enabled: false), and was not run: nothing " +
                "was opened, read or recorded. Set enabled to true to run it again."
            )
            Main.Exit.Ok

          case Some(source) =>
            val registry = connectors(profile, secrets)
            val refused =
              withoutConnector(source, registry).toSeq ++ missingRoots(profile) ++
                // Before a database is connected to: a connection takes the zone of the JVM.
                RunSession.jvmToUtc().left.toSeq
            if (refused.nonEmpty) {
              refused.foreach(problem => err.println(s"error: $problem"))
              err.println("Nothing was run.")
              Main.Exit.Permanent
            } else
              ControlDatabase.open(profile.databases.control, secrets) match {
                case Left(failure) =>
                  err.println(s"error: ${failure.message}")
                  err.println("Nothing was run.")
                  failure match {
                    case _: DatabaseFailure.Unreachable => Main.Exit.Transient
                    case _: DatabaseFailure.Refused => Main.Exit.Permanent
                  }
                case Right(database) =>
                  try withSession(profile, source, registry, database, out, err)
                  finally database.close()
              }
        }
    }

  /** The run, in a session of its own, which is stopped however the run ends. */
  private def withSession(
      profile: Profile,
      source: Source,
      registry: ConnectorRegistry,
      database: ControlDatabase,
      out: PrintStream,
      err: PrintStream
  ): Int = {
    val engine = RunSession.engine(profile, source.id)
    val spark = RunSession.open(RunSession.settings(profile, source.id))
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
        err.println("Nothing was run.")
        Main.Exit.Permanent
      } else
        guarded(spark, profile, err).getOrElse {
          val service = new IngestionService(
            database,
            registry,
            spark,
            profile.lake.uri,
            LeaseTiming(profile.leases.ttl)
          )
          val outcome = service.run(source, ConfigVersion.of(profile, source))
          val (stream, line) = report(source.id, outcome)
          (if (stream == Stream.Out) out else err).println(line)
          exitOf(outcome)
        }
    } finally RunSession.close(spark)
  }

  /**
   * The startup guard of the LogStore, with the Hadoop configuration that Delta would hand it.
   *
   * @return the exit code, if the process may not start
   */
  private def guarded(spark: SparkSession, profile: Profile, err: PrintStream): Option[Int] =
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
        err.println("Nothing was run.")
        Some(exitOfRefusal(refused))
    }

  /**
   * Why a source cannot be run by an application with these connectors, if it has none for the
   * kind of the source: a kind whose configuration is read, and whose connector was not
   * registered (`connectors`).
   */
  def withoutConnector(source: Source, registry: ConnectorRegistry): Option[String] =
    Option.when(!registry.kinds.exists(_.name == source.connector.kind))(
      s"the source ${source.id.value} is of kind '${source.connector.kind}', and this version " +
        "of the application has no connector for it: it has " +
        registry.kinds.map(kind => s"'${kind.name}'").mkString(", ")
    )

  /**
   * A coordination database that was not reached may be reached the next time (3), when it is
   * all that is wrong. Anything else that the guard finds is in the configuration of the
   * process, or in the database, and running again does not mend it (4).
   */
  def exitOfRefusal(refused: StartupRefusedException): Int =
    if (refused.problems.size == 1 && refused.coordinationFailure.exists(DatabaseFailure.mayPass))
      Main.Exit.Transient
    else Main.Exit.Permanent

  /**
   * The roots of a profile that are not there. Only a root on a local disk is looked at here:
   * a lake on object storage is the startup guard's to try, and a landing zone there is
   * refused when it is listed (E2).
   */
  def missingRoots(profile: Profile): Seq[String] =
    Seq("lake.uri" -> profile.lake.uri, "landing.root" -> profile.landing.root).collect {
      case (field, root) if root.isLocal && !Files.isDirectory(Paths.get(URI.create(root.value))) =>
        s"the root of $field, ${root.value}, is not a directory: it is not there, or is not a " +
          "directory. The engine does not make it, since a root that is missing is more often " +
          "a disk that is not mounted: make it once, by hand"
    }

  /** The code of a run, by how it ended (discovery §14.1). */
  def exitOf(outcome: Outcome): Int = outcome match {
    case _: Outcome.NothingNew | _: Outcome.Ingested | _: Outcome.RolledForward => Main.Exit.Ok
    case _: Outcome.Skipped => Main.Exit.SkippedLocked
    case _: Outcome.GivenUp => Main.Exit.Transient
    case Outcome.Failed(_, failure, _) =>
      failure match {
        case FailureClass.Transient | FailureClass.Conflict => Main.Exit.Transient
        case FailureClass.Permanent => Main.Exit.Permanent
        case FailureClass.Data => Main.Exit.DataThreshold
      }
  }

  sealed private[cli] trait Stream
  private[cli] object Stream {
    case object Out extends Stream
    case object Err extends Stream
  }

  /** One line for a run, and whether it is said as what the command did or as an error. */
  private[cli] def report(source: SourceId, outcome: Outcome): (Stream, String) = {
    val run = s"The run ${outcome.run} of ${source.value}"
    outcome match {
      case Outcome.NothingNew(_) =>
        Stream.Out -> s"$run found nothing new, and ended well with no batch."
      case Outcome.Ingested(_, batch, outputs, counts, _) =>
        Stream.Out -> (s"$run committed the batch $batch: ${counts.rowsRead.getOrElse(0L)} " +
          s"rows read, ${counts.rowsWritten.getOrElse(0L)} written, " +
          s"${counts.rowsRejected.getOrElse(0L)} rejected${written(outputs)}.")
      case Outcome.RolledForward(_, batch, outputs, _) =>
        Stream.Out -> (s"$run committed the batch $batch, which a run before it had written " +
          s"and not committed: nothing was read${written(outputs)}.")
      case Outcome.Skipped(_, holder, until) =>
        Stream.Out -> (s"$run was skipped: the run $holder holds the lease of the source, " +
          s"until $until at the latest. Nothing was read or written.")
      case Outcome.GivenUp(_, batch, reason) =>
        Stream.Err -> (s"error: $run gave up the batch $batch: $reason. The next run plans " +
          "anew.")
      case Outcome.Failed(_, failure, message) =>
        Stream.Err -> s"error: $run failed ($failure): $message"
    }
  }

  private def written(outputs: Seq[lakehouse.ingestion.core.control.BatchOutput]): String =
    if (outputs.isEmpty) ", into no table"
    else
      outputs
        .map(output => s"${output.table}${output.version.fold("")(v => s" at version $v")}")
        .mkString(", into ", " and ", "")
}
