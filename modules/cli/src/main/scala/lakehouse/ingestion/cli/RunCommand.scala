package lakehouse.ingestion.cli

import java.io.PrintStream
import java.net.URI
import java.nio.file.{Files, Path, Paths}

import lakehouse.ingestion.core.config._
import lakehouse.ingestion.core.control.{FailureClass, LeaseTiming}
import lakehouse.ingestion.engine.IngestionService
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.connector.ConnectorRegistry
import lakehouse.ingestion.engine.control.DatabaseFailure
import lakehouse.ingestion.engine.files.{FileSourceConnector, Landing}
import lakehouse.ingestion.engine.jdbc.JdbcSourceConnector
import lakehouse.ingestion.logstore.StartupRefusedException

/**
 * The command `run`: the runs of one source into Bronze, as an orchestrator starts it (a
 * process runs one source). What it stands on, and what is refused before anything costly is
 * started, is `SourceCommand`'s, with the connector of the source and the landing zone, and with
 * nothing done for a source that is disabled (0). Then the runs: one, and the next while the
 * batch that it committed says that the source has more, up to `--max-batches` and until a
 * signal asks it to stop (`runs`). The code is that of the last run (`exitOf`, discovery §14.1).
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
      err: PrintStream,
      maxBatches: Option[Int] = None,
      stop: Stop = Stop.Never
  ): Int =
    SourceCommand(
      profilePath,
      sourcesPath,
      id,
      secrets,
      out,
      err,
      reads = true,
      operator = false,
      done = "run"
    ) { ready =>
      val service = new IngestionService(
        ready.database,
        connectors(ready.profile, secrets),
        ready.spark,
        ready.profile.lake.uri,
        LeaseTiming(ready.profile.leases.ttl)
      )
      runs(service, ready.source, ready.config, Chain.Limits(maxBatches, stop), out, err)
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
  def missingRoots(profile: Profile, landing: Boolean = true): Seq[String] =
    (Seq("lake.uri" -> profile.lake.uri) ++
      Option.when(landing)("landing.root" -> profile.landing.root)).collect {
      case (field, root) if root.isLocal && !Files.isDirectory(Paths.get(URI.create(root.value))) =>
        s"the root of $field, ${root.value}, is not a directory: it is not there, or is not a " +
          "directory. The engine does not make it, since a root that is missing is more often " +
          "a disk that is not mounted: make it once, by hand"
    }

  /**
   * The runs of an invocation (`Chain`): one, and another after it for as long as the batch that
   * it committed says that the source has more, within the horizon of the first plan of the
   * invocation, each said as it ends.
   */
  private def runs(
      service: IngestionService,
      source: Source,
      config: ConfigVersion,
      limits: Chain.Limits,
      out: PrintStream,
      err: PrintStream
  ): Int =
    Chain(
      limits,
      Chain.Words(
        invocation = s"The invocation of ${source.id.value}",
        batch = "batch",
        batches = "batches",
        more = "and the source has more.",
        afterAnother = "after a batch that a run before this invocation left: whether the " +
          "source has more is for the next invocation to plan."
      ),
      out,
      err
    ) { outcome =>
      val (stream, line) = report(source.id, outcome)
      (if (stream == Stream.Out) out else err).println(line)
    }(horizon => service.run(source, config, horizon))

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
        // A count that the run did not make is not zero: a batch that another writer of its
        // sequence wrote, or whose commit does not say its rows, has rows that nobody here
        // counted, and the control plane records the count as not known.
        def counted(count: Option[Long]): String = count.fold("an unknown number")(_.toString)
        Stream.Out -> (s"$run committed the batch $batch: " +
          s"${counts.rowsRead.fold("an unknown number of")(_.toString)} rows read, " +
          s"${counted(counts.rowsWritten)} written, " +
          s"${counted(counts.rowsRejected)} rejected${written(outputs)}.")
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
