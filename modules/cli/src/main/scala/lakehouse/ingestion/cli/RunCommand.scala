package lakehouse.ingestion.cli

import java.io.PrintStream
import java.net.URI
import java.nio.file.{Files, Path, Paths}

import scala.annotation.tailrec

import io.circe.Json

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
      runs(service, ready.source, ready.config, Chain(maxBatches, stop), out, err)
    }

  /**
   * How far an invocation goes: no further than `maxBatches` committed, and no run after
   * `stop` has asked.
   */
  final private case class Chain(maxBatches: Option[Int], stop: Stop)

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
   * The runs of an invocation (discovery §17, P6.10): one, and another after it for as long as
   * the batch that it committed says that the source has more, within the horizon of the first
   * plan of the invocation. Each is a whole run, with its lease and its batch. The chain stops
   * at `--max-batches`, and before a run once a signal has asked it to; its code is that of its
   * last run, or 128 + the signal if it asked before the first.
   */
  private def runs(
      service: IngestionService,
      source: Source,
      config: ConfigVersion,
      chain: Chain,
      out: PrintStream,
      err: PrintStream
  ): Int = {
    @tailrec
    def next(horizon: Option[Json], ran: Int, committed: Int, last: Option[Int]): Int =
      chain.stop.requested match {
        case Some(signal) =>
          last match {
            case Some(code) =>
              out.println(
                s"${ofTheInvocation(source, ran, committed)}: ${Stop.name(signal)} asked it to " +
                  "stop, and no other run was started."
              )
              code
            case None =>
              err.println(
                s"error: ${Stop.name(signal)} came before the first run. Nothing was run."
              )
              128 + signal
          }
        case None =>
          val outcome = service.run(source, config, horizon)
          val (stream, line) = report(source.id, outcome)
          (if (stream == Stream.Out) out else err).println(line)
          val code = exitOf(outcome)
          val done = committed + (outcome match {
            case _: Outcome.Ingested | _: Outcome.RolledForward => 1
            case _ => 0
          })
          // What the next run plans against, if there is one: the horizon of the chain, or
          // none, for a first plan of the invocation after a batch of another.
          val more: Option[Option[Json]] = outcome match {
            case Outcome.Ingested(_, _, _, _, Outcome.Next.Within(within)) => Some(Some(within))
            case Outcome.Ingested(_, _, _, _, Outcome.Next.Afresh) => Some(None)
            case _: Outcome.RolledForward => Some(None)
            case _ => None
          }
          more match {
            case Some(within) if chain.maxBatches.forall(done < _) =>
              next(within, ran + 1, done, Some(code))
            case Some(within) =>
              // The plan of the batch says that there is more. After a batch of another
              // invocation no plan of this one says anything: the next plans anew, and finds
              // out.
              val left =
                if (within.nonEmpty) "and the source has more."
                else
                  "after a batch that a run before this invocation left: whether the source " +
                    "has more is for the next invocation to plan."
              out.println(
                s"${ofTheInvocation(source, ran + 1, done)}: it stopped at --max-batches " +
                  s"${chain.maxBatches.getOrElse(done)}, $left"
              )
              code
            case None =>
              if (ran > 0)
                out.println(s"${ofTheInvocation(source, ran + 1, done)}.")
              code
          }
      }
    next(horizon = None, ran = 0, committed = 0, last = None)
  }

  private def ofTheInvocation(source: Source, ran: Int, committed: Int): String =
    s"The invocation of ${source.id.value} made ${counted(ran, "run")} and committed " +
      counted(committed, "batch", "batches")

  private def counted(count: Int, one: String, many: String = ""): String =
    if (count == 1) s"1 $one" else s"$count ${if (many.isEmpty) one + "s" else many}"

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
