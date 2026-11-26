package lakehouse.ingestion.cli

import java.io.PrintStream
import java.nio.file.Path

import scala.util.Using

import lakehouse.ingestion.core.config._
import lakehouse.ingestion.core.control.{FailureClass, OperatorRequest}
import lakehouse.ingestion.engine.Failures
import lakehouse.ingestion.engine.control.{DatabaseFailure, LogStoreSettings}
import lakehouse.ingestion.engine.delta.Lake
import lakehouse.ingestion.engine.failure.RunFailedException
import lakehouse.ingestion.logstore.coordination.{
  CoordinationSettings,
  MaintenanceRequest,
  PostgresRegistryMaintenance
}
import lakehouse.ingestion.logstore.{
  MaintenanceRefusedException,
  TableBeingWrittenException,
  TableMaintenance,
  TempFate
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}

/**
 * `maintain-table`: what an operator does to a table of the lake that the LogStore refuses, or
 * whose temporary files are to be cleaned (ADR-0004 §3.5, §3.8; `TableMaintenance`).
 *
 * It acts on a table, not on a source: it takes no lease and is no run (ADR-0021 is of sources),
 * and its writers are to be stopped first, which the operations need not for their safety
 * (P2a.11) and the runbook asks for. It connects to the coordination database of the profile as
 * the role of maintenance, whose password, as that of the migrator, is in no profile: the command
 * is told where it is. Each operation is recorded in the maintenance log with who asked for it
 * and why (`delta_coordination.maintenance_log`).
 *
 * No Spark is started. The table is named by its location, as the error of a run names it, and
 * must be in the lake of the profile, as every table that the engine writes to (`Lake.confine`).
 */
private[cli] object MaintainTableCommand {

  /** An operation on a table, as the command line names it. */
  sealed abstract class Operation(val name: String)

  object Operation {
    case object Adopt extends Operation("adopt")
    case object Readopt extends Operation("readopt")
    case object Reset extends Operation("reset")
    case object CollectTemps extends Operation("collect-temps")

    val All: Seq[Operation] = Seq(Adopt, Readopt, Reset, CollectTemps)

    def of(name: String): Option[Operation] = All.find(_.name == name)
  }

  private val HadoopPrefix = "spark.hadoop."

  /**
   * The profile is validated whole and the password looked for before a connection is opened,
   * as `migrate` does them; then the table is confined to the lake, and the operation made.
   *
   * @param user the role of maintenance
   * @param password where its password is
   * @param environment how the LogStore looks a variable up, for a password of `env:`
   */
  def run(
      operation: Operation,
      profilePath: Path,
      table: TableLocation,
      user: SqlIdentifier,
      password: SecretRef,
      request: OperatorRequest,
      secrets: SecretResolver,
      environment: String => Option[String],
      out: PrintStream,
      err: PrintStream
  ): Int =
    ConfigLoader.loadProfile(profilePath) match {
      case Left(errors) =>
        errors.toChain.toVector.foreach(error => err.println(error.message))
        err.println("The profile is not valid, and nothing was done.")
        Main.Exit.Permanent

      case Right(Loaded(profile, warnings)) =>
        warnings.foreach(warning => err.println(s"warning: ${warning.message}"))
        secrets.resolve(password) match {
          case Left(problem) =>
            err.println(s"error: --password $problem")
            Main.Exit.Permanent
          case Right(_) =>
            val coordination = profile.databases.coordination.copy(user = user, password = password)
            maintained(operation, profile, coordination, table, request, environment, out, err)
        }
    }

  private def maintained(
      operation: Operation,
      profile: Profile,
      coordination: DatabaseConnection,
      table: TableLocation,
      request: OperatorRequest,
      environment: String => Option[String],
      out: PrintStream,
      err: PrintStream
  ): Int = {
    val hadoop = hadoopOf(profile)
    val asked = MaintenanceRequest(request.requestedBy, request.reason)
    try {
      new Lake(profile.lake.uri, hadoop).confine(table)
      Using.resource(new PostgresRegistryMaintenance(settingsOf(coordination), environment)) {
        registry =>
          val maintenance = new TableMaintenance(registry)
          val path = new HadoopPath(table.uri)
          val fs = path.getFileSystem(hadoop)
          operation match {
            case Operation.Adopt =>
              val id = maintenance.firstCommitId(path, fs)
              val adopted = maintenance.adopt(path, fs, id, asked)
              out.println(
                s"${adopted.table} was adopted at version ${adopted.version}, " +
                  id.fold("with no id: its first commit is not in its log, or says none")(id =>
                    s"with the id $id"
                  ) + ". It is written through the LogStore from the next version on."
              )
            case Operation.Readopt =>
              val readopted = maintenance.readopt(path, fs, None, asked)
              val removed = readopted.removed.removedEntries.map(_.version)
              out.println(
                s"${readopted.table} was adopted again at version ${readopted.version}" +
                  readopted.removed.clearedTaint.fold("; it was not tainted")(why =>
                    s"; its taint was cleared: $why"
                  ) +
                  (if (removed.isEmpty) "; its registry and its log agreed"
                   else
                     s"; the entries that its log contradicts were removed: " +
                       (if (removed.size == 1) "version " else "versions ") +
                       removed.mkString(", ")) +
                  ". It is written from the next version on."
              )
            case Operation.Reset =>
              maintenance.reset(path, fs, asked)
              out.println(
                s"The registry of the table at ${table.uri} was reset: a table can be created " +
                  "at its path again."
              )
            case Operation.CollectTemps =>
              val fates = maintenance.collectTemps(path, fs, asked)
              val (removed, kept) = fates.partition(_._2.removed)
              out.println(
                s"${removed.size} temporary files of the table at ${table.uri} were removed, " +
                  s"and ${kept.size} kept."
              )
              kept.toSeq.sortBy(_._1).foreach { case (name, fate) =>
                err.println(s"warning: $name was kept: ${keptBecause(fate)}.")
              }
          }
          out.println(
            s"It is recorded in the maintenance log, as asked by ${request.requestedBy}: " +
              s"${request.reason}"
          )
          Main.Exit.Ok
      }
    } catch {
      case writing: TableBeingWrittenException =>
        err.println(s"error: ${writing.getMessage}. Nothing was changed")
        Main.Exit.Transient
      case refused: MaintenanceRefusedException =>
        err.println(s"error: ${refused.getMessage}. Nothing was changed")
        Main.Exit.Permanent
      // The lake, and the coordination database by its SQLState, as for a run (ADR-0022 §2).
      // What is said is not the classifier's message, which speaks of a run: the lake's own,
      // and what the database said, which a pool of connections wraps in what it says itself.
      case failure @ Failures.Known(classed) =>
        val said = failure match {
          case lake: RunFailedException => lake.getMessage
          case database =>
            s"the coordination database, as ${coordination.user}: " +
              DatabaseFailure.said(database)
        }
        err.println(s"error: $said")
        exitOf(classed.failure)
    }
  }

  /** The code of a failure of the command, by its class (discovery §14.1). */
  private[cli] def exitOf(failure: FailureClass): Int = failure match {
    case FailureClass.Transient | FailureClass.Conflict => Main.Exit.Transient
    case FailureClass.Permanent => Main.Exit.Permanent
    case FailureClass.Data => Main.Exit.DataThreshold
    case FailureClass.Internal => Main.Exit.Internal
  }

  private def keptBecause(fate: TempFate): String = fate match {
    case TempFate.KeptUnfinished => "its commit is taken and not published yet"
    case TempFate.KeptOnlyCopy =>
      "it is the only copy of a commit that is not in the log, for somebody to look at"
    case TempFate.KeptRecent => "it is too young to tell from the file of a writer at work"
    case removed => removed.name
  }

  /** How the LogStore reaches the coordination database: as its own settings say it. */
  private def settingsOf(coordination: DatabaseConnection): CoordinationSettings = {
    val written = LogStoreSettings.hadoop(coordination)
    CoordinationSettings
      .from((key: String) => written.get(key))
      .fold(
        problems =>
          throw new IllegalStateException(
            s"the coordination database of the profile is no setting of the LogStore: " +
              problems.mkString("; ")
          ),
        settings => settings
      )
  }

  /** The Hadoop configuration that a session of the profile gives its file systems. */
  private def hadoopOf(profile: Profile): Configuration = {
    val conf = new Configuration()
    profile.spark.settings.foreach { case (key, value) =>
      if (key.startsWith(HadoopPrefix)) conf.set(key.stripPrefix(HadoopPrefix), value)
    }
    conf
  }
}
