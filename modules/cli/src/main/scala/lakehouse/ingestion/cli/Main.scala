package lakehouse.ingestion.cli

import java.io.PrintStream
import java.nio.file.Paths

import lakehouse.ingestion.core.config._
import lakehouse.ingestion.core.control.{BackfillId, BackfillRequest, BatchSeq, OperatorRequest}
import lakehouse.ingestion.core.watermark.BackfillRange
import lakehouse.ingestion.engine.control.DatabaseFailure
import lakehouse.ingestion.engine.operator.OperatorActions.WatermarkReset

/**
 * The entry point of the application: a command and its options.
 *
 *  - `validate` reads a configuration and says everything that is wrong in it, without opening
 *    a connection: it is what a pull request that changes a source is checked with, and what
 *    the other commands do first (ADR-0007: the whole configuration is validated before any
 *    data is touched);
 *  - `migrate` brings the schemas of a deployment to the version of this application. It is the
 *    one command that connects as the role that owns them (ADR-0013 §3), and that role's
 *    password is in no profile: the command is told where it is;
 *  - `run` runs one source once into Bronze, as an orchestrator starts it (`RunCommand`).
 */
object Main {

  /**
   * How a command ended, for whoever started the process: the contract with the orchestrator
   * (discovery §14). The framework does not schedule itself, so the exit code is all that an
   * orchestrator has to tell "run it again" from "do not", and a code has one meaning.
   *
   * All of them are here, those that no command gives yet as well, so that none is given a
   * second meaning by the next command that needs a number.
   */
  object Exit {

    /** The command did what it was asked. */
    val Ok = 0

    /**
     * `SKIPPED_LOCKED`: another run holds the lease of the source (ADR-0006). Nothing was read
     * or written. It is not a failure, and an orchestrator answers it by doing nothing until
     * its next trigger.
     */
    val SkippedLocked = 2

    /** A failure that passes and that outlasted the retries. Running again later may do. */
    val Transient = 3

    /**
     * Something that running again does not mend, until somebody changes something: a
     * configuration that is not valid, a database that refused, a source that the run failed
     * on for good.
     */
    val Permanent = 4

    /** More rows were rejected than the source allows, and the batch was not committed. */
    val DataThreshold = 5

    /**
     * The command line is not one: nothing was looked at. `EX_USAGE` of `sysexits.h`, far from
     * the codes of a run on purpose. It was 2 for a day, which is `SkippedLocked`: an
     * orchestrator would have answered a mistyped command by trying it again at every trigger,
     * for ever.
     */
    val Usage = 64

    /**
     * An exception that nothing classified: a mistake in the application itself. `EX_SOFTWARE`
     * of `sysexits.h`. Nothing gives it yet: until `Main` catches what nothing else did (P8),
     * such an exception ends the process with the JVM's own 1.
     */
    val Internal = 70

    /** Every code, with what it is called in a report. */
    val All: Seq[(Int, String)] = Seq(
      Ok -> "ok",
      SkippedLocked -> "skipped: locked",
      Transient -> "transient failure",
      Permanent -> "permanent failure",
      DataThreshold -> "data threshold exceeded",
      Usage -> "usage error",
      Internal -> "internal error"
    )
  }

  /** The role that owns the schemas, unless the command is told another (ADR-0013 §3). */
  val Migrator = "ingest_migrator"

  val Usage: String =
    s"""usage: lakehouse-ingestion validate --profile <file> --sources <directory>
       |       lakehouse-ingestion migrate --profile <file> --password <reference> [--user <role>]
       |       lakehouse-ingestion run --profile <file> --sources <directory> --source <id>
       |                               [--max-batches <n>]
       |       lakehouse-ingestion abandon-batch --profile <file> --sources <directory>
       |                               --source <id> --batch <n> --reason <text>
       |                               [--backfill <id>]
       |       lakehouse-ingestion reprocess --profile <file> --sources <directory>
       |                               --source <id> --batch <n> --reason <text>
       |       lakehouse-ingestion reset-state --profile <file> --sources <directory>
       |                               --source <id> --reason <text>
       |                               (--watermark <moment> | --forget-watermark
       |                                | --keep-watermark)
       |       lakehouse-ingestion backfill --profile <file> --sources <directory>
       |                               --source <id> --backfill <id> --from <moment>
       |                               --to <moment> [--chunk <duration>] --reason <text>
       |                               [--max-batches <n>]
       |
       |  validate    Reads the profile and every source, and says everything that is wrong in
       |              them. No connection is opened: it runs anywhere, in a moment.
       |  migrate     Brings the schemas of the control plane and of the commit coordination to
       |              the version of this application, in the databases of the profile, as the
       |              role that owns them.
       |  run         Runs one source into its table of Bronze, a batch a run: and again, while
       |              the source has more than one batch takes, up to where its first plan saw.
       |              The configuration is validated whole first. SIGTERM or SIGINT lets the
       |              batch in course finish, and starts no other; a second ends it at once.
       |
       |  abandon-batch  Gives up the batch of a source that is planned, if the table it was
       |              planned for does not have it: the next run plans anew from the state of
       |              the source. It is a run of its own, under the lease of the source, that
       |              records who asked for it (the user of the process) and why. With
       |              --backfill, the chunk of that backfill that is planned, under its lease:
       |              the next run of the backfill reads its window again.
       |  reprocess   Reads the files of a committed batch of a source again, into the table
       |              that its configuration names now, as a batch of its own whose run says
       |              who asked and why. The files stay taken by the batch that took them.
       |  reset-state  Resets the state of a source: its watermark is set, forgotten or kept,
       |              and its configuration as it is now is the one that later runs are
       |              compared with, which lets a change through that a run refuses. The
       |              numbers of its batches go on, and the files it took stay taken. A run of
       |              its own, as abandon-batch is.
       |  backfill    Reads the range (from, to] of the watermark column of a source: the rows
       |              whose watermark is from exactly are not read, and those at to are. It is
       |              read on a sequence of its own, a chunk a run, beside the regular runs of
       |              the source, which it neither waits for nor moves the state of. The first
       |              invocation under an id creates the backfill; each after it, with the same
       |              arguments, resumes it, until it is complete. What the regular runs read
       |              of the range too is in Bronze twice, and is warned of. Its runs say who
       |              asked and why, and are chained and stopped as those of run are.
       |
       |  --profile   the file of the profile, as conf/app/local.yaml
       |  --sources   the directory of the sources, one .yaml file for each, as conf/sources
       |  --source    the id of the source to run, as sales.returns
       |  --password  where the password of that role is, and never the password itself:
       |              env:NAME or file:/run/secrets/name
       |  --user      the role that owns the schemas; $Migrator unless said
       |  --max-batches  the most batches that run commits, or chunks that backfill does;
       |              no limit unless said
       |  --batch     abandon-batch: the number of the batch to give up, the one that is
       |              planned, or of the chunk with --backfill; reprocess: that of the
       |              committed batch whose files are read
       |  --reason    why, in one line: kept with the run and with the batch
       |  --watermark  the moment that the source is taken to be ingested up to, as
       |              load.watermark.start is written: 2026-03-01T00:00:00Z
       |  --forget-watermark  the next run plans as a source with no watermark does
       |  --keep-watermark  the watermark stays; only what later runs are compared with is
       |              reset. Its risks are warned of
       |  --backfill  the id of the backfill, as history-2024: lower-case letters, digits, '.',
       |              '_' and '-'. abandon-batch: the backfill whose chunk is given up
       |  --from      where the range starts, as 2025-01-01T00:00:00Z: it is not in the range,
       |              and a row whose watermark is from exactly is not read
       |  --to        where the range ends, which is in it: no later than the clock of the
       |              source less its safety_margin
       |  --chunk     how much of the watermark a chunk reads, as a configuration writes it: 7d,
       |              6h. max_batch_range of the source unless said; the backfill's own when it
       |              is resumed
       |
       |exit codes: 0 the configuration is valid, the schemas are at their version, the run
       |              ended well (a batch committed, nothing new, or a source that is
       |              disabled), the backfill ended well (chunks committed, or it is
       |              complete), the batch was given up or reprocessed, or the state was reset;
       |            2 another run holds the lease of the source, or of the backfill: nothing
       |              was done;
       |            3 a database could not be reached, the run lost its lease, its batch was
       |              given up, or a table could not be read, and running again later may do;
       |            4 the configuration is not valid, a database refused, the run failed for a
       |              reason that running again does not mend, the batch was not given up:
       |              none is planned, another is, or its table has it, or the backfill named
       |              is not there, or the state was not reset: a batch is planned, the source
       |              has no state, or no such watermark, or the batch was not reprocessed: it
       |              was not committed, read no files, or another is planned, or the backfill
       |              was refused: its source reads no range, it has no chunk, to has not
       |              settled, its id has another range or chunk, or its configuration changed
       |              in a way that it cannot follow;
       |            5 more records were rejected than the source allows: nothing committed;
       |            64 the command line is not one;
       |            128 + n run or backfill was stopped by the signal n before its first run:
       |              nothing was run. One stopped after a run ends in the code of its last""".stripMargin

  def main(args: Array[String]): Unit =
    sys.exit(run(args.toSeq, System.out, System.err, sys.env.get, Stop.onSignals(System.err)))

  /**
   * Runs a command line, and gives the exit code of the process.
   *
   * @param out what the command says of what it did
   * @param err errors and warnings, one on each line
   * @param environment how a variable is looked up; the environment of the process, unless a
   *   test says otherwise
   * @param stop whether the process has been asked to stop: its signals, in `main`
   * @param operator who runs the process, as it says: who asked for an operator's action
   */
  def run(
      args: Seq[String],
      out: PrintStream,
      err: PrintStream,
      environment: String => Option[String] = sys.env.get,
      stop: Stop = Stop.Never,
      operator: String = sys.props.getOrElse("user.name", "")
  ): Int = args.toList match {
    case "validate" :: options =>
      parse(options, Set("--profile", "--sources")) match {
        case Right(found) => validate(found("--profile"), found("--sources"), out, err)
        case Left(problem) => usage(problem, err)
      }
    case "migrate" :: options =>
      val asked = for {
        found <- parse(options, Set("--profile", "--password"), optional = Set("--user"))
        // What is wrong is said, and what was typed is not: it may be the password itself.
        password <- SecretRef.parse(found("--password")).left.map(reason => s"--password $reason")
        user <- SqlIdentifier
          .parse(found.getOrElse("--user", Migrator))
          .left
          .map(reason => s"--user $reason")
      } yield (found("--profile"), user, password)
      asked match {
        case Right((profile, user, password)) =>
          migrate(profile, user, password, SecretResolver(environment), out, err)
        case Left(problem) => usage(problem, err)
      }
    case "run" :: options =>
      val asked = for {
        found <- parse(
          options,
          Set("--profile", "--sources", "--source"),
          optional = Set("--max-batches")
        )
        source <- SourceId.parse(found("--source")).left.map(reason => s"--source $reason")
        limit <- maxBatchesOf(found.get("--max-batches"))
      } yield (found("--profile"), found("--sources"), source, limit)
      asked match {
        case Right((profile, sources, source, limit)) =>
          RunCommand.run(
            Paths.get(profile),
            Paths.get(sources),
            source,
            SecretResolver(environment),
            out,
            err,
            limit,
            stop
          )
        case Left(problem) => usage(problem, err)
      }
    case "abandon-batch" :: options =>
      val asked = for {
        found <- parse(
          options,
          Set("--profile", "--sources", "--source", "--batch", "--reason"),
          optional = Set("--backfill")
        )
        source <- SourceId.parse(found("--source")).left.map(reason => s"--source $reason")
        batch <- batchOf(found("--batch"))
        backfill <- found.get("--backfill") match {
          case None => Right(None)
          case Some(text) =>
            BackfillId
              .parse(text)
              .map(Some(_))
              .left
              .map(reason => s"--backfill '${shown(text)}' $reason")
        }
        request <- OperatorRequest.of(operator, found("--reason"))
      } yield (found("--profile"), found("--sources"), source, batch, backfill, request)
      asked match {
        case Right((profile, sources, source, batch, backfill, request)) =>
          AbandonBatchCommand.run(
            Paths.get(profile),
            Paths.get(sources),
            source,
            batch,
            request,
            SecretResolver(environment),
            out,
            err,
            backfill
          )
        case Left(problem) => usage(problem, err)
      }
    case "reprocess" :: options =>
      val asked = for {
        found <- parse(options, Set("--profile", "--sources", "--source", "--batch", "--reason"))
        source <- SourceId.parse(found("--source")).left.map(reason => s"--source $reason")
        batch <- batchOf(found("--batch"))
        request <- OperatorRequest.of(operator, found("--reason"))
      } yield (found("--profile"), found("--sources"), source, batch, request)
      asked match {
        case Right((profile, sources, source, batch, request)) =>
          ReprocessCommand.run(
            Paths.get(profile),
            Paths.get(sources),
            source,
            batch,
            request,
            SecretResolver(environment),
            out,
            err
          )
        case Left(problem) => usage(problem, err)
      }
    case "reset-state" :: options =>
      val modes = Set("--watermark", "--forget-watermark", "--keep-watermark")
      val asked = for {
        found <- parse(
          options,
          Set("--profile", "--sources", "--source", "--reason"),
          optional = Set("--watermark"),
          flags = modes - "--watermark"
        )
        source <- SourceId.parse(found("--source")).left.map(reason => s"--source $reason")
        to <- modes.filter(found.contains).toSeq.sorted match {
          case Seq("--watermark") =>
            Timestamps
              .parse(found("--watermark"))
              .map(WatermarkReset.At(_))
              .left
              .map(reason => s"--watermark '${shown(found("--watermark"))}' $reason")
          case Seq("--forget-watermark") => Right(WatermarkReset.Forget)
          case Seq("--keep-watermark") => Right(WatermarkReset.Keep)
          case Seq() => Left(s"one of ${modes.toSeq.sorted.mkString(", ")} must be given")
          case more => Left(s"${more.mkString(" and ")} are given: one of them is")
        }
        request <- OperatorRequest.of(operator, found("--reason"))
      } yield (found("--profile"), found("--sources"), source, to, request)
      asked match {
        case Right((profile, sources, source, to, request)) =>
          ResetStateCommand.run(
            Paths.get(profile),
            Paths.get(sources),
            source,
            to,
            request,
            SecretResolver(environment),
            out,
            err
          )
        case Left(problem) => usage(problem, err)
      }
    case "backfill" :: options =>
      val asked = for {
        found <- parse(
          options,
          Set("--profile", "--sources", "--source", "--backfill", "--from", "--to", "--reason"),
          optional = Set("--chunk", "--max-batches")
        )
        source <- SourceId.parse(found("--source")).left.map(reason => s"--source $reason")
        id <- BackfillId
          .parse(found("--backfill"))
          .left
          .map(reason => s"--backfill '${shown(found("--backfill"))}' $reason")
        from <- momentOf("--from", found("--from"))
        to <- momentOf("--to", found("--to"))
        range <- BackfillRange.of(from, to).left.map(reason => s"--from and --to: $reason")
        chunk <- found.get("--chunk") match {
          case None => Right(None)
          case Some(text) =>
            Durations
              .parse(text)
              .filterOrElse(_.toMillis > 0, "is no stretch of time: a chunk is 1ms at least")
              .map(Some(_))
              .left
              .map(reason => s"--chunk '${shown(text)}' $reason")
        }
        limit <- maxBatchesOf(found.get("--max-batches"))
        request <- OperatorRequest.of(operator, found("--reason"))
      } yield (
        found("--profile"),
        found("--sources"),
        source,
        BackfillRequest(id, range, chunk),
        request,
        limit
      )
      asked match {
        case Right((profile, sources, source, backfill, request, limit)) =>
          BackfillCommand.run(
            Paths.get(profile),
            Paths.get(sources),
            source,
            backfill,
            request,
            SecretResolver(environment),
            out,
            err,
            limit,
            stop
          )
        case Left(problem) => usage(problem, err)
      }
    case ("--help" | "-h" | "help") :: Nil =>
      out.println(Usage)
      Exit.Ok
    case Nil => usage("no command", err)
    case command :: _ => usage(s"'${shown(command)}' is not a command", err)
  }

  /** How many batches an invocation commits at most, from 1: none, for no limit. */
  private def maxBatchesOf(given: Option[String]): Either[String, Option[Int]] = given match {
    case None => Right(None)
    case Some(text) =>
      text.toIntOption
        .filter(n => n >= 1 && text == n.toString)
        .map(Some(_))
        .toRight(s"--max-batches '${shown(text)}' is not a number of batches, from 1")
  }

  /** A moment of an option, as `load.watermark.start` is written. */
  private def momentOf(option: String, text: String): Either[String, java.time.Instant] =
    Timestamps.parse(text).left.map(reason => s"$option '${shown(text)}' $reason")

  /** The number of a batch, from 1, written as a number is and no other way. */
  private def batchOf(text: String): Either[String, BatchSeq] =
    text.toLongOption
      .filter(n => text == n.toString)
      .flatMap(BatchSeq.of(_).toOption)
      .toRight(s"--batch '${shown(text)}' is not the number of a batch, from 1")

  private def usage(problem: String, err: PrintStream): Int = {
    err.println(s"error: $problem")
    err.println(Usage)
    Exit.Usage
  }

  /**
   * `--name value` pairs: each of `required` once, each of `optional` once at most. `flags` are
   * optional and take no value: one that is given is found with an empty one.
   */
  private def parse(
      options: List[String],
      required: Set[String],
      optional: Set[String] = Set.empty,
      flags: Set[String] = Set.empty
  ): Either[String, Map[String, String]] = {
    val names = required ++ optional ++ flags
    def go(rest: List[String], found: Map[String, String]): Either[String, Map[String, String]] =
      rest match {
        case Nil =>
          (required -- found.keySet).toSeq.sorted match {
            case Seq() => Right(found)
            case Seq(one) => Left(s"$one must be given")
            case missing =>
              Left(s"${missing.init.mkString(", ")} and ${missing.last} must be given")
          }
        case name :: _ if !names(name) => Left(s"'${shown(name)}' is not an option of this command")
        case name :: _ if found.contains(name) => Left(s"$name is given twice")
        case name :: more if flags(name) => go(more, found + (name -> ""))
        case name :: Nil => Left(s"$name has no value after it")
        case name :: value :: more => go(more, found + (name -> value))
      }
    go(options, Map.empty)
  }

  private def validate(profile: String, sources: String, out: PrintStream, err: PrintStream): Int =
    ConfigLoader.load(Paths.get(profile), Paths.get(sources)) match {
      case Left(errors) =>
        val all = errors.toChain.toVector
        all.foreach(error => err.println(error.message))
        val files = all.map(_.file).distinct.size
        err.println(
          s"${counted(all.size, "error")} in ${counted(files, "file")}. The configuration is not valid."
        )
        Exit.Permanent

      case Right(Loaded(configuration, warnings)) =>
        warnings.foreach(warning => err.println(s"warning: ${warning.message}"))
        out.println(report(profile, configuration))
        Exit.Ok
    }

  /**
   * The profile is validated whole first, and the password is looked for, before a connection
   * is opened. Then each schema in its turn: the first that fails ends the command, and what
   * was migrated before it stays migrated, which running the command again finds.
   */
  private def migrate(
      profile: String,
      user: SqlIdentifier,
      password: SecretRef,
      secrets: SecretResolver,
      out: PrintStream,
      err: PrintStream
  ): Int =
    ConfigLoader.loadProfile(Paths.get(profile)) match {
      case Left(errors) =>
        errors.toChain.toVector.foreach(error => err.println(error.message))
        err.println("The profile is not valid, and nothing was migrated.")
        Exit.Permanent

      case Right(Loaded(valid, warnings)) =>
        warnings.foreach(warning => err.println(s"warning: ${warning.message}"))
        secrets.resolve(password) match {
          case Left(problem) =>
            err.println(s"error: --password $problem")
            Exit.Permanent

          case Right(secret) =>
            val failed = Migrations
              .targets(valid)
              .iterator
              .map { target =>
                val result = Migrations.migrate(target, user, secret)
                result.foreach(done => out.println(migrated(done, target)))
                result
              }
              .collectFirst { case Left(failure) => failure }

            failed match {
              case None => Exit.Ok
              case Some(failure) =>
                err.println(s"error: ${failure.message}")
                failure match {
                  case _: DatabaseFailure.Unreachable => Exit.Transient
                  case _: DatabaseFailure.Refused => Exit.Permanent
                }
            }
        }
    }

  private def migrated(done: Migrations.Migrated, target: Migrations.Target): String = {
    val what =
      if (done.applied == 0) "nothing to apply"
      else s"${counted(done.applied, "migration")} applied"
    s"${done.schema}: version ${done.version}, $what (${target.connection.url.value})"
  }

  /** What a valid configuration is: its sources, each with the version it would run with. */
  private def report(profile: String, configuration: Configuration): String = {
    val rows = configuration.sources.map { source =>
      Seq(
        source.id.value,
        if (source.enabled) "enabled" else "disabled",
        source.connector.kind,
        source.load.mode.name,
        (source.targets.bronze.table.value +: source.targets.silver.toSeq.map(_.table.value))
          .mkString(", "),
        ConfigVersion.of(configuration.profile, source).hash.take(12)
      )
    }
    val widths = rows.transpose.map(_.map(_.length).max)
    val table = rows.map(
      _.zip(widths).map { case (cell, width) => cell.padTo(width, ' ') }.mkString("  ").trim
    )

    (s"The configuration is valid: $profile, and ${counted(configuration.sources.size, "source")}." +:
      s"The lake is ${configuration.profile.lake.uri}." +: table.map("  " + _)).mkString("\n")
  }

  private def counted(count: Int, what: String): String =
    if (count == 1) s"1 $what" else s"$count ${what}s"

  /** What was typed, as it can be shown: what ASCII does not print is left out. */
  private def shown(text: String): String = text.filter(c => c >= 0x20 && c < 0x7f).take(60)
}
