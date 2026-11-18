package lakehouse.ingestion.cli

import java.io.PrintStream
import java.nio.file.Paths

import lakehouse.ingestion.core.config.{ConfigLoader, Configuration, ConfigVersion, Loaded}

/**
 * The entry point of the application: a command and its options.
 *
 * `validate` is the one command so far. It reads a configuration and says everything that is
 * wrong in it, without opening a connection: it is what a pull request that changes a source is
 * checked with, and what the other commands will do first (ADR-0007: the whole configuration is
 * validated before any data is touched).
 */
object Main {

  /** How a command ended, for whoever started the process (discovery §14). */
  object Exit {
    val Ok = 0

    /** The command line is not one: nothing was looked at. */
    val Usage = 2

    /** Something that trying again does not mend: here, a configuration that is not valid. */
    val Permanent = 4
  }

  val Usage: String =
    """usage: lakehouse-ingestion validate --profile <file> --sources <directory>
      |
      |  validate   Reads the profile and every source, and says everything that is wrong in
      |             them. No connection is opened: it runs anywhere, in a moment.
      |
      |  --profile  the file of the profile, as conf/app/local.yaml
      |  --sources  the directory of the sources, one .yaml file for each, as conf/sources
      |
      |exit codes: 0 the configuration is valid; 2 the command line is not one;
      |            4 the configuration is not valid""".stripMargin

  def main(args: Array[String]): Unit = sys.exit(run(args.toSeq, System.out, System.err))

  /**
   * Runs a command line, and gives the exit code of the process.
   *
   * @param out what the command says of what it did
   * @param err errors and warnings, one on each line
   */
  def run(args: Seq[String], out: PrintStream, err: PrintStream): Int = args.toList match {
    case "validate" :: options =>
      parse(options, Set("--profile", "--sources")) match {
        case Right(found) => validate(found("--profile"), found("--sources"), out, err)
        case Left(problem) => usage(problem, err)
      }
    case ("--help" | "-h" | "help") :: Nil =>
      out.println(Usage)
      Exit.Ok
    case Nil => usage("no command", err)
    case command :: _ => usage(s"'${shown(command)}' is not a command", err)
  }

  private def usage(problem: String, err: PrintStream): Int = {
    err.println(s"error: $problem")
    err.println(Usage)
    Exit.Usage
  }

  /** `--name value` pairs: each of `names` once, and nothing else. */
  private def parse(
      options: List[String],
      names: Set[String]
  ): Either[String, Map[String, String]] = {
    def go(rest: List[String], found: Map[String, String]): Either[String, Map[String, String]] =
      rest match {
        case Nil =>
          (names -- found.keySet).toSeq.sorted match {
            case Seq() => Right(found)
            case missing => Left(s"${missing.mkString(" and ")} must be given")
          }
        case name :: _ if !names(name) => Left(s"'${shown(name)}' is not an option of this command")
        case name :: _ if found.contains(name) => Left(s"$name is given twice")
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
