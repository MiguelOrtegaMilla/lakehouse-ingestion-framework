package lakehouse.ingestion.cli

import java.io.PrintStream
import java.nio.file.Path

import lakehouse.ingestion.core.config.{SecretResolver, SourceId}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.Failures
import lakehouse.ingestion.engine.control.{Batches, Runs}
import lakehouse.ingestion.engine.quarantine.QuarantineReader

/**
 * The command `quarantine`: what a source had set apart, for whoever has to look at it
 * (discovery §13.4, §17 P8.10). The records are counted by the run that set them apart, their
 * file and why, the latest first; with `--records`, some of them are shown as they were
 * received.
 *
 * '''It reads, and writes nothing.''' It takes no lease, and is no run: nothing of it is in the
 * control plane, which it asks in transactions that may only read, and it reads a table of the
 * lake. It stands on what every command on a source stands on (`SourceCommand`), on a source
 * that is disabled as on any other.
 *
 * '''The records of the batches that were given up are left out''', always, as whoever reads a
 * quarantine must leave them out (`QuarantineReader`); how many they are is said.
 *
 * '''A record may hold personal data, and goes to the standard output alone.''' Its text is
 * what its source sent. With `--records` the standard output has the records, a line of JSON
 * each (`QuarantineLines`), and nothing else; the warning, and what is said of what was
 * written, are on the error output, with no record in them. Nothing here has a logger: no
 * record is logged, and none is in the message of a failure.
 *
 * Its codes (discovery §14.1): 0 what was asked was shown, were it nothing; 3 a database did
 * not answer; 4 the configuration is not valid, or what was asked names a run or a batch that
 * the source does not have.
 */
object QuarantineCommand {

  /**
   * What is asked of a quarantine, as the command line says it.
   *
   * @param batch the number of a batch, of the regular sequence unless `backfill` is given
   * @param backfill the backfill whose sequence `batch` is of
   * @param records how many records to show, as they were received; none, for the counts
   */
  final case class Asking(
      run: Option[RunId] = None,
      batch: Option[BatchSeq] = None,
      backfill: Option[BackfillId] = None,
      file: Option[String] = None,
      errorType: Option[String] = None,
      records: Option[Int] = None
  ) {
    require(backfill.isEmpty || batch.nonEmpty, "a backfill says the sequence of a batch")
  }

  val PersonalData: String =
    "warning: the records of a quarantine are what their source sent, as it sent it, and may " +
      "hold personal data. They are written to the standard output of this command, and " +
      "nowhere else: mind where it goes."

  def run(
      profilePath: Path,
      sourcesPath: Path,
      id: SourceId,
      asking: Asking,
      secrets: SecretResolver,
      out: PrintStream,
      err: PrintStream
  ): Int =
    SourceCommand(
      profilePath,
      sourcesPath,
      id,
      secrets,
      out,
      err,
      reads = false,
      operator = true,
      done = "read"
    ) { ready =>
      try
        asked(ready, asking) match {
          case Left(problem) =>
            err.println(s"error: $problem. Nothing was read.")
            Main.Exit.Permanent
          case Right(what) =>
            val reader = new QuarantineReader(ready.database, ready.spark, ready.profile.lake.uri)
            val none = s"The source ${id.value} has no quarantine: no record of it was ever " +
              "set apart."
            asking.records match {
              case None =>
                reader
                  .counts(id, what)
                  .fold(Seq(none))(QuarantineLines.table(id, _))
                  .foreach(out.println)
              case Some(records) =>
                err.println(PersonalData)
                reader.sample(id, what, records) match {
                  case None => err.println(none)
                  case Some(shown) =>
                    shown.foreach(record => out.println(QuarantineLines.line(record)))
                    out.flush()
                    err.println(QuarantineLines.written(id, shown, records))
                }
            }
            Main.Exit.Ok
        }
      catch {
        // What it says of itself has no value of a row (`Failures`), and so no record.
        case Failures.Known(classed) =>
          err.println(s"error: ${classed.getMessage}")
          classed.failure match {
            case FailureClass.Transient | FailureClass.Conflict => Main.Exit.Transient
            case FailureClass.Permanent => Main.Exit.Permanent
            case FailureClass.Data => Main.Exit.DataThreshold
            case FailureClass.Internal => Main.Exit.Internal
          }
      }
    }

  /**
   * What is asked, as the reader takes it, once the run and the batch that it names are known
   * to be of the source: a question that names what is not there is a mistake of whoever asks,
   * and not a quarantine with nothing in it.
   */
  private def asked(
      ready: SourceCommand.Ready,
      asking: Asking
  ): Either[String, QuarantineReader.Asked] = {
    val source = ready.source.id
    val layer = Layer.Bronze
    val batch = asking.batch.map { seq =>
      BatchId(
        asking.backfill.fold(TxnAppId.of(source, layer))(TxnAppId.backfill(source, layer, _)),
        seq
      )
    }
    val (run, found) = ready.database.reading { connection =>
      (asking.run.map(Runs.find(connection, _)), batch.map(Batches.find(connection, _)))
    }
    val ofTheRun = (asking.run, run) match {
      case (Some(id), Some(None)) => Left(s"there is no run $id")
      case (Some(id), Some(Some(record)))
          if record.start.source != source || record.start.layer != layer =>
        Left(
          s"the run $id is of ${record.start.source}/${record.start.layer}, and not of " +
            s"$source/$layer"
        )
      case _ => Right(())
    }
    val ofTheBatch = (batch, found) match {
      case (Some(id), Some(None)) => Left(s"there is no batch $id")
      case _ => Right(())
    }
    for {
      _ <- ofTheRun
      _ <- ofTheBatch
    } yield QuarantineReader.Asked(asking.run, batch, asking.file, asking.errorType)
  }
}
