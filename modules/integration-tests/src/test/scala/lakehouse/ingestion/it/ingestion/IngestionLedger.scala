package lakehouse.ingestion.it.ingestion

import io.circe.Json
import lakehouse.ingestion.core.control.{BatchStatus, RunId, RunKind, RunStatus}

/**
 * What the control plane, the tables of a source and `processed_file` must say of each other
 * once no run of the source is in flight (P7.2).
 *
 * Each test of a run checks what it is about. This checks what every one of them stands on,
 * whatever the test did before: a batch that is committed is in its table once, and in no
 * other; a batch that was given up is in none; a row says a run, and the run says its batch; the
 * state is that of the last batch, or of the reset after it; each reset was of the state that
 * there was, and went on from its last batch; the files taken are those of the committed plans;
 * no run is still running, and the run that committed a batch, or reset the state, succeeded. A
 * test
 * that kills a run, takes its lease over or changes its source asks it of what is left once the
 * next run has finished.
 *
 * It is plain data and one pure function, so that every rule is tested on facts made by hand
 * (`IngestionLedgerSpec`): a check that cannot fail proves nothing. What reads the facts is
 * `IngestionSuite.ledgerOf`.
 *
 * A planned batch is not a fault: it is what a run that died leaves, and what the next run
 * recovers. Whether that run replays it or rolls it forward is decided by the table of its data
 * alone (ADR-0005 §3, step 2), so that table may have it or not, and its quarantine may have it
 * or not either way: a run that dies after its quarantine is written leaves a batch that the
 * quarantine has and its data table has not, which is replayed.
 */
object IngestionLedger {

  /** A file as `processed_file` keys it: its path under the landing root, its size and time. */
  final case class TakenFile(path: String, size: Long, modifiedMillis: Long)

  /**
   * A batch of the regular sequence of the source.
   *
   * @param target the location of the table that its data was planned for
   * @param outputs the tables that its commit recorded, by location, each with its version or
   *   none, for a version that was not known
   * @param watermark what its commit moves the watermark to, from its plan
   * @param files the files of its plan, for a batch of files
   */
  final case class Batch(
      seq: Long,
      status: BatchStatus,
      target: String,
      committedBy: Option[RunId] = None,
      outputs: Seq[Output] = Nil,
      watermark: Option[Json] = None,
      files: Set[TakenFile] = Set.empty
  )

  final case class Output(table: String, version: Option[Long])

  /** A run of the source, with the batch that it planned or recovered. */
  final case class Run(
      id: RunId,
      batch: Option[Long],
      status: RunStatus,
      rowsWritten: Option[Long] = None,
      kind: RunKind = RunKind.Regular
  )

  final case class State(watermark: Option[Json], lastCommitted: Long, updatedBy: RunId)

  /** A reset of the state by an operator's run, as `state_change_log` has it (ADR-0021 §3). */
  final case class Reset(run: RunId, before: State, after: State)

  /** A commit of a table, with the `txnVersion`s that it carries under the source's `txnAppId`. */
  final case class Commit(version: Long, txns: Seq[Long])

  /** The rows of the source that a table has under one run and one batch number. */
  final case class Rows(run: String, batch: Long, count: Long)

  /**
   * A table that the source writes, as its log and its rows say.
   *
   * @param commits the commits that its log still keeps, read as files
   * @param keptFrom the first version whose commit the log still keeps; none, for a table that
   *   is not there
   * @param lastTxn the last `txnVersion` of the source that Delta says the table has, which
   *   holds what the commits that are no longer kept had
   */
  final case class Table(
      uri: String,
      commits: Seq[Commit] = Nil,
      keptFrom: Option[Long] = None,
      lastTxn: Option[Long] = None,
      rows: Seq[Rows] = Nil
  )

  /**
   * @param data every table that a batch of the source was planned for, and the one that its
   *   configuration names
   * @param resets the resets of the state, in the order they were made
   */
  final case class Facts(
      batches: Seq[Batch],
      runs: Seq[Run],
      state: Option[State],
      taken: Set[(TakenFile, Long)],
      data: Seq[Table],
      quarantine: Table,
      resets: Seq[Reset] = Nil
  )

  sealed abstract class Rule(val name: String) {
    override def toString: String = name
  }

  object Rule {
    case object RecordedNotInTable extends Rule("RecordedNotInTable")
    case object InTableNotRecorded extends Rule("InTableNotRecorded")
    case object InTwoCommits extends Rule("InTwoCommits")
    case object AtAnotherVersion extends Rule("AtAnotherVersion")
    case object InAnotherTable extends Rule("InAnotherTable")
    case object AbandonedInTable extends Rule("AbandonedInTable")
    case object PlannedNotLast extends Rule("PlannedNotLast")
    case object TxnOfNoBatch extends Rule("TxnOfNoBatch")
    case object RowsOfNoRun extends Rule("RowsOfNoRun")
    case object RowsOfAnotherBatch extends Rule("RowsOfAnotherBatch")
    case object RowsOfNoWrittenBatch extends Rule("RowsOfNoWrittenBatch")
    case object BatchByTwoRuns extends Rule("BatchByTwoRuns")
    case object RowsNotAsCounted extends Rule("RowsNotAsCounted")
    case object StateNotOfLastBatch extends Rule("StateNotOfLastBatch")
    case object ResetNotChained extends Rule("ResetNotChained")
    case object ResetNotSucceeded extends Rule("ResetNotSucceeded")
    case object FilesNotAsCommitted extends Rule("FilesNotAsCommitted")
    case object StillRunning extends Rule("StillRunning")
    case object CommitterNotSucceeded extends Rule("CommitterNotSucceeded")
  }

  final case class Violation(rule: Rule, message: String) {
    override def toString: String = s"$rule: $message"
  }

  /** Every way in which the facts disagree; none, for a source that is as it should be. */
  def check(facts: Facts): Seq[Violation] =
    tables(facts) ++ txns(facts) ++ rows(facts) ++ state(facts) ++ resets(facts) ++
      files(facts) ++ runs(facts)

  // -----------------------------------------------------------------------------------------------
  // Batches and the commits of the tables
  // -----------------------------------------------------------------------------------------------

  private def holding(table: Table, seq: Long): Seq[Long] =
    table.commits.filter(_.txns.contains(seq)).map(_.version).sorted

  /**
   * Whether a table has a batch whose commit its log no longer keeps: Delta says that it has
   * the number or a later one, and the batch says a version before the first one kept, or none.
   */
  private def beyondTheLog(table: Table, seq: Long, recorded: Output): Boolean =
    table.lastTxn.exists(_ >= seq) &&
      recorded.version.forall(version => table.keptFrom.forall(version < _))

  private def tables(facts: Facts): Seq[Violation] = {
    val all = facts.data :+ facts.quarantine
    val last = facts.batches.map(_.seq).maxOption
    facts.batches.sortBy(_.seq).flatMap { batch =>
      val seq = batch.seq
      def twice(table: Table) = {
        val versions = holding(table, seq)
        Option.when(versions.size > 1)(
          Violation(
            Rule.InTwoCommits,
            s"batch $seq is in ${table.uri} at versions ${versions.mkString(", ")}"
          )
        )
      }
      batch.status match {
        case BatchStatus.Committed =>
          all.flatMap { table =>
            val versions = holding(table, seq)
            val recorded = batch.outputs.find(_.table == table.uri)
            val present = versions.nonEmpty || recorded.exists(beyondTheLog(table, seq, _))
            twice(table).toSeq ++ ((recorded, present) match {
              case (Some(_), false) =>
                Seq(
                  Violation(
                    Rule.RecordedNotInTable,
                    s"batch $seq records ${table.uri}, which has no commit of it"
                  )
                )
              case (None, true) =>
                Seq(
                  Violation(
                    Rule.InTableNotRecorded,
                    s"batch $seq is in ${table.uri} at ${versions.mkString(", ")}, and its " +
                      "commit does not record the table"
                  )
                )
              case (Some(Output(_, Some(version))), true)
                  if versions.size == 1 && versions.head != version =>
                Seq(
                  Violation(
                    Rule.AtAnotherVersion,
                    s"batch $seq records ${table.uri} at $version, and is in it at " +
                      s"${versions.head}"
                  )
                )
              case _ => Nil
            }) ++ elsewhere(facts, batch, table, present)
          }
        case BatchStatus.Abandoned =>
          // A batch writes its quarantine before its data, whatever becomes of it: one that
          // was given up afterwards has its records there (ADR-0018, discovery §13.4).
          facts.data.flatMap { table =>
            val versions = holding(table, seq)
            Option.when(versions.nonEmpty)(
              Violation(
                Rule.AbandonedInTable,
                s"batch $seq was given up, and is in ${table.uri} at ${versions.mkString(", ")}"
              )
            )
          }
        case BatchStatus.Planned =>
          // Its data table has it or not, and its quarantine either way: the next run decides
          // by the data table alone. What it may not be is in two commits, or in another table.
          Option
            .when(!last.contains(seq))(
              Violation(
                Rule.PlannedNotLast,
                s"batch $seq is planned, and batch ${last.getOrElse(0L)} comes after it"
              )
            )
            .toSeq ++ all.flatMap { table =>
            twice(table).toSeq ++
              elsewhere(facts, batch, table, holding(table, seq).nonEmpty)
          }
      }
    }
  }

  /** A batch in a data table other than the one that it was planned for. */
  private def elsewhere(facts: Facts, batch: Batch, table: Table, present: Boolean) =
    Option.when(present && table.uri != batch.target && table.uri != facts.quarantine.uri)(
      Violation(
        Rule.InAnotherTable,
        s"batch ${batch.seq} was planned for ${batch.target}, and is in ${table.uri}"
      )
    )

  /** A number of the source's sequence in a table that no batch has. */
  private def txns(facts: Facts): Seq[Violation] = {
    val known = facts.batches.map(_.seq).toSet
    (facts.data :+ facts.quarantine).flatMap { table =>
      table.commits.flatMap(commit => commit.txns.map(commit.version -> _)).collect {
        case (version, seq) if !known(seq) =>
          Violation(
            Rule.TxnOfNoBatch,
            s"${table.uri} has batch $seq at version $version, and the control plane has no " +
              "such batch"
          )
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Rows
  // -----------------------------------------------------------------------------------------------

  private def rows(facts: Facts): Seq[Violation] = {
    val runs = facts.runs.map(run => run.id.toString -> run).toMap
    val batches = facts.batches.map(batch => batch.seq -> batch).toMap

    def ofRuns(table: Table, isData: Boolean): Seq[Violation] = {
      val each = table.rows.flatMap { rows =>
        val label = s"${rows.count} rows of ${table.uri} under the run ${rows.run} and batch " +
          rows.batch
        (runs.get(rows.run), batches.get(rows.batch)) match {
          case (None, _) =>
            Seq(Violation(Rule.RowsOfNoRun, s"$label: the source has no such run"))
          case (Some(run), _) if !run.batch.contains(rows.batch) =>
            Seq(
              Violation(
                Rule.RowsOfAnotherBatch,
                s"$label: the run took batch ${run.batch.fold("none")(_.toString)}"
              )
            )
          case (_, None) =>
            Seq(Violation(Rule.RowsOfNoWrittenBatch, s"$label: the source has no such batch"))
          // A quarantine keeps the records of a batch that was given up (discovery §13.4).
          case (_, Some(batch)) if isData && batch.status == BatchStatus.Abandoned =>
            Seq(Violation(Rule.RowsOfNoWrittenBatch, s"$label: the batch was given up"))
          case (_, Some(batch)) if isData && batch.target != table.uri =>
            Seq(
              Violation(
                Rule.RowsOfNoWrittenBatch,
                s"$label: the batch was planned for ${batch.target}"
              )
            )
          case _ => Nil
        }
      }
      // One commit writes a batch to a table, so one run.
      val twice = table.rows.groupBy(_.batch).toSeq.sortBy(_._1).collect {
        case (batch, groups) if groups.map(_.run).distinct.size > 1 =>
          Violation(
            Rule.BatchByTwoRuns,
            s"batch $batch has rows of ${table.uri} under the runs " +
              groups.map(_.run).distinct.sorted.mkString(", ")
          )
      }
      // The run that wrote and committed a batch counted its rows from the commit (P6.5).
      val counted =
        if (!isData) Nil
        else
          table.rows.groupBy(rows => (rows.batch, rows.run)).toSeq.sortBy(_._1).flatMap {
            case ((seq, run), groups) =>
              for {
                batch <- batches.get(seq).toSeq
                if batch.target == table.uri && batch.committedBy.exists(_.toString == run)
                written <- runs.get(run).flatMap(_.rowsWritten).toSeq
                there = groups.map(_.count).sum
                if written != there
              } yield Violation(
                Rule.RowsNotAsCounted,
                s"the run $run wrote and committed batch $seq, and says $written rows written; " +
                  s"${table.uri} has $there"
              )
          }
      each ++ twice ++ counted
    }

    facts.data.flatMap(ofRuns(_, isData = true)) ++ ofRuns(facts.quarantine, isData = false)
  }

  // -----------------------------------------------------------------------------------------------
  // The state, the files taken and the runs
  // -----------------------------------------------------------------------------------------------

  private def state(facts: Facts): Seq[Violation] = {
    val last = facts.batches.filter(_.status == BatchStatus.Committed).maxByOption(_.seq)
    (last, facts.state) match {
      case (None, None) => Nil
      case (None, Some(state)) =>
        Seq(
          Violation(
            Rule.StateNotOfLastBatch,
            s"the source has a state at batch ${state.lastCommitted}, and no committed batch"
          )
        )
      case (Some(batch), None) =>
        Seq(
          Violation(
            Rule.StateNotOfLastBatch,
            s"batch ${batch.seq} is committed, and the source has no state"
          )
        )
      case (Some(batch), Some(state)) if lastReset(facts, batch).nonEmpty =>
        val reset = lastReset(facts, batch).get
        Option
          .when(state != reset.after)(
            Violation(
              Rule.StateNotOfLastBatch,
              s"the state is ${shown(state)}, and the reset by the run ${reset.run} after batch " +
                s"${batch.seq} left it ${shown(reset.after)}"
            )
          )
          .toSeq
      case (Some(batch), Some(state)) =>
        val wrong = Seq(
          Option.when(state.lastCommitted != batch.seq)(
            s"is at batch ${state.lastCommitted}, and the last committed is ${batch.seq}"
          ),
          Option.when(!batch.committedBy.contains(state.updatedBy))(
            s"was moved by the run ${state.updatedBy}, and batch ${batch.seq} was committed by " +
              batch.committedBy.fold("no run")(_.toString)
          ),
          Option.when(state.watermark != batch.watermark)(
            s"has the watermark ${state.watermark.fold("none")(_.noSpaces)}, and the plan of " +
              s"batch ${batch.seq} says ${batch.watermark.fold("none")(_.noSpaces)}"
          )
        ).flatten
        wrong.map(why => Violation(Rule.StateNotOfLastBatch, s"the state $why"))
    }
  }

  /** The last reset after the last batch committed, which the state is then of. */
  private def lastReset(facts: Facts, last: Batch): Option[Reset] =
    facts.resets.lastOption.filter(_.after.lastCommitted == last.seq)

  private def shown(state: State): String =
    s"(watermark ${state.watermark.fold("none")(_.noSpaces)}, batch ${state.lastCommitted}, " +
      s"by ${state.updatedBy})"

  /**
   * Each reset is of the state that there was: that of the reset before it, if no batch was
   * committed between them, or else that of the last batch committed. It moves no batch, and
   * says it was made by its run, a run that resets and that succeeded.
   */
  private def resets(facts: Facts): Seq[Violation] = {
    val committed = facts.batches.filter(_.status == BatchStatus.Committed).map(b => b.seq -> b)
    val byId = facts.runs.map(run => run.id -> run).toMap
    facts.resets.zipWithIndex.flatMap { case (reset, index) =>
      val was = facts.resets
        .take(index)
        .lastOption
        .filter(_.after.lastCommitted == reset.before.lastCommitted)
        .map(_.after)
        .orElse(committed.toMap.get(reset.before.lastCommitted).flatMap { batch =>
          batch.committedBy.map(State(batch.watermark, batch.seq, _))
        })
      val by = s"the reset by the run ${reset.run}"
      Seq(
        Option.when(!was.contains(reset.before))(
          s"$by says the state was ${shown(reset.before)}, and it was " +
            was.fold("no state of a committed batch")(shown)
        ),
        Option.when(reset.after.lastCommitted != reset.before.lastCommitted)(
          s"$by moved the last batch from ${reset.before.lastCommitted} to " +
            s"${reset.after.lastCommitted}: the sequence goes on"
        ),
        Option.when(reset.after.updatedBy != reset.run)(
          s"$by says the state was moved by ${reset.after.updatedBy}"
        )
      ).flatten.map(Violation(Rule.ResetNotChained, _)) ++
        (byId.get(reset.run) match {
          case Some(run) if run.kind == RunKind.Reset && run.status == RunStatus.Succeeded => None
          case found =>
            Some(
              Violation(
                Rule.ResetNotSucceeded,
                s"the state was reset by " + found.fold(s"${reset.run}, which is no run") { run =>
                  s"the run ${run.id}, of the kind ${run.kind.name}, which ended ${run.status.name}"
                }
              )
            )
        })
    }
  }

  private def files(facts: Facts): Seq[Violation] = {
    val committed = facts.batches
      .filter(_.status == BatchStatus.Committed)
      .flatMap(batch => batch.files.map(_ -> batch.seq))
      .toSet
    def show(files: Set[(TakenFile, Long)]) =
      files.toSeq.sortBy { case (file, seq) => (seq, file.path) }.map { case (file, seq) =>
        s"${file.path} (${file.size} bytes, ${file.modifiedMillis} ms) by batch $seq"
      }
    show(committed -- facts.taken).map(file =>
      Violation(Rule.FilesNotAsCommitted, s"$file: in a committed plan, and not taken")
    ) ++ show(facts.taken -- committed).map(file =>
      Violation(Rule.FilesNotAsCommitted, s"$file: taken, and in no committed plan")
    )
  }

  private def runs(facts: Facts): Seq[Violation] = {
    val byId = facts.runs.map(run => run.id -> run).toMap
    facts.runs.filter(_.status == RunStatus.Running).map { run =>
      Violation(Rule.StillRunning, s"the run ${run.id} is still running")
    } ++ facts.batches.filter(_.status == BatchStatus.Committed).sortBy(_.seq).flatMap { batch =>
      batch.committedBy.flatMap(byId.get) match {
        case Some(run) if run.status == RunStatus.Succeeded => None
        case found =>
          Some(
            Violation(
              Rule.CommitterNotSucceeded,
              s"batch ${batch.seq} was committed by " +
                found.fold(batch.committedBy.fold("no run")(id => s"$id, which is no run")) { run =>
                  s"the run ${run.id}, which ended ${run.status.name}"
                }
            )
          )
      }
    }
  }
}
