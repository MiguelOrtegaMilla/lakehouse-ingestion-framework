package lakehouse.ingestion.it.cli

import lakehouse.ingestion.core.config.Source
import lakehouse.ingestion.core.control.{Layer, LeaseKey, RunId}
import lakehouse.ingestion.engine.control.Leases
import lakehouse.ingestion.it.control.ControlPlaneSuite
import lakehouse.ingestion.it.support.ControlPlaneSchema.{query, Ran}
import org.scalatest.Suite

/**
 * What the suites of the command `run` ask of the control plane after an invocation, and of
 * what it printed: the command says nothing else to whoever started it.
 */
trait RunCommandQueries extends ControlPlaneSuite { self: Suite =>

  /** What the command said, but the warnings of the profile: its lake is under /tmp. */
  protected def said(ran: Ran): Seq[String] = ran.errors.filterNot(_.startsWith("warning: "))

  /** The runs of a source, each with how it ended, in the order they started. */
  protected def runsOf(of: Source): Seq[(String, Option[String])] = asSuperuser { c =>
    query(
      c,
      "SELECT status, error_class FROM control.ingestion_run WHERE source_id = ? ORDER BY started_at",
      of.id.value
    ).map(row => (row(0), Option(row(1))))
  }

  /** The batches of a source, by their number, each with its status. */
  protected def batchesOf(of: Source): Seq[(String, String)] = asSuperuser { c =>
    query(
      c,
      "SELECT batch_seq::text, status FROM control.ingestion_batch WHERE source_id = ? ORDER BY 1",
      of.id.value
    ).map(row => (row(0), row(1)))
  }

  /** The rows that each run of a source that succeeded wrote, in the order they started. */
  protected def rowsWritten(of: Source): Seq[String] = asSuperuser { c =>
    query(
      c,
      "SELECT rows_written::text FROM control.ingestion_run WHERE source_id = ? AND " +
        "status = 'SUCCEEDED' ORDER BY started_at",
      of.id.value
    ).map(_.head)
  }

  /** The run that holds the lease of a source, if one does. */
  protected def holderOf(of: Source): Option[RunId] =
    database.run(Leases.find(_, LeaseKey.of(of.id, Layer.Bronze))).flatMap(_.holder).map(_.run)
}
