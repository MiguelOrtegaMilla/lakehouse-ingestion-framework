package lakehouse.ingestion.logstore.coordination

import java.util.UUID

/**
 * A writer's bid for one commit version of a table.
 *
 * @param tempPath where the commit's bytes are, relative to `_delta_log`. It contains a UUID of
 *   this attempt, so it also identifies the claim: an entry with this path belongs to this
 *   writer, whichever call created it.
 * @param writerInstance `host:pid:spark.app.id`, for the audit trail
 * @param runId the engine run that commits, when the writer knows it (ADR-0004 §3.7)
 */
final case class Claim(
    table: TableUri,
    version: Long,
    tempPath: String,
    writerInstance: String,
    runId: Option[UUID]
) {
  require(version >= 0, s"a commit version cannot be negative: $version")
  require(tempPath.nonEmpty, "a claim needs the path of its temporary file")
  require(writerInstance.nonEmpty, "a claim needs the instance that makes it")
}

/**
 * A commit version that belongs to a writer (ADR-0004 §3.1).
 *
 * From the moment the entry exists, the commit is durable: whoever finds the entry incomplete
 * publishes `tempPath` as the commit file.
 *
 * @param complete the commit file is published and that was acknowledged
 */
final case class CommitEntry(
    table: TableUri,
    version: Long,
    tempPath: String,
    complete: Boolean,
    writerInstance: String,
    runId: Option[UUID]
)
