package lakehouse.ingestion.logstore

import java.io.IOException

import lakehouse.ingestion.logstore.coordination.TableUri

/**
 * The table's registration does not allow the write (ADR-0004 §3.5, layer 2). Nothing was
 * written. Retrying does not help: an operator has to look at the table and at its registry.
 */
sealed abstract class WriteRefusedException(message: String) extends IOException(message)

/**
 * The table is not registered. It was neither created through this LogStore nor adopted, or it
 * is being reached through another path than the one it is registered under.
 */
final class TableNotRegisteredException(val table: TableUri)
    extends WriteRefusedException(
      s"$table is not a coordinated table: it was not created through this LogStore and has " +
        "not been adopted, or this is not the path it is registered under"
    )

/** A commit made outside the protocol was detected on the table, and nobody has cleared it. */
final class TableTaintedException(val table: TableUri, val reason: String)
    extends WriteRefusedException(
      s"$table is tainted and refuses writes until an operator verifies and re-adopts it: $reason"
    )

/**
 * The registry records commits that the table's log does not have: the table was deleted and
 * created again at the same path, or its log was cut short. The entries of its earlier life
 * would be taken for commits of the new one, so the registry has to be reset first.
 */
final class StaleRegistrationException(val table: TableUri, known: String)
    extends WriteRefusedException(
      s"$table is being written from an earlier version than its registry knows: $known. " +
        "If the table was deleted and created again, reset its registry first"
    )

/**
 * The commit took its version and its file could not be given its name (ADR-0017). The commit
 * will appear: the next reader or writer of the table publishes it. The caller must therefore
 * not assume that it failed, and has to look at the table before writing the same data again.
 */
final class CommitPublicationPendingException(
    val table: TableUri,
    val version: Long,
    cause: IOException
) extends IOException(
      s"version $version of $table is committed and its publication is pending: the next " +
        s"reader or writer of the table publishes it (${cause.getMessage})",
      cause
    )

/**
 * A commit that this protocol did not make is in the table's log (ADR-0004 §3.5, layers 3 and
 * 4): some writer runs without this LogStore. Nothing was replaced.
 */
final class UncoordinatedWriteException(val table: TableUri, val version: Long, found: String)
    extends IOException(s"uncoordinated write detected on $table at version $version: $found")

/**
 * The latest commit of the table is recorded as published, and its file is not in the log. The
 * file was deleted, or this is not the storage the table's writers use, or the table was
 * dropped by deleting its directory. A reader would be given an older table for the current
 * one, so nothing is read.
 */
final class CommitMissingException(val table: TableUri, val version: Long)
    extends IOException(
      s"version $version of $table is committed and acknowledged, and its file is not in the " +
        "log: it was deleted, or this is not the storage its writers use. If the table was " +
        "dropped, reset its registry"
    )

/**
 * The entry of a commit points at a temporary file that no longer exists, and the commit is not
 * published: its bytes are gone. Nothing can publish it, and nothing may take its version.
 */
final class UnpublishableCommitException(
    val table: TableUri,
    val version: Long,
    val tempPath: String
) extends IOException(
      s"version $version of $table is committed, and its temporary file $tempPath no longer " +
        "exists: the commit cannot be published, and an operator has to resolve its entry"
    )
