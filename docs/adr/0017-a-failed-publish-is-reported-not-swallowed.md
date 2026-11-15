# ADR-0017 — A publish that fails is retried and reported, not swallowed

| | |
|---|---|
| **Status** | Accepted (P2a, 2026-11-14) |
| **Supersedes** | In [ADR-0004](0004-postgresql-coordinated-delta-logstore.md): how errors after the `INSERT` are handled (§3.2), and the part of deviation 4 (§4) that ignores a failed publish. The rest of ADR-0004 stands. |
| **Related** | [ADR-0005](0005-batch-wal-and-idempotent-writes.md) |
| **Verified against** | Delta 4.4.1, `OptimisticTransaction.FileSystemBasedCommitCoordinatorClient.commit`. Pinned by `DeltaLogStoreContractSpec`: "Delta reads the commit file's status as soon as write returns". |

## Context

Once the `INSERT` of entry N is committed, version N is durable: any reader or writer will publish `N.json` (ADR-0004 §3.2). ADR-0004 therefore followed `BaseExternalLogStore`: an error while publishing or acknowledging is logged and swallowed, and `write` returns normally.

Delta 4.4 does not let that work. As soon as `write` returns, it reads the file status of `N.json`. If the publish failed and was swallowed, that read throws a `FileNotFoundException` and the transaction fails anyway:
- with an error that names a missing file, not a failed publish;
- for a commit that is durable and will appear when the next reader arrives.

Swallowing does not give the caller a successful commit. It only hides what happened.

## Decision

After the `INSERT`:
- **A failed publish is retried inside `write`**, a bounded number of times.
- **If `N.json` is still not visible, `write` throws a dedicated `IOException`.** It says that version N is committed and that its publication is pending. The next reader or writer publishes it.
- **A failed acknowledgement is still logged and swallowed.** `N.json` is visible, so the commit is complete for Delta, and recovery marks the entry.
- **Fatal integrity errors are never swallowed**, as ADR-0004 already decided (a foreign `N.json`, deviation 4).

## Alternatives

| Option | Verdict |
|---|---|
| Swallow the error (ADR-0004 as written, and the base class) | The transaction fails anyway, with a misleading error. Rejected. |
| Retry until the publish succeeds | A storage fault becomes a hang. Failing closed means failing in bounded time. Rejected. |
| Undo the entry when the publish fails | The `INSERT` is the durability point: another process may already be publishing that entry. The role on the commit path cannot delete rows either (ADR-0013 §3). Rejected. |

## Consequences

- **The caller sees a failed commit that will nevertheless appear.** It cannot tell that from an ordinary failure by catching `IOException` alone, so it must check before replaying. The engine always does: before replaying a batch it reads the table's `txn` version (ADR-0005). The dedicated exception lets the error classifier name the case (P8).
- Scenario **F16** tests it (ADR-0004 §7.1): a publish that fails without a crash, the exception, and the next `listFrom` publishing the commit.
- The number of retries, the backoff and the exception's name are fixed with the implementation (P2a.8).

## As built (P2a.8, 2026-11-15)

- The exception is `CommitPublicationPendingException`. It carries the table and the version.
- A publish is tried three times, 100 ms and then 200 ms apart (`RetryPolicy.Default`): a storage that stays broken fails the commit in well under a second.
- The same policy repeats the claim of a version when its outcome is unknown (ADR-0004 §3.2, F15).
- A publish that finds another file under the commit's name, or its own temporary file gone, is not a pending commit and is not retried: it is reported as `UncoordinatedWriteException` or `UnpublishableCommitException`.
