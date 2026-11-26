# ADR-0022 — A failure of a run is classified once, and a run that may pass resumes where it is durable

| | |
|---|---|
| **Status** | Accepted (P8.1, 2026-11-26) |
| **Supersedes** | [ADR-0010](0010-concurrent-write-conflict-handling.md) §2, the retry policy: a count of attempts (5, with a backoff from 500 ms to 30 s). A budget of time from the profile takes its place, shared with the failures that pass. The rest of ADR-0010 stands, its classification of the conflicts of Delta first. |
| **Related** | [ADR-0005](0005-batch-wal-and-idempotent-writes.md), [ADR-0006](0006-source-leases-with-epoch-fencing.md), [ADR-0008](0008-layer-responsibilities.md), [ADR-0018](0018-a-run-gives-up-a-batch-whose-source-changed.md), [ADR-0019](0019-a-commit-names-the-epoch-of-its-lease.md), [ADR-0021](0021-operator-actions-and-backfills.md) |

## Context

The framework does not schedule itself. The exit code of a command is all that an orchestrator has to tell "run it again" from "do not", and the discovery fixes what each code means (§14.1). The class of a failure is what decides the code, and what the run records of itself.

Up to P7 a failure has a class only where it is raised by something that knows what it is (`RunFailedException`): a configuration that is refused, a plan that cannot be read, a failure of a JDBC source as its connector tells it (`SourceConnector.failure`), the data threshold. What comes from inside a library carries no class: an error of the control plane in the middle of a run, a commit that the LogStore refuses for a table that is tainted or whose registry has a commit that its log does not, the coordination database not answering, an error of Spark around any of these. Such a run is left as a process that died leaves it, with its lease running out, and the process ends with the JVM's 1, which is no code of the contract. §14.1 has 4 for a tainted table, and nothing gives it.

Nothing is retried either. The profile has a budget of time for the failures that pass, and the backoff between attempts (`retries`, P3), with one rule held when it is loaded: the budget must be longer than the LogStore itself waits for the coordination database before a commit fails (P2b.5). Nothing reads it. A failure that passes ends the run in 3 at once, and the next trigger of the orchestrator is what recovers it. ADR-0010 §2 has a count of attempts, which P3 replaced with a budget of time without a record of it.

Two more things wait for this decision (discovery §20). The transaction that ends a run may be committed by the database with its answer lost, and the run then says that it failed. And a conflict that asks for a re-plan ends in 3 once its one retry is spent, where §14 has 4.

## Decision

### 1. The classes, and what each ends in

| Class | What | Retried in the run | `error_class` | Exit |
|---|---|---|---|---|
| Transient | What may pass by itself: a connection lost, a timeout, a database that is busy or not answering, the coordination database among them | Yes, within the budget (§5) | `transient` | 3 |
| Conflict, retryable | The conflicts of Delta that ADR-0010 §1 retries: another writer changed what a transaction read or removed | Yes, within the same budget, counted apart | `conflict` | 3 |
| Conflict, re-plan | `MetadataChangedException`, `ProtocolChangedException` | Once, on the table as it is now, where Delta checks the rows against its schema and protocol anew. Then a permanent failure. P9 adds the schema policy of the source to what is looked at again (ADR-0010 §1) | `permanent` | 4 |
| Conflict, identity | `ConcurrentTransactionException` | Never: the table is asked for the batch, as ADR-0010 §1 has it. A table that has it rolls the batch forward; one that does not is a permanent failure | `permanent` | 0 or 4 |
| Permanent | What running again does not mend, until somebody changes something | Never | `permanent` | 4 |
| Data | More records rejected than the source allows | Never: the same data gives the same result | `data` | 5 |
| Internal | A failure that nothing classified: a mistake of the application | Never | `internal` | 70 |

A run that lost its lease (ADR-0006, ADR-0019) and a batch whose source changed under its plan (ADR-0018) keep their own handling, and are not retried in the run: another run does the work, or the plan is what is wrong. Both end in 3.

`internal` is a new name of `error_class`, and the only one: a re-plan or an identity that fails is recorded as `permanent`, which is what it is for whoever reads the audit trail.

### 2. Where a failure is classified

What knows the class of a failure where it raises it keeps raising it with its class. Everything else is classified in one place: where the run ends, for every kind of run, by one classifier that reads the chain of causes of the failure, to a bounded depth. In this order:

1. the loss of the lease, and a commit that the LogStore refused for its epoch, whatever they are wrapped in, as up to P7;
2. a failure that has its class;
3. the classifier¹:
   - the LogStore. The coordination database not answering may pass, and so does what passes once the transaction starts again from the table as it is now: a registration that an operator changed under the commit, and a commit that took its version and whose publication is pending, which the next look at the table publishes. A table that the LogStore refuses to commit to for what it knows of the table (tainted, not registered, a commit that its registry has and its log does not, a write that it did not coordinate), or of itself (a coordination schema of another version), does not: the message names the table, and the command that mends it;
   - the conflicts of Delta, by ADR-0010 §1;
   - an error of a database, by its SQLState (`DatabaseFailure`, discovery §14): of the coordination database when the LogStore has wrapped it in a failure of I/O, and of the control plane otherwise;
4. otherwise, `internal`.

The connector of a source is not asked here. It says what a failure of its source is where its source is read: inside its plan and its read, and over the rows that are read as they are written (`SourceConnector.failure`), as up to P7. Where a run ends it would take a failure of the control plane for its source's, since the JDBC connector takes a failure of SQL that nothing of I/O wraps for its database's.

¹ *Corrected in P8.2 (2026-11-26).* This list first had the connector of the source among what the classifier asks (`SourceConnector.failure`), had the errors of a database before the LogStore, and had of the LogStore only a database that does not answer and the refusals of a table. The connector is asked where its source is read, as it was; a failure of SQL is told from the LogStore's by what wraps it, so the LogStore is looked at first; and two failures of the LogStore pass once the transaction starts again. The decision is unchanged: one classifier where the run ends, for what nothing classified where it was raised.

### 3. A failure that nothing classified

The run is recorded as `FAILED` with the class `internal`, and its lease is given up, in the one transaction in which any run that fails says so. The failure is logged whole. The audit trail then says that the application made a mistake, and not that a process died; and the next trigger of the source is not skipped until the lease runs out. Its batch, if it planned one, stays `PLANNED`, and the next run recovers it (ADR-0005). If the control plane cannot be told, the lease runs out, as that of a process that died.

`Main` ends the process in 70 for whatever nothing classified, in any command, after it has logged it whole. The fatal errors of the JVM are not caught: the process ends as the JVM ends it.

### 4. A retry resumes the run where it is durable

A retry is made by the same run, under the same lease, with the same `run_id`. A run is an attempt to take a batch (ADR-0005), and its retries are counted in it (`ingestion_run.retries`).

Until its batch is committed, a run resumes at step 2 of ADR-0005: the batch planned is found, its table is asked, and the batch is replayed or rolled forward. A plan does not change, and Delta skips a write that a table has already, so a quarantine that was written before the failure is not written again. The path of a retry is the path of recovery, which P7 tests at every fault point of the engine and inside the commits.

A failure at the transaction that ends a run is §6.

Only what is transient and the retryable conflicts are retried.

### 5. The budget

- **Its numbers are those of the profile**: `retries.budget`, `retries.first_backoff` and `retries.max_backoff` (P3; by default 2 minutes, 1 second and 30 seconds). The budget is a run's, and is counted from its first failure.
- **The wait before attempt n** (n from 1) is drawn evenly from 0 to the smaller of `max_backoff` and `first_backoff × 2^(n−1)` (full jitter), and is cut to what is left of the budget. Writers that collided do not try again in step. No attempt starts once the budget is spent.
- **How long a run with retries takes, at most.** The attempt in course when the budget runs out is let finish: stopped halfway, it would leave what recovery takes up anyway, and gain nothing. So, counted from its first failure, a run ends within the budget plus the length of one attempt. An attempt waits for no more than the timeouts of the profile and the LogStore's own wait allow, and stops when its lease is lost, and its work is that of one batch. Whoever bounds a run from outside, as the timeout of a task of an orchestrator, gives it that much beyond the time to its first failure.
- **One budget for both kinds of retry.** The failures that pass and the retryable conflicts draw on the same budget, and are counted apart: `retries` and `write_conflicts` of `ingestion_run`.
- **The lease is looked at before each attempt**, and a wait ends when it is lost. With the control plane down the heartbeat renews nothing, and the run stops by its own count, five sixths of the time to live after its last renewal (100 seconds by default), before a budget of 2 minutes is spent: it ends as a run that lost its lease, in 3, as one whose retries ran out would.
- **Taking the lease is retried the same way.** Until it answers, the run has no row: its retries are recorded in the row that it then has.
- **`validate` and `migrate` are not retried.** `validate` opens nothing, and `migrate` tries once (P4.2): its 3 says to run it again.

### 6. A commit whose answer is lost

The transaction that ends a run may be committed by the database with its answer lost: the commit of a batch with its state (ADR-0005, step 7), that of a run that found nothing new, and that of an operator's action (ADR-0021). A run that gets an error from it reads its own row before it says how it ended:

- `SUCCEEDED`: it ended well, and its batch is committed, if it had one;
- `RUNNING`: the transaction was not committed. It is retried within the budget, and its own conditions, the epoch and a batch that is still planned, take it or refuse it as ever;
- any other status is what happened, as far as the system can tell: `ABANDONED` is a run whose lease another run took.

The read is retried within the budget too. A budget that runs out ends the run in 3: what happened is in the control plane, and the next run of the source finds it.

## Alternatives

- **Each step retried where it failed.** It needs a reasoning of its own for each step, of what an attempt that failed halfway left, and grows a second path of recovery beside the one that P7 tests. Resuming at step 2 asks only what recovery asks.
- **No retries inside the process.** Correct and simpler: the run fails as transient, gives its lease up, and the next trigger recovers it. Each blip then costs a JVM and a Spark session started again, and a backoff that the orchestrator has to have; and the budget and its rule were decided in P3 and P2b.5.
- **A count of attempts** (ADR-0010 §2). An attempt against a database that is down takes the connect timeout to fail, so a count says little of how long an outage is ridden out; a budget of time is what the rule of P2b.5 can be held against.
- **A new run for each retry.** A run holds the lease, and a new one would take another with another epoch, between two attempts of one batch. How many retries a batch took would become a question across rows.
- **A budget for each class.** Two numbers to set and to explain, for failures that a run seldom meets together.
- **A run that nothing classified, left for its lease to run out**, as a process that died. The audit trail would say that it died, and its source would be skipped until then.
- **The transaction that ends a run made again, in place of the row read back.** It is refused for a batch that is committed already, as an error: the run would fail for a commit that it made.

## Consequences

- The retry of a run is its recovery: nothing new is to be trusted for it.
- A batch of a table that is read as it is written reads its window of the source again when it is retried, which counts against the source database. The window is bounded by `max_batch_range`.
- The appends of Bronze never meet a retryable conflict (ADR-0010). The class is classified from P8 and shares the loop of the failures that pass, and is shown end to end by the `MERGE` of Silver (P10) and by `OPTIMIZE` beside ingestion (P11).
- `retries` and `write_conflicts` are counted by the engine, and are real metrics of a run.
- 70 is an alert for a mistake of the application, and the run that ended in it is recorded as such.
- An orchestrator that bounds a run gives it more than `retries.budget` plus one attempt beyond its first failure, or it stops a run that would have ended well.

## As built (P8.2, 2026-11-26)

- **The classifier is `Failures`, in the root of the engine**, beside the service that ends a run, and not in the package of `RunFailedException`: it reads `DatabaseFailure`, whose package depends on the connectors, which raise the failures of that package, and the build refuses a cycle among the packages of a module.
- **What the LogStore says, by its types.** Permanent: `TableTaintedException`, `TableNotRegisteredException`, `StaleRegistrationException`, `UncoordinatedWriteException`, `CommitMissingException`, `UnpublishableCommitException` and `CoordinationSchemaException`. Transient: `RegistrationChangedException` and `CommitPublicationPendingException`. A commit refused for its epoch (`EpochNotInForceException`) is the loss of the lease, as before (§2, point 1). Any other failure of I/O has no class.
- **The lease that cannot be taken** is classified too. Such a run has no row, and nothing is told; if the database took the lease and its answer was lost, the lease runs out, and the run that takes it over ends this one.
