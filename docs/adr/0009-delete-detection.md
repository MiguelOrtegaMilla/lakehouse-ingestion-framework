# ADR-0009 — Detecting deletes in the source

| | |
|---|---|
| **Status** | Accepted (2026-09-25) |

## Context

An incremental extraction by `updated_at` cannot see rows deleted in the source.

## Decision

| Load mode / source capability | Strategy |
|---|---|
| Full load | Implicit: rows absent from the snapshot are soft-deleted in Silver with `MERGE … WHEN NOT MATCHED BY SOURCE THEN UPDATE SET _is_deleted = true, _deleted_at = …` |
| The source has a soft-delete column (`deleted_at`, `is_deleted`) | Use it (`deleted_flag_column` in metadata); zero extra cost |
| Incremental, no soft-delete column | **Key reconciliation**: an explicit `reconcile-deletes` run reads only the primary-key column(s) from the source, anti-joins them with the current Silver keys and soft-deletes the missing ones. The orchestrator invokes it periodically. Deletes are detected one reconciliation cycle late. |
| Real CDC (Debezium / logical replication) | Future evolution |

- Deletes in Silver are **soft** (auditability).
- Physical deletion is reserved for GDPR erasure (discovery §11), followed by `VACUUM`.
- The reconciliation `MERGE` of a fan-in table is scoped to the source's partition (ADR-0010), so it never soft-deletes another source's rows.

## Alternatives

- **A periodic full reload for every table**: correct but too expensive for large tables. Kept as an option for small ones (`mode: full`).
- **Triggers in the source**: invasive, and usually not allowed.
- **CDC now**: needs Kafka/Connect infrastructure; out of scope.

## Consequences

Delete latency is bounded by the reconciliation frequency, and this is documented per source. Reconciliation reads only an indexed key column, which is cheap on the source.
