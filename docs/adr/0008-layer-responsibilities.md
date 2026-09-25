# ADR-0008 — Layer responsibilities: Landing, Bronze, Silver (Gold out of scope)

| | |
|---|---|
| **Status** | Accepted (2026-09-25) |

## Decision

| Layer | Responsibility | Explicitly not |
|---|---|---|
| **Landing** (files only) | The producer's drop zone, **immutable from the framework's point of view**. Per-source path, "complete file" rule (minimum age / temp-name exclusion / marker file). Processed files are recorded in the control plane. | Copying or moving files. That would be a side effect not atomic with the Delta commit, and it would break replay. |
| **Bronze** | Fidelity to the source + minimal ingestion metadata (`_run_id`, `_batch_seq`, `_ingested_at`, `_source_file`). JDBC keeps native types. **CSV/JSON are kept as `string`** + `_rescued_data`. Written by idempotent append. | Typing text, deduplication, renames, business logic |
| **Silver** | Generic standardisation: casting to the declared schema, `snake_case` names, deduplication by key with an ordering guard, `MERGE` upsert (SCD1), soft deletes, PII pseudonymisation | Joins across sources, aggregates, business rules, KPIs |
| **Gold** | **Out of scope.** Use-case-specific business modelling belongs to downstream tools (dbt, domain jobs). | — |

**Why Bronze keeps text formats as strings.** If Bronze typed a CSV, a `"12,5"` in a decimal column would fail *in Bronze*, losing both fidelity and the original record. With strings, Bronze only rejects what is **structurally** broken (invalid JSON, wrong column count), and Silver is the typing boundary. Cast failures go to Silver quarantine together with the original Bronze record. This is also Auto Loader's default behaviour (`inferColumnTypes=false`).

**Write modes.** Bronze: append only (a history of what was received; full loads are appended as snapshots and pruned by retention). Silver: `MERGE` by key with an ordering guard (`source.updated_at >= target.updated_at`), so late or replayed data never overwrites newer data. Fan-in Silver tables are partitioned by the writer-separating column (ADR-0010).

**Errors by layer.**
- Landing: unreadable file → `FAILED` + policy (`fail`/`skip`).
- Bronze: structural corruption → quarantine; rejection ratio over threshold → the batch is not committed.
- Silver: cast failures → quarantine.

## Alternatives

- **Typed Bronze.** Rejected, for the fidelity reason above.
- **A copy-based landing zone.** Rejected: an extra copy with no guarantee gained.
- **Gold in scope.** Rejected: it contradicts "configuration, not code".

## Consequences

The boundary is testable: any transformation proposed for Silver must be expressible generically from metadata. If it needs knowledge of the business, it does not belong here.
