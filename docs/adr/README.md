# Architecture Decision Records

Each ADR records one decision: its context, the decision, the alternatives considered and the consequences. ADRs are immutable once accepted. A change of mind is a new ADR that supersedes the old one.

| ADR | Title | Status |
|---|---|---|
| [0001](0001-development-environment.md) | Development environment: WSL2 + Docker Engine + VS Code/Metals | Accepted |
| [0002](0002-technology-stack.md) | Technology stack and versions | Accepted |
| [0003](0003-control-plane-in-postgresql.md) | Control plane in PostgreSQL | Accepted |
| [0004](0004-postgresql-coordinated-delta-logstore.md) | PostgreSQL-coordinated Delta LogStore for multi-writer tables — **highest-risk component** | Accepted |
| [0005](0005-batch-wal-and-idempotent-writes.md) | Batch write-ahead log and idempotent Delta writes | Accepted |
| [0006](0006-source-leases-with-epoch-fencing.md) | Source leases with expiry, heartbeat and epoch fencing | Accepted |
| [0007](0007-metadata-format.md) | Source metadata as YAML with startup validation | Accepted |
| [0008](0008-layer-responsibilities.md) | Layer responsibilities: Landing, Bronze, Silver (Gold out of scope) | Accepted |
| [0009](0009-delete-detection.md) | Detecting deletes in the source | Accepted |
| [0010](0010-concurrent-write-conflict-handling.md) | Handling concurrent write conflicts on shared Delta tables | Accepted |
| [0011](0011-storage-profiles.md) | Lake storage profiles and canonical table identity | Accepted |
| [0012](0012-module-structure.md) | sbt multi-module structure and project identity | Accepted |
| [0013](0013-application-security-baseline.md) | Application security baseline: SQL identifiers, path confinement, least privilege, supply chain | Accepted |

Superseded decisions: *AD-06 "one writer per Delta table"* (discovery v1) was replaced by ADR-0004 before being recorded as an ADR.

Schema evolution (P9) gets its own ADR in its phase, once the policies are validated against Delta 4.4's type-widening support.

## Template

```markdown
# ADR-NNNN — Title

| | |
|---|---|
| **Status** | Proposed · Accepted · Superseded by ADR-XXXX |
| **Related** | … |

## Context
## Decision
## Alternatives
## Consequences
```
