# ADR-0011 — Lake storage profiles and canonical table identity

| | |
|---|---|
| **Status** | Accepted (consequence of D3, 2026-09-25) |
| **Related** | [ADR-0004](0004-postgresql-coordinated-delta-logstore.md), [ADR-0010](0010-concurrent-write-conflict-handling.md) |

## Context

The design must be correct for multi-instance, multi-node and multi-cluster deployments (D3):
- **multi-instance**: several drivers on one machine;
- **multi-node**: executors on several machines;
- **multi-cluster**: independent applications or clusters.

In a multi-node cluster **executors write the data files**, so the storage must be shared by every node. Drivers write the commits through the LogStore. Mutual exclusion also depends on every writer naming each table identically (ADR-0004 §3.6).

## Decision

### 1. The lake URI is a profile setting; tables are relative to it

```yaml
# conf/app/local.yaml
lake:
  uri: "file:///home/<user>/lake"     # inside the WSL ext4 filesystem, never /mnt/c
  deployment: single-machine
# conf/app/multinode.yaml (E1)
lake:
  uri: "s3a://lake/"                  # MinIO; path-style access, endpoint via Hadoop conf
  deployment: cluster
```

Source metadata refers to tables relatively (`bronze/sales/orders`). A `TableLocator` resolves them to one canonical URI. The MVP ships `PathTableLocator`; a future `CatalogTableLocator` would address `catalogManaged` tables by name without changing the engine.

### 2. Validation rules at startup

| Rule | Reason |
|---|---|
| Exactly one scheme per profile, and the LogStore registered for it (ADR-0004 safeguard 1) | `s3://` vs `s3a://` would be two coordination keys for one table |
| `file://` is allowed only with `deployment: single-machine`, or with an explicit `shared_filesystem: true` (e.g. NFS) that is logged as a warning | Executors on other nodes cannot see a local disk. NFS rename and listing semantics need their own verification. |
| A `local[*]` master with `deployment: cluster` is allowed (tests); a cluster master with `single-machine` + `file://` is rejected | Prevents silently writing data files to each worker's own disk |
| Canonicalisation: qualified URI, user info stripped, symlinks resolved for `file://`, trailing slashes normalised | One table ↔ one key |
| *(Note, 2026-11-17, at the start of P3.)* The canonicalisation above is split in two. `core` validates and normalises a table's URI by its text alone (scheme, user info, slashes, relative segments), at startup validation. Resolving symbolic links needs the file system, and stays in the LogStore's `TableIdentity` (ADR-0004 §3.6), where the engine applies it when it starts (P5) | `core` has no Hadoop and may not depend on the LogStore (ADR-0012). `TableIdentity` gives the key that commits are coordinated under, so there is one implementation of what touches the file system, and a test holds `core`'s answer to it for a path without links |
| `s3a` credentials only through environment variables / Hadoop credential providers, never YAML | Security (ADR-0007) |

*As built (P3.3, 2026-11-17):* `Profile` in `core`, validated from the text of the file alone.
- **A root is `file:///absolute/path` or `s3a://bucket/prefix`**, with segments of the grammar of relative paths (ADR-0013 §2) and no user, password or query. `s3://` is refused with the reason of the first row above.
- **`landing.root` and `lake.uri` must be apart**: neither is the other, nor inside it. Files that land are never read as tables.
- **A database is `jdbc:postgresql://host:port/database` and nothing after it.** TLS and timeouts are fields of their own, and a parameter in the URL would win over them without a word; some parameters of the driver also name a class for it to load (CVE-2022-21724).
- **TLS is `disable` or `verify-full`**, with no mode in between (ADR-0013 §5). `disable` to a host that is not this machine is a warning.
- **The profile has the timeouts of each database and the retry budget of the engine**, and one rule about them: the budget is longer than the LogStore itself waits for the coordination database, its connect timeout times its three attempts. A batch without that database takes 5 s to fail (P2b.5), and a shorter budget would give up on outages that would have passed.
- **Spark settings may not name a LogStore, a master or a credential.**
- **Nothing is connected to and no name is looked up.** A profile whose databases cannot be reached validates in milliseconds.

### 3. Everything a recovering node needs lives next to the table

The LogStore temp files are in `<table>/_delta_log/.tmp/` on the shared storage, so any driver on any node or cluster can finish a half-published commit. Nothing correctness-related is kept on local disk or in JVM memory. Spark's shuffle and scratch directories stay node-local, as usual.

### 4. Object storage in extensions

MinIO is strongly consistent for listings, which satisfies Delta's *consistent listing* requirement. `hadoop-aws` must match the Hadoop version bundled with the project's Spark version (ADR-0014: Hadoop 3.4.2) (verified in E1). The same coordination DB and the same lake URI are shared by every cluster in E2.

## Consequences

- Moving from multi-instance to multi-node or multi-cluster changes **only the profile** (plus infrastructure), not code. That property is what the extensions demonstrate.
- One operational rule to document loudly: every writer, in every cluster, uses the same lake URI and the same coordination database.
