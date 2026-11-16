# ADR-0013 — Application security baseline

| | |
|---|---|
| **Status** | Accepted (Phase 0 closure, 2026-11-10) |
| **Related** | [ADR-0003](0003-control-plane-in-postgresql.md), [ADR-0004](0004-postgresql-coordinated-delta-logstore.md), [ADR-0007](0007-metadata-format.md), [ADR-0011](0011-storage-profiles.md) |

## Context

Discovery §11 covers **data governance**: secrets, PII, masking and GDPR. It does not cover the **application's own attack surface**: the places where input crosses a trust boundary into code that acts on it.

| Input | What the code does with it | Threat |
|---|---|---|
| Source metadata (YAML) | Identifiers become SQL; paths become filesystem access | SQL injection; path traversal; parser abuse |
| Landing directory | Written by producers; listed and read by the engine | A symlink or crafted name reaching files outside the landing root |
| Coordination and control databases | Every commit to every table goes through the coordination store | An over-privileged role turns a bug or a leaked credential into corruption of every table |
| Dependencies and container images | Run with the engine's privileges | Known vulnerabilities; unpinned images changing under us |

**Configuration is treated as untrusted input to the engine**, although it is reviewed in git, for three reasons:
- defence in depth;
- the metadata may later come from tables or a UI (ADR-0007, future evolution);
- a typo must never become a destructive statement.

## Decision

Levels follow the project convention: *Required*, *Recommended*, *Enterprise*, *Future*.

### 1. SQL — validated identifiers, no concatenated SQL (Required)

**Identifiers.** Every identifier that comes from configuration is parsed into a `SqlIdentifier` value type in `core` during startup validation. That covers `connector.table`, `watermark.column`, `keys`, `columns` and `partition_by`.
- The grammar is an **allow-list**: `[A-Za-z_][A-Za-z0-9_]*`, at most 63 bytes (PostgreSQL `NAMEDATALEN − 1`), at most `schema.table` qualification. Anything else is a validation error that names the file and the field.
- Names that need quoting (spaces, hyphens) are rejected in the MVP. Identifiers are always quoted when used, so a wider grammar would also be safe. Relaxing the grammar later is a validation change, not a security change.

**No SQL assembled from config or data values:**
- **Our own JDBC** (control plane, LogStore): `PreparedStatement` with bind parameters only. SQL text is a compile-time constant, and schema names are fixed by the migrations.
- **Spark JDBC reads:**
  - The table goes to the `dbtable` option as the identifier quoted by the dialect (`JdbcDialects.get(url).quoteIdentifier`). The `query` option is never fed assembled text.
  - Watermark bounds and key filters are DataFrame predicates (`col(c) > lit(v)`). Spark pushes them down and renders the literals through the dialect.
  - Spark writes the final SQL; our code concatenates nothing. Pushdown is also the performance requirement (§18 of the brief), so the safe path and the fast path are the same path.

**Tests:**
- **Unit (P3)**, malicious corpus, all rejected:
  - `orders; DROP TABLE orders` and `orders--`;
  - `orders" OR "1"="1` and `public.orders.extra`;
  - empty, whitespace-only, 64-byte and NUL-containing names;
  - Unicode look-alikes (Cyrillic `о` in `оrders`).
- **Integration (P6)**, against real PostgreSQL:
  - a text watermark or key value `'; DROP TABLE canary; --` is read back as data, and `canary` still exists;
  - a mixed-case table name is quoted correctly.

### 2. Paths confined to allowed roots (Required)

Each lake profile declares a `landing.root` next to `lake.uri` (ADR-0011). Source paths and target table paths are **relative** to their root.

- **Static validation (P3):** reject absolute paths, URI schemes, `..` or empty segments, backslashes, percent-encoded sequences and NUL.
- **Runtime check (P5):**
  - The engine canonicalises with the same rules as ADR-0011, which resolve symlinks on `file://`. The result must lie under the canonical root.
  - The check runs on the source directory **and on every listed file**, because a symlink inside the landing directory can point anywhere.
  - A file outside the root is handled like an unreadable file (ADR-0008): a `FAILED` record with reason `path_outside_root`, then the source's file-error policy. It is never skipped silently.
- **Targets** obey the same rule. `bronze/../../x` would write outside the lake. On `s3a://`, containment means the same bucket and key prefix.
- **Tests:**
  - `../`, `a/../../b`, `/etc/passwd`, `file:///etc`, `s3a://other-bucket/x`, `%2e%2e/` and `..\`;
  - a symlinked file and a symlinked directory inside the landing directory that point outside the root;
  - a target escaping the lake root.

### 3. Least privilege in PostgreSQL (Required)

Discovery v2 §11 rated separate roles as *Recommended*. **They are now Required**, with one addition: the data path cannot delete coordination rows.

| Role | Privileges | Used by |
|---|---|---|
| `ingest_migrator` | Owns schemas `control` and `delta_coordination`; DDL | Flyway only. Never at runtime. |
| `ingest_control` | DML on `control`; read access to commit entries for the lineage views (`v_table_commits`) | Engine |
| `ingest_coordination` | `SELECT` and `INSERT` on `coordinated_table`, `commit_entry` and `table_taint`. `UPDATE` only of `complete` and `completed_at` on `commit_entry`. `SELECT` on `schema_version`. **No `DELETE`, no other `UPDATE`, no DDL, no access to `control`.** ¹ | `PostgresLogStore` |
| `ingest_maintenance` | `SELECT` and `DELETE` on `commit_entry`, `table_taint` and `coordinated_table`. `INSERT` on `coordinated_table`, and `UPDATE` of its `registered_version`, `delta_table_id` and `registered_at`. `SELECT` on `schema_version`. Nothing else. ¹ ² | Adopting a table and adopting it again, registry reset (F9), entry pruning, in maintenance mode and runbooks |
| `<source>_ro` | `SELECT` on the source tables | JDBC connector |

¹ *Updated in P2a.3 (2026-11-14) to the privileges the migration grants, which are narrower than first written. The rows said "`SELECT`, `INSERT`, `UPDATE` on `commit_entry` and `coordinated_table`" and "`DELETE` on the coordination tables". The commit path can now only add rows and mark an entry complete: it cannot rewrite an entry, change a registration or clear a taint. The decision is unchanged.*

² *Updated in P2a.11 (2026-11-16) to the privileges of migration V2. The row said "`SELECT` and `DELETE` on `commit_entry`, `table_taint` and `coordinated_table`" and nothing more. Adopting a table registers it, and adopting it again moves its version, so the operator's role may now insert a registration and update those three columns of it. It still cannot make or change an entry, taint a table, or rename one in the registry. The decision is unchanged.*

- **Why a separate maintenance role:** a bug on the commit path cannot erase a commit entry, because the role it runs as is not allowed to.
- **Where it lives:**
  - the compose init script creates the roles (P1);
  - the migrations hold the grants (coordination in P2a, control in P4);
  - passwords are secret references (ADR-0007).
- ADR-0004 deviation 10 ("no DDL from the data path") is now **enforced by the database**, not only by the code.
- **Tests:**
  - P2a: as `ingest_coordination`, `CREATE TABLE`, `DELETE FROM commit_entry` and any read of `control` fail with SQLState `42501` (`CoordinationSchemaSpec`, with the rest of what each role is denied).
  - P4: `ingest_control` cannot write `delta_coordination`.

### 4. Dependency vulnerability scanning in CI (Required)

- **Alerts.** CI submits the resolved sbt dependency graph to GitHub, which turns on Dependabot alerts. GitHub does not parse sbt builds by itself; the submission API is how sbt projects get a dependency graph.
  - The graph is resolved by the Scala Center's `sbt-github-dependency-submission` plugin (`scripts/dependency-snapshot.sh`), and submitted as **one manifest for the whole build**.
  - *Why not the `scalacenter/sbt-dependency-submission` action as is* (amended in P1.5, after the first pull request): the action submits one manifest per sbt project, all pointing at `build.sbt`. A library used by several projects (174 of 218 here) is then listed under that file with different scopes: runtime where it ships, development in the test-only project. GitHub does not keep those entries apart, and which scope it records changes between two submissions of the same graph. Dependency review reported every such library as removed and added, so the first pull request, a documentation change, failed on vulnerabilities already on `main`.
  - The merge (`scripts/merge_dependency_manifests.py`, with unit tests) keeps one entry per package: runtime if any project uses it at runtime, development otherwise. The check itself is unchanged.
  - Trade-off: the plugin version is pinned in the script, where Dependabot does not update it; it is bumped by hand.
- **Gate.** `actions/dependency-review-action` fails a pull request that **introduces** a dependency with a known vulnerability of severity *high* or above.
- **Why gate on introduced vulnerabilities rather than all of them:**
  - The Spark and Hadoop transitive tree carries known CVEs, mostly in `provided` scope, that this project cannot fix.
  - A gate on all of them would be permanently red, and a permanently red check gets ignored.
  - Existing alerts are triaged instead. Accepted ones are documented with a reason and a review date in the [dependency triage register](../security/dependency-triage.md).
- **Prerequisite.** The repository must be on GitHub and **public**: dependency review is not available on private repositories of a personal account. The repository (`MiguelOrtegaMilla/lakehouse-ingestion-framework`) exists and becomes public when P1's CI is in place.
- **Workflow.** The gate acts on pull requests, so changes reach `main` through PRs.

| Alternative | Verdict |
|---|---|
| OWASP Dependency-Check | Needs an NVD API key and a slow database download; many false positives on Spark's tree. Rejected. |
| Snyk | External SaaS account. Rejected. |
| Trivy | Strong for container images; sbt support needs a lockfile plugin. Future, if the project builds its own images. |

### 5. Recommended hardening

| Item | Decision | Phase |
|---|---|---|
| **Pinned images** | Every `image:` in compose and every Testcontainers image is pinned by tag **and digest**. A CI check fails on an unpinned image. | P1 (E1 for MinIO and Spark) |
| **Non-root containers** | Containers run as a non-root user (`user:`) where the image supports it. Exceptions are documented. | P1, E1 |
| **Pinned GitHub Actions** *(added at closure)* | Third-party actions pinned by commit SHA. `GITHUB_TOKEN` defaults to `permissions: contents: read`; each job widens it only where needed. | P1 |
| **TLS** | Configurable per connection in the profiles. PostgreSQL uses `sslmode=verify-full` with a root certificate, applied to the JDBC connector, the control plane and the LogStore. MinIO uses `fs.s3a.connection.ssl.enabled` and a truststore. Off in the local profile, where all traffic stays on the WSL host. One integration test runs against a TLS-only PostgreSQL. | P3 (config), P4 (test), E1 (MinIO) |
| **Safe YAML loading** | circe-yaml builds a node tree and converts it to JSON: no tag-driven object construction, which is the class of the SnakeYAML CVE-2022-1471. On top of that: limits on alias expansion (billion laughs), nesting depth and document size; duplicate keys rejected; YAML 1.2 semantics preferred, so `yes`/`no`/`on`/`off` are not booleans. | P3 |
| **Threat model** | `docs/security/threat-model.md`: assets, trust boundaries, a STRIDE-lite table mapped to these controls, and what is out of scope. Written from what is actually built. | P12 |

### 6. Enterprise / future

- Storage ACLs so that only the service identity can write `_delta_log/` (already in ADR-0004).
- A secrets manager (Vault, KMS) and mTLS.
- SAST beyond compiler linting.
- Signed artifacts.
- Image scanning (Trivy) once the project builds its own images.

## Placement in the phase plan

| Control | Level | Phases |
|---|---|---|
| SQL identifiers, no concatenated SQL | Required | P3 (grammar + malicious corpus) · P6 (connector + injection integration test) · P2a/P4 (bind parameters only) |
| Path confinement | Required | P3 (static) · P5 (runtime, symlink tests) |
| Least-privilege roles | Required | P1 (roles) · P2a (coordination grants + denial tests) · P4 (control grants + denial tests) |
| Dependency scanning | Required | P1 |
| Pinned images and actions, non-root | Recommended | P1 (E1) |
| TLS | Recommended | P3 · P4 · E1 |
| Safe YAML loading | Recommended | P3 |
| Threat model | Recommended | P12 |

No new phase: each control lands in the phase that builds the component it protects.

## Consequences

- **Every Required control has a test that fails if the control is removed.** Security is demonstrated, not declared, the same standard as the rest of the non-functional requirements.
- The strict identifier grammar rejects some legitimate source names that need quoting. This is a documented limitation.
- The dependency gate ties CI to GitHub, which ADR-0002 already does.
- Five PostgreSQL roles instead of one. Each is a few lines of setup, and the denial tests document them.
