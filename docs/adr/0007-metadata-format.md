# ADR-0007 — Source metadata as YAML with startup validation

| | |
|---|---|
| **Status** | Accepted (2026-09-25) |

## Context

Adding a source must mean writing configuration, not code. The configuration must be:
- reviewable;
- versioned;
- validated before any data is touched;
- scalable to many sources;
- free of secrets.

## Decision

- **One YAML file per source** under `conf/sources/`, plus application **profiles** (`conf/app/<profile>.yaml`: lake URI, databases, Spark settings).
- **Decoding** with circe + circe-yaml into typed case classes.
  - *(Note, 2026-11-17, P3.1.)* circe-yaml is not used. The JSON that circe decodes is built from the events of snakeyaml-engine's parser, in `SafeYaml` ([ADR-0013](0013-application-security-baseline.md) §5 has what was measured). Everything after the JSON is as decided here: circe decoders into typed case classes.
- **Two-stage validation at startup**, accumulating *all* errors (`ValidatedNel`):
  1. Structure: decoding errors with the file path and field.
  2. Semantics:
     - connector-specific `validate`;
     - duplicate ids;
     - partition keys included in the keys of fan-in tables (ADR-0010);
     - canonical table paths (ADR-0011);
     - breaking changes against the last used snapshot (primary key, watermark column, target).
- **Secrets by reference only**: `secretRef: env:NAME` or `file:/run/secrets/name`, resolved by a `SecretResolver`. Literal passwords are rejected by validation.
- **Versioning**: SHA-256 of the canonicalised, redacted config, recorded on every run. The redacted content is stored once in `control.config_snapshot`.

*As built (P3.3 and P3.4, 2026-11-17):* `Profile` and `Source` in `core`.
- **A mapping knows its fields.** A field that the model does not have is an error, with the field that was probably meant: a misspelt `watermark` must not become a full load that nobody asked for.
- **The connector-specific `validate` is a `ConnectorKind`**: it reads the fields of its own section, with the same reader and the same errors as the rest of the file, and says which ways of loading it has. The kinds are given to `Source.load`; the model of a source names none.
- **Validation opens no connection and looks no name up.** Whether a database or a source answers is another step, with timeouts of its own.
- **Warnings are apart from errors**: what is allowed and unusual, as a database on another machine without TLS, is said once and stops nothing.

*As built (P3.7, 2026-11-18):* `ConfigVersion` in `core`.
- **What is hashed is the resolved configuration**, the profile and the source with every default, written one way: keys in order, no space, nothing that is null, and a `schema_version`. The order of the keys of a file, its comments, `120s` for `2m`, a default written out: none of them changes the hash. A new field with a default changes every hash when the application is upgraded, and a run records the version of the application next to its hash.
- **The content is a configuration.** It has the names of the fields of the files, so a snapshot can be read back, and gives the same profile and the same source. A test does that for every configuration of the repository and of the corpus.
- **It is not redacted, because it holds no secret**: a configuration has references to secrets, and its types have no place for a value.
- **The comparison with the last snapshot looks at a few paths of the content**, so a snapshot that an older version of the application wrote can still be compared. Breaking: another key while Silver is merged into, another watermark column, another Bronze or Silver table, another partitioning of the same Silver table, another lake. It is a pure function; where the last snapshot comes from, and what lets a breaking change through, are P4 and P7.
- **Extended beyond the three of the decision above (2026-11-18): where the rows come from, for a source loaded by a watermark.** The decision names the primary key, the watermark column and the target. A source that is loaded incrementally and now reads another table, or another database, has a stored watermark that is of the other one: going on from it would skip every row of the new origin below it, without a word. So for a source that was incremental and still is, a change of the kind of its connector, or of the fields that its connector names as its origin (`ConnectorConfig.origin`: the URL of the database and the table, for JDBC), is a breaking change. Not for a full load, which takes everything whatever it is, nor for what only says how the origin is reached: the role, the reference to the password, TLS, the timeouts.

## Alternatives

| Option | Verdict |
|---|---|
| HOCON / PureConfig | Excellent for Scala app config, but uncommon outside Scala. Source definitions should be readable by non-Scala engineers. Rejected for sources. |
| JSON | No comments; noisy for humans. Rejected. |
| Delta / SQL metadata tables | Worse authoring experience and loses PR review. Worth it at hundreds of sources with a UI. **Future evolution.** |
| Custom DSL | Over-engineering. Rejected. |

## Consequences

- Governance through git: every source change is a reviewed PR, and every run is traceable to an exact config.
- Testing configurations is plain unit testing (`core` has no Spark).
