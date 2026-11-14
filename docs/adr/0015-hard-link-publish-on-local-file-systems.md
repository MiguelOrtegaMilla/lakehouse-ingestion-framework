# ADR-0015 — Commit files on `file://` are published with a hard link

| | |
|---|---|
| **Status** | Accepted (P2a, 2026-11-14) |
| **Supersedes** | In [ADR-0004](0004-postgresql-coordinated-delta-logstore.md): the `file://` publish of §3.4 (copy to a second temporary file, then rename), and the rows that describe it in §3.8 and §4 (deviation 2). It also answers the open item of §8 on `.crc` sidecars. The rest of ADR-0004 stands. |
| **Related** | [ADR-0011](0011-storage-profiles.md) |
| **Sources studied** | Hadoop 3.4.2: `ChecksumFileSystem`, `ChecksumFs`, `RawLocalFileSystem`. The RED spike ([docs/spikes/logstore-red.md](../spikes/logstore-red.md)). |

## Context

ADR-0004 §3.4 publishes commit N on `file://` by copying `T(N)` to a second temporary file and renaming it to `N.json`. It accepts that `rename(2)` replaces an existing file: exclusivity comes from PostgreSQL, and concurrent recoverers publish identical bytes.

Three things found since then argue against it:
- **A foreign writer's file is not identical.** The publish-race safeguard (ADR-0004 §3.5, layer 4, scenario F11) must notice a foreign `N.json` at the moment of publishing. A rename replaces it and destroys the evidence. Checking first leaves a window between the check and the rename.
- **Hadoop's local file system adds a second step.** It keeps a `.crc` sidecar next to each file and renames it after the file, without overwriting. A crash between the two steps, or a foreign writer, leaves a sidecar that does not match, and every reader that checks it then fails on the commit (the RED spike).
- **Hadoop's rename can stop being atomic.** `RawLocalFileSystem.rename` copies the file when the rename fails.

## Decision

On `file://`, `PostgresLogStore` publishes commit N with a **hard link**: `N.json` becomes a second name of `T(N)`, through `java.nio` (`Files.createLink`) and not through Hadoop's file system.

- `T(N)` is written without Hadoop's checksummed file system and **forced to disk before the `INSERT`**, so an entry never points at a file the storage could still lose.
- `link(2)` is one system call. The name appears with the complete file behind it, and the call **fails if `N.json` already exists**.
- If it exists, the LogStore compares it with `T(N)`. Equal bytes mean another recoverer published the same commit. Different bytes are a foreign writer: `UncoordinatedWriteDetected`, and the table is tainted.
- After the link, the `_delta_log` directory is forced to disk.
- **Coordinated commit files have no `.crc` sidecar.** Hadoop reads a file without one unchecked and without an error (`ChecksumFileSystem.ChecksumFSInputChecker`), so readers with and without checksums both read the table.

The `s3a://` publish of ADR-0004 §3.4 is unchanged, and so is the rest of the protocol.

## Alternatives

| Option | Verdict |
|---|---|
| Rename a second temporary file (ADR-0004 as written) | Atomic, but it replaces. It leaves the publish race to a check followed by a rename, and needs a second temporary file. Rejected. |
| Publish through Hadoop's checksummed file system, keeping sidecars | Two renames instead of one. Recovery would have to repair sidecars, and F3 and F11 would have to cover each half. Rejected. |
| `renameat2` with `RENAME_NOREPLACE` | The same guarantee as the link, but Linux only and not available from the JDK. Rejected. |
| Create `N.json` exclusively (`O_EXCL`) and write into it | Exclusive, but a reader can see the file half written. Rejected. |

## Consequences

- **F3** has no middle to crash in: there is no state between unpublished and published, and no second temporary file to collect. It asserts that no partial `N.json` and no sidecar exist at any crash point.
- **F11** becomes one atomic test instead of a check and a rename. It asserts that the foreign `N.json` and its sidecar are left as found, so the table stays readable for forensics.
- **No CRC check of commit files by Hadoop on a local disk.** Object stores never had it.
- **The lake's file system must support hard links.** ext4, XFS and NFS do; some FUSE and Windows-mounted file systems do not. Startup probes the lake root and refuses to run without them (ADR-0004 §3.5, layer 1).
- **Two names, one file.** `T(N)` and `N.json` are the same file until the temp GC removes the first name. The commit's modification time is the time `T(N)` was written. That precedes its `INSERT`, which precedes the writing of `T(N+1)`, so modification times stay ordered by version.
- **The spike's delay no longer applies.** `DelayingLocalFs` delays a rename the LogStore never makes, so GREEN widens the LogStore's own window instead (ADR-0004 §7.3).
- A `.N.json.crc` next to a commit this LogStore published can only come from a foreign writer. How it is handled is fixed with the implementation.
- Implemented and tested in P2a.7 (discovery §17).
