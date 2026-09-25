# ADR-0001 — Development environment: WSL2 + Docker Engine + VS Code/Metals

| | |
|---|---|
| **Status** | Accepted (D1, 2026-09-25) |
| **Setup guide** | [`docs/setup/dev-environment.md`](../setup/dev-environment.md) |

## Context

The host is Windows 10 Home 22H2 (build 19045) with 7.4 GB RAM, 8 cores, firmware virtualisation enabled, and no Docker, WSL, Scala or sbt. Specific problems on native Windows:
- Spark's Standalone launch scripts are bash.
- Hadoop needs `winutils.exe`/`hadoop.dll` to write to local disk.
- Docker Desktop on Windows Home runs on WSL2 anyway, and costs RAM.

## Decision

- **WSL2** with Ubuntu 24.04, capped through `.wslconfig` (`memory=5GB`, `swap=4GB`, `processors=6`).
- **Docker Engine installed inside WSL** (official apt repository), without Docker Desktop. Testcontainers talks to the local socket.
- **JDK 17 (Temurin) and sbt inside WSL**, installed with SDKMAN. The project pins the JDK in `.sdkmanrc` and sbt in `project/build.properties`.
- **VS Code on Windows with the Remote-WSL extension, plus Metals** running inside WSL.
- **Code lives in the WSL filesystem** (`~/code/<repo>`), never under `/mnt/c`. Cross-filesystem I/O is an order of magnitude slower and breaks file watching.
- **All development tooling runs inside WSL** from P1 onwards, so that builds, Docker and tests execute where the code lives.

## Alternatives

| Option | Verdict |
|---|---|
| WSL2 + Docker Desktop + IntelliJ on Windows | Better Scala IDE, but more RAM (IntelliJ ~2 GB plus Docker Desktop's VM overhead) and cross-filesystem friction. Rejected by the owner. |
| Native Windows + `winutils` + Docker Desktop | Fragile Spark/Hadoop setup; bash scripts don't run. Rejected. |
| Cloud dev environment (Codespaces) | Hides the local-execution requirement of the brief; free-tier limits. Rejected. |

## Consequences

- The Linux environment matches CI (`ubuntu-latest`), so no "works on my machine" gap.
- The memory budget is the binding constraint. Heavy suites are sized down locally and run full size in CI (discovery §7.3).
- Metals shares the WSL memory cap. It is stopped during stress tests and Standalone demos.
