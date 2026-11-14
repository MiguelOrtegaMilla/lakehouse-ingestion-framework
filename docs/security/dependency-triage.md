# Dependency vulnerability triage

The register of known vulnerabilities in this project's dependencies that are **accepted** rather than fixed, as [ADR-0013 §4](../adr/0013-application-security-baseline.md) requires. Each entry has a reason and a review date.

**Baseline: 2026-11-14.**

| | |
|---|---|
| Commit | `b5c1b0d` |
| Stack | Spark 4.1.4 · Delta 4.4.1 · Hadoop 3.4.2 · log4j 2.24.3 |
| Open Dependabot alerts | **91**: 3 critical, 41 high, 45 moderate, 2 low |
| Accepted | **91**, in 8 groups (T-01 to T-08) |
| Fixed | none (see [Why no version overrides](#why-no-version-overrides)) |
| Next review | **2027-02-14**, or earlier on a [trigger](#review-triggers) |

## How alerts are produced

- CI submits the resolved sbt graph as one manifest (`scripts/dependency-snapshot.sh`), and Dependabot matches it against the GitHub Advisory Database.
- There is one alert per advisory and package. When two versions of one package are in the graph (`snappy-java` 1.1.10.4 and 1.1.10.8), they share an alert.
- Withdrawn advisories raise no alert.
- **Gate.** The pull-request gate (dependency review) fails only on vulnerabilities a pull request **introduces**. This register covers the ones already present.

## Where the vulnerable packages come from

The project's own runtime classpath is small. It is everything outside `provided` and test scope:

- `delta-storage`;
- the Unity Catalog client libraries `delta-storage` pulls in;
- log4j;
- slf4j-api;
- jsr305;
- scala-library.

Only **log4j** (T-08) is both on that classpath and carries alerts.

Everything else with alerts belongs to the **Spark and Delta runtime**, declared `provided`:

- the Spark installation supplies those jars at run time;
- the project only compiles and tests against them.

| Group | Packages | Version | Alerts (C/H/M/L) | Enters through | On the project's runtime classpath |
|---|---|---|---|---|---|
| T-01 | Netty (12 modules) | 4.2.7.Final | 46 (1/23/21/1) | `spark-core`, `spark-network-common` (`netty-all`) | No (`provided`) |
| T-02 | `bcprov-jdk18on` | 1.80 | 5 (2/2/1/0) | `netty-all` ← `spark-core` | No (`provided`) |
| T-03 | `jackson-core`, `jackson-databind` | 2.21.2 | 19 (0/8/11/0) | `spark-core`, `spark-sql`, `delta-spark` | No (`provided`) |
| T-04 | `guava` | 32.0.0-jre | 1 (0/1/0/0) | `curator-client` ← `spark-core` | No (`provided`) |
| T-05 | `zookeeper` | 3.9.4 | 2 (0/2/0/0) | `spark-core` | No (`provided`) |
| T-06 | `lz4-java` | 1.8.0 | 8 (0/3/4/1) | `spark-core` | No (`provided`) |
| T-07 | `snappy-java` | 1.1.10.8 / 1.1.10.4 | 3 (0/2/1/0) | `spark-core`, Parquet; `hadoop-client-api` | No (`provided`) |
| T-08 | `log4j-api`, `log4j-core`, `log4j-layout-template-json` | 2.24.3 | 7 (0/0/7/0) | Spark; `delta-storage` → Unity Catalog client | **Yes** (aligned to Spark's version) |

## Exposure model

These conditions hold for every entry. If one of them changes, it is a review trigger:

- The framework runs as a **batch job on a developer machine**:
  - in Spark local mode;
  - in the E1 extension, on Spark Standalone inside a private Docker network.
- **It runs no network service of its own.** Spark's internal RPC and shuffle ports are the only listeners, and they are not exposed outside the machine or the Docker network.
- **Inputs are trusted:**
  - synthetic files that the project generates;
  - a local PostgreSQL;
  - Delta tables that the project writes itself.
- Spark's RPC TLS (`spark.ssl.rpc.enabled`) and Standalone high availability (`spark.deploy.recoveryMode=ZOOKEEPER`) are **off**.

## Why no version overrides

`dependencyOverrides` can raise a transitive version in the build. For these packages that would be the wrong fix:

- **It would not change what runs.** Under `spark-submit`, the jars of the Spark installation come first on the classpath. An override changes only the classpath the tests use, so the tests would pass against versions that never run in practice. `Dependencies.log4jAlignment` exists to prevent exactly that drift for log4j.
- **Spark is tested only against the versions it ships.** Netty's modules must all share one version, and a library that Spark's network layer or its JSON and Parquet readers rely on would be swapped under them in tests only.
- **The real fix is upstream:** a Spark or Delta release that ships patched versions. That is a stack decision (ADR-0014), not an alert fix.

## Register

### T-01 Netty 4.2.7.Final: accept

- **Use in Spark.** Spark's RPC, block transfer and shuffle run Spark's own binary protocol over Netty's transport, codec and handler modules. The Spark UI is served by Jetty, not Netty.
- **Modules Spark does not use.** `netty-all` also puts these on the classpath, though Spark does not use them: the HTTP, HTTP/2, HTTP/3, QUIC, DNS, SPDY, proxy and compression codecs (`codec-http*`, `codec-classes-quic`, `codec-dns`, `resolver-dns`, `handler-proxy`, `codec-compression`). Together they account for 38 of the 46 alerts.
- **`netty-handler` (1 critical, 3 high, 1 medium).** All five concern TLS and SNI handling (`SniHandler`, the `ClientHello` reassembly, trust-manager wrapping) or `IpSubnetFilter`. Spark's RPC TLS is off, and SNI and subnet filtering are not used.
- **epoll/kqueue transport (1 high, 2 medium).** These are denial-of-service and descriptor-leak bugs on the socket. Spark's ports are not reachable from outside the machine or the Docker network.
- **Fixed in:** Netty 4.2.17.Final, for all of them. Spark 4.2.1 ships 4.2.13.Final, which fixes some.
- **GitHub dismissal reason:** *Vulnerable code is not actually used.*

<details><summary>Advisories (46)</summary>

- `netty-handler`: GHSA-c4c3-7fpv-j4q5 (critical), GHSA-3qp7-7mw8-wx86, GHSA-c653-97m9-rcg9, GHSA-x4gw-5cx5-pgmh, GHSA-fccg-mwvh-qqg4
- `netty-codec-http`: GHSA-57rv-r2g8-2cj3, GHSA-6jqx-86gh-f27w, GHSA-f6hv-jmp6-3vwv, GHSA-jppx-w49h-x2qq, GHSA-mvh2-crg5-v77c, GHSA-pwqr-wmgm-9rr8, GHSA-38f8-5428-x5cv, GHSA-4mp9-239f-g9hg, GHSA-6cqp-g7gg-8hr5, GHSA-84h7-rjj3-6jx4, GHSA-8c42-7qj2-3j46, GHSA-gcjf-9mgh-3p7g, GHSA-hvcg-qmg6-jm4c, GHSA-m4cv-j2px-7723, GHSA-q4f6-jm68-57ww, GHSA-v8h7-rr48-vmmv, GHSA-xxqh-mfjm-7mv9
- `netty-codec-http2`: GHSA-93wv-jw9v-4972, GHSA-f6hv-jmp6-3vwv, GHSA-w9fj-cfpg-grvv, GHSA-563q-j3cm-6jxm, GHSA-5x3r-wrvg-rp6q, GHSA-c2gf-v879-257j, GHSA-c69g-56f8-xwqj
- `netty-codec-http3`: GHSA-2c5c-chwr-9hqw, GHSA-4grm-h2qv-h6w6, GHSA-c2rx-5r8w-8xr2, GHSA-hpcc-26xq-25fv
- `netty-codec-classes-quic`: GHSA-cmm3-54f8-px4j, GHSA-cq4q-cv5g-r8q5
- `netty-codec-compression`: GHSA-558v-64gr-wgg4, GHSA-mj4r-2hfc-f8p6
- `netty-codec-dns`: GHSA-cm33-6792-r9fm, GHSA-mfg7-5gfp-c4w3
- `netty-resolver-dns`: GHSA-5pvg-856g-cp85, GHSA-676x-f7gg-47vc, GHSA-xmv7-r254-6q78
- `netty-transport-classes-epoll`: GHSA-rwm7-x88c-3g2p
- `netty-transport-native-epoll`, `netty-transport-native-kqueue`: GHSA-w573-9ffj-6ff9
- `netty-handler-proxy`: GHSA-45q3-82m4-75jr (low)

</details>

### T-02 Bouncy Castle `bcprov-jdk18on` 1.80: accept

- **How it enters.** `netty-all` brings it in for optional TLS features (certificate and PEM handling).
- **Exposure.** Neither Spark's RPC with TLS off nor this project calls it. The advisories cover GOST CTR keystream reuse, a name-constraints bypass, a timing channel, an LDAP injection and ASN.1 parsing depth.
- **Fixed in:** 1.85. It is no longer in the graph with Spark 4.2.1.
- **GitHub dismissal reason:** *Vulnerable code is not actually used.*
- **Advisories:** GHSA-574f-3g2m-x479 (critical), GHSA-9pwp-9qqc-pr26 (critical), GHSA-p93r-85wp-75v3, GHSA-qp49-qgx5-5m26, GHSA-c3fc-8qff-9hwx.

### T-03 Jackson 2.21.2: accept, and the one group that is reachable

- **What reaches it.** Spark's JSON data source parses input files with `jackson-core`, and Delta reads its transaction log with Jackson. The file connector (P5) reads JSON, so a crafted JSON file **does** reach the `jackson-core` parser.
- **`jackson-core` (3 high).** All three are resource-exhaustion bugs: a ReDoS in float parsing, an async-parser length bypass, and an unbounded error token. The worst case is a failed or stalled ingestion run, not code execution.
- **`jackson-databind` (5 high, 11 medium).** These need features that Spark's parsing does not use: polymorphic typing and `PolymorphicTypeValidator`, `@JsonView`, `@JsonIgnore`, `InetSocketAddress`, `Path` and `XMLGregorianCalendar` deserialisation.
- **Why accept.** Inputs are trusted (see the exposure model), and the fix cannot be applied without moving Spark (see [Why no version overrides](#why-no-version-overrides)).
- **Fixed in:** 2.21.7. Spark 4.2.1 still ships 2.21.2.
- **What changes it.** Accepting untrusted files triggers a review. The first controls to look at are then a size limit on input files and Jackson's `StreamReadConstraints`.
- **GitHub dismissal reason:** *Risk is tolerable to this project.*

<details><summary>Advisories (19)</summary>

- `jackson-core`: GHSA-7hhh-6rmp-j9qf, GHSA-p6pp-m3f8-5c89, GHSA-r7wm-3cxj-wff9
- `jackson-databind`: GHSA-cxp5-3px4-pw24, GHSA-j3rv-43j4-c7qm, GHSA-q4xh-88c3-wmh7, GHSA-rmj7-2vxq-3g9f, GHSA-wv8q-qhhj-9h54, GHSA-3pjw-73gf-8qr5, GHSA-5gvw-p9qm-jgwh, GHSA-5hh8-q8hv-fr38, GHSA-5jmj-h7xm-6q6v, GHSA-9fxm-vc8v-hj55, GHSA-gx83-3vf8-gh7j, GHSA-hgj6-7826-r7m5, GHSA-mhm7-754m-9p8w, GHSA-rcqc-6cw3-h962, GHSA-vvgp-rfg2-7rr6, GHSA-wjgm-6hv5-3cvf

</details>

### T-04 Guava 32.0.0-jre: accept

- **How it enters.** Through Apache Curator, which Spark uses only for ZooKeeper-based Standalone high availability.
- **Exposure.** The advisory needs Java deserialisation of attacker-supplied Guava collections. Nothing in this project or in the Spark paths it uses does that.
- **Fixed in:** 33.7.2. Spark 4.2.1 still resolves 32.0.0-jre.
- **GitHub dismissal reason:** *Vulnerable code is not actually used.*
- **Advisory:** GHSA-xxph-c9ww-hj94.

### T-05 ZooKeeper 3.9.4: accept

- **Use in Spark.** Only for Standalone master high availability, which is off. E1 runs a single master.
- **Fixed in:** 3.9.5. Spark 4.2.1 ships it.
- **GitHub dismissal reason:** *Vulnerable code is not actually used.*
- **Advisories:** GHSA-7xrh-hqfc-g7qr, GHSA-crhr-qqj8-rpxc.

### T-06 `lz4-java` 1.8.0: accept

- **Use in Spark.** Spark's default codec for shuffle, broadcast and spill blocks (`spark.io.compression.codec=lz4`). The decoder bugs (out-of-bounds reads, unvalidated lengths, recursion) need crafted compressed input. Spark decompresses only blocks that the same application wrote.
- **Native-library extraction (GHSA-mcr4-qmvw-px4g).** The library extracts itself into a shared temporary directory, which matters on multi-user hosts. The development machine has one user.
- **Fixed in:** no release of `org.lz4:lz4-java`, which is no longer maintained. Fixes exist only in the `at.yawk.lz4` fork, which Spark 4.2.1 uses at 1.11.0 and which is still affected by some of them.
- **GitHub dismissal reason:** *Risk is tolerable to this project.*
- **Advisories:** GHSA-cmp6-m4wj-q63q, GHSA-mcr4-qmvw-px4g, GHSA-vqf4-7m7x-wgfc, GHSA-4v53-57pg-c464, GHSA-6cx8-rjf8-pr8g, GHSA-gm45-99xc-r7wv, GHSA-xx22-p4ch-683r, GHSA-343h-94h5-c4wr (low).

### T-07 `snappy-java` 1.1.10.8 and 1.1.10.4: accept

- **Use.** Snappy is the default Parquet codec, so the project's Delta tables are snappy-compressed Parquet. The bugs (heap overflow, unbounded allocation, a JVM crash on unchecked ranges) need crafted compressed input. The project decompresses only files it wrote itself, and its file sources are CSV and JSON.
- **Where 1.1.10.4 comes from.** `hadoop-client-api`, which `delta-pg-logstore` declares `provided`.
- **Fixed in:** 1.1.10.9. Spark 4.2.1 still ships 1.1.10.8.
- **What changes it.** Parquet as a file source format would make crafted input reachable, and triggers a review.
- **GitHub dismissal reason:** *Risk is tolerable to this project.*
- **Advisories:** GHSA-gwq9-9ffj-mvrv, GHSA-q233-g92q-qxp6, GHSA-xqrx-qh46-34m7.

### T-08 log4j 2.24.3: accept

- **How it enters.** This is the only group on the project's own runtime classpath:
  - `delta-storage` pulls in the Unity Catalog client libraries, which bring log4j 2.25.3;
  - `Dependencies.log4jAlignment` pins every log4j module to Spark's 2.24.3, because that is the version that runs under `spark-submit`.
- **Exposure.** The advisories concern components this project does not configure: `XmlLayout`, `Log4j1XmlLayout`, `Rfc5424Layout`, `SocketAppender` TLS hostname verification, and the encoding of non-finite floats in `JsonTemplateLayout` and `MapMessage`. The only log4j configuration in the repository (`log4j2-test.properties`) is a console appender with a `PatternLayout`.
- **What changes it.** P11 adds JSON logs. If they use `JsonTemplateLayout` or `MapMessage`, GHSA-w35j-pv5h-q9q9 and GHSA-qv9r-c865-cp47 become reachable, because metrics such as rates can be `NaN` or infinite. That is a review trigger.
- **Fixed in:** 2.25.4, except `log4j-api`, which needs 2.25.5. Spark 4.2.1 ships 2.25.4.
- **GitHub dismissal reason:** *Vulnerable code is not actually used.*
- **Advisories:** GHSA-qv9r-c865-cp47 (`log4j-api`), GHSA-3pxv-7cmr-fjr4, GHSA-445c-vh5m-36rj, GHSA-6hg6-v5c8-fphq, GHSA-h383-gmxw-35v2, GHSA-vc5p-v9hr-52mj (`log4j-core`), GHSA-w35j-pv5h-q9q9 (`log4j-layout-template-json`).

## Upgrade path

The fix for most of this register is a newer Spark.

- **Candidate.** Spark 4.2.1 with Delta 4.4.1 built for it (`delta-spark_4.2_2.13`). It needs Scala 2.13.18 and Hadoop 3.5.0.
- **Measured effect.** Resolving that graph against the same advisory database on 2026-11-14 leaves **56 alerts (1 critical, 22 high, 32 moderate, 1 low)**:
  - T-02 and T-05 disappear;
  - T-08 drops to one alert;
  - T-01 drops to the modules not fixed by Netty 4.2.13.Final.
- **Not changed by it:** T-03, T-04, T-06 and T-07.

Moving the stack is a decision for ADR-0014, not for this register.

## Review triggers

Review the register on **2027-02-14** at the latest. Review it earlier when:

- the Spark, Delta or Hadoop version changes;
- P11 configures structured (JSON) logging (T-08);
- the framework accepts files it did not generate, or adds Parquet as a source format (T-03, T-07);
- Spark's RPC TLS, Standalone high availability, or any network exposure beyond the machine or Docker network is turned on (T-01, T-02, T-04, T-05);
- a new alert appears. The pull-request gate blocks new ones that a change introduces. A new advisory against an existing package appears as an alert and is triaged here.

## Procedure

1. **New alert.** If the package is on the project's own runtime classpath and a patched version exists that matches what runs under `spark-submit`, fix it. Otherwise add it to an existing group or a new one, with a reason.
2. **In GitHub.** Dismiss the alert with the reason given for its group, and the comment `Triaged: docs/security/dependency-triage.md T-0x, review 2027-02-14`.
3. **At review.** Update the baseline, the counts and the next review date. An entry whose package left the graph is removed, and its alerts close by themselves.
