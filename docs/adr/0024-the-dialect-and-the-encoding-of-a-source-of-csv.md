# ADR-0024 — A source of CSV says its dialect and its encoding, a plan says how its files are read, and a file that is not in its encoding cannot be read

| | |
|---|---|
| **Status** | Accepted (P8.9a, 2026-11-27) |
| **Amends** | [ADR-0023](0023-a-file-that-cannot-be-read.md) §2: a third kind of file that cannot be read, one whose bytes are not of the encoding of its source, found by a look at its bytes once Spark has read it (§4, §5). The policy, and what each of its values does, stand as they are. |
| **Related** | [ADR-0005](0005-batch-wal-and-idempotent-writes.md) §3, [ADR-0007](0007-metadata-format.md), [ADR-0008](0008-layer-responsibilities.md), [ADR-0014](0014-stack-versions-pinned-at-p1.md), [ADR-0018](0018-a-run-gives-up-a-batch-whose-source-changed.md), [ADR-0021](0021-operator-actions-and-backfills.md), [ADR-0022](0022-the-classification-of-failures-and-the-retries-of-a-run.md) |

## Context

The file connector reads CSV in one dialect, RFC 4180's, with a comma between values and a quote doubled inside a quoted value, and takes every file as UTF-8 (P5.13). A source that separates its values otherwise, escapes a quote with a backslash, or writes in another encoding is read wrong: values under the wrong column, or with other characters than its source's, or records in the quarantine that were right. The discovery has the dialect and the encoding as required in configuration (§4), and §20 has had them open since P5.13. The breakdown of P8 gives them fields of their own, `connector.csv`, and passes no option of Spark through: an option of Spark could set `ignoreCorruptFiles` or `DROPMALFORMED`, and lose rows without a word.

Three things are this ADR's beyond the fields: which encodings a source may say; what a run does with a file whose bytes are not of the encoding said, which the breakdown left as a known limit; and with which dialect a batch is read when the configuration changes between its plan and its replay.

### What Spark 4.1 does

`SparkCsvDialectContractSpec` reads files written byte by byte with the options of the file connector, at the inference of the columns and at the checkpoint, with no code of the engine:

| What is read | What Spark does |
|---|---|
| windows-1252, in a session as Spark makes one | Refused at the inference, `INVALID_PARAMETER_VALUE.CHARSET`. Spark's reader takes seven charsets: ISO-8859-1, US-ASCII, UTF-8, UTF-16, UTF-16BE, UTF-16LE and UTF-32, by their names whatever their case, and no other name of them (`latin1` is refused). |
| windows-1252, in a session with `spark.sql.legacy.javaCharsets` | Read: the euro, the quotes of 0x93 and 0x94, in gzip, in a value over two lines, with a delimiter and an escape of its own. The setting is the session's (another session of the same context still refuses), internal, off by default, from Spark 4.0.0. The five bytes that windows-1252 does not define are read as U+FFFD. |
| ISO-8859-1 | Read, every byte a character: the bytes 0x80 to 0x9F are control characters (U+0080 to U+009F). A file of windows-1252 read as ISO-8859-1 has its euro as U+0080 and its quotes as U+0093 and U+0094, and nothing says so. |
| A byte that is not of the encoding, in UTF-8 | U+FFFD, one for each sequence that is not of it: in a value, in a name of the header, between quotes, in any file of the batch, at the end of a file. Nothing is set apart, and nothing fails. |
| A U+FFFD that the source wrote | Read as itself: the read cannot tell it from a replacement. |
| The mark of the order of bytes of UTF-8 | Taken off, of the first file of a batch and of the others, in gzip, before a quoted name. A second mark is kept, at the start of the first name. In a file read as ISO-8859-1 or windows-1252 the mark is three characters, `ï»¿`, at the start of the first name. |
| A file of UTF-16 read as UTF-8 | Its mark is two U+FFFD; a file with no mark is read as UTF-8, with a NUL between its characters. |
| A file with nothing but a mark | A header of one column, named U+0000. Beside files with a header, the batch fails as one whose files do not have the header that it is read by. An empty file, or a mark and an end of line, has no header and is passed over. |
| The escape `\` | `\"` is a quote and `\\` a backslash, between quotes; a backslash before another character, and every backslash outside quotes, is itself. A quote doubled as RFC 4180 doubles it is no quote: the value keeps every quote that it was written with, or is cut at the comma after them and its record set apart. |
| A NUL as the quote | Quoting is off: a comma between quotes separates two values. |
| The end of a line | A file's is the first that it has: CRLF, CR or LF. Between quotes, that end of line is read as LF, and every other is kept as written: a value of a file whose lines end in CRLF has LF where it had CRLF. A file that mixes them has, after a first CRLF, two records read as one and set apart; after a first LF, a CR at the end of the last value of each line that ends in CRLF. |
| Values over several lines (`multiLine`) | A file is read whole, by one task, whatever its size; several files may share one. |
| The lines of a file read as text, as JSON is read (P8.8b) | Their bytes are kept, and each line is given as a string with U+FFFD for a byte that is not UTF-8. |

And the decoders of the JDK, made to report what they cannot decode: UTF-8 refuses a byte that is not of it (`MalformedInputException`); windows-1252 refuses its five bytes that it does not define (`UnmappableCharacterException`) and makes every other byte of 0x80 to 0x9F a printable character; ISO-8859-1 refuses nothing. The three are of `java.base`.

So Spark replaces what it cannot decode and says nothing, in the one place where it is told that a file is of an encoding; a read of its rows cannot tell what it replaced from what the source wrote; and a file in an encoding of one byte cannot be found to be in another by a decoder, but for what the bytes of its encoding are used for.

## Decision

**A source of CSV says its dialect and its encoding in fields of its own, from a closed list of encodings, and a batch is read with the dialect and the format that its plan was written with. A file whose bytes are not of the encoding of its source is a file that cannot be read, under the policy of ADR-0023: every file of a batch is decoded once more, by a decoder that reports, once Spark has read it.**

### 1. The fields

`connector.csv`, for a source of files whose format is `csv`, with four fields and no other:

| Field | Default | What it is |
|---|---|---|
| `delimiter` | `,` | What separates the values of a record. |
| `quote` | `"` | What a value is quoted with. |
| `escape` | `"` | What a quote inside a quoted value is written after. The quote itself is RFC 4180's: a quote inside a value is the quote twice. |
| `encoding` | `UTF-8` | What the bytes of every file of the source are in (§2). |

- **Validated when the configuration is loaded**, as every field is, before anything is opened. Each of the three characters is one character of the basic plane, which is what Spark's parser takes; none is the end of a line (CR, LF), which ends a record; none is NUL, which as the quote turns quoting off; each can be written in the encoding of the source. The delimiter is neither the quote nor the escape; the quote may be the escape, as it is by default. `connector.csv` with another format is refused, and so is any field that is not one of the four.
- **In the hash of the configuration when the dialect is not the default, and then with its four fields** (ADR-0007). A source that says nothing of its dialect, or says the default, keeps the hash that it had, as with `on_unreadable_file` (ADR-0023 §1). A dialect that changes is no change that Bronze cannot follow: its columns of CSV are text (ADR-0008).
- **Values over several lines are always read**, as they have been since P5.7, and a source has no field for it, as for JSON (P8.8a). Without it, Spark splits a file by its bytes and reads it a line at a time, and a quoted value with the end of a line in it would be two records.

### 2. The encodings

**`UTF-8`, `ISO-8859-1` and `windows-1252`, by those names, whatever their case**, and stored by them. Another name of one of them (`utf8`, `latin1`, `cp1252`) is refused with the list.

- **A closed list**, and not any charset of the JVM. Which charsets a JVM has is the JVM's, so a configuration would be valid on one machine and not on another; two names of one charset would be two hashes; and what a read does with a byte that is not of the encoding, which §4 rests on, is known only for the encodings that it was tested with. A new encoding is one more name and its tests in `SparkCsvDialectContractSpec`.
- **windows-1252** is what a spreadsheet and the tools of Windows write as CSV in Western Europe. It has the euro, and the quotes and the dashes of 0x80 to 0x9F, which ISO-8859-1 has as control characters.
- **windows-1252 is read in a session that may read with any charset of the JDK** (`spark.sql.legacy.javaCharsets`, on in the session that the application makes). The setting is of the session, and it is internal: it lets `encode()` and `decode()`, and every reader with a charset, take any charset of the JDK, for any SQL of the session, Silver's of P10 among it. For the seven that Spark takes without it nothing changes, and the list of §2 is what reaches Spark from a source. `SparkCsvDialectContractSpec` pins both: that windows-1252 is refused without the setting and read with it. An upgrade of Spark that takes the setting away fails there, and windows-1252 leaves the list; a file of a spreadsheet then said to be ISO-8859-1 is refused by §5, and is not read with other characters.
- **A charset that Spark refuses is a permanent failure of the run**, by Spark's condition (`INVALID_PARAMETER_VALUE.CHARSET`), wherever it is raised: a session made without the setting is the application's mistake, and no file of the batch is one that cannot be read. Classified as anything else, it would have each file read alone (ADR-0023 §3), and under `skip` every file recorded as `FAILED`.
- **UTF-16 is not on the list.** A file of UTF-16 with no mark, said to be `UTF-16`, is read as big-endian, and its characters, swapped, are characters still: no decoder refuses them. Said to be `UTF-16LE`, its mark is kept as a character. The producers that matter here do not write it as CSV (PostgreSQL's `COPY` cannot), and a file of UTF-16 with its mark, from a source of UTF-8, is refused by §4, since its mark is no UTF-8.

### 3. A plan says how its files are read

**The plan of a batch of files has the format of its files and, for CSV, the dialect, all four fields, defaults included** (`FilePlan`, version 3). The read takes them from the plan, and never from the configuration, at every read of the plan: by the run that planned it, by every run that replays it (ADR-0005 §3), and by the reads of each file alone (ADR-0023 §3).

- **Why.** A plan is replayed exactly until it is committed. A batch whose quarantine is written before its process dies, and whose source then says another delimiter, would be replayed with it: Delta takes the quarantine once (ADR-0005), so the records set apart would be of one read and the rows of Bronze of another, with counts that do not agree, and a record in both or in neither. The defaults are in the plan so that the plan does not depend on what a later version of the application takes as one.
- **What the plan has decides what the rows are; what the configuration has decides what is done with a failure.** The format and the dialect are the plan's. `on_unreadable_file` and `quality.max_rejected_ratio` are the configuration's at the moment of the run, which is how an operator lets a batch through today: by `skip`, or by a higher threshold.
- **A plan of version 1 or 2 is read with the default dialect**, which was the only one when it was written, and with the format of the configuration, as before. A plan whose encoding the application does not know is not one that it reads, and fails as permanent, as a plan of another form does.
- **A configuration whose dialect differs from its plan's** has the run say so, with a warning: the batch is read as it was planned. `abandon-batch` gives it up (ADR-0021 §4: it mends a plan that is wrong, and a plan of the wrong dialect is one), and the next plan takes the same files with the dialect of the configuration. A reprocess plans its batch when it is asked for, so it reads with the dialect of the configuration: it is how files already committed are read again with a dialect corrected.

### 4. A file whose bytes are not of its encoding cannot be read

**A third kind of file that cannot be read** (ADR-0023 §2): a file of CSV whose bytes are not of the encoding of its source, or a file of JSON whose bytes are not UTF-8, which is what JSON is written in between systems (RFC 8259 §8.1). JSON has no field for an encoding.

- **Found by a decoder of the JDK that reports what it cannot decode**, for every file of every batch of CSV and of JSON, with the codec that Spark reads the file with (a file in gzip is decoded as Spark reads it). The look says the first byte that is not of the encoding, by its place in the file and its line, and the mark that the file starts with. Every byte of a file is looked at: the cost is one more read of each file, which a measurement prices (P17).
- **Once Spark has read the batch, and before the look at the files after the read** (`Landing.confirmAgain`). A file that Spark cannot read has been found as ADR-0023 finds one, and the look opens only files that Spark read whole; and the bytes looked at are between the two looks at the files, as the bytes read are. A look that fails as it reads a file has the files looked at again: one that changed makes it a batch whose source changed (ADR-0018); otherwise the failure passes, and the run tries again within its budget (ADR-0022).
- **A job of Spark, a task for each file**, in the jobs of the run, so that a run that loses its lease stops it (ADR-0006).
- **Under `fail`**, the run fails as permanent (4), naming each file, its encoding, the byte and the line. The batch stays planned. If the encoding of the source is what is wrong, the configuration is corrected and the batch given up (`abandon-batch`), since its plan has the encoding that it was planned with (§3).
- **Under `skip`**, the batch is given up and each such file recorded as `FAILED`, with its encoding, the byte and the line, as ADR-0023 §5 has it. The warning for each says what to do: correct `connector.csv.encoding`, if the source's is wrong, and touch the files, so that they are taken again (ADR-0023 §5, point 3). A reprocess cannot take them: it reads the files of a batch that was committed, and these are of one that was given up.
- **A reprocess** with such a file fails as permanent, whatever the policy (ADR-0023 §7).
- **Not a record's.** The file is the unit: an encoding is the file's, and what is set apart in a quarantine is a record as it was received, which its text, once decoded, no longer is. Such a file is not counted against `quality.max_rejected_ratio`.
- **Nothing of what the file holds is said.** What is recorded and logged is the encoding, the place and the line, and never the bytes, which may be part of a value.

### 5. What else is not of its encoding

Two things that no decoder refuses, and that the look of §4 finds by the bytes:

1. **Under ISO-8859-1, a byte of 0x80 to 0x9F.** It is a control character of ISO-8859-1, which text does not have, and the euro or a quote of windows-1252, which a spreadsheet writes: the message says to try `windows-1252`. A file of ISO-8859-1 that does have such characters is refused with it (known limit).
2. **The mark of UTF-8 at the start of a file whose source says an encoding of one byte.** It is a file of UTF-8, and read so its first name starts with `ï»¿`.

### 6. The mark of the order of bytes

- **The mark of UTF-8 is taken off by Spark**, of every file of a batch (pinned by `SparkCsvDialectContractSpec`), and never reaches a name or a value.
- **A name of a column that starts with U+FEFF fails its batch as permanent**, as a name that is empty does: it is a second mark, which some tools write before a file that has one. Spark keeps it, and the name would be one that no producer gave.
- **A mark of another encoding** is bytes that are not of the encoding said (§4), or the mark of §5.

## Alternatives

| Option | Verdict |
|---|---|
| Any charset of the JVM | Valid on one JVM and not on another, two hashes for two names of one charset, and §4 true only for the encodings tested. Rejected. |
| The options of Spark passed through | `ignoreCorruptFiles` or `DROPMALFORMED` would lose rows without a word. Rejected with the breakdown of P8. |
| windows-1252 left out, so that no internal setting of Spark is needed | A file of a spreadsheet said to be ISO-8859-1, the one encoding left for it, has its euro and its quotes as control characters, which no decoder refuses. With §5 such a file is refused, and the source has no encoding to read it in. Rejected: the setting is pinned, and §5 holds the case if it goes. |
| windows-1252 read into UTF-8 by the engine before Spark reads it | A copy of every file: the engine neither copies nor moves files (discovery §6.3). Rejected. |
| Bytes not of the encoding as a warning | Bronze keeps what the source did not send. Rejected. |
| A record with such bytes to the quarantine | What a quarantine keeps is a record as it was received, and its bytes are gone once it is decoded; Spark gives no place in the file for a record of CSV; a U+FFFD that the source wrote would be set apart; and a file in the wrong encoding would be split between Bronze and the quarantine. Rejected. |
| Look for U+FFFD in the rows read, and decode the files that have one | Cheaper when the files are sound, and as exact if every replacement is a U+FFFD in a value, a name or a record set apart, which is a fact of Spark's parser and not of the JDK. Not now: the look of §4 is right whatever the parser does, and a measurement may choose the other (P17). |
| The dialect of a replay from the configuration of the run that planned the batch (`planned_by_run`, its snapshot) | No new form of plan, but the replay would read an old configuration with the parser of today, and read with two configurations at once. The plan is the record of the batch (ADR-0005). Rejected. |
| A replay whose dialect changed refused, as a plan of another directory is | A source stopped by a change made for the files to come, and the files of the plan read again with a dialect that was not theirs once given up. Rejected. |
| UTF-16 on the list | A file with no mark is read as big-endian, wrong and with no error. Not now (§2). |
| A field for values over several lines | Without them a quoted value with the end of a line in it is two records, and nothing gains from it but the split of one large file. Rejected, as for JSON. |

## Consequences

- A source of CSV reads its own dialect and encoding, from four fields that are validated before anything is opened, and every source that says nothing reads as before, with the hash that it had.
- A batch reads the same on every replay, whatever the configuration says by then, and the run says when the two differ.
- **No byte that is not of the encoding of its source reaches Bronze as something else.** What is found is the file, by its name, the place and the line, stopped or skipped as its source says.
- **Every file of CSV and of JSON is read once more, whole**, at every batch. Measured in P17, where the look of the alternatives is the optimization.
- **Known limit: a file in an encoding of one byte said to be in another is not found, but for the bytes of §5.** A file of windows-1252 said to be ISO-8859-1 is found by its bytes of 0x80 to 0x9F; one without them is read as it would be in windows-1252, the same characters.
- **Known limit: a file of ISO-8859-1 with control characters of 0x80 to 0x9F is refused** (§5). Such a source has no encoding of the list to read it.
- **Known limit: a file of UTF-16 with no mark, said to be UTF-8, is UTF-8** with a NUL between its characters.
- **Known limit: between quotes, the end of line of a file is read as LF.** A file whose lines end in CRLF, as RFC 4180 writes them, has LF in Bronze where a quoted value had CRLF; one whose lines end in CR, LF for each CR and two LF for a CRLF. Spark gives no option for it. The reference of CSV says so.
- **Known limit: a file that mixes its ends of lines** has its records set apart, or a CR left at the end of a value, by which end of line comes first.
- **Known limit: a file is read whole, by one task.** A large file is read by one core.
- Under `skip`, an encoding of the source that is wrong has every file recorded as `FAILED`, each with a warning that says to correct the encoding and touch the files.
- `spark.sql.legacy.javaCharsets` is on for any SQL of the session, Silver's included (P10).
- `SparkCsvDialectContractSpec` fails on an upgrade of Spark that changes what the table above says.
