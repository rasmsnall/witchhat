# witchhat: Architecture

**Document type** Technical architecture specification
**Status** Partial. One kernel (composite hashing) is implemented end to end, behind both the Rust and the Python surface. Schema validation, JSON normalization, regex cleanup, and native transformations are not yet built; see Chapter XI for what that means for this document.
**Audience** Anyone integrating, operating, or extending this library. No prior context assumed.
**Companion documents** `api.md` for the callable surface, `operations.md` for building and deploying it.
**Version** 1.0
**Date** 2026-09-16

---

## Contents

- I. Introduction
  - 1. Purpose
  - 2. Rationale
  - 3. Scope and non-goals
- II. Data Model
  - 1. Arrow as the interop boundary
  - 2. Why not a bespoke format
- III. Composite Hashing
  - 1. What it computes
  - 2. Versioning discipline
  - 3. Type tagging
  - 4. Null and float handling
  - 5. The two fingerprint shapes
- IV. CPU Feature Detection
  - 1. What it does today
  - 2. The constraint it exists to enforce
- V. Concurrency Model
- VI. Failure Model
- VII. Security Model
- VIII. Python Binding Boundary
  - 1. The pyo3 version pin
  - 2. Two-crate split
- IX. Dependencies
- X. Assessment
  - 1. Advantages
  - 2. Disadvantages
  - 3. Conditions under which this design is inappropriate
- XI. Status and What Comes Next
- References
- Appendix A. Glossary

### List of Tables

- `<Table 3-1>` Type tags used by the hashing kernel
- `<Table 3-2>` Arrow types supported by `hash_batch`
- `<Table 9-1>` Direct dependencies
- `<Table A-1>` Glossary of terms

### List of Figures

- `[Figure 1-1]` Where witchhat sits between Python and Arrow-native data
- `[Figure 3-1]` Row hash construction

---

## I. Introduction

### 1. Purpose

witchhat is a Rust core, exposed to Python via PyO3, aimed at Spark and Databricks
workloads: replacing Spark-native transformations (composite hashing, schema validation,
JSON normalization, regex-heavy cleanup) with native kernels that run an order of
magnitude faster on a single node, while staying comparable to Spark's own output.

### 2. Rationale

Spark's per-row and per-column transformation UDFs cross the JVM/Python boundary for
every batch, and generic UDF execution carries interpreter and serialization overhead
that a purpose-built native kernel does not pay. For workloads that are CPU-bound rather
than shuffle-bound, that overhead is often the dominant cost. witchhat's kernels are
meant to be dropped into that gap: called from a Databricks notebook or job exactly where
a Spark UDF would have been, but running as compiled Rust against Arrow's native memory
layout instead of interpreted per-row logic.

### 3. Scope and non-goals

In scope, as of this document's version:

- Composite row and table hashing over Arrow `RecordBatch` data (Chapter III).
- Schema shape fingerprinting.
- CPU feature detection as infrastructure for future SIMD kernels (Chapter IV).

Explicitly not in scope for this document, because not yet built: schema validation
against an expected shape, JSON normalization, regex-based cleanup kernels, an
output-equivalence test harness, and any kernel that replaces a Spark filter, join, or
aggregate. See Chapter XI and the repository's `README.md` for the ordered backlog.

Also not in scope, by design: witchhat is not a distributed engine. It targets
single-node throughput on one Databricks driver or worker; splitting work across a Spark
cluster is the caller's job, not this library's.

```
Spark UDF (JVM <-> Python <-> interpreted row logic)
Databricks notebook/job -> witchhat kernel (Rust, Arrow-native, single node)
```

[Figure 1-1] Where witchhat sits between Python and Arrow-native data

## II. Data Model

### 1. Arrow as the interop boundary

`witchhat-core` re-exports `arrow_schema`/`arrow_array` types (`Schema`, `Field`,
`DataType`, `RecordBatch`) rather than defining its own columnar representation. Arrow is
already the format Spark, Databricks, pyarrow, polars and pandas converge on, so a batch
can move from any of those into a witchhat kernel and back with no copy: the Python
binding boundary (Chapter VIII) accepts anything implementing the Arrow C Data interface,
not `pyarrow` specifically.

### 2. Why not a bespoke format

A hand-rolled columnar type (as an earlier prototype of this repository had, for an
unrelated monitoring workload) would need its own conversion layer at every boundary this
library touches: pyarrow, Spark's Arrow-based UDF path, Delta/Parquet readers. Arrow
removes that layer entirely, at the cost of depending on a large external crate family.
That cost is accepted because every consumer this library targets already pays it.

## III. Composite Hashing

### 1. What it computes

`hash_batch(batch, columns, version)` fingerprints the named columns of a `RecordBatch`,
in the given order, into one `u64` per row. It starts from a version-specific seed and
folds each column's per-cell hash into a running accumulator with an order-sensitive
combiner, so `(a, b)` and `(b, a)` hash differently for the same values:

```
seed(version)
  -> combine(column[0][row])
  -> combine(column[1][row])
  -> ... -> row hash
```

[Figure 3-1] Row hash construction

`hash_batch_all_columns` is the same operation over every column in schema order.

### 2. Versioning discipline

[`HashVersion`](../crates/witchhat-core/src/hash.rs) names one exact, frozen algorithm.
`"v1"` is the only version today. A future improvement to the algorithm (a faster mixer,
a wider type table) ships as `"v2"`, never as a silent change to what `"v1"` produces:
a fingerprint stored today, or used as a partition key, must still be reproducible next
year under the same version name. This is the same discipline `schema_fingerprint` and
`table_fingerprint` follow, and the reason both take a `HashVersion` parameter rather than
always using whatever the latest algorithm happens to be.

### 3. Type tagging

Two Arrow columns of different types can share a byte pattern: an `Int32` value of `5`
and an `Int64` value of `5` both contain the bytes `[5, 0, 0, 0]` in their common prefix,
and a `Boolean` `true` is the single byte `1`. Every value hashed is therefore prefixed
with a one-byte type tag before hashing, so values that would otherwise collide on raw
bytes never do.

<Table 3-1> Type tags used by the hashing kernel

| Tag | Type |
|---|---|
| `0x00` | (reserved: marks a null, appended after the value's own tag) |
| `0x01` | Boolean |
| `0x02`-`0x09` | Int8/16/32/64, UInt8/16/32/64 |
| `0x0A`-`0x0B` | Float32, Float64 |
| `0x0C` | Utf8 / LargeUtf8 |
| `0x0D` | Binary / LargeBinary |

<Table 3-2> Arrow types supported by `hash_batch`

| Supported | Not yet supported |
|---|---|
| Boolean, all integer widths, Float32/64, Utf8/LargeUtf8, Binary/LargeBinary | Decimal, Date/Time/Timestamp, List, Struct, Dictionary |

A column outside the supported set returns `Error::UnsupportedType` rather than silently
falling back to a text representation; see Chapter VI.

### 4. Null and float handling

A `null` cell hashes distinctly from every non-null value of its column, including an
empty string or a numeric zero, by hashing a fixed sentinel byte instead of the (absent)
value's bytes. `Float32`/`Float64` values are canonicalized before hashing: every `NaN`
bit pattern collapses to Rust's canonical `NAN` constant, and `-0.0` is normalized to
`0.0`. This makes the hash agree with IEEE-754 equality (`NaN != NaN` is irrelevant here;
what matters is that two `NaN` payloads that Spark or pyarrow both call "the same missing
value" hash the same) rather than with the bit pattern, which would make two
representations of the same logical float hash differently.

### 5. The two fingerprint shapes

`hash_batch`/`hash_batch_all_columns` produce a per-row fingerprint: the shape a dedup
key or change-data-capture identity needs. `table_fingerprint` folds a whole array of row
hashes into one `u64` with wrapping addition, which is commutative and associative, so a
re-shuffled or re-partitioned batch fingerprints the same. This is the shape an
output-equivalence check needs: fingerprint witchhat's rows, fingerprint Spark's rows,
compare the two integers, with neither side sorted first.

## IV. CPU Feature Detection

### 1. What it does today

`witchhat_core::cpu::features()` detects AVX2, AVX-512F and SSE4.2 on x86_64 and NEON on
aarch64, once per process, cached in a `OnceLock`. Nothing in the current hashing kernel
branches on it: `xxhash-rust`'s XXH3 implementation already does its own internal SIMD
dispatch, and its output is defined to be identical regardless of which internal code
path ran.

### 2. The constraint it exists to enforce

This module exists ahead of a concrete user because every kernel this library adds later
(regex cleanup, JSON normalization) is a SIMD dispatch candidate, and the constraint has
to be established before the first one is written, not retrofitted: **a SIMD-accelerated
path may only change speed, never output.** A fingerprint or transformation computed on a
Databricks driver with AVX-512 must equal one computed on a laptop with only SSE4.2, or
results stop being comparable across a mixed-hardware fleet, which defeats the purpose of
`table_fingerprint`-style equivalence checking. Any future kernel that adds a SIMD fast
path is expected to carry a differential test asserting exactly that.

## V. Concurrency Model

Every function in `witchhat-core` is synchronous, single-threaded, and allocation-bounded
by its input size; none spawn threads, perform I/O, or hold a lock across a call. The
PyO3 boundary (Chapter VIII) does not release the GIL during a call, because every
current operation is CPU-bound and short relative to the cost of a Python call itself.
This is expected to change once a kernel is expensive enough (a multi-hundred-megabyte
JSON normalization pass, say) that releasing the GIL for the duration becomes worth its
own overhead; Chapter XI tracks it as an open item.

## VI. Failure Model

`witchhat-core` returns `Result<T, witchhat_core::Error>` from every fallible function;
nothing panics on a caller-supplied input. `Error` has four variants: `UnknownColumn` (a
requested column name is not in the schema), `TypeMismatch` and `UnsupportedType` (a
column's Arrow type cannot be processed), and `Config` (an invalid argument combination).
At the Python boundary, every variant becomes a `RuntimeError` except an unrecognised
`HashVersion` name, which is checked separately and raised as `ValueError`, matching
Python's own convention of using `ValueError` for a bad argument rather than a generic
runtime failure.

## VII. Security Model

witchhat's current kernels take no network input, spawn no subprocess, and read no
filesystem path: the only input is Arrow data already resident in the caller's process.
`witchhat-core` is `#![forbid(unsafe_code)]`; the only `unsafe` in the dependency tree is
inside `pyo3`, `arrow`, and `xxhash-rust` themselves, none of which this crate's own code
touches directly. There is no credential handling, no path construction from
caller-supplied strings, and no logging of row data, so the security requirements that
dominate a system reading untrusted external input (see `rust-streamer-pgdb`'s
`CLAUDE.md` for an example of what that looks like) mostly do not yet apply here. They
will become relevant again once a kernel reads from an external path or network source
(a JSON-from-object-storage normalization pass, for instance), and should be revisited at
that point rather than assumed to still not apply.

## VIII. Python Binding Boundary

### 1. The pyo3 version pin

`witchhat-py` pins `pyo3 = "=0.25.1"` exactly, not a newer release. `arrow`'s `pyarrow`
Cargo feature, used for zero-copy `RecordBatch`/`ArrayData` conversion, links `pyo3-ffi`
as a native library and Cargo only tolerates one exact version of a `links`-declaring
crate across the whole dependency graph. Bumping `pyo3` therefore requires `arrow` to
have caught up to a newer `pyo3` first, not just editing the version string in
`Cargo.toml`; attempting that produces a resolver error at `cargo build` time (`only one
package in the dependency graph may specify the same links value`), which is the signal
to check `arrow`'s current `pyo3` pin before retrying.

### 2. Two-crate split

`witchhat-core` has no PyO3 dependency; `witchhat-py` is a thin translation layer over it
(see `crates/witchhat-py/src/python.rs`). This differs from `rust-streamer-pgdb`'s
single-crate layout, and is deliberate here: a future Rust-only consumer (a CLI, a
service embedding witchhat directly) links `witchhat-core` without pulling in `pyo3` or
its `abi3`/`extension-module` feature machinery at all.

## IX. Dependencies

<Table 9-1> Direct dependencies

| Crate | Why |
|---|---|
| `arrow-array`, `arrow-schema`, `arrow-data` | The data model (Chapter II); `witchhat-core` depends on the first two only |
| `arrow` (feature `pyarrow`) | Zero-copy conversion at the PyO3 boundary, `witchhat-py` only |
| `xxhash-rust` (feature `xxh3`) | The per-value hash function underlying `hash_batch` |
| `thiserror` | The `Error` enum (Chapter VI) |
| `pyo3` | Python bindings, `witchhat-py` only; see Section VIII.1 for the version pin |

Pinned 2026-09 (probed via `cargo build`; crates.io index reachable): `arrow 56.2.1`,
`xxhash-rust 0.8.18`, `thiserror 2.0.20`, `pyo3 0.25.1`.

## X. Assessment

### 1. Advantages

- Zero-copy Arrow interop: no serialization step between Spark/pyarrow/polars and a
  witchhat kernel.
- Versioning discipline applied from the first kernel, not retrofitted after a second one
  needed it.
- No `unsafe` in this crate's own code; the dependency surface is small and each
  dependency's role is documented (Chapter IX).

### 2. Disadvantages

- Single kernel implemented so far: the "replace Spark" goal is aspirational until
  Chapter XI's backlog lands.
- No SIMD-accelerated path yet, despite Chapter IV's infrastructure; XXH3's own internal
  dispatch is the only acceleration currently in effect.
- `table_fingerprint`'s wrapping-sum combiner is not collision-resistant against an
  adversarial input (Chapter III, Section 5); fine for equivalence testing between
  trusted pipelines, not a substitute for a cryptographic MAC.

### 3. Conditions under which this design is inappropriate

- A dataset whose Arrow representation does not fit in memory on one node: witchhat has
  no distributed execution model, and is not intended to gain one (Chapter I, Section 3).
- A column type outside Table 3-2's supported set, until that type is added.
- A use case needing cryptographic collision resistance from `table_fingerprint`, rather
  than equivalence-testing evidence.

## XI. Status and What Comes Next

Implemented and tested: composite row/table hashing, schema fingerprinting, CPU feature
detection, the Python binding boundary, the `abi3-py310` wheel build.

Not yet built, in the order the project's stated goal needs them: schema validation
against an expected shape, JSON normalization, a regex-based cleanup kernel, an
output-equivalence test harness built on `table_fingerprint`, and the native
transformations (filter/project/join/aggregate) that are the actual Spark replacement.
See the repository `README.md` for the up-to-date backlog; this document describes the
architecture of what exists, and is expected to gain chapters as each item above lands
rather than being rewritten from scratch.

## References

- Apache Arrow columnar format: <https://arrow.apache.org/docs/format/Columnar.html>
- Arrow C Data Interface: <https://arrow.apache.org/docs/format/CDataInterface.html>
- `xxhash-rust` / XXH3: <https://docs.rs/xxhash-rust>
- PyO3 user guide: <https://pyo3.rs>
- `rust-streamer-pgdb`, a companion project this documentation and CI convention is
  adopted from: `D:\ruststreamer\rust-streamer-pgdb\CLAUDE.md`

## Appendix A. Glossary

<Table A-1> Glossary of terms

| Term | Meaning |
|---|---|
| Row hash | One `u64` per row, from `hash_batch`/`hash_batch_all_columns` |
| Table fingerprint | One order-independent `u64` over a whole batch of row hashes |
| `HashVersion` | A named, frozen hashing algorithm; see Chapter III, Section 2 |
| Type tag | A one-byte prefix distinguishing Arrow types that could share raw bytes |
| abi3 | CPython's stable ABI; one compiled extension loads on every Python from the
declared floor version onward |
