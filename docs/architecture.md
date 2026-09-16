# witchhat: Architecture

**Document type** Technical architecture specification
**Status** Eight kernels (composite hashing, schema validation, JSON normalization, regex cleanup, output-equivalence testing, deduplication, join, aggregate) are implemented end to end, behind both the Rust and the Python surface. Filter and project need no witchhat-specific kernel (Arrow's own compute kernels already cover them); see Chapter XVIII for what remains.
**Audience** Anyone integrating, operating, or extending this library. No prior context assumed.
**Companion documents** `api.md` for the callable surface, `operations.md` for building and deploying it.
**Version** 1.3
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
- IV. Schema Validation
  - 1. What it computes
  - 2. Numeric widening
  - 3. Nullability compatibility
  - 4. Breaking versus informational
- V. JSON Normalization
  - 1. What it computes
  - 2. Malformed versus type-mismatched versus absent
  - 3. Why only four target types
- VI. Regex Cleanup
  - 1. Two ways to get rules
  - 2. Why only presets are versioned
- VII. Output Equivalence Testing
  - 1. What it computes
  - 2. Column order versus column type
  - 3. What "equivalent" does not mean
- VIII. Deduplication
  - 1. What it computes
  - 2. Why filter and project are not wrapped here
- IX. Join
  - 1. What it computes
  - 2. Why not built on `hash_batch`
  - 3. Column-name collisions and nullability
  - 4. Row order
- X. Aggregate
  - 1. What it computes
  - 2. Why group keys use the same row format as join
  - 3. Min/Max preserve type; Sum/Mean go through `f64`
  - 4. The empty-`group_by` case
- XI. CPU Feature Detection
  - 1. What it does today
  - 2. The constraint it exists to enforce
- XII. Concurrency Model
- XIII. Failure Model
- XIV. Security Model
- XV. Python Binding Boundary
  - 1. The pyo3 version pin
  - 2. Two-crate split
- XVI. Dependencies
- XVII. Assessment
  - 1. Advantages
  - 2. Disadvantages
  - 3. Conditions under which this design is inappropriate
- XVIII. Status and What Comes Next
- References
- Appendix A. Glossary

### List of Tables

- `<Table 3-1>` Type tags used by the hashing kernel
- `<Table 3-2>` Arrow types supported by `hash_batch`
- `<Table 4-1>` Numeric widenings accepted under `allow_numeric_widening`
- `<Table 5-1>` JSON-to-Arrow type mapping
- `<Table 6-1>` Built-in cleanup presets (`v1`)
- `<Table 10-1>` Aggregate functions
- `<Table 16-1>` Direct dependencies
- `<Table A-1>` Glossary of terms

### List of Figures

- `[Figure 1-1]` Where witchhat sits between Python and Arrow-native data
- `[Figure 3-1]` Row hash construction
- `[Figure 4-1]` Shape of a schema diff
- `[Figure 7-1]` Shape of an equivalence check
- `[Figure 9-1]` Shape of a join

---

## I. Introduction

### 1. Purpose

witchhat is a Rust core, exposed to Python via PyO3, aimed at Spark and Databricks
workloads: replacing Spark-native transformations (composite hashing, schema validation,
JSON normalization, regex-heavy cleanup, join, aggregate) with native kernels that run an
order of magnitude faster on a single node, while staying comparable to Spark's own
output.

### 2. Rationale

Spark's per-row and per-column transformation UDFs cross the JVM/Python boundary for
every batch, and generic UDF execution carries interpreter and serialization overhead
that a purpose-built native kernel does not pay. For workloads that are CPU-bound rather
than shuffle-bound, that overhead is often the dominant cost. witchhat's kernels are
meant to be dropped into that gap: called from a Databricks notebook or job exactly where
a Spark UDF would have been, but running as compiled Rust against Arrow's native memory
layout instead of interpreted per-row logic.

### 3. Scope and non-goals

In scope, as of this document's version: composite row and table hashing (Chapter III);
schema fingerprinting and validation (Chapter IV); JSON normalization (Chapter V); regex
cleanup (Chapter VI); output-equivalence testing (Chapter VII); deduplication
(Chapter VIII); join (Chapter IX); aggregate (Chapter X); CPU feature detection as
infrastructure for future SIMD kernels (Chapter XI).

Explicitly not in scope, by design: filter (row selection by a boolean predicate) and
project (column selection/reorder) already exist as Arrow compute kernels
(`arrow_select::filter::filter_record_batch`, `RecordBatch::project`) with no
witchhat-specific behaviour to add, so witchhat does not wrap them. See Chapter XVIII and
the repository's `README.md` for anything still genuinely open.

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
binding boundary (Chapter XV) accepts anything implementing the Arrow C Data interface,
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
year under the same version name. Every kernel that defines its own named behaviour
follows this same discipline with its own version enum: [`NormalizeVersion`](Chapter V)
and [`CleanupVersion`](Chapter VI). `join` and `aggregate` (Chapters IX-X) do not, since
neither defines a witchhat-specific algorithm the way hashing does; see Section 2 of each.

### 3. Type tagging

Two Arrow columns of different types can share a byte pattern: an `Int32` value of `5`
and an `Int64` value of `5` both contain the bytes `[5, 0, 0, 0]` in their common prefix,
and a `Boolean` `true` is the single byte `1`. Every value hashed is therefore prefixed
with a type key before hashing, so values that would otherwise collide on raw bytes never
do. For a type with parameters that change what a raw value means (`Time32`/`Time64`'s
unit, `Timestamp`'s unit and timezone, `Decimal128`/`Decimal256`'s precision and scale),
those parameters are folded into the type key too, not just a fixed tag byte: a
`Time32(Second)` `5` and a `Time32(Millisecond)` `5` are different instants, and a naive
`Timestamp` and one carrying an explicit `"UTC"` are not interchangeable (the same lesson
`rust-streamer-pgdb` learned the hard way about naive-vs-UTC timestamps, noted in its own
`CLAUDE.md`), so both hash differently despite identical underlying bytes.

<Table 3-1> Type tags used by the hashing kernel

| Tag | Type |
|---|---|
| `0x00` | (reserved: marks a null, appended after the value's own tag) |
| `0x01` | Boolean |
| `0x02`-`0x09` | Int8/16/32/64, UInt8/16/32/64 |
| `0x0A`-`0x0B` | Float32, Float64 |
| `0x0C` | Utf8 / LargeUtf8 |
| `0x0D` | Binary / LargeBinary |
| `0x0E`-`0x0F` | Date32, Date64 |
| `0x10` | Time32 (+ unit byte) |
| `0x11` | Time64 (+ unit byte) |
| `0x12` | Timestamp (+ unit byte, + timezone bytes if present) |
| `0x13` | Decimal128 (+ precision byte, + scale byte) |
| `0x14` | Decimal256 (+ precision byte, + scale byte) |

<Table 3-2> Arrow types supported by `hash_batch`

| Supported | Not yet supported |
|---|---|
| Boolean, all integer widths, Float32/64, Utf8/LargeUtf8, Binary/LargeBinary, Date32/64, Time32(Second\|Millisecond), Time64(Microsecond\|Nanosecond), Timestamp (any unit/timezone), Decimal128, Decimal256 | List, Struct, Dictionary |

A column outside the supported set returns `Error::UnsupportedType` rather than silently
falling back to a text representation; see Chapter XIII. `join` (Chapter IX) and
`aggregate`'s group keys (Chapter X) are not limited to this table, since neither is
built on `hash_batch`; see each chapter's Section 2.

### 4. Null and float handling

A `null` cell hashes distinctly from every non-null value of its column, including an
empty string or a numeric zero, by hashing a fixed sentinel byte instead of the (absent)
value's bytes. `Float32`/`Float64` values are canonicalized before hashing: every `NaN`
bit pattern collapses to Rust's canonical `NAN` constant, and `-0.0` is normalized to
`0.0`. This makes the hash agree with IEEE-754 equality rather than with the bit pattern,
which would make two representations of the same logical float hash differently.

### 5. The two fingerprint shapes

`hash_batch`/`hash_batch_all_columns` produce a per-row fingerprint: the shape a dedup
key or change-data-capture identity needs (Chapter VIII uses exactly this). `table_
fingerprint` folds a whole array of row hashes into one `u64` with wrapping addition,
which is commutative and associative, so a re-shuffled or re-partitioned batch
fingerprints the same. Chapter VII's equivalence check is built on this second shape.

## IV. Schema Validation

### 1. What it computes

`validate_schema(actual, expected, options)` compares two schemas column by column,
matched by name (case-sensitive), and returns a `SchemaDiff` describing exactly how they
differ rather than a bare yes/no:

```
expected.fields ---+                     missing:      in expected, not actual
                    +--> validate_schema  unexpected:   in actual, not expected
actual.fields   ----+                     retyped:      present in both, type differs
                                           nullability:  present in both, nullability tightened
```

[Figure 4-1] Shape of a schema diff

This complements `schema_fingerprint`: fingerprinting only says "these differ",
`validate_schema` says how, which is what a caller needs to decide whether a difference
is safe to proceed with.

### 2. Numeric widening

By default a column's type must match exactly. `ValidateSchemaOptions.allow_numeric_
widening` relaxes this for one specific, safe direction: `actual` being a wider numeric
type than `expected` within the same signedness and int-vs-float class.

<Table 4-1> Numeric widenings accepted under `allow_numeric_widening`

| Expected | Accepted actual |
|---|---|
| Int8 | Int16, Int32, Int64 |
| Int16 | Int32, Int64 |
| Int32 | Int64 |
| UInt8 | UInt16, UInt32, UInt64 |
| UInt16 | UInt32, UInt64 |
| UInt32 | UInt64 |
| Float32 | Float64 |

A narrower actual type, a cross-signedness change, or an integer-to-float change is never
accepted, even with the option on: those can silently change what a value means, not just
how many bits hold it. Deliberately more conservative than Spark's own implicit-cast
rules, since witchhat has no way to know whether a given caller's downstream logic can
tolerate the precision or sign change.

### 3. Nullability compatibility

`actual`'s nullability is checked against `expected`'s in one direction only: `actual`
may be nullable when `expected` is too, or non-nullable when `expected` allows null, but
not nullable when `expected` declares the column non-nullable. A promise of "never null"
is the only direction that can break a caller.

### 4. Breaking versus informational

`SchemaDiff` distinguishes what most callers cannot safely ignore from what is merely
informational. `missing`, `retyped` and `nullability` entries make `is_breaking()` true.
An `unexpected` column does not: additive schema evolution does not usually invalidate
code written against the old, narrower schema. Chapter VII's equivalence check is
deliberately *stricter* than this distinction (Section 3 there explains why).

## V. JSON Normalization

### 1. What it computes

`normalize_json(json, schema, version)` parses one JSON object per row of a `Utf8`
column into a fixed target `Schema`, returning both the resulting `RecordBatch` and a
`NormalizeStats`. A target field's name is a `.`-separated path into the JSON object
(`"address.city"` reads `{"address": {"city": ...}}`), so one level of nested-object
flattening is supported without a caller having to pre-flatten the JSON themselves.

<Table 5-1> JSON-to-Arrow type mapping

| JSON type | Target Arrow type |
|---|---|
| string | Utf8 |
| number (integral) | Int64 |
| number (any) | Float64 |
| boolean | Boolean |
| `null`, absent | (any of the above; see Section 2) |
| object, array | Not supported as a leaf value; see Section 2 |

### 2. Malformed versus type-mismatched versus absent

Three distinct situations produce a null, and only one of them is counted as a problem in
`NormalizeStats`, following the same "structural problems are counted, type uncertainty
degrades quietly" split `rust-streamer-pgdb` uses for its own third-party input:

- **A row's JSON does not parse, or is not an object at the top level.** Every column is
  null for that row, and it counts in `NormalizeStats::rows_malformed`. This is the
  loudest signal: the input was not JSON in the shape this function requires at all.
- **A path is absent, or its value is JSON `null`.** An ordinary, expected null. Not
  counted anywhere: a caller passing a schema wider than what every record actually
  populates is normal, not a data-quality problem.
- **A path is present with a value of the wrong JSON type** (a string where the column
  is `Int64`, an object or array as a leaf value, since neither is representable by any
  of the four target types). Written as null, and counted in `NormalizeStats::
  type_mismatches` per column. This is the middle case: the input's actual shape
  disagreed with what the caller declared, worth surfacing but not worth failing the
  whole row over.

### 3. Why only four target types

`Utf8`, `Int64`, `Float64`, `Boolean` cover every JSON scalar type one-to-one. Nested
objects and arrays as a *leaf* value are deliberately unsupported (they fall into the
type-mismatch case in Section 2) rather than serialized back to a JSON string or given a
`List`/`Struct` Arrow representation: either of those is a real feature with its own
design questions (recursive flattening depth, array-of-object handling) that has not
been asked for yet. One level of *object* nesting is supported today only because a
dotted field path costs nothing extra to implement once path-walking exists at all.

## VI. Regex Cleanup

### 1. Two ways to get rules

`apply_rules(input, rules)` takes a caller-supplied list of `CleanRule` (a compiled
regex plus its replacement text) and applies them in order to every non-null value.
`clean_with_preset(input, name, version)` is `preset(name, version)` (look up one of
witchhat's own named rule sets) followed by `apply_rules`.

<Table 6-1> Built-in cleanup presets (`v1`)

| Preset | Effect |
|---|---|
| `trim_whitespace` | Removes leading and trailing whitespace |
| `collapse_whitespace` | Collapses any run of whitespace to a single space |
| `strip_control_characters` | Removes ASCII control characters |
| `strip_non_alphanumeric` | Removes everything except letters, digits and whitespace |
| `digits_only` | Removes everything except `0`-`9` |

### 2. Why only presets are versioned

`CleanupVersion` gates `preset`'s name-to-rules table, not `apply_rules` itself. A
caller's own regex, passed directly to `apply_rules`, is the caller's own algorithm:
witchhat did not write it and has no more business versioning it than it does
versioning a caller's SQL. Only witchhat's *own* named behaviour (a preset shipped under
a fixed name) needs the same "a version, once shipped, never changes what it produces"
discipline as `HashVersion`, because only that behaviour is something a caller might
depend on by name across a witchhat upgrade.

## VII. Output Equivalence Testing

### 1. What it computes

`check_equivalence(actual, expected, options)` answers "are these two batches the same
data", combining Chapter IV's schema comparison with Chapter III's table fingerprint
rather than adding a third comparison algorithm:

```
actual.schema, expected.schema -> validate_schema -> schema_diff
actual.rows,   expected.rows   -> hash_batch, table_fingerprint (shared column order) -> two u64s
                                                                          |
                                                            EquivalenceReport.is_equivalent()
```

[Figure 7-1] Shape of an equivalence check

### 2. Column order versus column type

The two table fingerprints are computed over the same column list and order
(`EquivalenceOptions.columns`, defaulting to the columns `expected` and `actual` have in
common, in `expected`'s order), so a column merely declared in a different position on
each side does not by itself cause a mismatch, even though `hash_batch` is normally
order-sensitive (Chapter III, Section 1). A column whose *type* differs still causes a
mismatch, because `hash_batch` hashes a value's type tag along with its bytes
(Chapter III, Section 3): an `Int32` `5` and an `Int64` `5` are not the same fingerprint,
even though `validate_schema` might consider that difference an accepted widening. This
is deliberate: equivalence testing asks "is the output the same", not "is the output an
acceptable evolution of the reference", which is what schema validation asks.

### 3. What "equivalent" does not mean

`EquivalenceReport::is_equivalent()` is deliberately stricter than
`SchemaDiff::is_breaking()`: it requires the schema diff to be entirely empty, not just
non-breaking. An `actual` batch with one extra column passes `validate_schema`'s
breaking check (additive change) but fails `is_equivalent()`, because the question being
asked is "are these the same", not "would `actual` be a safe evolution of `expected`".

## VIII. Deduplication

### 1. What it computes

`drop_duplicates(batch, columns, version)` is a native transformation equivalent to
Spark's `df.dropDuplicates(subset=columns)`. It reuses `hash_batch` as the dedup key: one
pass computes each row's composite hash over `columns`, a `HashSet<u64>` tracks which
hashes have been seen, and `arrow_select::filter::filter_record_batch` keeps only the
rows whose hash was new. Which row survives within a duplicate group is always the first
one by input order, unlike Spark's own `dropDuplicates`, whose choice there is
unspecified.

### 2. Why filter and project are not wrapped here

Filter (row selection by a boolean predicate) and project (column selection/reorder)
already exist as Arrow compute kernels (`arrow_select::filter::filter_record_batch`,
`RecordBatch::project`) with no witchhat-specific behaviour to add: wrapping them would
be a naming exercise, not a transformation. `dedup.rs` exists because deduplication
*does* need witchhat's own hashing, and is the natural first module for anything that
does; `join` and `aggregate` (Chapters IX-X) followed for the same reason.

## IX. Join

### 1. What it computes

`join(left, right, left_keys, right_keys, how)` matches rows of `left` and `right` on
key columns, in one of four modes:

```
left.keys  ---+                right_index = HashMap<row bytes, right row indices>
              +--> row-format  for each left row: probe right_index
right.keys ---+  (arrow_row)   -> matched pairs, plus outer-join leftovers depending on `how`
```

[Figure 9-1] Shape of a join

`Inner` keeps only matched rows; `Left`/`Right` additionally keep every unmatched row of
`left`/`right` respectively, with the other side's columns null; `Full` keeps every
unmatched row of both.

### 2. Why not built on `hash_batch`

A join result is only as correct as its key comparison. `table_fingerprint`
(Chapter VII) treats a `u64` collision as an acceptable, documented risk because it is
evidence toward a yes/no answer a human or CI job interprets; a join that silently
merged two distinct keys into one match because their fingerprints happened to collide
would produce *wrong data*, not weaker evidence. `join.rs` therefore uses
[`arrow_row::RowConverter`](https://docs.rs/arrow-row) instead: it converts each side's
key columns into a canonical, memcmp-comparable byte row, so two rows compare equal only
when they truly are. This has a useful side effect: `RowConverter` supports a broader
range of Arrow types than `hash_batch`'s own hand-rolled dispatch (Table 3-2), so a join
key can be a type `hash_batch` does not yet cover.

### 3. Column-name collisions and nullability

The output schema is every field of `left` followed by every field of `right`; a `right`
field whose name collides with a `left` field is suffixed `_right` (matching a common
convention, e.g. `pandas.merge`'s `suffixes`), rather than raising or silently keeping a
schema with a duplicate field name (which would confuse any downstream name-based column
lookup). Every output field is nullable regardless of the input schemas' own
nullability: an outer join can always introduce a null on either side, and a schema
should never promise "never null" for a column that can, in fact, be null under a
different `how` than the one just used.

### 4. Row order

Every `left` row in its original order, each repeated once per match (or once with null
`right` columns, under `Left`/`Full`, if unmatched), followed by every unmatched `right`
row in its original order, under `Right`/`Full`. Chosen for predictability (`left`'s
order is always the primary order) rather than to match Spark's own join output order,
which is not itself guaranteed.

## X. Aggregate

### 1. What it computes

`aggregate(batch, group_by, aggregations)` groups `batch`'s rows by `group_by` and
reduces each group's columns with the given `aggregations`.

<Table 10-1> Aggregate functions

| Function | Input types | Output | Null handling |
|---|---|---|---|
| `Count` | any | `Int64` | Counts non-null values |
| `Sum` | numeric (Table 3-2's numeric subset) | `Float64` | `None` if every value in the group is null |
| `Mean` | numeric | `Float64` | `None` if every value in the group is null |
| `Min` | numeric | matches input | `None` if every value in the group is null |
| `Max` | numeric | matches input | `None` if every value in the group is null |

### 2. Why group keys use the same row format as join

A group boundary is exactly the same kind of correctness question a join's key match is
(Chapter IX, Section 2): which rows belong together, not "probably belong together". So
`aggregate.rs` groups rows with the same `arrow_row::RowConverter` approach `join.rs`
uses, for the same reason, rather than `hash_batch`'s fingerprint. Group order in the
output is first-seen order (the order each distinct key first appears in `batch`), the
same convention `dedup` (Chapter VIII) uses for surviving rows.

### 3. Min/Max preserve type; Sum/Mean go through `f64`

`Sum` and `Mean` accumulate through `f64` regardless of the input's integer or float
type, which is unavoidable for a sum that must not overflow a fixed-width integer type,
but does mean an `Int64`/`UInt64` value beyond `f64`'s exact-integer range (±2^53) can
lose precision in the accumulated result. `Min`/`Max` avoid this for the *output* value:
rather than converting to compare, then returning the converted value, they use the
`f64` conversion only to find which row is the extreme value, then `take` that row's
original, untouched value from the source array — so a `Min` of an `Int64` column stays
an exact `Int64`, and only the comparison step (not the returned value) has `f64`'s
precision limit.

### 4. The empty-`group_by` case

`group_by` may be empty, treating the whole batch as one group: a whole-table aggregate,
matching `df.agg(...)` called with no preceding `groupBy`. An empty input batch in that
case still produces exactly one output row (`Count` `0`, every other aggregation
`None`), matching the conventional definition of an aggregate over zero rows, rather than
zero output rows, which would make a caller's `for row in result` silently do nothing
instead of seeing the "no data" answer.

## XI. CPU Feature Detection

### 1. What it does today

`witchhat_core::cpu::features()` detects AVX2, AVX-512F and SSE4.2 on x86_64 and NEON on
aarch64, once per process, cached in a `OnceLock`. Nothing in the current kernels
branches on it: `xxhash-rust`'s XXH3 (Chapter III) already does its own internal SIMD
dispatch with output defined to be identical regardless of code path; the regex engine
(Chapter VI) does its own dispatch the same way; JSON parsing (Chapter V), schema
validation (Chapter IV), deduplication (Chapter VIII), join (Chapter IX) and aggregate
(Chapter X) do no per-byte SIMD-shaped work at all.

### 2. The constraint it exists to enforce

This module exists ahead of a concrete user because a future kernel could be a SIMD
dispatch candidate, and the constraint has to be established before one is written, not
retrofitted: **a SIMD-accelerated path may only change speed, never output.** A
fingerprint or transformation computed on a Databricks driver with AVX-512 must equal one
computed on a laptop with only SSE4.2, or results stop being comparable across a
mixed-hardware fleet, which defeats the purpose of `table_fingerprint`/`check_
equivalence`-style checking. Any future kernel that adds a SIMD fast path is expected to
carry a differential test asserting exactly that.

## XII. Concurrency Model

Every function in `witchhat-core` is synchronous, single-threaded, and allocation-bounded
by its input size; none spawn threads, perform I/O, or hold a lock across a call. The
PyO3 boundary (Chapter XV) does not release the GIL during a call, because every
current operation is CPU-bound and short relative to the cost of a Python call itself.
This is expected to change once a kernel is expensive enough that releasing the GIL for
the duration becomes worth its own overhead; Chapter XVIII tracks it as an open item.

## XIII. Failure Model

`witchhat-core` returns `Result<T, witchhat_core::Error>` from every fallible function;
nothing panics on a caller-supplied input. `Error` has five variants: `UnknownColumn`,
`TypeMismatch` (used directly by `join`'s key-type check, Chapter IX) and
`UnsupportedType` (a column cannot be processed), `SchemaMismatch` (wraps an
`arrow_select`/`RecordBatch::try_new` failure from `dedup`, `join` or `aggregate`'s
output construction), and `Config` (an invalid argument: an unrecognised cleanup preset
name, a regex that does not compile, or `join`'s key-list length/emptiness check).
`validate_schema` and `normalize_json`'s row-level problems are deliberately *not*
`Error`: an unequal schema is a normal `SchemaDiff`, and a malformed JSON row is a
normal, counted null, not a failure of the whole batch (Chapter V, Section 2) — only a
structural problem that makes the *call itself* impossible (an unsupported target type,
an unknown column, mismatched join key types) is an `Error`. At the Python boundary,
every `Error` variant becomes a `RuntimeError` except an unrecognised version/preset/
join-type/aggregate-function name, which is raised as `ValueError`, matching Python's
own convention.

## XIV. Security Model

witchhat's current kernels take no network input, spawn no subprocess, and read no
filesystem path: the only input is Arrow data (or, for `normalize_json`, JSON text
already inside an Arrow column) already resident in the caller's process. `witchhat-core`
is `#![forbid(unsafe_code)]`; the only `unsafe` in the dependency tree is inside `pyo3`,
`arrow`, `xxhash-rust`, `regex`, `serde_json` and `arrow_row` themselves, none of which
this crate's own code touches directly. `normalize_json` parses caller-supplied JSON text
with `serde_json`, a widely used, actively maintained parser; a malformed or adversarial
JSON string degrades to a counted null (Chapter V, Section 2) rather than being retried
or logged verbatim, so a hostile row cannot escalate into anything beyond "this row is
null". There is no credential handling and no logging of row data, so the security
requirements that dominate a system reading untrusted *external* input (network, files;
see `rust-streamer-pgdb`'s `CLAUDE.md` for what that looks like) mostly do not yet apply
here, and should be revisited once a kernel reads from an external path or network
source.

## XV. Python Binding Boundary

### 1. The pyo3 version pin

`witchhat-py` pins `pyo3 = "=0.25.1"` exactly, not a newer release. `arrow`'s `pyarrow`
Cargo feature, used for zero-copy `RecordBatch`/`ArrayData` conversion, links `pyo3-ffi`
as a native library and Cargo only tolerates one exact version of a `links`-declaring
crate across the whole dependency graph. Bumping `pyo3` therefore requires `arrow` to
have caught up to a newer `pyo3` first, not just editing the version string.

One consequence, hit while adding schema validation and reused for every kernel since:
`arrow`'s pyarrow bridge implements `ToPyArrow`/`FromPyArrow` for `ArrayData`, `DataType`,
`Schema`, `Field`, `RecordBatch` and `Vec<T>` of those, but not for typed arrays
(`UInt64Array`, `StringArray`) and not `Clone` on the `PyArrowType<T>` wrapper itself.
Every kernel taking or returning a typed array converts through `ArrayData` at the
`witchhat-py` boundary (`u64_array_to_pyarrow`/`_from_pyarrow`,
`string_array_to_pyarrow`/`_from_pyarrow` in `crates/witchhat-py/src/python.rs`); a
`#[pyclass]` field holding per-column `DataType` pairs (`SchemaDiff.retyped`) stores
plain, `Clone`-able `DataType` and builds a fresh `PyArrowType` in a hand-written
`#[getter]`, since `#[pyo3(get)]` needs to clone the field to return it.

### 2. Two-crate split

`witchhat-core` has no PyO3 dependency; `witchhat-py` is a thin translation layer over it
(see `crates/witchhat-py/src/python.rs`). This differs from `rust-streamer-pgdb`'s
single-crate layout, and is deliberate here: a future Rust-only consumer (a CLI, a
service embedding witchhat directly) links `witchhat-core` without pulling in `pyo3` or
its `abi3`/`extension-module` feature machinery at all.

## XVI. Dependencies

<Table 16-1> Direct dependencies

| Crate | Why |
|---|---|
| `arrow-array`, `arrow-schema`, `arrow-data` | The data model (Chapter II); `witchhat-core` depends on the first two only |
| `arrow-select` | `filter_record_batch`/`take`, underlying `dedup`, `join` and `aggregate`'s output construction |
| `arrow-row` | `RowConverter`, underlying `join` (Chapter IX) and `aggregate`'s (Chapter X) grouping/matching |
| `arrow` (feature `pyarrow`) | Zero-copy conversion at the PyO3 boundary, `witchhat-py` only |
| `xxhash-rust` (feature `xxh3`) | The per-value hash function underlying `hash_batch` |
| `serde_json` | JSON parsing for `normalize_json` (Chapter V) |
| `regex` | Pattern compilation and replacement for `clean` (Chapter VI) |
| `thiserror` | The `Error` enum (Chapter XIII) |
| `pyo3` | Python bindings, `witchhat-py` only; see Section XV.1 for the version pin |

Pinned 2026-09 (probed via `cargo build`; crates.io index reachable): `arrow 56.2.1`,
`xxhash-rust 0.8.18`, `serde_json 1.0.151`, `regex 1.13.1`, `thiserror 2.0.20`,
`pyo3 0.25.1`.

## XVII. Assessment

### 1. Advantages

- Zero-copy Arrow interop: no serialization step between Spark/pyarrow/polars and a
  witchhat kernel.
- Versioning discipline applied consistently across every kernel that defines its own
  named behaviour (hashing, JSON normalization, cleanup presets).
- `validate_schema` and `check_equivalence` both report *how* two things differ, not
  just whether they do, so a caller can act on the specific difference.
- `join` and `aggregate` use the correctness-appropriate tool (`arrow_row`'s exact byte
  comparison) rather than reusing `hash_batch`'s probabilistic fingerprint where a
  collision would silently produce wrong data, not just weaker evidence.
- Deduplication reuses the hashing kernel rather than adding a second row-comparison
  algorithm; filter and project were left to Arrow's own kernels rather than reinvented.
- No `unsafe` in this crate's own code; the dependency surface is small and each
  dependency's role is documented (Chapter XVI).

### 2. Disadvantages

- No SIMD-accelerated path yet, despite Chapter XI's infrastructure; XXH3's and
  `regex`'s own internal dispatch is the only acceleration currently in effect.
- `table_fingerprint`'s wrapping-sum combiner (and by extension `check_equivalence`) is
  not collision-resistant against an adversarial input; fine for equivalence testing
  between trusted pipelines, not a substitute for a cryptographic MAC.
- `normalize_json`'s one-level-of-object-path support (Chapter V, Section 3) will not
  satisfy every real-world nested JSON shape; array-of-object flattening in particular
  is unbuilt.
- `validate_schema`'s numeric-widening table is deliberately narrower than Spark's own
  implicit-cast rules; a schema comparison Spark would accept silently can still be
  reported as retyped here.
- `aggregate`'s `Sum`/`Mean`/`Min`/`Max` are limited to the numeric subset of Table 3-2
  (no string min/max, no date/timestamp/decimal aggregation yet), and `Sum`/`Mean`
  accumulate through `f64` (Chapter X, Section 3).
- `join` requires an exact Arrow type match between paired key columns; no implicit
  coercion, matching `validate_schema`'s own conservatism (Chapter IV, Section 2).

### 3. Conditions under which this design is inappropriate

- A dataset whose Arrow representation does not fit in memory on one node: witchhat has
  no distributed execution model, and is not intended to gain one (Chapter I, Section 3).
- A column type outside Table 3-2's supported set, for `hash_batch`/`dedup`/
  `check_equivalence` specifically (`join`/`aggregate`'s group/key columns are not
  limited to this table).
- JSON with array-valued fields, or more than one level of object nesting, that need to
  be preserved rather than degraded to a counted null.
- A use case needing cryptographic collision resistance from `table_fingerprint`/
  `check_equivalence`, rather than equivalence-testing evidence.
- An aggregate over non-numeric columns beyond `Count`, or over `Decimal`/`Date`/
  `Time`/`Timestamp` columns.

## XVIII. Status and What Comes Next

Implemented and tested: composite row/table hashing (now covering Date/Time/Timestamp/
Decimal in addition to the original numeric/string/binary set), schema fingerprinting
and validation, JSON normalization, regex cleanup (ad hoc and presets),
output-equivalence testing, deduplication, join (inner/left/right/full), aggregate
(count/sum/mean/min/max), CPU feature detection, the Python binding boundary, the
`abi3-py310` wheel build.

Open, tracked separately from the Rust/Python surface itself because neither is a matter
of more code: publishing to a package repository (needs credentials this repository's
automation does not have) and verifying installation from a Unity Catalog Volume against
a real Databricks workspace (needs access this development environment does not have);
see `docs/operations.md` Chapter VI for both. Also open: broader `aggregate` type
support (Section 2 above), and array-valued/deeper-nested JSON (Chapter V, Section 3).
See the repository `README.md` for the up-to-date backlog; this document describes the
architecture of what exists, and is expected to gain chapters as each item lands rather
than being rewritten from scratch.

## References

- Apache Arrow columnar format: <https://arrow.apache.org/docs/format/Columnar.html>
- Arrow C Data Interface: <https://arrow.apache.org/docs/format/CDataInterface.html>
- `arrow-row` (`RowConverter`): <https://docs.rs/arrow-row>
- `xxhash-rust` / XXH3: <https://docs.rs/xxhash-rust>
- `serde_json`: <https://docs.rs/serde_json>
- `regex` crate syntax: <https://docs.rs/regex>
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
| Type key | A tag (plus parameters, for types like `Timestamp`) distinguishing Arrow types that could share raw bytes; see Chapter III, Section 3 |
| `SchemaDiff` | The structured result of `validate_schema`; see Chapter IV |
| Breaking (schema diff) | A missing, retyped, or nullability-tightened column; see Chapter IV, Section 4 |
| `NormalizeStats` | Malformed-row and type-mismatch counts from `normalize_json`; see Chapter V, Section 2 |
| Preset | A named, versioned built-in `CleanRule` set; see Chapter VI |
| `EquivalenceReport` | The structured result of `check_equivalence`; see Chapter VII |
| Row format | `arrow_row`'s canonical, memcmp-comparable byte encoding of one or more columns; the basis for `join` and `aggregate`'s grouping; see Chapter IX, Section 2 |
| abi3 | CPython's stable ABI; one compiled extension loads on every Python from the
declared floor version onward |
