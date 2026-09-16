# witchhat: API Reference

**Document type** Interface specification
**Status** Describes the surface as built: composite hashing, schema validation, JSON normalization, regex cleanup, output-equivalence testing, deduplication, join, aggregate, schema/CPU introspection, the `witchhat.spark` Databricks/Spark integration layer, and optional JSON metric logging.
**Audience** Anyone calling this library from Python or from Rust.
**Companion documents** `architecture.md` for why the design is shaped this way, `operations.md` for building and deploying it.
**Version** 1.7
**Date** 2026-09-16

---

## Contents

- I. Introduction
  - 1. Purpose
  - 2. Which surface to use
- II. Python Surface
  - 1. Installation and import
  - 2. `hash_rows`
  - 3. `hash_rows_all_columns`
  - 4. `table_fingerprint`
  - 5. `schema_fingerprint`
  - 6. `validate_schema` and `SchemaDiff`
  - 7. `normalize_json` and `NormalizeStats`
  - 8. `clean_with_preset` and `clean_with_rules`
  - 9. `check_equivalence` and `EquivalenceReport`
  - 10. `drop_duplicates`
  - 11. `join`, `join_null_safe`
  - 12. `aggregate`
  - 13. `cpu_features` and `CpuFeatures`
- III. Databricks/Spark Surface (`witchhat.spark`)
  - 1. Import and dependency
  - 2. `to_arrow_schema`, `map_in_arrow`
  - 3. `hash_rows`
  - 4. `clean_with_preset`, `clean_with_rules`
  - 5. `drop_duplicates`
  - 6. `aggregate`
  - 7. `collect_as_record_batch`, `broadcast_join`
  - 8. `validate_schema`, `schema_fingerprint`
- IV. Metric Logging (`witchhat.metrics`)
  - 1. `enable`, `disable`, `is_enabled`
  - 2. Event shape
  - 3. `row_count`, `measure`
- V. Rust Surface
  - 1. Entry points
  - 2. Version enums
  - 3. Module map
  - 4. Error type
- VI. Semantics Callers Must Know
  - 1. Column order
  - 2. Null and float equality
  - 3. Version pinning
  - 4. What accepts non-pyarrow objects
  - 5. `validate_schema` argument order
  - 6. `check_equivalence` is stricter than "not breaking"
  - 7. `join` requires exact key-type matches
  - 8. `aggregate` is numeric-only beyond `Count`
  - 9. `witchhat.spark`'s `repartition=True` default
- VII. Worked Examples
  - 1. Deduplicating rows
  - 2. Checking output against Spark
  - 3. Validating a batch before processing it
  - 4. Normalizing JSON, then cleaning a column
  - 5. Joining and aggregating
  - 6. A Databricks notebook pipeline with `witchhat.spark`
  - 7. Measuring latency and throughput
  - 8. Calling from Rust
- References
- Appendix A. Parameter quick reference

### List of Tables

- `<Table 2-1>` Parameters of `hash_rows`
- `<Table 2-2>` Exceptions raised by the Python surface
- `<Table 2-3>` Fields and methods of `SchemaDiff`
- `<Table 2-4>` Fields of `NormalizeStats`
- `<Table 2-5>` Built-in cleanup presets
- `<Table 2-6>` Fields and methods of `EquivalenceReport`
- `<Table 2-7>` Aggregate functions accepted by `aggregate`
- `<Table 3-1>` `witchhat.spark` functions
- `<Table 4-1>` Fields common to every metric event
- `<Table 5-1>` Public Rust modules
- `<Table A-1>` Parameter quick reference

### List of Figures

- `[Figure 2-1]` Shape of a call

---

## I. Introduction

### 1. Purpose

This document specifies the callable surface of `witchhat`: every parameter, every
returned value, and the semantics a caller must understand to use the results correctly.
It does not explain the internal design, which is `architecture.md`'s subject.

### 2. Which surface to use

The Python surface (Chapter II) is the entry point for working with a single Arrow
batch already in hand. The Databricks/Spark surface (Chapter III,
`witchhat.spark`) is the entry point for calling witchhat against a
`pyspark.sql.DataFrame` in a Databricks notebook or job; it is built entirely on
Chapter II underneath. The Rust surface (Chapter IV) is for embedding witchhat
directly in a Rust binary or service with no Python in the loop; it is what
`witchhat-py` itself calls.

## II. Python Surface

### 1. Installation and import

```
pip install maturin
cd crates/witchhat-py
maturin build --release   # or `maturin develop --release` inside a venv
pip install ../../target/wheels/witchhat-*.whl
```

```python
import witchhat
```

### 2. `hash_rows`

```python
witchhat.hash_rows(batch, columns, version="v1") -> pyarrow.Array
```

Fingerprints `columns` of `batch`, in the given order, into one `uint64` per row.

<Table 2-1> Parameters of `hash_rows`

| Parameter | Type | Meaning |
|---|---|---|
| `batch` | Arrow-C-Data-exportable | The rows to hash. Any object implementing `__arrow_c_array__`: `pyarrow.RecordBatch`, a `polars` batch export, etc. |
| `columns` | `list[str]` | Column names, in the order to hash them |
| `version` | `str`, default `"v1"` | The hashing algorithm; see Chapter IV, Section 3 |

```
batch (pyarrow.RecordBatch) --+
columns (["id", "email"])   --+--> hash_rows --> pyarrow.Array[uint64], one per row
version ("v1")               -+
```

[Figure 2-1] Shape of a call

<Table 2-2> Exceptions raised by the Python surface

| Exception | Raised when |
|---|---|
| `ValueError` | `version` does not name a known algorithm, a preset `name` is unrecognised, a `how`/`func` string (`join`/`join_null_safe`/`aggregate`) is unrecognised, or `wspark.broadcast_join` was called with `how="right"`/`"full"` |
| `RuntimeError` | A name in `columns` is not in `batch`'s schema, a column's Arrow type has no defined hash/kernel, `join`/`join_null_safe`'s key types mismatch, or `aggregate`'s exact `"sum"`/`"mean"` accumulator overflows |

`validate_schema` and `normalize_json` are the exceptions to the `RuntimeError` row: a
schema mismatch and a malformed JSON row are both normal, representable results, not
failures. See their own sections below.

### 3. `hash_rows_all_columns`

```python
witchhat.hash_rows_all_columns(batch, version="v1") -> pyarrow.Array
```

`hash_rows` over every column of `batch`, in schema order. Same exceptions as
`hash_rows`.

### 4. `table_fingerprint`

```python
witchhat.table_fingerprint(row_hashes, version="v1") -> int
```

Folds an array of row hashes (such as `hash_rows`'s return value) into one
order-independent `uint64`. `row_hashes` is any Arrow-C-Data-exportable `uint64` array.
Raises `ValueError` for an unrecognised `version`.

### 5. `schema_fingerprint`

```python
witchhat.schema_fingerprint(schema, version="v1") -> int
```

Fingerprints `schema`'s shape (field names in order, types, nullability). `schema` is any
object implementing `__arrow_c_schema__`, such as `batch.schema` on a
`pyarrow.RecordBatch`. Raises `ValueError` for an unrecognised `version`.

### 6. `validate_schema` and `SchemaDiff`

```python
witchhat.validate_schema(actual, expected, allow_numeric_widening=False) -> witchhat.SchemaDiff
```

Compares `actual` against `expected`, column by column matched by name
(case-sensitive), and returns a `SchemaDiff` describing exactly how they differ. Never
raises for a mismatch; see `architecture.md` Chapter IV for the full comparison rules.

<Table 2-3> Fields and methods of `SchemaDiff`

| Member | Type | Meaning |
|---|---|---|
| `.missing` | `list[str]` | Columns in `expected`, absent from `actual` |
| `.unexpected` | `list[str]` | Columns in `actual`, absent from `expected` |
| `.retyped` | `list[tuple[str, pyarrow.DataType, pyarrow.DataType]]` | `(column, expected_type, actual_type)` |
| `.nullability` | `list[tuple[str, bool, bool]]` | `(column, expected_nullable, actual_nullable)` |
| `.is_empty()` | `bool` | True if `actual` and `expected` agreed on every point checked |
| `.is_breaking()` | `bool` | True if `missing`, `retyped`, or `nullability` is non-empty; `unexpected` alone does not count |

### 7. `normalize_json` and `NormalizeStats`

```python
witchhat.normalize_json(json, schema, version="v1") -> tuple[pyarrow.RecordBatch, witchhat.NormalizeStats]
```

Parses one JSON object per row of `json` into `schema`. A field name may be a
`.`-separated path (`"address.city"`) to read one level into a nested object. See
`architecture.md` Chapter V for the full malformed/mismatch/absent distinction.

<Table 2-4> Fields of `NormalizeStats`

| Field | Type | Meaning |
|---|---|---|
| `.rows_malformed` | `int` | Rows whose JSON text did not parse, or was not an object |
| `.type_mismatches` | `dict[str, int]` | `{column: count}` for rows whose value at that path had the wrong JSON type |

### 8. `clean_with_preset` and `clean_with_rules`

```python
witchhat.clean_with_preset(input, name, version="v1") -> pyarrow.Array
witchhat.clean_with_rules(input, rules) -> pyarrow.Array
```

`clean_with_preset` applies one of witchhat's own named, versioned rule sets.
`clean_with_rules` applies caller-supplied `(pattern, replacement)` regex rules
directly; these are the caller's own rules and are not versioned by witchhat (see
`architecture.md` Chapter VI, Section 2).

<Table 2-5> Built-in cleanup presets

| Preset | Effect |
|---|---|
| `trim_whitespace` | Removes leading and trailing whitespace |
| `collapse_whitespace` | Collapses any run of whitespace to a single space |
| `strip_control_characters` | Removes ASCII control characters |
| `strip_non_alphanumeric` | Removes everything except letters, digits and whitespace |
| `digits_only` | Removes everything except `0`-`9` |

Both raise `ValueError`: `clean_with_preset` for an unrecognised `name` or `version`,
`clean_with_rules` for a pattern that does not compile.

### 9. `check_equivalence` and `EquivalenceReport`

```python
witchhat.check_equivalence(actual, expected, columns=None, allow_numeric_widening=False, hash_version="v1") -> witchhat.EquivalenceReport
```

Compares `actual` against `expected`: same schema, same row count, same rows regardless
of order. See `architecture.md` Chapter VII for how the schema and row comparisons
combine, and Section 6 below for what `is_equivalent()` does and does not mean.

<Table 2-6> Fields and methods of `EquivalenceReport`

| Member | Type | Meaning |
|---|---|---|
| `.schema_diff` | `SchemaDiff` | The schema half of the comparison |
| `.row_count_actual`, `.row_count_expected` | `int` | Row counts of each side |
| `.table_fingerprint_actual`, `.table_fingerprint_expected` | `int` | Order-independent fingerprints over the compared columns |
| `.fingerprints_match` | `bool` | Whether the two table fingerprints matched |
| `.is_equivalent()` | `bool` | Empty schema diff, matching row counts, matching fingerprints |

Raises `RuntimeError` for an unknown column or unsupported column type, `ValueError` for
an unrecognised `hash_version`.

### 10. `drop_duplicates`

```python
witchhat.drop_duplicates(batch, columns, version="v1") -> pyarrow.RecordBatch
```

Keeps the first row of every distinct value of `columns`, dropping the rest, preserving
the relative order of the rows that remain. Equivalent to Spark's
`df.dropDuplicates(subset=columns)`. A hash collision between two distinct rows never
causes a false duplicate: rows sharing a hash are additionally compared exactly before
either is treated as a duplicate (`architecture.md` Chapter VIII). Same exceptions as
`hash_rows`.

### 11. `join`, `join_null_safe`

```python
witchhat.join(left, right, left_keys, right_keys, how="inner") -> pyarrow.RecordBatch
witchhat.join_null_safe(left, right, left_keys, right_keys, how="inner") -> pyarrow.RecordBatch
```

Joins `left` and `right` on `left_keys`/`right_keys`, matched pairwise by position.
`how` is `"inner"`, `"left"`, `"right"` or `"full"`. The output schema is every field of
`left` followed by every field of `right`, with a `right` name collision suffixed
`_right`, and every field nullable (an outer join can null either side). See
`architecture.md` Chapter IX for the full design, including why key comparison uses
`arrow_row` rather than `hash_batch`.

`join`'s null key semantics match Spark/SQL: a row with a null in any key column never
matches, including another row that is also null there (`NULL = NULL` is never true).
`join_null_safe` is the explicit opt-in for the opposite: a null key matches another null
key, column by column (`architecture.md` Chapter IX, Section 4). Reach for `join` unless
you have a specific, deliberate reason to want null-matches-null.

Raises `ValueError` for an unrecognised `how`; `RuntimeError` for an unknown column, or
`left_keys[i]`'s type not exactly matching `right_keys[i]`'s (see Section 7 below).

### 12. `aggregate`

```python
witchhat.aggregate(batch, group_by, aggregations) -> pyarrow.RecordBatch
```

Groups `batch` by `group_by` and reduces each group with `aggregations`, a list of
`(column, func, alias)` triples. `group_by` may be empty (whole-table aggregate). See
`architecture.md` Chapter X for grouping semantics and how `Sum`/`Mean`/`Min`/`Max`
accumulate and compare at each column's own exact numeric precision (`i128`/`u128`/
decimal mantissa), never through a universal `float64` downcast.

<Table 2-7> Aggregate functions accepted by `aggregate`

| `func` | Input types | Output | Null handling |
|---|---|---|---|
| `"count"` | any | `int64` | Counts non-null values |
| `"sum"` | numeric, `decimal128`/`decimal256` | `int64` (signed source), `uint64` (unsigned source), `float64` (float source), or the source's own decimal type | `None` if the group is all-null |
| `"mean"` / `"avg"` | numeric, `decimal128`/`decimal256` | `float64` (integer/float source) or the source's own decimal type | `None` if the group is all-null |
| `"min"` | numeric, `decimal128`/`decimal256` | matches input | `None` if the group is all-null |
| `"max"` | numeric, `decimal128`/`decimal256` | matches input | `None` if the group is all-null |

`"sum"`/`"mean"` raise `RuntimeError` if the exact accumulator cannot represent a group's
running total (an `Overflow` error on the Rust side), rather than silently losing
precision the way a `float64` accumulator would.

"numeric" here is `int8`..`int64`, `uint8`..`uint64`, `float32`, `float64` (see
Section 8 below). Raises `ValueError` for an unrecognised `func`; `RuntimeError` for an
unknown column, or a non-numeric column passed to anything but `"count"`.

### 13. `cpu_features` and `CpuFeatures`

```python
witchhat.cpu_features() -> witchhat.CpuFeatures
```

Returns a `CpuFeatures` instance with boolean properties `sse42`, `avx2`, `avx512f`,
`neon`. See `architecture.md` Chapter XI for why this exists and what it does (and does
not yet) affect.

## III. Databricks/Spark Surface (`witchhat.spark`)

Pure Python, calling straight into the functions above; not a second implementation.
See `architecture.md` Chapter XVI for the design (row-local vs. partition-coordinating
functions, the `uint64` problem, why `broadcast_join` is not a shuffle join).

### 1. Import and dependency

```python
from witchhat import spark as wspark
```

Every function lazily imports pyspark and raises `ImportError` with an install hint if
it is missing; `import witchhat` itself never requires pyspark.

<Table 3-1> `witchhat.spark` functions

| Function | Class | Signature |
|---|---|---|
| `to_arrow_schema` | schema helper | `(schema_or_df) -> pyarrow.Schema` |
| `map_in_arrow` | primitive | `(df, func, schema) -> DataFrame` |
| `hash_rows` | row-local | `(df, columns, output_column="row_hash", version="v1") -> DataFrame` |
| `clean_with_preset` | row-local | `(df, column, name, output_column=None, version="v1") -> DataFrame` |
| `clean_with_rules` | row-local | `(df, column, rules, output_column=None) -> DataFrame` |
| `drop_duplicates` | partition-coordinating | `(df, columns, version="v1", repartition=True) -> DataFrame` |
| `aggregate` | partition-coordinating | `(df, group_by, aggregations, repartition=True) -> DataFrame` |
| `collect_as_record_batch` | driver collection | `(df) -> pyarrow.RecordBatch` |
| `broadcast_join` | broadcast | `(df, small_table, left_keys, right_keys, how="inner") -> DataFrame` |
| `validate_schema` | schema helper | `(df, expected, allow_numeric_widening=False) -> SchemaDiff` |
| `schema_fingerprint` | schema helper | `(df, version="v1") -> int` |

### 2. `to_arrow_schema`, `map_in_arrow`

`to_arrow_schema(schema_or_df)` converts a `pyspark.sql.types.StructType` (or a
`DataFrame`, whose `.schema` is used) to a `pyarrow.Schema`, via pyspark's own
`pyspark.sql.pandas.types.to_arrow_schema`. `map_in_arrow(df, func, schema)` is a thin,
documented wrapper around `DataFrame.mapInArrow`: every other function below is built on
it, and it is the escape hatch for anything not already wrapped.

### 3. `hash_rows`

```python
wspark.hash_rows(df, columns, output_column="row_hash", version="v1") -> DataFrame
```

Adds `output_column` (Spark `LongType`), one witchhat row hash per row over `columns`.
Row-local, correct regardless of partitioning. The value is `witchhat.hash_rows`'s
`uint64` result bit-reinterpreted as `int64` (Spark/Arrow interop has no unsigned type):
equality, joins and group-bys on it behave identically to the `uint64`; `df.show()` can
print a negative number for a hash with its high bit set.

### 4. `clean_with_preset`, `clean_with_rules`

```python
wspark.clean_with_preset(df, column, name, output_column=None, version="v1") -> DataFrame
wspark.clean_with_rules(df, column, rules, output_column=None) -> DataFrame
```

`column` in place by default (`output_column=None`), or a new string column if given.
Row-local, thin wrappers around `witchhat.clean_with_preset`/`clean_with_rules`.

### 5. `drop_duplicates`

```python
wspark.drop_duplicates(df, columns, version="v1", repartition=True, max_distinct_keys=None) -> DataFrame
```

`witchhat.drop_duplicates`'s logic applied across a whole partition, streamed: each
incoming Arrow batch (`mapInArrow` can hand back more than one per partition) is hashed
and filtered against a running per-partition index that persists across batches but
never buffers a whole partition's raw rows into one `RecordBatch` first
(`architecture.md` Chapter XVI, Section 2). `repartition=True` (default) calls
`df.repartition(*columns)` first, which is what makes the result correct for the whole
`DataFrame`, not just within whatever partition Spark happened to place each row in.
`max_distinct_keys` (default `None`) raises `MemoryError` if a partition's distinct-key
count would exceed it, guarding against an unexpectedly skewed partition. See Section 9
below before passing `repartition=False`.

### 6. `aggregate`

```python
wspark.aggregate(df, group_by, aggregations, repartition=True) -> DataFrame
```

`witchhat.aggregate`'s logic applied across a whole partition, streamed: each batch is
reduced independently and only a per-group merge state (one partial reduction per
distinct group) persists across batches, not raw row data (`"mean"`/`"avg"` is
requested as `sum`+`count` per batch and divided once at the end, since the mean of
per-batch means is not the whole mean unless every batch is the same size).
`repartition=True` (default) calls `df.repartition(*group_by)` first. Unlike
`witchhat.aggregate`, `group_by` must be non-empty: a whole-table aggregate needs every
row in one partition, which hash-repartitioning cannot arrange, so this raises
`ValueError` instead of silently returning a partial answer.

### 7. `collect_as_record_batch`, `broadcast_join`

```python
wspark.collect_as_record_batch(df) -> pyarrow.RecordBatch
wspark.broadcast_join(df, small_table, left_keys, right_keys, how="inner") -> DataFrame
```

`collect_as_record_batch` pulls a small `DataFrame` to the driver as one `RecordBatch`
(`DataFrame.toArrow()` where available, `toPandas()` otherwise). `broadcast_join`
broadcasts `small_table` (already collected) to every partition and joins each
partition's batch against it with `witchhat.join`, mirroring Spark's own broadcast-join
optimization. Not a distributed shuffle join: use `DataFrame.join` directly for two
large sides.

`how` accepts only `"inner"`/`"left"`; `"right"`/`"full"` raise `ValueError` outright,
not merely a documented caveat, because `small_table` is broadcast independently to every
partition, so an unmatched `small_table` row under those two modes would surface once per
partition instead of once overall (`architecture.md` Chapter XVI, Section 5).

### 8. `validate_schema`, `schema_fingerprint`

```python
wspark.validate_schema(df, expected, allow_numeric_widening=False) -> witchhat.SchemaDiff
wspark.schema_fingerprint(df, version="v1") -> int
```

`witchhat.validate_schema`/`schema_fingerprint`, converting `df` (and `expected`, if it
is a `DataFrame` or `StructType` rather than an already-`pyarrow.Schema`) via
`to_arrow_schema` first.

## IV. Metric Logging (`witchhat.metrics`)

Optional JSON instrumentation for every function in Chapter II, applied automatically to
every `witchhat.spark` call too (Chapter III), since those call straight into the same,
instrumented functions. Disabled by default and free when disabled. See
`architecture.md` Chapter XVII for the design.

```python
witchhat.metrics.enable(sink="stdout") -> None
witchhat.metrics.disable() -> None
witchhat.metrics.is_enabled() -> bool
witchhat.metrics.row_count(obj) -> int | None
witchhat.metrics.measure(function, **context) -> contextmanager
```

### 1. `enable`, `disable`, `is_enabled`

`enable(sink="stdout")` turns on metric logging: every wrapped call in Chapter II (and
transitively Chapter III) emits one JSON event to `sink` on completion, success or
failure. `sink` is `"stdout"` (JSON lines, the default), a file path (`str`/`Path`,
lines appended), or a callable receiving each event as a `dict` (the extension point for
routing into a logger, a `pyspark.Accumulator`, or anything else). `disable()` turns it
back off; `is_enabled()` reports the current state.

### 2. Event shape

<Table 4-1> Fields common to every metric event

| Field | Type | Meaning |
|---|---|---|
| `function` | `str` | Which witchhat function was called |
| `ts` | `str` | ISO 8601 UTC timestamp |
| `status` | `str` | `"ok"` or `"error"` |
| `duration_ms` | `float` | Wall-clock time of the call |
| `error` | `str`, present only on failure | The exception's type and message, never its traceback |
| `rows_in`, `rows_out` | `int \| None`, where applicable | Row counts of the primary input/output |
| `rows_per_second` | `float`, present when `rows_in` is known | `rows_in` divided by `duration_ms` |

Every function also adds its own schema-level parameters: `hash_rows` adds `columns` and
`version`; `join` adds `left_keys`/`right_keys`/`how` and a second `rows_in_right`;
`aggregate` adds `group_by` and `aggregation_count`; and so on. No event, from any
function, ever includes a cell value or a row's contents.

### 3. `row_count`, `measure`

`row_count(obj)` is the same best-effort helper the wrapped functions use internally
(`.num_rows` for a `RecordBatch`-like value, `len()` for an `Array`-like value, `None`
otherwise); useful for building a custom sink that wants to compute its own derived
fields. `measure(function, **context)` is the context manager every wrapped function is
built on; call it directly to instrument your own code the same way, including calls
into functions this module does not already wrap.

## V. Rust Surface

### 1. Entry points

```rust
witchhat_core::hash_batch(batch: &RecordBatch, columns: &[&str], version: HashVersion) -> Result<UInt64Array>
witchhat_core::hash_batch_all_columns(batch: &RecordBatch, version: HashVersion) -> Result<UInt64Array>
witchhat_core::table_fingerprint(row_hashes: &UInt64Array, version: HashVersion) -> u64
witchhat_core::schema_fingerprint(schema: &Schema, version: HashVersion) -> u64
witchhat_core::validate_schema(actual: &Schema, expected: &Schema, options: ValidateSchemaOptions) -> SchemaDiff
witchhat_core::normalize_json(json: &StringArray, schema: &Schema, version: NormalizeVersion) -> Result<(RecordBatch, NormalizeStats)>
witchhat_core::apply_rules(input: &StringArray, rules: &[CleanRule]) -> StringArray
witchhat_core::clean_with_preset(input: &StringArray, name: &str, version: CleanupVersion) -> Result<StringArray>
witchhat_core::check_equivalence(actual: &RecordBatch, expected: &RecordBatch, options: EquivalenceOptions) -> Result<EquivalenceReport>
witchhat_core::drop_duplicates(batch: &RecordBatch, columns: &[&str], version: HashVersion) -> Result<RecordBatch>
witchhat_core::join(left: &RecordBatch, right: &RecordBatch, left_keys: &[&str], right_keys: &[&str], how: JoinType) -> Result<RecordBatch>
witchhat_core::join_null_safe(left: &RecordBatch, right: &RecordBatch, left_keys: &[&str], right_keys: &[&str], how: JoinType) -> Result<RecordBatch>
witchhat_core::aggregate(batch: &RecordBatch, group_by: &[&str], aggregations: &[Aggregation]) -> Result<RecordBatch>
witchhat_core::features() -> CpuFeatures
```

All synchronous, none perform I/O; see `architecture.md` Chapter XII for the concurrency
model. Full rustdoc, including examples, is on every item (`cargo doc --no-deps -p
witchhat-core`).

### 2. Version enums

```rust
pub enum HashVersion { V1 }          // hash_batch, table_fingerprint, schema_fingerprint,
                                      // check_equivalence, drop_duplicates, join, aggregate*
pub enum NormalizeVersion { V1 }     // normalize_json
pub enum CleanupVersion { V1 }       // preset, clean_with_preset
```

Each has `CURRENT`, `as_str(self) -> &'static str`, `parse(s: &str) -> Option<Self>`, and
implements `Default` (returning `CURRENT`). `JoinType` and `AggFunc` are plain mode
enums, not versioned algorithms (`architecture.md` Chapter III, Section 2 explains why);
each still has a `parse(s: &str) -> Option<Self>` for the Python boundary's string
arguments. *`aggregate` and `join` do not take a `HashVersion` themselves; grouping and
key matching use `arrow_row`, not `hash_batch` (`architecture.md` Chapter IX, Section 2).

### 3. Module map

<Table 5-1> Public Rust modules

| Module | Contents |
|---|---|
| `witchhat_core::schema` | Re-exported Arrow schema types, `schema_fingerprint` |
| `witchhat_core::hash` | `HashVersion`, `hash_batch`, `hash_batch_all_columns`, `table_fingerprint` |
| `witchhat_core::validate` | `ValidateSchemaOptions`, `SchemaDiff`, `RetypedColumn`, `NullabilityChange`, `validate_schema` |
| `witchhat_core::json` | `NormalizeVersion`, `NormalizeStats`, `normalize_json` |
| `witchhat_core::clean` | `CleanRule`, `CleanupVersion`, `apply_rules`, `preset`, `clean_with_preset` |
| `witchhat_core::equivalence` | `EquivalenceOptions`, `EquivalenceReport`, `check_equivalence` |
| `witchhat_core::dedup` | `drop_duplicates` |
| `witchhat_core::join` | `JoinType`, `join`, `join_null_safe` |
| `witchhat_core::aggregate` | `AggFunc`, `Aggregation`, `aggregate` |
| `witchhat_core::cpu` | `CpuFeatures`, `features` |
| `witchhat_core::error` | `Error`, `Result` |

### 4. Error type

`witchhat_core::Error` (`thiserror`-derived, `Clone`, `'static`): `UnknownColumn`,
`TypeMismatch`, `SchemaMismatch`, `UnsupportedType`, `Config`, `Overflow`. See
`architecture.md` Chapter XIII. `validate_schema` does not return `Result`: an unequal
schema is a normal result, not an `Error`. `normalize_json` returns `Result` only for a
structural problem (an unsupported target type in `schema`), never for a malformed row,
which is counted in `NormalizeStats` instead. `Overflow` is returned only by
`aggregate`'s `"sum"`/`"mean"`, when the exact accumulator cannot represent a group's
running total (`architecture.md` Chapter X, Section 3).

## VI. Semantics Callers Must Know

### 1. Column order

`hash_rows(batch, ["a", "b"])` and `hash_rows(batch, ["b", "a"])` produce different
fingerprints for the same rows. Pick one order and use it consistently for anything
comparing fingerprints across calls. `check_equivalence` is the one function that
compensates for this automatically across its two inputs; see Section 6.

### 2. Null and float equality

A `null` cell never hashes equal to a non-null value in its column, including an empty
string or a numeric zero. `-0.0` and `0.0` hash equal; every `NaN` bit pattern hashes
equal to every other. See `architecture.md` Chapter III, Section 4.

### 3. Version pinning

Pass `version` explicitly (rather than relying on the `"v1"` default) anywhere a result
is stored and compared against a value computed by a different call site or a later
release. This applies to `hash_rows`/`table_fingerprint`/`schema_fingerprint`/
`check_equivalence`/`drop_duplicates` (`HashVersion`), `normalize_json`
(`NormalizeVersion`), and `clean_with_preset` (`CleanupVersion`). `validate_schema`,
`clean_with_rules`, `join` and `aggregate` have no version parameter: the first two are
a structural check and the caller's own rules respectively, and the latter two use
`arrow_row`'s exact comparison rather than a witchhat-defined algorithm.

### 4. What accepts non-pyarrow objects

Every Arrow-shaped parameter accepts anything implementing the relevant Arrow C Data
method (`__arrow_c_array__` or `__arrow_c_schema__`), not `pyarrow` specifically: a
`polars.DataFrame`'s `to_arrow()` result, for instance, satisfies this. witchhat never
imports `pyarrow` itself; array/batch return values and `SchemaDiff.retyped`'s type
entries happen to be constructed as real `pyarrow` objects because that is what
`arrow-rs`'s Python bridge produces on the export side.

### 5. `validate_schema` argument order

`validate_schema(actual, expected, ...)`: `actual` first, `expected` second. Swapping the
two still produces a structurally meaningful diff without error, but `.missing`/
`.unexpected` and the nullability-tightening direction flip meaning, so a swapped call
can silently validate the wrong thing.

### 6. `check_equivalence` is stricter than "not breaking"

`EquivalenceReport.is_equivalent()` requires the schema diff to be entirely empty, not
just non-breaking: an `actual` with one extra column fails `is_equivalent()` even though
that same diff would pass `SchemaDiff.is_breaking() == False` on its own. Use
`validate_schema` directly if "no breaking change" rather than "identical" is the
question being asked.

### 7. `join` requires exact key-type matches

`left_keys[i]`'s Arrow type must exactly equal `right_keys[i]`'s; there is no implicit
coercion (`int32` joined against `int64` raises `RuntimeError`, even though
`validate_schema` would accept that pairing as a widening under
`allow_numeric_widening=True`). Cast one side to match the other first if the types
should be treated as comparable.

### 8. `aggregate` is numeric-only beyond `Count`

`"sum"`/`"mean"`/`"min"`/`"max"` only accept `int8`..`int64`, `uint8`..`uint64`,
`float32`, `float64`, `decimal128`, `decimal256` columns; a `Date`/`Time`/`Timestamp`/
`Utf8` column raises `RuntimeError` for any of those four (`"count"` accepts any column
type). All four accumulate/compare at the source column's own exact precision
(`i128`/`u128`/decimal mantissa), not through `f64`, so an `int64`/`uint64` value or sum
beyond `f64`'s exact-integer range (±2^53) stays exact; `"sum"`/`"mean"` raise
`RuntimeError` (an `Overflow` error on the Rust side) if the exact accumulator itself
cannot represent the running total, rather than silently losing precision.

### 9. `witchhat.spark`'s `repartition=True` default

`drop_duplicates` and `aggregate` in `witchhat.spark` default to `repartition=True`
because they are only correct for a whole `DataFrame` when Spark has already put every
related row in one partition; `mapInArrow` gives them no visibility across partitions.
Pass `repartition=False` only when `df` is already known to be partitioned by the same
columns (for instance, right after another call in this module repartitioned by them),
to skip a redundant shuffle. Do not pass it as a general "make this faster" switch: doing
so on an arbitrarily partitioned `DataFrame` silently produces a partial, wrong answer
rather than an error.

## VII. Worked Examples

### 1. Deduplicating rows

```python
import pyarrow as pa
import witchhat

batch = pa.record_batch({
    "id": pa.array([1, 2, 3, 2]),
    "email": pa.array(["a@x.com", "b@x.com", "c@x.com", "b@x.com"]),
})
deduped = witchhat.drop_duplicates(batch, ["id", "email"])
# deduped has 3 rows: the second (id=2, email="b@x.com") occurrence is dropped
```

### 2. Checking output against Spark

```python
report = witchhat.check_equivalence(witchhat_batch, spark_result_as_arrow_batch)
assert report.is_equivalent(), report
```

Works even if the two sides produced their rows, or declared their columns, in a
different order.

### 3. Validating a batch before processing it

```python
expected_schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("email", pa.string(), nullable=True),
])

diff = witchhat.validate_schema(incoming_batch.schema, expected_schema)
if diff.is_breaking():
    raise ValueError(f"incoming batch does not match expected schema: {diff!r}")
```

### 4. Normalizing JSON, then cleaning a column

```python
json_col = pa.array(['{"id": 1, "note": "  hi   there  "}'])
target_schema = pa.schema([pa.field("id", pa.int64()), pa.field("note", pa.string())])

batch, stats = witchhat.normalize_json(json_col, target_schema)
if stats.rows_malformed:
    logging.warning("dropped %d malformed rows", stats.rows_malformed)

cleaned_note = witchhat.clean_with_preset(batch.column("note"), "collapse_whitespace")
```

### 5. Joining and aggregating

```python
users = pa.record_batch({
    "id": pa.array([1, 2, 3]),
    "country": pa.array(["NO", "SE", "NO"]),
})
orders = pa.record_batch({
    "user_id": pa.array([1, 1, 2]),
    "amount": pa.array([10, 5, 20]),
})

joined = witchhat.join(users, orders, ["id"], ["user_id"], how="inner")
totals = witchhat.aggregate(joined, ["country"], [("amount", "sum", "total")])
# totals: NO -> 15.0, SE -> 20.0
```

### 6. A Databricks notebook pipeline with `witchhat.spark`

```python
from witchhat import spark as wspark

events = spark.table("bronze.events")  # a large Spark DataFrame

# row-local: safe on the DataFrame exactly as partitioned
tagged = wspark.hash_rows(events, ["user_id", "event_time"], output_column="event_hash")
cleaned = wspark.clean_with_preset(tagged, "note", "collapse_whitespace")

# partition-coordinating: repartition=True (the default) makes this correct for the
# whole DataFrame, not just within whichever partition a row happened to land in
deduped = wspark.drop_duplicates(cleaned, ["user_id", "event_hash"])

# broadcast a small dimension table and join it in, instead of a large-large shuffle join
countries = wspark.collect_as_record_batch(spark.table("dim.countries"))
enriched = wspark.broadcast_join(deduped, countries, ["user_id"], ["user_id"], how="left")

totals = wspark.aggregate(enriched, ["country"], [("amount", "sum", "total")])
totals.write.saveAsTable("silver.totals_by_country")
```

### 7. Measuring latency and throughput

```python
witchhat.metrics.enable()  # JSON lines to stdout

witchhat.hash_rows(batch, ["id", "email"])
# {"columns":["id","email"],"version":"v1","function":"hash_rows","ts":"...",
#  "status":"ok","rows_in":4,"rows_out":4,"duration_ms":0.15,"rows_per_second":26666.7}

witchhat.metrics.disable()
```

Route events elsewhere instead of stdout by passing a sink:

```python
events = []
witchhat.metrics.enable(sink=events.append)  # or sink="/path/to/log.jsonl"
```

### 8. Calling from Rust

```rust
use witchhat_core::{HashVersion, hash_batch};

let hashes = hash_batch(&batch, &["id", "email"], HashVersion::CURRENT)?;
```

## References

- `architecture.md`, this repository, for the design behind this surface.
- Arrow C Data Interface: <https://arrow.apache.org/docs/format/CDataInterface.html>

## Appendix A. Parameter quick reference

<Table A-1> Parameter quick reference

| Function | Required | Optional |
|---|---|---|
| `hash_rows` | `batch`, `columns` | `version="v1"` |
| `hash_rows_all_columns` | `batch` | `version="v1"` |
| `table_fingerprint` | `row_hashes` | `version="v1"` |
| `schema_fingerprint` | `schema` | `version="v1"` |
| `validate_schema` | `actual`, `expected` | `allow_numeric_widening=False` |
| `normalize_json` | `json`, `schema` | `version="v1"` |
| `clean_with_preset` | `input`, `name` | `version="v1"` |
| `clean_with_rules` | `input`, `rules` | (none) |
| `check_equivalence` | `actual`, `expected` | `columns=None`, `allow_numeric_widening=False`, `hash_version="v1"` |
| `drop_duplicates` | `batch`, `columns` | `version="v1"` |
| `join` | `left`, `right`, `left_keys`, `right_keys` | `how="inner"` |
| `aggregate` | `batch`, `group_by`, `aggregations` | (none) |
| `cpu_features` | (none) | (none) |
