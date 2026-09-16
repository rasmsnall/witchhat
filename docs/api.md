# witchhat: API Reference

**Document type** Interface specification
**Status** Describes the surface as built: composite hashing, schema validation, JSON normalization, regex cleanup, output-equivalence testing, deduplication, join, aggregate, and schema/CPU introspection.
**Audience** Anyone calling this library from Python or from Rust.
**Companion documents** `architecture.md` for why the design is shaped this way, `operations.md` for building and deploying it.
**Version** 1.3
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
  - 11. `join`
  - 12. `aggregate`
  - 13. `cpu_features` and `CpuFeatures`
- III. Rust Surface
  - 1. Entry points
  - 2. Version enums
  - 3. Module map
  - 4. Error type
- IV. Semantics Callers Must Know
  - 1. Column order
  - 2. Null and float equality
  - 3. Version pinning
  - 4. What accepts non-pyarrow objects
  - 5. `validate_schema` argument order
  - 6. `check_equivalence` is stricter than "not breaking"
  - 7. `join` requires exact key-type matches
  - 8. `aggregate` is numeric-only beyond `Count`
- V. Worked Examples
  - 1. Deduplicating rows
  - 2. Checking output against Spark
  - 3. Validating a batch before processing it
  - 4. Normalizing JSON, then cleaning a column
  - 5. Joining and aggregating
  - 6. Calling from Rust
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
- `<Table 3-1>` Public Rust modules
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

The Python surface (Chapter II) is the intended entry point for a Databricks notebook or
job. The Rust surface (Chapter III) is for embedding witchhat directly in a Rust binary
or service with no Python in the loop; it is what `witchhat-py` itself calls.

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
| `ValueError` | `version` does not name a known algorithm, a preset `name` is unrecognised, or a `how`/`func` string (`join`/`aggregate`) is unrecognised |
| `RuntimeError` | A name in `columns` is not in `batch`'s schema, a column's Arrow type has no defined hash/kernel, or `join`'s key types mismatch |

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
`df.dropDuplicates(subset=columns)`. Same exceptions as `hash_rows`.

### 11. `join`

```python
witchhat.join(left, right, left_keys, right_keys, how="inner") -> pyarrow.RecordBatch
```

Joins `left` and `right` on `left_keys`/`right_keys`, matched pairwise by position.
`how` is `"inner"`, `"left"`, `"right"` or `"full"`. The output schema is every field of
`left` followed by every field of `right`, with a `right` name collision suffixed
`_right`, and every field nullable (an outer join can null either side). See
`architecture.md` Chapter IX for the full design, including why key comparison uses
`arrow_row` rather than `hash_batch`.

Raises `ValueError` for an unrecognised `how`; `RuntimeError` for an unknown column, or
`left_keys[i]`'s type not exactly matching `right_keys[i]`'s (see Section 7 below).

### 12. `aggregate`

```python
witchhat.aggregate(batch, group_by, aggregations) -> pyarrow.RecordBatch
```

Groups `batch` by `group_by` and reduces each group with `aggregations`, a list of
`(column, func, alias)` triples. `group_by` may be empty (whole-table aggregate). See
`architecture.md` Chapter X for grouping semantics and why `Min`/`Max` preserve the
source type while `Sum`/`Mean` accumulate through `f64`.

<Table 2-7> Aggregate functions accepted by `aggregate`

| `func` | Input types | Output | Null handling |
|---|---|---|---|
| `"count"` | any | `int64` | Counts non-null values |
| `"sum"` | numeric | `float64` | `None` if the group is all-null |
| `"mean"` / `"avg"` | numeric | `float64` | `None` if the group is all-null |
| `"min"` | numeric | matches input | `None` if the group is all-null |
| `"max"` | numeric | matches input | `None` if the group is all-null |

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

## III. Rust Surface

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

<Table 3-1> Public Rust modules

| Module | Contents |
|---|---|
| `witchhat_core::schema` | Re-exported Arrow schema types, `schema_fingerprint` |
| `witchhat_core::hash` | `HashVersion`, `hash_batch`, `hash_batch_all_columns`, `table_fingerprint` |
| `witchhat_core::validate` | `ValidateSchemaOptions`, `SchemaDiff`, `RetypedColumn`, `NullabilityChange`, `validate_schema` |
| `witchhat_core::json` | `NormalizeVersion`, `NormalizeStats`, `normalize_json` |
| `witchhat_core::clean` | `CleanRule`, `CleanupVersion`, `apply_rules`, `preset`, `clean_with_preset` |
| `witchhat_core::equivalence` | `EquivalenceOptions`, `EquivalenceReport`, `check_equivalence` |
| `witchhat_core::dedup` | `drop_duplicates` |
| `witchhat_core::join` | `JoinType`, `join` |
| `witchhat_core::aggregate` | `AggFunc`, `Aggregation`, `aggregate` |
| `witchhat_core::cpu` | `CpuFeatures`, `features` |
| `witchhat_core::error` | `Error`, `Result` |

### 4. Error type

`witchhat_core::Error` (`thiserror`-derived, `Clone`, `'static`): `UnknownColumn`,
`TypeMismatch`, `SchemaMismatch`, `UnsupportedType`, `Config`. See `architecture.md`
Chapter XIII. `validate_schema` does not return `Result`: an unequal schema is a normal
result, not an `Error`. `normalize_json` returns `Result` only for a structural problem
(an unsupported target type in `schema`), never for a malformed row, which is counted in
`NormalizeStats` instead.

## IV. Semantics Callers Must Know

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
`float32`, `float64` columns; a `Decimal`/`Date`/`Time`/`Timestamp`/`Utf8` column raises
`RuntimeError` for any of those four (`"count"` accepts any column type). `"sum"`/
`"mean"` accumulate through `f64`, so an `int64`/`uint64` column with values beyond
`f64`'s exact-integer range (±2^53) can lose precision in the result; `"min"`/`"max"`
are unaffected, since they return the original, untouched value from the source column.

## V. Worked Examples

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

### 6. Calling from Rust

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
