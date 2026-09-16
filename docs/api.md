# witchhat: API Reference

**Document type** Interface specification
**Status** Describes the surface as built: composite hashing, schema validation, and schema/CPU introspection.
**Audience** Anyone calling this library from Python or from Rust.
**Companion documents** `architecture.md` for why the design is shaped this way, `operations.md` for building and deploying it.
**Version** 1.1
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
  - 7. `cpu_features` and `CpuFeatures`
- III. Rust Surface
  - 1. Entry points
  - 2. `HashVersion`
  - 3. `ValidateSchemaOptions` and `SchemaDiff`
  - 4. Module map
  - 5. Error type
- IV. Semantics Callers Must Know
  - 1. Column order
  - 2. Null and float equality
  - 3. Version pinning
  - 4. What accepts non-pyarrow objects
  - 5. `validate_schema` argument order
- V. Worked Examples
  - 1. Deduplicating rows
  - 2. Checking output against Spark
  - 3. Validating a batch before processing it
  - 4. Calling from Rust
- References
- Appendix A. Parameter quick reference

### List of Tables

- `<Table 2-1>` Parameters of `hash_rows`
- `<Table 2-2>` Exceptions raised by the Python surface
- `<Table 2-3>` Fields and methods of `SchemaDiff`
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
| `ValueError` | `version` does not name a known algorithm |
| `RuntimeError` | A name in `columns` is not in `batch`'s schema, or a column's Arrow type has no defined hash |

`validate_schema` (Section 6) is the one exception: it never raises for a schema
mismatch, since a difference is a normal, representable result, not a failure. It still
raises `TypeError`/`ValueError` for a malformed argument, like any Python function.

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
`version` should match what `row_hashes` was computed with, though nothing enforces this:
by the time an array of plain integers reaches this function, its provenance is gone.
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
raises for a mismatch; see `architecture.md` Chapter IV for the full comparison rules
(numeric widening, nullability direction).

<Table 2-3> Fields and methods of `SchemaDiff`

| Member | Type | Meaning |
|---|---|---|
| `.missing` | `list[str]` | Columns in `expected`, absent from `actual` |
| `.unexpected` | `list[str]` | Columns in `actual`, absent from `expected` |
| `.retyped` | `list[tuple[str, pyarrow.DataType, pyarrow.DataType]]` | `(column, expected_type, actual_type)` |
| `.nullability` | `list[tuple[str, bool, bool]]` | `(column, expected_nullable, actual_nullable)` |
| `.is_empty()` | `bool` | True if `actual` and `expected` agreed on every point checked |
| `.is_breaking()` | `bool` | True if `missing`, `retyped`, or `nullability` is non-empty; `unexpected` alone does not count |

`.retyped`'s type entries are real `pyarrow.DataType` objects (`arrow-rs`'s pyarrow
bridge constructs them directly), not strings, so `pyarrow.types.is_integer(...)` and
similar predicates work on them without parsing.

### 7. `cpu_features` and `CpuFeatures`

```python
witchhat.cpu_features() -> witchhat.CpuFeatures
```

Returns a `CpuFeatures` instance with boolean properties `sse42`, `avx2`, `avx512f`,
`neon`. See `architecture.md` Chapter V for why this exists and what it does (and does
not yet) affect.

## III. Rust Surface

### 1. Entry points

```rust
witchhat_core::hash_batch(batch: &RecordBatch, columns: &[&str], version: HashVersion) -> Result<UInt64Array>
witchhat_core::hash_batch_all_columns(batch: &RecordBatch, version: HashVersion) -> Result<UInt64Array>
witchhat_core::table_fingerprint(row_hashes: &UInt64Array, version: HashVersion) -> u64
witchhat_core::schema_fingerprint(schema: &Schema, version: HashVersion) -> u64
witchhat_core::validate_schema(actual: &Schema, expected: &Schema, options: ValidateSchemaOptions) -> SchemaDiff
witchhat_core::features() -> CpuFeatures
```

All synchronous, none perform I/O; see `architecture.md` Chapter VI for the concurrency
model. Full rustdoc, including examples, is on every item (`cargo doc --no-deps -p
witchhat-core`).

### 2. `HashVersion`

```rust
pub enum HashVersion { V1 }
impl HashVersion {
    pub const CURRENT: HashVersion;
    pub fn as_str(self) -> &'static str;
    pub fn parse(s: &str) -> Option<Self>;
}
```

### 3. `ValidateSchemaOptions` and `SchemaDiff`

```rust
pub struct ValidateSchemaOptions { pub allow_numeric_widening: bool }

pub struct SchemaDiff {
    pub missing: Vec<Arc<str>>,
    pub unexpected: Vec<Arc<str>>,
    pub retyped: Vec<RetypedColumn>,
    pub nullability: Vec<NullabilityChange>,
}
impl SchemaDiff {
    pub fn is_empty(&self) -> bool;
    pub fn is_breaking(&self) -> bool;
}

pub struct RetypedColumn { pub column: Arc<str>, pub expected: DataType, pub actual: DataType }
pub struct NullabilityChange { pub column: Arc<str>, pub expected_nullable: bool, pub actual_nullable: bool }
```

### 4. Module map

<Table 3-1> Public Rust modules

| Module | Contents |
|---|---|
| `witchhat_core::schema` | Re-exported Arrow schema types, `schema_fingerprint` |
| `witchhat_core::hash` | `HashVersion`, `hash_batch`, `hash_batch_all_columns`, `table_fingerprint` |
| `witchhat_core::validate` | `ValidateSchemaOptions`, `SchemaDiff`, `RetypedColumn`, `NullabilityChange`, `validate_schema` |
| `witchhat_core::cpu` | `CpuFeatures`, `features` |
| `witchhat_core::error` | `Error`, `Result` |

### 5. Error type

`witchhat_core::Error` (`thiserror`-derived, `Clone`, `'static`): `UnknownColumn`,
`TypeMismatch`, `SchemaMismatch`, `UnsupportedType`, `Config`. See `architecture.md`
Chapter VII. `validate_schema` does not return `Result`: an unequal schema is a normal
result (a `SchemaDiff`), not an `Error`.

## IV. Semantics Callers Must Know

### 1. Column order

`hash_rows(batch, ["a", "b"])` and `hash_rows(batch, ["b", "a"])` produce different
fingerprints for the same rows. This is deliberate (`architecture.md` Chapter III,
Section 1): pick one order and use it consistently for anything comparing fingerprints
across calls.

### 2. Null and float equality

A `null` cell never hashes equal to a non-null value in its column, including an empty
string or a numeric zero. `-0.0` and `0.0` hash equal; every `NaN` bit pattern hashes
equal to every other. See `architecture.md` Chapter III, Section 4.

### 3. Version pinning

Pass `version` explicitly (rather than relying on the `"v1"` default) anywhere a
fingerprint is stored and compared against a value computed by a different call site or
a later release, so an accidental version mismatch is visible immediately rather than
producing a silent, wrong "not equal" result. `validate_schema` has no version parameter:
schema comparison is a structural check, not a hash, so there is nothing to version yet.

### 4. What accepts non-pyarrow objects

`batch`, `row_hashes` and `schema`/`actual`/`expected` parameters accept anything
implementing the relevant Arrow C Data method (`__arrow_c_array__` or
`__arrow_c_schema__`), not `pyarrow` specifically: a `polars.DataFrame`'s `to_arrow()`
result, for instance, satisfies this. witchhat never imports `pyarrow` itself;
`hash_rows`'s return value and `SchemaDiff.retyped`'s type entries happen to be
constructed as real `pyarrow` objects because that is what `arrow-rs`'s Python bridge
produces on the export side.

### 5. `validate_schema` argument order

`validate_schema(actual, expected, ...)`: `actual` first, `expected` second, matching
the reading "validate actual [against] expected". Swapping the two arguments still
produces a structurally meaningful diff (Rust and Python both accept it without error),
but `.missing`/`.unexpected` and the direction of `.nullability`'s tightening check flip
meaning, so a swapped call can silently validate the wrong thing rather than fail loudly.

## V. Worked Examples

### 1. Deduplicating rows

```python
import pyarrow as pa
import witchhat

batch = pa.record_batch({
    "id": pa.array([1, 2, 3, 2]),
    "email": pa.array(["a@x.com", "b@x.com", "c@x.com", "b@x.com"]),
})
row_hash = witchhat.hash_rows(batch, ["id", "email"])
# row_hash[1] == row_hash[3]: rows 1 and 3 are exact duplicates on (id, email)
```

### 2. Checking output against Spark

```python
witchhat_rows = witchhat.hash_rows_all_columns(witchhat_batch)
spark_rows = witchhat.hash_rows_all_columns(spark_result_as_arrow_batch)

assert witchhat.table_fingerprint(witchhat_rows) == witchhat.table_fingerprint(spark_rows)
```

Works even if the two sides produced their rows in a different order, since
`table_fingerprint` is order-independent (`architecture.md` Chapter III, Section 5).

### 3. Validating a batch before processing it

```python
expected_schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("email", pa.string(), nullable=True),
])

diff = witchhat.validate_schema(incoming_batch.schema, expected_schema)
if diff.is_breaking():
    raise ValueError(f"incoming batch does not match expected schema: {diff!r}")
if not diff.is_empty():
    logging.warning("additive schema change: %r", diff.unexpected)
```

### 4. Calling from Rust

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
| `cpu_features` | (none) | (none) |
