//! Python bindings.
//!
//! A thin translation layer over [`witchhat_core`]: every function here accepts and
//! returns Arrow data via the Arrow C Data / pyarrow interface
//! ([`arrow::pyarrow::PyArrowType`]), converts a string version name to [`HashVersion`],
//! and converts a [`witchhat_core::Error`] to a Python exception. `python/witchhat/__init__.py`
//! re-exports everything registered here, so callers import from `witchhat`, not
//! `witchhat._witchhat`.
//!
//! Everything here executes on the thread that called into it: no GIL release, no
//! background threads, since every operation is CPU-bound and short relative to a Python
//! call's own overhead. Revisit if a future kernel is expensive enough to be worth
//! releasing the GIL for during the call.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::pyarrow::PyArrowType;
use arrow_array::{Array, RecordBatch, StringArray, UInt64Array};
use arrow_data::ArrayData;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use witchhat_core::{
    CleanupVersion, DataType, HashVersion, NormalizeVersion, ValidateSchemaOptions,
};

fn parse_version(version: &str) -> PyResult<HashVersion> {
    HashVersion::parse(version)
        .ok_or_else(|| PyValueError::new_err(format!("unknown hash version {version:?}")))
}

fn parse_normalize_version(version: &str) -> PyResult<NormalizeVersion> {
    NormalizeVersion::parse(version)
        .ok_or_else(|| PyValueError::new_err(format!("unknown normalize version {version:?}")))
}

fn parse_cleanup_version(version: &str) -> PyResult<CleanupVersion> {
    CleanupVersion::parse(version)
        .ok_or_else(|| PyValueError::new_err(format!("unknown cleanup version {version:?}")))
}

// arrow's pyarrow bridge only implements ToPyArrow/FromPyArrow for ArrayData, not for
// typed arrays like UInt64Array or StringArray, so the typed <-> untyped conversion
// happens at this boundary rather than in witchhat-core, which never depends on pyo3
// at all.
fn u64_array_to_pyarrow(array: UInt64Array) -> PyArrowType<ArrayData> {
    PyArrowType(array.into_data())
}

fn u64_array_from_pyarrow(array: PyArrowType<ArrayData>) -> UInt64Array {
    UInt64Array::from(array.0)
}

fn string_array_to_pyarrow(array: StringArray) -> PyArrowType<ArrayData> {
    PyArrowType(array.into_data())
}

fn string_array_from_pyarrow(array: PyArrowType<ArrayData>) -> StringArray {
    StringArray::from(array.0)
}

/// Fingerprint `columns` of `batch`, in the given order, into one uint64 per row.
///
/// `batch` is any object implementing the Arrow C Data / pyarrow interface: a
/// `pyarrow.RecordBatch`, a `polars` batch export, or anything else exposing
/// `__arrow_c_array__`. `version` names the exact hashing algorithm to use (see
/// `witchhat.HashVersion` in the type stub); the default, `"v1"`, is the only version
/// implemented so far. Raises `ValueError` for an unrecognised `version`, and
/// `RuntimeError` for a column name not present in `batch`'s schema or a column whose
/// Arrow type has no defined hash (see the Rust `hash_batch` docs for the supported list).
#[pyfunction]
#[pyo3(signature = (batch, columns, version = "v1"))]
fn hash_rows(
    batch: PyArrowType<RecordBatch>,
    columns: Vec<String>,
    version: &str,
) -> PyResult<PyArrowType<ArrayData>> {
    let version = parse_version(version)?;
    let names: Vec<&str> = columns.iter().map(String::as_str).collect();
    let hashes = witchhat_core::hash_batch(&batch.0, &names, version)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok(u64_array_to_pyarrow(hashes))
}

/// `hash_rows` over every column of `batch`, in schema order. See `hash_rows` for the
/// meaning of `version` and the exceptions raised.
#[pyfunction]
#[pyo3(signature = (batch, version = "v1"))]
fn hash_rows_all_columns(
    batch: PyArrowType<RecordBatch>,
    version: &str,
) -> PyResult<PyArrowType<ArrayData>> {
    let version = parse_version(version)?;
    let hashes = witchhat_core::hash_batch_all_columns(&batch.0, version)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok(u64_array_to_pyarrow(hashes))
}

/// Order-independent fingerprint of a whole batch of row hashes, e.g. from `hash_rows`.
///
/// Two batches with the same rows in a different order produce the same table
/// fingerprint, so this is how to check witchhat's output against Spark's without
/// sorting either side first. Raises `ValueError` for an unrecognised `version`.
#[pyfunction]
#[pyo3(signature = (row_hashes, version = "v1"))]
fn table_fingerprint(row_hashes: PyArrowType<ArrayData>, version: &str) -> PyResult<u64> {
    let version = parse_version(version)?;
    Ok(witchhat_core::table_fingerprint(
        &u64_array_from_pyarrow(row_hashes),
        version,
    ))
}

/// Fingerprint `schema`'s shape: field names in order, their types, and their
/// nullability. Raises `ValueError` for an unrecognised `version`.
#[pyfunction]
#[pyo3(signature = (schema, version = "v1"))]
fn schema_fingerprint(schema: PyArrowType<witchhat_core::Schema>, version: &str) -> PyResult<u64> {
    let version = parse_version(version)?;
    Ok(witchhat_core::schema_fingerprint(&schema.0, version))
}

/// The result of comparing an actual schema against an expected one.
///
/// Empty (`is_empty()`) when the two agree. Not constructible directly; returned by
/// `validate_schema`.
#[pyclass(name = "SchemaDiff")]
#[derive(Clone)]
struct PySchemaDiff {
    /// Column names in `expected` that `actual` does not have.
    #[pyo3(get)]
    missing: Vec<String>,
    /// Column names in `actual` that `expected` does not have. Reported, but does not
    /// make `is_breaking()` true: an additive column does not usually invalidate code
    /// written against the narrower, expected schema.
    #[pyo3(get)]
    unexpected: Vec<String>,
    /// `(column, expected_type, actual_type)` for every column present in both schemas
    /// whose type differs and was not an accepted widening.
    ///
    /// Exposed via a hand-written `#[getter]` below rather than `#[pyo3(get)]`, since
    /// `PyArrowType` does not implement `Clone` and `#[pyo3(get)]` needs to clone a
    /// field to return it; the getter builds a fresh `PyArrowType` from the plain,
    /// `Clone`-able `DataType` stored here instead.
    retyped: Vec<(String, DataType, DataType)>,
    /// `(column, expected_nullable, actual_nullable)` for every column whose
    /// nullability tightened.
    #[pyo3(get)]
    nullability: Vec<(String, bool, bool)>,
}

#[pymethods]
impl PySchemaDiff {
    #[getter]
    fn retyped(&self) -> Vec<(String, PyArrowType<DataType>, PyArrowType<DataType>)> {
        self.retyped
            .iter()
            .map(|(column, expected, actual)| {
                (
                    column.clone(),
                    PyArrowType(expected.clone()),
                    PyArrowType(actual.clone()),
                )
            })
            .collect()
    }

    /// Whether `actual` and `expected` agreed on every point checked.
    fn is_empty(&self) -> bool {
        self.missing.is_empty()
            && self.unexpected.is_empty()
            && self.retyped.is_empty()
            && self.nullability.is_empty()
    }

    /// Whether the difference is one a caller most likely cannot safely ignore: a
    /// missing column, a retyped column, or a nullability tightening. An `unexpected`
    /// column alone does not count.
    fn is_breaking(&self) -> bool {
        !self.missing.is_empty() || !self.retyped.is_empty() || !self.nullability.is_empty()
    }

    fn __repr__(&self) -> String {
        format!(
            "SchemaDiff(missing={:?}, unexpected={:?}, retyped={} column(s), nullability={} column(s))",
            self.missing,
            self.unexpected,
            self.retyped.len(),
            self.nullability.len()
        )
    }
}

fn to_py_schema_diff(diff: witchhat_core::SchemaDiff) -> PySchemaDiff {
    PySchemaDiff {
        missing: diff.missing.iter().map(ToString::to_string).collect(),
        unexpected: diff.unexpected.iter().map(ToString::to_string).collect(),
        retyped: diff
            .retyped
            .into_iter()
            .map(|r| (r.column.to_string(), r.expected, r.actual))
            .collect(),
        nullability: diff
            .nullability
            .into_iter()
            .map(|n| (n.column.to_string(), n.expected_nullable, n.actual_nullable))
            .collect(),
    }
}

/// Compares `actual` against `expected` and returns their difference.
///
/// Columns are matched by name, case-sensitively. `allow_numeric_widening` (default
/// `False`) accepts `actual` having a wider numeric type than `expected` for the same
/// column (`int32` -> `int64`, `float32` -> `float64`, and so on within a signedness
/// class); a narrower type, a cross-signedness change, or an integer-to-float change is
/// never accepted regardless. See `witchhat.SchemaDiff` for the returned shape.
#[pyfunction]
#[pyo3(signature = (actual, expected, allow_numeric_widening = false))]
fn validate_schema(
    actual: PyArrowType<witchhat_core::Schema>,
    expected: PyArrowType<witchhat_core::Schema>,
    allow_numeric_widening: bool,
) -> PySchemaDiff {
    let options = ValidateSchemaOptions {
        allow_numeric_widening,
    };
    to_py_schema_diff(witchhat_core::validate_schema(
        &actual.0,
        &expected.0,
        options,
    ))
}

/// What happened while normalizing a batch, beyond the columns themselves.
///
/// Not constructible directly; returned by `normalize_json`.
#[pyclass(name = "NormalizeStats")]
struct PyNormalizeStats {
    /// Rows whose JSON text did not parse, or parsed to something other than a JSON
    /// object. Every target column is null for such a row.
    #[pyo3(get)]
    rows_malformed: u64,
    /// `{column: count}` for rows whose JSON value at that column's path had the
    /// wrong JSON type (so it was written as null). Does not count an absent path or
    /// an explicit JSON `null`: both are an ordinary, expected null.
    #[pyo3(get)]
    type_mismatches: HashMap<String, u64>,
}

#[pymethods]
impl PyNormalizeStats {
    fn __repr__(&self) -> String {
        format!(
            "NormalizeStats(rows_malformed={}, type_mismatches={:?})",
            self.rows_malformed, self.type_mismatches
        )
    }
}

/// Parses `json`, one JSON object per row, into `schema`.
///
/// A field's name is a `.`-separated path into the JSON object (`"address.city"`
/// reads `{"address": {"city": ...}}`). Supported target types are `utf8`, `int64`,
/// `float64` and `boolean`. Raises `RuntimeError` for a `schema` field of any other
/// type. See `witchhat.NormalizeStats` for what is reported alongside the batch.
#[pyfunction]
#[pyo3(signature = (json, schema, version = "v1"))]
fn normalize_json(
    json: PyArrowType<ArrayData>,
    schema: PyArrowType<witchhat_core::Schema>,
    version: &str,
) -> PyResult<(PyArrowType<RecordBatch>, PyNormalizeStats)> {
    let version = parse_normalize_version(version)?;
    let array = string_array_from_pyarrow(json);
    let (batch, stats) = witchhat_core::normalize_json(&array, &schema.0, version)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok((
        PyArrowType(batch),
        PyNormalizeStats {
            rows_malformed: stats.rows_malformed,
            type_mismatches: stats
                .type_mismatches
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        },
    ))
}

/// Applies a named, built-in cleanup preset to `input`. See `witchhat.CleanupVersion`
/// in the type stub for the preset names `version` defines. Raises `ValueError` for an
/// unrecognised `name` or `version`.
#[pyfunction]
#[pyo3(signature = (input, name, version = "v1"))]
fn clean_with_preset(
    input: PyArrowType<ArrayData>,
    name: &str,
    version: &str,
) -> PyResult<PyArrowType<ArrayData>> {
    let version = parse_cleanup_version(version)?;
    let array = string_array_from_pyarrow(input);
    let out = witchhat_core::clean_with_preset(&array, name, version)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok(string_array_to_pyarrow(out))
}

/// Applies caller-supplied regex find-and-replace rules to `input`, in order. Each
/// rule is `(pattern, replacement)`; `replacement` follows the `regex` crate's syntax
/// (`$1`, `${name}` for capture groups, `$$` for a literal `$`). Unlike
/// `clean_with_preset`, these rules are the caller's own and are not versioned by
/// witchhat. Raises `ValueError` if a pattern does not compile.
#[pyfunction]
#[pyo3(signature = (input, rules))]
fn clean_with_rules(
    input: PyArrowType<ArrayData>,
    rules: Vec<(String, String)>,
) -> PyResult<PyArrowType<ArrayData>> {
    let array = string_array_from_pyarrow(input);
    let compiled: witchhat_core::Result<Vec<witchhat_core::CleanRule>> = rules
        .into_iter()
        .map(|(pattern, replacement)| witchhat_core::CleanRule::new(&pattern, replacement))
        .collect();
    let compiled = compiled.map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok(string_array_to_pyarrow(witchhat_core::apply_rules(
        &array, &compiled,
    )))
}

/// The result of comparing an actual batch against an expected one.
///
/// Not constructible directly; returned by `check_equivalence`.
#[pyclass(name = "EquivalenceReport")]
struct PyEquivalenceReport {
    /// The schema half of the comparison.
    #[pyo3(get)]
    schema_diff: PySchemaDiff,
    /// `actual.num_rows`.
    #[pyo3(get)]
    row_count_actual: usize,
    /// `expected.num_rows`.
    #[pyo3(get)]
    row_count_expected: usize,
    /// Order-independent fingerprint of `actual`'s rows over the compared columns.
    #[pyo3(get)]
    table_fingerprint_actual: u64,
    /// Order-independent fingerprint of `expected`'s rows over the compared columns.
    #[pyo3(get)]
    table_fingerprint_expected: u64,
    /// Whether the two table fingerprints matched.
    #[pyo3(get)]
    fingerprints_match: bool,
}

#[pymethods]
impl PyEquivalenceReport {
    /// Whether `actual` and `expected` are equivalent: empty schema diff, matching row
    /// counts, and matching table fingerprints.
    fn is_equivalent(&self) -> bool {
        self.schema_diff.is_empty()
            && self.row_count_actual == self.row_count_expected
            && self.fingerprints_match
    }

    fn __repr__(&self) -> String {
        format!(
            "EquivalenceReport(is_equivalent={}, row_count_actual={}, row_count_expected={}, fingerprints_match={})",
            self.is_equivalent(),
            self.row_count_actual,
            self.row_count_expected,
            self.fingerprints_match
        )
    }
}

/// Compares `actual` against `expected`: same schema, same row count, same rows
/// regardless of order.
///
/// `columns` restricts (and orders) which columns are fingerprinted; `None` (the
/// default) uses every column `expected` and `actual` have in common, in `expected`'s
/// order, so a column declared in a different position on each side does not by itself
/// cause a mismatch. `allow_numeric_widening` is passed through to the schema
/// comparison. Raises `RuntimeError` for an unknown column or an unsupported column
/// type, and `ValueError` for an unrecognised `hash_version`.
#[pyfunction]
#[pyo3(signature = (actual, expected, columns = None, allow_numeric_widening = false, hash_version = "v1"))]
fn check_equivalence(
    actual: PyArrowType<RecordBatch>,
    expected: PyArrowType<RecordBatch>,
    columns: Option<Vec<String>>,
    allow_numeric_widening: bool,
    hash_version: &str,
) -> PyResult<PyEquivalenceReport> {
    let version = parse_version(hash_version)?;
    let options = witchhat_core::EquivalenceOptions {
        columns: columns.map(|cols| cols.into_iter().map(|c| Arc::from(c.as_str())).collect()),
        hash_version: version,
        schema_options: ValidateSchemaOptions {
            allow_numeric_widening,
        },
    };
    let report = witchhat_core::check_equivalence(&actual.0, &expected.0, options)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok(PyEquivalenceReport {
        schema_diff: to_py_schema_diff(report.schema_diff),
        row_count_actual: report.row_count_actual,
        row_count_expected: report.row_count_expected,
        table_fingerprint_actual: report.table_fingerprint_actual,
        table_fingerprint_expected: report.table_fingerprint_expected,
        fingerprints_match: report.fingerprints_match,
    })
}

/// Keeps the first row of every distinct value of `columns` in `batch`, dropping the
/// rest, preserving the relative order of the rows that remain. Equivalent to Spark's
/// `df.dropDuplicates(subset=columns)`. Raises `RuntimeError` for an unknown column or
/// an unsupported column type, `ValueError` for an unrecognised `version`.
#[pyfunction]
#[pyo3(signature = (batch, columns, version = "v1"))]
fn drop_duplicates(
    batch: PyArrowType<RecordBatch>,
    columns: Vec<String>,
    version: &str,
) -> PyResult<PyArrowType<RecordBatch>> {
    let version = parse_version(version)?;
    let names: Vec<&str> = columns.iter().map(String::as_str).collect();
    let deduped = witchhat_core::drop_duplicates(&batch.0, &names, version)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok(PyArrowType(deduped))
}

/// CPU features detected on the machine running this process.
///
/// Informational only: nothing in this release branches on it. It exists so a future
/// SIMD-accelerated kernel can be introspected, and so a caller can confirm what
/// dispatch a given machine would get.
#[pyclass(name = "CpuFeatures")]
struct PyCpuFeatures {
    /// Whether SSE4.2 is available, on x86_64.
    #[pyo3(get)]
    sse42: bool,
    /// Whether AVX2 is available, on x86_64.
    #[pyo3(get)]
    avx2: bool,
    /// Whether AVX-512 Foundation is available, on x86_64.
    #[pyo3(get)]
    avx512f: bool,
    /// Whether NEON is available, on aarch64.
    #[pyo3(get)]
    neon: bool,
}

#[pymethods]
impl PyCpuFeatures {
    fn __repr__(&self) -> String {
        format!(
            "CpuFeatures(sse42={}, avx2={}, avx512f={}, neon={})",
            self.sse42, self.avx2, self.avx512f, self.neon
        )
    }
}

/// Detect the CPU features of the machine running this process. Cached after the first
/// call within a process.
#[pyfunction]
fn cpu_features() -> PyCpuFeatures {
    let f = witchhat_core::features();
    PyCpuFeatures {
        sse42: f.sse42,
        avx2: f.avx2,
        avx512f: f.avx512f,
        neon: f.neon,
    }
}

/// Registers everything the extension module exposes.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyCpuFeatures>()?;
    module.add_class::<PySchemaDiff>()?;
    module.add_class::<PyNormalizeStats>()?;
    module.add_class::<PyEquivalenceReport>()?;
    module.add_function(wrap_pyfunction!(hash_rows, module)?)?;
    module.add_function(wrap_pyfunction!(hash_rows_all_columns, module)?)?;
    module.add_function(wrap_pyfunction!(table_fingerprint, module)?)?;
    module.add_function(wrap_pyfunction!(schema_fingerprint, module)?)?;
    module.add_function(wrap_pyfunction!(validate_schema, module)?)?;
    module.add_function(wrap_pyfunction!(normalize_json, module)?)?;
    module.add_function(wrap_pyfunction!(clean_with_preset, module)?)?;
    module.add_function(wrap_pyfunction!(clean_with_rules, module)?)?;
    module.add_function(wrap_pyfunction!(check_equivalence, module)?)?;
    module.add_function(wrap_pyfunction!(drop_duplicates, module)?)?;
    module.add_function(wrap_pyfunction!(cpu_features, module)?)?;
    Ok(())
}
