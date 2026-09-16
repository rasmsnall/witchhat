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

use arrow::pyarrow::PyArrowType;
use arrow_array::{Array, RecordBatch, UInt64Array};
use arrow_data::ArrayData;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use witchhat_core::HashVersion;

fn parse_version(version: &str) -> PyResult<HashVersion> {
    HashVersion::parse(version)
        .ok_or_else(|| PyValueError::new_err(format!("unknown hash version {version:?}")))
}

// arrow's pyarrow bridge only implements ToPyArrow/FromPyArrow for ArrayData, not for
// typed arrays like UInt64Array, so the typed <-> untyped conversion happens at this
// boundary rather than in witchhat-core, which never depends on pyo3 at all.
fn u64_array_to_pyarrow(array: UInt64Array) -> PyArrowType<ArrayData> {
    PyArrowType(array.into_data())
}

fn u64_array_from_pyarrow(array: PyArrowType<ArrayData>) -> UInt64Array {
    UInt64Array::from(array.0)
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
    module.add_function(wrap_pyfunction!(hash_rows, module)?)?;
    module.add_function(wrap_pyfunction!(hash_rows_all_columns, module)?)?;
    module.add_function(wrap_pyfunction!(table_fingerprint, module)?)?;
    module.add_function(wrap_pyfunction!(schema_fingerprint, module)?)?;
    module.add_function(wrap_pyfunction!(cpu_features, module)?)?;
    Ok(())
}
