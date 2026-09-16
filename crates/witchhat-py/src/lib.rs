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

// arrow's pyarrow bridge only implements ToPyArrow/FromPyArrow for
// ArrayData, not for typed arrays like UInt64Array, so the typed <-> untyped
// conversion happens at this boundary rather than in witchhat-core.
fn u64_array_to_pyarrow(array: UInt64Array) -> PyArrowType<ArrayData> {
    PyArrowType(array.into_data())
}

fn u64_array_from_pyarrow(array: PyArrowType<ArrayData>) -> UInt64Array {
    UInt64Array::from(array.0)
}

/// Fingerprint `columns` of `batch`, in the given order, into one uint64 per
/// row. `batch` is any object implementing the Arrow C Data / pyarrow
/// interface (a `pyarrow.RecordBatch`, a `polars` batch export, etc).
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

/// [`hash_rows`] over every column in the batch, in schema order.
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

/// Order-independent fingerprint of a whole batch of row hashes, e.g. from
/// [`hash_rows`]. Two batches with the same rows in a different order
/// produce the same table fingerprint.
#[pyfunction]
#[pyo3(signature = (row_hashes, version = "v1"))]
fn table_fingerprint(row_hashes: PyArrowType<ArrayData>, version: &str) -> PyResult<u64> {
    let version = parse_version(version)?;
    Ok(witchhat_core::table_fingerprint(
        &u64_array_from_pyarrow(row_hashes),
        version,
    ))
}

#[pyfunction]
#[pyo3(signature = (schema, version = "v1"))]
fn schema_fingerprint(schema: PyArrowType<witchhat_core::Schema>, version: &str) -> PyResult<u64> {
    let version = parse_version(version)?;
    Ok(witchhat_core::schema_fingerprint(&schema.0, version))
}

#[pyclass(name = "CpuFeatures")]
struct PyCpuFeatures {
    #[pyo3(get)]
    sse42: bool,
    #[pyo3(get)]
    avx2: bool,
    #[pyo3(get)]
    avx512f: bool,
    #[pyo3(get)]
    neon: bool,
}

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

#[pymodule]
fn witchhat(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCpuFeatures>()?;
    m.add_function(wrap_pyfunction!(hash_rows, m)?)?;
    m.add_function(wrap_pyfunction!(hash_rows_all_columns, m)?)?;
    m.add_function(wrap_pyfunction!(table_fingerprint, m)?)?;
    m.add_function(wrap_pyfunction!(schema_fingerprint, m)?)?;
    m.add_function(wrap_pyfunction!(cpu_features, m)?)?;
    Ok(())
}
