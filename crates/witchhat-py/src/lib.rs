//! PyO3 bindings for [`witchhat_core`].
//!
//! This crate is a thin translation layer, not a second implementation: every kernel
//! lives in `witchhat-core` and operates on plain Arrow types with no Python dependency,
//! so it can be linked from Rust directly. [`python`] converts to and from the Arrow
//! C Data / pyarrow interface at the PyO3 boundary. See `python/witchhat/__init__.py` and
//! `python/witchhat/__init__.pyi` for the Python-facing surface and its type stubs; this
//! crate compiles to `witchhat._witchhat`, which that package wraps.

#![warn(rustdoc::broken_intra_doc_links)]

mod python;

use pyo3::prelude::*;

/// The compiled half of the `witchhat` package.
#[pymodule]
fn _witchhat(module: &Bound<'_, PyModule>) -> PyResult<()> {
    python::register(module)
}
