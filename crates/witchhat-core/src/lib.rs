//! Native kernels for Spark/Databricks-style data transformations.
//!
//! `witchhat-core` has no Python dependency: it operates purely on
//! [`arrow_array`]/[`arrow_schema`] types, so it can be linked directly from Rust (a CLI,
//! a service) as well as from the `witchhat-py` PyO3 bindings. See [`hash`] for the one
//! transformation kernel implemented so far, and `docs/architecture.md` at the repository
//! root for the design this crate follows (versioning discipline, why Arrow is the data
//! model, why CPU-feature detection must never change output).
//!
//! # Modules
//!
//! - [`schema`]: re-exports of the canonical Arrow schema types, plus [`schema_fingerprint`].
//! - [`hash`]: versioned composite row and table hashing.
//! - [`validate`]: comparing an actual schema against an expected one.
//! - [`cpu`]: runtime CPU feature detection for future SIMD dispatch.
//! - [`error`]: the crate's error type.
//!
//! Every public function here is synchronous and allocation-bounded by its input size; none
//! spawn threads or perform I/O.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]

pub mod cpu;
pub mod error;
pub mod hash;
pub mod schema;
pub mod validate;

pub use cpu::{CpuFeatures, features};
pub use error::{Error, Result};
pub use hash::{HashVersion, hash_batch, hash_batch_all_columns, table_fingerprint};
pub use schema::{DataType, Field, Fields, Schema, SchemaRef, schema_fingerprint};
pub use validate::{
    NullabilityChange, RetypedColumn, SchemaDiff, ValidateSchemaOptions, validate_schema,
};
