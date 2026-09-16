//! Native kernels for Spark/Databricks-style data transformations.
//!
//! `witchhat-core` has no Python dependency: it operates purely on
//! [`arrow_array`]/[`arrow_schema`] types, so it can be linked directly from Rust (a CLI,
//! a service) as well as from the `witchhat-py` PyO3 bindings. See `docs/architecture.md`
//! at the repository root for the design this crate follows (versioning discipline, why
//! Arrow is the data model, why CPU-feature detection must never change output).
//!
//! # Modules
//!
//! - [`schema`]: re-exports of the canonical Arrow schema types, plus [`schema_fingerprint`].
//! - [`hash`]: versioned composite row and table hashing.
//! - [`validate`]: comparing an actual schema against an expected one.
//! - [`json`]: parsing a column of JSON strings into a fixed Arrow schema.
//! - [`clean`]: regex-based find-and-replace cleanup, ad hoc or from a named preset.
//! - [`equivalence`]: checking a witchhat pipeline's output against a reference.
//! - [`dedup`]: deduplication, built on [`hash`].
//! - [`mod@join`]: matching rows of two batches on key columns, built on [`arrow_row`].
//! - [`mod@aggregate`]: grouping rows and reducing each group, also built on [`arrow_row`].
//! - [`cpu`]: runtime CPU feature detection for future SIMD dispatch.
//! - [`error`]: the crate's error type.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]

pub mod aggregate;
pub mod clean;
pub mod cpu;
pub mod dedup;
pub mod equivalence;
pub mod error;
pub mod hash;
pub mod join;
pub mod json;
pub mod schema;
pub mod validate;

pub use aggregate::{AggFunc, Aggregation, aggregate};
pub use clean::{CleanRule, CleanupVersion, apply_rules, clean_with_preset};
pub use cpu::{CpuFeatures, features};
pub use dedup::drop_duplicates;
pub use equivalence::{EquivalenceMode, EquivalenceOptions, EquivalenceReport, check_equivalence};
pub use error::{Error, Result};
pub use hash::{HashVersion, hash_batch, hash_batch_all_columns, table_fingerprint};
pub use join::{JoinType, join, join_null_safe};
pub use json::{NormalizeStats, NormalizeVersion, normalize_json};
pub use schema::{DataType, Field, Fields, Schema, SchemaRef, schema_fingerprint};
pub use validate::{
    NullabilityChange, RetypedColumn, SchemaDiff, ValidateSchemaOptions, validate_schema,
};
