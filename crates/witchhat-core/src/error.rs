//! The crate's error type.
//!
//! One enum covers every failure mode across [`crate::schema`] and [`crate::hash`], since
//! both are small enough that splitting per-module would just add conversions. Every
//! variant carries enough context (column name, expected vs. actual type) to build a
//! useful message without the caller re-deriving it.

use std::sync::Arc;

/// Everything that can go wrong in this crate.
///
/// Cloneable and `'static`: no borrowed data, so a value can be stored, matched on
/// multiple times, or turned into a Python exception without a lifetime fight.
#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    /// A caller named a column that is not present in the batch's schema.
    #[error("column {0:?} not found in schema")]
    UnknownColumn(Arc<str>),

    /// A column exists but does not hold the type an operation required.
    #[error("column {column:?} has type {actual}, expected {expected}")]
    TypeMismatch {
        /// Name of the offending column.
        column: Arc<str>,
        /// The type the operation needed.
        expected: Arc<str>,
        /// The type the column actually has.
        actual: Arc<str>,
    },

    /// Two schemas that were expected to agree do not.
    #[error("schema mismatch: {0}")]
    SchemaMismatch(Arc<str>),

    /// An Arrow [`DataType`](arrow_schema::DataType) has no defined behaviour for the
    /// requested operation.
    #[error("unsupported arrow type for this operation: {0}")]
    UnsupportedType(Arc<str>),

    /// A caller supplied an invalid combination of arguments.
    #[error("invalid configuration: {0}")]
    Config(Arc<str>),
}

impl Error {
    /// Builds an [`Error::UnknownColumn`].
    pub fn unknown_column(name: impl Into<Arc<str>>) -> Self {
        Error::UnknownColumn(name.into())
    }

    /// Builds an [`Error::TypeMismatch`]. `expected` and `actual` are formatted with
    /// [`Display`](std::fmt::Display), so an Arrow [`DataType`](arrow_schema::DataType)
    /// can be passed directly.
    pub fn type_mismatch(
        column: impl Into<Arc<str>>,
        expected: impl std::fmt::Display,
        actual: impl std::fmt::Display,
    ) -> Self {
        Error::TypeMismatch {
            column: column.into(),
            expected: expected.to_string().into(),
            actual: actual.to_string().into(),
        }
    }

    /// Builds an [`Error::SchemaMismatch`].
    pub fn schema_mismatch(reason: impl std::fmt::Display) -> Self {
        Error::SchemaMismatch(reason.to_string().into())
    }

    /// Builds an [`Error::UnsupportedType`].
    pub fn unsupported_type(reason: impl std::fmt::Display) -> Self {
        Error::UnsupportedType(reason.to_string().into())
    }

    /// Builds an [`Error::Config`].
    pub fn config(reason: impl std::fmt::Display) -> Self {
        Error::Config(reason.to_string().into())
    }
}

/// This crate's `Result`, defaulting the error type to [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;
