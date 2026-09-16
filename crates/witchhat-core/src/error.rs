use std::sync::Arc;

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    #[error("column {0:?} not found in schema")]
    UnknownColumn(Arc<str>),

    #[error("column {column:?} has type {actual}, expected {expected}")]
    TypeMismatch {
        column: Arc<str>,
        expected: Arc<str>,
        actual: Arc<str>,
    },

    #[error("schema mismatch: {0}")]
    SchemaMismatch(Arc<str>),

    #[error("unsupported arrow type for this operation: {0}")]
    UnsupportedType(Arc<str>),

    #[error("invalid configuration: {0}")]
    Config(Arc<str>),
}

impl Error {
    pub fn unknown_column(name: impl Into<Arc<str>>) -> Self {
        Error::UnknownColumn(name.into())
    }

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

    pub fn schema_mismatch(reason: impl std::fmt::Display) -> Self {
        Error::SchemaMismatch(reason.to_string().into())
    }

    pub fn unsupported_type(reason: impl std::fmt::Display) -> Self {
        Error::UnsupportedType(reason.to_string().into())
    }

    pub fn config(reason: impl std::fmt::Display) -> Self {
        Error::Config(reason.to_string().into())
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
