//! Canonical schema types.
//!
//! witchhat does not define its own schema representation: `arrow_schema`
//! already is the lingua franca between Spark, Databricks, pyarrow, polars
//! and pandas, so reusing it is what makes zero-copy interop possible.
pub use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};

use xxhash_rust::xxh3::Xxh3;

use crate::hash::HashVersion;

/// A hash of a schema's shape: field names in order, their types and
/// nullability. Versioned like row hashes ([`HashVersion`]) so a fingerprint
/// stored today stays reproducible even if this function's internals change.
pub fn schema_fingerprint(schema: &Schema, version: HashVersion) -> u64 {
    let mut hasher = Xxh3::with_seed(version.seed());
    for field in schema.fields() {
        hasher.update(field.name().as_bytes());
        hasher.update(field.data_type().to_string().as_bytes());
        hasher.update(&[field.is_nullable() as u8]);
    }
    hasher.digest()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema(fields: Vec<(&str, DataType, bool)>) -> Schema {
        Schema::new(
            fields
                .into_iter()
                .map(|(name, ty, nullable)| Field::new(name, ty, nullable))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn same_shape_same_fingerprint() {
        let a = schema(vec![("id", DataType::Int64, false)]);
        let b = schema(vec![("id", DataType::Int64, false)]);
        assert_eq!(
            schema_fingerprint(&a, HashVersion::CURRENT),
            schema_fingerprint(&b, HashVersion::CURRENT)
        );
    }

    #[test]
    fn field_order_changes_fingerprint() {
        let a = schema(vec![
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
        ]);
        let b = schema(vec![
            ("name", DataType::Utf8, true),
            ("id", DataType::Int64, false),
        ]);
        assert_ne!(
            schema_fingerprint(&a, HashVersion::CURRENT),
            schema_fingerprint(&b, HashVersion::CURRENT)
        );
    }

    #[test]
    fn nullability_changes_fingerprint() {
        let a = schema(vec![("id", DataType::Int64, false)]);
        let b = schema(vec![("id", DataType::Int64, true)]);
        assert_ne!(
            schema_fingerprint(&a, HashVersion::CURRENT),
            schema_fingerprint(&b, HashVersion::CURRENT)
        );
    }
}
