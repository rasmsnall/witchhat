//! JSON normalization: parsing a column of JSON strings into a fixed Arrow schema.
//!
//! The usual first step before anything else in a pipeline can run: raw JSON, one
//! object per row, becomes flat typed columns. Like [`crate::hash`], the parsing rules
//! are versioned ([`NormalizeVersion`]) so the same input JSON produces the same output
//! columns indefinitely, not just today.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Schema};
use serde_json::Value;

use crate::error::{Error, Result};

/// Names one exact, frozen set of JSON-parsing rules: how a value's JSON type maps to
/// a target Arrow type, how a dotted field name is walked, and what counts as
/// malformed. See [`HashVersion`](crate::HashVersion) for why this is versioned rather
/// than left to evolve freely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum NormalizeVersion {
    /// The first normalization ruleset. See [`normalize_json`] for what it does.
    #[default]
    V1,
}

impl NormalizeVersion {
    /// The version [`normalize_json`] uses when a caller does not pin one.
    pub const CURRENT: NormalizeVersion = NormalizeVersion::V1;

    /// The version's stable name.
    pub fn as_str(self) -> &'static str {
        match self {
            NormalizeVersion::V1 => "v1",
        }
    }

    /// Parses a version's stable name, as produced by [`NormalizeVersion::as_str`].
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "v1" => Some(NormalizeVersion::V1),
            _ => None,
        }
    }
}

/// What happened while normalizing a batch, beyond the columns themselves.
///
/// Follows the same "structural problems are counted, not hidden" convention as a
/// Spark-facing ingestion pipeline: a malformed row does not fail the whole batch (one
/// bad record in a million-row file should not lose the other 999,999), but it must be
/// visible, not silently absorbed as an ordinary null.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NormalizeStats {
    /// Rows whose JSON text did not parse, or parsed to something other than a JSON
    /// object. Every target column is null for such a row.
    pub rows_malformed: u64,
    /// Per target column, how many rows had a JSON value present at that path whose
    /// type did not match the column's declared Arrow type (so it was written as
    /// null). Does **not** count a path that was simply absent, or explicitly `null`
    /// in the JSON: both of those are an ordinary, expected null, not a problem.
    pub type_mismatches: HashMap<Arc<str>, u64>,
}

fn supported_target_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::Int64 | DataType::Float64 | DataType::Boolean
    )
}

/// Walks `root` along `path` (a `.`-separated sequence of object keys), returning the
/// leaf value, or `None` if any component is missing or the value at that point is not
/// an object.
fn walk<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = root;
    for component in path.split('.') {
        current = current.as_object()?.get(component)?;
    }
    Some(current)
}

enum Builder {
    Utf8(StringBuilder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
}

impl Builder {
    fn new(data_type: &DataType, capacity: usize) -> Self {
        match data_type {
            DataType::Utf8 => Builder::Utf8(StringBuilder::with_capacity(capacity, 0)),
            DataType::Int64 => Builder::Int64(Int64Builder::with_capacity(capacity)),
            DataType::Float64 => Builder::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Boolean => Builder::Boolean(BooleanBuilder::with_capacity(capacity)),
            other => unreachable!("unsupported target type {other:?} reached the builder"),
        }
    }

    fn append_null(&mut self) {
        match self {
            Builder::Utf8(b) => b.append_null(),
            Builder::Int64(b) => b.append_null(),
            Builder::Float64(b) => b.append_null(),
            Builder::Boolean(b) => b.append_null(),
        }
    }

    /// Appends `value`. Returns `false` (and appends null) if `value`'s JSON type does
    /// not match this column, `true` otherwise (including for an explicit JSON `null`,
    /// which is not a mismatch).
    fn append(&mut self, value: &Value) -> bool {
        if value.is_null() {
            self.append_null();
            return true;
        }
        match self {
            Builder::Utf8(b) => match value.as_str() {
                Some(s) => {
                    b.append_value(s);
                    true
                }
                None => {
                    b.append_null();
                    false
                }
            },
            Builder::Int64(b) => match value.as_i64() {
                Some(n) => {
                    b.append_value(n);
                    true
                }
                None => {
                    b.append_null();
                    false
                }
            },
            Builder::Float64(b) => match value.as_f64() {
                Some(n) => {
                    b.append_value(n);
                    true
                }
                None => {
                    b.append_null();
                    false
                }
            },
            Builder::Boolean(b) => match value.as_bool() {
                Some(v) => {
                    b.append_value(v);
                    true
                }
                None => {
                    b.append_null();
                    false
                }
            },
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Builder::Utf8(mut b) => Arc::new(b.finish()),
            Builder::Int64(mut b) => Arc::new(b.finish()),
            Builder::Float64(mut b) => Arc::new(b.finish()),
            Builder::Boolean(mut b) => Arc::new(b.finish()),
        }
    }
}

/// Parses `json`, one JSON object per row, into `schema`.
///
/// A field's name is a `.`-separated path into the JSON object (`"address.city"` reads
/// `{"address": {"city": ...}}`); a plain name is a top-level key. Supported target
/// types are `Utf8`, `Int64`, `Float64` and `Boolean`; any other type in `schema`
/// returns [`Error::UnsupportedType`] before any row is processed.
///
/// A row whose text fails to parse, or that parses to something other than a JSON
/// object, becomes null in every column and counts toward
/// [`NormalizeStats::rows_malformed`]. Within a row, a path that is absent, or whose
/// value is JSON `null`, becomes an ordinary null and is not counted. A path present
/// with a value of the wrong JSON type (a string where the column is `Int64`, say)
/// becomes null and counts in [`NormalizeStats::type_mismatches`] for that column: this
/// is the signal that the input's actual shape disagreed with `schema`, distinct from
/// a field that was simply never there.
///
/// Nested objects and arrays as a target value are not supported in this version: a
/// path whose value is an object or array is treated as a type mismatch, the same as
/// any other type disagreement, since no target type here can represent it. `version`
/// gates only the parsing rules just described, not the argument types; there is one
/// version so far.
///
/// # Errors
///
/// [`Error::UnsupportedType`] if a field in `schema` is not one of the four supported
/// types.
///
/// # Panics
///
/// Does not panic. Not async; runs on the calling thread in time linear in
/// `json.len() * schema.fields().len()`, with no I/O. Every row's JSON parse is
/// independent, so this is `Send`-safe to run in parallel across row ranges from the
/// caller's side, though nothing here does that itself.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use arrow_array::StringArray;
/// use arrow_schema::{DataType, Field, Schema};
/// use witchhat_core::normalize_json;
///
/// let json = StringArray::from(vec![
///     Some(r#"{"id": 1, "address": {"city": "Oslo"}}"#),
///     Some(r#"{"id": 2}"#),
///     Some("not json"),
/// ]);
/// let schema = Schema::new(vec![
///     Field::new("id", DataType::Int64, true),
///     Field::new("address.city", DataType::Utf8, true),
/// ]);
///
/// let (batch, stats) = normalize_json(&json, &schema, Default::default()).unwrap();
/// assert_eq!(batch.num_rows(), 3);
/// assert_eq!(stats.rows_malformed, 1); // "not json"
/// # let _ = Arc::new(batch);
/// ```
pub fn normalize_json(
    json: &StringArray,
    schema: &Schema,
    version: NormalizeVersion,
) -> Result<(RecordBatch, NormalizeStats)> {
    let _ = version; // one ruleset so far; the parameter exists for the versioning contract
    for field in schema.fields() {
        if !supported_target_type(field.data_type()) {
            return Err(Error::unsupported_type(field.data_type().clone()));
        }
    }

    let capacity = json.len();
    let mut builders: Vec<Builder> = schema
        .fields()
        .iter()
        .map(|f| Builder::new(f.data_type(), capacity))
        .collect();
    let mut stats = NormalizeStats::default();

    for row in 0..json.len() {
        let parsed = if json.is_null(row) {
            None
        } else {
            serde_json::from_str::<Value>(json.value(row))
                .ok()
                .filter(Value::is_object)
        };

        match parsed {
            None => {
                stats.rows_malformed += 1;
                for builder in &mut builders {
                    builder.append_null();
                }
            }
            Some(object) => {
                for (field, builder) in schema.fields().iter().zip(&mut builders) {
                    match walk(&object, field.name()) {
                        None => builder.append_null(),
                        Some(value) => {
                            if !builder.append(value) {
                                *stats
                                    .type_mismatches
                                    .entry(Arc::from(field.name().as_str()))
                                    .or_insert(0) += 1;
                            }
                        }
                    }
                }
            }
        }
    }

    let columns: Vec<ArrayRef> = builders.into_iter().map(Builder::finish).collect();
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)
        .map_err(|e| Error::schema_mismatch(e.to_string()))?;
    Ok((batch, stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("address.city", DataType::Utf8, true),
        ])
    }

    #[test]
    fn flat_and_nested_fields_are_extracted() {
        let json = StringArray::from(vec![Some(
            r#"{"id": 1, "name": "a", "active": true, "address": {"city": "Oslo"}}"#,
        )]);
        let (batch, stats) = normalize_json(&json, &schema(), NormalizeVersion::CURRENT).unwrap();
        assert_eq!(stats.rows_malformed, 0);
        assert!(stats.type_mismatches.is_empty());
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap()
                .value(0),
            1
        );
        assert_eq!(
            batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "Oslo"
        );
    }

    #[test]
    fn malformed_json_nulls_every_column_and_is_counted() {
        let json = StringArray::from(vec![Some("not json")]);
        let (batch, stats) = normalize_json(&json, &schema(), NormalizeVersion::CURRENT).unwrap();
        assert_eq!(stats.rows_malformed, 1);
        for i in 0..batch.num_columns() {
            assert!(batch.column(i).is_null(0), "column {i} should be null");
        }
    }

    #[test]
    fn non_object_top_level_is_malformed() {
        let json = StringArray::from(vec![
            Some("[1, 2, 3]"),
            Some("42"),
            Some(r#""just a string""#),
        ]);
        let (_, stats) = normalize_json(&json, &schema(), NormalizeVersion::CURRENT).unwrap();
        assert_eq!(stats.rows_malformed, 3);
    }

    #[test]
    fn absent_key_is_null_but_not_counted_as_mismatch() {
        let json = StringArray::from(vec![Some(r#"{"id": 1}"#)]);
        let (batch, stats) = normalize_json(&json, &schema(), NormalizeVersion::CURRENT).unwrap();
        assert!(batch.column(1).is_null(0));
        assert!(stats.type_mismatches.is_empty());
    }

    #[test]
    fn explicit_json_null_is_null_but_not_counted_as_mismatch() {
        let json = StringArray::from(vec![Some(r#"{"id": null, "name": "a"}"#)]);
        let (batch, stats) = normalize_json(&json, &schema(), NormalizeVersion::CURRENT).unwrap();
        assert!(batch.column(0).is_null(0));
        assert!(stats.type_mismatches.is_empty());
    }

    #[test]
    fn wrong_type_is_null_and_counted() {
        let json = StringArray::from(vec![Some(r#"{"id": "not a number"}"#)]);
        let (batch, stats) = normalize_json(&json, &schema(), NormalizeVersion::CURRENT).unwrap();
        assert!(batch.column(0).is_null(0));
        assert_eq!(stats.type_mismatches.get("id"), Some(&1));
    }

    #[test]
    fn null_input_row_is_malformed() {
        let json = StringArray::from(vec![None::<&str>]);
        let (_, stats) = normalize_json(&json, &schema(), NormalizeVersion::CURRENT).unwrap();
        assert_eq!(stats.rows_malformed, 1);
    }

    #[test]
    fn unsupported_target_type_errors_before_processing_rows() {
        let bad_schema = Schema::new(vec![Field::new("v", DataType::Date32, true)]);
        let json = StringArray::from(vec![Some("{}")]);
        assert!(normalize_json(&json, &bad_schema, NormalizeVersion::CURRENT).is_err());
    }
}
