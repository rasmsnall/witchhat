//! Output equivalence testing: checking a witchhat pipeline's result against a
//! reference (typically Spark's), without requiring either side to be sorted.
//!
//! Built directly on [`crate::hash`] and [`crate::validate`] rather than adding a new
//! comparison algorithm: schema agreement is [`validate_schema`], row-set agreement is
//! [`table_fingerprint`] over a shared column order. See [`check_equivalence`].

use std::sync::Arc;

use arrow_array::RecordBatch;

use crate::error::Result;
use crate::hash::{HashVersion, hash_batch, table_fingerprint};
use crate::validate::{SchemaDiff, ValidateSchemaOptions, validate_schema};

/// Controls how [`check_equivalence`] compares two batches.
#[derive(Debug, Clone, Default)]
pub struct EquivalenceOptions {
    /// Which columns to fingerprint, in that order. `None` (the default) uses every
    /// field name in `expected`'s schema that `actual` also has, in `expected`'s
    /// field order: this way, two batches whose columns are declared in a different
    /// order but hold the same data still compare equal, since column order otherwise
    /// affects [`hash_batch`]'s output (see `docs/architecture.md` Chapter III,
    /// Section 1).
    pub columns: Option<Vec<Arc<str>>>,
    /// The row-hashing algorithm. Defaults to [`HashVersion::CURRENT`].
    pub hash_version: HashVersion,
    /// Passed through to [`validate_schema`] for the schema half of the comparison.
    pub schema_options: ValidateSchemaOptions,
}

/// The result of comparing `actual` against `expected`.
#[derive(Debug, Clone)]
pub struct EquivalenceReport {
    /// The schema comparison; see [`validate_schema`].
    pub schema_diff: SchemaDiff,
    /// `actual.num_rows()`.
    pub row_count_actual: usize,
    /// `expected.num_rows()`.
    pub row_count_expected: usize,
    /// Order-independent fingerprint of `actual`'s rows over the compared columns.
    pub table_fingerprint_actual: u64,
    /// Order-independent fingerprint of `expected`'s rows over the compared columns.
    pub table_fingerprint_expected: u64,
    /// Whether the two table fingerprints matched.
    pub fingerprints_match: bool,
}

impl EquivalenceReport {
    /// Whether `actual` and `expected` are equivalent: the schema diff is empty, row
    /// counts match, and the table fingerprints match.
    ///
    /// Deliberately stricter than [`SchemaDiff::is_breaking`]: an equivalence check
    /// has no notion of "additive change is fine", since the whole point is asking
    /// whether two outputs are the same, not whether one is a safe evolution of the
    /// other. An extra column in `actual` fails this even though it would not make
    /// `schema_diff.is_breaking()` true on its own.
    pub fn is_equivalent(&self) -> bool {
        self.schema_diff.is_empty()
            && self.row_count_actual == self.row_count_expected
            && self.fingerprints_match
    }
}

/// Compares `actual` against `expected`: same schema, same row count, same rows
/// regardless of order.
///
/// The two table fingerprints are computed over the same column list and order (see
/// [`EquivalenceOptions::columns`]), so a column that is merely declared in a
/// different position on each side does not by itself cause a mismatch. A column
/// whose *type* differs still does, because [`hash_batch`] hashes a value's Arrow type
/// tag along with its bytes (`docs/architecture.md` Chapter III, Section 3): an
/// `Int32` `5` and an `Int64` `5` are not the same fingerprint. Pre-cast one side if
/// that particular difference should not matter for a given comparison.
///
/// # Errors
///
/// Whatever [`hash_batch`] returns: [`crate::Error::UnknownColumn`] if
/// `options.columns` names something absent from `actual` or `expected`, and
/// [`crate::Error::UnsupportedType`] if a compared column's Arrow type has no defined
/// hash.
///
/// # Panics
///
/// Does not panic. Not async; runs on the calling thread in time linear in the two
/// batches' sizes, with no I/O.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use arrow_array::{Int64Array, RecordBatch};
/// use arrow_schema::{DataType, Field, Schema};
/// use witchhat_core::{EquivalenceOptions, check_equivalence};
///
/// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
/// let actual = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![2, 1]))]).unwrap();
/// let expected = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
///
/// let report = check_equivalence(&actual, &expected, EquivalenceOptions::default()).unwrap();
/// assert!(report.is_equivalent()); // same rows, different order
/// ```
pub fn check_equivalence(
    actual: &RecordBatch,
    expected: &RecordBatch,
    options: EquivalenceOptions,
) -> Result<EquivalenceReport> {
    let schema_diff = validate_schema(
        actual.schema().as_ref(),
        expected.schema().as_ref(),
        options.schema_options,
    );

    let owned_columns: Vec<Arc<str>>;
    let columns: Vec<&str> = match &options.columns {
        Some(cols) => cols.iter().map(|s| s.as_ref()).collect(),
        None => {
            owned_columns = expected
                .schema()
                .fields()
                .iter()
                .map(|f| Arc::from(f.name().as_str()))
                .filter(|name: &Arc<str>| actual.schema().index_of(name).is_ok())
                .collect();
            owned_columns.iter().map(|s| s.as_ref()).collect()
        }
    };

    let actual_hashes = hash_batch(actual, &columns, options.hash_version)?;
    let expected_hashes = hash_batch(expected, &columns, options.hash_version)?;

    let table_fingerprint_actual = table_fingerprint(&actual_hashes, options.hash_version);
    let table_fingerprint_expected = table_fingerprint(&expected_hashes, options.hash_version);

    Ok(EquivalenceReport {
        schema_diff,
        row_count_actual: actual.num_rows(),
        row_count_expected: expected.num_rows(),
        table_fingerprint_actual,
        table_fingerprint_expected,
        fingerprints_match: table_fingerprint_actual == table_fingerprint_expected,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn batch(schema: Arc<Schema>, ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    fn id_name_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    #[test]
    fn identical_batches_are_equivalent() {
        let s = id_name_schema();
        let a = batch(s.clone(), vec![1, 2], vec!["a", "b"]);
        let b = batch(s, vec![1, 2], vec!["a", "b"]);
        let report = check_equivalence(&a, &b, EquivalenceOptions::default()).unwrap();
        assert!(report.is_equivalent());
    }

    #[test]
    fn reordered_rows_are_still_equivalent() {
        let s = id_name_schema();
        let a = batch(s.clone(), vec![2, 1], vec!["b", "a"]);
        let b = batch(s, vec![1, 2], vec!["a", "b"]);
        let report = check_equivalence(&a, &b, EquivalenceOptions::default()).unwrap();
        assert!(report.is_equivalent());
    }

    #[test]
    fn different_values_are_not_equivalent() {
        let s = id_name_schema();
        let a = batch(s.clone(), vec![1, 2], vec!["a", "b"]);
        let b = batch(s, vec![1, 2], vec!["a", "c"]);
        let report = check_equivalence(&a, &b, EquivalenceOptions::default()).unwrap();
        assert!(!report.is_equivalent());
        assert!(!report.fingerprints_match);
    }

    #[test]
    fn different_row_counts_are_not_equivalent() {
        let s = id_name_schema();
        let a = batch(s.clone(), vec![1], vec!["a"]);
        let b = batch(s, vec![1, 2], vec!["a", "b"]);
        let report = check_equivalence(&a, &b, EquivalenceOptions::default()).unwrap();
        assert!(!report.is_equivalent());
    }

    #[test]
    fn column_order_does_not_affect_equivalence() {
        let schema_a = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
        ]));
        let a = RecordBatch::try_new(
            schema_a,
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let b = batch(id_name_schema(), vec![1], vec!["a"]);
        let report = check_equivalence(&a, &b, EquivalenceOptions::default()).unwrap();
        assert!(report.is_equivalent());
    }

    #[test]
    fn missing_column_is_visible_in_schema_diff_and_breaks_equivalence() {
        let s = id_name_schema();
        let a = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let b = batch(s, vec![1], vec!["a"]);
        let report = check_equivalence(&a, &b, EquivalenceOptions::default()).unwrap();
        assert!(!report.schema_diff.is_empty());
        assert!(!report.is_equivalent());
    }

    #[test]
    fn explicit_columns_restrict_the_comparison() {
        let s = id_name_schema();
        let a = batch(s.clone(), vec![1, 2], vec!["different", "values"]);
        let b = batch(s, vec![1, 2], vec!["a", "b"]);
        let options = EquivalenceOptions {
            columns: Some(vec![Arc::from("id")]),
            ..Default::default()
        };
        let report = check_equivalence(&a, &b, options).unwrap();
        assert!(report.fingerprints_match); // only "id" was compared, and it matches
    }
}
