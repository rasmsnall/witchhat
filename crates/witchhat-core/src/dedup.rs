//! Deduplication: the first native transformation, replacing Spark's
//! `dropDuplicates`.
//!
//! Built directly on [`crate::hash`] rather than a second row-comparison algorithm: a
//! row's composite hash over the given columns already is a dedup key, so
//! deduplication is "keep the first row whose hash has not been seen yet", one pass,
//! one extra `HashSet`. Filter and project, the other basic relational operations, are
//! not wrapped here because [`arrow_select`] already provides them
//! (`arrow_select::filter::filter_record_batch`, `RecordBatch::project`) with no
//! witchhat-specific behaviour to add; this module exists for the one operation that
//! actually needs witchhat's own hashing.

use arrow_array::RecordBatch;
use arrow_select::filter::filter_record_batch;

use crate::error::{Error, Result};
use crate::hash::{HashVersion, hash_batch};

/// Keeps the first row of every distinct value of `columns`, dropping the rest, and
/// preserving the relative order of the rows that remain.
///
/// Equivalent to Spark's `df.dropDuplicates(subset=columns)`, except which row within
/// a duplicate group survives is always the first one by input order (Spark's choice
/// there is not guaranteed). Rows are compared by their [`hash_batch`] fingerprint
/// over `columns`, so this inherits that function's null and float-canonicalization
/// semantics (`docs/architecture.md` Chapter III, Section 4): two rows whose `columns`
/// values are equal under those rules count as duplicates, including when both are
/// null in every listed column.
///
/// Hash collisions are not distinguished from true duplicates (the `u64` fingerprint
/// space is large enough that this is not a practical concern for realistic batch
/// sizes, but it is not a proof of distinctness either); see
/// `docs/architecture.md` Chapter XI for the same caveat as `table_fingerprint`.
///
/// # Errors
///
/// [`Error::UnknownColumn`] if a name in `columns` is not in `batch`'s schema.
/// [`Error::UnsupportedType`] if a requested column's Arrow type has no defined hash.
///
/// # Panics
///
/// Does not panic. Not async; runs on the calling thread in time linear in
/// `batch.num_rows() * columns.len()`, with one `HashSet<u64>` sized to at most
/// `batch.num_rows()` entries and no I/O.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use arrow_array::{Int64Array, RecordBatch};
/// use arrow_schema::{DataType, Field, Schema};
/// use witchhat_core::{HashVersion, drop_duplicates};
///
/// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
/// let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 1, 3, 2]))]).unwrap();
///
/// let deduped = drop_duplicates(&batch, &["id"], HashVersion::CURRENT).unwrap();
/// assert_eq!(deduped.num_rows(), 3); // 1, 2, 3, each kept once, in first-seen order
/// ```
pub fn drop_duplicates(
    batch: &RecordBatch,
    columns: &[&str],
    version: HashVersion,
) -> Result<RecordBatch> {
    let hashes = hash_batch(batch, columns, version)?;

    let mut seen = std::collections::HashSet::with_capacity(batch.num_rows());
    let keep: Vec<bool> = hashes.values().iter().map(|&h| seen.insert(h)).collect();

    filter_record_batch(batch, &arrow_array::BooleanArray::from(keep))
        .map_err(|e| Error::schema_mismatch(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn keeps_first_occurrence_of_each_value() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 1, 3, 2]))],
        )
        .unwrap();

        let deduped = drop_duplicates(&batch, &["id"], HashVersion::CURRENT).unwrap();
        let ids: Vec<i64> = deduped
            .column(0)
            .as_primitive::<Int64Type>()
            .values()
            .to_vec();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn composite_key_over_multiple_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 1])),
                Arc::new(StringArray::from(vec!["x", "x", "y"])),
            ],
        )
        .unwrap();

        // (1, "x") appears twice, (1, "y") once: two rows survive
        let deduped = drop_duplicates(&batch, &["a", "b"], HashVersion::CURRENT).unwrap();
        assert_eq!(deduped.num_rows(), 2);
    }

    #[test]
    fn no_duplicates_returns_every_row() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

        let deduped = drop_duplicates(&batch, &["id"], HashVersion::CURRENT).unwrap();
        assert_eq!(deduped.num_rows(), 3);
    }

    #[test]
    fn unknown_column_errors() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        assert!(drop_duplicates(&batch, &["missing"], HashVersion::CURRENT).is_err());
    }

    #[test]
    fn preserves_all_columns_not_just_the_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(StringArray::from(vec!["first", "second"])),
            ],
        )
        .unwrap();

        let deduped = drop_duplicates(&batch, &["id"], HashVersion::CURRENT).unwrap();
        assert_eq!(deduped.num_rows(), 1);
        assert_eq!(deduped.column(1).as_string::<i32>().value(0), "first");
    }
}
