//! Deduplication: the first native transformation, replacing Spark's
//! `dropDuplicates`.
//!
//! Uses [`crate::hash`] as a bucketing key, not as the final answer: a row's composite
//! hash over the given columns groups candidate duplicates cheaply, but a `u64`
//! fingerprint collision between two genuinely different rows is possible, and treating
//! it as equality would silently drop a distinct, valid row. So within a hash bucket,
//! [`arrow_row::RowConverter`] (the same exact, byte-comparable row format
//! [`mod@crate::join`]/[`mod@crate::aggregate`] use) confirms true equality before a row is
//! treated as a duplicate; the hash only decides which small bucket to compare against,
//! never decides equality itself. Filter and project, the other basic relational
//! operations, are not wrapped here because [`arrow_select`] already provides them
//! (`arrow_select::filter::filter_record_batch`, `RecordBatch::project`) with no
//! witchhat-specific behaviour to add; this module exists for the one operation that
//! actually needs witchhat's own hashing.

use std::collections::HashMap;

use arrow_array::RecordBatch;
use arrow_row::{RowConverter, SortField};
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
/// A `u64` fingerprint collision between two distinct rows never causes a false
/// duplicate: rows sharing a hash are additionally compared with
/// [`arrow_row::RowConverter`]'s exact, byte-comparable row format before either is
/// treated as a duplicate of the other, so the hash only narrows which rows get
/// compared, and never substitutes for the comparison itself.
///
/// # Errors
///
/// [`Error::UnknownColumn`] if a name in `columns` is not in `batch`'s schema.
/// [`Error::UnsupportedType`] if a requested column's Arrow type has no defined hash.
///
/// # Panics
///
/// Does not panic. Not async; runs on the calling thread with no I/O. Hashing is linear
/// in `batch.num_rows() * columns.len()`; the exactness check is linear overall unless
/// an adversarial or pathological input produces many rows sharing one hash bucket, in
/// which case that bucket's own rows are compared pairwise against each newcomer.
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

    let key_columns: Vec<arrow_array::ArrayRef> = columns
        .iter()
        .map(|name| {
            batch
                .schema()
                .index_of(name)
                .map(|idx| std::sync::Arc::clone(batch.column(idx)))
                .map_err(|_| Error::unknown_column(*name))
        })
        .collect::<Result<_>>()?;
    let fields: Vec<SortField> = key_columns
        .iter()
        .map(|c| SortField::new(c.data_type().clone()))
        .collect();
    let converter = RowConverter::new(fields).map_err(|e| Error::config(e.to_string()))?;
    let rows = converter
        .convert_columns(&key_columns)
        .map_err(|e| Error::config(e.to_string()))?;

    let mut buckets: HashMap<u64, Vec<arrow_row::OwnedRow>> = HashMap::new();
    let keep: Vec<bool> = hashes
        .values()
        .iter()
        .enumerate()
        .map(|(i, &h)| {
            let row = rows.row(i).owned();
            let bucket = buckets.entry(h).or_default();
            if bucket.contains(&row) {
                false
            } else {
                bucket.push(row);
                true
            }
        })
        .collect();

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
