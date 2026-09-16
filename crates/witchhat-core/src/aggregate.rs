//! Aggregate: grouping rows and reducing each group to one row.
//!
//! Grouping uses the same [`arrow_row::RowConverter`] approach as [`mod@crate::join`], for
//! the same reason: a group boundary is a correctness question (which rows belong
//! together), not a "probably the same" one, so the exact byte-comparable row format is
//! used instead of [`crate::hash`]'s fingerprint. See `docs/architecture.md`
//! Chapter VIII, Section 2 for the fuller rationale shared with `join`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch, UInt32Array};
use arrow_row::{OwnedRow, RowConverter, SortField};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::take::take;

use crate::error::{Error, Result};

/// A reduction applied to one group's rows in a single column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggFunc {
    /// Non-null values in the group. Works on any column type. Output `Int64`.
    Count,
    /// Sum of non-null values, `None` if every value in the group is null. Numeric
    /// columns only. Output `Float64`.
    Sum,
    /// Arithmetic mean of non-null values, `None` if every value in the group is null.
    /// Numeric columns only. Output `Float64`.
    Mean,
    /// The smallest non-null value, `None` if every value in the group is null.
    /// Numeric columns only. Output type matches the input column.
    Min,
    /// The largest non-null value, `None` if every value in the group is null.
    /// Numeric columns only. Output type matches the input column.
    Max,
}

impl AggFunc {
    /// Parses `"count"`, `"sum"`, `"mean"` (or `"avg"`), `"min"` or `"max"`
    /// (case-sensitive). Returns `None` for anything else.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "count" => Some(AggFunc::Count),
            "sum" => Some(AggFunc::Sum),
            "mean" | "avg" => Some(AggFunc::Mean),
            "min" => Some(AggFunc::Min),
            "max" => Some(AggFunc::Max),
            _ => None,
        }
    }
}

/// One column of an [`aggregate`] call: reduce `column` with `func`, name the result
/// `alias`.
#[derive(Debug, Clone)]
pub struct Aggregation {
    /// The source column to reduce.
    pub column: Arc<str>,
    /// How to reduce it.
    pub func: AggFunc,
    /// The output column's name.
    pub alias: Arc<str>,
}

impl Aggregation {
    /// Builds an [`Aggregation`].
    pub fn new(column: impl Into<Arc<str>>, func: AggFunc, alias: impl Into<Arc<str>>) -> Self {
        Self {
            column: column.into(),
            func,
            alias: alias.into(),
        }
    }
}

fn numeric_supported(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    )
}

/// Extracts row `row` of `array` as `f64` for comparison/arithmetic purposes only; the
/// output columns of [`AggFunc::Min`]/[`AggFunc::Max`] are built separately, from the
/// original array, so this conversion never touches what a caller actually sees for
/// those. `Int64`/`UInt64` values beyond `f64`'s exact-integer range (±2^53) may
/// therefore compare or sum with reduced precision; see `docs/architecture.md`
/// Chapter VIII.
fn extract_f64(array: &dyn Array, row: usize) -> Option<f64> {
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Int8 => Some(array.as_primitive::<Int8Type>().value(row) as f64),
        DataType::Int16 => Some(array.as_primitive::<Int16Type>().value(row) as f64),
        DataType::Int32 => Some(array.as_primitive::<Int32Type>().value(row) as f64),
        DataType::Int64 => Some(array.as_primitive::<Int64Type>().value(row) as f64),
        DataType::UInt8 => Some(array.as_primitive::<UInt8Type>().value(row) as f64),
        DataType::UInt16 => Some(array.as_primitive::<UInt16Type>().value(row) as f64),
        DataType::UInt32 => Some(array.as_primitive::<UInt32Type>().value(row) as f64),
        DataType::UInt64 => Some(array.as_primitive::<UInt64Type>().value(row) as f64),
        DataType::Float32 => Some(array.as_primitive::<Float32Type>().value(row) as f64),
        DataType::Float64 => Some(array.as_primitive::<Float64Type>().value(row)),
        _ => None,
    }
}

fn resolve_index(batch: &RecordBatch, name: &str) -> Result<usize> {
    batch
        .schema()
        .index_of(name)
        .map_err(|_| Error::unknown_column(name))
}

/// Groups `batch` by `group_by` and reduces each group with `aggregations`.
///
/// `group_by` may be empty, in which case every row of `batch` is one group (a
/// whole-table aggregate, matching `df.agg(...)` with no preceding `groupBy`); an empty
/// `batch` in that case still produces exactly one output row, with `Count` `0` and
/// every other aggregation `null`, matching how an empty-input whole-table aggregate is
/// conventionally defined. Output row order is first-seen group order: the order in
/// which each distinct key first appears in `batch`, not sorted.
///
/// The output schema is `group_by`'s columns (types preserved from `batch`, always
/// nullable), followed by one column per `aggregations` entry, named by its `alias`, in
/// the order given.
///
/// # Errors
///
/// [`Error::UnknownColumn`] if a name in `group_by` or an [`Aggregation::column`] is not
/// in `batch`'s schema.
/// [`Error::UnsupportedType`] if a [`Aggregation::column`] for `Sum`/`Mean`/`Min`/`Max`
/// is not one of `Int8`..`Int64`, `UInt8`..`UInt64`, `Float32`, `Float64`. `Count`
/// accepts any column type.
///
/// # Panics
///
/// Does not panic. Not async; runs on the calling thread in time linear in
/// `batch.num_rows() * (group_by.len() + aggregations.len())`, with no I/O.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use arrow_array::{Int64Array, RecordBatch, StringArray};
/// use arrow_schema::{DataType, Field, Schema};
/// use witchhat_core::{AggFunc, Aggregation, aggregate};
///
/// let batch = RecordBatch::try_new(
///     Arc::new(Schema::new(vec![
///         Field::new("country", DataType::Utf8, false),
///         Field::new("amount", DataType::Int64, false),
///     ])),
///     vec![
///         Arc::new(StringArray::from(vec!["NO", "SE", "NO"])),
///         Arc::new(Int64Array::from(vec![10, 20, 5])),
///     ],
/// )
/// .unwrap();
///
/// let out = aggregate(
///     &batch,
///     &["country"],
///     &[Aggregation::new("amount", AggFunc::Sum, "total")],
/// )
/// .unwrap();
/// assert_eq!(out.num_rows(), 2); // "NO" and "SE"
/// ```
pub fn aggregate(
    batch: &RecordBatch,
    group_by: &[&str],
    aggregations: &[Aggregation],
) -> Result<RecordBatch> {
    let group_indices: Vec<usize> = group_by
        .iter()
        .map(|name| resolve_index(batch, name))
        .collect::<Result<_>>()?;
    let group_columns: Vec<ArrayRef> = group_indices
        .iter()
        .map(|&i| Arc::clone(batch.column(i)))
        .collect();

    for agg in aggregations {
        let idx = resolve_index(batch, &agg.column)?;
        if agg.func != AggFunc::Count && !numeric_supported(batch.column(idx).data_type()) {
            return Err(Error::unsupported_type(
                batch.column(idx).data_type().clone(),
            ));
        }
    }

    let mut rows_per_group: Vec<Vec<u32>> = Vec::new();
    if group_by.is_empty() {
        rows_per_group.push((0..batch.num_rows() as u32).collect());
    } else {
        let fields: Vec<SortField> = group_columns
            .iter()
            .map(|c| SortField::new(c.data_type().clone()))
            .collect();
        let converter = RowConverter::new(fields).map_err(|e| Error::config(e.to_string()))?;
        let rows = converter
            .convert_columns(&group_columns)
            .map_err(|e| Error::config(e.to_string()))?;

        let mut group_index: HashMap<OwnedRow, usize> = HashMap::new();
        for (i, row) in rows.iter().enumerate() {
            let owned = row.owned();
            let gi = match group_index.get(&owned) {
                Some(&gi) => gi,
                None => {
                    let gi = rows_per_group.len();
                    group_index.insert(owned, gi);
                    rows_per_group.push(Vec::new());
                    gi
                }
            };
            rows_per_group[gi].push(i as u32);
        }
    }

    let mut out_fields: Vec<Field> = Vec::new();
    let mut out_columns: Vec<ArrayRef> = Vec::new();

    if !group_by.is_empty() {
        let representatives: Vec<u32> = rows_per_group.iter().map(|g| g[0]).collect();
        let representatives = UInt32Array::from(representatives);
        for (name, column) in group_by.iter().zip(&group_columns) {
            let taken = take(column.as_ref(), &representatives, None)
                .map_err(|e| Error::schema_mismatch(e.to_string()))?;
            out_fields.push(Field::new(*name, column.data_type().clone(), true));
            out_columns.push(taken);
        }
    }

    for agg in aggregations {
        let idx = resolve_index(batch, &agg.column)?;
        let source = Arc::clone(batch.column(idx));

        match agg.func {
            AggFunc::Count => {
                let counts: Vec<i64> = rows_per_group
                    .iter()
                    .map(|g| g.iter().filter(|&&r| !source.is_null(r as usize)).count() as i64)
                    .collect();
                out_fields.push(Field::new(agg.alias.as_ref(), DataType::Int64, false));
                out_columns.push(Arc::new(Int64Array::from(counts)));
            }
            AggFunc::Sum => {
                let sums: Vec<Option<f64>> = rows_per_group
                    .iter()
                    .map(|g| {
                        let mut acc = 0.0;
                        let mut any = false;
                        for &r in g {
                            if let Some(v) = extract_f64(source.as_ref(), r as usize) {
                                acc += v;
                                any = true;
                            }
                        }
                        any.then_some(acc)
                    })
                    .collect();
                out_fields.push(Field::new(agg.alias.as_ref(), DataType::Float64, true));
                out_columns.push(Arc::new(Float64Array::from(sums)));
            }
            AggFunc::Mean => {
                let means: Vec<Option<f64>> = rows_per_group
                    .iter()
                    .map(|g| {
                        let mut acc = 0.0;
                        let mut n = 0u32;
                        for &r in g {
                            if let Some(v) = extract_f64(source.as_ref(), r as usize) {
                                acc += v;
                                n += 1;
                            }
                        }
                        (n > 0).then_some(acc / f64::from(n))
                    })
                    .collect();
                out_fields.push(Field::new(agg.alias.as_ref(), DataType::Float64, true));
                out_columns.push(Arc::new(Float64Array::from(means)));
            }
            AggFunc::Min | AggFunc::Max => {
                let winners: Vec<Option<u32>> = rows_per_group
                    .iter()
                    .map(|g| {
                        let mut best: Option<(u32, f64)> = None;
                        for &r in g {
                            if let Some(v) = extract_f64(source.as_ref(), r as usize) {
                                best = Some(match best {
                                    None => (r, v),
                                    Some((br, bv)) => {
                                        let better = if agg.func == AggFunc::Min {
                                            v < bv
                                        } else {
                                            v > bv
                                        };
                                        if better { (r, v) } else { (br, bv) }
                                    }
                                });
                            }
                        }
                        best.map(|(r, _)| r)
                    })
                    .collect();
                let winners = UInt32Array::from(winners);
                let taken = take(source.as_ref(), &winners, None)
                    .map_err(|e| Error::schema_mismatch(e.to_string()))?;
                out_fields.push(Field::new(
                    agg.alias.as_ref(),
                    source.data_type().clone(),
                    true,
                ));
                out_columns.push(taken);
            }
        }
    }

    RecordBatch::try_new(Arc::new(Schema::new(out_fields)), out_columns)
        .map_err(|e| Error::schema_mismatch(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;

    fn sales() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("country", DataType::Utf8, false),
                Field::new("amount", DataType::Int64, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["NO", "SE", "NO", "SE", "NO"])),
                Arc::new(Int64Array::from(vec![
                    Some(10),
                    Some(20),
                    Some(5),
                    None,
                    Some(100),
                ])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn sum_per_group() {
        let out = aggregate(
            &sales(),
            &["country"],
            &[Aggregation::new("amount", AggFunc::Sum, "total")],
        )
        .unwrap();
        assert_eq!(out.num_rows(), 2);
        let country = out.column(0).as_string::<i32>();
        let total = out
            .column(1)
            .as_primitive::<arrow_array::types::Float64Type>();
        assert_eq!(country.value(0), "NO");
        assert_eq!(total.value(0), 115.0); // 10 + 5 + 100
        assert_eq!(country.value(1), "SE");
        assert_eq!(total.value(1), 20.0); // 20, null skipped
    }

    #[test]
    fn count_skips_nulls() {
        let out = aggregate(
            &sales(),
            &["country"],
            &[Aggregation::new("amount", AggFunc::Count, "n")],
        )
        .unwrap();
        let n = out
            .column(1)
            .as_primitive::<arrow_array::types::Int64Type>();
        // NO: 3 rows all non-null; SE: 2 rows, 1 null -> count 1
        let country = out.column(0).as_string::<i32>();
        for i in 0..out.num_rows() {
            if country.value(i) == "NO" {
                assert_eq!(n.value(i), 3);
            } else {
                assert_eq!(n.value(i), 1);
            }
        }
    }

    #[test]
    fn min_and_max_preserve_source_type() {
        let out = aggregate(
            &sales(),
            &["country"],
            &[
                Aggregation::new("amount", AggFunc::Min, "lo"),
                Aggregation::new("amount", AggFunc::Max, "hi"),
            ],
        )
        .unwrap();
        assert_eq!(out.schema().field(1).data_type(), &DataType::Int64);
        let lo = out
            .column(1)
            .as_primitive::<arrow_array::types::Int64Type>();
        let hi = out
            .column(2)
            .as_primitive::<arrow_array::types::Int64Type>();
        let country = out.column(0).as_string::<i32>();
        for i in 0..out.num_rows() {
            if country.value(i) == "NO" {
                assert_eq!(lo.value(i), 5);
                assert_eq!(hi.value(i), 100);
            }
        }
    }

    #[test]
    fn no_group_by_aggregates_whole_batch() {
        let out = aggregate(
            &sales(),
            &[],
            &[Aggregation::new("amount", AggFunc::Sum, "total")],
        )
        .unwrap();
        assert_eq!(out.num_rows(), 1);
        let total = out
            .column(0)
            .as_primitive::<arrow_array::types::Float64Type>();
        assert_eq!(total.value(0), 135.0);
    }

    #[test]
    fn empty_batch_whole_table_aggregate_produces_one_null_row() {
        let empty = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Int64,
                true,
            )])),
            vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
        )
        .unwrap();
        let out = aggregate(
            &empty,
            &[],
            &[
                Aggregation::new("amount", AggFunc::Count, "n"),
                Aggregation::new("amount", AggFunc::Sum, "total"),
            ],
        )
        .unwrap();
        assert_eq!(out.num_rows(), 1);
        assert_eq!(
            out.column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0),
            0
        );
        assert!(out.column(1).is_null(0));
    }

    #[test]
    fn unsupported_type_for_sum_errors() {
        let out = aggregate(
            &sales(),
            &["country"],
            &[Aggregation::new("country", AggFunc::Sum, "bad")],
        );
        assert!(out.is_err());
    }

    #[test]
    fn unknown_column_errors() {
        assert!(
            aggregate(
                &sales(),
                &["missing"],
                &[Aggregation::new("amount", AggFunc::Sum, "total")]
            )
            .is_err()
        );
    }
}
