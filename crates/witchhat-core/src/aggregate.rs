//! Aggregate: grouping rows and reducing each group to one row.
//!
//! Grouping uses the same [`arrow_row::RowConverter`] approach as [`mod@crate::join`], for
//! the same reason: a group boundary is a correctness question (which rows belong
//! together), not a "probably the same" one, so the exact byte-comparable row format is
//! used instead of [`crate::hash`]'s fingerprint. See `docs/architecture.md`
//! Chapter VIII, Section 2 for the fuller rationale shared with `join`.
//!
//! Numeric accumulation and comparison are type-specific, not a universal `f64`
//! downcast: an internal `Num` type holds a signed integer column's exact value in
//! `i128`, an unsigned integer column's in `u128` (either is wide enough to sum an
//! entire `Int64`/`UInt64` column without overflow in realistic cases, checked
//! regardless), and a `Decimal128`/`Decimal256` column's mantissa in its native width, so an
//! `Int64`/`UInt64` value beyond `f64`'s exact-integer range (±2^53) is summed and
//! compared exactly, and two distinct large integers never compare equal just because
//! they rounded to the same `f64`. `f64` is still used, deliberately, for an actual
//! `Float32`/`Float64` column (nothing more exact would be truthful there) and as
//! `Mean`'s single final division for non-decimal columns (`Mean`'s result is generally
//! fractional regardless of input type, so representing it as anything but a float would
//! be its own kind of dishonesty; what changed is that the *summation* feeding that
//! division is exact, not that the final division became exact too). `Sum`'s output
//! type follows the input's signedness/kind (`Int64` for a signed source, `UInt64` for
//! an unsigned source, `Float64` for a float source, the source's own
//! `Decimal128`/`Decimal256` type for a decimal source) rather than always `Float64`.
//! Date/Time/Timestamp aggregation is not implemented at all yet (see
//! `docs/architecture.md`'s "Still open" list); when it is, it belongs in that same
//! internal `Num` type, as a native integer representation, not through `f64`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, Decimal128Array, Decimal256Array, Float64Array, Int64Array, RecordBatch,
    UInt32Array, UInt64Array,
};
use arrow_buffer::i256;
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
    /// columns only, summed at the input's own exact precision, not through `f64`.
    /// Output `Int64` for a signed integer source, `UInt64` for an unsigned integer
    /// source, `Float64` for a float source, or the source's own
    /// `Decimal128`/`Decimal256` type for a decimal source.
    Sum,
    /// Arithmetic mean of non-null values, `None` if every value in the group is null.
    /// Numeric columns only; the sum feeding the division is exact (see [`AggFunc::Sum`]),
    /// only the final division is not. Output `Float64` for an integer or float source,
    /// or the source's own `Decimal128`/`Decimal256` type (mantissa integer-divided by
    /// the count, truncating any remainder) for a decimal source.
    Mean,
    /// The smallest non-null value, `None` if every value in the group is null.
    /// Numeric columns only, compared at the input's own exact precision, not through
    /// `f64`. Output type matches the input column.
    Min,
    /// The largest non-null value, `None` if every value in the group is null.
    /// Numeric columns only, compared at the input's own exact precision, not through
    /// `f64`. Output type matches the input column.
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
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

/// A numeric value extracted at its own exact precision, for comparison and
/// accumulation. Every value from one column carries the same variant, since a column
/// has one `DataType`; nothing here ever mixes variants.
#[derive(Debug, Clone, Copy)]
enum Num {
    /// `Int8`..`Int64`, widened losslessly into `i128`.
    SInt(i128),
    /// `UInt8`..`UInt64`, widened losslessly into `u128`.
    UInt(u128),
    /// `Float32`..`Float64`. Not "type specific" the way the integer variants are:
    /// `f64` already *is* the exact, native representation of a float column, so no
    /// further precision is available to preserve.
    Float(f64),
    /// A `Decimal128` column's raw mantissa; the scale is fixed per column (it is part
    /// of the column's `DataType`), so comparing/summing mantissas directly is exactly
    /// comparing/summing the decimal values they represent.
    Decimal128(i128),
    /// Same as [`Num::Decimal128`], for `Decimal256`.
    Decimal256(i256),
}

/// Extracts row `row` of `array` at its own exact precision. `None` for a null row or
/// an unsupported type (callers only reach this after [`numeric_supported`] already
/// rejected the latter).
fn extract_num(array: &dyn Array, row: usize) -> Option<Num> {
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Int8 => Some(Num::SInt(i128::from(
            array.as_primitive::<Int8Type>().value(row),
        ))),
        DataType::Int16 => Some(Num::SInt(i128::from(
            array.as_primitive::<Int16Type>().value(row),
        ))),
        DataType::Int32 => Some(Num::SInt(i128::from(
            array.as_primitive::<Int32Type>().value(row),
        ))),
        DataType::Int64 => Some(Num::SInt(i128::from(
            array.as_primitive::<Int64Type>().value(row),
        ))),
        DataType::UInt8 => Some(Num::UInt(u128::from(
            array.as_primitive::<UInt8Type>().value(row),
        ))),
        DataType::UInt16 => Some(Num::UInt(u128::from(
            array.as_primitive::<UInt16Type>().value(row),
        ))),
        DataType::UInt32 => Some(Num::UInt(u128::from(
            array.as_primitive::<UInt32Type>().value(row),
        ))),
        DataType::UInt64 => Some(Num::UInt(u128::from(
            array.as_primitive::<UInt64Type>().value(row),
        ))),
        DataType::Float32 => Some(Num::Float(f64::from(
            array.as_primitive::<Float32Type>().value(row),
        ))),
        DataType::Float64 => Some(Num::Float(array.as_primitive::<Float64Type>().value(row))),
        DataType::Decimal128(_, _) => Some(Num::Decimal128(
            array.as_primitive::<Decimal128Type>().value(row),
        )),
        DataType::Decimal256(_, _) => Some(Num::Decimal256(
            array.as_primitive::<Decimal256Type>().value(row),
        )),
        _ => None,
    }
}

/// Exact ordering between two [`Num`]s from the same column (and therefore always the
/// same variant).
fn compare_num(a: Num, b: Num) -> std::cmp::Ordering {
    match (a, b) {
        (Num::SInt(a), Num::SInt(b)) => a.cmp(&b),
        (Num::UInt(a), Num::UInt(b)) => a.cmp(&b),
        (Num::Float(a), Num::Float(b)) => a.total_cmp(&b),
        (Num::Decimal128(a), Num::Decimal128(b)) => a.cmp(&b),
        (Num::Decimal256(a), Num::Decimal256(b)) => a.cmp(&b),
        _ => unreachable!("a single column's values are always the same Num variant"),
    }
}

/// The one place `SInt`/`UInt`/`Float` convert to `f64`: `Mean`'s final division for a
/// non-decimal source, after the summation itself has already happened exactly. Never
/// called for a `Decimal128`/`Decimal256` accumulation, which stays in its native
/// representation instead (see [`build_mean_column`]).
fn num_as_f64(n: Num) -> f64 {
    match n {
        Num::SInt(v) => v as f64,
        Num::UInt(v) => v as f64,
        Num::Float(v) => v,
        Num::Decimal128(_) | Num::Decimal256(_) => {
            unreachable!("decimal Num never converts through num_as_f64")
        }
    }
}

/// Sums the non-null values of `source` at `rows`, plus how many were non-null.
/// `(None, 0)` if every row was null. Accumulates at `Num`'s exact precision the whole
/// way; the addition itself (not just the final total) is exact, so error never
/// compounds across many rows the way repeated `f64` addition's would.
///
/// # Errors
///
/// [`Error::Overflow`] if the exact accumulator (`i128`/`u128`/`i128`-mantissa/`i256`-
/// mantissa, depending on `source`'s type) cannot represent the running total.
fn sum_and_count(
    source: &dyn Array,
    rows: &[u32],
    column_name: &str,
) -> Result<(Option<Num>, u32)> {
    let mut acc: Option<Num> = None;
    let mut count = 0u32;
    for &r in rows {
        let Some(v) = extract_num(source, r as usize) else {
            continue;
        };
        count += 1;
        acc = Some(match (acc, v) {
            (None, v) => v,
            (Some(Num::SInt(a)), Num::SInt(b)) => Num::SInt(a.checked_add(b).ok_or_else(|| {
                Error::overflow(format!("sum of {column_name:?} overflowed i128"))
            })?),
            (Some(Num::UInt(a)), Num::UInt(b)) => Num::UInt(a.checked_add(b).ok_or_else(|| {
                Error::overflow(format!("sum of {column_name:?} overflowed u128"))
            })?),
            (Some(Num::Float(a)), Num::Float(b)) => Num::Float(a + b),
            (Some(Num::Decimal128(a)), Num::Decimal128(b)) => {
                Num::Decimal128(a.checked_add(b).ok_or_else(|| {
                    Error::overflow(format!("sum of {column_name:?} overflowed decimal128"))
                })?)
            }
            (Some(Num::Decimal256(a)), Num::Decimal256(b)) => {
                Num::Decimal256(a.checked_add(b).ok_or_else(|| {
                    Error::overflow(format!("sum of {column_name:?} overflowed decimal256"))
                })?)
            }
            _ => unreachable!("a single column's values are always the same Num variant"),
        });
    }
    Ok((acc, count))
}

/// Builds `Sum`'s output column from one exact sum per group. See [`AggFunc::Sum`] for
/// the output type per source kind.
fn build_sum_column(
    dt: &DataType,
    sums: Vec<Option<Num>>,
    column_name: &str,
) -> Result<(DataType, ArrayRef)> {
    match dt {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let values: Result<Vec<Option<i64>>> = sums
                .into_iter()
                .map(|s| match s {
                    None => Ok(None),
                    Some(Num::SInt(v)) => i64::try_from(v).map(Some).map_err(|_| {
                        Error::overflow(format!("sum of {column_name:?} overflowed i64"))
                    }),
                    _ => unreachable!(),
                })
                .collect();
            Ok((DataType::Int64, Arc::new(Int64Array::from(values?))))
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            let values: Result<Vec<Option<u64>>> = sums
                .into_iter()
                .map(|s| match s {
                    None => Ok(None),
                    Some(Num::UInt(v)) => u64::try_from(v).map(Some).map_err(|_| {
                        Error::overflow(format!("sum of {column_name:?} overflowed u64"))
                    }),
                    _ => unreachable!(),
                })
                .collect();
            Ok((DataType::UInt64, Arc::new(UInt64Array::from(values?))))
        }
        DataType::Float32 | DataType::Float64 => {
            let values: Vec<Option<f64>> = sums.into_iter().map(|s| s.map(num_as_f64)).collect();
            Ok((DataType::Float64, Arc::new(Float64Array::from(values))))
        }
        DataType::Decimal128(p, s) => {
            let values: Vec<Option<i128>> = sums
                .into_iter()
                .map(|v| match v {
                    None => None,
                    Some(Num::Decimal128(m)) => Some(m),
                    _ => unreachable!(),
                })
                .collect();
            let array = Decimal128Array::from(values)
                .with_precision_and_scale(*p, *s)
                .map_err(|e| Error::schema_mismatch(e.to_string()))?;
            Ok((DataType::Decimal128(*p, *s), Arc::new(array)))
        }
        DataType::Decimal256(p, s) => {
            let values: Vec<Option<i256>> = sums
                .into_iter()
                .map(|v| match v {
                    None => None,
                    Some(Num::Decimal256(m)) => Some(m),
                    _ => unreachable!(),
                })
                .collect();
            let array = Decimal256Array::from(values)
                .with_precision_and_scale(*p, *s)
                .map_err(|e| Error::schema_mismatch(e.to_string()))?;
            Ok((DataType::Decimal256(*p, *s), Arc::new(array)))
        }
        other => Err(Error::unsupported_type(other.clone())),
    }
}

/// Builds `Mean`'s output column from one `(exact sum, non-null count)` pair per group.
/// See [`AggFunc::Mean`] for the output type per source kind and the decimal rounding
/// rule (truncates, no half-up rounding).
fn build_mean_column(
    dt: &DataType,
    sums_and_counts: Vec<(Option<Num>, u32)>,
) -> Result<(DataType, ArrayRef)> {
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64 => {
            let values: Vec<Option<f64>> = sums_and_counts
                .into_iter()
                .map(|(sum, count)| sum.map(|s| num_as_f64(s) / f64::from(count)))
                .collect();
            Ok((DataType::Float64, Arc::new(Float64Array::from(values))))
        }
        DataType::Decimal128(p, s) => {
            let values: Vec<Option<i128>> = sums_and_counts
                .into_iter()
                .map(|(sum, count)| match sum {
                    None => None,
                    Some(Num::Decimal128(m)) => Some(m / i128::from(count)),
                    _ => unreachable!(),
                })
                .collect();
            let array = Decimal128Array::from(values)
                .with_precision_and_scale(*p, *s)
                .map_err(|e| Error::schema_mismatch(e.to_string()))?;
            Ok((DataType::Decimal128(*p, *s), Arc::new(array)))
        }
        DataType::Decimal256(p, s) => {
            let values: Vec<Option<i256>> = sums_and_counts
                .into_iter()
                .map(|(sum, count)| match sum {
                    None => None,
                    Some(Num::Decimal256(m)) => Some(m / i256::from_i128(i128::from(count))),
                    _ => unreachable!(),
                })
                .collect();
            let array = Decimal256Array::from(values)
                .with_precision_and_scale(*p, *s)
                .map_err(|e| Error::schema_mismatch(e.to_string()))?;
            Ok((DataType::Decimal256(*p, *s), Arc::new(array)))
        }
        other => Err(Error::unsupported_type(other.clone())),
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
/// is not one of `Int8`..`Int64`, `UInt8`..`UInt64`, `Float32`, `Float64`,
/// `Decimal128`, `Decimal256`. `Count` accepts any column type.
/// [`Error::Overflow`] if `Sum`/`Mean`'s exact accumulator cannot represent a group's
/// running total (see `docs/architecture.md` Chapter VIII for how wide each
/// accumulator is); this replaces silent, wrong output from a previous `f64`-based
/// accumulator overflowing its precision unnoticed.
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
                let sums: Vec<Option<Num>> = rows_per_group
                    .iter()
                    .map(|g| sum_and_count(source.as_ref(), g, &agg.column).map(|(s, _)| s))
                    .collect::<Result<_>>()?;
                let (dt, array) = build_sum_column(source.data_type(), sums, &agg.column)?;
                out_fields.push(Field::new(agg.alias.as_ref(), dt, true));
                out_columns.push(array);
            }
            AggFunc::Mean => {
                let sums_and_counts: Vec<(Option<Num>, u32)> = rows_per_group
                    .iter()
                    .map(|g| sum_and_count(source.as_ref(), g, &agg.column))
                    .collect::<Result<_>>()?;
                let (dt, array) = build_mean_column(source.data_type(), sums_and_counts)?;
                out_fields.push(Field::new(agg.alias.as_ref(), dt, true));
                out_columns.push(array);
            }
            AggFunc::Min | AggFunc::Max => {
                let winners: Vec<Option<u32>> = rows_per_group
                    .iter()
                    .map(|g| {
                        let mut best: Option<(u32, Num)> = None;
                        for &r in g {
                            if let Some(v) = extract_num(source.as_ref(), r as usize) {
                                best = Some(match best {
                                    None => (r, v),
                                    Some((br, bv)) => {
                                        let ord = compare_num(v, bv);
                                        let better = if agg.func == AggFunc::Min {
                                            ord == std::cmp::Ordering::Less
                                        } else {
                                            ord == std::cmp::Ordering::Greater
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
        // Sum of a signed integer source stays Int64, not Float64: the whole point of
        // the exact i128 accumulator is that a caller gets an exact integer back.
        assert_eq!(out.schema().field(1).data_type(), &DataType::Int64);
        let country = out.column(0).as_string::<i32>();
        let total = out
            .column(1)
            .as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(country.value(0), "NO");
        assert_eq!(total.value(0), 115); // 10 + 5 + 100
        assert_eq!(country.value(1), "SE");
        assert_eq!(total.value(1), 20); // 20, null skipped
    }

    #[test]
    fn sum_exact_beyond_f64_precision() {
        // i64::MAX - 1 and 3, individually representable in Int64 but their sum
        // (i64::MAX + 2) overflows i64, so this also exercises the Overflow error;
        // a value comfortably beyond 2^53 on its own already breaks the old f64 path.
        let big = i64::MAX - 2;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("g", DataType::Utf8, false),
                Field::new("amount", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a", "a"])),
                Arc::new(Int64Array::from(vec![big, 1])),
            ],
        )
        .unwrap();

        // big + 1 still fits in i64: exact, not rounded the way f64 would round a
        // value this large.
        let out = aggregate(
            &batch,
            &["g"],
            &[Aggregation::new("amount", AggFunc::Sum, "total")],
        )
        .unwrap();
        let total = out
            .column(1)
            .as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(total.value(0), big + 1);
    }

    #[test]
    fn sum_overflow_errors_instead_of_silently_wrapping() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("g", DataType::Utf8, false),
                Field::new("amount", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a", "a"])),
                Arc::new(Int64Array::from(vec![i64::MAX, i64::MAX])),
            ],
        )
        .unwrap();

        let out = aggregate(
            &batch,
            &["g"],
            &[Aggregation::new("amount", AggFunc::Sum, "total")],
        );
        assert!(out.is_err());
    }

    #[test]
    fn min_max_distinguish_large_integers_f64_would_conflate() {
        // 2^53 + 1 and 2^53 + 2 both round to the same f64, so the old f64-based
        // comparison could not tell them apart; the exact i128 comparison must.
        let a = (1i64 << 53) + 1;
        let b = (1i64 << 53) + 2;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("g", DataType::Utf8, false),
                Field::new("amount", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["x", "x"])),
                Arc::new(Int64Array::from(vec![b, a])), // b first, so "min" isn't just "first row"
            ],
        )
        .unwrap();

        let out = aggregate(
            &batch,
            &["g"],
            &[
                Aggregation::new("amount", AggFunc::Min, "lo"),
                Aggregation::new("amount", AggFunc::Max, "hi"),
            ],
        )
        .unwrap();
        let lo = out
            .column(1)
            .as_primitive::<arrow_array::types::Int64Type>();
        let hi = out
            .column(2)
            .as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(lo.value(0), a);
        assert_eq!(hi.value(0), b);
        assert_ne!(lo.value(0), hi.value(0)); // an f64 comparison would have called these equal
    }

    #[test]
    fn sum_and_mean_of_unsigned_source_stay_unsigned_and_exact() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("g", DataType::Utf8, false),
                Field::new("amount", DataType::UInt64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a", "a"])),
                Arc::new(arrow_array::UInt64Array::from(vec![u64::MAX - 1, 1u64])),
            ],
        )
        .unwrap();

        let out = aggregate(
            &batch,
            &["g"],
            &[
                Aggregation::new("amount", AggFunc::Sum, "total"),
                Aggregation::new("amount", AggFunc::Mean, "avg"),
            ],
        )
        .unwrap();
        assert_eq!(out.schema().field(1).data_type(), &DataType::UInt64);
        let total = out
            .column(1)
            .as_primitive::<arrow_array::types::UInt64Type>();
        assert_eq!(total.value(0), u64::MAX); // exact: (u64::MAX - 1) + 1
        assert_eq!(out.schema().field(2).data_type(), &DataType::Float64);
    }

    #[test]
    fn decimal128_sum_and_min_max_stay_decimal_not_float() {
        let array = arrow_array::Decimal128Array::from(vec![Some(1_000_i128), Some(2_000_i128)])
            .with_precision_and_scale(20, 2)
            .unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("g", DataType::Utf8, false),
                Field::new("amount", DataType::Decimal128(20, 2), false),
            ])),
            vec![Arc::new(StringArray::from(vec!["a", "a"])), Arc::new(array)],
        )
        .unwrap();

        let out = aggregate(
            &batch,
            &["g"],
            &[
                Aggregation::new("amount", AggFunc::Sum, "total"),
                Aggregation::new("amount", AggFunc::Max, "hi"),
            ],
        )
        .unwrap();
        assert_eq!(
            out.schema().field(1).data_type(),
            &DataType::Decimal128(20, 2)
        );
        let total = out.column(1).as_primitive::<Decimal128Type>();
        assert_eq!(total.value(0), 3_000); // 10.00 + 20.00, mantissa-exact
        let hi = out.column(2).as_primitive::<Decimal128Type>();
        assert_eq!(hi.value(0), 2_000);
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
            .as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(total.value(0), 135);
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
