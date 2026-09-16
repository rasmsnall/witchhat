//! Join: matching rows of two batches on one or more key columns.
//!
//! Deliberately not built on [`crate::hash`]: a join result is only as correct as its
//! key comparison, and a `u64` fingerprint collision that is an acceptable, documented
//! risk for [`crate::equivalence`]'s evidence-of-equality check would silently merge
//! two distinct keys into one match here. [`arrow_row::RowConverter`] converts each
//! side's key columns into a canonical, memcmp-comparable byte row instead: equal rows
//! are guaranteed to compare equal, not just probably. As a side effect, join keys
//! support every Arrow type `RowConverter` does, a broader range than [`crate::hash`]'s
//! own hand-rolled type dispatch (`docs/architecture.md` Chapter VIII, Section 2 has
//! the fuller rationale for why the two kernels made different choices here).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use arrow_row::{RowConverter, Rows, SortField};
use arrow_schema::{Field, Schema};
use arrow_select::take::take;

use crate::error::{Error, Result};

/// Which rows of `left` and `right` survive a [`join`] with no match on the other side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum JoinType {
    /// Only rows with a match on both sides.
    #[default]
    Inner,
    /// Every row of `left`; unmatched rows get null `right` columns.
    Left,
    /// Every row of `right`; unmatched rows get null `left` columns.
    Right,
    /// Every row of both; a row unmatched on one side gets null columns for the other.
    Full,
}

impl JoinType {
    /// Parses `"inner"`, `"left"`, `"right"` or `"full"` (case-sensitive). Returns
    /// `None` for anything else.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "inner" => Some(JoinType::Inner),
            "left" => Some(JoinType::Left),
            "right" => Some(JoinType::Right),
            "full" => Some(JoinType::Full),
            _ => None,
        }
    }
}

fn resolve_columns(batch: &RecordBatch, names: &[&str]) -> Result<Vec<ArrayRef>> {
    names
        .iter()
        .map(|name| {
            batch
                .schema()
                .index_of(name)
                .map(|idx| Arc::clone(batch.column(idx)))
                .map_err(|_| Error::unknown_column(*name))
        })
        .collect()
}

fn row_index(rows: &Rows) -> HashMap<arrow_row::OwnedRow, Vec<u32>> {
    let mut map: HashMap<arrow_row::OwnedRow, Vec<u32>> = HashMap::new();
    for (i, row) in rows.iter().enumerate() {
        map.entry(row.owned()).or_default().push(i as u32);
    }
    map
}

/// Joins `left` and `right` on `left_keys`/`right_keys`, matched pairwise by position
/// (`left_keys[0]` compares against `right_keys[0]`, and so on).
///
/// The output schema is every field of `left` followed by every field of `right`; a
/// `right` field whose name collides with a `left` field is suffixed `_right`. Every
/// output field is nullable regardless of the input schemas' own nullability, since an
/// outer join can introduce a null on either side; an `Inner` join never actually
/// produces one, but the schema still allows it rather than promising something a
/// caller could not rely on for a different `how`.
///
/// Row order: every `left` row in its original order, each repeated once per match (or
/// once with null `right` columns, under [`JoinType::Left`]/[`JoinType::Full`], if it
/// has none), followed by every unmatched `right` row in its original order, under
/// [`JoinType::Right`]/[`JoinType::Full`].
///
/// # Errors
///
/// [`Error::Config`] if `left_keys` and `right_keys` have different lengths, or either
/// is empty.
/// [`Error::UnknownColumn`] if a name in either key list is not in its batch's schema.
/// [`Error::TypeMismatch`] if `left_keys[i]`'s Arrow type does not exactly equal
/// `right_keys[i]`'s (no implicit coercion; cast one side first if the types should be
/// treated as comparable).
///
/// # Panics
///
/// Does not panic. Not async; runs on the calling thread, building one hash index over
/// `right`'s key rows (`O(right.num_rows())` extra memory) and probing it once per
/// `left` row, with no I/O.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use arrow_array::{Int64Array, RecordBatch, StringArray};
/// use arrow_schema::{DataType, Field, Schema};
/// use witchhat_core::{JoinType, join};
///
/// let users = RecordBatch::try_new(
///     Arc::new(Schema::new(vec![
///         Field::new("id", DataType::Int64, false),
///         Field::new("name", DataType::Utf8, false),
///     ])),
///     vec![
///         Arc::new(Int64Array::from(vec![1, 2])),
///         Arc::new(StringArray::from(vec!["alice", "bob"])),
///     ],
/// )
/// .unwrap();
/// let orders = RecordBatch::try_new(
///     Arc::new(Schema::new(vec![Field::new("user_id", DataType::Int64, false)])),
///     vec![Arc::new(Int64Array::from(vec![1, 1]))],
/// )
/// .unwrap();
///
/// let joined = join(&users, &orders, &["id"], &["user_id"], JoinType::Left).unwrap();
/// assert_eq!(joined.num_rows(), 3); // alice matched twice, bob unmatched once
/// ```
pub fn join(
    left: &RecordBatch,
    right: &RecordBatch,
    left_keys: &[&str],
    right_keys: &[&str],
    how: JoinType,
) -> Result<RecordBatch> {
    if left_keys.is_empty() || right_keys.len() != left_keys.len() {
        return Err(Error::config(
            "left_keys and right_keys must be the same non-zero length",
        ));
    }

    let left_key_columns = resolve_columns(left, left_keys)?;
    let right_key_columns = resolve_columns(right, right_keys)?;

    for (i, (l, r)) in left_key_columns.iter().zip(&right_key_columns).enumerate() {
        if l.data_type() != r.data_type() {
            return Err(Error::type_mismatch(
                format!("{}/{}", left_keys[i], right_keys[i]),
                l.data_type().clone(),
                r.data_type().clone(),
            ));
        }
    }

    let fields: Vec<SortField> = left_key_columns
        .iter()
        .map(|c| SortField::new(c.data_type().clone()))
        .collect();
    let converter = RowConverter::new(fields).map_err(|e| Error::config(e.to_string()))?;
    let left_rows = converter
        .convert_columns(&left_key_columns)
        .map_err(|e| Error::config(e.to_string()))?;
    let right_rows = converter
        .convert_columns(&right_key_columns)
        .map_err(|e| Error::config(e.to_string()))?;

    let right_index = row_index(&right_rows);
    let mut right_matched = vec![false; right.num_rows()];
    let mut left_out: Vec<Option<u32>> = Vec::new();
    let mut right_out: Vec<Option<u32>> = Vec::new();

    for (li, lrow) in left_rows.iter().enumerate() {
        match right_index.get(&lrow.owned()) {
            Some(matches) => {
                for &ri in matches {
                    left_out.push(Some(li as u32));
                    right_out.push(Some(ri));
                    right_matched[ri as usize] = true;
                }
            }
            None if matches!(how, JoinType::Left | JoinType::Full) => {
                left_out.push(Some(li as u32));
                right_out.push(None);
            }
            None => {}
        }
    }

    if matches!(how, JoinType::Right | JoinType::Full) {
        for (ri, matched) in right_matched.iter().enumerate() {
            if !matched {
                left_out.push(None);
                right_out.push(Some(ri as u32));
            }
        }
    }

    let left_indices = UInt32Array::from(left_out);
    let right_indices = UInt32Array::from(right_out);

    let mut fields = Vec::with_capacity(left.num_columns() + right.num_columns());
    let mut columns = Vec::with_capacity(left.num_columns() + right.num_columns());

    for (field, column) in left.schema().fields().iter().zip(left.columns()) {
        let taken = take(column.as_ref(), &left_indices, None)
            .map_err(|e| Error::schema_mismatch(e.to_string()))?;
        fields.push(Field::new(field.name(), field.data_type().clone(), true));
        columns.push(taken);
    }
    for (field, column) in right.schema().fields().iter().zip(right.columns()) {
        let taken = take(column.as_ref(), &right_indices, None)
            .map_err(|e| Error::schema_mismatch(e.to_string()))?;
        let name = if left.schema().index_of(field.name()).is_ok() {
            format!("{}_right", field.name())
        } else {
            field.name().clone()
        };
        fields.push(Field::new(name, field.data_type().clone(), true));
        columns.push(taken);
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| Error::schema_mismatch(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_array::{Array, Int64Array, StringArray};
    use arrow_schema::DataType;

    fn users() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
            ],
        )
        .unwrap()
    }

    fn orders() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("user_id", DataType::Int64, false),
                Field::new("item", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 4])),
                Arc::new(StringArray::from(vec!["book", "pen", "ghost"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn inner_join_keeps_only_matches() {
        let out = join(&users(), &orders(), &["id"], &["user_id"], JoinType::Inner).unwrap();
        assert_eq!(out.num_rows(), 2); // alice x book, alice x pen
        let names = out.column(1).as_string::<i32>();
        assert_eq!(names.value(0), "alice");
        assert_eq!(names.value(1), "alice");
    }

    #[test]
    fn left_join_keeps_unmatched_left_rows() {
        let out = join(&users(), &orders(), &["id"], &["user_id"], JoinType::Left).unwrap();
        // alice x book, alice x pen, bob x null, carol x null
        assert_eq!(out.num_rows(), 4);
        let item = out.column(3).as_string::<i32>();
        assert!(item.is_null(2)); // bob
        assert!(item.is_null(3)); // carol
    }

    #[test]
    fn right_join_keeps_unmatched_right_rows() {
        let out = join(&users(), &orders(), &["id"], &["user_id"], JoinType::Right).unwrap();
        // alice x book, alice x pen, null x ghost
        assert_eq!(out.num_rows(), 3);
        let name = out.column(1).as_string::<i32>();
        assert!(name.is_null(2)); // the unmatched "ghost" order
    }

    #[test]
    fn full_join_keeps_every_unmatched_row_from_both_sides() {
        let out = join(&users(), &orders(), &["id"], &["user_id"], JoinType::Full).unwrap();
        // alice x book, alice x pen, bob x null, carol x null, null x ghost
        assert_eq!(out.num_rows(), 5);
    }

    #[test]
    fn colliding_column_names_get_suffixed() {
        let left = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let right = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let out = join(&left, &right, &["id"], &["id"], JoinType::Inner).unwrap();
        assert_eq!(out.schema().field(0).name(), "id");
        assert_eq!(out.schema().field(1).name(), "id_right");
    }

    #[test]
    fn key_type_mismatch_errors() {
        let left = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let right = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["1"]))],
        )
        .unwrap();
        assert!(join(&left, &right, &["id"], &["id"], JoinType::Inner).is_err());
    }

    #[test]
    fn unknown_column_errors() {
        assert!(
            join(
                &users(),
                &orders(),
                &["missing"],
                &["user_id"],
                JoinType::Inner
            )
            .is_err()
        );
    }

    #[test]
    fn composite_key_join() {
        let left = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();
        let right = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Utf8, false),
                Field::new("v", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(StringArray::from(vec!["x", "z"])),
                Arc::new(Int64Array::from(vec![100, 200])),
            ],
        )
        .unwrap();
        let out = join(&left, &right, &["a", "b"], &["a", "b"], JoinType::Inner).unwrap();
        assert_eq!(out.num_rows(), 1);
        assert_eq!(out.column(4).as_primitive::<Int64Type>().value(0), 100);
    }
}
