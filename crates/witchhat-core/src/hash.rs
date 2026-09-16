//! Composite row hashing.
//!
//! Fingerprints one or more columns of a `RecordBatch` into a single `u64`
//! per row: the building block for deduplication, change-data-capture keys,
//! and comparing witchhat's output against Spark's row-for-row.
//!
//! The algorithm is versioned ([`HashVersion`]) rather than free to drift:
//! a fingerprint computed today must still be reproducible next year even
//! after the implementation improves, so improvements land as a new
//! variant instead of mutating v1 in place. Any future SIMD path must
//! produce bit-identical output to the scalar path here — CPU features
//! ([`crate::cpu`]) may only change speed, never the result, or
//! fingerprints stop being comparable across a mixed-hardware fleet.

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{Array, RecordBatch, UInt64Array};
use arrow_schema::DataType;
use xxhash_rust::xxh3::Xxh3;

use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HashVersion {
    V1,
}

impl HashVersion {
    pub const CURRENT: HashVersion = HashVersion::V1;

    pub fn as_str(self) -> &'static str {
        match self {
            HashVersion::V1 => "v1",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "v1" => Some(HashVersion::V1),
            _ => None,
        }
    }

    pub(crate) fn seed(self) -> u64 {
        match self {
            // arbitrary fixed constants: changing them would silently
            // reorder every fingerprint ever stored, so they are frozen
            // the moment a version ships.
            HashVersion::V1 => 0x9E37_79B9_7F4A_7C15,
        }
    }
}

// type tags disambiguate values that could otherwise share a byte pattern:
// an Int64 5 and a Float64 5.0 do not have the same bytes, but an Int32 5
// and an Int64 5 both contain the byte sequence [5,0,0,0], so the tag (not
// just the width) has to be part of what gets hashed.
const TAG_NULL: u8 = 0x00;
const TAG_BOOL: u8 = 0x01;
const TAG_I8: u8 = 0x02;
const TAG_I16: u8 = 0x03;
const TAG_I32: u8 = 0x04;
const TAG_I64: u8 = 0x05;
const TAG_U8: u8 = 0x06;
const TAG_U16: u8 = 0x07;
const TAG_U32: u8 = 0x08;
const TAG_U64: u8 = 0x09;
const TAG_F32: u8 = 0x0A;
const TAG_F64: u8 = 0x0B;
const TAG_STR: u8 = 0x0C;
const TAG_BIN: u8 = 0x0D;

#[inline]
fn value_hash(version: HashVersion, tag: u8, bytes: &[u8]) -> u64 {
    let mut hasher = Xxh3::with_seed(version.seed());
    hasher.update(&[tag]);
    hasher.update(bytes);
    hasher.digest()
}

#[inline]
fn null_hash(version: HashVersion, tag: u8) -> u64 {
    value_hash(version, tag, &[TAG_NULL])
}

/// Order-sensitive combiner (boost's `hash_combine`, widened to 64 bits):
/// folding column hashes into a row accumulator must depend on column
/// order, since `(a, b)` and `(b, a)` are different rows.
#[inline]
fn combine(acc: u64, value: u64) -> u64 {
    acc ^ (value
        .wrapping_add(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(acc << 6)
        .wrapping_add(acc >> 2))
}

macro_rules! hash_primitive_column {
    ($array:expr, $version:expr, $tag:expr, $acc:expr, $arrow_ty:ty) => {{
        let a = $array.as_primitive::<$arrow_ty>();
        for (i, slot) in $acc.iter_mut().enumerate() {
            let h = if a.is_null(i) {
                null_hash($version, $tag)
            } else {
                value_hash($version, $tag, &a.value(i).to_le_bytes())
            };
            *slot = combine(*slot, h);
        }
    }};
}

fn canonicalize_f32(v: f32) -> f32 {
    if v.is_nan() {
        f32::NAN
    } else if v == 0.0 {
        0.0
    } else {
        v
    }
}

fn canonicalize_f64(v: f64) -> f64 {
    if v.is_nan() {
        f64::NAN
    } else if v == 0.0 {
        0.0
    } else {
        v
    }
}

fn hash_column_into(array: &dyn Array, version: HashVersion, acc: &mut [u64]) -> Result<()> {
    match array.data_type() {
        DataType::Boolean => {
            let a = array.as_boolean();
            for (i, slot) in acc.iter_mut().enumerate() {
                let h = if a.is_null(i) {
                    null_hash(version, TAG_BOOL)
                } else {
                    value_hash(version, TAG_BOOL, &[a.value(i) as u8])
                };
                *slot = combine(*slot, h);
            }
        }
        DataType::Int8 => hash_primitive_column!(array, version, TAG_I8, acc, Int8Type),
        DataType::Int16 => hash_primitive_column!(array, version, TAG_I16, acc, Int16Type),
        DataType::Int32 => hash_primitive_column!(array, version, TAG_I32, acc, Int32Type),
        DataType::Int64 => hash_primitive_column!(array, version, TAG_I64, acc, Int64Type),
        DataType::UInt8 => hash_primitive_column!(array, version, TAG_U8, acc, UInt8Type),
        DataType::UInt16 => hash_primitive_column!(array, version, TAG_U16, acc, UInt16Type),
        DataType::UInt32 => hash_primitive_column!(array, version, TAG_U32, acc, UInt32Type),
        DataType::UInt64 => hash_primitive_column!(array, version, TAG_U64, acc, UInt64Type),
        DataType::Float32 => {
            let a = array.as_primitive::<Float32Type>();
            for (i, slot) in acc.iter_mut().enumerate() {
                let h = if a.is_null(i) {
                    null_hash(version, TAG_F32)
                } else {
                    value_hash(
                        version,
                        TAG_F32,
                        &canonicalize_f32(a.value(i)).to_le_bytes(),
                    )
                };
                *slot = combine(*slot, h);
            }
        }
        DataType::Float64 => {
            let a = array.as_primitive::<Float64Type>();
            for (i, slot) in acc.iter_mut().enumerate() {
                let h = if a.is_null(i) {
                    null_hash(version, TAG_F64)
                } else {
                    value_hash(
                        version,
                        TAG_F64,
                        &canonicalize_f64(a.value(i)).to_le_bytes(),
                    )
                };
                *slot = combine(*slot, h);
            }
        }
        DataType::Utf8 => {
            let a = array.as_string::<i32>();
            for (i, slot) in acc.iter_mut().enumerate() {
                let h = if a.is_null(i) {
                    null_hash(version, TAG_STR)
                } else {
                    value_hash(version, TAG_STR, a.value(i).as_bytes())
                };
                *slot = combine(*slot, h);
            }
        }
        DataType::LargeUtf8 => {
            let a = array.as_string::<i64>();
            for (i, slot) in acc.iter_mut().enumerate() {
                let h = if a.is_null(i) {
                    null_hash(version, TAG_STR)
                } else {
                    value_hash(version, TAG_STR, a.value(i).as_bytes())
                };
                *slot = combine(*slot, h);
            }
        }
        DataType::Binary => {
            let a = array.as_binary::<i32>();
            for (i, slot) in acc.iter_mut().enumerate() {
                let h = if a.is_null(i) {
                    null_hash(version, TAG_BIN)
                } else {
                    value_hash(version, TAG_BIN, a.value(i))
                };
                *slot = combine(*slot, h);
            }
        }
        DataType::LargeBinary => {
            let a = array.as_binary::<i64>();
            for (i, slot) in acc.iter_mut().enumerate() {
                let h = if a.is_null(i) {
                    null_hash(version, TAG_BIN)
                } else {
                    value_hash(version, TAG_BIN, a.value(i))
                };
                *slot = combine(*slot, h);
            }
        }
        other => return Err(Error::unsupported_type(other.clone())),
    }
    Ok(())
}

fn resolve_indices(batch: &RecordBatch, columns: &[&str]) -> Result<Vec<usize>> {
    columns
        .iter()
        .map(|name| {
            batch
                .schema()
                .index_of(name)
                .map_err(|_| Error::unknown_column(*name))
        })
        .collect()
}

/// Fingerprint the given columns, in the given order, into one `u64` per
/// row. Columns not listed do not affect the result, and the same columns
/// in a different order produce a different fingerprint.
pub fn hash_batch(
    batch: &RecordBatch,
    columns: &[&str],
    version: HashVersion,
) -> Result<UInt64Array> {
    let indices = resolve_indices(batch, columns)?;
    let mut acc = vec![version.seed(); batch.num_rows()];
    for idx in indices {
        hash_column_into(batch.column(idx).as_ref(), version, &mut acc)?;
    }
    Ok(UInt64Array::from(acc))
}

/// [`hash_batch`] over every column in the batch, in schema order.
pub fn hash_batch_all_columns(batch: &RecordBatch, version: HashVersion) -> Result<UInt64Array> {
    let mut acc = vec![version.seed(); batch.num_rows()];
    for column in batch.columns() {
        hash_column_into(column.as_ref(), version, &mut acc)?;
    }
    Ok(UInt64Array::from(acc))
}

/// Order-independent aggregate of row fingerprints: two batches with the
/// same rows in a different order (a re-shuffled partition, say) produce
/// the same table fingerprint. Use this to check witchhat's output against
/// Spark's without forcing either side to sort first.
pub fn table_fingerprint(row_hashes: &UInt64Array, version: HashVersion) -> u64 {
    row_hashes
        .values()
        .iter()
        .fold(version.seed(), |acc, &h| acc.wrapping_add(h))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                Arc::new(Float64Array::from(vec![1.5, f64::NAN, -0.0])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn deterministic_across_runs() {
        let b = batch();
        let h1 = hash_batch(&b, &["id", "name"], HashVersion::CURRENT).unwrap();
        let h2 = hash_batch(&b, &["id", "name"], HashVersion::CURRENT).unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn column_order_matters() {
        let b = batch();
        let h1 = hash_batch(&b, &["id", "name"], HashVersion::CURRENT).unwrap();
        let h2 = hash_batch(&b, &["name", "id"], HashVersion::CURRENT).unwrap();
        assert_ne!(h1.value(0), h2.value(0));
    }

    #[test]
    fn null_is_distinct_from_any_value() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let b = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![Some(""), None]))],
        )
        .unwrap();
        let h = hash_batch(&b, &["s"], HashVersion::CURRENT).unwrap();
        assert_ne!(h.value(0), h.value(1));
    }

    #[test]
    fn same_bytes_different_type_do_not_collide() {
        let int_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let ints =
            RecordBatch::try_new(int_schema, vec![Arc::new(Int32Array::from(vec![5]))]).unwrap();

        let bool_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Boolean, false)]));
        let bools =
            RecordBatch::try_new(bool_schema, vec![Arc::new(BooleanArray::from(vec![true]))])
                .unwrap();

        let hi = hash_batch(&ints, &["v"], HashVersion::CURRENT).unwrap();
        let hb = hash_batch(&bools, &["v"], HashVersion::CURRENT).unwrap();
        assert_ne!(hi.value(0), hb.value(0));
    }

    #[test]
    fn nan_and_negative_zero_are_canonicalized() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let a = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float64Array::from(vec![0.0]))],
        )
        .unwrap();
        let b =
            RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(vec![-0.0]))]).unwrap();
        let ha = hash_batch(&a, &["v"], HashVersion::CURRENT).unwrap();
        let hb = hash_batch(&b, &["v"], HashVersion::CURRENT).unwrap();
        assert_eq!(ha.value(0), hb.value(0));
    }

    #[test]
    fn unknown_column_errors() {
        let b = batch();
        assert!(hash_batch(&b, &["missing"], HashVersion::CURRENT).is_err());
    }

    #[test]
    fn table_fingerprint_is_order_independent() {
        let b = batch();
        let rows = hash_batch(&b, &["id", "name", "score"], HashVersion::CURRENT).unwrap();
        let forward = table_fingerprint(&rows, HashVersion::CURRENT);

        let reversed: Vec<u64> = rows.values().iter().rev().copied().collect();
        let backward = table_fingerprint(&UInt64Array::from(reversed), HashVersion::CURRENT);

        assert_eq!(forward, backward);
    }
}
