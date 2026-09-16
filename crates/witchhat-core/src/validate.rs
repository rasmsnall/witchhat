//! Schema validation: comparing an actual schema against an expected one.
//!
//! Complements [`crate::schema::schema_fingerprint`], which only says whether two
//! schemas match, not how they differ. [`validate_schema`] returns the actual
//! difference, so a caller can decide what to do about a mismatch (fail loudly, warn
//! and coerce, or accept an additive change) rather than being told only "yes" or "no".

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};

/// Controls how strict [`validate_schema`] is.
#[derive(Debug, Clone, Copy, Default)]
pub struct ValidateSchemaOptions {
    /// Accept `actual` having a wider numeric type than `expected` for the same
    /// column (`Int32` -> `Int64`, `Float32` -> `Float64`, and so on within a
    /// signedness class). A narrower actual type, a cross-signedness change
    /// (`Int32` -> `UInt32`), or an integer-to-float change is never accepted
    /// regardless of this flag: those can silently change what a value means, not
    /// just how many bits hold it.
    pub allow_numeric_widening: bool,
}

/// One column present in both schemas whose type differs and was not an accepted
/// widening.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetypedColumn {
    /// The column's name.
    pub column: Arc<str>,
    /// The type `expected` declared.
    pub expected: DataType,
    /// The type `actual` declared.
    pub actual: DataType,
}

/// One column present in both schemas whose nullability tightened in a way that could
/// break a caller relying on `expected`'s promise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NullabilityChange {
    /// The column's name.
    pub column: Arc<str>,
    /// Whether `expected` allows null.
    pub expected_nullable: bool,
    /// Whether `actual` allows null.
    pub actual_nullable: bool,
}

/// The result of comparing an actual schema against an expected one. Empty
/// ([`SchemaDiff::is_empty`]) when the two agree under the given
/// [`ValidateSchemaOptions`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaDiff {
    /// Column names in `expected` that `actual` does not have.
    pub missing: Vec<Arc<str>>,
    /// Column names in `actual` that `expected` does not have.
    pub unexpected: Vec<Arc<str>>,
    /// Columns present in both whose type differs.
    pub retyped: Vec<RetypedColumn>,
    /// Columns present in both whose nullability tightened.
    pub nullability: Vec<NullabilityChange>,
}

impl SchemaDiff {
    /// Whether `actual` and `expected` agreed on every point [`validate_schema`]
    /// checks.
    pub fn is_empty(&self) -> bool {
        self.missing.is_empty()
            && self.unexpected.is_empty()
            && self.retyped.is_empty()
            && self.nullability.is_empty()
    }

    /// Whether the difference is one a caller most likely cannot safely ignore: a
    /// missing column, a retyped column, or a nullability tightening.
    ///
    /// An [`unexpected`](SchemaDiff::unexpected) column alone is not breaking:
    /// additive schema evolution (a new column showing up) does not usually
    /// invalidate code written against the old, narrower schema, so it is reported
    /// without being counted as a break.
    pub fn is_breaking(&self) -> bool {
        !self.missing.is_empty() || !self.retyped.is_empty() || !self.nullability.is_empty()
    }
}

fn is_numeric_widening(expected: &DataType, actual: &DataType) -> bool {
    use DataType::{Float32, Float64, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64};
    matches!(
        (expected, actual),
        (Int8, Int16 | Int32 | Int64)
            | (Int16, Int32 | Int64)
            | (Int32, Int64)
            | (UInt8, UInt16 | UInt32 | UInt64)
            | (UInt16, UInt32 | UInt64)
            | (UInt32, UInt64)
            | (Float32, Float64)
    )
}

fn types_compatible(
    expected: &DataType,
    actual: &DataType,
    options: ValidateSchemaOptions,
) -> bool {
    expected == actual || (options.allow_numeric_widening && is_numeric_widening(expected, actual))
}

/// A promise of "never null" is the only direction that can break a caller: code
/// written against a non-nullable `expected` may skip a null check that `actual` then
/// needs. The reverse (`expected` nullable, `actual` non-nullable) only means `actual`
/// guarantees more than was promised, which is safe to accept.
fn nullability_compatible(expected_nullable: bool, actual_nullable: bool) -> bool {
    expected_nullable || !actual_nullable
}

fn field_name_index(schema: &Schema) -> HashMap<&str, &Field> {
    schema
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.as_ref()))
        .collect()
}

/// Compares `actual` against `expected` and returns their difference.
///
/// Columns are matched by name, case-sensitively. For a column present in both:
///
/// - its type must match exactly, unless `options.allow_numeric_widening` accepts the
///   specific widening seen (see [`ValidateSchemaOptions`]);
/// - `actual`'s nullability must be compatible with `expected`'s: `actual` may be
///   nullable when `expected` is too, or non-nullable when `expected` allows null, but
///   not nullable when `expected` declares the column non-nullable. A promise of
///   "never null" is the only direction that can break a caller.
///
/// A column in `expected` but not `actual` is [`SchemaDiff::missing`]; the reverse is
/// [`SchemaDiff::unexpected`]. Duplicate field names within one schema are not
/// rejected; the last field with a given name wins, matching how a `HashMap` built
/// from the field list would behave.
///
/// # Panics
///
/// Does not panic. Not async; runs in time and memory linear in the field counts of
/// both schemas, with no I/O.
///
/// # Examples
///
/// ```
/// use arrow_schema::{DataType, Field, Schema};
/// use witchhat_core::{ValidateSchemaOptions, validate_schema};
///
/// let expected = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
/// let actual = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
///
/// // strict by default: a retyped column is reported even though it only widened
/// assert!(!validate_schema(&actual, &expected, ValidateSchemaOptions::default()).is_empty());
///
/// let widening = ValidateSchemaOptions { allow_numeric_widening: true };
/// assert!(validate_schema(&actual, &expected, widening).is_empty());
/// ```
pub fn validate_schema(
    actual: &Schema,
    expected: &Schema,
    options: ValidateSchemaOptions,
) -> SchemaDiff {
    let actual_fields = field_name_index(actual);

    let mut seen: HashSet<&str> = HashSet::new();
    let mut missing = Vec::new();
    let mut retyped = Vec::new();
    let mut nullability = Vec::new();

    for expected_field in expected.fields() {
        seen.insert(expected_field.name().as_str());
        match actual_fields.get(expected_field.name().as_str()) {
            None => missing.push(Arc::from(expected_field.name().as_str())),
            Some(actual_field) => {
                if !types_compatible(
                    expected_field.data_type(),
                    actual_field.data_type(),
                    options,
                ) {
                    retyped.push(RetypedColumn {
                        column: Arc::from(expected_field.name().as_str()),
                        expected: expected_field.data_type().clone(),
                        actual: actual_field.data_type().clone(),
                    });
                }
                if !nullability_compatible(expected_field.is_nullable(), actual_field.is_nullable())
                {
                    nullability.push(NullabilityChange {
                        column: Arc::from(expected_field.name().as_str()),
                        expected_nullable: expected_field.is_nullable(),
                        actual_nullable: actual_field.is_nullable(),
                    });
                }
            }
        }
    }

    let unexpected = actual
        .fields()
        .iter()
        .filter(|f| !seen.contains(f.name().as_str()))
        .map(|f| Arc::from(f.name().as_str()))
        .collect();

    SchemaDiff {
        missing,
        unexpected,
        retyped,
        nullability,
    }
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
    fn identical_schemas_are_empty() {
        let s = schema(vec![("id", DataType::Int64, false)]);
        let diff = validate_schema(&s, &s, ValidateSchemaOptions::default());
        assert!(diff.is_empty());
        assert!(!diff.is_breaking());
    }

    #[test]
    fn missing_column_is_breaking() {
        let expected = schema(vec![
            ("id", DataType::Int64, false),
            ("email", DataType::Utf8, true),
        ]);
        let actual = schema(vec![("id", DataType::Int64, false)]);
        let diff = validate_schema(&actual, &expected, ValidateSchemaOptions::default());
        assert_eq!(diff.missing.as_slice(), &[Arc::from("email")]);
        assert!(diff.is_breaking());
    }

    #[test]
    fn unexpected_column_is_reported_but_not_breaking() {
        let expected = schema(vec![("id", DataType::Int64, false)]);
        let actual = schema(vec![
            ("id", DataType::Int64, false),
            ("extra", DataType::Utf8, true),
        ]);
        let diff = validate_schema(&actual, &expected, ValidateSchemaOptions::default());
        assert_eq!(diff.unexpected.as_slice(), &[Arc::from("extra")]);
        assert!(!diff.is_empty());
        assert!(!diff.is_breaking());
    }

    #[test]
    fn retype_is_rejected_by_default() {
        let expected = schema(vec![("id", DataType::Int32, false)]);
        let actual = schema(vec![("id", DataType::Int64, false)]);
        let diff = validate_schema(&actual, &expected, ValidateSchemaOptions::default());
        assert_eq!(diff.retyped.len(), 1);
        assert_eq!(diff.retyped[0].expected, DataType::Int32);
        assert_eq!(diff.retyped[0].actual, DataType::Int64);
        assert!(diff.is_breaking());
    }

    #[test]
    fn numeric_widening_accepted_only_when_opted_in() {
        let expected = schema(vec![("id", DataType::Int32, false)]);
        let actual = schema(vec![("id", DataType::Int64, false)]);
        let widening = ValidateSchemaOptions {
            allow_numeric_widening: true,
        };
        assert!(validate_schema(&actual, &expected, widening).is_empty());
        assert!(!validate_schema(&actual, &expected, ValidateSchemaOptions::default()).is_empty());
    }

    #[test]
    fn narrowing_is_never_accepted() {
        let expected = schema(vec![("id", DataType::Int64, false)]);
        let actual = schema(vec![("id", DataType::Int32, false)]);
        let widening = ValidateSchemaOptions {
            allow_numeric_widening: true,
        };
        assert!(!validate_schema(&actual, &expected, widening).is_empty());
    }

    #[test]
    fn cross_signedness_is_never_widening() {
        let expected = schema(vec![("id", DataType::Int32, false)]);
        let actual = schema(vec![("id", DataType::UInt32, false)]);
        let widening = ValidateSchemaOptions {
            allow_numeric_widening: true,
        };
        assert!(!validate_schema(&actual, &expected, widening).is_empty());
    }

    #[test]
    fn tightened_nullability_is_breaking() {
        let expected = schema(vec![("id", DataType::Int64, false)]);
        let actual = schema(vec![("id", DataType::Int64, true)]);
        let diff = validate_schema(&actual, &expected, ValidateSchemaOptions::default());
        assert_eq!(diff.nullability.len(), 1);
        assert!(diff.is_breaking());
    }

    #[test]
    fn relaxed_nullability_is_fine() {
        let expected = schema(vec![("id", DataType::Int64, true)]);
        let actual = schema(vec![("id", DataType::Int64, false)]);
        let diff = validate_schema(&actual, &expected, ValidateSchemaOptions::default());
        assert!(diff.is_empty());
    }
}
