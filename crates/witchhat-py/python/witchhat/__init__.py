"""High-performance data transformation kernels for Spark/Databricks workloads.

Composite hashing: :func:`hash_rows` fingerprints named columns of an Arrow batch into
one uint64 per row, :func:`hash_rows_all_columns` does the same over every column, and
:func:`table_fingerprint` folds a batch of row hashes into one order-independent value
for comparing witchhat's output against Spark's without sorting either side.
:func:`schema_fingerprint` fingerprints a schema's shape.

Schema validation: :func:`validate_schema` compares an actual schema against an expected
one and returns a :class:`SchemaDiff` describing exactly how they differ (missing,
unexpected, retyped or renullable columns), rather than a bare yes/no.

JSON normalization: :func:`normalize_json` parses a column of JSON strings into a fixed
Arrow schema, returning both the batch and a :class:`NormalizeStats` reporting malformed
rows and type mismatches.

Regex cleanup: :func:`clean_with_preset` applies one of witchhat's named, versioned
presets to a string column; :func:`clean_with_rules` applies caller-supplied
find-and-replace rules directly.

Output equivalence testing: :func:`check_equivalence` compares two batches (schema, row
count, and an order-independent fingerprint of their rows) and returns an
:class:`EquivalenceReport`.

Native transformations: :func:`drop_duplicates` (equivalent to Spark's
``dropDuplicates``, exact even under a hash collision), :func:`join` (inner/left/
right/full, matched on key columns, excluding null keys the same way Spark does;
:func:`join_null_safe` is the explicit opt-in for null-matches-null), and
:func:`aggregate` (group by columns, reduce with count/sum/mean/min/max, each summed
and compared at its own exact numeric precision, never downcast through ``f64``).

:func:`cpu_features` reports what SIMD dispatch this machine would get from a future
accelerated kernel.

Every function accepts any object implementing the Arrow C Data / pyarrow interface
(``pyarrow``, ``polars``, ...), not just ``pyarrow`` specifically.

Every function below is optionally instrumented: call :func:`witchhat.metrics.enable`
to have each one emit a JSON event (function name, row counts, duration, throughput) to
a configurable sink. Disabled by default and effectively free when disabled; see
:mod:`witchhat.metrics`.
"""

from . import metrics
from ._witchhat import (
    CpuFeatures,
    EquivalenceReport,
    NormalizeStats,
    SchemaDiff,
    cpu_features,
)
from ._witchhat import aggregate as _aggregate
from ._witchhat import check_equivalence as _check_equivalence
from ._witchhat import clean_with_preset as _clean_with_preset
from ._witchhat import clean_with_rules as _clean_with_rules
from ._witchhat import drop_duplicates as _drop_duplicates
from ._witchhat import hash_rows as _hash_rows
from ._witchhat import hash_rows_all_columns as _hash_rows_all_columns
from ._witchhat import join as _join
from ._witchhat import join_null_safe as _join_null_safe
from ._witchhat import normalize_json as _normalize_json
from ._witchhat import schema_fingerprint as _schema_fingerprint
from ._witchhat import table_fingerprint as _table_fingerprint
from ._witchhat import validate_schema as _validate_schema


def hash_rows(batch, columns, version="v1"):
    if not metrics.is_enabled():
        return _hash_rows(batch, columns, version)
    with metrics.measure("hash_rows", columns=list(columns), version=version) as event:
        event["rows_in"] = metrics.row_count(batch)
        result = _hash_rows(batch, columns, version)
        event["rows_out"] = metrics.row_count(result)
        return result


hash_rows.__doc__ = _hash_rows.__doc__


def hash_rows_all_columns(batch, version="v1"):
    if not metrics.is_enabled():
        return _hash_rows_all_columns(batch, version)
    with metrics.measure("hash_rows_all_columns", version=version) as event:
        event["rows_in"] = metrics.row_count(batch)
        result = _hash_rows_all_columns(batch, version)
        event["rows_out"] = metrics.row_count(result)
        return result


hash_rows_all_columns.__doc__ = _hash_rows_all_columns.__doc__


def table_fingerprint(row_hashes, version="v1"):
    if not metrics.is_enabled():
        return _table_fingerprint(row_hashes, version)
    with metrics.measure("table_fingerprint", version=version) as event:
        event["rows_in"] = metrics.row_count(row_hashes)
        return _table_fingerprint(row_hashes, version)


table_fingerprint.__doc__ = _table_fingerprint.__doc__


def schema_fingerprint(schema, version="v1"):
    if not metrics.is_enabled():
        return _schema_fingerprint(schema, version)
    with metrics.measure("schema_fingerprint", version=version):
        return _schema_fingerprint(schema, version)


schema_fingerprint.__doc__ = _schema_fingerprint.__doc__


def validate_schema(actual, expected, allow_numeric_widening=False):
    if not metrics.is_enabled():
        return _validate_schema(actual, expected, allow_numeric_widening)
    with metrics.measure(
        "validate_schema", allow_numeric_widening=allow_numeric_widening
    ) as event:
        result = _validate_schema(actual, expected, allow_numeric_widening)
        event["is_empty"] = result.is_empty()
        event["is_breaking"] = result.is_breaking()
        return result


validate_schema.__doc__ = _validate_schema.__doc__


def normalize_json(json, schema, version="v1"):
    if not metrics.is_enabled():
        return _normalize_json(json, schema, version)
    with metrics.measure("normalize_json", version=version) as event:
        event["rows_in"] = metrics.row_count(json)
        batch, stats = _normalize_json(json, schema, version)
        event["rows_out"] = metrics.row_count(batch)
        event["rows_malformed"] = stats.rows_malformed
        return batch, stats


normalize_json.__doc__ = _normalize_json.__doc__


def clean_with_preset(input, name, version="v1"):
    if not metrics.is_enabled():
        return _clean_with_preset(input, name, version)
    with metrics.measure("clean_with_preset", name=name, version=version) as event:
        event["rows_in"] = metrics.row_count(input)
        result = _clean_with_preset(input, name, version)
        event["rows_out"] = metrics.row_count(result)
        return result


clean_with_preset.__doc__ = _clean_with_preset.__doc__


def clean_with_rules(input, rules):
    if not metrics.is_enabled():
        return _clean_with_rules(input, rules)
    with metrics.measure("clean_with_rules", rule_count=len(rules)) as event:
        event["rows_in"] = metrics.row_count(input)
        result = _clean_with_rules(input, rules)
        event["rows_out"] = metrics.row_count(result)
        return result


clean_with_rules.__doc__ = _clean_with_rules.__doc__


def check_equivalence(
    actual, expected, columns=None, allow_numeric_widening=False, hash_version="v1"
):
    if not metrics.is_enabled():
        return _check_equivalence(
            actual, expected, columns, allow_numeric_widening, hash_version
        )
    with metrics.measure("check_equivalence", hash_version=hash_version) as event:
        event["rows_in"] = metrics.row_count(actual)
        result = _check_equivalence(
            actual, expected, columns, allow_numeric_widening, hash_version
        )
        event["is_equivalent"] = result.is_equivalent()
        return result


check_equivalence.__doc__ = _check_equivalence.__doc__


def drop_duplicates(batch, columns, version="v1"):
    if not metrics.is_enabled():
        return _drop_duplicates(batch, columns, version)
    with metrics.measure(
        "drop_duplicates", columns=list(columns), version=version
    ) as event:
        event["rows_in"] = metrics.row_count(batch)
        result = _drop_duplicates(batch, columns, version)
        event["rows_out"] = metrics.row_count(result)
        return result


drop_duplicates.__doc__ = _drop_duplicates.__doc__


def join(left, right, left_keys, right_keys, how="inner"):
    if not metrics.is_enabled():
        return _join(left, right, left_keys, right_keys, how)
    with metrics.measure(
        "join", left_keys=list(left_keys), right_keys=list(right_keys), how=how
    ) as event:
        event["rows_in"] = metrics.row_count(left)
        event["rows_in_right"] = metrics.row_count(right)
        result = _join(left, right, left_keys, right_keys, how)
        event["rows_out"] = metrics.row_count(result)
        return result


join.__doc__ = _join.__doc__


def join_null_safe(left, right, left_keys, right_keys, how="inner"):
    if not metrics.is_enabled():
        return _join_null_safe(left, right, left_keys, right_keys, how)
    with metrics.measure(
        "join_null_safe", left_keys=list(left_keys), right_keys=list(right_keys), how=how
    ) as event:
        event["rows_in"] = metrics.row_count(left)
        event["rows_in_right"] = metrics.row_count(right)
        result = _join_null_safe(left, right, left_keys, right_keys, how)
        event["rows_out"] = metrics.row_count(result)
        return result


join_null_safe.__doc__ = _join_null_safe.__doc__


def aggregate(batch, group_by, aggregations):
    if not metrics.is_enabled():
        return _aggregate(batch, group_by, aggregations)
    with metrics.measure(
        "aggregate", group_by=list(group_by), aggregation_count=len(aggregations)
    ) as event:
        event["rows_in"] = metrics.row_count(batch)
        result = _aggregate(batch, group_by, aggregations)
        event["rows_out"] = metrics.row_count(result)
        return result


aggregate.__doc__ = _aggregate.__doc__


__all__ = [
    "hash_rows",
    "hash_rows_all_columns",
    "table_fingerprint",
    "schema_fingerprint",
    "validate_schema",
    "SchemaDiff",
    "normalize_json",
    "NormalizeStats",
    "clean_with_preset",
    "clean_with_rules",
    "check_equivalence",
    "EquivalenceReport",
    "drop_duplicates",
    "join",
    "join_null_safe",
    "aggregate",
    "cpu_features",
    "CpuFeatures",
]
__version__ = "0.1.0"
