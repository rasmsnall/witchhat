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
``dropDuplicates``), :func:`join` (inner/left/right/full, matched on key columns), and
:func:`aggregate` (group by columns, reduce with count/sum/mean/min/max).

:func:`cpu_features` reports what SIMD dispatch this machine would get from a future
accelerated kernel.

Every function accepts any object implementing the Arrow C Data / pyarrow interface
(``pyarrow``, ``polars``, ...), not just ``pyarrow`` specifically.
"""

from ._witchhat import (
    CpuFeatures,
    EquivalenceReport,
    NormalizeStats,
    SchemaDiff,
    aggregate,
    check_equivalence,
    clean_with_preset,
    clean_with_rules,
    cpu_features,
    drop_duplicates,
    hash_rows,
    hash_rows_all_columns,
    join,
    normalize_json,
    schema_fingerprint,
    table_fingerprint,
    validate_schema,
)

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
    "aggregate",
    "cpu_features",
    "CpuFeatures",
]
__version__ = "0.1.0"
