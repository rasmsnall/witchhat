"""High-performance data transformation kernels for Spark/Databricks workloads.

The public surface is versioned composite hashing: :func:`hash_rows` fingerprints named
columns of an Arrow batch into one uint64 per row, :func:`hash_rows_all_columns` does the
same over every column, and :func:`table_fingerprint` folds a batch of row hashes into one
order-independent value for comparing witchhat's output against Spark's without sorting
either side. :func:`schema_fingerprint` fingerprints a schema's shape. :func:`cpu_features`
reports what SIMD dispatch this machine would get from a future accelerated kernel.

Every function accepts any object implementing the Arrow C Data / pyarrow interface
(``pyarrow``, ``polars``, ...), not just ``pyarrow`` specifically.
"""

from ._witchhat import (
    CpuFeatures,
    cpu_features,
    hash_rows,
    hash_rows_all_columns,
    schema_fingerprint,
    table_fingerprint,
)

__all__ = [
    "hash_rows",
    "hash_rows_all_columns",
    "table_fingerprint",
    "schema_fingerprint",
    "cpu_features",
    "CpuFeatures",
]
__version__ = "0.1.0"
