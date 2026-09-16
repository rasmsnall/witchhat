"""Databricks/Spark integration: calling witchhat's kernels from a
``pyspark.sql.DataFrame`` via ``mapInArrow``.

witchhat itself has no dependency on pyspark (Databricks already provides it, and
nothing else in the ``witchhat`` package should require it to import). Every
function here imports pyspark lazily and raises a clear ``ImportError`` if it is
missing, so ``import witchhat`` never fails outside Databricks, but ``import
witchhat.spark`` does if pyspark is not installed.

witchhat is not a distributed engine (see ``docs/architecture.md`` Chapter I): every
kernel operates on one ``RecordBatch`` at a time, with no coordination across
partitions. This module is the bridge, not a new engine of its own, and it is
explicit about what each wrapper does and does not guarantee across a whole
DataFrame:

- Row-local kernels (:func:`hash_rows`, :func:`clean_with_preset`,
  :func:`clean_with_rules`) are correct regardless of partitioning: they stream
  one input batch to one output batch.
- Partition-coordinating kernels (:func:`drop_duplicates`, :func:`aggregate`) need
  every row that belongs together to already be in the same partition. Each
  buffers its whole partition into one ``RecordBatch`` first (`mapInArrow` can
  hand back more than one batch per partition; deduplicating or aggregating each
  separately would silently miss cross-batch duplicates or split a group's rows
  across two output rows), and repartitions by the relevant columns by default to
  make that guarantee hold.
- :func:`broadcast_join` joins each partition against one small side already
  collected to the driver, mirroring Spark's own broadcast-join optimization. A
  large-large shuffle join is out of scope: Spark's native ``DataFrame.join`` is
  the right tool for that, not witchhat.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable, Iterator

import pyarrow as pa

import witchhat as _witchhat

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


def _require_pyspark() -> None:
    try:
        import pyspark  # noqa: F401
    except ImportError as exc:  # pragma: no cover - exercised only without pyspark
        raise ImportError(
            "witchhat.spark requires pyspark, which is not installed here. "
            "On Databricks it is already provided by the runtime; elsewhere, "
            "`pip install pyspark`."
        ) from exc


def _with_field(schema: "StructType", name: str, data_type, nullable: bool) -> "StructType":
    """Returns a new `StructType` with one field appended, without touching `schema`.

    `StructType.add` mutates and returns `self` rather than a copy. Calling it on
    a live `DataFrame`'s own `df.schema` (as `df.schema.add(...)`) silently
    corrupts that DataFrame: `df.columns` afterward reports the added field even
    though the underlying JVM plan was never given it, and a later `mapInArrow`
    call on that same `df` fails with an unresolved-column error pointing at the
    field this function was trying to add in the first place. Every output-schema
    helper in this module goes through here instead of `.add()` directly.
    """
    from pyspark.sql.types import StructField, StructType

    return StructType(list(schema.fields) + [StructField(name, data_type, nullable=nullable)])


def _u64_to_i64(array: pa.Array) -> pa.Array:
    """Bit-reinterprets a `uint64` array as `int64`.

    Spark/Arrow interop has no unsigned integer type (`pyspark.sql.pandas.types.
    from_arrow_schema` raises on `uint64`), so a witchhat row hash cannot be
    returned to Spark as-is. `Array.view` reinterprets the same bits rather than
    converting the value, so equality, joins and group-bys on the result behave
    identically to the original `uint64`; only the printed decimal can look
    negative for a hash whose high bit happens to be set.
    """
    return array.view(pa.int64())


def to_arrow_schema(schema: "DataFrame | StructType") -> pa.Schema:
    """Converts a Spark schema (or a DataFrame's `.schema`) to a `pyarrow.Schema`,
    via pyspark's own conversion utility.

    Parameters
    ----------
    schema:
        A `pyspark.sql.types.StructType`, or a `DataFrame` (its `.schema` is used).

    Returns
    -------
    pyarrow.Schema

    Raises
    ------
    ImportError
        pyspark is not installed.
    pyspark.errors.PySparkTypeError
        `schema` contains a Spark type with no Arrow equivalent.
    """
    _require_pyspark()
    from pyspark.sql.pandas.types import to_arrow_schema as _to_arrow_schema
    from pyspark.sql.types import StructType

    if not isinstance(schema, StructType):
        schema = schema.schema
    return _to_arrow_schema(schema)


def map_in_arrow(df: "DataFrame", func, schema: "StructType | str") -> "DataFrame":
    """Thin, documented wrapper around `DataFrame.mapInArrow`.

    `func` is called once per partition with an iterator of `pyarrow.RecordBatch`
    and must yield `pyarrow.RecordBatch`. Every other function in this module is
    built on this; call it directly for anything not already wrapped below.

    Runs `func` independently, once per partition, with no coordination across
    partitions: an operation that needs rows from more than one partition to be
    correct (deduplication, aggregation) must ensure the DataFrame is already
    partitioned so that related rows land together, e.g. via
    `df.repartition(*columns)`, before calling this.

    Raises
    ------
    ImportError
        pyspark is not installed.
    """
    _require_pyspark()
    return df.mapInArrow(func, schema)


def hash_rows(
    df: "DataFrame",
    columns: Iterable[str],
    output_column: str = "row_hash",
    version: str = "v1",
) -> "DataFrame":
    """Adds `output_column`, one witchhat row hash per row, computed over
    `columns`. See `witchhat.hash_rows`.

    Row-local: correct regardless of how `df` is partitioned, and streams one
    input batch to one output batch with no buffering. See the module docstring
    for why the stored value is a bit-reinterpreted `int64`, not `uint64`.
    """
    from pyspark.sql.types import LongType

    columns = list(columns)
    out_schema = _with_field(df.schema, output_column, LongType(), nullable=False)

    def process(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        for batch in batches:
            hashes = _u64_to_i64(_witchhat.hash_rows(batch, columns, version))
            out_fields = list(batch.schema) + [pa.field(output_column, pa.int64(), nullable=False)]
            yield pa.RecordBatch.from_arrays(list(batch.columns) + [hashes], schema=pa.schema(out_fields))

    return map_in_arrow(df, process, out_schema)


def clean_with_preset(
    df: "DataFrame",
    column: str,
    name: str,
    output_column: str | None = None,
    version: str = "v1",
) -> "DataFrame":
    """Applies a named witchhat cleanup preset to `column`, writing the result to
    `output_column` (`column` itself, in place, by default). See
    `witchhat.clean_with_preset`. Row-local: streams one input batch to one output
    batch with no buffering.
    """
    output_column = output_column or column
    if output_column == column:
        out_schema = df.schema
    else:
        from pyspark.sql.types import StringType

        out_schema = _with_field(df.schema, output_column, StringType(), nullable=True)

    def process(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        for batch in batches:
            idx = batch.schema.get_field_index(column)
            cleaned = _witchhat.clean_with_preset(batch.column(idx), name, version)
            if output_column == column:
                columns = list(batch.columns)
                columns[idx] = cleaned
                yield pa.RecordBatch.from_arrays(columns, schema=batch.schema)
            else:
                out_fields = list(batch.schema) + [pa.field(output_column, pa.string(), nullable=True)]
                yield pa.RecordBatch.from_arrays(list(batch.columns) + [cleaned], schema=pa.schema(out_fields))

    return map_in_arrow(df, process, out_schema)


def clean_with_rules(
    df: "DataFrame",
    column: str,
    rules: list[tuple[str, str]],
    output_column: str | None = None,
) -> "DataFrame":
    """Applies caller-supplied `(pattern, replacement)` regex rules to `column`.
    See `witchhat.clean_with_rules`. Same shape as :func:`clean_with_preset`
    otherwise, including row-local, unbuffered streaming.
    """
    output_column = output_column or column
    if output_column == column:
        out_schema = df.schema
    else:
        from pyspark.sql.types import StringType

        out_schema = _with_field(df.schema, output_column, StringType(), nullable=True)

    def process(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        for batch in batches:
            idx = batch.schema.get_field_index(column)
            cleaned = _witchhat.clean_with_rules(batch.column(idx), rules)
            if output_column == column:
                columns = list(batch.columns)
                columns[idx] = cleaned
                yield pa.RecordBatch.from_arrays(columns, schema=batch.schema)
            else:
                out_fields = list(batch.schema) + [pa.field(output_column, pa.string(), nullable=True)]
                yield pa.RecordBatch.from_arrays(list(batch.columns) + [cleaned], schema=pa.schema(out_fields))

    return map_in_arrow(df, process, out_schema)


def drop_duplicates(
    df: "DataFrame",
    columns: Iterable[str],
    version: str = "v1",
    repartition: bool = True,
) -> "DataFrame":
    """witchhat.drop_duplicates applied per partition, buffered so it sees a whole
    partition (not just one Arrow batch) at a time.

    `repartition=True` (the default) calls `df.repartition(*columns)` first.
    Without it, two duplicate rows that Spark happened to place in different
    partitions both survive: `mapInArrow` gives this function no visibility across
    partitions, so deduplication is only correct for the whole DataFrame when
    every duplicate of a given key is guaranteed to already be in the same
    partition. Spark hash-partitions by key, so repartitioning by the same
    `columns` guarantees exactly that. Pass `repartition=False` only when `df` is
    already known to be partitioned that way (e.g. right after another call in
    this module repartitioned by the same columns), to skip a redundant shuffle.
    """
    columns = list(columns)
    if repartition:
        df = df.repartition(*columns)

    def process(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        batches = list(batches)
        if not batches:
            return
        whole_partition = pa.concat_batches(batches)
        yield _witchhat.drop_duplicates(whole_partition, columns, version)

    return map_in_arrow(df, process, df.schema)


def _aggregate_output_schema(df: "DataFrame", group_by: list[str], aggregations: list[tuple[str, str, str]]) -> "StructType":
    from pyspark.sql.types import DoubleType, LongType, StructField, StructType

    fields = [StructField(name, df.schema[name].dataType, nullable=True) for name in group_by]
    for column, func, alias in aggregations:
        if func == "count":
            fields.append(StructField(alias, LongType(), nullable=False))
        elif func in ("sum", "mean", "avg"):
            fields.append(StructField(alias, DoubleType(), nullable=True))
        elif func in ("min", "max"):
            fields.append(StructField(alias, df.schema[column].dataType, nullable=True))
        else:
            raise ValueError(f"unknown aggregate function {func!r}")
    return StructType(fields)


def aggregate(
    df: "DataFrame",
    group_by: list[str],
    aggregations: list[tuple[str, str, str]],
    repartition: bool = True,
) -> "DataFrame":
    """witchhat.aggregate applied per partition, buffered so it sees a whole
    partition at a time, correct for the whole DataFrame only when every row of a
    given `group_by` key is in the same partition.

    `repartition=True` (the default) calls `df.repartition(*group_by)` first:
    Spark hash-partitions by key, so every row sharing a `group_by` value lands in
    the same partition, and the per-partition aggregate for that key then equals
    the true whole-DataFrame aggregate for it. This is why the default is safe,
    not just convenient; pass `repartition=False` only when `df` is already
    partitioned that way.

    Unlike `witchhat.aggregate`, `group_by` must be non-empty here: a whole-table
    aggregate needs every row in one partition, which hash-repartitioning cannot
    arrange, and silently returning a partial answer under `repartition=False`
    would be worse than refusing outright.

    Raises
    ------
    ValueError
        `group_by` is empty, or an aggregation names an unrecognised function.
    """
    if not group_by:
        raise ValueError(
            "group_by must be non-empty for witchhat.spark.aggregate: a whole-table "
            "aggregate needs every row in one partition, which this function cannot "
            "arrange safely. Use df.coalesce(1) and witchhat.aggregate directly "
            "inside your own mapInArrow call if you specifically need that."
        )
    out_schema = _aggregate_output_schema(df, list(group_by), list(aggregations))
    if repartition:
        df = df.repartition(*group_by)

    def process(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        batches = list(batches)
        if not batches:
            return
        whole_partition = pa.concat_batches(batches)
        yield _witchhat.aggregate(whole_partition, list(group_by), list(aggregations))

    return map_in_arrow(df, process, out_schema)


def collect_as_record_batch(df: "DataFrame") -> pa.RecordBatch:
    """Collects a small DataFrame to the driver as one `pyarrow.RecordBatch`, for
    use as the small/broadcast side of :func:`broadcast_join`.

    Only for a DataFrame small enough to fit in the driver's memory: this pulls
    every row to the driver, the same as `DataFrame.collect()`. Uses
    `DataFrame.toArrow()` where available (Spark >= 4.0), falling back to
    `toPandas()` otherwise (Spark 3.x, including current Databricks LTS runtimes
    as of this writing).
    """
    _require_pyspark()
    if hasattr(df, "toArrow"):
        table = df.toArrow()
    else:
        table = pa.Table.from_pandas(df.toPandas(), preserve_index=False)
    table = table.combine_chunks()
    batches = table.to_batches()
    if not batches:
        empty_arrays = [pa.array([], type=f.type) for f in table.schema]
        return pa.RecordBatch.from_arrays(empty_arrays, schema=table.schema)
    return batches[0]


def _broadcast_join_output_schema(left_schema: "StructType", right_schema: pa.Schema) -> "StructType":
    from pyspark.sql.pandas.types import from_arrow_schema
    from pyspark.sql.types import StructField, StructType

    right_spark_schema = from_arrow_schema(right_schema)
    left_names = {f.name for f in left_schema}
    fields = [StructField(f.name, f.dataType, nullable=True) for f in left_schema]
    for f in right_spark_schema:
        name = f"{f.name}_right" if f.name in left_names else f.name
        fields.append(StructField(name, f.dataType, nullable=True))
    return StructType(fields)


def broadcast_join(
    df: "DataFrame",
    small_table: pa.RecordBatch,
    left_keys: list[str],
    right_keys: list[str],
    how: str = "inner",
) -> "DataFrame":
    """Joins each partition of `df` against `small_table`, already collected to
    the driver (see :func:`collect_as_record_batch`), using `witchhat.join`.

    This is the correct way to use witchhat's join inside Spark: witchhat is not a
    distributed engine (`docs/architecture.md` Chapter I), so it cannot itself
    join two large, independently partitioned Spark DataFrames the way
    `DataFrame.join` does. Broadcasting the small side to every partition and
    joining locally mirrors Spark's own broadcast-join optimization; a
    large-large shuffle join is out of scope here, since Spark's native join
    already does that better than routing it through a per-partition Python call
    would. Row-local across batches (each input batch is joined against the same
    fixed `small_table` independently), so this streams without buffering a whole
    partition, unlike :func:`drop_duplicates`/:func:`aggregate`.

    Parameters
    ----------
    df:
        The (potentially large) left side.
    small_table:
        The right side, already collected to the driver as one `RecordBatch`.
    left_keys, right_keys:
        Column names, matched pairwise; see `witchhat.join`.
    how:
        `"inner"`, `"left"`, `"right"` or `"full"`; see `witchhat.join`. `"right"`/
        `"full"` also surface `small_table` rows unmatched in a given partition,
        which is very likely not what you want when `small_table` is broadcast to
        *every* partition (an unmatched small-table row would appear once per
        partition, not once overall) — prefer `"inner"`/`"left"` unless you have
        specifically accounted for that.

    Raises
    ------
    ImportError
        pyspark is not installed.
    """
    _require_pyspark()
    bc = df.sparkSession.sparkContext.broadcast(small_table)
    out_schema = _broadcast_join_output_schema(df.schema, small_table.schema)

    def process(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        right = bc.value
        for batch in batches:
            yield _witchhat.join(batch, right, list(left_keys), list(right_keys), how)

    return map_in_arrow(df, process, out_schema)


def validate_schema(
    df: "DataFrame",
    expected: "DataFrame | StructType | pa.Schema",
    allow_numeric_widening: bool = False,
) -> Any:
    """witchhat.validate_schema between `df`'s schema and `expected`, converting
    either side from a Spark schema first if needed. See `witchhat.validate_schema`.
    """
    actual_arrow = to_arrow_schema(df)
    expected_arrow = expected if isinstance(expected, pa.Schema) else to_arrow_schema(expected)
    return _witchhat.validate_schema(actual_arrow, expected_arrow, allow_numeric_widening)


def schema_fingerprint(df: "DataFrame", version: str = "v1") -> int:
    """witchhat.schema_fingerprint of `df`'s schema, converted from Spark first.
    See `witchhat.schema_fingerprint`.
    """
    return _witchhat.schema_fingerprint(to_arrow_schema(df), version)
