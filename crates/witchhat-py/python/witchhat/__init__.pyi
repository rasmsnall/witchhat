"""Type stubs for the ``witchhat`` package.

Every function below can be optionally instrumented: see ``witchhat.metrics`` (a
plain, fully-annotated Python module, not part of this stub) for JSON latency and
throughput logging, disabled by default.
"""

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import pyarrow as pa

__version__: str
__all__: list[str]

class _ArrowArrayExportable(Protocol):
    """Anything implementing the Arrow C Data / pyarrow interface for an array-like
    value: a ``pyarrow.RecordBatch``, a ``pyarrow.Array``, a ``polars`` export, or
    anything else exposing ``__arrow_c_array__``."""

    def __arrow_c_array__(
        self, requested_schema: object | None = ...
    ) -> tuple[object, object]: ...

class _ArrowSchemaExportable(Protocol):
    """Anything implementing the Arrow C Data / pyarrow interface for a schema, such
    as a ``pyarrow.Schema``."""

    def __arrow_c_schema__(self) -> object: ...

class CpuFeatures:
    """CPU features detected on the machine running this process.

    Informational only: nothing in this release branches on it. Returned by
    :func:`cpu_features`; not constructible directly.
    """

    @property
    def sse42(self) -> bool:
        """Whether SSE4.2 is available, on x86_64."""

    @property
    def avx2(self) -> bool:
        """Whether AVX2 is available, on x86_64."""

    @property
    def avx512f(self) -> bool:
        """Whether AVX-512 Foundation is available, on x86_64."""

    @property
    def neon(self) -> bool:
        """Whether NEON is available, on aarch64."""

def hash_rows(
    batch: _ArrowArrayExportable,
    columns: list[str],
    version: str = "v1",
) -> "pa.Array":
    """Fingerprint ``columns`` of ``batch``, in the given order, into one uint64 per row.

    Columns not listed do not affect the result, and the same columns in a different
    order produce a different fingerprint. A ``null`` cell hashes distinctly from every
    non-null value of its column, including an empty string or a zero. ``NaN`` and
    ``-0.0`` are canonicalized before hashing: every ``NaN`` bit pattern collapses to one
    hash, and ``0.0``/``-0.0`` hash equal.

    Parameters
    ----------
    batch:
        Any object implementing the Arrow C Data / pyarrow interface, such as a
        ``pyarrow.RecordBatch`` or a ``polars`` batch export.
    columns:
        Column names, in the order to hash them. Order matters: ``["a", "b"]`` and
        ``["b", "a"]`` produce different fingerprints for the same row.
    version:
        The hashing algorithm to use, by name. ``"v1"`` (the default) is the only
        version implemented so far. A fingerprint computed under a given version is
        reproducible under that version indefinitely, even after later versions ship.

    Returns
    -------
    pyarrow.Array
        One ``uint64`` per row of ``batch``, same length and order as its input rows.

    Raises
    ------
    ValueError
        ``version`` does not name a known hashing algorithm.
    RuntimeError
        A name in ``columns`` is not in ``batch``'s schema, or a requested column's
        Arrow type has no defined hash (supported: booleans, all signed and unsigned
        integer widths, floats, and UTF-8/binary strings, plus their ``Large*``
        variants).
    """

def hash_rows_all_columns(
    batch: _ArrowArrayExportable,
    version: str = "v1",
) -> "pa.Array":
    """:func:`hash_rows` over every column of ``batch``, in schema order.

    See :func:`hash_rows` for the meaning of ``version``, the return shape, and the
    exceptions raised.
    """

def table_fingerprint(
    row_hashes: _ArrowArrayExportable,
    version: str = "v1",
) -> int:
    """Fold a batch of row hashes (e.g. from :func:`hash_rows`) into one
    order-independent fingerprint.

    Two batches holding the same rows in a different order, such as a reshuffled
    partition, produce the same table fingerprint. This is the intended way to check
    witchhat's output against Spark's: fingerprint both sides' rows and compare the two
    integers, without sorting either side first. Not collision-free in the way a
    per-row comparison is, so treat a match as strong evidence, not a proof, for
    anything security-sensitive.

    Parameters
    ----------
    row_hashes:
        A ``uint64`` array of row hashes, such as :func:`hash_rows`'s return value.
    version:
        Must match the version ``row_hashes`` was computed with; nothing enforces this
        automatically, since ``row_hashes`` is already a plain integer array by the
        time it reaches this function.

    Returns
    -------
    int
        The order-independent table fingerprint.

    Raises
    ------
    ValueError
        ``version`` does not name a known hashing algorithm.
    """

def schema_fingerprint(
    schema: _ArrowSchemaExportable,
    version: str = "v1",
) -> int:
    """Fingerprint ``schema``'s shape: field names in order, their types, and their
    nullability.

    Two schemas with the same fields in the same order, same types and same
    nullability hash equal; changing the field order, a type, or a nullability flag
    changes the result.

    Parameters
    ----------
    schema:
        Any object implementing the Arrow C Data schema interface, such as a
        ``pyarrow.Schema`` (``batch.schema`` on a ``pyarrow.RecordBatch``).
    version:
        The hashing algorithm to use, by name. See :func:`hash_rows`.

    Returns
    -------
    int

    Raises
    ------
    ValueError
        ``version`` does not name a known hashing algorithm.
    """

class SchemaDiff:
    """The result of comparing an actual schema against an expected one.

    Empty (:meth:`is_empty`) when the two agree under the ``allow_numeric_widening``
    passed to :func:`validate_schema`. Not constructible directly.
    """

    @property
    def missing(self) -> list[str]:
        """Column names in ``expected`` that ``actual`` does not have."""

    @property
    def unexpected(self) -> list[str]:
        """Column names in ``actual`` that ``expected`` does not have.

        Reported, but does not make :meth:`is_breaking` true: an additive column does
        not usually invalidate code written against the narrower, expected schema.
        """

    @property
    def retyped(self) -> list[tuple[str, "pa.DataType", "pa.DataType"]]:
        """``(column, expected_type, actual_type)`` for every column present in both
        schemas whose type differs and was not an accepted widening."""

    @property
    def nullability(self) -> list[tuple[str, bool, bool]]:
        """``(column, expected_nullable, actual_nullable)`` for every column whose
        nullability tightened (``expected`` non-nullable, ``actual`` nullable)."""

    def is_empty(self) -> bool:
        """Whether ``actual`` and ``expected`` agreed on every point checked."""

    def is_breaking(self) -> bool:
        """Whether the difference is one a caller most likely cannot safely ignore: a
        missing column, a retyped column, or a nullability tightening. An
        :attr:`unexpected` column alone does not count."""

def validate_schema(
    actual: _ArrowSchemaExportable,
    expected: _ArrowSchemaExportable,
    allow_numeric_widening: bool = False,
) -> SchemaDiff:
    """Compare ``actual`` against ``expected`` and return their difference.

    Columns are matched by name, case-sensitively. For a column present in both: its
    type must match exactly unless ``allow_numeric_widening`` accepts the specific
    widening seen (``int32`` -> ``int64``, ``float32`` -> ``float64``, and so on within
    a signedness class; a narrower type, a cross-signedness change, or an
    integer-to-float change is never accepted regardless), and ``actual``'s nullability
    must be compatible with ``expected``'s: nullable is fine when ``expected`` is too,
    or non-nullable when ``expected`` allows null, but not nullable when ``expected``
    declares the column non-nullable.

    Parameters
    ----------
    actual:
        The schema to check, such as a ``pyarrow.RecordBatch``'s ``.schema``.
    expected:
        The schema ``actual`` is being checked against.
    allow_numeric_widening:
        Accept ``actual`` having a wider numeric type than ``expected`` for the same
        column. ``False`` by default (exact type match required).

    Returns
    -------
    SchemaDiff
    """

class NormalizeStats:
    """What happened while normalizing a batch, beyond the columns themselves.

    Not constructible directly; returned by :func:`normalize_json`.
    """

    @property
    def rows_malformed(self) -> int:
        """Rows whose JSON text did not parse, or parsed to something other than a
        JSON object. Every target column is null for such a row."""

    @property
    def type_mismatches(self) -> dict[str, int]:
        """``{column: count}`` for rows whose JSON value at that column's path had
        the wrong JSON type (so it was written as null). Does not count an absent
        path or an explicit JSON ``null``: both are an ordinary, expected null."""

def normalize_json(
    json: _ArrowArrayExportable,
    schema: _ArrowSchemaExportable,
    version: str = "v1",
) -> tuple["pa.RecordBatch", NormalizeStats]:
    """Parse ``json``, one JSON object per row, into ``schema``.

    A field's name is a ``.``-separated path into the JSON object
    (``"address.city"`` reads ``{"address": {"city": ...}}``); a plain name is a
    top-level key. Supported target types are ``utf8``, ``int64``, ``float64`` and
    ``boolean``.

    A row whose text fails to parse, or that parses to something other than a JSON
    object, becomes null in every column and counts toward
    :attr:`NormalizeStats.rows_malformed`. Within a row, an absent path or an explicit
    JSON ``null`` becomes an ordinary null; a path present with the wrong JSON type
    becomes null and counts in :attr:`NormalizeStats.type_mismatches` for that column.

    Parameters
    ----------
    json:
        A ``utf8`` array of JSON text, one row per element. A null element is treated
        like unparseable text (malformed).
    schema:
        The target schema. Field names may be dotted paths; see above.
    version:
        The parsing ruleset to use, by name. ``"v1"`` (the default) is the only
        version implemented so far.

    Returns
    -------
    tuple[pyarrow.RecordBatch, NormalizeStats]

    Raises
    ------
    RuntimeError
        A field in ``schema`` is not one of the four supported types.
    """

def clean_with_preset(
    input: _ArrowArrayExportable,
    name: str,
    version: str = "v1",
) -> "pa.Array":
    """Apply a named, built-in cleanup preset to ``input``.

    Presets defined by ``version="v1"`` (the only version implemented so far):
    ``trim_whitespace``, ``collapse_whitespace``, ``strip_control_characters``,
    ``strip_non_alphanumeric``, ``digits_only``. See the Rust ``clean::preset`` docs
    for exactly what each one does.

    Parameters
    ----------
    input:
        A ``utf8`` array. A null element stays null.
    name:
        The preset name.
    version:
        The preset set to use, by name.

    Returns
    -------
    pyarrow.Array

    Raises
    ------
    ValueError
        ``name`` or ``version`` is not recognised.
    """

def clean_with_rules(
    input: _ArrowArrayExportable,
    rules: list[tuple[str, str]],
) -> "pa.Array":
    """Apply caller-supplied regex find-and-replace rules to ``input``, in order.

    Each rule is ``(pattern, replacement)``; ``replacement`` follows the Rust
    ``regex`` crate's syntax (``$1``, ``${name}`` for capture groups, ``$$`` for a
    literal ``$``). Unlike :func:`clean_with_preset`, these rules are the caller's
    own and are not versioned by witchhat.

    Raises
    ------
    ValueError
        A pattern does not compile.
    """

class EquivalenceReport:
    """The result of comparing an actual batch against an expected one.

    Not constructible directly; returned by :func:`check_equivalence`.
    """

    @property
    def schema_diff(self) -> SchemaDiff:
        """The schema half of the comparison."""

    @property
    def row_count_actual(self) -> int: ...
    @property
    def row_count_expected(self) -> int: ...
    @property
    def table_fingerprint_actual(self) -> int:
        """Order-independent fingerprint of the actual batch's rows over the
        compared columns."""

    @property
    def table_fingerprint_expected(self) -> int:
        """Order-independent fingerprint of the expected batch's rows over the
        compared columns."""

    @property
    def fingerprints_match(self) -> bool: ...
    def is_equivalent(self) -> bool:
        """Whether the two batches are equivalent: empty schema diff, matching row
        counts, and matching table fingerprints. Stricter than
        ``schema_diff.is_breaking()``: an extra column alone fails this, unlike a
        breaking-change check."""

def check_equivalence(
    actual: _ArrowArrayExportable,
    expected: _ArrowArrayExportable,
    columns: list[str] | None = None,
    allow_numeric_widening: bool = False,
    hash_version: str = "v1",
) -> EquivalenceReport:
    """Compare ``actual`` against ``expected``: same schema, same row count, same
    rows regardless of order.

    Parameters
    ----------
    actual, expected:
        The two batches to compare, e.g. witchhat's output and Spark's.
    columns:
        Which columns to fingerprint, in that order. ``None`` (the default) uses
        every column ``expected`` and ``actual`` have in common, in ``expected``'s
        order, so a column declared in a different position on each side does not by
        itself cause a mismatch. A column whose *type* differs still does.
    allow_numeric_widening:
        Passed through to the schema comparison; see :func:`validate_schema`.
    hash_version:
        The row-hashing algorithm to use, by name.

    Returns
    -------
    EquivalenceReport

    Raises
    ------
    RuntimeError
        A name in ``columns`` is absent from ``actual`` or ``expected``, or a
        compared column's Arrow type has no defined hash.
    ValueError
        ``hash_version`` does not name a known hashing algorithm.
    """

def drop_duplicates(
    batch: _ArrowArrayExportable,
    columns: list[str],
    version: str = "v1",
) -> "pa.RecordBatch":
    """Keep the first row of every distinct value of ``columns`` in ``batch``,
    dropping the rest, preserving the relative order of the rows that remain.

    Equivalent to Spark's ``df.dropDuplicates(subset=columns)``, except which row
    within a duplicate group survives is always the first one by input order.

    Raises
    ------
    ValueError
        ``version`` does not name a known hashing algorithm.
    RuntimeError
        A name in ``columns`` is not in ``batch``'s schema, or a requested column's
        Arrow type has no defined hash.
    """

def join(
    left: _ArrowArrayExportable,
    right: _ArrowArrayExportable,
    left_keys: list[str],
    right_keys: list[str],
    how: str = "inner",
) -> "pa.RecordBatch":
    """Join ``left`` and ``right`` on ``left_keys``/``right_keys``, matched pairwise
    by position (``left_keys[0]`` compares against ``right_keys[0]``, and so on).

    A row whose key has a null in any of ``left_keys``/``right_keys`` never matches
    another row, the same as Spark/SQL (``NULL = NULL`` is never true); use
    :func:`join_null_safe` for the opposite. Such a row still appears as unmatched
    wherever ``how`` keeps unmatched rows.

    The output schema is every field of ``left`` followed by every field of
    ``right``; a ``right`` field whose name collides with a ``left`` field is
    suffixed ``_right``. Every output field is nullable regardless of the input
    schemas' own nullability, since an outer join can introduce a null on either
    side.

    Row order: every ``left`` row in its original order, each repeated once per
    match (or once with null ``right`` columns, under ``"left"``/``"full"``, if it
    has none), followed by every unmatched ``right`` row in its original order,
    under ``"right"``/``"full"``.

    Parameters
    ----------
    left, right:
        The two batches to join.
    left_keys, right_keys:
        Column names, matched pairwise: ``left_keys[i]``'s Arrow type must exactly
        equal ``right_keys[i]``'s. Must be the same non-empty length.
    how:
        ``"inner"`` (only rows with a match on both sides), ``"left"`` (every
        ``left`` row), ``"right"`` (every ``right`` row), or ``"full"`` (every row
        of both).

    Returns
    -------
    pyarrow.RecordBatch

    Raises
    ------
    ValueError
        ``how`` is not one of the four recognised join types.
    RuntimeError
        A name in ``left_keys``/``right_keys`` is not in its batch's schema, or
        ``left_keys[i]``'s type does not exactly match ``right_keys[i]``'s.
    """

def join_null_safe(
    left: _ArrowArrayExportable,
    right: _ArrowArrayExportable,
    left_keys: list[str],
    right_keys: list[str],
    how: str = "inner",
) -> "pa.RecordBatch":
    """Same as :func:`join`, except a null key matches another null key instead of
    never matching anything.

    Spark's own ``DataFrame.join`` never does this, so reach for :func:`join` by
    default; this exists only for a caller with a specific, deliberate reason to
    want it. Matching is a plain per-column comparison with no null exclusion: a
    null in one key column matches a null in the same key column on the other
    side, and every other, non-null key column still needs an exact match, so a
    partial-null composite key is not a wildcard, only that one column's null is.

    Raises
    ------
    Same as :func:`join`.
    """

def aggregate(
    batch: _ArrowArrayExportable,
    group_by: list[str],
    aggregations: list[tuple[str, str, str]],
) -> "pa.RecordBatch":
    """Group ``batch`` by ``group_by`` and reduce each group with ``aggregations``.

    ``group_by`` may be empty, in which case every row of ``batch`` is one group (a
    whole-table aggregate); an empty ``batch`` in that case still produces exactly
    one output row, with ``"count"`` ``0`` and every other aggregation ``None``.
    Output row order is first-seen group order, not sorted.

    The output schema is ``group_by``'s columns (types preserved from ``batch``,
    always nullable), followed by one column per ``aggregations`` entry, named by
    its alias, in the order given.

    Parameters
    ----------
    batch:
        The batch to aggregate.
    group_by:
        Column names to group by.
    aggregations:
        ``(column, func, alias)`` triples. ``func`` is ``"count"`` (non-null values,
        any column type, output ``int64``); ``"sum"`` (numeric columns only,
        summed at the input's own exact precision, ``None`` if every value in the
        group is null; output ``int64`` for a signed integer source, ``uint64`` for
        an unsigned source, ``float64`` for a float source, or the source's own
        ``decimal128``/``decimal256`` type for a decimal source); ``"mean"`` (same
        exact summation as ``"sum"``, only the final division is not exact; output
        ``float64`` for an integer or float source, or the source's own decimal
        type, mantissa integer-divided by the count and truncated, for a decimal
        source); or ``"min"``/``"max"`` (numeric columns only, compared at the
        input's own exact precision, never through ``float64``; output type
        matches the input column, ``None`` if every value in the group is null).

    Returns
    -------
    pyarrow.RecordBatch

    Raises
    ------
    ValueError
        A ``func`` is not one of the five recognised aggregate functions.
    RuntimeError
        A name in ``group_by`` or an aggregation's ``column`` is not in ``batch``'s
        schema, or a ``sum``/``mean``/``min``/``max`` column is not numeric
        (``int8``..``int64``, ``uint8``..``uint64``, ``float32``, ``float64``,
        ``decimal128``, ``decimal256``). Also raised (as ``RuntimeError``) if
        ``sum``/``mean``'s exact accumulator cannot represent a group's running
        total; this replaces silent, wrong output from the previous ``float64``
        accumulator overflowing its precision unnoticed.
    """

def cpu_features() -> CpuFeatures:
    """Detect the CPU features of the machine running this process.

    Detection runs once per process and is cached; every call after the first returns
    the cached value.
    """
