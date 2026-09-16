"""Type stubs for the ``witchhat`` package."""

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

def cpu_features() -> CpuFeatures:
    """Detect the CPU features of the machine running this process.

    Detection runs once per process and is cached; every call after the first returns
    the cached value.
    """
