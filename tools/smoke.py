"""Round-trip a real pyarrow batch through the built ``witchhat`` wheel.

Run after installing the wheel (see docs/operations.md):

    python tools/smoke.py

Exercises every exported function and checks the properties architecture.md documents:
duplicate rows hash equal, distinct rows do not, a reordered batch's table fingerprint is
unchanged, and an unknown column is rejected. Not a substitute for `cargo test`, which
covers the algorithm itself in far more depth; this only confirms the wheel as built
actually behaves the way the Rust tests say it should.
"""

from __future__ import annotations

import sys

import pyarrow as pa

import witchhat


def main() -> int:
    print("witchhat version:", witchhat.__version__)
    print("cpu features:", witchhat.cpu_features())

    batch = pa.record_batch(
        {
            "id": pa.array([1, 2, 3, 2], type=pa.int64()),
            "name": pa.array(["a", "b", "c", "b"], type=pa.string()),
            "score": pa.array([1.5, None, -0.0, None], type=pa.float64()),
        }
    )

    row_hashes = witchhat.hash_rows(batch, ["id", "name"])
    assert row_hashes[1].as_py() == row_hashes[3].as_py(), "duplicate rows must hash equal"
    assert row_hashes[0].as_py() != row_hashes[2].as_py(), "distinct rows must hash differently"

    all_columns = witchhat.hash_rows_all_columns(batch)
    fingerprint = witchhat.table_fingerprint(all_columns)

    reordered = batch.take(pa.array([3, 1, 0, 2]))
    reordered_fingerprint = witchhat.table_fingerprint(witchhat.hash_rows_all_columns(reordered))
    assert fingerprint == reordered_fingerprint, "table fingerprint must be order-independent"

    schema_fp = witchhat.schema_fingerprint(batch.schema)
    assert isinstance(schema_fp, int)

    try:
        witchhat.hash_rows(batch, ["missing"])
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected an unknown column to raise RuntimeError")

    try:
        witchhat.hash_rows(batch, ["id"], version="v99")
    except ValueError:
        pass
    else:
        raise AssertionError("expected an unknown hash version to raise ValueError")

    expected_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
            pa.field("score", pa.float64(), nullable=True),
            pa.field("country", pa.string(), nullable=True),
        ]
    )
    diff = witchhat.validate_schema(batch.schema, expected_schema)
    assert diff.missing == ["country"], diff.missing
    assert diff.is_breaking(), "a missing column must be breaking"

    identical = witchhat.validate_schema(batch.schema, batch.schema)
    assert identical.is_empty(), "a schema compared against itself must be empty"

    narrower_expected = pa.schema([pa.field("id", pa.int32(), nullable=False)])
    narrower_actual = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    assert not witchhat.validate_schema(narrower_actual, narrower_expected).is_empty()
    assert witchhat.validate_schema(
        narrower_actual, narrower_expected, allow_numeric_widening=True
    ).is_empty()

    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
