"""Round-trip witchhat.metrics against real pyarrow calls.

Run after installing the wheel (see docs/operations.md):

    python tools/metrics_smoke.py

Confirms metric logging is disabled by default, that enabling it captures one JSON
event per call (including an error case) with the expected fields, and that disabling
it again fully stops emission. Not a benchmark: the same role tools/smoke.py plays for
the core package, but for the instrumentation layer.
"""

from __future__ import annotations

import sys

import pyarrow as pa

import witchhat


def main() -> int:
    events: list[dict] = []

    # disabled by default: no events, and metrics.measure's fast path is exercised by
    # every other witchhat call in tools/smoke.py already passing.
    batch = pa.record_batch(
        {
            "id": pa.array([1, 2, 3, 2], type=pa.int64()),
            "name": pa.array(["a", "b", "c", "b"]),
        }
    )
    witchhat.hash_rows(batch, ["id", "name"])
    assert events == []
    assert not witchhat.metrics.is_enabled()

    witchhat.metrics.enable(sink=events.append)
    assert witchhat.metrics.is_enabled()

    hashes = witchhat.hash_rows(batch, ["id", "name"])
    deduped = witchhat.drop_duplicates(batch, ["id", "name"])

    users = pa.record_batch({"id": pa.array([1, 2]), "country": pa.array(["NO", "SE"])})
    orders = pa.record_batch({"user_id": pa.array([1, 1]), "amount": pa.array([10, 20])})
    joined = witchhat.join(users, orders, ["id"], ["user_id"], how="inner")
    witchhat.aggregate(joined, ["country"], [("amount", "sum", "total")])

    try:
        witchhat.hash_rows(batch, ["missing"])
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected an unknown column to raise")

    assert len(events) == 5, events

    hash_event = events[0]
    assert hash_event["function"] == "hash_rows"
    assert hash_event["columns"] == ["id", "name"]
    assert hash_event["rows_in"] == batch.num_rows == 4
    assert hash_event["rows_out"] == len(hashes) == 4
    assert hash_event["status"] == "ok"
    assert isinstance(hash_event["duration_ms"], float)
    assert isinstance(hash_event["rows_per_second"], float)
    assert "ts" in hash_event

    dedup_event = events[1]
    assert dedup_event["function"] == "drop_duplicates"
    assert dedup_event["rows_out"] == deduped.num_rows == 3

    join_event = events[2]
    assert join_event["function"] == "join"
    assert join_event["how"] == "inner"
    assert join_event["rows_in"] == 2
    assert join_event["rows_in_right"] == 2

    agg_event = events[3]
    assert agg_event["function"] == "aggregate"
    assert agg_event["group_by"] == ["country"]

    error_event = events[4]
    assert error_event["function"] == "hash_rows"
    assert error_event["status"] == "error"
    assert "missing" in error_event["error"], error_event

    # every event must be JSON-serializable as-is (the sink used above is a plain
    # list, not the real JSON-encoding stdout/file sinks, so this is not implied)
    import json

    for event in events:
        json.dumps(event)

    witchhat.metrics.disable()
    assert not witchhat.metrics.is_enabled()
    witchhat.hash_rows(batch, ["id"])
    assert len(events) == 5, "disable() must stop further events"

    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
