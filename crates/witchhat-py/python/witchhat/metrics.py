"""Optional JSON metric logging: latency and throughput for every witchhat function,
for spotting bottlenecks in a pipeline or a distributed Spark job.

Disabled by default, and effectively free when disabled: every wrapped function checks
one module-level boolean before doing anything metrics-related, so the default,
uninstrumented path pays no timing call, no allocation, and no I/O. Enable with
:func:`enable` (JSON lines to stdout by default), a file path, or a callable that
receives each event as a plain `dict`, for routing into whatever telemetry system a
caller already has (a logger, a Spark accumulator, a queue).

Every event is schema-level only: function name, row/column counts, duration, and the
caller-supplied parameters (column names, a version string, a join `how`) that a caller
already knows. Never a cell value or a row's contents, the same principle
`docs/architecture.md`'s Security Model chapter applies to every kernel's own behaviour.

Because `witchhat.spark`'s wrapper functions call straight into the (instrumented)
functions in this package rather than a second implementation, enabling metrics here
also instruments every `witchhat.spark` call with no separate configuration: a
partition-coordinating function (`drop_duplicates`, `aggregate`) emits one event per
Spark partition, a row-local function (`hash_rows`, `clean_with_preset`/`clean_with_rules`)
one event per Arrow batch, matching each function's own natural granularity.
"""

from __future__ import annotations

import json
import sys
import time
from collections.abc import Callable
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Union

Sink = Callable[[dict[str, Any]], None]

_enabled = False
_sink: Sink | None = None


def _stdout_sink(event: dict[str, Any]) -> None:
    print(json.dumps(event, separators=(",", ":")), file=sys.stdout, flush=True)


def _file_sink(path: Path) -> Sink:
    def write(event: dict[str, Any]) -> None:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, separators=(",", ":")))
            f.write("\n")

    return write


def enable(sink: Union[str, Path, Sink] = "stdout") -> None:
    """Turns on metric logging for every witchhat function (including, transitively,
    every `witchhat.spark` call).

    Parameters
    ----------
    sink:
        `"stdout"` (the default): print one JSON line per event.
        A file path (`str`/`Path`): append one JSON line per event, creating the file
        if needed. On Databricks this is local to whichever driver or executor the
        call runs on; it is not itself a distributed collection mechanism.
        A callable: called once per event with the event as a `dict`. Use this to
        route events into your own logger, a `pyspark.Accumulator`, a queue, or
        anything else; it is the extension point for cross-executor aggregation,
        which this module does not attempt itself.
    """
    global _enabled, _sink
    if callable(sink):
        _sink = sink
    elif sink == "stdout":
        _sink = _stdout_sink
    else:
        _sink = _file_sink(Path(sink))
    _enabled = True


def disable() -> None:
    """Turns off metric logging. Cheap and safe to call even if never enabled."""
    global _enabled
    _enabled = False


def is_enabled() -> bool:
    """Whether metric logging is currently on. Every wrapped function checks this
    first and does nothing metrics-related at all when it is `False`."""
    return _enabled


def row_count(obj: Any) -> int | None:
    """Best-effort row count of an Arrow-shaped object: `.num_rows` for a
    `RecordBatch`-like value, `len()` for an `Array`-like value, `None` if neither
    works. Never raises; a `None` in an event means "not available", not "zero".
    """
    try:
        return obj.num_rows
    except AttributeError:
        pass
    try:
        return len(obj)
    except TypeError:
        return None


@contextmanager
def measure(function: str, **context: Any) -> Iterator[dict[str, Any]]:
    """Times the wrapped block and emits one JSON event to the configured sink when
    metric logging is enabled; otherwise a no-op (no timing, no allocation, no `dict`
    beyond the one empty placeholder returned).

    `context` is merged into the event verbatim: pass schema-level facts (column
    names, a version string), never row values. Set `event["rows_in"]`/
    `event["rows_out"]` (see :func:`row_count`) or anything else on the yielded
    `dict` from inside the block to have it included in the emitted event.
    `duration_ms` is always filled in; `rows_per_second` is added automatically
    when `rows_in` was set to an `int`.

    An exception raised inside the block is recorded (`status: "error"`, `error`:
    the exception's type and message, never its full traceback, which could contain
    row-derived values) and still re-raised: this function only ever observes,
    never swallows or changes what the wrapped call does.
    """
    if not _enabled:
        yield {}
        return

    event: dict[str, Any] = dict(context)
    event["function"] = function
    event["ts"] = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    event["status"] = "ok"
    start = time.perf_counter()
    try:
        yield event
    except Exception as exc:
        event["status"] = "error"
        event["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        duration_ms = (time.perf_counter() - start) * 1000.0
        event["duration_ms"] = round(duration_ms, 3)
        rows_in = event.get("rows_in")
        if isinstance(rows_in, int) and duration_ms > 0:
            event["rows_per_second"] = round(rows_in / (duration_ms / 1000.0), 1)
        sink = _sink
        if sink is not None:
            sink(event)
