"""Benchmarks a complete Spark action through witchhat.spark against Spark's own
native equivalent, end to end (JVM-to-Arrow conversion, Python worker startup,
mapInArrow overhead, and materialization all included), not just the isolated
Rust kernel call in isolation.

An external code review (2026-09-16) raised this as a real gap: a fast Rust
kernel can still lose end to end because of everything Spark itself adds around
a `mapInArrow` call. This script answers that question honestly, not by
asserting witchhat is faster, but by actually measuring both paths the same way
and printing both numbers so a reader can judge for themselves.

Run after installing the wheel and pyspark (see docs/operations.md):

    pip install pyspark
    python tools/spark_benchmark.py [--rows N]

Not part of CI (see witchhat.spark's module docstring for why pyspark stays an
optional, lazily-imported dependency) and not a substitute for a real
multi-node benchmark: this runs `local[2]` on one machine, in whatever this
process's sandbox happens to be under load from at the time, and reports
wall-clock time for a handful of runs, not a statistically rigorous benchmark
suite. Treat the numbers as directional, not authoritative; rerun on real
hardware, under real cluster conditions, before making a capacity or migration
decision from them.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from statistics import mean

# See tools/spark_smoke.py for why this is needed on JDK 17+.
_ADD_OPENS = " ".join(
    f"--add-opens=java.base/{pkg}=ALL-UNNAMED"
    for pkg in (
        "java.lang",
        "java.lang.invoke",
        "java.lang.reflect",
        "java.io",
        "java.net",
        "java.nio",
        "java.util",
        "java.util.concurrent",
        "java.util.concurrent.atomic",
        "sun.nio.ch",
        "sun.nio.cs",
        "sun.security.action",
        "sun.util.calendar",
    )
)
_JVM_OPTS = f"{_ADD_OPENS} -Dio.netty.tryReflectionSetAccessible=true"
os.environ.setdefault("JDK_JAVA_OPTIONS", _JVM_OPTS)
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS", f'--driver-java-options="{_JVM_OPTS}" pyspark-shell'
)


def _time_action(label: str, action, repeats: int) -> list[float]:
    """Times `action()` `repeats` times, discarding a warm-up run (JVM/Python
    worker startup, JIT/codegen warm-up) so later runs measure steady-state
    cost, not one-time setup. Prints each run so a reader can see the spread,
    not just a single, possibly lucky, number.
    """
    action()  # warm-up, not counted
    durations = []
    for i in range(repeats):
        start = time.perf_counter()
        action()
        elapsed = time.perf_counter() - start
        durations.append(elapsed)
        print(f"  {label} run {i + 1}/{repeats}: {elapsed:.3f}s")
    return durations


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rows", type=int, default=200_000, help="synthetic dataset size (default: 200000)"
    )
    parser.add_argument(
        "--repeats", type=int, default=3, help="timed runs per comparison, after a warm-up"
    )
    args = parser.parse_args()

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("witchhat-benchmark")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        run(spark, args.rows, args.repeats)
    except Exception as exc:  # noqa: BLE001 - see tools/spark_smoke.py
        if "sun.misc.Unsafe" in str(exc) or "DirectByteBuffer" in str(exc):
            print(
                "\nThis failed in Spark's own JVM<->Python Arrow bridge, before any "
                "witchhat code ran; see tools/spark_smoke.py's module docstring for "
                "the known JDK 17+ incompatibility this is, and its workaround.\n",
                file=sys.stderr,
            )
        raise
    finally:
        spark.stop()
    return 0


def run(spark, num_rows: int, repeats: int) -> None:
    import pyspark.sql.functions as F

    from witchhat import spark as wspark

    print(f"Benchmarking with {num_rows:,} rows, {repeats} timed runs per comparison "
          "(local[2], one machine: directional numbers only, see this script's docstring).\n")

    df = (
        spark.range(num_rows)
        .withColumn("key", (F.col("id") % (num_rows // 10)).cast("long"))
        .withColumn("group", (F.col("id") % 100).cast("long"))
        .withColumn("amount", (F.col("id") % 997).cast("long"))
        .repartition(8)
        .cache()
    )
    df.count()  # force the cache to materialize before any comparison starts

    results: dict[str, tuple[list[float], list[float]]] = {}

    # dropDuplicates: witchhat.spark vs Spark's own, both forcing full
    # materialization via .count() as the action.
    print("drop_duplicates (dedup on `key`, full materialization via .count()):")
    witchhat_times = _time_action(
        "witchhat", lambda: wspark.drop_duplicates(df, ["key"]).count(), repeats
    )
    native_times = _time_action(
        "spark-native", lambda: df.dropDuplicates(["key"]).count(), repeats
    )
    results["drop_duplicates"] = (witchhat_times, native_times)
    print()

    # aggregate: sum+count per group, again forcing materialization.
    print("aggregate (sum+count of `amount` grouped by `group`, via .count()):")
    witchhat_times = _time_action(
        "witchhat",
        lambda: wspark.aggregate(
            df, ["group"], [("amount", "sum", "total"), ("amount", "count", "n")]
        ).count(),
        repeats,
    )
    native_times = _time_action(
        "spark-native",
        lambda: df.groupBy("group").agg(F.sum("amount"), F.count("amount")).count(),
        repeats,
    )
    results["aggregate"] = (witchhat_times, native_times)
    print()

    print(f"{'Operation':<20}{'witchhat (mean s)':<20}{'Spark-native (mean s)':<22}{'Ratio':<10}")
    for name, (witchhat_times, native_times) in results.items():
        w_mean = mean(witchhat_times)
        n_mean = mean(native_times)
        ratio = w_mean / n_mean if n_mean else float("nan")
        print(f"{name:<20}{w_mean:<20.3f}{n_mean:<22.3f}{ratio:<10.2f}")
    print(
        "\nRatio > 1.0 means witchhat's mapInArrow path was slower than Spark's own "
        "operator for this comparison, on this machine, right now. This script exists "
        "to make that number visible, not to guarantee it favors witchhat: JVM<->Arrow "
        "conversion, Python worker startup, and mapInArrow's own overhead are real costs "
        "a kernel's own speed cannot buy back, especially at small-to-medium row counts "
        "where they dominate. See docs/architecture.md Chapter XIX for the honest "
        "trade-offs this project makes."
    )


if __name__ == "__main__":
    sys.exit(main())
