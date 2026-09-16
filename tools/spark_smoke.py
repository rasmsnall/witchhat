"""Round-trip witchhat.spark against a real local pyspark session.

Run after installing the wheel and pyspark (see docs/operations.md):

    pip install pyspark
    python tools/spark_smoke.py

Not part of the wheel's own CI (pyspark is a large, optional dependency; see
witchhat.spark's module docstring for why it is lazily imported rather than
required), but exercises every function in witchhat.spark against a real local
Spark session, the same role tools/smoke.py plays for the core package against
real pyarrow.

On a JDK 17+ machine, local Spark's bundled Arrow Java library can fail with
`UnsupportedOperationException: sun.misc.Unsafe ... not available` on *any*
Arrow-based Python UDF (mapInArrow, mapInPandas, applyInPandas alike) before
witchhat ever runs: this script sets the JVM `--add-opens` flags known to fix
it, but the underlying compatibility gap is between pyspark's bundled Arrow
Java and newer JDKs, not something in this repository, and it does not affect
Databricks (which manages its own JDK/Arrow versions).
"""

from __future__ import annotations

import os
import sys

import pyarrow as pa

import witchhat
from witchhat import spark as wspark

# Spark's bundled Arrow Java library needs the JVM module system opened up under
# JDK 17+ (it still uses sun.misc.Unsafe-based direct buffer access that the
# default module boundaries block), or every mapInArrow call fails with
# `UnsupportedOperationException: sun.misc.Unsafe ... not available`. Databricks
# clusters already set this; a local test run needs it set before the JVM starts.
# Harmless, and unread, on a JDK where it is not needed.
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
# JDK_JAVA_OPTIONS alone does not reach local mode's driver JVM in every pyspark
# build (it is launched via bin/spark-class, which only forwards options passed
# this way); PYSPARK_SUBMIT_ARGS is the mechanism spark-submit itself reads.
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    f'--driver-java-options="{_JVM_OPTS}" pyspark-shell',
)


def main() -> int:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local[2]").appName("witchhat-smoke").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        run(spark)
    except Exception as exc:  # noqa: BLE001 - deliberately broad, see below
        if "sun.misc.Unsafe" in str(exc) or "DirectByteBuffer" in str(exc):
            print(
                "\nThis failed in Spark's own JVM<->Python Arrow bridge "
                "(org.apache.arrow.memory.util.MemoryUtil), before any witchhat "
                "code ran: confirmed by the same failure on a bare df.mapInPandas() "
                "call with no witchhat involved at all. A known JDK 17+/21 "
                "incompatibility with the Arrow Java version pyspark bundles, not a "
                "witchhat bug and not expected on a Databricks cluster (which "
                "manages its own JDK/Arrow versions). Try a JDK 17 JVM, or add\n"
                f"  {_JVM_OPTS}\nto your JVM's startup options if this reappears.\n",
                file=sys.stderr,
            )
        raise
    finally:
        spark.stop()
    print("OK")
    return 0


def run(spark) -> None:
    df = spark.createDataFrame(
        [(1, "alice", 10), (2, "bob", 20), (1, "alice", 10), (3, "carol", 5)],
        ["id", "name", "amount"],
    )

    # hash_rows: adds a bit-reinterpreted int64 column, row-local
    hashed = wspark.hash_rows(df, ["id", "name"])
    rows = hashed.collect()
    assert rows[0]["row_hash"] == rows[2]["row_hash"], "duplicate (id, name) must hash equal"
    assert isinstance(rows[0]["row_hash"], int)

    # clean_with_preset / clean_with_rules: row-local string transforms
    messy = spark.createDataFrame([("  Hello   World  ",)], ["note"])
    cleaned = wspark.clean_with_preset(messy, "note", "collapse_whitespace")
    assert cleaned.collect()[0]["note"] == " Hello World "
    trimmed = wspark.clean_with_preset(cleaned, "note", "trim_whitespace")
    assert trimmed.collect()[0]["note"] == "Hello World"

    digits = spark.createDataFrame([("+1 (555) 123-4567",)], ["phone"])
    only_digits = wspark.clean_with_rules(digits, "phone", [(r"[^0-9]", "")], output_column="phone_digits")
    assert only_digits.collect()[0]["phone_digits"] == "15551234567"

    # drop_duplicates: must repartition to be correct across the whole DataFrame,
    # not just within a partition. Force many partitions to actually exercise that.
    many_partitions = df.repartition(8)
    deduped = wspark.drop_duplicates(many_partitions, ["id", "name"])
    assert deduped.count() == 3, deduped.count()  # (1,alice) collapsed from 2 to 1

    without_repartition_but_default_true = wspark.drop_duplicates(df, ["id", "name"])
    assert without_repartition_but_default_true.count() == 3

    # aggregate: whole-table-correct via automatic repartition-by-group_by
    totals = wspark.aggregate(
        df.repartition(8), ["name"], [("amount", "sum", "total"), ("amount", "count", "n")]
    )
    by_name = {r["name"]: (r["total"], r["n"]) for r in totals.collect()}
    assert by_name["alice"] == (20.0, 2), by_name  # two (1, alice, 10) rows
    assert by_name["bob"] == (20.0, 1)

    try:
        wspark.aggregate(df, [], [("amount", "sum", "total")])
    except ValueError:
        pass
    else:
        raise AssertionError("expected empty group_by to raise ValueError")

    # broadcast_join: join each partition against a small driver-side table
    countries = spark.createDataFrame([(1, "NO"), (2, "SE")], ["id", "country"])
    small = wspark.collect_as_record_batch(countries)
    assert isinstance(small, pa.RecordBatch)
    joined = wspark.broadcast_join(df.repartition(4), small, ["id"], ["id"], how="left")
    joined_rows = {(r["id"], r["name"]): r["country"] for r in joined.collect()}
    assert joined_rows[(1, "alice")] == "NO"
    assert joined_rows[(3, "carol")] is None  # no country for id=3, left join keeps the row

    # schema helpers
    arrow_schema = wspark.to_arrow_schema(df)
    assert isinstance(arrow_schema, pa.Schema)
    fp = wspark.schema_fingerprint(df)
    assert isinstance(fp, int)
    diff = wspark.validate_schema(df, df)
    assert diff.is_empty()


if __name__ == "__main__":
    sys.exit(main())
