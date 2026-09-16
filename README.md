# witchhat

A high-performance data-transformation library for Spark and Databricks
workloads: a Rust core exposed to Python via PyO3, aimed at replacing
Spark-native operations (hashing, schema validation, JSON normalization,
regex-heavy cleanup, join, aggregate) with something an order of magnitude
faster on a single node, while staying byte-for-byte comparable to Spark's
output.

## Documentation

- [`docs/architecture.md`](docs/architecture.md): design and rationale.
- [`docs/api.md`](docs/api.md): the full Python and Rust callable surface.
- [`docs/operations.md`](docs/operations.md): building the wheel and installing it on
  Databricks.

Markdown in `docs/` is the source of truth; `python tools/md2docx.py` generates a
matching `.docx` for each, following the same convention as the companion project
`rust-streamer-pgdb`. Regenerate after any docs change.

## Layout

| crate | role |
| --- | --- |
| `witchhat-core` | data model and native transformations. Arrow-backed, no Python dependency |
| `witchhat-py` | PyO3 module (`witchhat._witchhat`), abi3 for Python >= 3.10, mixed layout under `python/witchhat/` |

`crates/witchhat-py/python/witchhat/spark.py` is pure Python (no Rust, no compiled
extension): the Databricks/Spark integration layer, calling the compiled functions above
from `pyspark.sql.DataFrame.mapInArrow`. Lazily imports pyspark, so it never affects
`import witchhat` outside Databricks.

`crates/witchhat-py/python/witchhat/metrics.py` is also pure Python, stdlib only:
opt-in, disabled-by-default JSON event logging (latency, row counts, throughput) wrapped
around every exported function at the `__init__.py` boundary, so `witchhat.spark` gets it
for free.

`rust-toolchain.toml` pins the Rust toolchain (rustc/rustfmt/clippy) so CI and a
developer's machine agree. `tools/smoke.py` round-trips a real pyarrow batch through the
built wheel; `tools/spark_smoke.py` does the same for `witchhat.spark` against a real
local Spark session (not in CI; pyspark is large and optional).
`.github/workflows/ci.yml` builds and verifies `x86_64` and `aarch64` (Graviton) wheels
on native runners of each.

## Quick start, Python

```python
import pyarrow as pa
import witchhat

batch = pa.record_batch({
    "id": pa.array([1, 2, 3]),
    "email": pa.array(["a@x.com", "b@x.com", "a@x.com"]),
})

# one uint64 fingerprint per row, over the given columns in order
row_hashes = witchhat.hash_rows(batch, ["id", "email"])

# order-independent: a reshuffled batch fingerprints the same
table_fp = witchhat.table_fingerprint(witchhat.hash_rows_all_columns(batch))

witchhat.schema_fingerprint(batch.schema)
witchhat.cpu_features()

# compare an incoming batch's schema against what a pipeline expects
diff = witchhat.validate_schema(batch.schema, expected_schema)
if diff.is_breaking():
    raise ValueError(f"schema mismatch: {diff!r}")

# JSON -> a fixed schema, one level of nested-object flattening
records, stats = witchhat.normalize_json(json_column, target_schema)

# regex cleanup: a named preset, or your own rules
witchhat.clean_with_preset(batch.column("note"), "collapse_whitespace")
witchhat.clean_with_rules(batch.column("phone"), [(r"[^0-9]", "")])

# compare witchhat's output against Spark's, order-independent
report = witchhat.check_equivalence(witchhat_batch, spark_batch)
assert report.is_equivalent()

# native transformations: dropDuplicates, join, and group-by aggregation
witchhat.drop_duplicates(batch, ["id", "email"])
joined = witchhat.join(users, orders, ["id"], ["user_id"], how="left")
witchhat.aggregate(joined, ["country"], [("amount", "sum", "total")])
```

Or against a real Spark DataFrame in a Databricks notebook, via `witchhat.spark`:

```python
from witchhat import spark as wspark

events = spark.table("bronze.events")
tagged = wspark.hash_rows(events, ["user_id", "event_time"])
deduped = wspark.drop_duplicates(tagged, ["user_id", "event_hash"])  # repartitions by default, for correctness
totals = wspark.aggregate(deduped, ["country"], [("amount", "sum", "total")])
```

`batch` can be anything that implements the Arrow C Data / pyarrow interface
(pyarrow, polars, pandas via `pandas.api.interchange`, DuckDB) — witchhat
never depends on pyarrow specifically, only on Arrow's in-memory format.

Build the wheel:

```
pip install maturin
cd crates/witchhat-py
maturin build --release   # or `maturin develop --release` inside a venv
```

## Design

**Arrow is the data model, not a bespoke one.** `witchhat-core` re-exports
`arrow_schema`/`arrow_array` types directly rather than inventing its own
columnar representation, because Arrow is already the interop format between
Spark, Databricks, pyarrow, polars and pandas. Zero-copy across that boundary
falls out for free.

**Row hashing is versioned, not just implemented.** [`HashVersion`] pins the
exact algorithm (type tags, null handling, float canonicalization, the
combiner) behind a name (`"v1"`). A future improvement ships as `"v2"`
instead of silently changing what `"v1"` produces, so a fingerprint computed
today is still reproducible next year. Covers booleans, all integer widths,
floats, strings, binary, `Date32/64`, `Time32/64`, `Timestamp` (any
unit/timezone), and `Decimal128/256` — a `Timestamp` with an explicit `"UTC"`
hashes differently from a naive one, even at the same raw value. See
[`crates/witchhat-core/src/hash.rs`](crates/witchhat-core/src/hash.rs).

**CPU features change speed, never output.** [`witchhat_core::cpu`] detects
AVX2/AVX-512/NEON at runtime for future SIMD paths, but any such path must be
bit-identical to the scalar fallback — a hash computed on a Databricks driver
must match one computed on a laptop, or fingerprints stop being comparable
across a mixed-hardware fleet.

**Two fingerprint shapes cover both use cases.** [`hash_batch`] hashes named
columns per-row (dedup keys, CDC identity). [`table_fingerprint`] folds a
whole batch of row hashes into one order-independent value with a
commutative combiner, so verifying witchhat's output against Spark's does
not require either side to sort first.

**Schema validation reports a diff, not a bool.** [`validate_schema`] compares
an actual schema against an expected one and returns exactly what differs
(missing, unexpected, retyped, or nullability-tightened columns), so a caller
can distinguish a breaking change from additive schema evolution instead of
being told only "matches" or "doesn't". See
[`crates/witchhat-core/src/validate.rs`](crates/witchhat-core/src/validate.rs).

**Malformed is counted, not hidden.** [`normalize_json`] parses one JSON
object per row into a fixed schema; a row that fails to parse becomes null
and is counted in `NormalizeStats.rows_malformed` rather than failing the
whole batch, and a value of the wrong JSON type is counted separately from
an ordinary absent key. See
[`crates/witchhat-core/src/json.rs`](crates/witchhat-core/src/json.rs).

**Only witchhat's own named behaviour is versioned.** [`clean_with_preset`]
looks up one of witchhat's own frozen rule sets by name; a caller's own
regex, passed to [`apply_rules`] directly, is the caller's algorithm and
isn't versioned by witchhat at all. See
[`crates/witchhat-core/src/clean.rs`](crates/witchhat-core/src/clean.rs).

**Equivalence testing reuses hashing and validation, not a third algorithm.**
[`check_equivalence`] combines `validate_schema` (schema agreement) with
`table_fingerprint` over a shared column order (row-set agreement) into one
`EquivalenceReport`, deliberately stricter than "not breaking": an extra
column fails equivalence even though it wouldn't fail schema validation. See
[`crates/witchhat-core/src/equivalence.rs`](crates/witchhat-core/src/equivalence.rs).

**Deduplication uses hashing to bucket, not to decide.** [`drop_duplicates`] is
a native transformation equivalent to Spark's `dropDuplicates`: `hash_batch`'s
composite hash groups candidate duplicates cheaply, but a `u64` collision
between two distinct rows never causes a false duplicate, since rows sharing
a hash are additionally compared with `arrow_row::RowConverter`'s exact,
byte-comparable format before either is treated as a duplicate. Filter and
project were left to Arrow's own compute kernels, which already do the job
with nothing witchhat-specific to add. See
[`crates/witchhat-core/src/dedup.rs`](crates/witchhat-core/src/dedup.rs).

**Join and aggregate use exact comparison, not a fingerprint, and exact
numeric precision, not `f64`.** A join or a group-by boundary is a
correctness question, not a "probably the same" one, so [`join`] and
[`aggregate`] key rows through
[`arrow_row::RowConverter`](https://docs.rs/arrow-row) instead of
`hash_batch`: two rows compare equal only when they truly are, not when
their `u64` fingerprints happen to collide. [`join`] excludes null keys from
matching by default, the same as Spark/SQL (`NULL = NULL` is never true);
[`join_null_safe`] is the explicit opt-in for the opposite.
[`aggregate`]'s `Sum`/`Mean`/`Min`/`Max` accumulate and compare at each
numeric column's own exact precision (`i128`/`u128`/native `Decimal128`/
`Decimal256` mantissa) instead of a universal `f64` downcast, so an integer
value or sum beyond `f64`'s exact range (±2^53) stays exact and two distinct
large integers never compare equal by accident. See
[`crates/witchhat-core/src/join.rs`](crates/witchhat-core/src/join.rs) and
[`crates/witchhat-core/src/aggregate.rs`](crates/witchhat-core/src/aggregate.rs).

**`witchhat.spark` defaults to correct, not just fast.** The
[`drop_duplicates`](crates/witchhat-py/python/witchhat/spark.py)/`aggregate`
Spark wrappers repartition by the relevant columns before calling the
underlying kernel unless told not to — `mapInArrow` hands them one Spark
partition at a time with no visibility across partitions, so that default is
what makes the result correct for the whole DataFrame, not merely a
convenience. `broadcast_join` mirrors Spark's own broadcast-join
optimization rather than attempting a distributed shuffle join, which
witchhat (not a distributed engine) cannot do, and rejects
`how="right"`/`"full"` outright (`ValueError`): broadcasting the small side
independently to every partition makes those two modes structurally unsound,
not just discouraged. See
[`crates/witchhat-py/python/witchhat/spark.py`](crates/witchhat-py/python/witchhat/spark.py).

## Not built yet

Composite hashing, schema validation, JSON normalization, regex cleanup,
output-equivalence testing, deduplication, join, aggregate, a
Databricks/Spark integration layer (`witchhat.spark`), and opt-in JSON metric
logging (`witchhat.metrics`) are done. What's left:

1. **Reproducible-build check.** manylinux abi3 wheels build + CI for both
   `x86_64` and `aarch64` (Graviton) (see `.github/workflows/ci.yml`); still
   need a same-inputs -> byte-identical-wheel check.
2. **Broader `aggregate` type support.** `Sum`/`Mean`/`Min`/`Max` now cover
   `Decimal128`/`Decimal256` alongside the numeric types (2026-09-16); string
   min/max and `Date`/`Time`/`Timestamp` aggregation are still unbuilt.
3. **Deeper JSON normalization.** `normalize_json` supports one level of
   nested-object flattening; array-valued fields and deeper nesting are not
   yet handled.
4. **A distributed shuffle join through witchhat.** `witchhat.spark.broadcast_join`
   only covers the broadcast pattern (a small side collected to the driver);
   a large-large join has no witchhat-provided path and is left to Spark's own
   `DataFrame.join`, deliberately (see `docs/architecture.md` Chapter XVI,
   Section 5).

Package-repository publishing and live Databricks Volumes verification were
raised and deliberately decided against (2026-09-16): the project stays
wheel-only, and the Volumes install path (`docs/operations.md` Chapter III)
is documented as the standard, sufficient procedure rather than something
verified against a live workspace. See `docs/operations.md` Chapter VI,
Section 2 for the reasoning.

Type hints and generated docs (`.pyi` stubs, `py.typed`, `docs/*.md` + generated
`.docx`) are done; see the Documentation section above.

## Correctness fixes from external review

An external (Codex) code review on 2026-09-16 found real correctness issues in code that
was already implemented and, until then, only documented as a caveat rather than fixed.
All twelve findings were addressed the same day (five highest-priority, then seven
lower-priority):

- `drop_duplicates` no longer treats a hash collision as row equality (falls back to an
  exact `arrow_row` comparison within a hash bucket).
- `join` excludes null keys from matching by default, the same as Spark, with the new
  `join_null_safe` as an explicit opt-in for null-matches-null.
- `aggregate`'s `Sum`/`Mean`/`Min`/`Max` accumulate and compare at each numeric column's
  own exact precision (`i128`/`u128`/native decimal mantissa, plus new `Decimal128`/
  `Decimal256` support) instead of downcasting through `f64`.
- `witchhat.spark.broadcast_join` rejects `how="right"`/`"full"` outright instead of
  merely documenting why they were unsound.
- Every PyO3-bound kernel call releases the GIL for its duration.
- `witchhat.spark.drop_duplicates`/`aggregate` stream per Arrow batch instead of
  buffering a whole partition first (a new `max_distinct_keys` guards `drop_duplicates`
  against unbounded growth on a skewed partition).
- `check_equivalence` gained a real `exact` mode: a genuine sorted-row comparison, not
  just a fingerprint, via `EquivalenceReport.rows_exactly_match`.
- The Rust-`regex`-versus-Spark/Java-regex dialect gap is now documented explicitly
  (`witchhat_core::clean`'s docs), with a test confirming lookaround/backreferences are
  rejected at compile time, not silently different.
- `tools/spark_benchmark.py` (new) times a complete Spark action through `witchhat.spark`
  against Spark's own native operator and prints both numbers honestly; on this local
  sandbox witchhat's `mapInArrow` path was measurably *slower*, driven by JVM/Arrow
  conversion overhead, not kernel speed, which the script exists to surface, not hide.
- `witchhat.spark`/`mapInArrow` now has real CI coverage (a `spark-integration` job).
- Root `Cargo.toml` no longer sets `panic = "abort"`, so a Rust panic raises a catchable
  Python exception instead of killing the whole worker (PyO3 already had the machinery
  for this; the release profile was silently defeating it).
- Wheels now carry a commit-SHA stamp (`witchhat.__commit__`) and a `SHA256SUMS` file;
  a full SBOM and a cryptographic signature are **not** done, and not claimed as done,
  since both need infrastructure (a signing key/OIDC identity, an SBOM generator) this
  project has not provisioned.

See `CLAUDE.md`'s "Open items" for exactly what changed in each case.

## Tests

```
cargo test --workspace
```

81 unit tests plus 15 doctests, all in `witchhat-core`, covering: hash determinism,
column-order sensitivity, null-vs-value distinctness, type-tag collision avoidance
(including the Date/Time/Timestamp/Decimal types added alongside join/aggregate), float
canonicalization; schema-diff correctness (missing/unexpected/retyped/nullability,
numeric widening opt-in); JSON normalization (malformed rows, type mismatches, absent
vs. explicit-null, nested-path extraction); regex cleanup (every built-in preset, rule
ordering, invalid-pattern rejection, lookaround/backreference rejection); output
equivalence (identical, reordered, different-value, different-row-count,
column-order-independent, explicit-column-subset batches, fast-versus-exact mode);
deduplication (first-occurrence order, composite keys, unknown columns); join
(inner/left/right/full, composite keys, name-collision suffixing, key-type mismatch,
null-key exclusion versus `join_null_safe`); and aggregate (sum/count/min/max per group,
exact numeric precision beyond `f64`'s ±2^53 range, `Decimal128` type preservation,
sum-overflow rejection, whole-table and empty-batch aggregation, unsupported-type
rejection).
`cargo doc --no-deps -p witchhat-core` and `cargo clippy --workspace --all-targets`
both run clean with warnings denied (`missing_docs`, `broken_intra_doc_links`, clippy's
default lint set); see `.github/workflows/ci.yml`.
