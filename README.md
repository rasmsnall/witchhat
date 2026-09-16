# witchhat

A high-performance data-transformation library for Spark and Databricks
workloads: a Rust core exposed to Python via PyO3, aimed at replacing
Spark-native operations (hashing, schema validation, JSON normalization,
regex-heavy cleanup) with something an order of magnitude faster on a single
node, while staying byte-for-byte comparable to Spark's output.

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

`rust-toolchain.toml` pins the Rust toolchain (rustc/rustfmt/clippy) so CI and a
developer's machine agree. `tools/smoke.py` round-trips a real pyarrow batch through the
built wheel; `.github/workflows/ci.yml` runs it as part of the manylinux wheel build.

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
today is still reproducible next year. See
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

## Not built yet

The core data model, composite row hashing, and schema validation are done.
Ordered by what the stated goal needs next:

1. **JSON normalization.** Flatten/normalize nested JSON columns into a
   fixed Arrow schema, the usual first step before anything else in the
   pipeline can run.
2. **Regex-heavy cleanup.** A transform kernel for the regex-based
   normalization rules that currently live in Spark UDFs, with the same
   versioning discipline as hashing.
3. **Output equivalence testing.** A harness built on `table_fingerprint`
   plus per-column diffing, so a witchhat pipeline and its Spark equivalent
   can be asserted equal in CI.
4. **Native transformations.** The actual replacements for Spark operations
   (filter/project/join/aggregate paths), the point of the exercise.
5. **Reproducible-build check.** manylinux abi3 wheel build + CI are done (see
   `.github/workflows/ci.yml`); still need a same-inputs -> byte-identical-wheel check.
6. **Publish to a package repository.** Currently wheel-only, no index; see
   `docs/operations.md` Chapter III.
7. **Databricks Volumes distribution.** Confirm `pip install` from a Unity
   Catalog volume path works with the abi3 wheel as built (documented as the intended
   path in `docs/operations.md`, not yet verified against a real workspace).

Type hints and generated docs (`.pyi` stubs, `py.typed`, `docs/*.md` + generated
`.docx`) are done; see the Documentation section above.

## Tests

```
cargo test --workspace
```

20 unit tests plus 7 doctests, all in `witchhat-core`: hash determinism, column-order
sensitivity, null-vs-value distinctness, type-tag collision avoidance, float
canonicalization (NaN, -0.0), unknown-column errors, order-independent table
fingerprints, schema-fingerprint sensitivity to field order/type/nullability, and
schema-diff correctness (missing/unexpected/retyped/nullability, numeric widening
opt-in, narrowing and cross-signedness always rejected).
`cargo doc --no-deps -p witchhat-core` and `cargo clippy --workspace --all-targets`
both run clean with warnings denied (`missing_docs`, `broken_intra_doc_links`, clippy's
default lint set); see `.github/workflows/ci.yml`.
