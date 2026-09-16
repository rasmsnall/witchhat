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

# JSON -> a fixed schema, one level of nested-object flattening
records, stats = witchhat.normalize_json(json_column, target_schema)

# regex cleanup: a named preset, or your own rules
witchhat.clean_with_preset(batch.column("note"), "collapse_whitespace")
witchhat.clean_with_rules(batch.column("phone"), [(r"[^0-9]", "")])

# compare witchhat's output against Spark's, order-independent
report = witchhat.check_equivalence(witchhat_batch, spark_batch)
assert report.is_equivalent()

# the first native transformation: dropDuplicates, built on hash_rows
witchhat.drop_duplicates(batch, ["id", "email"])
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

**Deduplication builds on hashing instead of reinventing row comparison.**
[`drop_duplicates`] is the first native transformation, equivalent to
Spark's `dropDuplicates`: one pass with `hash_batch` as the dedup key.
Filter and project were left to Arrow's own compute kernels, which already
do the job with nothing witchhat-specific to add. See
[`crates/witchhat-core/src/dedup.rs`](crates/witchhat-core/src/dedup.rs).

## Not built yet

Composite hashing, schema validation, JSON normalization, regex cleanup,
output-equivalence testing, and deduplication are done. Ordered by what the
stated goal needs next:

1. **Join and aggregate.** The two relational operations still needed for
   "native transformations" to be a real Spark replacement. Filter and
   project already exist as Arrow compute kernels with nothing
   witchhat-specific to add.
2. **Reproducible-build check.** manylinux abi3 wheel build + CI are done (see
   `.github/workflows/ci.yml`); still need a same-inputs -> byte-identical-wheel check.
3. **Publish to a package repository.** Needs a PyPI (or internal index)
   account and an upload credential this repository's automation does not
   hold; a package upload is one-way, so this is left to a human running it
   deliberately rather than attempted by default. See `docs/operations.md`
   Chapter VI, Section 2 for what's needed before this can happen.
4. **Databricks Volumes distribution — verify, not just document.** The
   `/Volumes/...` install path is written up in `docs/operations.md`
   Chapter III, but has not been run against a real Databricks workspace;
   this development environment has none. See `docs/operations.md`
   Chapter VI, Section 2.

Type hints and generated docs (`.pyi` stubs, `py.typed`, `docs/*.md` + generated
`.docx`) are done; see the Documentation section above.

## Tests

```
cargo test --workspace
```

49 unit tests plus 12 doctests, all in `witchhat-core`, covering: hash determinism,
column-order sensitivity, null-vs-value distinctness, type-tag collision avoidance,
float canonicalization; schema-diff correctness (missing/unexpected/retyped/nullability,
numeric widening opt-in); JSON normalization (malformed rows, type mismatches, absent
vs. explicit-null, nested-path extraction); regex cleanup (every built-in preset, rule
ordering, invalid-pattern rejection); output equivalence (identical, reordered,
different-value, different-row-count, column-order-independent, explicit-column-subset
batches); and deduplication (first-occurrence order, composite keys, unknown columns).
`cargo doc --no-deps -p witchhat-core` and `cargo clippy --workspace --all-targets`
both run clean with warnings denied (`missing_docs`, `broken_intra_doc_links`, clippy's
default lint set); see `.github/workflows/ci.yml`.
