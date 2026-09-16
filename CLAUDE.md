# witchhat

Python library, implemented in Rust, of native data-transformation kernels aimed at
Spark/Databricks workloads. Status: **eight kernels implemented end to end** (composite
hashing, schema validation, JSON normalization, regex cleanup, output-equivalence
testing, deduplication, join, aggregate), plus a pure-Python `witchhat.spark` layer
bridging them to `pyspark.sql.DataFrame`, plus `aarch64` (Graviton) wheels alongside
`x86_64` in CI, plus opt-in JSON metric logging (`witchhat.metrics`) wrapping every
kernel including through `witchhat.spark`. Filter/project need no witchhat kernel
(Arrow's own compute kernels already cover them). Remaining open items are broader
`aggregate` type support and deeper JSON normalization; package-repo publishing and live
Databricks Volumes verification were raised and explicitly closed as not pursued (see
"Open items" below).

A round of external code review (Codex) on 2026-09-16 raised concrete correctness gaps
in several of the "resolved and shipped" items below (dedup hash-collision risk,
`broadcast_join` right/full semantics, join null-key semantics, aggregate `f64`
precision, PyO3 GIL retention, plus several lower-priority items: partition-buffering
memory, equivalence testing being probabilistic-only, Rust-vs-Spark regex dialect,
unproven Spark-integration performance/CI coverage, `panic = "abort"` worker safety,
artifact provenance). The five highest-priority items (everything except the
lower-priority list) were fixed the same day; see "Resolved and shipped" below for what
changed and "Still open" for the lower-priority items, which were raised but not acted
on yet.

## Goals / constraints (from the user, verbatim intent)

- Rust core, Python bindings. Target consumer is Databricks (Spark).
- Optimised, high performance: single-node throughput is the point, not distributed
  scale.
- Native Kernel Library: manylinux wheels, `abi3-py310` compatibility, so one wheel
  loads on every Databricks Runtime's Python from 3.10 onward.
- Type hints and generated docs, following the same convention as the companion project
  `rust-streamer-pgdb` (`D:\ruststreamer\rust-streamer-pgdb`): Markdown in `docs/` as the
  single source of truth, Word (`.docx`) generated from it via `tools/md2docx.py`, and
  full rustdoc (`missing_docs` denied) on every public Rust item.
- Versioned transformation behaviour: an algorithm, once shipped under a name, never
  changes what it produces. Applies per kernel: `HashVersion` (hashing), `NormalizeVersion`
  (JSON), `CleanupVersion` (named cleanup presets only, never a caller's own regex).
- CPU feature detection with a portable fallback, and the stronger constraint that
  follows from it: a SIMD-accelerated path may change speed, never output. See
  `crates/witchhat-core/src/cpu.rs` and `docs/architecture.md` Chapter XI.
- Wheels usable from Databricks Volumes or a package repository; reproducible builds.
- Generic framework for: composite hashing, schema validation, JSON normalization,
  regex-heavy cleanup, output-equivalence testing, and native transformations
  (deduplication, join, aggregate) — all done — with the eventual goal of replacing
  Spark operations outright. Join/aggregate deliberately do *not* reuse `hash_batch`:
  see "Layout" below.

## Pivot history

The repository started 2026-09-15 as a monitoring framework (API/HTTP checks, logs,
metrics through one Rust event pipeline). On 2026-09-16 the user redirected it entirely
to the Spark/Databricks data-transformation goal above, an explicit "fresh pivot" choice
(confirmed via an explicit either/or question) over "keep the monitoring code and add
this alongside it". The `witchhat-pipeline` and `witchhat-probe` crates (event batching,
HTTP probe source) were deleted; only the workspace scaffolding, `abi3-py310` PyO3 setup,
and maturin build config carried over.

## Layout

```
crates/witchhat-core/src/lib.rs        crate docs, #![forbid(unsafe_code)], #![warn(missing_docs)]
crates/witchhat-core/src/schema.rs     re-exported Arrow schema types, schema_fingerprint
crates/witchhat-core/src/hash.rs       HashVersion, hash_batch, hash_batch_all_columns, table_fingerprint
crates/witchhat-core/src/validate.rs   ValidateSchemaOptions, SchemaDiff, validate_schema
crates/witchhat-core/src/json.rs       NormalizeVersion, NormalizeStats, normalize_json
crates/witchhat-core/src/clean.rs      CleanRule, CleanupVersion, apply_rules, preset, clean_with_preset
crates/witchhat-core/src/equivalence.rs EquivalenceOptions, EquivalenceReport, check_equivalence
crates/witchhat-core/src/dedup.rs      drop_duplicates (hash_batch buckets, arrow_row confirms)
crates/witchhat-core/src/join.rs       JoinType, join, join_null_safe (built on arrow_row, not hash_batch)
crates/witchhat-core/src/aggregate.rs  AggFunc, Aggregation, aggregate (arrow_row grouping, exact per-type numeric accumulation)
crates/witchhat-core/src/cpu.rs        CpuFeatures, features()
crates/witchhat-core/src/error.rs      Error, Result
crates/witchhat-py/src/lib.rs          crate docs + pyo3 module shell (_witchhat)
crates/witchhat-py/src/python.rs       the actual pyo3 bindings (register())
crates/witchhat-py/python/witchhat/__init__.py    re-exports _witchhat, __all__, __version__
crates/witchhat-py/python/witchhat/__init__.pyi   type stubs, one docstring per export
crates/witchhat-py/python/witchhat/py.typed
crates/witchhat-py/python/witchhat/spark.py       pure Python, no Rust: mapInArrow bridge to pyspark
crates/witchhat-py/python/witchhat/metrics.py     pure Python, stdlib only: opt-in JSON latency/throughput logging
crates/witchhat-py/pyproject.toml      maturin config (abi3-py310, mixed layout)
rust-toolchain.toml   pins rustc/rustfmt/clippy to one version
README.md
docs/architecture.md
docs/api.md
docs/operations.md
tools/md2docx.py      generates docs/*.docx from docs/*.md
tools/smoke.py        round-trips a real pyarrow batch through the built wheel, every function
tools/spark_smoke.py  same, for witchhat.spark, against a real local pyspark session
tools/metrics_smoke.py  verifies witchhat.metrics event shape/enable-disable against the built wheel
.github/workflows/ci.yml   fmt+clippy+test+doc job, manylinux abi3 wheel matrix (x86_64+aarch64), multi-interpreter matrix
```

Two-crate split (`witchhat-core` has no PyO3 dependency; `witchhat-py` is a thin
translation layer over it), unlike `rust-streamer-pgdb`'s single-crate layout. Deliberate
here: a future Rust-only consumer links `witchhat-core` without pulling in `pyo3` at all.
See `docs/architecture.md` Chapter XV for the full rationale, including why
`witchhat-py` pins `pyo3 = "=0.25.1"` exactly (arrow's `pyarrow` Cargo feature links
`pyo3-ffi` and tolerates only one exact version across the dependency graph; bumping pyo3
later needs arrow to catch up first, not just an edited version string) and why every
typed array crosses the PyO3 boundary via `ArrayData` (`u64_array_*`/`string_array_*`
helpers in `python.rs`) since `PyArrowType<T>` isn't `Clone` and arrow's pyarrow bridge
doesn't implement `ToPyArrow`/`FromPyArrow` for typed arrays directly.

`join`/`aggregate` use `arrow_row::RowConverter` for key/group comparison, not
`hash_batch`: a `u64` fingerprint collision is an acceptable, documented risk for
`table_fingerprint`/`check_equivalence` (weaker evidence toward a yes/no a human or CI
job interprets), but would silently produce *wrong data* in a join or a group-by, so
those two kernels use `arrow_row`'s exact byte-comparable row format instead. See
`docs/architecture.md` Chapter IX, Section 2.

**pyspark gotcha found and worked around, worth remembering**: `pyspark.sql.types.
StructType.add()` mutates and returns `self`, not a copy. Calling it on a live
`DataFrame`'s own `df.schema` (`df.schema.add(...)`) silently corrupts that `df`:
`df.columns` afterward reports the added field even though the JVM plan was never given
it, and the *next* `mapInArrow` call on that `df` fails with an unresolved-column error
naming the field that was just "added". Found by reproducing it in bare pyspark with no
witchhat code involved at all. `witchhat/spark.py`'s `_with_field` helper works around it
by building a fresh `StructType` from `list(schema.fields)` instead; see
`docs/architecture.md` Chapter XVI, Section 4. If a future output-schema helper needs a
modified `StructType`, go through `_with_field`, never `.add()` on anything the caller
still holds.

## Dependency budget

| Crate | Why |
|---|---|
| `arrow-array`, `arrow-schema` | The data model. `witchhat-core` depends on these only, no pyo3 |
| `arrow-select` | `filter_record_batch`/`take`, underlying `drop_duplicates`, `join`, `aggregate` |
| `arrow-row` | `RowConverter`, underlying `join`/`aggregate`'s exact key/group comparison and `drop_duplicates`'s hash-collision fallback |
| `arrow-buffer` | `i256`, `aggregate`'s `Decimal256` accumulator/comparator |
| `arrow-data` | `ArrayData`, the untyped array arrow's pyarrow bridge actually converts (witchhat-py only) |
| `arrow` (feature `pyarrow`) | Zero-copy `RecordBatch`/`ArrayData` <-> pyarrow conversion (witchhat-py only) |
| `xxhash-rust` (feature `xxh3`) | Per-value hash function underlying `hash_batch` |
| `serde_json` | JSON parsing for `normalize_json` |
| `regex` | Pattern compilation/replacement for `clean` |
| `thiserror` | The `Error` enum |
| `pyo3` | Python bindings (witchhat-py only); pinned to `=0.25.1`, see above |

Pinned 2026-09 (probed via `cargo build`; crates.io index reachable): `arrow 56.2.1`,
`xxhash-rust 0.8.18`, `serde_json 1.0.151`, `regex 1.13.1`, `thiserror 2.0.20`,
`pyo3 0.25.1`.

## Documentation standard

Same standard as `rust-streamer-pgdb`, adopted 2026-09-16 at the user's request:

- **Rustdoc (`///`, `//!`) documents the API.** Mandatory on every public item: what it
  does, every parameter and return value, an `# Errors` section, a `# Panics` section (or
  an explicit statement that it does not panic), whether it blocks/is async, and a
  compiling example where one is practical. Enforced with `#![warn(missing_docs)]` and
  `#![warn(rustdoc::broken_intra_doc_links)]` in `witchhat-core`.
- **Inline comments (`//`) stay rare.** Only non-obvious *why*, never restating *what*.
  See [[code-comment-style]] for the general house style this follows.
- **Python surface**: docstrings on every exported function plus a `.pyi` type stub
  (`crates/witchhat-py/python/witchhat/__init__.pyi`).
- **Long-form documents** are authored as Markdown in `docs/`, single source of truth.
  Word (`.docx`) is generated from that Markdown by `tools/md2docx.py`, never
  hand-edited. Regenerate after any docs change: `python tools/md2docx.py`. Requires
  `python-docx` (1.2.0 present locally); no pandoc on this machine.
- **Formatting follows South Korean university thesis convention**: chapters in Roman
  numerals, sections `1.`/`2.` restarting per chapter, table captions above the table
  (`<Table C-N>`), figure captions below the figure (`[Figure C-N]`), a numbered
  reference list, front matter listing tables/figures.
- **No em dashes**, anywhere: docs, code comments, commit messages, error strings.

| Document | Contents |
|---|---|
| `docs/architecture.md` | Data model, per-kernel design (Chapters III-X), versioning/CPU-feature discipline, concurrency/failure/security model, dependency budget, what's not built yet |
| `docs/api.md` | Python and Rust surface, parameter semantics, worked examples |
| `docs/operations.md` | Building and installing the wheel, sizing per kernel, what's deferred because unbuilt vs. deferred pending access this environment lacks (Chapter VI) |

Chapter numbers shift as kernels are added; always check the Contents section of the
`.md` file itself rather than trusting a remembered chapter number from an old session.

## CI

Adopted the same shape as `rust-streamer-pgdb`'s `.github/workflows/ci.yml`: a
format/clippy/test/doc job, a manylinux abi3 wheel job (matrixed over `x86_64` on
`ubuntu-latest` and `aarch64` on `ubuntu-24.04-arm`, a real native ARM64 runner, not
QEMU) that verifies the `cp310-abi3` tag and architecture tag and round-trips a real
pyarrow batch through the wheel it just built (`tools/smoke.py`), and a matrix job
installing the `x86_64` wheel on Python 3.10/3.12/3.13 to prove the abi3 promise holds
across interpreters (not duplicated for `aarch64`: the wheel job's own native-runner
round-trip already proves that wheel executes; doubling the interpreter matrix onto ARM
runners would roughly double this job's runtime for the same signal). No optional Cargo
features exist yet (unlike pgdb's `azure`/`fast-gzip`/`zstd`), so the workspace-wide
commands need no feature flags. `witchhat.spark` is pure Python and not covered by this
CI at all (see `tools/spark_smoke.py` instead, run manually).

## Open items

Resolved and shipped:

- Composite row/table hashing (`hash.rs`), versioned, type-tagged, null- and
  float-canonicalized.
- Schema validation (`validate.rs`): `SchemaDiff` (missing/unexpected/retyped/
  nullability-tightened), opt-in conservative numeric widening,
  `is_breaking()`/`is_empty()`.
- JSON normalization (`json.rs`): `normalize_json` parses one JSON object per row into a
  flat/one-level-dotted-path target schema (`utf8`/`int64`/`float64`/`boolean` only);
  `NormalizeStats` distinguishes malformed rows (counted) from absent/explicit-null
  (ordinary, uncounted) from wrong-type-present (counted per column). Nested objects
  beyond one level and arrays are not supported (fall into the type-mismatch case).
- Regex cleanup (`clean.rs`): `apply_rules` for caller-supplied rules (unversioned, since
  they're the caller's own algorithm), `preset`/`clean_with_preset` for five named,
  versioned built-ins (`trim_whitespace`, `collapse_whitespace`,
  `strip_control_characters`, `strip_non_alphanumeric`, `digits_only`).
- Output equivalence testing (`equivalence.rs`): `check_equivalence` combines
  `validate_schema` + `table_fingerprint` over a column order shared between both sides
  (so declared column order alone doesn't cause a false mismatch); `is_equivalent()` is
  deliberately stricter than `SchemaDiff::is_breaking()` (an extra column fails it).
- Deduplication (`dedup.rs`): `drop_duplicates`, using `hash_batch` as a bucketing key
  plus `arrow_row::RowConverter` to confirm exact equality within a bucket (see the
  2026-09-16 correctness-fix bullet below for why: a bucket alone would let a hash
  collision drop a valid row) plus `arrow_select::filter::filter_record_batch`.
  Filter/project were deliberately *not* wrapped: Arrow's own kernels already do the job.
- Join (`join.rs`): `join`/`join_null_safe` (inner/left/right/full), keyed via
  `arrow_row::RowConverter` rather than `hash_batch` (correctness, not just speed: see
  "Layout" above). Colliding right-side column names get suffixed `_right`; every output
  field is nullable. Right/left key columns require an exact Arrow type match, no
  implicit coercion. `join` excludes null keys (Spark semantics); `join_null_safe` is
  the opt-in for null-matches-null (see the fix bullet below).
- Aggregate (`aggregate.rs`): `aggregate` with `Count`/`Sum`/`Mean`/`Min`/`Max`, grouped
  via the same `arrow_row` approach as `join`. Numeric accumulation/comparison is
  type-specific (`i128`/`u128`/native decimal mantissa), not a universal `f64` downcast;
  see the fix bullet below. Empty `group_by` means whole-table aggregate; an empty batch
  in that case still produces one row (`Count` 0, others null), not zero rows.
- Widened `hash_batch`/`dedup`/`check_equivalence` type coverage: added `Date32/64`,
  `Time32/64`, `Timestamp` (any unit/timezone), `Decimal128/256`. Parameterized types
  fold their parameters (unit, timezone, precision, scale) into the hash's type key, not
  just a tag byte, so e.g. `Timestamp(Microsecond, None)` and `Timestamp(Microsecond,
  Some("UTC"))` hash differently at the same raw value — same naive-vs-UTC lesson
  `rust-streamer-pgdb` learned the hard way, noted in its own `CLAUDE.md`.
- `schema_fingerprint`, `witchhat_core::cpu::features()`.
- The `witchhat-py` mixed maturin layout (`python/witchhat/`), type stubs, `py.typed`,
  full Python bindings for every kernel above (`SchemaDiff.retyped` and
  `EquivalenceReport.schema_diff` both return/nest real `pyarrow.DataType`/`SchemaDiff`
  objects, not strings/dicts; `join`/`aggregate` take `how`/`func` as plain strings,
  parsed via `JoinType::parse`/`AggFunc::parse` at the boundary).
- The wheel builds (`maturin build --release`) and was verified against a real
  `pyarrow.RecordBatch` (`tools/smoke.py` exercises every exported function), not just
  the Rust unit tests.
- Full rustdoc on every public `witchhat-core` item; `cargo doc --no-deps -D warnings`
  and `cargo clippy --all-targets -- -D warnings` both clean. 68 Rust unit tests plus 14
  doctests, all passing.
- `rust-toolchain.toml` pinned; CI and docs/CI conventions adopted from
  `rust-streamer-pgdb`.
- `witchhat.spark` (`spark.py`, pure Python, no Rust changes): `hash_rows`/
  `clean_with_preset`/`clean_with_rules` (row-local), `drop_duplicates`/`aggregate`
  (partition-coordinating, buffer a whole partition via `pa.concat_batches`, default
  `repartition=True` for whole-DataFrame correctness, `aggregate` refuses an empty
  `group_by`), `broadcast_join`/`collect_as_record_batch` (broadcast pattern, not a
  shuffle join), `to_arrow_schema`/`validate_schema`/`schema_fingerprint` (schema
  helpers). Verified: schema-level logic directly against a real local `SparkSession`
  (schema conversion, the `_with_field` fix, aggregate/broadcast-join output-schema
  construction). Not fully verified: full `mapInArrow` execution end to end in this
  session's sandbox, blocked by a local JDK 17+/21-vs-bundled-Arrow-Java incompatibility
  confirmed to affect bare `pyspark` (`mapInPandas` with no witchhat code at all), not
  specific to witchhat and not expected on Databricks; see Environment notes and
  `tools/spark_smoke.py`.
- ARM64 wheel: CI now builds and verifies `aarch64` alongside `x86_64`, both on native
  runners (see "CI" above).
- Metric logging (`metrics.py`): opt-in, disabled by default, zero overhead when off.
  `enable(sink=...)`/`disable()`/`is_enabled()` plus a `measure()` context manager
  emitting one flat JSON event per call (`function`, `ts`, `status`, `duration_ms`,
  `rows_in`/`rows_out`/`rows_per_second` where applicable, caller context; errors record
  `status: "error"` and `f"{type(exc).__name__}: {exc}"`, never a full traceback, so a
  row-derived value can't leak into a log sink). Wrapped at the `__init__.py` boundary
  around every kernel, so `witchhat.spark` inherits it transitively without its own
  instrumentation code. `tools/metrics_smoke.py` verifies event shape and the
  enable/disable toggle against the built wheel; wired into CI's wheel round-trip step.
- Review-driven correctness fixes (2026-09-16, same day as the review), all five
  highest-priority items:
  - `drop_duplicates` (`dedup.rs`) no longer trusts a `hash_batch` collision as
    equality: rows sharing a hash are bucketed, then confirmed with
    `arrow_row::RowConverter`'s exact comparison (the same one `join`/`aggregate`
    already used) before either is treated as a duplicate.
  - `join` (`join.rs`) now excludes null keys from matching by default, the same as
    Spark/SQL (`NULL = NULL` is never true); `join_null_safe` is the new, separately
    named, explicit opt-in for null-matches-null. Both share one internal
    implementation (`join_impl`) that applies or skips the exclusion.
  - `aggregate.rs` was rewritten around an internal `Num` type: `Sum`/`Mean` accumulate
    and `Min`/`Max` compare at each numeric column's own exact precision (`i128` for
    signed integers, `u128` for unsigned, native mantissa width for `Decimal128`/
    `Decimal256`, added as newly-supported input types), never through a universal
    `f64` downcast. `Sum`'s output type now follows the source (`Int64`/`UInt64`/
    `Float64`/the source's own decimal type) instead of always `Float64`; `Sum`'s
    accumulator overflowing is a new `Error::Overflow`, a hard error, not silent
    wraparound. `Mean`'s single final division for a non-decimal source still uses
    `f64` (unavoidable: the result is generally fractional), but the summation feeding
    it is exact. Needed a new `arrow-buffer` dependency for `i256` (`Decimal256`'s
    mantissa type).
  - `witchhat.spark.broadcast_join` (`spark.py`) now raises `ValueError` outright for
    `how="right"`/`"full"` instead of only documenting the unsoundness: `small_table`
    is broadcast independently to every partition, so an unmatched row under those two
    modes would previously have surfaced once per partition, silently, instead of once
    overall. Also required fixing `_aggregate_output_schema` in the same file, a
    regression the aggregate rewrite caused: it still declared `DoubleType` for every
    `"sum"`/`"mean"` output, which no longer matches an integer/decimal source's new
    exact output type and broke real `mapInArrow` execution (`getDouble` called on an
    `Int64` Arrow array) — caught by actually running `tools/spark_smoke.py`, not by
    the Rust test suite, which is exactly the "Spark integration is not tested in CI"
    gap the review separately flagged.
  - PyO3 bindings (`witchhat-py/src/python.rs`) now release the GIL for every kernel
    call via `Python::allow_threads` (confirmed correct for the pinned `pyo3 0.25.1`;
    `Python::detach` is a 0.29+ rename, do not use it here). Safe because pyo3 has
    already finished extracting every argument into an owned Rust value before a
    function's body runs, so the kernel call itself never touches a Python object.

Still open, lower-priority items from the same review round, not yet triaged into
concrete fixes: whole Spark
  partitions are buffered in memory (`drop_duplicates`/`aggregate` in `spark.py`, via
  `pa.concat_batches`) rather than streamed; `check_equivalence` is fingerprint-only
  (probabilistic), with no `exact` mode (schema + sorted/full row comparison); Rust's
  `regex` crate syntax is not Spark/Java regex syntax and the gap is undocumented; no
  benchmark proves Spark-side wins net of JVM/Arrow conversion and `mapInArrow` overhead;
  `witchhat.spark`/`mapInArrow` has no CI coverage, only the manual `tools/spark_smoke.py`;
  root `Cargo.toml`'s `panic = "abort"` release profile means an unexpected Rust panic
  kills the whole Python worker instead of raising a catchable exception; wheels carry no
  provenance (checksums, source commit SHA, SBOM, signing) linking them to the source they
  were built from.

Still open, pre-existing:

- Broader `aggregate` type support: `Decimal128`/`Decimal256` are now supported
  (2026-09-16 fix, see above); string min/max and `Date`/`Time`/`Timestamp` aggregation
  are still unbuilt.
- Deeper JSON normalization: array-valued fields and more than one level of object
  nesting are unbuilt (fall into the type-mismatch case in `normalize_json` today).
- A distributed shuffle join through witchhat: `witchhat.spark.broadcast_join` only
  covers the broadcast pattern; deliberately not pursuing a large-large join wrapper,
  since Spark's own `DataFrame.join` already does that better (`docs/architecture.md`
  Chapter XVI, Section 5).
- Full live execution testing of `witchhat.spark`'s `mapInArrow` path, blocked in this
  session by a local JDK/pyspark JVM issue, not by anything in the code; see the
  "Resolved and shipped" entry above and Environment notes.
- **Closed, not open**: publishing to a package repository, and live-verifying
  installation from a Databricks Unity Catalog Volume. Both were raised, then the user
  explicitly said to skip package-repo publishing and to leave Volumes install as
  documented rather than pursued (2026-09-16). The project stays wheel-only; the
  `/Volumes/...` install path is documented in `docs/operations.md` Chapter III as the
  standard, sufficient Databricks procedure, not something this repository verifies
  live. See `docs/operations.md` Chapter VI, Section 2 for the reasoning. Do not
  re-open either as a backlog item without the user asking again.
- Whether/when a kernel becomes expensive enough to justify releasing the GIL
  (`docs/architecture.md` Chapter XII); none does yet, though JSON normalization and
  regex cleanup are the most CPU-intensive kernels so far per input byte
  (`docs/operations.md` Chapter IV, Section 5).

## Environment notes

- Rust 1.94.0 (pinned via `rust-toolchain.toml`), Python 3.10.11, maturin 1.15.0,
  python-docx 1.2.0, JDK 21 (Temurin).
- `cargo fmt --all` reformats aggressively; run it after any hand-edit to `.rs` files
  before committing, since CI checks `cargo fmt --all --check`.
- No `gh` CLI in this environment (neither Git Bash nor PowerShell `PATH`). Pushing to
  GitHub uses `git push` directly against an `origin` remote the user creates and shares
  the URL/name for; this session cannot create a GitHub repo itself.
- **Local pyspark cannot fully execute `mapInArrow`/`mapInPandas`/any Arrow-based Python
  UDF in this environment** (tried pyspark 3.5.9 and 4.2.0, both fail identically):
  `UnsupportedOperationException: sun.misc.Unsafe or java.nio.DirectByteBuffer.<init>
  (long, int) not available`, thrown inside `org.apache.arrow.memory.util.MemoryUtil`
  before any Python code runs. Confirmed via a bare `df.mapInPandas(...)` with zero
  witchhat involvement, so it is a JDK 21 (only JDK on this machine)-vs-pyspark's-bundled
  Arrow-Java incompatibility, not a witchhat bug, and not expected on Databricks (which
  controls its own JDK/Arrow versions). `--add-opens=java.base/java.nio=ALL-UNNAMED` (and
  siblings) via `JDK_JAVA_OPTIONS`/`PYSPARK_SUBMIT_ARGS` did not resolve it here; a JDK 17
  install likely would, untested (no JDK 17 available on this machine at the time). Do
  not re-diagnose this from scratch in a future session: it is an environment limitation,
  not a code defect, and `tools/spark_smoke.py` already detects and explains it instead
  of failing with a bare stack trace. Non-`mapInArrow` pyspark operations (plain
  `.collect()`, `SparkSession` creation, schema access) work fine.
