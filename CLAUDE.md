# witchhat

Python library, implemented in Rust, of native data-transformation kernels aimed at
Spark/Databricks workloads. Status: **eight kernels implemented end to end** (composite
hashing, schema validation, JSON normalization, regex cleanup, output-equivalence
testing, deduplication, join, aggregate). Filter/project need no witchhat kernel (Arrow's
own compute kernels already cover them). Remaining open items are broader `aggregate`
type support, deeper JSON normalization, and two items blocked on the user (package-repo
publishing, Databricks Volumes verification) — see "Open items" below.

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
crates/witchhat-core/src/dedup.rs      drop_duplicates (built on hash_batch)
crates/witchhat-core/src/join.rs       JoinType, join (built on arrow_row, not hash_batch)
crates/witchhat-core/src/aggregate.rs  AggFunc, Aggregation, aggregate (built on arrow_row too)
crates/witchhat-core/src/cpu.rs        CpuFeatures, features()
crates/witchhat-core/src/error.rs      Error, Result
crates/witchhat-py/src/lib.rs          crate docs + pyo3 module shell (_witchhat)
crates/witchhat-py/src/python.rs       the actual pyo3 bindings (register())
crates/witchhat-py/python/witchhat/__init__.py    re-exports _witchhat, __all__, __version__
crates/witchhat-py/python/witchhat/__init__.pyi   type stubs, one docstring per export
crates/witchhat-py/python/witchhat/py.typed
crates/witchhat-py/pyproject.toml      maturin config (abi3-py310, mixed layout)
rust-toolchain.toml   pins rustc/rustfmt/clippy to one version
README.md
docs/architecture.md
docs/api.md
docs/operations.md
tools/md2docx.py      generates docs/*.docx from docs/*.md
tools/smoke.py        round-trips a real pyarrow batch through the built wheel, every function
.github/workflows/ci.yml   fmt+clippy+test+doc job, manylinux abi3 wheel job, multi-interpreter matrix
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

## Dependency budget

| Crate | Why |
|---|---|
| `arrow-array`, `arrow-schema` | The data model. `witchhat-core` depends on these only, no pyo3 |
| `arrow-select` | `filter_record_batch`/`take`, underlying `drop_duplicates`, `join`, `aggregate` |
| `arrow-row` | `RowConverter`, underlying `join`/`aggregate`'s exact key/group comparison |
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
format/clippy/test/doc job, a manylinux abi3 wheel job that verifies the `cp310-abi3` tag
and round-trips a real pyarrow batch through the built wheel (`tools/smoke.py`), and a
matrix job installing that one wheel on Python 3.10/3.12/3.13 to prove the abi3 promise
holds across interpreters. No optional Cargo features exist yet (unlike pgdb's
`azure`/`fast-gzip`/`zstd`), so the workspace-wide commands need no feature flags.

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
- Deduplication (`dedup.rs`): `drop_duplicates`, reusing `hash_batch` as the dedup key
  plus `arrow_select::filter::filter_record_batch`. Filter/project were deliberately
  *not* wrapped: Arrow's own kernels already do the job.
- Join (`join.rs`): `join` (inner/left/right/full), keyed via `arrow_row::RowConverter`
  rather than `hash_batch` (correctness, not just speed: see "Layout" above). Colliding
  right-side column names get suffixed `_right`; every output field is nullable.
  Right/left key columns require an exact Arrow type match, no implicit coercion.
- Aggregate (`aggregate.rs`): `aggregate` with `Count`/`Sum`/`Mean`/`Min`/`Max`, grouped
  via the same `arrow_row` approach as `join`. `Min`/`Max` preserve the source column's
  exact type (compare through `f64` to find the extreme row, then `take` its original
  value); `Sum`/`Mean` accumulate through `f64` (documented precision caveat beyond
  ±2^53). Empty `group_by` means whole-table aggregate; an empty batch in that case still
  produces one row (`Count` 0, others null), not zero rows.
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

Still open:

- Broader `aggregate` type support: string min/max, `Decimal`/`Date`/`Time`/`Timestamp`
  aggregation are unbuilt (`Sum`/`Mean`/`Min`/`Max` are numeric-only today).
- Deeper JSON normalization: array-valued fields and more than one level of object
  nesting are unbuilt (fall into the type-mismatch case in `normalize_json` today).
- Publishing to a package repository. **Blocked on the user, not on more code**: needs a
  PyPI (or internal index) account and an upload credential this repository's automation
  does not hold. A package upload is one-way (cannot be un-published, only yanked), so
  this should not be attempted without the user explicitly providing credentials and
  confirming the target index. See `docs/operations.md` Chapter VI, Section 2.
- Verifying installation from a Databricks Unity Catalog Volume against a real
  workspace. **Blocked on the user, not on more code**: this development environment has
  no Databricks workspace to test against. The `/Volumes/...` install procedure is
  documented in `docs/operations.md` Chapter III as the intended path, but unconfirmed.
- Whether/when a kernel becomes expensive enough to justify releasing the GIL
  (`docs/architecture.md` Chapter XII); none does yet, though JSON normalization and
  regex cleanup are the most CPU-intensive kernels so far per input byte
  (`docs/operations.md` Chapter IV, Section 5).

## Environment notes

- Rust 1.94.0 (pinned via `rust-toolchain.toml`), Python 3.10.11, maturin 1.15.0,
  python-docx 1.2.0.
- `cargo fmt --all` reformats aggressively; run it after any hand-edit to `.rs` files
  before committing, since CI checks `cargo fmt --all --check`.
- No `gh` CLI in this environment (neither Git Bash nor PowerShell `PATH`). Pushing to
  GitHub uses `git push` directly against an `origin` remote the user creates and shares
  the URL/name for; this session cannot create a GitHub repo itself.
