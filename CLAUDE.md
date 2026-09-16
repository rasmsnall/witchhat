# witchhat

Python library, implemented in Rust, of native data-transformation kernels aimed at
Spark/Databricks workloads. Status: **composite hashing and schema validation
implemented end to end.** JSON normalization, regex cleanup, and the native
transformations that would actually replace a Spark operation are not yet built.

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
  changes what it produces. See `HashVersion` in `crates/witchhat-core/src/hash.rs`.
- CPU feature detection with a portable fallback, and the stronger constraint that
  follows from it: a SIMD-accelerated path may change speed, never output. See
  `crates/witchhat-core/src/cpu.rs` and `docs/architecture.md` Chapter IV.
- Wheels usable from Databricks Volumes or a package repository; reproducible builds.
- Generic framework for: composite hashing (done), schema validation, JSON
  normalization, regex-heavy cleanup, output-equivalence testing, and native
  transformations, with the eventual goal of replacing Spark operations outright.

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
crates/witchhat-core/src/lib.rs      crate docs, #![forbid(unsafe_code)], #![warn(missing_docs)]
crates/witchhat-core/src/schema.rs   re-exported Arrow schema types, schema_fingerprint
crates/witchhat-core/src/hash.rs     HashVersion, hash_batch, hash_batch_all_columns, table_fingerprint
crates/witchhat-core/src/validate.rs ValidateSchemaOptions, SchemaDiff, validate_schema
crates/witchhat-core/src/cpu.rs      CpuFeatures, features()
crates/witchhat-core/src/error.rs    Error, Result
crates/witchhat-py/src/lib.rs        crate docs + pyo3 module shell (_witchhat)
crates/witchhat-py/src/python.rs     the actual pyo3 bindings (register())
crates/witchhat-py/python/witchhat/__init__.py    re-exports _witchhat, __all__, __version__
crates/witchhat-py/python/witchhat/__init__.pyi   type stubs, one docstring per export
crates/witchhat-py/python/witchhat/py.typed
crates/witchhat-py/pyproject.toml    maturin config (abi3-py310, mixed layout)
rust-toolchain.toml   pins rustc/rustfmt/clippy to one version
README.md
docs/architecture.md
docs/api.md
docs/operations.md
tools/md2docx.py      generates docs/*.docx from docs/*.md
tools/smoke.py        round-trips a real pyarrow batch through the built wheel
.github/workflows/ci.yml   fmt+clippy+test+doc job, manylinux abi3 wheel job, multi-interpreter matrix
```

Two-crate split (`witchhat-core` has no PyO3 dependency; `witchhat-py` is a thin
translation layer over it), unlike `rust-streamer-pgdb`'s single-crate layout. Deliberate
here: a future Rust-only consumer links `witchhat-core` without pulling in `pyo3` at all.
See `docs/architecture.md` Chapter VIII for the full rationale, including why
`witchhat-py` pins `pyo3 = "=0.25.1"` exactly (arrow's `pyarrow` Cargo feature links
`pyo3-ffi` and tolerates only one exact version across the dependency graph; bumping pyo3
later needs arrow to catch up first, not just an edited version string).

## Dependency budget

| Crate | Why |
|---|---|
| `arrow-array`, `arrow-schema` | The data model. `witchhat-core` depends on these only, no pyo3 |
| `arrow-data` | `ArrayData`, the untyped array arrow's pyarrow bridge actually converts (witchhat-py only) |
| `arrow` (feature `pyarrow`) | Zero-copy `RecordBatch`/`ArrayData` <-> pyarrow conversion (witchhat-py only) |
| `xxhash-rust` (feature `xxh3`) | Per-value hash function underlying `hash_batch` |
| `thiserror` | The `Error` enum |
| `pyo3` | Python bindings (witchhat-py only); pinned to `=0.25.1`, see above |

Pinned 2026-09 (probed via `cargo build`; crates.io index reachable): `arrow 56.2.1`,
`xxhash-rust 0.8.18`, `thiserror 2.0.20`, `pyo3 0.25.1`.

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
| `docs/architecture.md` | Data model, hashing design, versioning/CPU-feature discipline, concurrency/failure/security model, dependency budget, what's not built yet |
| `docs/api.md` | Python and Rust surface, parameter semantics |
| `docs/operations.md` | Building and installing the wheel, sizing, what a conventional ops manual would cover but does not apply yet (no write path, no job to schedule) |

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
- Schema validation (`validate.rs`): `validate_schema` returns a `SchemaDiff`
  (missing/unexpected/retyped/nullability-tightened columns) rather than a bare bool,
  with opt-in numeric widening (narrower type, cross-signedness, and int-to-float are
  never accepted regardless) and a `is_breaking()` vs. `is_empty()` distinction so
  additive schema evolution does not count as a break. 20 Rust unit tests plus 7
  doctests, all passing.
- `schema_fingerprint`, `witchhat_core::cpu::features()`.
- The `witchhat-py` mixed maturin layout (`python/witchhat/`), type stubs, `py.typed`.
  `SchemaDiff.retyped` returns real `pyarrow.DataType` objects, not strings, via
  `arrow`'s pyarrow bridge (`ToPyArrow` is implemented for `DataType`/`Field`/`Schema`/
  `ArrayData`/`RecordBatch`, not for typed arrays; see `docs/architecture.md`
  Chapter IX, Section 1 for the `PyArrowType: !Clone` workaround this required for a
  `#[pyo3(get)]` field).
- The wheel builds (`maturin build --release`) and was verified against a real
  `pyarrow.RecordBatch` (`tools/smoke.py`), not just the Rust unit tests.
- Full rustdoc on every public `witchhat-core` item; `cargo doc --no-deps -D warnings`
  and `cargo clippy --all-targets -- -D warnings` both clean.
- `rust-toolchain.toml` pinned; CI and docs/CI conventions adopted from
  `rust-streamer-pgdb`.

Still open:

- JSON normalization and a regex-heavy cleanup kernel.
- An output-equivalence test harness built on `table_fingerprint` plus per-column
  diffing, for asserting a witchhat pipeline and its Spark equivalent agree in CI.
- The native transformations (filter/project/join/aggregate) that are the actual point:
  everything so far is supporting infrastructure for them.
- Publishing to a package repository (currently wheel-only, no index).
- Whether/when a kernel becomes expensive enough to justify releasing the GIL
  (`docs/architecture.md` Chapter V); none does yet.

## Environment notes

- Rust 1.94.0 (pinned via `rust-toolchain.toml`), Python 3.10.11, maturin 1.15.0,
  python-docx 1.2.0.
- `cargo fmt --all` reformats aggressively; run it after any hand-edit to `.rs` files
  before committing, since CI checks `cargo fmt --all --check`.
