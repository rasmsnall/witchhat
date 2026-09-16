# witchhat: Operations

**Document type** Operations manual
**Status** Partial by necessity: witchhat is a stateless transformation library today, with no write path, no job to schedule, and no storage of its own. This document covers what already applies (build, install, sizing) and defers what does not yet (Chapter VI).
**Audience** Whoever builds the wheel and installs it on a Databricks workspace.
**Companion documents** `architecture.md` for the design, `api.md` for the callable surface.
**Version** 1.0
**Date** 2026-09-16

---

## Contents

- I. Introduction
  - 1. Purpose
  - 2. What "operating" witchhat means today
- II. Building the Wheel
  - 1. Prerequisites
  - 2. Build command
  - 3. Verifying the wheel
- III. Installing on Databricks
  - 1. From a Unity Catalog Volume
  - 2. From a package repository
  - 3. Compatibility floor
- IV. Sizing
  - 1. What scales and what does not
  - 2. Memory
  - 3. Cores
- V. Failure Modes
  - 1. Exceptions and what they mean
  - 2. What cannot fail
- VI. Deferred Until Applicable
- References
- Appendix A. Reproducing a build

### List of Tables

- `<Table 3-1>` Installation options
- `<Table 5-1>` Exception to likely cause

### List of Figures

(none)

---

## I. Introduction

### 1. Purpose

This document covers building the `witchhat` wheel and installing it on Databricks. It
assumes `architecture.md` is not of interest until something needs explaining, and that
`api.md` already answers what a given function does.

### 2. What "operating" witchhat means today

witchhat has no daemon, no scheduled job, and no state of its own: every call reads
Arrow data already in the caller's process and returns a result with no side effect.
"Operating" it, at this stage, means building a correct wheel and getting it installed
where a notebook or job can `import witchhat`. Chapter VI lists what a more conventional
operations manual would also cover, and explains why it does not apply yet.

## II. Building the Wheel

### 1. Prerequisites

- Rust, pinned via `rust-toolchain.toml` (currently 1.94.0); `rustup` installs it
  automatically on first build.
- Python >= 3.10.
- `maturin` (`pip install maturin`).

### 2. Build command

```
cd crates/witchhat-py
maturin build --release
```

Produces `target/wheels/witchhat-<version>-cp310-abi3-<platform>.whl`. For a manylinux
wheel suitable for a Databricks Linux cluster, built from any host via Docker:

```
maturin build --release --target x86_64-unknown-linux-gnu --manylinux 2_28 --zig
```

(or run inside the `ghcr.io/pyo3/maturin` manylinux container; see
`.github/workflows/ci.yml` for the exact invocation CI uses, via `PyO3/maturin-action`).

### 3. Verifying the wheel

The filename must contain `cp310-abi3`, not `cp310-cp310`: that is the difference
between "loads on 3.10 and every later interpreter from one build" and "loads on 3.10
only". CI's `wheel` job asserts this and then imports the wheel and exercises every
exported function (`tools/smoke.py`) before publishing it as an artifact.

## III. Installing on Databricks

<Table 3-1> Installation options

| Source | When to use |
|---|---|
| Unity Catalog Volume (`/Volumes/...`) | The wheel was built in-house and is not meant to be public |
| A package repository (internal PyPI-compatible index, or public PyPI once published) | The wheel should be installable by name, like any other dependency |

### 1. From a Unity Catalog Volume

Upload the built `.whl` to a Volume path, then either install it as a cluster library
(Compute -> Libraries -> Install new -> Volumes) or, inside a notebook:

```python
%pip install /Volumes/<catalog>/<schema>/<volume>/witchhat-0.1.0-cp310-abi3-manylinux_2_28_x86_64.whl
```

### 2. From a package repository

Once published to an index Databricks can reach, `%pip install witchhat` (or a cluster
library entry naming the package) works like any other PyPI dependency. Not yet done for
this project; tracked as an open item.

### 3. Compatibility floor

The `abi3-py310` build loads on every CPython from 3.10 onward, which covers every
Databricks Runtime in current use as of this document's date. A manylinux 2_28 wheel
requires a correspondingly recent glibc on the cluster's base image, which every current
DBR image satisfies.

## IV. Sizing

### 1. What scales and what does not

Every operation is a single pass over the input `RecordBatch`, O(rows x columns) in time
and O(rows) in output size; nothing buffers more than one batch at a time. There is
currently no chunking or streaming concern because there is no code path that reads more
data than the caller already handed it in one call.

### 2. Memory

Peak memory for `hash_rows`/`hash_rows_all_columns` is the input batch plus one `u64` per
row of output. There is no hidden multiplier: no intermediate copy of the batch is made
beyond what Arrow's own reference-counted buffers already share with the caller.

### 3. Cores

Every kernel runs on the calling thread; nothing here uses multiple cores yet. On a
Databricks driver or worker, parallelism today comes from calling witchhat once per Spark
partition (e.g. from a `mapInArrow`/Pandas UDF), not from anything internal to the
library. A future kernel expensive enough to justify releasing the GIL (`architecture.md`
Chapter V) would also be a candidate for internal parallelism; neither exists yet.

## V. Failure Modes

### 1. Exceptions and what they mean

<Table 5-1> Exception to likely cause

| Exception | Likely cause | First action |
|---|---|---|
| `ValueError: unknown hash version ...` | A typo in `version`, or code written against a version this build does not implement | Check `witchhat.__version__` and this build's supported versions |
| `RuntimeError: column "..." not found in schema` | A column name mismatch, often from a schema that drifted upstream | Compare the caller's expected columns against `batch.schema` |
| `RuntimeError: unsupported arrow type for this operation: ...` | A column of a type Table 3-2 in `architecture.md` does not list | Cast the column, or wait for that type to be added |

### 2. What cannot fail

There is no network call, no filesystem access, and no subprocess anywhere in this
library's current code path, so none of the usual "operations" failure modes (timeout,
permission denied, disk full, credential expiry) apply to a witchhat call itself. A
failure surfaces from the surrounding Spark/Databricks job instead.

## VI. Deferred Until Applicable

The following sections exist in a conventional operations manual and are deliberately
absent here, because the feature they would document does not exist yet
(`architecture.md` Chapter XI has the full backlog):

- **Storage maintenance (VACUUM, retention).** witchhat writes nothing.
- **Monitoring and alerting.** No long-running process to monitor; a witchhat call fails
  or succeeds synchronously within the caller's own job.
- **Change management for a schema drift policy.** No load semantics exist yet for
  witchhat to enforce one.
- **Re-run/recovery procedure.** Every call is a pure function of its input; there is no
  partial state to recover from.

Revisit this chapter as each corresponding backlog item in `architecture.md` Chapter XI
lands, rather than writing these sections speculatively now.

## References

- `architecture.md`, this repository.
- `.github/workflows/ci.yml`, for the exact wheel build and verification CI runs.

## Appendix A. Reproducing a build

```
git clone <repository>
cd witchhat
rustup show                       # installs the pinned toolchain from rust-toolchain.toml
cd crates/witchhat-py
pip install maturin
maturin build --release
python -m pip install ../../target/wheels/witchhat-*.whl
python -c "import witchhat; print(witchhat.__version__)"
```
