# witchhat: Operations

**Document type** Operations manual
**Status** Partial by necessity: witchhat is a stateless transformation library today, with no write path, no job to schedule, and no storage of its own. This document covers what already applies (build, install, sizing) and defers what does not yet (Chapter VI).
**Audience** Whoever builds the wheel and installs it on a Databricks workspace.
**Companion documents** `architecture.md` for the design, `api.md` for the callable surface.
**Version** 1.3
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
  - 4. What is documented but not verified
- IV. Sizing
  - 1. What scales and what does not
  - 2. Memory
  - 3. Cores
  - 4. Schema validation is a different shape
  - 5. JSON normalization and regex cleanup cost more per row than hashing
  - 6. Deduplication's extra cost is one `HashSet<u64>`
  - 7. Join builds one index over the larger of its two inputs
  - 8. Aggregate's extra cost is one row-index vector per group
- V. Failure Modes
  - 1. Exceptions and what they mean
  - 2. What cannot fail
- VI. Deferred Until Applicable
  - 1. Deferred because the feature does not exist yet
  - 2. Deferred pending access this environment does not have
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
library entry naming the package) works like any other PyPI dependency. Not yet done:
publishing needs a PyPI (or internal index) account and an upload credential this
repository's automation does not hold, and pushing a public package version is a
one-way action (a bad upload cannot be un-published, only yanked), so it is deliberately
left to a human running it deliberately rather than attempted by default. See
Chapter VI for what is needed before this can happen.

### 3. Compatibility floor

The `abi3-py310` build loads on every CPython from 3.10 onward, which covers every
Databricks Runtime in current use as of this document's date. A manylinux 2_28 wheel
requires a correspondingly recent glibc on the cluster's base image, which every current
DBR image satisfies.

### 4. What is documented but not verified

Section 1's Volume-install command has not been run against a real Databricks
workspace: this development environment has no workspace to test against. `tools/
smoke.py` exercises every function against a real `pyarrow` install and CI's `wheel`
job proves the wheel itself installs and imports correctly on Linux, but neither
confirms the `/Volumes/...` FUSE path behaves as documented on an actual cluster.
Treat Section 1 as the intended procedure, not a confirmed one, until someone with
workspace access runs it once and this section is updated to say so.

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
Chapter XII) would also be a candidate for internal parallelism; neither exists yet.

### 4. Schema validation is a different shape

`validate_schema` is O(fields), not O(rows): it never touches the data in a batch, only
its schema. Cost is negligible relative to any hashing or transformation kernel, so it
is cheap enough to run on every batch as a pre-flight check rather than sampled or
skipped for performance reasons.

### 5. JSON normalization and regex cleanup cost more per row than hashing

Both parse or scan every byte of their input (`serde_json` parsing one JSON object,
`regex` scanning for each rule's pattern) rather than hashing fixed-width or
length-prefixed values, so they are the most CPU-intensive kernels so far per input
byte. Still a single pass with no hidden buffering; still O(1) additional memory beyond
the output batch. If a `clean_with_preset`/`clean_with_rules` pipeline applies several
rules, each rule is its own full pass over every value (`architecture.md` Chapter VI,
Section 1): fold rules that can be expressed as one pattern into one rule where
practical, rather than chaining many small ones, if this becomes a measured bottleneck.

### 6. Deduplication's extra cost is one `HashSet<u64>`

`drop_duplicates` computes the same row hash `hash_rows` would, plus one `HashSet<u64>`
sized to at most `batch.num_rows()` entries to track which hashes have already been
seen. No second pass over the data beyond the `filter_record_batch` call that builds the
output.

### 7. Join builds one index over the larger of its two inputs

`join` builds one hash index (keyed by `arrow_row`'s byte-comparable row format) over
`right`'s key rows, `O(right.num_rows())` extra memory, then probes it once per `left`
row. Pass the larger side as `right` if the two are very different sizes; the current
implementation does not choose automatically.

### 8. Aggregate's extra cost is one row-index vector per group

`aggregate` builds the same `arrow_row` structures as `join` for `group_by`, plus one
`Vec<u32>` of row indices per distinct group (`O(batch.num_rows())` total across all
groups) to know which rows belong to which output row. `Sum`/`Mean`/`Min`/`Max` then
each make one additional pass over every group's row indices per aggregation requested,
so an `aggregate` call with many aggregation columns costs proportionally more, not just
proportionally to `batch.num_rows()`.

## V. Failure Modes

### 1. Exceptions and what they mean

<Table 5-1> Exception to likely cause

| Exception | Likely cause | First action |
|---|---|---|
| `ValueError: unknown hash version ...` | A typo in `version`, or code written against a version this build does not implement | Check `witchhat.__version__` and this build's supported versions |
| `RuntimeError: column "..." not found in schema` | A column name mismatch, often from a schema that drifted upstream | Compare the caller's expected columns against `batch.schema` |
| `RuntimeError: unsupported arrow type for this operation: ...` | A column of a type Table 3-2 in `architecture.md` does not list | Cast the column, or wait for that type to be added |

`validate_schema` and `normalize_json` are not in this table: a schema mismatch is a
normal `SchemaDiff` return value and a malformed JSON row is a normal, counted null in
`NormalizeStats`, neither an exception. If a caller wants either condition to fail
loudly, that is their own `if diff.is_breaking(): raise ...` or
`if stats.rows_malformed: raise ...`, not something witchhat raises for them (see
`docs/api.md` Section V.3 for the pattern).

### 2. What cannot fail

There is no network call, no filesystem access, and no subprocess anywhere in this
library's current code path, so none of the usual "operations" failure modes (timeout,
permission denied, disk full, credential expiry) apply to a witchhat call itself. A
failure surfaces from the surrounding Spark/Databricks job instead.

## VI. Deferred Until Applicable

Two kinds of "deferred" live here: features that do not exist yet, and steps that exist
and are documented but cannot be completed by this repository's own automation because
they need something only a human with the right access can provide.

### 1. Deferred because the feature does not exist yet

The following sections exist in a conventional operations manual and are deliberately
absent here (`architecture.md` Chapter XVIII has the full backlog):

- **Storage maintenance (VACUUM, retention).** witchhat writes nothing.
- **Monitoring and alerting.** No long-running process to monitor; a witchhat call fails
  or succeeds synchronously within the caller's own job.
- **Change management for a schema drift policy.** No load semantics exist yet for
  witchhat to enforce one; `validate_schema`/`check_equivalence` are the building blocks
  a caller's own policy would be built from.
- **Re-run/recovery procedure.** Every call is a pure function of its input; there is no
  partial state to recover from.

Revisit as each corresponding backlog item in `architecture.md` Chapter XVIII lands.

### 2. Deferred pending access this environment does not have

- **Publishing to a package repository** (Chapter III, Section 2). Needs a PyPI (or
  internal index) account and an upload token; an automated agent building this project
  does not hold one and should not be given one implicitly, since a package upload is a
  one-way, externally-visible action. Before this can happen: decide which index
  (public PyPI, or an internal one Databricks can already reach), create or provide the
  account/token, and decide whether publishing happens by hand or is wired into CI
  (e.g. a tag-triggered release job using PyPI's trusted-publisher OIDC flow, which
  needs no long-lived token stored in the repository, is the lower-risk option if CI
  publishing is wanted).
- **Verifying installation from a Unity Catalog Volume** (Chapter III, Section 4).
  Needs a reachable Databricks workspace with Volumes enabled; this development
  environment has none. Before this can happen: run the Section 1 command (or the
  cluster-library UI flow) against a real workspace once, and update Section 4 to
  record that it was confirmed, on what DBR version, and by whom.

Both are the two open items in `README.md`'s backlog that are not a matter of writing
more code.

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
