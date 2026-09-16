# witchhat: Operations

**Document type** Operations manual
**Status** Partial by necessity: witchhat is a stateless transformation library today, with no write path, no job to schedule, and no storage of its own. This document covers what already applies (build, install, sizing) and defers what does not yet (Chapter VI).
**Audience** Whoever builds the wheel and installs it on a Databricks workspace.
**Companion documents** `architecture.md` for the design, `api.md` for the callable surface.
**Version** 1.7
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
  - 4. Testing `witchhat.spark` locally
- III. Installing on Databricks
  - 1. From a Unity Catalog Volume
  - 2. From a package repository
  - 3. Compatibility floor
- IV. Sizing
  - 1. What scales and what does not
  - 2. Memory
  - 3. Cores
  - 4. Schema validation is a different shape
  - 5. JSON normalization and regex cleanup cost more per row than hashing
  - 6. Deduplication's extra cost is one `HashSet<u64>`
  - 7. Join builds one index over the larger of its two inputs
  - 8. Aggregate's extra cost is one row-index vector per group
  - 9. `witchhat.spark`'s partition-coordinating functions buffer a whole partition
  - 10. Metric logging is free when off, and one JSON encode per call when on
- V. Failure Modes
  - 1. Exceptions and what they mean
  - 2. What cannot fail
- VI. Deferred Until Applicable
  - 1. Deferred because the feature does not exist yet
  - 2. Decided out of scope, documented rather than pursued
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

Produces `target/wheels/witchhat-<version>-cp310-abi3-<platform>.whl` for the host's own
architecture. For a manylinux wheel suitable for a Databricks Linux cluster, built from
any host via Docker:

```
maturin build --release --target x86_64-unknown-linux-gnu --manylinux 2_28 --zig
maturin build --release --target aarch64-unknown-linux-gnu --manylinux 2_28 --zig
```

(or run inside the `ghcr.io/pyo3/maturin` manylinux container). CI builds and verifies
both architectures on native runners of each (`ubuntu-latest` for `x86_64`,
`ubuntu-24.04-arm` for `aarch64`, not cross-compiled and assumed to work); see
`.github/workflows/ci.yml`'s `wheel` job matrix for the exact invocation. The `aarch64`
build is for Graviton (AWS ARM) Databricks clusters, which Databricks supports alongside
`x86_64`.

### 3. Verifying the wheel

The filename must contain `cp310-abi3`, not `cp310-cp310`: that is the difference
between "loads on 3.10 and every later interpreter from one build" and "loads on 3.10
only". It must also carry the correct architecture tag (`x86_64`/`aarch64`) for the
matrix entry that built it. CI's `wheel` job asserts both and then imports the wheel and
exercises every exported function (`tools/smoke.py`) on the native runner that built
it, before publishing it as an artifact.

### 4. Testing `witchhat.spark` locally

`tools/spark_smoke.py` round-trips every `witchhat.spark` function against a real, local
`pyspark` session (not part of CI: pyspark is a large, optional dependency, matching why
`witchhat.spark` lazily imports it rather than requiring it). On a JDK 17+ machine, local
Spark's bundled Arrow Java library can fail with `UnsupportedOperationException:
sun.misc.Unsafe ... not available` on *any* Arrow-based Python UDF, before witchhat ever
runs; this is a known pyspark/JDK compatibility gap, not a witchhat bug, does not affect
Databricks (which manages its own JDK and Arrow versions), and `tools/spark_smoke.py`
detects and explains it rather than failing with a bare Java stack trace. See the script's
own module docstring for the JVM flags that resolve it on an affected machine.

## III. Installing on Databricks

<Table 3-1> Installation options

| Source | When to use |
|---|---|
| Unity Catalog Volume (`/Volumes/...`) | The intended, primary path: install a built wheel from where it was uploaded |
| A package repository | Not pursued for this project; see Section 2 |

### 1. From a Unity Catalog Volume

The primary distribution path. Upload the built `.whl` to a Volume path, then either
install it as a cluster library (Compute -> Libraries -> Install new -> Volumes) or,
inside a notebook:

```python
%pip install /Volumes/<catalog>/<schema>/<volume>/witchhat-0.1.0-cp310-abi3-manylinux_2_28_x86_64.whl
# or, on a Graviton (ARM) cluster:
%pip install /Volumes/<catalog>/<schema>/<volume>/witchhat-0.1.0-cp310-abi3-manylinux_2_28_aarch64.whl
```

This is the standard Databricks pattern for installing a wheel from a Volume path, not
anything witchhat-specific, and is documented here as the complete, intended procedure.
`tools/smoke.py` and CI's `wheel` job already confirm the wheel itself is correct
(imports cleanly, carries the `cp310-abi3` tag, every function round-trips against real
`pyarrow`); the `/Volumes/...` filesystem mechanism above it is Databricks' own,
well-established behaviour, so this repository does not additionally verify it against
a live workspace (decided 2026-09-16, see Chapter VI, Section 2).

### 2. From a package repository

Deliberately not pursued: `%pip install witchhat` by name would need publishing to an
index Databricks can reach (public PyPI, or an internal one), which needs an account and
upload credential this repository's automation does not hold, and is a one-way action (a
bad upload cannot be un-published, only yanked). The project stays wheel-only,
distributed via Section 1 instead. See Chapter VI, Section 2 for the decision.

### 3. Compatibility floor

The `abi3-py310` build loads on every CPython from 3.10 onward, which covers every
Databricks Runtime in current use as of this document's date. A manylinux 2_28 wheel
requires a correspondingly recent glibc on the cluster's base image, which every current
DBR image satisfies, on both `x86_64` and `aarch64` (Graviton) clusters — install the
wheel matching the cluster's architecture; the two are not interchangeable.

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
library. Every PyO3-bound kernel call does release the GIL for its duration
(`architecture.md` Chapter XII), so multiple Python threads calling witchhat
concurrently (a Spark executor running several `mapInArrow` tasks, for instance) do not
block each other on it; that is a concurrency improvement for the Python process as a
whole, not internal multi-core parallelism within one kernel call, which still does not
exist.

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

### 9. `witchhat.spark`'s partition-coordinating functions buffer a whole partition

`witchhat.spark.drop_duplicates`/`aggregate` (`architecture.md` Chapter XVI, Section 2)
materialize every `RecordBatch` `mapInArrow` hands them for one partition into a single
batch (`pyarrow.concat_batches`) before calling the underlying kernel, so peak memory per
task is one partition's worth of data, not one Arrow batch's worth. Size Spark's
partition count accordingly (more, smaller partitions if memory is tight) rather than
assuming `spark.sql.execution.arrow.maxRecordsPerBatch` alone bounds memory here, the way
it would for a row-local `witchhat.spark` function. `repartition=True` (the default on
both) adds a shuffle before this, the same cost `df.repartition(...)` always has.

### 10. Metric logging is free when off, and one JSON encode per call when on

`witchhat.metrics` (`architecture.md` Chapter XVII) checks one boolean per wrapped call
when disabled (the default) and does nothing else: no timing call, no `dict`
allocation, no I/O. Enabled, the added cost per call is one `time.perf_counter()` pair
and, per event, one JSON encode plus whatever the configured sink itself costs (a
`print` to stdout, an `open`+`write` for a file sink, or whatever a callable sink does).
Enabling it in a distributed job means every partition's Python worker pays that cost
independently; there is no coordination overhead beyond that, since there is no
cross-task aggregation to coordinate (Chapter VI, Section 1's monitoring note, and
`architecture.md` Chapter XVII, Section 4).

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

Two kinds of "deferred" live here: features that do not exist yet, and steps that were
raised, then deliberately decided against pursuing further, in favour of documenting the
intended approach instead.

### 1. Deferred because the feature does not exist yet

The following sections exist in a conventional operations manual and are deliberately
absent here (`architecture.md` Chapter XVIII has the full backlog):

- **Storage maintenance (VACUUM, retention).** witchhat writes nothing.
- **Monitoring and alerting.** No long-running process to monitor; a witchhat call fails
  or succeeds synchronously within the caller's own job. `witchhat.metrics`
  (Chapter IV, Section 10) is a building block a caller's own monitoring could be built from
  (latency, throughput, error events as JSON), not a monitoring system in itself: it
  has no built-in alerting, dashboards, or cross-task aggregation.
- **Change management for a schema drift policy.** No load semantics exist yet for
  witchhat to enforce one; `validate_schema`/`check_equivalence` are the building blocks
  a caller's own policy would be built from.
- **Re-run/recovery procedure.** Every call is a pure function of its input; there is no
  partial state to recover from.

Revisit as each corresponding backlog item in `architecture.md` Chapter XVIII lands.

### 2. Decided out of scope, documented rather than pursued

Two more items that a conventional operations manual would chase down were raised and
then explicitly descoped by the user (2026-09-16), rather than left as open work:

- **Publishing to a package repository** (Chapter III, Section 2). Not pursued: the
  project is wheel-only, installed from a built artifact (a Unity Catalog Volume, or by
  handing the `.whl` to whoever needs it) rather than `pip install witchhat` by name.
  Section 2 above still describes what publishing would look like if that changes later,
  but nothing here is waiting on it.
- **Verifying Unity Catalog Volume installation against a live workspace**
  (Chapter III, Section 1). Not pursued as a live check: Section 1's procedure is the
  documented, standard Databricks pattern for installing a wheel from a Volume path
  (`%pip install /Volumes/...` or the cluster-library UI), and that documentation is
  considered sufficient on its own rather than something this repository needs to prove
  against a real cluster. `tools/smoke.py` and CI's `wheel` job already confirm the
  wheel itself is correct (imports, `cp310-abi3` tag, every function round-trips against
  real `pyarrow`); what remained unverified was only the Volumes *filesystem* path
  specifically, which is standard Databricks behaviour rather than anything
  witchhat-specific.

Neither is tracked as an open item in `README.md` any longer.

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
