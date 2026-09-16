"""Provenance for this build: which source commit it was built from, and when.

`COMMIT`/`BUILT_AT` are `None` here in the checked-in source (a local
`maturin develop`/`maturin build` from a working tree has no single meaningful
commit if there are uncommitted changes, and no CI-assigned build time). CI's
`wheel` job overwrites this file's two constants with the real values,
immediately before `maturin build --release`, so a wheel built and uploaded by
CI has them baked in as plain Python constants, readable without importing the
compiled extension. This is deliberately not a claim of a full SBOM or a
cryptographic signature (`docs/architecture.md` Chapter XIX records that as a
known gap, not something this file pretends to close): it answers one
narrower, still useful question a downloaded wheel could not otherwise answer
at all: which exact source commit produced it.
"""

COMMIT: str | None = None
BUILT_AT: str | None = None
