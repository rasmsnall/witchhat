"""Overwrites crates/witchhat-py/python/witchhat/_build_info.py's COMMIT/BUILT_AT
constants with real values. Run by CI's `wheel` job immediately before
`maturin build`, so a wheel it uploads has the exact source commit and build
time baked in as plain Python constants (`witchhat.__commit__`/`__built_at__`).
See `_build_info.py`'s own docstring for what this does and does not prove.

A standalone script rather than an inline CI shell heredoc: a heredoc embedded
in a YAML `run: |` block is indented along with the rest of the block, and a
heredoc terminator must start at column zero to be recognised, which is easy
to get wrong (and did, in this project's history) without ever failing a local
test, since nothing local exercises the workflow file itself.

Usage: python tools/stamp_build_info.py <commit-sha>
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

_TEMPLATE = '''\
"""Provenance for this build; see this file's own docstring in source
control for what these constants do and do not prove. Overwritten by
CI's wheel job just before building; the source-controlled version
has both constants as None.
"""

COMMIT: str | None = {commit!r}
BUILT_AT: str | None = {built_at!r}
'''


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: stamp_build_info.py <commit-sha>", file=sys.stderr)
        return 1
    commit = sys.argv[1]
    built_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    target = (
        Path(__file__).resolve().parent.parent
        / "crates"
        / "witchhat-py"
        / "python"
        / "witchhat"
        / "_build_info.py"
    )
    target.write_text(_TEMPLATE.format(commit=commit, built_at=built_at), encoding="utf-8")
    print(f"stamped {target}: COMMIT={commit!r} BUILT_AT={built_at!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
