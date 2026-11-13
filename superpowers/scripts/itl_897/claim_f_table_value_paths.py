"""ITL-897 claim (f): every runtime path by which a ``pa.Table`` / ``pa.RecordBatch`` reaches
the type-dispatched ``ArrowTableHandler``.

Wraps ``ArrowTableHandler.handle`` (in this process only) to record who handed the table to
the semantic hasher: the nearest frame outside the hashing package, plus the
``identity_structure`` owner when there is one. Then runs the given pytest selection (default:
the whole ``tests/`` tree) and prints the distinct call sites with counts.

Run: ``uv run python superpowers/scripts/itl_897/claim_f_table_value_paths.py [pytest args]``
"""

from __future__ import annotations

import collections
import sys
from typing import Any

import pytest

from orcapod.hashing.semantic_hashing import builtin_handlers as bh

SITES: collections.Counter[str] = collections.Counter()
_original = bh.ArrowTableHandler.handle


def _short(filename: str) -> str:
    for marker in ("/src/", "/tests/"):
        if marker in filename:
            return marker.strip("/") + "/" + filename.split(marker, 1)[1]
    return filename


def _site() -> str:
    """Name the object whose identity carried the table, or the direct caller."""
    frame = sys._getframe(2)  # noqa: SLF001 - diagnostic script
    caller = None
    while frame is not None:
        code = frame.f_code
        owner = frame.f_locals.get("self")
        if code.co_name in ("content_hash", "pipeline_hash") and owner is not None:
            return (
                f"{code.co_name}() of {type(owner).__module__}.{type(owner).__name__}"
            )
        if caller is None and "/orcapod/hashing/" not in code.co_filename:
            caller = f"{_short(code.co_filename)}:{code.co_name}"
        frame = frame.f_back
    return f"direct hash_object from {caller}"


def _recording_handle(self: Any, obj: Any, hasher: Any) -> Any:
    SITES[_site()] += 1
    return _original(self, obj, hasher)


bh.ArrowTableHandler.handle = _recording_handle  # type: ignore[method-assign]


if __name__ == "__main__":
    args = sys.argv[1:] or ["tests/", "-q", "-p", "no:cacheprovider", "-x"]
    code = pytest.main(args)
    by_kind: dict[str, int] = collections.Counter()
    print("\n## ArrowTableHandler call sites\n")
    for site, count in sorted(SITES.items(), key=lambda kv: -kv[1]):
        kind = "tests" if "from tests/" in site else "library objects"
        by_kind[kind] += count
        print(f"{count:8d}  {site}")
    print(f"\npytest exit code: {code}; calls by origin: {dict(by_kind)}")
    sys.exit(code)
