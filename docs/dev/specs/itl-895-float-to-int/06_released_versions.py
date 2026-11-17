"""Claim 6: released versions have the bug (version-agnostic reproduction).

Runs the issue's reproduction against whatever ``orcapod`` is importable, using only the
API every release since v0.1.0 shares (``function_pod``, ``DictSource``, ``.pod(...)``,
``as_table()``).

Run against a release without touching this worktree:

    git archive v0.1.4 | tar -x -C <scratch>/v0.1.4
    uv run --no-project --python 3.12 --with <scratch>/v0.1.4 \
        python -I superpowers/specs/itl-895-float-to-int/06_released_versions.py
"""

from __future__ import annotations

from importlib.metadata import version

from orcapod import function_pod
from orcapod.sources import DictSource


@function_pod(output_keys="y")
def f(x: int) -> int:
    return x + 0.5  # pyright: ignore[reportReturnType]  # deliberate wrong return type: reproduces ITL-895


src = DictSource(data=[{"k": "a", "x": 1}], tag_columns=["k"])
t = f.pod(src).as_table()
print(
    f"orcapod {version('orcapod')}: y={t['y'].to_pylist()} type={t.schema.field('y').type}"
)
