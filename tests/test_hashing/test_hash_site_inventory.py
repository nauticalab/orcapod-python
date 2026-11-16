"""Every hash site the library reaches is in the inventory (ITL-1154).

``docs/reference/hashing.md`` ("Hash site inventory") lists every library call site that
produces a digest or reads one, with its class. This test runs the trace
(``hash_site_trace.py``) over every golden case and the sites no golden case reaches, in a
fresh process, and fails when a call site it sees is not listed: a new hash site must be
added to the table with its class.
"""

from __future__ import annotations

import ast
import json
import os
import re
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REFERENCE = PROJECT_ROOT / "docs" / "reference" / "hashing.md"
SITE = re.compile(r"`([\w/]+\.py:[\w.<>]+)`")


def _documented_sites() -> set[str]:
    """The call sites named in the inventory section of the hashing reference."""
    text = REFERENCE.read_text(encoding="utf-8")
    start = text.index("## Hash site inventory")
    end = text.index("\n## ", start + 1)
    return set(SITE.findall(text[start:end]))


def _subprocess_env() -> dict[str, str]:
    """The environment for a fresh interpreter: ``src`` on ``PYTHONPATH``.

    The child does not inherit pytest.ini's ``pythonpath = src``, so it is passed
    explicitly rather than relying on orcapod being installed.
    """
    env = dict(os.environ)
    src = str(PROJECT_ROOT / "src")
    env["PYTHONPATH"] = (
        os.pathsep.join([src, env["PYTHONPATH"]]) if env.get("PYTHONPATH") else src
    )
    return env


def _traced_sites() -> set[str]:
    """The library call sites the trace sees, from a fresh process."""
    result = subprocess.run(
        [sys.executable, "-m", "tests.test_hashing.hash_site_trace"],
        capture_output=True,
        text=True,
        check=True,
        cwd=PROJECT_ROOT,
        env=_subprocess_env(),
    )
    return set(json.loads(result.stdout))


_COMPREHENSIONS: dict[type[ast.AST], str] = {ast.GeneratorExp: "<genexpr>"}
"""Code blocks with a frame of their own. A list, set or dict comprehension is named
by the function it is written in (the trace names it so on Python 3.11 too)."""


def _qualified_names(tree: ast.Module) -> set[str]:
    """Every function's qualified name in a module, as ``__qualname__`` spells it.

    Nested functions get ``<locals>`` and lambdas ``<lambda>``, so a call site the trace
    records (``Class.method.<locals>.<lambda>``) is found by its whole name.
    """
    names: set[str] = set()

    def visit(node: ast.AST, prefix: str) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.ClassDef):
                visit(child, f"{prefix}{child.name}.")
            elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                qualname = f"{prefix}{child.name}"
                names.add(qualname)
                visit(child, f"{qualname}.<locals>.")
            elif isinstance(child, ast.Lambda):
                names.add(f"{prefix}<lambda>")
                visit(child, prefix)
            elif type(child) in _COMPREHENSIONS:
                names.add(f"{prefix}{_COMPREHENSIONS[type(child)]}")
                visit(child, prefix)
            else:
                visit(child, prefix)

    visit(tree, "")
    return names


def test_every_traced_hash_site_is_in_the_inventory() -> None:
    """A call site that produces or reads a digest is listed, with its class."""
    traced = _traced_sites()
    assert len(traced) > 40, "the trace saw almost nothing; is it installed?"
    missing = sorted(traced - _documented_sites())
    assert not missing, (
        "hash sites missing from docs/reference/hashing.md 'Hash site inventory' "
        f"(add each with its class): {missing}"
    )


def test_every_documented_site_exists() -> None:
    """Each documented call site names a function or method the module defines."""
    stale: list[str] = []
    for site in sorted(_documented_sites()):
        path, qualname = site.split(":", 1)
        source = (PROJECT_ROOT / "src" / "orcapod" / path).read_text(encoding="utf-8")
        if qualname not in _qualified_names(ast.parse(source)):
            stale.append(site)
    assert not stale, f"documented hash sites that no longer exist: {stale}"
