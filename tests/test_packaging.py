"""Packaging guards for the dependency list in ``pyproject.toml``."""

from __future__ import annotations

import tomllib
from pathlib import Path

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"


def test_pygraphviz_is_not_a_dependency():
    """``pygraphviz`` needs Graphviz C headers to build and nothing imports it (ITL-134)."""
    project = tomllib.loads(PYPROJECT.read_text())["project"]
    declared = list(project["dependencies"])
    for extra in project.get("optional-dependencies", {}).values():
        declared.extend(extra)
    assert not [d for d in declared if d.lower().startswith("pygraphviz")]
