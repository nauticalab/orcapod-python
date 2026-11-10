"""The documentation site must build with ``mkdocs build --strict``.

Strict mode turns every warning into a failure: a ``:::`` reference to a name
that no longer exists, a broken link between pages, or a docstring whose
``Args:`` section names a parameter the function does not have.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_mkdocs_build_strict(tmp_path: Path):
    """The site builds with no warnings; skipped when the docs tools are absent."""
    pytest.importorskip("mkdocs", reason="mkdocs is in the 'dev' dependency group")
    pytest.importorskip(
        "mkdocstrings", reason="mkdocstrings is in the 'dev' dependency group"
    )

    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "mkdocs",
            "build",
            "--strict",
            "--site-dir",
            str(tmp_path / "site"),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    output = proc.stdout + proc.stderr
    problems = [
        line for line in output.splitlines() if "WARNING" in line or "ERROR" in line
    ]
    assert proc.returncode == 0, "mkdocs build --strict failed:\n" + "\n".join(
        problems or output.splitlines()[-30:]
    )
