"""Run every script in ``examples/`` so an example cannot break silently (ITL-821).

Each example runs in its own subprocess with a fresh temporary working directory,
because examples write files (databases, saved pipelines) relative to the
current directory.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"
EXAMPLE_SCRIPTS = sorted(EXAMPLES_DIR.glob("*.py"))


def test_examples_directory_has_scripts() -> None:
    """The discovery glob finds at least one example, so the parametrized test is not vacuous."""
    assert EXAMPLE_SCRIPTS, f"no example scripts found in {EXAMPLES_DIR}"


@pytest.mark.parametrize("script", EXAMPLE_SCRIPTS, ids=lambda path: path.name)
def test_example_runs(script: Path, tmp_path: Path) -> None:
    """An example script exits 0 when run alone in an empty working directory.

    Args:
        script: Path of the example script under ``examples/``.
        tmp_path: Fresh directory used as the working directory, so files the
            example writes do not leak into the repository or other tests.
    """
    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert result.returncode == 0, (
        f"{script.name} exited with {result.returncode}\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )
