"""Prototype regression check for ITL-892: a process that read a Delta store must exit.

Not collected by the suite (``testpaths = tests``); run it explicitly:

    uv run pytest superpowers/specs/itl-892-exit-hang/regression_check_proto.py -v
    ITL892_PATCH=native-fs uv run pytest superpowers/specs/itl-892-exit-hang/regression_check_proto.py -v

Each case starts RUNS fresh interpreters that use Orcapod's public API on a local Delta
store (``DeltaTableDatabase`` write, flush, read through a fresh handle; and
``DeltaTableSource``) and then return normally. A run fails if it does not exit within
TIMEOUT seconds (the macOS and Python 3.13 symptom) or exits non-zero (the Linux
3.11/3.12 symptom: SIGABRT, "terminate called without an active exception").
``ITL892_PATCH`` is passed to the children, so a mitigation prototype can be checked
with the same test.
"""

from __future__ import annotations

import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

SCRIPT = Path(__file__).with_name("repro_orcapod.py")
RUNS = int(os.environ.get("ITL892_RUNS", "40"))
TIMEOUT = float(os.environ.get("ITL892_TIMEOUT", "60"))
JOBS = 4


def _run(mode: str) -> str | None:
    """Run one child; return a failure description, or None if it exited cleanly."""
    try:
        res = subprocess.run(
            [sys.executable, str(SCRIPT), mode],
            capture_output=True,
            text=True,
            timeout=TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        return f"did not exit within {TIMEOUT:.0f}s"
    if res.returncode != 0:
        return f"exit code {res.returncode}: {res.stderr.strip()[-200:]}"
    return None


@pytest.mark.parametrize("mode", ["plain-read", "source-read"])
def test_process_exits_after_delta_read(mode: str) -> None:
    """Every one of ``RUNS`` fresh interpreters runs ``mode`` and exits cleanly."""
    with ThreadPoolExecutor(max_workers=JOBS) as pool:
        failures = [f for f in pool.map(_run, [mode] * RUNS) if f is not None]
    assert not failures, (
        f"{len(failures)}/{RUNS} runs failed to exit cleanly: {failures[:3]}"
    )
