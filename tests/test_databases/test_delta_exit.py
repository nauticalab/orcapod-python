"""A process that read a local Delta store must exit cleanly (ITL-892).

deltalake's default pyarrow reader goes through a filesystem implemented in Python. Arrow
finishes tearing down a scan on a thread-pool worker after the scan returns. If that worker
releases a Python-backed buffer while the interpreter is finalizing, the process hangs in
Arrow's thread-pool destructor, or on Linux with Python 3.11/3.12 aborts with SIGABRT. The
upstream reports are apache/arrow#50482 and apache/arrow#54536.

The failure is a race at exit, so each case starts several fresh interpreters and fails if
any of them misses the deadline or exits non-zero. Without the fix, about half the runs of
the database case fail on macOS and about one in ten on Linux. Twenty runs per case make a
missed regression unlikely: below 1% per CI job.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

RUNS = 20
DEADLINE_S = 120.0
PARALLEL = 4

_DATABASE_READ = """
    import sys

    import pyarrow as pa

    from orcapod.databases import DeltaTableDatabase

    store = sys.argv[1]
    table = pa.table(
        {
            "__record_id": pa.array(["a", "b", "c"], pa.large_string()),
            "v": pa.array([1, 2, 3], pa.int64()),
        }
    )
    db = DeltaTableDatabase(store)
    db.add_records(("t",), table, record_id_column="__record_id")
    db.flush()
    records = DeltaTableDatabase(store).get_all_records(
        ("t",), record_id_column="__record_id"
    )
    assert records is not None and records.num_rows == 3
    print("read", flush=True)
"""

_SOURCE_READ = """
    import sys
    from pathlib import Path

    import deltalake
    import pyarrow as pa

    import orcapod as op

    path = Path(sys.argv[1]) / "src"
    deltalake.write_deltalake(
        str(path),
        pa.table({"id": pa.array([1, 2, 3]), "x": pa.array([1.0, 2.0, 3.0])}),
    )
    source = op.sources.DeltaTableSource(path, tag_columns=["id"])
    assert source.as_table().num_rows == 3
    print("read", flush=True)
"""


def _run_once(script: str, store: Path) -> str | None:
    """Run ``script`` in a fresh interpreter; describe the failure, or return None."""
    try:
        result = subprocess.run(
            [sys.executable, "-c", textwrap.dedent(script), str(store)],
            capture_output=True,
            text=True,
            timeout=DEADLINE_S,
        )
    except subprocess.TimeoutExpired as exc:
        out = exc.stdout or ""
        if isinstance(out, bytes):  # TimeoutExpired keeps bytes even with text=True
            out = out.decode(errors="replace")
        finished = "read" in out
        when = "after its work finished" if finished else "before finishing its work"
        return f"did not exit within {DEADLINE_S:.0f}s ({when})"
    if result.returncode != 0:
        return f"exited with code {result.returncode}: {result.stderr.strip()[-300:]}"
    return None


@pytest.mark.parametrize(
    "script",
    [_DATABASE_READ, _SOURCE_READ],
    ids=["database_get_all_records", "delta_table_source"],
)
def test_process_exits_after_reading_a_local_delta_store(
    tmp_path: Path, script: str
) -> None:
    """Every one of ``RUNS`` fresh interpreters reads a local Delta store and exits cleanly.

    Each run must finish within ``DEADLINE_S`` with exit code 0. A run that misses the
    deadline or aborts is the ITL-892 exit hang.
    """
    stores = [tmp_path / f"run{i}" for i in range(RUNS)]
    with ThreadPoolExecutor(max_workers=PARALLEL) as pool:
        failures = [
            f for f in pool.map(lambda s: _run_once(script, s), stores) if f is not None
        ]
    assert not failures, (
        f"{len(failures)} of {RUNS} processes did not exit cleanly after reading a "
        f"local Delta store (ITL-892: Arrow thread-pool teardown at interpreter exit). "
        f"First failures: {failures[:3]}"
    )
