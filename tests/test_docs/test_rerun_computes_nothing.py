"""The tutorial's central claim: a second run over the same database computes nothing.

Parts 5 and 6 of ``docs/tutorial/`` are built on it. The claim once failed for
about half of all Python processes: ``Join`` emitted tag columns in ``set``
order, ``GroupBy`` sorted group members by those columns, and so the lists a
downstream function received (which are its cache key) differed between a first
run and a re-run read back from the database (ITL-817). The failure depends on
``PYTHONHASHSEED``, so each seed runs in its own interpreter.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

# The shape of the tutorial pipeline, reduced: two sources joined inside a
# function pod, grouped on "s" so that each group's members are ordered by the two
# other tag columns ("p" and "u"), then a second function pod over the lists.
_SCRIPT = textwrap.dedent(
    """
    import json

    from orcapod import PipelineJob, function_pod
    from orcapod.databases import InMemoryArrowDatabase
    from orcapod.sources import DictSource

    calls = {"rate": 0, "mean": 0}


    @function_pod(output_keys="rate")
    def rate(n: int, dur: float) -> float:
        calls["rate"] += 1
        return n / dur


    @function_pod(output_keys="mean")
    def mean(rate: list[float]) -> float:
        calls["mean"] += 1
        return sum(rate) / len(rate)


    units = [
        {"s": s, "p": p, "u": u, "n": 10 * (1 + u) + (s == "b") + 3 * (p == "y")}
        for s in "ab"
        for p in "xy"
        for u in range(2)
    ]
    sessions = [{"s": "a", "dur": 2.0}, {"s": "b", "dur": 4.0}]

    database = InMemoryArrowDatabase()
    runs = []
    for _ in range(2):
        job = PipelineJob(name="w", database=database)
        with job:
            u = DictSource(data=units, tag_columns=["s", "p", "u"], tag_only_columns=True, source_id="units")
            s = DictSource(data=sessions, tag_columns=["s"], tag_only_columns=True, source_id="sessions")
            rates = rate.pod(u.select_data_columns(["n"]), s, label="rate")
            mean.pod(rates.group_by(["s"]).select_data_columns(["rate"]), label="mean")
        job.run()
        runs.append(
            {
                "calls": dict(calls),
                "groups": [d.as_dict() for _, d in job.nodes["GroupBy"].iter_data()],
                "rate_columns": job.nodes["rate"].as_table().column_names,
            }
        )
    print(json.dumps(runs))
    """
)


@pytest.mark.parametrize("seed", ["0", "1", "2", "3", "5"])
def test_second_run_over_the_same_database_computes_nothing(seed: str, tmp_path: Path):
    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO_ROOT / "src")
    env["PYTHONHASHSEED"] = seed
    proc = subprocess.run(
        [sys.executable, "-c", _SCRIPT],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
    first, second = json.loads(proc.stdout.splitlines()[-1])

    # 8 unit rows, 2 groups: computed once, on the first run only.
    assert first["calls"] == {"rate": 8, "mean": 2}
    assert second["calls"] == first["calls"], (
        f"PYTHONHASHSEED={seed}: the re-run called functions again"
    )
    # The lists handed to the downstream function are the cache key.
    assert second["groups"] == first["groups"]
    # Tag columns come in declaration order, not set order.
    assert first["rate_columns"] == ["s", "p", "u", "rate"]
    assert second["rate_columns"] == first["rate_columns"]
