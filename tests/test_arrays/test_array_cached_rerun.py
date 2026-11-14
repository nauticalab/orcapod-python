"""A cached re-run in a fresh process recomputes nothing and reads arrays back exactly (ITL-888)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest

SCRIPT = textwrap.dedent(
    """
    import json, sys
    from pathlib import Path

    import numpy as np

    from orcapod import PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.pipeline import AsyncPipelineOrchestrator
    from orcapod.sources import DictSource
    from tests.test_arrays.array_cases import ARRAYS, NUMERIC

    store, counter, mode = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]

    def bump(name):
        with counter.open("a") as f:
            f.write(name + "\\n")

    @function_pod(output_keys="y")
    def identity(x: np.ndarray) -> np.ndarray:
        bump("identity")
        return x.copy()

    @function_pod(output_keys="total")
    def total(y: np.ndarray) -> float:
        bump("total")
        return float(np.real(y).sum())

    src = DictSource(
        data=[{"k": k, "x": v} for k, v in ARRAYS.items()],
        tag_columns=["k"],
        source_id="every",
    )
    numeric = DictSource(
        data=[{"k": k, "x": ARRAYS[k]} for k in NUMERIC],
        tag_columns=["k"],
        source_id="numeric",
    )
    job = PipelineJob(name="rerun", database=DeltaTableDatabase(base_path=store))
    with job:
        identity.pod(src, label="identity_all")
        total.pod(identity.pod(numeric, label="identity_numeric"), label="total")
    if mode == "sync":
        job.run()
    else:
        AsyncPipelineOrchestrator().run(job.dag)

    out = {"arrays": {}, "hashes": {}}
    for t, d in job.nodes["identity_all"].iter_data():
        y = d.as_dict()["y"]
        out["arrays"][t.as_dict()["k"]] = [
            type(y).__name__, y.dtype.str, list(y.shape), y.tobytes().hex()
        ]
    for label in ("identity_all", "identity_numeric", "total"):
        node = job.nodes[label]
        out["hashes"][label] = [
            node.content_hash().to_string(),
            node.pipeline_hash().to_string(),
            sorted(d.content_hash().to_string() for _, d in node.iter_data()),
        ]
    print("JSON:" + json.dumps(out, sort_keys=True))
    """
)


def _run(store: Path, counter: Path, mode: str) -> dict[str, Any]:
    repo_root = Path(__file__).resolve().parents[2]
    env = {**os.environ, "PYTHONPATH": str(repo_root), "RUST_LOG": "error"}
    proc = subprocess.run(
        [sys.executable, "-c", SCRIPT, str(store), str(counter), mode],
        capture_output=True,
        text=True,
        env=env,
        cwd=repo_root,
        timeout=600,
    )
    assert proc.returncode == 0, proc.stderr[-3000:]
    line = next(x for x in proc.stdout.splitlines() if x.startswith("JSON:"))
    return json.loads(line[5:])


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_cached_rerun_in_fresh_process(tmp_path: Path, mode: str) -> None:
    """A cached re-run in a fresh process calls no pod and reads every array back exactly."""
    from tests.test_arrays.array_cases import ARRAYS, NUMERIC

    store, counter = tmp_path / "store", tmp_path / "calls.txt"
    first = _run(store, counter, mode)
    calls_first = counter.read_text().splitlines()
    second = _run(store, counter, mode)
    calls_second = counter.read_text().splitlines()[len(calls_first) :]

    # ``identity_numeric`` may reuse results ``identity_all`` computed for the same
    # inputs (the result cache is keyed by function and input hash), so the first run
    # calls ``identity`` once per distinct input, at least.
    assert len(ARRAYS) <= calls_first.count("identity") <= len(ARRAYS) + len(NUMERIC)
    assert calls_first.count("total") == len(NUMERIC)
    assert calls_second == []
    assert second == first
    for name, expected in ARRAYS.items():
        assert first["arrays"][name] == [
            "ndarray",
            expected.dtype.str,
            list(expected.shape),
            expected.tobytes().hex(),
        ]
