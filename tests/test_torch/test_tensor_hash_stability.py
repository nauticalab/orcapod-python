"""Tensor hashes are identical across processes and ``PYTHONHASHSEED`` values (ITL-871)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

pytest.importorskip("torch")

SCRIPT = textwrap.dedent(
    """
    import json

    from orcapod.contexts import get_default_context
    from orcapod.sources import DictSource
    from tests.test_torch.tensor_cases import TENSORS

    hasher = get_default_context().semantic_hasher
    src = DictSource(
        data=[{"k": k, "x": v} for k, v in TENSORS.items()],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="every",
    )
    out = {
        "semantic": {k: hasher.hash_object(v).to_string() for k, v in TENSORS.items()},
        "data": {
            t.as_dict()["k"]: d.content_hash().to_string() for t, d in src.iter_data()
        },
        "source_content": src.content_hash().to_string(),
        "source_pipeline": src.pipeline_hash().to_string(),
    }
    print(json.dumps(out, sort_keys=True))
    """
)


def _run(seed: str) -> dict[str, object]:
    """Hash the shared tensors in a fresh process with ``PYTHONHASHSEED=seed``."""
    repo_root = Path(__file__).resolve().parents[2]
    env = {**os.environ, "PYTHONPATH": str(repo_root), "PYTHONHASHSEED": seed}
    proc = subprocess.run(
        [sys.executable, "-c", SCRIPT],
        capture_output=True,
        text=True,
        env=env,
        cwd=repo_root,
        timeout=300,
    )
    assert proc.returncode == 0, proc.stderr[-3000:]
    return json.loads(proc.stdout.strip().splitlines()[-1])


def test_tensor_hashes_stable_across_processes_and_hash_seeds() -> None:
    """Fresh processes with different hash seeds compute identical hashes."""
    results = [_run(seed) for seed in ("0", "1", "4242", "random")]
    assert all(r == results[0] for r in results[1:])


def test_tensor_hashes_match_this_process() -> None:
    """A fresh process computes the same semantic hashes as this one."""
    from orcapod.contexts import get_default_context
    from tests.test_torch.tensor_cases import TENSORS

    hasher = get_default_context().semantic_hasher
    here = {k: hasher.hash_object(v).to_string() for k, v in TENSORS.items()}
    assert _run("7")["semantic"] == here
