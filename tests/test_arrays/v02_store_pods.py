"""Pods and source of the v0.2-format store fixture (ITL-888).

``data/v02_array_store`` was written by Orcapod v0.2.0 (tag ``v0.2.0``, arrays as
``.npy`` under the extension name ``numpy.ndarray``) running ``build_job`` below;
``make_v02_array_store.py`` regenerates it. The module path is part of every
pod's identity, so the pods must stay in this module, unchanged.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np

# Each call of ``plus_two`` appends here, so a test can tell whether it was recomputed.
CALLS: list[str] = []

ARRAYS = {
    "a": np.arange(6, dtype=np.float32).reshape(2, 3),
    "b": np.arange(6, dtype=np.float64).reshape(3, 2),
    "c": np.array([1, -2, 3], dtype=np.int8),
}


def build_job(store_root: Path) -> Any:
    """Build the fixture's pipeline on a Delta store at ``store_root``."""
    from orcapod import PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.operators import DropDataColumns
    from orcapod.sources import DictSource

    @function_pod(output_keys="y")
    def double(x: np.ndarray) -> np.ndarray:  # pyright: ignore[reportMissingTypeArgument]  # pinned fixture signature
        return np.asarray(x * 2)

    @function_pod(output_keys="total")
    def total(y: np.ndarray) -> float:  # pyright: ignore[reportMissingTypeArgument]  # pinned fixture signature
        return float(y.sum())

    @function_pod(output_keys="s")
    def direct_sum(x: np.ndarray) -> float:  # pyright: ignore[reportMissingTypeArgument]  # pinned fixture signature
        return float(x.sum())

    @function_pod(output_keys="m")
    def plus_one(n: int) -> int:
        return n + 1

    @function_pod(output_keys="p")
    def plus_two(n: int) -> int:
        CALLS.append("plus_two")
        return n + 2

    arrays = DictSource(
        data=[{"k": k, "x": v} for k, v in ARRAYS.items()],
        tag_columns=["k"],
        source_id="arrays",
    )
    ints = DictSource(
        data=[{"k": k, "n": i} for i, k in enumerate(ARRAYS)],
        tag_columns=["k"],
        source_id="ints",
    )
    mixed = DictSource(
        data=[{"k": k, "x": v, "n": i} for i, (k, v) in enumerate(ARRAYS.items())],
        tag_columns=["k"],
        source_id="mixed",
    )
    job = PipelineJob(name="v02", database=DeltaTableDatabase(base_path=store_root))
    with job:
        total.pod(double.pod(arrays, label="double"), label="total")
        direct_sum.pod(arrays, label="direct_sum")
        plus_one.pod(ints, label="plus_one")
        # An array-free pod downstream of an array column.
        plus_two.pod(DropDataColumns(["x"])(mixed), label="after_drop")
    return job
