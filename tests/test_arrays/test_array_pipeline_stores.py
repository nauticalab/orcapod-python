"""Array values through ``PipelineJob`` on in-memory and Delta stores, sync and async (ITL-888)."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.operators import Batch, GroupBy
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.sources import DictSource
from tests.test_arrays.array_cases import ARRAYS, NUMERIC, assert_arrays_identical


@function_pod(output_keys="y")
def identity(x: np.ndarray) -> np.ndarray:  # pyright: ignore[reportMissingTypeArgument]  # the bare np.ndarray annotation is under test
    return x.copy()


@function_pod(output_keys="d")
def double(y: np.ndarray) -> np.ndarray:  # pyright: ignore[reportMissingTypeArgument]  # the bare np.ndarray annotation is under test
    return np.asarray(y * 2)


@function_pod(output_keys="total")
def total(d: np.ndarray) -> float:  # pyright: ignore[reportMissingTypeArgument]  # the bare np.ndarray annotation is under test
    return float(np.real(d).sum())


def _build(store: Any) -> PipelineJob:
    numeric = DictSource(
        data=[{"k": k, "x": ARRAYS[k]} for k in NUMERIC],
        tag_columns=["k"],
        source_id="numeric",
    )
    every = DictSource(
        data=[{"k": k, "x": v} for k, v in ARRAYS.items()],
        tag_columns=["k"],
        source_id="every",
    )
    job = PipelineJob(name="arrays", database=store)
    with job:
        identity.pod(every, label="identity_all")
        y = identity.pod(numeric, label="identity_numeric")
        d = double.pod(y, label="double")
        total.pod(d, label="total")
    return job


def _run_sync(job: PipelineJob) -> None:
    job.run()


def _run_async(job: PipelineJob) -> None:
    AsyncPipelineOrchestrator().run(job.dag)


STORES: dict[str, Callable[[Path], Any]] = {
    "in_memory": lambda _: InMemoryArrowDatabase(),
    "delta": lambda tmp: DeltaTableDatabase(base_path=tmp / "store"),
}
RUNNERS: dict[str, Callable[[PipelineJob], None]] = {
    "sync": _run_sync,
    "async": _run_async,
}


def _node(job: PipelineJob, label: str, column: str) -> dict[str, Any]:
    return {
        str(t.as_dict()["k"]): d.as_dict()[column]
        for t, d in job.nodes[label].iter_data()
    }


@pytest.mark.parametrize("runner", sorted(RUNNERS))
@pytest.mark.parametrize("store", sorted(STORES))
def test_pipeline_keeps_arrays_exact(tmp_path: Path, store: str, runner: str) -> None:
    """Arrays come back exact through every orchestrator and every store."""
    job = _build(STORES[store](tmp_path))
    RUNNERS[runner](job)

    every = _node(job, "identity_all", "y")
    assert sorted(every) == sorted(ARRAYS)
    for name, expected in ARRAYS.items():
        assert_arrays_identical(every[name], expected)

    doubled = _node(job, "double", "d")
    totals = _node(job, "total", "total")
    for name in NUMERIC:
        expected = np.asarray(ARRAYS[name] * 2)
        assert_arrays_identical(doubled[name], expected)
        assert totals[name] == pytest.approx(float(np.real(expected).sum()))


@function_pod(output_keys="parts")
def split(x: np.ndarray) -> list[np.ndarray]:  # pyright: ignore[reportMissingTypeArgument]  # the bare np.ndarray annotation is under test
    return [x.copy(), np.asarray(x * 2)]


@pytest.mark.parametrize(
    ("store", "runner"),
    [
        ("in_memory", "sync"),
        ("in_memory", "async"),
        ("delta", "sync"),
        ("delta", "async"),
    ],
)
def test_pod_output_list_of_arrays(tmp_path: Path, store: str, runner: str) -> None:
    """A pod output ``list[np.ndarray]`` is stored and read back on every database and runner."""
    source = DictSource(
        data=[{"k": k, "x": ARRAYS[k]} for k in NUMERIC],
        tag_columns=["k"],
        source_id="numeric",
    )
    job = PipelineJob(name="array_lists", database=STORES[store](tmp_path))
    with job:
        split.pod(source, label="split")
    RUNNERS[runner](job)

    parts = _node(job, "split", "parts")
    assert sorted(parts) == sorted(NUMERIC)
    for name in NUMERIC:
        assert_arrays_identical(parts[name][0], ARRAYS[name])
        assert_arrays_identical(parts[name][1], np.asarray(ARRAYS[name] * 2))


@function_pod(output_keys="firsts")
def first_elements(x: list[np.ndarray]) -> list[float]:  # pyright: ignore[reportMissingTypeArgument]  # the bare np.ndarray annotation is under test
    assert all(type(a) is np.ndarray and a.shape == (3,) for a in x)
    return sorted(float(a[0]) for a in x)


@pytest.mark.parametrize("runner", sorted(RUNNERS))
@pytest.mark.parametrize("store", sorted(STORES))
@pytest.mark.parametrize("operator", ["batch", "group_by"])
def test_batch_and_group_by_over_an_array_column(
    tmp_path: Path, operator: str, store: str, runner: str
) -> None:
    """``Batch`` and ``GroupBy`` over an array column feed a pod on every database and runner."""
    source = DictSource(
        data=[
            {"g": "a" if i < 2 else "b", "k": f"r{i}", "x": np.arange(3.0) + i}
            for i in range(4)
        ],
        tag_columns=["g", "k"],
        source_id="grouped",
    )
    job = PipelineJob(name="array_groups", database=STORES[store](tmp_path))
    with job:
        if operator == "batch":
            grouped = Batch(batch_size=2)(source)
        else:
            grouped = GroupBy(["g"])(source)
        first_elements.pod(grouped, label="firsts")
    RUNNERS[runner](job)

    firsts = sorted(d.as_dict()["firsts"] for _, d in job.nodes["firsts"].iter_data())
    assert firsts == [[0.0, 1.0], [2.0, 3.0]]
