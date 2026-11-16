"""``DropNulls`` in a ``PipelineJob`` against a Delta Lake store (ITL-943).

The workshop shape: a source whose value column is nullable by specification,
``drop_nulls``, then a pod with a non-optional parameter. A second run on
the same database must reuse the stored results.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase
from orcapod.pipeline import PipelineJob

# Module level so the function's identity is the same across the runs of a test.
_CALLS: list[int] = []


def to_seconds(recording_start_timestamp: int) -> float:
    """A pod function with a non-optional parameter; records each call."""
    _CALLS.append(recording_start_timestamp)
    return recording_start_timestamp / 1000


@pytest.fixture(autouse=True)
def _reset_calls() -> Iterator[None]:
    """Clear the shared invocation log before every test."""
    _CALLS.clear()
    yield


@pytest.fixture
def store(tmp_path: Path) -> DeltaTableDatabase:
    """A Delta Lake store in the test's temporary directory."""
    return DeltaTableDatabase(base_path=tmp_path / "store")


def _recordings(timestamps: list[int | None]) -> ArrowTableSource:
    """A source whose ``recording_start_timestamp`` column is nullable."""
    table = pa.table(
        {
            "session": pa.array(
                [f"s{i}" for i in range(len(timestamps))], pa.large_string()
            ),
            "recording_start_timestamp": pa.array(timestamps, pa.int64()),
        },
        schema=pa.schema(
            [
                pa.field("session", pa.large_string(), nullable=False),
                pa.field("recording_start_timestamp", pa.int64(), nullable=True),
            ]
        ),
    )
    return ArrowTableSource(
        table, tag_columns=["session"], tag_only_columns=True, source_id="recordings"
    )


def _run(store: DeltaTableDatabase, timestamps: list[int | None]) -> PipelineJob:
    """Record and run ``source -> drop_nulls -> to_seconds`` as a ``PipelineJob``."""
    pod = FunctionPod(PythonDataFunction(to_seconds, output_keys="seconds"))  # pyright: ignore[reportArgumentType]  # PythonDataFunction is not yet typed as matching DataFunctionProtocol (baselined in other tests); it works at runtime
    job = PipelineJob(name="recordings", database=store)
    with job:
        present = _recordings(timestamps).drop_nulls(
            "recording_start_timestamp", label="present"
        )
        pod(present, label="seconds")
    _ = job.run()
    return job


def _seconds(job: PipelineJob) -> dict[str, float]:
    """Return the pod's output as ``{session: seconds}``."""
    table = job.nodes["seconds"].as_table()
    return dict(
        zip(table.column("session").to_pylist(), table.column("seconds").to_pylist())
    )


def test_job_drops_null_rows_and_runs_the_pod(store: DeltaTableDatabase) -> None:
    """The pod runs once per non-null row and the operator node reports ``int``."""
    job = _run(store, [1000, None, 3000])
    assert sorted(_CALLS) == [1000, 3000]
    assert _seconds(job) == {"s0": 1.0, "s2": 3.0}
    present = job.nodes["present"]
    assert present.output_schema()[1] == {"recording_start_timestamp": int}
    field = present.as_table().schema.field("recording_start_timestamp")
    assert field.nullable is False


def test_identical_rerun_hits_the_cache(store: DeltaTableDatabase) -> None:
    """A second identical run on the same database makes no pod call."""
    _run(store, [1000, None, 3000])
    assert sorted(_CALLS) == [1000, 3000]

    _CALLS.clear()
    job = _run(store, [1000, None, 3000])
    assert _CALLS == [], "second identical run must not recompute"
    assert _seconds(job) == {"s0": 1.0, "s2": 3.0}


def test_fresh_store_recomputes(tmp_path: Path) -> None:
    """Control for the cache-hit test: a different database has nothing to reuse."""
    _run(DeltaTableDatabase(base_path=tmp_path / "a"), [1000, None, 3000])
    _CALLS.clear()
    _run(DeltaTableDatabase(base_path=tmp_path / "b"), [1000, None, 3000])
    assert sorted(_CALLS) == [1000, 3000]


def test_a_newly_non_null_row_is_the_only_one_computed(
    store: DeltaTableDatabase,
) -> None:
    """Rows computed before stay cached when a null row gains a value."""
    _run(store, [1000, None, 3000])
    _CALLS.clear()
    job = _run(store, [1000, 2000, 3000])
    assert _CALLS == [2000]
    assert _seconds(job) == {"s0": 1.0, "s1": 2.0, "s2": 3.0}


def test_job_save_and_load_round_trips_the_operator(
    store: DeltaTableDatabase, tmp_path: Path
) -> None:
    """A saved job records the operator's config and loads it back."""
    import json

    job = _run(store, [1000, None, 3000])
    path = tmp_path / "job.json"
    job.save(str(path))

    saved = [
        node["operator_config"]
        for node in json.loads(path.read_text())["pipeline"]["nodes"].values()
        if node.get("operator_config")
    ]
    assert [c["class_name"] for c in saved] == ["DropNulls"]
    assert saved[0]["config"] == {"columns": ["recording_start_timestamp"]}

    loaded = PipelineJob.load(str(path), database=store)
    present = loaded.nodes["present"]
    assert present.output_schema() == (
        {"session": str},
        {"recording_start_timestamp": int},
    )
