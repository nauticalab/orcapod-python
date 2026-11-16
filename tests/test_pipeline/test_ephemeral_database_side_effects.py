"""``PipelineJob.set_ephemeral_database`` on a job with a side-effect node (ITL-1147).

A side-effect node takes no ephemeral database; the pipeline setter passes the database
only to the function and operator nodes. Live and loaded jobs behave alike.
"""

from __future__ import annotations

import pathlib
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import SelectDataColumns
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.side_effects import SideEffectPod, SideEffectPodConfig
from orcapod.sources import DictSource


@function_pod(output_keys="y")
def plus(x: int) -> int:
    """``x + 100``."""
    return x + 100


def _job(root: pathlib.Path) -> PipelineJob:
    """``s -> tap (side effect) -> select -> down``, run and saved."""

    def deliver(ctx: Any, x: int) -> None:
        """Receive ``x``."""

    source = DictSource(
        [{"k": i, "x": i} for i in range(3)],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="s",
    )
    job = PipelineJob(name="eph", database=DeltaTableDatabase(root / "db"))
    tap = SideEffectPod(deliver, config=SideEffectPodConfig(track_completion=False))
    with job:
        tapped = tap(source, label="tap")
        selected = SelectDataColumns(["x"])(tapped, label="select")
        plus.pod(selected, label="down")
    return job


@pytest.fixture(params=["live", "loaded"])
def job(request: pytest.FixtureRequest, tmp_path: pathlib.Path) -> PipelineJob:
    """A job with a side-effect node: the live one and the one loaded from its file."""
    live = _job(tmp_path)
    if request.param == "live":
        return live
    path = tmp_path / "job.json"
    live.save(path)
    return PipelineJob.load(path)


def test_setting_and_clearing_the_ephemeral_database_with_a_side_effect_node(
    job: PipelineJob,
) -> None:
    """The setter succeeds, with a database and with ``None``."""
    job.set_ephemeral_database(InMemoryArrowDatabase())
    job.set_ephemeral_database(None)


def test_function_nodes_receive_the_ephemeral_database(job: PipelineJob) -> None:
    """The function node holds the database set on the job, and none after ``None``."""
    database = InMemoryArrowDatabase()
    job.set_ephemeral_database(database)
    assert job.nodes["down"].ephemeral_result_database is database  # pyright: ignore[reportAttributeAccessIssue]
    job.set_ephemeral_database(None)
    assert job.nodes["down"].ephemeral_result_database is None  # pyright: ignore[reportAttributeAccessIssue]
