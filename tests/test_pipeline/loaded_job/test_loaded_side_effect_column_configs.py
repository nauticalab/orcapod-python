"""A restored side-effect node passes its upstream's columns through (ITL-1094).

A side-effect node passes its input rows on unchanged, so under every column
configuration its schema is its upstream's. The node ``PipelineJob.load`` restores has
the same schema as the live node, derived from the restored upstream: system tags
included. A load-only run on the sync path names the system tags of an operator below a
node that has no pod (a restored side-effect node, a loaded function node) as the live
job and the async path do, so the function nodes below find their stored entries.
"""

from __future__ import annotations

import pathlib
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import Batch, GroupBy, Join
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.databases import DeltaTableDatabase
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.side_effects import SideEffectPod, SideEffectPodConfig
from orcapod.sources import DictSource

ROWS = 4

COLUMN_CONFIGS: dict[str, dict[str, Any]] = {
    "default": {},
    "system_tags": {"columns": {"system_tags": True}},
    "source": {"columns": {"source": True}},
    "meta": {"columns": {"meta": True}},
    "all_info": {"all_info": True},
}


def _left() -> DictSource:
    """Tags ``k`` and ``g`` (``k % 2``), data ``x``."""
    return DictSource(
        [{"k": i, "g": i % 2, "x": i} for i in range(ROWS)],
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id="left",
    )


def _right() -> DictSource:
    """Tag ``k``, data ``z``."""
    return DictSource(
        [{"k": i, "z": 10 * i} for i in range(ROWS)],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="right",
    )


def _deliver(ctx: Any, x: int) -> None:
    """Deliver nothing anywhere."""


@function_pod(output_keys="y")
def _plus(x: int) -> int:
    """``x + 100``."""
    return x + 100


@function_pod(output_keys="w")
def _x_plus_z(x: int, z: int) -> int:
    """``x + z``."""
    return x + z


@function_pod(output_keys="v")
def _y_plus_z(y: int, z: int) -> int:
    """``y + z``."""
    return y + z


def _side_effect_job(database: DeltaTableDatabase) -> PipelineJob:
    """``left -> delivered -> {joined (with right) -> summed, grouped, batched}``."""
    job = PipelineJob(name="se_columns", database=database)
    pod = SideEffectPod(_deliver, config=SideEffectPodConfig(track_completion=True))
    with job:
        delivered = pod(_left(), label="delivered")
        joined = Join()(delivered, _right(), label="joined")
        _ = _x_plus_z.pod(joined, label="summed")
        _ = GroupBy(by=["g"])(delivered, label="grouped")
        _ = Batch(batch_size=2)(delivered, label="batched")
    return job


def _function_job(database: DeltaTableDatabase) -> PipelineJob:
    """``left -> plus -> joined (with right) -> summed``."""
    job = PipelineJob(name="fn_columns", database=database)
    with job:
        plus = _plus.pod(_left(), label="plus")
        joined = Join()(plus, _right(), label="joined")
        _ = _y_plus_z.pod(joined, label="summed")
    return job


def _run_save_load(build: Any, root: pathlib.Path) -> tuple[PipelineJob, PipelineJob]:
    """Run and save the job ``build`` makes; return it built again from code, and loaded.

    The loaded job has both sources bound.
    """
    database = DeltaTableDatabase(root / "store")
    job = build(database)
    _ = job.run()
    job.save(root / "job.json")
    loaded = PipelineJob.load(root / "job.json")
    loaded.bind(sources={"left": _left(), "right": _right()})
    return build(database), loaded


def _run_load_only(job: PipelineJob, orchestrator: str) -> Any:
    """``job.run(load_only=True)`` on the sync or async orchestrator."""
    if orchestrator == "async":
        return job.run(orchestrator=AsyncPipelineOrchestrator(), load_only=True)
    return job.run(load_only=True)


@pytest.mark.parametrize("config", list(COLUMN_CONFIGS))
def test_a_restored_side_effect_node_has_the_live_schema_under_every_column_config(
    tmp_path: pathlib.Path, config: str
) -> None:
    """Its schema and keys are the live node's and its upstream's, before and after a run.

    The operators below it predict their schemas from it, so theirs equal the live
    job's too.
    """
    live, loaded = _run_save_load(_side_effect_job, tmp_path)
    kwargs = COLUMN_CONFIGS[config]
    restored = loaded.nodes["delivered"]
    assert restored.output_schema(**kwargs) == loaded.nodes["left"].output_schema(
        **kwargs
    )
    assert restored.keys(**kwargs) == loaded.nodes["left"].keys(**kwargs)
    for _ in ("before the run", "after a load-only run"):
        for label in ("delivered", "joined", "grouped", "batched"):
            assert loaded.nodes[label].output_schema(**kwargs) == live.nodes[
                label
            ].output_schema(**kwargs), label
            assert loaded.nodes[label].keys(**kwargs) == live.nodes[label].keys(
                **kwargs
            ), label
        _ = _run_load_only(live, "sync")
        _ = _run_load_only(loaded, "sync")


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
def test_a_load_only_run_below_a_restored_side_effect_node_matches_the_live_job(
    tmp_path: pathlib.Path, orchestrator: str
) -> None:
    """Every node's table equals the live job's; ``summed`` loads all its entries."""
    live, loaded = _run_save_load(_side_effect_job, tmp_path)
    _ = _run_load_only(live, orchestrator)
    summary = _run_load_only(loaded, orchestrator)
    assert summary.nodes["summed"].reused == ROWS
    assert summary.nodes["summed"].not_stored == 0
    for label in ("delivered", "joined", "summed", "grouped", "batched"):
        assert (
            loaded.nodes[label]
            .as_table(all_info=True)
            .equals(live.nodes[label].as_table(all_info=True))
        ), label


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
def test_a_load_only_run_below_a_loaded_function_node_loads_every_stored_entry(
    tmp_path: pathlib.Path, orchestrator: str
) -> None:
    """A loaded function node has no pod either: the Join below it names its system
    tags as the live job does, so ``summed`` loads every stored entry."""
    live, loaded = _run_save_load(_function_job, tmp_path)
    _ = _run_load_only(live, orchestrator)
    summary = _run_load_only(loaded, orchestrator)
    assert summary.nodes["summed"].reused == ROWS
    assert summary.nodes["summed"].not_stored == 0
    for label in ("joined", "summed"):
        assert (
            loaded.nodes[label]
            .as_table(all_info=True)
            .equals(live.nodes[label].as_table(all_info=True))
        ), label


@pytest.mark.parametrize("with_rows", [True, False])
def test_an_upstreams_rebuilt_rows_report_its_pipeline_hash(
    tmp_path: pathlib.Path, with_rows: bool
) -> None:
    """Live and loaded nodes of every kind, and sources: the hash is the node's own.

    About the non-public entry point ``_materialize_upstream``: it is how the sync
    orchestrator rebuilds an upstream's rows as an operator's input.
    """
    live, loaded = _run_save_load(_side_effect_job, tmp_path)
    fn_live, fn_loaded = _run_save_load(_function_job, tmp_path / "fn")
    for job in (live, loaded, fn_live, fn_loaded):
        _ = _run_load_only(job, "sync")
    upstreams = [
        _left(),
        *(job.nodes[label] for job in (live, loaded) for label in live.nodes),
        *(job.nodes[label] for job in (fn_live, fn_loaded) for label in fn_live.nodes),
    ]
    for upstream in upstreams:
        rows = list(upstream.iter_data()) if with_rows else []
        # The non-public entry point under test (see the docstring).
        rebuilt = StaticOutputOperatorPod._materialize_upstream(rows, upstream)
        assert rebuilt.pipeline_hash() == upstream.pipeline_hash(), upstream


def test_as_pipeline_of_a_loaded_job_keeps_its_restored_side_effect_node(
    tmp_path: pathlib.Path,
) -> None:
    """The blueprint node has the live blueprint's identity, and never delivers."""
    database = DeltaTableDatabase(tmp_path / "store")
    job = PipelineJob(name="se_only", database=database)
    pod = SideEffectPod(_deliver)
    with job:
        _ = pod(_left(), label="delivered")
    _ = job.run()
    job.save(tmp_path / "job.json")
    blueprint = PipelineJob.load(tmp_path / "job.json").as_pipeline()
    live_blueprint = job.as_pipeline()
    node = blueprint.nodes["delivered"]
    assert node.content_hash() == live_blueprint.nodes["delivered"].content_hash()
    assert node.pipeline_hash() == live_blueprint.nodes["delivered"].pipeline_hash()
    assert node.output_schema() == live_blueprint.nodes["delivered"].output_schema()
    with pytest.raises(RuntimeError, match="cannot deliver"):
        _ = list(node.iter_data())
