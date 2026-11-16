"""A loaded job restores its side-effect nodes, read-only (ITL-1094).

A saved job file records each side-effect node (``node_type`` ``"side_effect"``) but no
side-effect function and no pod configuration. ``PipelineJob.load`` and ``Pipeline.load``
restore the node with its saved label, content hash, pipeline hash, output schema and
position in the graph. It never delivers: a read returns the rows of a run in this process
(none), and every run raises. The nodes below it keep their saved identity.
"""

from __future__ import annotations

import asyncio
import json
import logging
import pathlib
from collections.abc import Callable
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.pipeline import Pipeline
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.observer import NoOpObserver
from orcapod.side_effects import SideEffectPod, SideEffectPodConfig
from orcapod.sources import DictSource
from orcapod.streams import ViewStatus

ROWS = 3
SIDE_EFFECT_NODES = ("delivered", "tail")


def _source() -> DictSource:
    """Three rows: tag ``k``, data ``x``."""
    return DictSource(
        [{"k": i, "x": i} for i in range(ROWS)],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="s",
    )


def _saved_job(root: pathlib.Path) -> tuple[PipelineJob, list[int], pathlib.Path]:
    """Run and save ``s -> delivered -> down -> tail``; return the job, deliveries, file.

    ``delivered`` (a side-effect node in the middle, ``track_completion`` and
    ``on_error="log"``) passes ``x`` on to the function node ``down`` (``y = x + 100``),
    and ``tail`` (a side-effect node at the end) receives ``y``.
    """
    sent: list[int] = []

    def deliver(ctx: Any, x: int) -> None:
        """Record ``x``."""
        sent.append(x)

    def notify(ctx: Any, y: int) -> None:
        """Record ``y``."""
        sent.append(y)

    @function_pod(output_keys="y")
    def plus(x: int) -> int:
        """``x + 100``."""
        return x + 100

    job = PipelineJob(name="se", database=DeltaTableDatabase(root / "store"))
    middle = SideEffectPod(
        deliver, config=SideEffectPodConfig(track_completion=True, on_error="log")
    )
    end = SideEffectPod(notify, config=SideEffectPodConfig(track_completion=False))
    with job:
        delivered = middle(_source(), label="delivered")
        down = plus.pod(delivered, label="down")
        _ = end(down, label="tail")
    _ = job.run()
    assert len(sent) == 2 * ROWS
    path = root / "job.json"
    job.save(path)
    return job, sent, path


@pytest.fixture
def saved(tmp_path: pathlib.Path) -> tuple[PipelineJob, list[int], pathlib.Path]:
    """A job with two side-effect nodes, run and saved."""
    return _saved_job(tmp_path)


def _descriptors(path: pathlib.Path) -> dict[str, dict[str, Any]]:
    """The node entries of the job file at ``path``, by label."""
    nodes = json.loads(path.read_text())["pipeline"]["nodes"]
    return {descriptor["label"]: descriptor for descriptor in nodes.values()}


def _reads(node: Any) -> dict[str, Callable[[], Any]]:
    """Every public read of ``node``, by name."""

    async def collect() -> list[Any]:
        return [row async for row in node.async_iter_data()]

    return {
        "head": lambda: node.head(100),
        "preview": node._repr_html_,
        "iter_data": lambda: list(node.iter_data()),
        "__iter__": lambda: list(node),
        "async_iter_data": lambda: asyncio.run(collect()),
        "as_table": node.as_table,
        "as_polars_df": node.as_polars_df,
        "as_df": node.as_df,
        "as_lazy_frame": lambda: node.as_lazy_frame().collect(),
        "as_pandas_df": node.as_pandas_df,
        "flow": node.flow,
    }


def test_a_loaded_job_lists_its_side_effect_nodes(
    saved: tuple[PipelineJob, list[int], pathlib.Path],
) -> None:
    """The node in the middle and the node at the end are both restored."""
    job, _, path = saved
    loaded = PipelineJob.load(path)
    assert set(loaded.nodes) == set(job.nodes) == {"s", "delivered", "down", "tail"}
    for label in SIDE_EFFECT_NODES:
        assert loaded.nodes[label].label == label


def test_a_restored_side_effect_node_has_its_saved_identity_and_position(
    saved: tuple[PipelineJob, list[int], pathlib.Path],
) -> None:
    """The job's content hash and pipeline hash; upstream is the restored node above it."""
    job, _, path = saved
    loaded = PipelineJob.load(path)
    for label in SIDE_EFFECT_NODES:
        node = loaded.nodes[label]
        # The saved job's (bound) hash, as a loaded function node has; not the
        # blueprint's hash in the node's descriptor.
        assert node.content_hash() == job.nodes[label].content_hash()
        # The pipeline hash names the invocation log: the run's, unchanged.
        assert node.pipeline_hash() == job.nodes[label].pipeline_hash()
        assert node.output_schema() == job.nodes[label].output_schema()
        assert node.keys() == job.nodes[label].keys()
    assert loaded.nodes["delivered"].upstreams == (loaded.nodes["s"],)
    assert loaded.nodes["tail"].upstreams == (loaded.nodes["down"],)


def test_the_node_below_keeps_its_identity_and_its_upstream(
    saved: tuple[PipelineJob, list[int], pathlib.Path],
) -> None:
    """``down`` hangs below the restored ``delivered`` with the run's hashes."""
    job, _, path = saved
    loaded = PipelineJob.load(path)
    down = loaded.nodes["down"]
    assert down.upstreams == (loaded.nodes["delivered"],)
    assert down.content_hash() == job.nodes["down"].content_hash()
    assert down.pipeline_hash() == job.nodes["down"].pipeline_hash()


def test_no_read_or_run_of_a_loaded_job_delivers(
    saved: tuple[PipelineJob, list[int], pathlib.Path],
) -> None:
    """Reads return no rows, runs raise, and nothing is delivered."""
    _, sent, path = saved
    delivered_before = list(sent)
    loaded = PipelineJob.load(path)
    for label in SIDE_EFFECT_NODES:
        node = loaded.nodes[label]
        for name, read in _reads(node).items():
            _ = read()
            assert sent == delivered_before, f"{label}.{name} delivered"
        assert list(node.iter_data()) == []
        assert node.as_table().num_rows == 0
        assert node.head(100).status is ViewStatus.NOT_COMPUTED
        with pytest.raises(RuntimeError, match="cannot deliver"):
            _ = node.run()
    with pytest.raises(RuntimeError):
        _ = loaded.run()
    with pytest.raises(RuntimeError):
        _ = loaded.run(orchestrator=AsyncPipelineOrchestrator())
    assert sent == delivered_before


def test_saving_a_loaded_job_keeps_its_side_effect_nodes(
    saved: tuple[PipelineJob, list[int], pathlib.Path], tmp_path: pathlib.Path
) -> None:
    """Their entries and every edge are written back unchanged; loading again keeps them."""
    _, _, path = saved
    again = tmp_path / "again.json"
    PipelineJob.load(path).save(again)
    first = json.loads(path.read_text())["pipeline"]
    second = json.loads(again.read_text())["pipeline"]
    for key, descriptor in first["nodes"].items():
        if descriptor["node_type"] == "side_effect":
            assert second["nodes"][key] == descriptor
    assert set(second["nodes"]) == set(first["nodes"])
    assert sorted(map(tuple, second["edges"])) == sorted(map(tuple, first["edges"]))
    assert set(PipelineJob.load(again).nodes) == {"s", "delivered", "down", "tail"}


def test_pipeline_load_restores_side_effect_nodes(
    saved: tuple[PipelineJob, list[int], pathlib.Path], tmp_path: pathlib.Path
) -> None:
    """The pipeline blueprint lists them too, with the saved identity."""
    job, sent, _ = saved
    delivered_before = list(sent)
    blueprint = tmp_path / "pipeline.json"
    job.pipeline.save(blueprint)
    saved_nodes = json.loads(blueprint.read_text())["nodes"]
    by_label = {descriptor["label"]: descriptor for descriptor in saved_nodes.values()}
    loaded = Pipeline.load(blueprint)
    assert set(loaded.nodes) == {"s", "delivered", "down", "tail"}
    for label in SIDE_EFFECT_NODES:
        node = loaded.nodes[label]
        assert node.content_hash().to_string() == by_label[label]["content_hash"]
        assert node.pipeline_hash().to_string() == by_label[label]["pipeline_hash"]
        with pytest.raises(RuntimeError, match="cannot deliver"):
            _ = node.as_table()
    assert sent == delivered_before


def test_a_file_without_recorded_nullability_restores_side_effect_nodes(
    saved: tuple[PipelineJob, list[int], pathlib.Path], tmp_path: pathlib.Path
) -> None:
    """A job file of format 0.1.0 (v0.2: no ``nullable`` record) restores them too."""
    _, _, path = saved
    data = json.loads(path.read_text())
    data["orcapod_pipeline_job_version"] = "0.1.0"
    data["pipeline"]["orcapod_pipeline_version"] = "0.1.0"
    for descriptor in data["pipeline"]["nodes"].values():
        del descriptor["output_schema"]["nullable"]
        descriptor.get("source_config", {}).pop("nullable", None)
    old = tmp_path / "format_0_1_0.json"
    old.write_text(json.dumps(data))

    loaded = PipelineJob.load(old)
    assert set(loaded.nodes) == {"s", "delivered", "down", "tail"}
    assert loaded.nodes["delivered"].keys() == (("k",), ("x",))
    # Saving it again keeps the nullability unrecorded, as for every other node.
    again = tmp_path / "again.json"
    loaded.save(again)
    for descriptor in _descriptors(again).values():
        assert "nullable" not in descriptor["output_schema"]


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
def test_a_load_only_run_passes_rows_through_and_delivers_nothing(
    saved: tuple[PipelineJob, list[int], pathlib.Path], orchestrator: str
) -> None:
    """``loaded.run(load_only=True)``: the rows pass through, nothing is delivered.

    The function node below a side-effect node loads its stored results, and each
    side-effect node holds the rows it passed on as loaded only.
    """
    job, sent, path = saved
    delivered_before = list(sent)
    loaded = PipelineJob.load(path)
    loaded.bind(sources={"s": _source()})
    # The same job built from code, run load-only, is what the loaded job must match.
    for label in SIDE_EFFECT_NODES:
        job.nodes[label].clear_cache()
    for each in (job, loaded):
        if orchestrator == "sync":
            _ = each.run(load_only=True)
        else:
            _ = each.run(orchestrator=AsyncPipelineOrchestrator(), load_only=True)
    assert sent == delivered_before
    assert sorted(loaded.nodes["down"].as_table()["y"].to_pylist()) == [
        100 + i for i in range(ROWS)
    ]
    for label in SIDE_EFFECT_NODES:
        node = loaded.nodes[label]
        assert node.as_table().equals(job.nodes[label].as_table())
        assert node.head(100).status is job.nodes[label].head(100).status
        assert node.head(100).status is ViewStatus.LOADED_ONLY
    # A run that is not load-only still raises, and still delivers nothing.
    with pytest.raises(RuntimeError):
        _ = loaded.run()
    for label in SIDE_EFFECT_NODES:
        with pytest.raises(RuntimeError, match="cannot deliver"):
            _ = loaded.nodes[label].run()
    assert sent == delivered_before


def test_a_load_only_node_run_of_a_loaded_side_effect_node(
    saved: tuple[PipelineJob, list[int], pathlib.Path],
) -> None:
    """``node.run(load_only=True)`` loads the nodes above it and delivers nothing."""
    _, sent, path = saved
    delivered_before = list(sent)
    loaded = PipelineJob.load(path)
    loaded.bind(sources={"s": _source()})
    view = loaded.nodes["tail"].run(load_only=True)
    assert view.status is ViewStatus.LOADED_ONLY
    assert view.as_table().num_rows == ROWS
    assert loaded.nodes["delivered"].as_table().num_rows == ROWS
    assert loaded.nodes["down"].as_table().num_rows == ROWS
    assert sent == delivered_before


def test_a_loaded_side_effect_node_keeps_the_content_hash_of_the_job_it_was_saved_from(
    saved: tuple[PipelineJob, list[int], pathlib.Path], tmp_path: pathlib.Path
) -> None:
    """Before bind, after bind and a load-only run, and after saving and loading again."""
    job, _, path = saved
    live = {label: job.nodes[label].content_hash() for label in SIDE_EFFECT_NODES}
    loaded = PipelineJob.load(path)
    assert {label: loaded.nodes[label].content_hash() for label in live} == live
    loaded.bind(sources={"s": _source()})
    _ = loaded.run(load_only=True)
    assert {label: loaded.nodes[label].content_hash() for label in live} == live
    again = tmp_path / "again.json"
    loaded.save(again)
    reloaded = PipelineJob.load(again)
    assert {label: reloaded.nodes[label].content_hash() for label in live} == live


def test_a_job_file_without_the_recorded_hash_restores_the_descriptor_hash(
    saved: tuple[PipelineJob, list[int], pathlib.Path], tmp_path: pathlib.Path
) -> None:
    """A file saved before side-effect nodes' hashes were recorded still loads.

    Such a node then has the hash its descriptor records, the blueprint's, as a
    loaded function node without a recorded hash has.
    """
    _, _, path = saved
    data = json.loads(path.read_text())
    for label in SIDE_EFFECT_NODES:
        del data["run"]["node_job_content_hashes"][label]
    older = tmp_path / "older.json"
    older.write_text(json.dumps(data))
    descriptors = _descriptors(older)
    loaded = PipelineJob.load(older)
    for label in SIDE_EFFECT_NODES:
        node = loaded.nodes[label]
        assert node.content_hash().to_string() == descriptors[label]["content_hash"]


class _RunUris(NoOpObserver):
    """Records the ``pipeline_uri`` of every run it observes."""

    def __init__(self) -> None:
        self.uris: list[str] = []

    def on_run_start(self, run_id: str, pipeline_uri: str = "") -> None:
        """Record ``pipeline_uri``."""
        self.uris.append(pipeline_uri)


def test_a_load_only_run_of_a_loaded_job_records_the_live_jobs_pipeline_uri(
    saved: tuple[PipelineJob, list[int], pathlib.Path],
) -> None:
    """The run's ``pipeline_uri`` hashes its end nodes: here the side-effect node ``tail``."""
    job, sent, path = saved
    delivered_before = list(sent)
    loaded = PipelineJob.load(path)
    loaded.bind(sources={"s": _source()})
    for label in SIDE_EFFECT_NODES:
        job.nodes[label].clear_cache()
    live_uris, loaded_uris = _RunUris(), _RunUris()
    _ = job.run(load_only=True, observer=live_uris)
    _ = loaded.run(load_only=True, observer=loaded_uris)
    assert len(live_uris.uris) == len(loaded_uris.uris) == 1
    assert loaded_uris.uris == live_uris.uris
    assert sent == delivered_before


def test_a_side_effect_node_below_a_node_not_restored_is_skipped_with_a_warning(
    saved: tuple[PipelineJob, list[int], pathlib.Path],
    tmp_path: pathlib.Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A node of a kind this version does not restore does not fail the whole load.

    The side-effect node below it is left out with a warning naming it; the rest loads.
    """
    _, _, path = saved
    data = json.loads(path.read_text())
    for descriptor in data["pipeline"]["nodes"].values():
        if descriptor["label"] == "down":
            descriptor["node_type"] = "a_kind_this_version_does_not_know"
    unknown = tmp_path / "unknown_kind.json"
    unknown.write_text(json.dumps(data))

    with caplog.at_level(logging.WARNING):
        loaded = PipelineJob.load(unknown)
    assert set(loaded.nodes) == {"s", "delivered"}
    assert any("'tail'" in record.getMessage() for record in caplog.records)


def test_a_loaded_blueprint_side_effect_node_cannot_become_an_invocation(
    tmp_path: pathlib.Path,
) -> None:
    """``to_invocations()`` refuses a loaded side-effect node, as it does a loaded
    function or operator node: the node has no side-effect pod to hand on."""
    sent: list[int] = []

    def deliver(ctx: Any, x: int) -> None:
        """Record ``x``."""
        sent.append(x)

    job = PipelineJob(name="only", database=DeltaTableDatabase(tmp_path / "store"))
    with job:
        _ = SideEffectPod(deliver)(_source(), label="delivered")
    _ = job.run()
    blueprint = tmp_path / "pipeline.json"
    job.pipeline.save(blueprint)

    loaded = Pipeline.load(blueprint)
    with pytest.raises(RuntimeError, match="SideEffectNode.*no live side-effect pod"):
        _ = loaded.to_invocations()


def test_a_loaded_side_effect_node_over_a_non_job_stream_runs_load_only(
    saved: tuple[PipelineJob, list[int], pathlib.Path],
) -> None:
    """A load-only run reaches ``run_alone`` when no job node feeds the node.

    ``PipelineJob.load`` always puts a job node upstream, so the node is built directly
    over a source's stream (#381 x #384: ``run_alone`` takes the unchecked source ids).
    """
    from orcapod.pipeline.loaded_side_effects import LoadedSideEffectJobNode

    _, sent, path = saved
    delivered_before = list(sent)
    descriptor = _descriptors(path)["delivered"]
    node = LoadedSideEffectJobNode(descriptor, _source())  # pyright: ignore[reportArgumentType]  # a source is a stream
    view = node.run(load_only=True)
    assert view.status is ViewStatus.LOADED_ONLY
    assert view.as_table().num_rows == ROWS
    assert sent == delivered_before
