"""Defects in the job node current-run view found by a full review (ITL-1121).

The job node view (ITL-1095): a job node shows its current run; a run that raises leaves the
nodes it ran empty and stale; a node that holds a current run is not run again; a load-only
run computes, delivers and writes nothing. Each test reproduces one review comment through the
public interface (``superpowers/scripts/itl_1121`` holds the scripts).
"""

from __future__ import annotations

from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.pipeline.observer import NoOpObserver
from orcapod.side_effects import SideEffectPod, tap_pod
from orcapod.sources import DictSource

ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}


@function_pod(output_keys="y")
def inc(x: int) -> int:
    """Return ``x + 1``."""
    return x + 1


def _tap(fn: Any) -> SideEffectPod:
    """A tap side-effect pod over ``fn``."""
    pod = tap_pod(fn)
    assert isinstance(pod, SideEffectPod)
    return pod


def _source() -> DictSource:
    """Three rows, tag ``k``, data ``x``."""
    return DictSource(
        [{"k": i, "x": i} for i in range(3)], tag_columns=["k"], source_id="s"
    )


# ---------------------------------------------------------------------------
# Comment 1: a function node is stamped before on_node_end
# ---------------------------------------------------------------------------


class _ReadingObserver(NoOpObserver):
    """Reads each job node in ``on_node_end``: the row count it sees, by label."""

    def __init__(self, job: PipelineJob) -> None:
        self.job = job
        self.seen: dict[str, int] = {}

    def contextualize(self, *identity_path: str) -> _ReadingObserver:
        """The same observer for every node."""
        return self

    def on_node_end(self, node_label: str, node_hash: str) -> None:
        """Read the node that just ended."""
        node = self.job.nodes.get(node_label)
        if node is not None:
            self.seen[node_label] = node.as_table().num_rows


def _rowwise_job(db: Any) -> PipelineJob:
    """``rowwise`` = ``inc`` over three rows."""
    job = PipelineJob(name="j", database=db)
    with job:
        inc.pod(_source(), label="rowwise")
    return job


@pytest.mark.parametrize("load_only", [False, True])
@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_an_observer_reading_a_node_in_on_node_end_sees_its_rows(
    orchestrator: str, load_only: bool
) -> None:
    """The node is current when ``on_node_end`` fires, and keeps its rows after the run."""
    db = InMemoryArrowDatabase()
    if load_only:
        _rowwise_job(db).run()  # store the results a load-only run loads
    job = _rowwise_job(db)
    observer = _ReadingObserver(job)
    job.run(
        orchestrator=ORCHESTRATORS[orchestrator](),
        observer=observer,
        load_only=load_only,
    )
    node = job.nodes["rowwise"]
    assert observer.seen["rowwise"] == 3
    assert node.as_table().num_rows == 3
    assert node.holds_current_run(load_only=load_only)


# ---------------------------------------------------------------------------
# Comment 2: an operator node's lazy output that fails to materialize clears the node
# ---------------------------------------------------------------------------

_FAIL = {"on": True}


@function_pod(output_keys="y")
def flaky(x: int) -> int:
    """Raise while ``_FAIL["on"]``; else return ``x + 1``."""
    if _FAIL["on"]:
        raise RuntimeError("input failed")
    return x + 1


def _base_stream() -> Any:
    """Two rows, tag ``id`` and data ``x``, both non-nullable ``int64``."""
    import pyarrow as pa

    from orcapod.core.streams import ArrowTableStream

    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
        ]
    )
    return ArrowTableStream(
        pa.table({"id": [1, 2], "x": [1, 2]}, schema=schema), tag_columns=["id"]
    )


@pytest.mark.parametrize("with_database", [False, True])
def test_an_operator_run_whose_lazy_output_raises_holds_no_run(
    with_database: bool,
) -> None:
    """No database, or ``CacheMode.OFF``: the output is computed when the view is built;
    if that raises, the node reads as one with no run, and the next run computes."""
    from orcapod.core.nodes.operator_node import OperatorJobNode
    from orcapod.core.operators import MapData
    from orcapod.types import CacheMode

    _FAIL["on"] = True
    node = OperatorJobNode(
        operator=MapData({"y": "z"}),
        input_streams=(flaky.pod(_base_stream()),),
        pipeline_database=InMemoryArrowDatabase() if with_database else None,
        cache_mode=CacheMode.OFF,
    )
    with pytest.raises(RuntimeError, match="input failed"):
        node.run()
    assert not node.holds_current_run(load_only=False)
    assert node.is_stale
    assert node.as_table().num_rows == 0
    _FAIL["on"] = False
    assert node.run().as_table().num_rows == 2


# ---------------------------------------------------------------------------
# Comment 4: the summary says "nothing to do" only when the run ran nothing
# ---------------------------------------------------------------------------

_DELIVERED: list[int] = []
_NOTHING_TO_DO = "nothing to do; every node holds a current run"


def _deliver(y: int, ctx: Any) -> None:
    """A side effect: record the delivered value."""
    _DELIVERED.append(y)


def _job_below_rowwise(below: str) -> PipelineJob:
    """``rowwise`` (``inc``) with a side-effect node or an operator node ``below`` it."""
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        rows: Any = inc.pod(_source(), label="rowwise")
        if below == "side_effect":
            _tap(_deliver).process(rows, label="below")
        else:
            rows.select_data_columns(["y"], label="below")
    return job


def _run_job_or_node(job: PipelineJob, how: str) -> Any:
    """Run ``job`` on an orchestrator, or run its node ``below``; return the summary."""
    if how == "node.run":
        job.nodes["below"].run()
        return job.nodes["below"].last_run
    return job.run(orchestrator=ORCHESTRATORS[how]())


@pytest.mark.parametrize("how", ["sync", "async", "node.run"])
@pytest.mark.parametrize("below", ["side_effect", "operator"])
def test_a_run_that_ran_a_cleared_node_is_not_nothing_to_do(
    below: str, how: str
) -> None:
    """A node cleared below a held function node runs again (a side effect delivers);
    the summary says what ran, not that there was nothing to do."""
    _DELIVERED.clear()
    job = _job_below_rowwise(below)
    _run_job_or_node(job, how)
    job.nodes["below"].clear_cache()
    _DELIVERED.clear()
    summary = _run_job_or_node(job, how)
    if below == "side_effect":
        assert len(_DELIVERED) == 3
    assert job.nodes["below"].holds_current_run(load_only=False)
    assert summary.nodes["rowwise"].held == 3
    assert _NOTHING_TO_DO not in repr(summary)
    assert repr(summary).splitlines()[0].endswith("all rows succeeded.")


# A node.run() of a node that holds a current run returns at once and makes no summary
# (node.last_run stays the previous one), so "nothing ran" is a job run's case.
@pytest.mark.parametrize("how", ["sync", "async"])
@pytest.mark.parametrize("below", ["side_effect", "operator"])
def test_a_run_that_ran_nothing_is_nothing_to_do(below: str, how: str) -> None:
    """A second run of an unchanged job runs nothing and says so."""
    job = _job_below_rowwise(below)
    _run_job_or_node(job, how)
    summary = _run_job_or_node(job, how)
    assert _NOTHING_TO_DO in repr(summary).splitlines()[0]


@pytest.mark.parametrize("how", ["sync", "async", "node.run"])
def test_a_job_with_no_function_node_says_what_ran(how: str) -> None:
    """Only an operator: the first run succeeds; a second job run has nothing to do."""
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _source().select_data_columns(["x"], label="below")
    first = _run_job_or_node(job, how)
    assert repr(first).splitlines()[0].endswith("all rows succeeded.")
    if how != "node.run":  # a held node.run() makes no summary (see above)
        second = _run_job_or_node(job, how)
        assert _NOTHING_TO_DO in repr(second).splitlines()[0]


# ---------------------------------------------------------------------------
# Comment 5: a load-only execute() whose input raises clears the side-effect node
# ---------------------------------------------------------------------------


@function_pod(output_keys="z")
def _boom(y: int) -> int:
    """Always raise."""
    raise RuntimeError("input failed")


def _failing_y_stream() -> Any:
    """A pod stream outside a job (tag ``k``, data ``z``) that raises when read."""
    import pyarrow as pa

    from orcapod.core.streams import ArrowTableStream

    schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("y", pa.int64(), nullable=False),
        ]
    )
    return _boom.pod(
        ArrowTableStream(
            pa.table({"k": [1], "y": [1]}, schema=schema), tag_columns=["k"]
        )
    )


class _FailingChannel:
    """An input channel whose iteration raises."""

    def __aiter__(self) -> _FailingChannel:
        return self

    async def __anext__(self) -> Any:
        raise RuntimeError("input failed")


@pytest.mark.parametrize("path", ["sync", "async"])
@pytest.mark.parametrize("load_only", [False, True])
def test_an_execute_whose_input_raises_leaves_the_side_effect_node_stale(
    path: str, load_only: bool
) -> None:
    """The node and the node below it read as not run, whatever the run kind.

    ``execute``/``async_execute`` are the node's run entry points (orchestrators call
    them); this test is about them, so it calls them directly.
    """
    import asyncio

    from orcapod.channels import Channel

    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        rows = inc.pod(_source(), label="rowwise")
        tapped: Any = _tap(_deliver).process(rows, label="tap")
        tapped.select_data_columns(["y"], label="down")
    job.run()
    tap, down = job.nodes["tap"], job.nodes["down"]
    # The async delivery path reads its input inside a task group, which raises the
    # input's error in an ExceptionGroup.
    with pytest.raises((RuntimeError, ExceptionGroup)):
        if path == "sync":
            tap.execute(_failing_y_stream(), load_only=load_only)
        else:
            out: Channel[Any] = Channel()
            asyncio.run(
                tap.async_execute([_FailingChannel()], out.writer, load_only=load_only)
            )
    assert tap.is_stale and down.is_stale
    assert tap.as_table().num_rows == 0
    assert down.as_table().num_rows == 0


# ---------------------------------------------------------------------------
# Comment 6: a load-only run refuses an input that delivers when it is read
# ---------------------------------------------------------------------------

_UPSTREAM_DELIVERED: list[int] = []


def _deliver_upstream(x: int, ctx: Any) -> None:
    """The side effect of the stream outside a job: record the delivered value."""
    _UPSTREAM_DELIVERED.append(x)


def _deliver_own(x: int, ctx: Any) -> None:
    """The side effect of the node under test: record the delivered value."""
    _DELIVERED.append(x)


@function_pod(output_keys="w")
def _twice(x: int) -> int:
    """Return ``2 * x``."""
    return 2 * x


# The decorated function's pod, typed for a node's constructor.
_twice_pod: Any = _twice.pod


def _delivering_stream() -> Any:
    """A side-effect pod's stream outside a job: reading it delivers each row."""
    return _tap(_deliver_upstream).process(_base_stream())


def _node_over(kind: str, stream: Any) -> Any:
    """A job node of ``kind`` built directly over ``stream`` (no job)."""
    from orcapod.core.nodes.function_node import FunctionJobNode
    from orcapod.core.nodes.operator_node import OperatorJobNode
    from orcapod.core.operators import MapData
    from orcapod.side_effects import SideEffectJobNode
    from orcapod.types import CacheMode

    if kind == "side_effect":
        return SideEffectJobNode(_tap(_deliver_own), stream)
    if kind == "function":
        return FunctionJobNode(
            _twice_pod, stream, pipeline_database=InMemoryArrowDatabase()
        )
    return OperatorJobNode(
        operator=MapData({"x": "renamed"}),
        input_streams=(stream,),
        pipeline_database=InMemoryArrowDatabase(),
        cache_mode=CacheMode.OFF,
    )


@pytest.mark.parametrize("kind", ["side_effect", "function", "operator"])
def test_a_load_only_run_refuses_an_input_that_delivers(kind: str) -> None:
    """Refused before the input is read: nothing is delivered and the node holds no run."""
    _UPSTREAM_DELIVERED.clear()
    _DELIVERED.clear()
    node = _node_over(kind, _delivering_stream())
    with pytest.raises(ValueError, match="delivers its side effects when it is read"):
        node.run(load_only=True)
    assert _UPSTREAM_DELIVERED == []
    assert _DELIVERED == []
    assert not node.holds_current_run(load_only=True)


@pytest.mark.parametrize("kind", ["side_effect", "function", "operator"])
def test_a_load_only_cascade_refuses_an_input_that_delivers(kind: str) -> None:
    """The input that delivers feeds a node the cascade would run first."""
    from orcapod.core.nodes.operator_node import OperatorJobNode
    from orcapod.core.operators import DropNulls
    from orcapod.types import CacheMode

    _UPSTREAM_DELIVERED.clear()
    first = _node_over(kind, _delivering_stream())
    target = OperatorJobNode(
        operator=DropNulls(),
        input_streams=(first,),
        pipeline_database=InMemoryArrowDatabase(),
        cache_mode=CacheMode.OFF,
    )
    with pytest.raises(ValueError, match="delivers its side effects when it is read"):
        target.run(load_only=True)
    assert _UPSTREAM_DELIVERED == []
    assert not first.holds_current_run(load_only=True)
    assert not target.holds_current_run(load_only=True)


@pytest.mark.parametrize("kind", ["side_effect", "function", "operator"])
def test_a_full_run_over_an_input_that_delivers_reads_it(kind: str) -> None:
    """Unchanged: a full run reads the stream outside a job, which delivers."""
    _UPSTREAM_DELIVERED.clear()
    node = _node_over(kind, _delivering_stream())
    assert node.run().as_table().num_rows == 2
    assert len(_UPSTREAM_DELIVERED) == 2


def _job_over_source_bound_to_a_delivering_stream() -> PipelineJob:
    """``tw`` (``_twice``) over source slot ``s``, run once, then bound to a stream that
    delivers when read."""
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        source = DictSource(
            [{"id": 1, "x": 1}, {"id": 2, "x": 2}], tag_columns=["id"], source_id="s"
        )
        _twice_pod(source, label="tw")
    job.run()
    job.bind(sources={"s": _delivering_stream()})
    return job


@pytest.mark.parametrize("how", ["sync", "async", "node.run", "source.run"])
def test_a_load_only_run_refuses_a_source_slot_bound_to_a_delivering_stream(
    how: str,
) -> None:
    """A source slot reads its bound stream: refused before it is read."""
    _UPSTREAM_DELIVERED.clear()
    job = _job_over_source_bound_to_a_delivering_stream()
    with pytest.raises(ValueError, match="delivers its side effects when it is read"):
        if how == "node.run":
            job.nodes["tw"].run(load_only=True)
        elif how == "source.run":
            job.nodes["tw"].upstreams[0].run(load_only=True)
        else:
            job.run(orchestrator=ORCHESTRATORS[how](), load_only=True)
    assert _UPSTREAM_DELIVERED == []


@pytest.mark.parametrize("kind", ["side_effect", "function", "operator"])
def test_a_load_only_run_refuses_a_stream_that_reads_a_delivering_stream(
    kind: str,
) -> None:
    """An operator's stream outside a job over a side-effect stream delivers when read."""
    _UPSTREAM_DELIVERED.clear()
    stream: Any = _delivering_stream().select_data_columns(["x"])
    node = _node_over(kind, stream)
    with pytest.raises(ValueError, match="delivers its side effects when it is read"):
        node.run(load_only=True)
    assert _UPSTREAM_DELIVERED == []


def test_a_load_only_operator_run_without_a_database_is_loaded_only() -> None:
    """Its rows come from a load-only run: shown as such, and not a full run's result."""
    from orcapod.core.nodes.operator_node import OperatorJobNode
    from orcapod.core.operators import MapData
    from orcapod.core.streams import ViewStatus

    node = OperatorJobNode(
        operator=MapData({"x": "y"}),
        input_streams=(_base_stream(),),
        pipeline_database=None,
    )
    assert node.run(load_only=True).status == ViewStatus.LOADED_ONLY
    assert node.holds_current_run(load_only=True)
    assert not node.holds_current_run(load_only=False)
    assert node.run().status == ViewStatus.ROWS


def test_an_interrupted_log_commit_leaves_the_side_effect_node_without_a_run() -> None:
    """A standalone side-effect node with a store: a KeyboardInterrupt while its run commits
    the invocation log clears the node, so the next run runs (and commits) again."""
    from unittest import mock

    from orcapod.side_effects import SideEffectJobNode

    _DELIVERED.clear()
    node = SideEffectJobNode(_tap(_deliver_own), _base_stream())
    node.attach_databases(InMemoryArrowDatabase())
    with (
        mock.patch.object(
            InMemoryArrowDatabase, "flush", side_effect=KeyboardInterrupt
        ),
        pytest.raises(KeyboardInterrupt),
    ):
        node.run()
    assert not node.holds_current_run(load_only=False)
    assert node.is_stale
    with mock.patch.object(
        InMemoryArrowDatabase, "flush", autospec=True, side_effect=lambda self: None
    ) as flush:
        node.run()
    assert flush.call_count >= 1


# ---------------------------------------------------------------------------
# Developer decision (November 14, 2026): a load-only run also refuses an input that
# computes when read ("Refuse is consistent")
# ---------------------------------------------------------------------------

_UPSTREAM_CALLS: list[int] = []
_COMPUTES = "computes when it is read"


@function_pod(output_keys="x")
def _recorded_copy(x: int) -> int:
    """Return ``x``, recording the call."""
    _UPSTREAM_CALLS.append(x)
    return x


def _computing_stream(kind: str) -> Any:
    """A stream outside a job that computes ``_recorded_copy`` when read.

    ``pod``: the function pod's stream; ``cached``: a cached function pod's stream over
    an empty store; ``wrapped``: an operator's stream outside a job over the pod's
    stream. (A ``FunctionNode`` outside a job is a blueprint: reading it raises.)
    """
    from orcapod.core.cached_function_pod import CachedFunctionPod

    base = _base_stream()
    pod: Any = _recorded_copy.pod
    if kind == "pod":
        return pod(base)
    if kind == "cached":
        return CachedFunctionPod(pod, result_database=InMemoryArrowDatabase())(base)
    stream: Any = pod(base)
    return stream.select_data_columns(["x"])


@pytest.mark.parametrize("stream_kind", ["pod", "cached", "wrapped"])
@pytest.mark.parametrize("kind", ["side_effect", "function", "operator"])
def test_a_load_only_run_refuses_an_input_that_computes(
    kind: str, stream_kind: str
) -> None:
    """Refused before the input is read: its function is never called."""
    _UPSTREAM_CALLS.clear()
    node = _node_over(kind, _computing_stream(stream_kind))
    with pytest.raises(ValueError, match=_COMPUTES):
        node.run(load_only=True)
    assert _UPSTREAM_CALLS == []
    assert not node.holds_current_run(load_only=True)


@pytest.mark.parametrize("kind", ["side_effect", "function", "operator"])
def test_a_load_only_cascade_refuses_an_input_that_computes(kind: str) -> None:
    """The computing input feeds a node the cascade would run first."""
    from orcapod.core.nodes.operator_node import OperatorJobNode
    from orcapod.core.operators import DropNulls
    from orcapod.types import CacheMode

    _UPSTREAM_CALLS.clear()
    first = _node_over(kind, _computing_stream("pod"))
    target = OperatorJobNode(
        operator=DropNulls(),
        input_streams=(first,),
        pipeline_database=InMemoryArrowDatabase(),
        cache_mode=CacheMode.OFF,
    )
    with pytest.raises(ValueError, match=_COMPUTES):
        target.run(load_only=True)
    assert _UPSTREAM_CALLS == []


@pytest.mark.parametrize("how", ["sync", "async", "node.run", "source.run"])
def test_a_load_only_run_refuses_a_source_slot_bound_to_a_computing_stream(
    how: str,
) -> None:
    """A source slot bound to a function pod's stream outside a job: refused."""
    _UPSTREAM_CALLS.clear()
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        source = DictSource(
            [{"id": 1, "x": 1}, {"id": 2, "x": 2}], tag_columns=["id"], source_id="s"
        )
        _twice_pod(source, label="tw")
    job.run()
    job.bind(sources={"s": _computing_stream("pod")})
    with pytest.raises(ValueError, match=_COMPUTES):
        if how == "node.run":
            job.nodes["tw"].run(load_only=True)
        elif how == "source.run":
            job.nodes["tw"].upstreams[0].run(load_only=True)
        else:
            job.run(orchestrator=ORCHESTRATORS[how](), load_only=True)
    assert _UPSTREAM_CALLS == []


@pytest.mark.parametrize("kind", ["side_effect", "function", "operator"])
def test_a_full_run_over_an_input_that_computes_reads_it(kind: str) -> None:
    """Unchanged: a run without ``load_only`` reads (computes) the outside stream."""
    _UPSTREAM_CALLS.clear()
    node = _node_over(kind, _computing_stream("pod"))
    assert node.run().as_table().num_rows == 2
    assert len(_UPSTREAM_CALLS) == 2


# ---------------------------------------------------------------------------
# PR #388 review round 2: what the load-only refusal walks through
# ---------------------------------------------------------------------------


def _held_table_over_a_computing_stream() -> Any:
    """An ``ArrowTableStream`` holding its rows, whose ``upstreams`` name a function pod's
    stream outside a job; reading it reads only the held table."""
    import pyarrow as pa

    from orcapod.core.streams import ArrowTableStream

    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
        ]
    )
    return ArrowTableStream(
        pa.table({"id": [1, 2], "x": [1, 2]}, schema=schema),
        tag_columns=["id"],
        upstreams=(_computing_stream("pod"),),
    )


@pytest.mark.parametrize("kind", ["side_effect", "function", "operator"])
def test_a_load_only_run_reads_a_table_stream_whatever_its_upstreams(
    kind: str,
) -> None:
    """An ``ArrowTableStream`` neither computes nor delivers: not refused."""
    _UPSTREAM_CALLS.clear()
    node = _node_over(kind, _held_table_over_a_computing_stream())
    node.run(load_only=True)
    assert node.holds_current_run(load_only=True)
    assert _UPSTREAM_CALLS == []


def _blueprint_delivering_node() -> Any:
    """A ``SideEffectNode`` outside a job: reading it delivers each row."""
    from orcapod.side_effects import SideEffectNode

    return SideEffectNode(_tap(_deliver_upstream), _base_stream())


@pytest.mark.parametrize("kind", ["side_effect", "function", "operator"])
def test_a_load_only_run_refuses_a_side_effect_node_outside_a_job(kind: str) -> None:
    """Refused before the input is read: nothing is delivered."""
    _UPSTREAM_DELIVERED.clear()
    node = _node_over(kind, _blueprint_delivering_node())
    with pytest.raises(ValueError, match="delivers its side effects when it is read"):
        node.run(load_only=True)
    assert _UPSTREAM_DELIVERED == []
    assert not node.holds_current_run(load_only=True)


@pytest.mark.parametrize("how", ["sync", "async", "node.run", "source.run"])
def test_a_load_only_run_refuses_a_source_slot_bound_to_a_side_effect_node(
    how: str,
) -> None:
    """A source slot bound to a ``SideEffectNode`` outside a job: refused."""
    _UPSTREAM_DELIVERED.clear()
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        source = DictSource(
            [{"id": 1, "x": 1}, {"id": 2, "x": 2}], tag_columns=["id"], source_id="s"
        )
        _twice_pod(source, label="tw")
    job.run()
    job.bind(sources={"s": _blueprint_delivering_node()})
    with pytest.raises(ValueError, match="delivers its side effects when it is read"):
        if how == "node.run":
            job.nodes["tw"].run(load_only=True)
        elif how == "source.run":
            job.nodes["tw"].upstreams[0].run(load_only=True)
        else:
            job.run(orchestrator=ORCHESTRATORS[how](), load_only=True)
    assert _UPSTREAM_DELIVERED == []


@pytest.mark.parametrize("kind", ["side_effect", "function", "operator"])
def test_a_full_run_over_a_side_effect_node_outside_a_job_reads_it(kind: str) -> None:
    """Unchanged: a run without ``load_only`` reads it, which delivers."""
    _UPSTREAM_DELIVERED.clear()
    node = _node_over(kind, _blueprint_delivering_node())
    assert node.run().as_table().num_rows == 2
    assert len(_UPSTREAM_DELIVERED) == 2
