"""Every stream has ``run()``, which computes it and returns a ``StreamView`` (ITL-1081).

Developer decision, November 13, 2026: the notebook preview and ``head()`` never compute,
and there is one uniform way to compute any stream: ``run()``. It takes no arguments, does
the work ``as_table()`` does (through the same path and cache) and returns a ``StreamView``
of all the computed rows. Job nodes keep their own ``run()`` (it computes, where their
``as_table()`` only reads) and return the view too. A side-effect stream outside a job keeps
no rows: each ``run()`` delivers again, as each ``as_table()`` does, and its ``head()`` stays
rowless. A job's side-effect node keeps the rows its run passed on, and its reads deliver
nothing (ITL-1091).
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil
from collections.abc import Callable
from typing import Any
from unittest import mock

import pyarrow as pa
import pytest

import orcapod
from orcapod import function_pod
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod, FunctionPodStream
from orcapod.core.nodes.function_node import FunctionJobNode, FunctionNode
from orcapod.core.nodes.operator_node import OperatorJobNode, OperatorNode
from orcapod.core.nodes.source_node import SourceJobNode, SourceNode, SourceNodeBase
from orcapod.core.operators import Join, MapData, SelectDataColumns
from orcapod.core.sources import (
    ArrowTableSource,
    CachedSource,
    DerivedSource,
    DictSource,
)
from orcapod.core.sources.polling_source import PollingSource
from orcapod.core.sources.source_proxy import SourceProxy
from orcapod.core.streams import ArrowTableStream
from orcapod.core.streams.base import StreamBase
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import PipelineJobRequiredError, UnboundSourceError
from orcapod.pipeline.job import PipelineJob
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.side_effects import SideEffectJobNode, SideEffectNode, SideEffectPod
from orcapod.streams import StreamView, ViewStatus
from orcapod.types import CacheMode, Schema
from tests.test_core.streams.test_head_and_preview import _FakeDynamicSource

ROWS = 25


def _table(rows: int = ROWS) -> pa.Table:
    """``id`` and ``x``, both non-nullable ``int64``."""
    return pa.table(
        {"id": list(range(rows)), "x": [10 * i for i in range(rows)]},
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )


def _source(rows: int = ROWS, label: str = "readings") -> ArrowTableSource:
    """An ``ArrowTableSource`` over ``_table(rows)``."""
    return ArrowTableSource(
        _table(rows),
        tag_columns=["id"],
        source_id="readings",
        label=label,
        tag_only_columns=True,
    )


def _counting_pod(calls: list[int]) -> Any:
    """A function pod tripling ``x`` that appends each input to ``calls``."""

    @function_pod(output_keys="y")
    def triple(x: int) -> int:
        """Triple ``x``, recording the call."""
        calls.append(x)
        return 3 * x

    return triple


# ---------------------------------------------------------------------------
# One signature on every stream kind
# ---------------------------------------------------------------------------


def _all_stream_classes() -> list[type]:
    """Every class deriving from ``StreamBase``, plus the source node classes."""
    for module in pkgutil.walk_packages(orcapod.__path__, "orcapod."):
        try:
            importlib.import_module(module.name)
        except ImportError:  # an optional dependency is missing
            continue
    found: set[type] = set()
    pending: list[type] = [StreamBase, SourceNodeBase]
    while pending:
        cls = pending.pop()
        if cls in found:
            continue
        found.add(cls)
        pending.extend(cls.__subclasses__())
    return sorted(found, key=lambda c: f"{c.__module__}.{c.__qualname__}")


@pytest.mark.parametrize("cls", _all_stream_classes(), ids=lambda c: c.__qualname__)
def test_every_stream_class_has_run_without_positional_arguments(cls: type) -> None:
    """``run`` exists on every stream class and takes no positional argument (developer
    decision, ITL-1081); a job node's ``run``, and that of every subclass of one, takes
    one keyword-only argument, ``load_only`` (ITL-1095)."""
    run = getattr(cls, "run", None)
    assert callable(run), f"{cls.__qualname__} has no run()"
    parameters = inspect.signature(run).parameters
    names = list(parameters)
    if issubclass(
        cls, (FunctionJobNode, OperatorJobNode, SideEffectJobNode, SourceJobNode)
    ):
        assert names == ["self", "load_only"], f"{cls.__qualname__}.run{names}"
        load_only = parameters["load_only"]
        assert load_only.kind is inspect.Parameter.KEYWORD_ONLY
        assert load_only.default is False
    else:
        assert names == ["self"], f"{cls.__qualname__}.run{names}"


def test_the_stream_protocol_declares_run() -> None:
    """``run`` is part of ``StreamProtocol``; a stream without it does not conform."""
    assert "run" in StreamProtocol.__dict__
    assert isinstance(_source(), StreamProtocol)
    assert isinstance(
        SourceNode("s", Schema({"id": int}), Schema({"x": int})), StreamProtocol
    )


# ---------------------------------------------------------------------------
# Stream kinds that compute or read: run() is the as_table() work, then a view
# ---------------------------------------------------------------------------


def _function_pod_stream() -> Any:
    """A function pod stream tripling ``x`` of ``_source()``, labelled ``triple``."""
    return _counting_pod([]).pod(_source(), label="triple")


def _cached_function_pod_stream() -> Any:
    """The stream of a ``CachedFunctionPod`` (in-memory result database) over ``_source()``."""
    pod = CachedFunctionPod(
        _counting_pod([]).pod, result_database=InMemoryArrowDatabase()
    )
    return pod.process(_source())


def _operator_stream() -> Any:
    """A ``MapData`` stream renaming ``y`` of ``_function_pod_stream()`` to ``z``."""
    # An operator that computes a new table (a rename), not one that hands its input on.
    return MapData({"y": "z"})(_function_pod_stream(), label="rename")


def _join_stream() -> Any:
    """A ``Join`` stream of ``_source()`` with a ``DictSource`` of ``z`` on ``id``."""
    other = DictSource(
        [{"id": i, "z": i + 1} for i in range(ROWS)],
        tag_columns=["id"],
        label="z",
        tag_only_columns=True,
    )
    return Join()(_source(), other)


def _polling_source() -> Any:
    """A ``PollingSource`` over a fake dynamic source of one row."""
    return PollingSource(_FakeDynamicSource(), tag_columns="id")


def _cached_source() -> Any:
    """A ``CachedSource`` over ``_source()`` with an in-memory cache database."""
    return CachedSource(_source(), cache_database=InMemoryArrowDatabase())


def _derived_source() -> Any:
    """A ``DerivedSource`` of ``_source()`` offering tag ``id`` as data ``id_value``."""
    return DerivedSource(_source(), tags_as_data={"id": "id_value"})


def _bound_source_proxy() -> Any:
    """A ``SourceProxy`` for ``_source()``, bound to a live ``_source()``."""
    live = _source()
    proxy = SourceProxy(
        source_id="readings",
        expected_class_name="ArrowTableSource",
        tag_schema=Schema({"id": int}),
        data_schema=Schema({"x": int}),
        content_hash_str=live.content_hash().to_string(),
        pipeline_hash_str=live.pipeline_hash().to_string(),
    )
    proxy.bind(live)
    return proxy


def _source_job_node() -> Any:
    """A ``SourceJobNode`` over ``_source()``."""
    return SourceJobNode.from_stream(_source(), name="readings")


def _function_job_node() -> Any:
    """The ``triple`` function job node of a ``PipelineJob`` over ``_source()``, not run."""
    job = PipelineJob(name="itl1081", database=InMemoryArrowDatabase())
    with job:
        _ = _counting_pod([]).pod(_source(), label="triple")
    return job.nodes["triple"]


def _operator_job_node() -> Any:
    """The ``rename`` operator job node of a ``PipelineJob``, its upstream ``triple`` run."""
    job = PipelineJob(name="itl1081", database=InMemoryArrowDatabase())
    with job:
        tripled = _counting_pod([]).pod(_source(), label="triple")
        _ = MapData({"y": "z"})(tripled, label="rename")
    job.nodes["triple"].run()
    return job.nodes["rename"]


KINDS: dict[str, Callable[[], Any]] = {
    "arrow_table_stream": lambda: ArrowTableStream(_table(), tag_columns=["id"]),
    "arrow_table_source": _source,
    "dict_source": lambda: DictSource(
        [{"id": i, "x": i} for i in range(ROWS)],
        tag_columns=["id"],
        label="d",
        tag_only_columns=True,
    ),
    "function_pod_stream": _function_pod_stream,
    "cached_function_pod_stream": _cached_function_pod_stream,
    "operator_stream": _operator_stream,
    "join_stream": _join_stream,
    "polling_source": _polling_source,
    "cached_source": _cached_source,
    "derived_source": _derived_source,
    "source_proxy": _bound_source_proxy,
    "source_job_node": _source_job_node,
    "function_job_node": _function_job_node,
    "operator_job_node": _operator_job_node,
}


@pytest.mark.parametrize("kind", list(KINDS))
def test_run_returns_a_view_of_every_computed_row(kind: str) -> None:
    """``run()`` returns a ``StreamView`` of all rows: ``as_table()`` and ``head(n)`` agree."""
    stream = KINDS[kind]()
    view = stream.run()
    assert isinstance(view, StreamView)
    assert view.status is ViewStatus.ROWS
    table = stream.as_table()
    assert view.as_table().equals(table)
    assert view.total_rows == table.num_rows == len(view)
    assert stream.head(3).table.equals(view.head(3).table)
    assert view.head(3).table.equals(table.slice(0, 3))


@pytest.mark.parametrize(
    "kind",
    [
        "function_pod_stream",
        "cached_function_pod_stream",
        "operator_stream",
        "join_stream",
        "polling_source",
        "cached_source",
        "function_job_node",
        "operator_job_node",
    ],
)
def test_head_before_run_has_no_rows(kind: str) -> None:
    """Before ``run()``, a stream that must compute or read first shows no rows."""
    stream = KINDS[kind]()
    assert len(stream.head()) == 0
    assert stream.head().status is not ViewStatus.ROWS
    _ = stream.run()
    assert stream.head().status is ViewStatus.ROWS
    assert len(stream.head()) > 0


def test_run_view_captions() -> None:
    """The view of ``run()`` says it holds all the computed rows."""
    stream = _function_pod_stream()
    assert stream.head().header() == (
        "FunctionPodStream[triple] — not computed; run() computes it"
    )
    assert stream.run().header() == "FunctionPodStream[triple] — all 25 computed rows"
    assert _source().run().header() == "ArrowTableSource[readings] — all 25 rows"
    held = ArrowTableStream(_table(), tag_columns=["id"])
    assert held.run().header() == "ArrowTableStream[ArrowTableStream] — all 25 rows"


def _operator_job_node_replaying() -> Any:
    """An operator job node in ``CacheMode.REPLAY`` over rows an earlier node stored."""
    store = InMemoryArrowDatabase()
    _ = OperatorJobNode(
        MapData({"x": "z"}),
        (_source(),),
        pipeline_database=store,
        cache_mode=CacheMode.LOG,
    ).run()
    return OperatorJobNode(
        MapData({"x": "z"}),
        (_source(),),
        pipeline_database=store,
        cache_mode=CacheMode.REPLAY,
    )


def _job_node_after_job_run(name: str) -> Any:
    """Node ``name`` (``triple`` or ``rename``) of a ``PipelineJob`` that has run."""
    job = PipelineJob(name="itl1081", database=InMemoryArrowDatabase())
    with job:
        tripled = _counting_pod([]).pod(_source(), label="triple")
        _ = MapData({"y": "z"})(tripled, label="rename")
    job.run()
    return job.nodes[name]


CAPTION_KINDS: dict[str, Callable[[], Any]] = {
    **KINDS,
    "operator_job_node_replaying": _operator_job_node_replaying,
    "function_job_node_after_job_run": lambda: _job_node_after_job_run("triple"),
    "operator_job_node_after_job_run": lambda: _job_node_after_job_run("rename"),
}


@pytest.mark.parametrize("kind", list(CAPTION_KINDS))
def test_run_view_and_head_name_the_same_rows_alike(kind: str) -> None:
    """A ``run()`` view says what ``head()`` says of the same rows afterwards."""
    stream = CAPTION_KINDS[kind]()
    view = stream.run()
    assert view.head(3).header() == stream.head(3).header()


# ---------------------------------------------------------------------------
# No second computation path
# ---------------------------------------------------------------------------


def test_function_is_called_once_per_row_across_run_and_as_table() -> None:
    """``run()``, ``as_table()`` and a second ``run()`` share one computation."""
    calls: list[int] = []
    stream = _counting_pod(calls).pod(_source())
    assert isinstance(stream, FunctionPodStream)
    _ = stream.run()
    _ = stream.as_table()
    _ = stream.run()
    assert len(calls) == ROWS


def test_operator_over_a_function_computes_each_row_once() -> None:
    """Through an operator stream too, each row is computed once."""
    calls: list[int] = []
    out = MapData({"y": "z"})(_counting_pod(calls).pod(_source()))
    _ = out.run()
    _ = out.as_table()
    _ = out.run()
    assert len(calls) == ROWS


def test_cached_function_pod_stream_computes_each_row_once() -> None:
    """A cached pod's stream computes each row once across run and as_table."""
    calls: list[int] = []
    pod = CachedFunctionPod(
        _counting_pod(calls).pod, result_database=InMemoryArrowDatabase()
    )
    stream = pod.process(_source())
    _ = stream.run()
    _ = stream.as_table()
    _ = stream.run()
    assert len(calls) == ROWS


def _calls_over_rows(spy: mock.MagicMock) -> int:
    """How many calls of the spied ``static_process`` were given rows.

    A call over inputs with no rows gives only the operator's zero-row table, which a
    head with no rows shows without computing a row (ITL-912).
    """
    return sum(
        any(stream.as_table().num_rows for stream in call.args[1:])
        for call in spy.call_args_list
    )


def test_operator_stream_run_recomputes_after_its_upstream_changed() -> None:
    """``run()`` on an operator stream recomputes a stale result, as ``as_table()`` does."""
    calls: list[int] = []
    fn_stream = _counting_pod(calls).pod(_source())
    assert isinstance(fn_stream, FunctionPodStream)
    out = MapData({"y": "z"})(fn_stream)
    original = MapData.static_process
    # A spy on the non-public static_process: the test is about whether it runs again.
    with mock.patch.object(
        MapData, "static_process", autospec=True, side_effect=original
    ) as spy:
        first = out.run()
        assert _calls_over_rows(spy) == 1
        _ = out.run()
        assert _calls_over_rows(spy) == 1
        fn_stream.clear_cache()  # the upstream is modified after the result was kept
        assert out.head().status is ViewStatus.NOT_COMPUTED
        second = out.run()
        assert _calls_over_rows(spy) == 2
    assert first.as_table().equals(second.as_table())
    assert len(calls) == 2 * ROWS


def test_head_after_run_of_a_pass_through_operator_shows_the_rows() -> None:
    """After ``run()``, a pass-through operator stream's ``head()`` shows its rows."""
    out = SelectDataColumns(["y"])(_counting_pod([]).pod(_source()))
    view = out.run()
    assert out.head(3).table.equals(view.head(3).table)


def test_job_node_run_over_a_pass_through_chain_returns_its_rows() -> None:
    """A job node without a database returns the rows its ``run()`` computed."""

    @function_pod(output_keys="w")
    def plus(y: int) -> int:
        """Add one to ``y``."""
        return y + 1

    def same(w: int) -> int:
        """Return ``w``."""
        return w

    passed_on = SelectDataColumns(["y"])(_counting_pod([]).pod(_source()))
    # PythonDataFunction conforms to DataFunctionProtocol at runtime; basedpyright
    # rejects it (the same false positive is in the baseline for other node tests).
    same_pod = FunctionPod(
        PythonDataFunction(same, output_keys="v")  # pyright: ignore[reportArgumentType]
    )
    node = FunctionJobNode(same_pod, plus.pod(passed_on))
    assert len(node.run()) == ROWS


# ---------------------------------------------------------------------------
# Sources: run() does the as_table() work
# ---------------------------------------------------------------------------


def test_polling_source_run_polls() -> None:
    """A polling source's ``run()`` polls, as ``as_table()`` does; ``head()`` never does."""
    fake = _FakeDynamicSource()
    source = PollingSource(fake, tag_columns="id")
    _ = source.head()
    assert (fake.polls, fake.fetches) == (0, 0)
    view = source.run()
    assert fake.fetches > 0
    assert view.table.column("val").to_pylist() == [7]


def test_cached_source_run_reads_and_updates_its_cache() -> None:
    """A cached source's ``run()`` writes its cache, as ``as_table()`` does."""
    store = InMemoryArrowDatabase()
    source = CachedSource(_source(), cache_database=store)
    _ = source.head()
    assert store.list_table_paths() == []
    view = source.run()
    assert store.list_table_paths() != []
    assert view.as_table().equals(source.as_table())


# ---------------------------------------------------------------------------
# Side effects: every run() delivers, as every as_table() does
# ---------------------------------------------------------------------------


def _deliveries() -> tuple[list[int], Callable[..., None]]:
    """A delivery list and a side-effect function that appends each ``x`` it delivers."""
    delivered: list[int] = []

    def deliver(x: int, ctx: Any) -> None:
        """The side effect: record the delivery."""
        delivered.append(x)

    return delivered, deliver


def test_side_effect_stream_run_delivers_each_time_and_head_stays_rowless() -> None:
    """``run()`` delivers the side effects and returns the rows that call produced."""
    delivered, deliver = _deliveries()
    stream = SideEffectPod(deliver)(_source())
    view = stream.run()
    assert len(delivered) == ROWS
    assert view.status is ViewStatus.ROWS
    assert view.as_table().equals(_source().as_table())
    # Nothing is kept: head() stays rowless and a second run() delivers again.
    assert len(stream.head()) == 0
    assert stream.head().status is ViewStatus.NOT_COMPUTED
    assert "run() delivers its side effects" in stream.head().header()
    _ = stream.run()
    assert len(delivered) == 2 * ROWS


def test_side_effect_node_run_delivers_as_as_table_does() -> None:
    """A side-effect node outside a job delivers on ``run()``, as on ``as_table()``."""
    delivered, deliver = _deliveries()
    node = SideEffectNode(
        side_effect_pod=SideEffectPod(deliver), input_stream=_source()
    )
    view = node.run()
    assert len(delivered) == ROWS
    assert len(view) == ROWS
    _ = node.as_table()
    assert len(delivered) == 2 * ROWS


def test_side_effect_job_node_run_delivers_and_its_reads_do_not() -> None:
    """A side-effect job node's ``run()`` delivers; its ``as_table()`` reads what it passed on (ITL-1091)."""
    delivered, deliver = _deliveries()
    node = SideEffectJobNode(
        side_effect_pod=SideEffectPod(deliver), input_stream=_source()
    )
    view = node.run()
    assert len(delivered) == ROWS
    assert len(view) == ROWS
    assert len(node.as_table()) == ROWS
    assert node.head().status is ViewStatus.ROWS
    assert len(delivered) == ROWS


# ---------------------------------------------------------------------------
# Blueprints raise what as_table() raises; job nodes keep their run()
# ---------------------------------------------------------------------------


def test_blueprint_nodes_raise_what_as_table_raises() -> None:
    """A blueprint node or an unbound input slot cannot compute."""
    slot = SourceNode("s", Schema({"id": int}), Schema({"x": int}))
    other = SourceNode("t", Schema({"id": int}), Schema({"z": int}))
    with pytest.raises(UnboundSourceError):
        slot.run()
    with pytest.raises(PipelineJobRequiredError):
        OperatorNode(operator=Join(), input_streams=(slot, other)).run()
    with pytest.raises(PipelineJobRequiredError):
        FunctionNode(_counting_pod([]).pod, input_stream=slot).run()


def test_function_job_node_run_computes_and_returns_the_view() -> None:
    """A job function node's ``run()`` computes as before and returns its view."""
    calls: list[int] = []
    job = PipelineJob(name="itl1081", database=InMemoryArrowDatabase())
    with job:
        _ = _counting_pod(calls).pod(_source(), label="triple")
    node = job.nodes["triple"]
    view = node.run()
    assert len(calls) == ROWS
    assert view.header() == "FunctionJobNode[triple] — all 25 rows"
    assert view.as_table().equals(node.as_table())
    _ = node.run()
    assert len(calls) == ROWS


@pytest.mark.parametrize("stored", [False, True], ids=["no_database", "cache_off"])
def test_operator_job_node_is_current_after_run_over_a_lazy_input(stored: bool) -> None:
    """After ``run()`` over an uncomputed pod stream, the node shows its rows and is kept.

    Without a database, and with ``CacheMode.OFF``, the operator's output is computed only
    when ``run()`` builds its view; the node is current after that, not stale.
    """
    calls: list[int] = []
    database = InMemoryArrowDatabase() if stored else None
    node = OperatorJobNode(
        MapData({"y": "z"}),
        (_counting_pod(calls).pod(_source()),),
        pipeline_database=database,
        cache_mode=CacheMode.OFF,
    )
    original = MapData.static_process
    # A spy on the non-public static_process: the test is about whether it runs again.
    with mock.patch.object(
        MapData, "static_process", autospec=True, side_effect=original
    ) as spy:
        view = node.run()
        assert not node.is_stale
        assert node.head(3).status is ViewStatus.ROWS
        assert node.head(3).table.equals(view.head(3).table)
        _ = node.run()
        assert spy.call_count == 1
    assert len(calls) == ROWS
