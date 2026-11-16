"""An operator stream whose result is its unread input stays current after a read (ITL-1083).

An operator that has nothing to do (a select of every column, a rename of no column, a filter
with no predicate, a join of one stream, a semi-join with no common key) returns its input stream
itself, unread. The operator stream keeps that stream as its result before the input has computed.
When the input then completes its first computation (the read of the operator stream reads it),
that is not a change of the input (the rule ITL-1065 set for pod streams): the operator stream
stays current, its head shows the rows, and the next read neither runs the operator again nor
calls a function. A real change of the input after the read (``clear_cache()``) still makes the
next read run the operator again, with the new values.
"""

from __future__ import annotations

import contextlib
from collections import Counter
from collections.abc import Callable, Iterator
from typing import Any, cast
from unittest import mock

import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import (
    DropDataColumns,
    DropNulls,
    DropTagColumns,
    Join,
    MapData,
    MapTags,
    PolarsFilter,
    SelectDataColumns,
    SelectTagColumns,
    SemiJoin,
)
from orcapod.core.operators.static_output_pod import (
    DynamicPodStream,
    StaticOutputOperatorPod,
)
from orcapod.core.sources import ArrowTableSource, DictSource
from orcapod.core.streams.base import StreamBase
from orcapod.databases import InMemoryArrowDatabase
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.streams import ViewStatus

IDS = [3, 1, 4, 0, 2]

CALLS: Counter[str] = Counter()
EPOCH = [0]


def stamped(x: int) -> int:
    """Return ``x`` tagged with the current epoch: a function whose values can change."""
    CALLS["stamped"] += 1
    return 1000 * EPOCH[0] + x


def double(y: int) -> int:
    """Return ``2 * y``; counted."""
    CALLS["double"] += 1
    return 2 * y


@pytest.fixture(autouse=True)
def _reset_state() -> Iterator[None]:
    """Clear the call record and the epoch around each test."""
    CALLS.clear()
    EPOCH[0] = 0
    yield
    CALLS.clear()
    EPOCH[0] = 0


def _source() -> ArrowTableSource:
    """A source with tag ``id`` and data ``x``."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
        ]
    )
    table = pa.Table.from_arrays(
        [pa.array(IDS, type=pa.int64()), pa.array(IDS, type=pa.int64())],
        schema=schema,
    )
    return ArrowTableSource(
        table, tag_columns=["id"], source_id="src", tag_only_columns=True
    )


def _unrelated() -> ArrowTableSource:
    """A source sharing no column with any stream here: tag ``j``, data ``q``."""
    schema = pa.schema(
        [
            pa.field("j", pa.int64(), nullable=False),
            pa.field("q", pa.int64(), nullable=False),
        ]
    )
    table = pa.Table.from_arrays(
        [pa.array([0, 1], type=pa.int64()), pa.array([5, 6], type=pa.int64())],
        schema=schema,
    )
    return ArrowTableSource(
        table, tag_columns=["j"], source_id="unrelated", tag_only_columns=True
    )


def _partner() -> ArrowTableSource:
    """A source keyed like ``_source`` (tag ``id``) with data ``q``."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("q", pa.int64(), nullable=False),
        ]
    )
    table = pa.Table.from_arrays(
        [pa.array(IDS, type=pa.int64()), pa.array(IDS, type=pa.int64())],
        schema=schema,
    )
    return ArrowTableSource(
        table, tag_columns=["id"], source_id="partner", tag_only_columns=True
    )


def _stamped_pod() -> FunctionPod:
    """The pod of ``stamped`` (``x`` to ``y``)."""
    pod = function_pod(output_keys="y")(stamped).pod
    assert isinstance(pod, FunctionPod)
    return pod


def _double_pod() -> FunctionPod:
    """The pod of ``double`` (``y`` to ``z``)."""
    pod = function_pod(output_keys="z")(double).pod
    assert isinstance(pod, FunctionPod)
    return pod


# Each pass-through operator applied to a stream: it has nothing to do, so its
# result is the stream itself.
PASS_THROUGH: dict[str, Callable[[StreamProtocol], StreamProtocol]] = {
    "select_tags": lambda s: SelectTagColumns(list(s.keys()[0]))(s),
    "select_data": lambda s: SelectDataColumns(list(s.keys()[1]))(s),
    "drop_tags": lambda s: DropTagColumns(["absent"], strict=False)(s),
    "drop_data": lambda s: DropDataColumns(["absent"], strict=False)(s),
    "map_tags": lambda s: MapTags({"absent": "renamed"}, strict=False)(s),
    "map_data": lambda s: MapData({"absent": "renamed"}, strict=False)(s),
    "filter": lambda s: PolarsFilter()(s),
    "join_one": lambda s: Join()(s),
    "semi_join_disjoint": lambda s: SemiJoin()(s, _unrelated()),
}


def _upstream(kind: str) -> tuple[StreamProtocol, StreamProtocol]:
    """The stream an operator is applied to, and the pod stream of ``stamped`` under it.

    Kinds: a pod stream; a cached pod stream; a pass-through operator stream over a pod
    stream; a pod stream over a pod stream; a pass-through operator stream over a
    pass-through operator stream over a pod stream.
    """
    if kind == "cached_pod":
        first = CachedFunctionPod(
            _stamped_pod(), result_database=InMemoryArrowDatabase()
        ).process(_source())
        return first, first
    first = _stamped_pod().process(_source())
    if kind == "pod":
        return first, first
    if kind == "operator":
        return SelectDataColumns(["y"])(first), first
    if kind == "pod_chain":
        return _double_pod().process(first), first
    if kind == "operator_chain":
        return MapData({"absent": "renamed"}, strict=False)(
            SelectDataColumns(["y"])(first)
        ), first
    raise AssertionError(kind)


UPSTREAMS = ["pod", "cached_pod", "operator", "pod_chain", "operator_chain"]


def _chain_of(stream: StreamProtocol) -> list[StreamProtocol]:
    """``stream`` and every stream above it, down to (not including) the source."""
    chain = [stream]
    while chain[-1].upstreams and not isinstance(
        chain[-1].upstreams[0], ArrowTableSource
    ):
        chain.append(chain[-1].upstreams[0])
    return chain


@contextlib.contextmanager
def _counting_operator_runs(streams: list[StreamProtocol]) -> Iterator[Counter[int]]:
    """Count the runs of the operator of every operator stream in ``streams``.

    A spy on the non-public ``static_process`` of each operator instance: no public
    signal tells whether an operator stream ran its operator again.
    """
    runs: Counter[int] = Counter()
    with contextlib.ExitStack() as patches:
        for index, stream in enumerate(streams):
            if not isinstance(stream, DynamicPodStream):
                continue
            pod = stream.producer
            assert isinstance(pod, StaticOutputOperatorPod)
            original = pod.static_process

            def counting(
                *args: Any,
                _index: int = index,
                _original: Callable[..., Any] = original,
                **kwargs: Any,
            ) -> Any:
                runs[_index] += 1
                return _original(*args, **kwargs)

            patches.enter_context(mock.patch.object(pod, "static_process", counting))
        yield runs


def _read(stream: StreamProtocol, how: str) -> int:
    """Read ``stream`` with ``how`` and return the number of rows read."""
    if how == "as_table":
        return stream.as_table().num_rows
    if how == "iter_data":
        return len(list(stream.iter_data()))
    if how == "run":
        return stream.run().as_table().num_rows
    if how == "flow":
        assert isinstance(stream, StreamBase)
        return len(stream.flow())
    raise AssertionError(how)


def _stale(streams: list[StreamProtocol]) -> list[bool]:
    """Whether each stream is stale."""
    return [bool(getattr(stream, "is_stale")) for stream in streams]


def _ys(stream: StreamProtocol) -> dict[int, int]:
    """Each row's ``y`` value, keyed by its ``id`` tag."""
    table = stream.as_table()
    return dict(
        zip(
            cast(list[int], table.column("id").to_pylist()),
            cast(list[int], table.column("y").to_pylist()),
            strict=True,
        )
    )


def test_the_reported_reproduction() -> None:
    """A select of every column over an unread pod stream keeps its rows after ``as_table``."""
    calls: list[int] = []

    @function_pod(output_keys="y")
    def plus_one(x: int) -> int:
        calls.append(x)
        return x + 1

    source = DictSource(
        [{"k": i, "x": i} for i in range(3)],
        tag_columns=["k"],
        source_id="s",
        tag_only_columns=True,
    )
    selected = SelectDataColumns(["y"])(plus_one.pod(source))
    assert selected.as_table().num_rows == 3
    assert not getattr(selected, "is_stale")
    head = selected.head()
    assert head.status is ViewStatus.ROWS, head.header()
    assert head.table.num_rows == 3
    with _counting_operator_runs([selected]) as runs:
        assert selected.as_table().num_rows == 3
    assert runs == Counter()
    assert len(calls) == 3


@pytest.mark.parametrize("how", ["as_table", "iter_data", "flow", "run"])
@pytest.mark.parametrize("upstream", UPSTREAMS)
@pytest.mark.parametrize("operator", sorted(PASS_THROUGH))
def test_a_pass_through_operator_stream_is_current_after_a_read(
    operator: str, upstream: str, how: str
) -> None:
    """After a read, the stream and everything above it is current and is not run again."""
    top, _ = _upstream(upstream)
    stream = PASS_THROUGH[operator](top)
    streams = _chain_of(stream)
    assert _read(stream, how) == len(IDS)
    calls = sum(CALLS.values())
    assert _stale(streams) == [False] * len(streams)
    head = stream.head(100)
    assert head.status is ViewStatus.ROWS, head.header()
    assert head.table.num_rows == len(IDS)
    with _counting_operator_runs(streams) as runs:
        assert stream.as_table().num_rows == len(IDS)
        assert _read(stream, how) == len(IDS)
    assert runs == Counter()
    assert sum(CALLS.values()) == calls
    assert _stale(streams) == [False] * len(streams)


@pytest.mark.parametrize("how", ["as_table", "iter_data", "flow", "run"])
@pytest.mark.parametrize("operator", sorted(PASS_THROUGH))
def test_a_pod_stream_over_a_pass_through_operator_stream_reads_once(
    operator: str, how: str
) -> None:
    """A pod stream reading the operator stream is current after a read: no function call again."""
    first = _stamped_pod().process(_source())
    last = _double_pod().process(PASS_THROUGH[operator](first))
    streams = _chain_of(last)
    assert _read(last, how) == len(IDS)
    assert CALLS == Counter(stamped=len(IDS), double=len(IDS))
    assert _stale(streams) == [False] * len(streams)
    CALLS.clear()
    for again in ("as_table", "iter_data", "flow", "run"):
        assert _read(last, again) == len(IDS)
    head = last.head(100)
    assert head.status is ViewStatus.ROWS, head.header()
    assert head.table.num_rows == len(IDS)
    assert CALLS == Counter()
    assert _stale(streams) == [False] * len(streams)


@pytest.mark.parametrize("upstream", UPSTREAMS)
@pytest.mark.parametrize("operator", sorted(PASS_THROUGH))
def test_a_real_change_after_the_read_still_reruns_the_operator(
    operator: str, upstream: str
) -> None:
    """``clear_cache()`` of the pod stream after the read: the next read runs the operator again.

    The head shows no rows until then. A cached pod stream may serve its stored results
    again, so there only the rerun and the agreement with the pod stream are checked.
    """
    top, first = _upstream(upstream)
    stream = PASS_THROUGH[operator](top)
    streams = _chain_of(stream)
    assert _read(stream, "as_table") == len(IDS)
    EPOCH[0] = 1
    getattr(first, "clear_cache")()
    assert stream.head(100).status is ViewStatus.NOT_COMPUTED
    with _counting_operator_runs(streams) as runs:
        table = stream.as_table()
    assert runs[0] == 1
    assert table.num_rows == len(IDS)
    ys = _ys(first)
    if "y" in table.column_names:
        assert _ys(stream) == ys
    if upstream != "cached_pod":
        assert {y // 1000 for y in ys.values()} == {1}
    assert _stale(streams) == [False] * len(streams)


@pytest.mark.parametrize("operator", sorted(PASS_THROUGH))
def test_a_reset_further_up_a_chain_still_reruns_the_operator(operator: str) -> None:
    """A reset of the first pod in a pod chain makes the operator over the second run again.

    The second pod stream is reset only when it is next read; the operator stream still
    sees the change, and its next read gives values of the new state.
    """
    first = _stamped_pod().process(_source())
    second = _double_pod().process(first)
    stream = PASS_THROUGH[operator](second)
    streams = _chain_of(stream)
    before = stream.as_table()
    EPOCH[0] = 1
    getattr(first, "clear_cache")()
    with _counting_operator_runs(streams) as runs:
        after = stream.as_table()
    assert runs[0] == 1
    zs = sorted(cast(list[int], after.column("z").to_pylist()))
    assert zs == sorted(2 * (1000 + i) for i in IDS)
    assert zs != sorted(cast(list[int], before.column("z").to_pylist()))
    with _counting_operator_runs(streams) as runs:
        assert stream.as_table().equals(after)
    assert runs == Counter()
    assert _stale(streams) == [False] * len(streams)


RESETS: list[Callable[[], None]] = []


def double_and_reset(y: int) -> int:
    """Return ``2 * y``; the first call runs the pending reset, if any."""
    CALLS["double_and_reset"] += 1
    if RESETS:
        RESETS.pop()()
    return 2 * y


@pytest.mark.parametrize("reader", ["drop_nulls", "join"])
def test_a_reset_while_a_reading_operator_reads_leaves_it_stale(reader: str) -> None:
    """An upstream reset while the operator reads its input: the kept result is not current.

    The operator reads a pod stream whose function resets the pod stream above it on its
    first call, so the operator reads rows of two states. The operator stream is then
    stale, and its next read gives rows of the state after the reset only.
    """
    first = _stamped_pod().process(_source())
    second = function_pod(output_keys="z")(double_and_reset).pod.process(first)
    stream = (
        DropNulls()(second) if reader == "drop_nulls" else Join()(second, _partner())
    )

    def reset_first() -> None:
        """Move to a new epoch and reset the first pod stream."""
        EPOCH[0] = 1
        getattr(first, "clear_cache")()

    RESETS.append(reset_first)
    _ = stream.as_table()
    assert RESETS == []
    assert getattr(stream, "is_stale")
    zs = cast(list[int], stream.as_table().column("z").to_pylist())
    assert sorted(zs) == sorted(2 * (1000 + i) for i in IDS)
    assert _stale(_chain_of(stream)) == [False, False, False]
