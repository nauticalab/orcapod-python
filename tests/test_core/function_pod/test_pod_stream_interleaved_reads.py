"""Interleaved and resumed reads of a function pod stream compute each row once (ITL-1065).

Two iterators open on one pod stream share its results: a row one of them computed is the
row the other yields, and a later full read yields the same datagrams. An upstream that
completes (or, for an operator stream, computes) for the first time while the stream is
being read is not a change of the stream's input, whatever the upstream's kind: a source, an
operator stream (the automatic column selection of ``pod(stream)``), a pod stream, or a pod
stream over an operator stream. A real reset of the input between two reads is still a
change: the stream's next read starts again, and every later read is of one state.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterator
from typing import cast

import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from orcapod.streams import HeadStatus

from .test_pod_stream_staleness import _ConcurrentExecutor

# Rows of the source, not in tag order.
IDS = [3, 1, 4, 0, 2, 5]

CALLS: Counter[str] = Counter()
EPOCH = [0]

UPSTREAMS = ["source", "operator", "pod_stream", "pod_over_operator"]


def f(x: int) -> int:
    """Return ``x + 1``; counted."""
    CALLS["f"] += 1
    return x + 1


def g(y: int) -> int:
    """Return ``2 * y``; counted."""
    CALLS["g"] += 1
    return 2 * y


def stamped(x: int) -> int:
    """Return ``x`` tagged with the current epoch: a function whose values change."""
    CALLS["stamped"] += 1
    return 1000 * EPOCH[0] + x


@pytest.fixture(autouse=True)
def _reset() -> Iterator[None]:
    """Clear the call record and the epoch around each test."""
    CALLS.clear()
    EPOCH[0] = 0
    yield
    CALLS.clear()
    EPOCH[0] = 0


def _source(extra: bool) -> ArrowTableSource:
    """A source with tag ``id`` and data ``x``; with ``extra``, a data column ``e`` too.

    A pod taking only ``x`` called on the source with ``e`` puts an automatic
    ``SelectDataColumns`` operator stream in front of itself.
    """
    fields = [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("x", pa.int64(), nullable=False),
    ]
    arrays = [pa.array(IDS, pa.int64()), pa.array([10 * i for i in IDS], pa.int64())]
    if extra:
        fields.append(pa.field("e", pa.int64(), nullable=False))
        arrays.append(pa.array([-i for i in IDS], pa.int64()))
    table = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
    return ArrowTableSource(table, tag_columns=["id"], source_id="src")


def _other() -> ArrowTableSource:
    """A second source with tag ``id`` and data ``q``, in another row order."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("q", pa.int64(), nullable=False),
        ]
    )
    ids = sorted(IDS)
    table = pa.Table.from_arrays(
        [pa.array(ids, pa.int64()), pa.array([7 * i for i in ids], pa.int64())],
        schema=schema,
    )
    return ArrowTableSource(table, tag_columns=["id"], source_id="other")


def _pod(
    function: object, output: str, executor: str, cached: bool
) -> FunctionPod | CachedFunctionPod:
    """The pod of ``function``; when ``cached``, storing its results in a fresh store."""
    pod = function_pod(
        output_keys=output,
        executor=_ConcurrentExecutor() if executor == "concurrent" else None,
    )(function).pod
    assert isinstance(pod, FunctionPod)
    if not cached:
        return pod
    return CachedFunctionPod(pod, result_database=InMemoryArrowDatabase())


def _stream(upstream: str, executor: str, cached: bool) -> StreamProtocol:
    """The pod stream read by the tests, over an input of kind ``upstream``."""
    if upstream == "source":
        return _pod(f, "y", executor, cached)(_source(extra=False))
    if upstream == "operator":
        return _pod(f, "y", executor, cached)(_source(extra=True))
    first = _pod(f, "y", executor, cached)(
        _source(extra=upstream == "pod_over_operator")
    )
    return _pod(g, "z", executor, cached)(first)


def _expected_calls(upstream: str) -> Counter[str]:
    """Each function of the stream called once per input row."""
    if upstream in ("source", "operator"):
        return Counter({"f": len(IDS)})
    return Counter({"f": len(IDS), "g": len(IDS)})


Row = tuple[TagProtocol, DataProtocol]


def _same_datagrams(left: list[Row], right: list[Row]) -> bool:
    """Whether two reads yielded the very same data datagrams, in the same order."""
    return len(left) == len(right) and all(
        a is b for (_, a), (_, b) in zip(left, right, strict=True)
    )


def _stale(stream: StreamProtocol) -> bool:
    """Whether ``stream`` is stale (``is_stale`` is not on ``StreamProtocol``)."""
    return bool(getattr(stream, "is_stale"))


@pytest.mark.parametrize("cached", [False, True], ids=["plain", "cached"])
@pytest.mark.parametrize("executor", ["sequential", "concurrent"])
@pytest.mark.parametrize("upstream", UPSTREAMS)
def test_interleaved_reads_compute_each_row_once(
    upstream: str, executor: str, cached: bool
) -> None:
    """Two interleaved reads and a later full read share one computation of every row."""
    stream = _stream(upstream, executor, cached)
    first = iter(stream.iter_data())
    second = iter(stream.iter_data())
    first_rows = [next(first)]
    second_rows = [next(second) for _ in range(3)]
    first_rows += list(first)
    second_rows += list(second)
    assert CALLS == _expected_calls(upstream)
    assert not _stale(stream)

    third_rows = list(stream.iter_data())
    assert CALLS == _expected_calls(upstream)
    assert len(third_rows) == len(IDS)
    assert _same_datagrams(first_rows, third_rows)
    assert _same_datagrams(second_rows, third_rows)


@pytest.mark.parametrize("cached", [False, True], ids=["plain", "cached"])
@pytest.mark.parametrize("executor", ["sequential", "concurrent"])
@pytest.mark.parametrize("upstream", UPSTREAMS)
def test_a_read_after_an_abandoned_partial_read_computes_each_row_once(
    upstream: str, executor: str, cached: bool
) -> None:
    """A read left after one row is resumed by the next read, not started again."""
    stream = _stream(upstream, executor, cached)
    partial = iter(stream.iter_data())
    first_row = next(partial)
    del partial
    rows = list(stream.iter_data())
    assert CALLS == _expected_calls(upstream)
    assert len(rows) == len(IDS)
    assert rows[0][1] is first_row[1]
    assert not _stale(stream)


def _values(stream: StreamProtocol, column: str) -> dict[int, int]:
    """Each row's ``column`` value, keyed by its ``id`` tag, read with ``iter_data``."""
    return {
        cast(int, tag.as_dict()["id"]): cast(int, data.as_dict()[column])
        for tag, data in stream.iter_data()
    }


def h(z: int) -> int:
    """Return ``z - 3``; counted."""
    CALLS["h"] += 1
    return z - 3


def _over_stamped(through: str, executor: str) -> tuple[StreamProtocol, StreamProtocol]:
    """The stamped stream and the last stream over it, ``through`` the given links.

    ``pod_stream``: a pod reading the stamped stream. ``operator``: a pod over a join of
    it (the pod call adds an automatic selection). ``pod_chain``: two pods, so the reset
    is two links up from the last stream.
    """
    stamped_stream = _pod(stamped, "y", executor, cached=False)(_source(extra=False))
    if through == "operator":
        # A join adds data column ``q``, so the pod call adds an automatic selection.
        last = _pod(g, "z", executor, cached=False)(Join()(stamped_stream, _other()))
    else:
        last = _pod(g, "z", executor, cached=False)(stamped_stream)
    if through == "pod_chain":
        last = _pod(h, "w", executor, cached=False)(last)
    return stamped_stream, last


def _derived(through: str, y: int) -> int:
    """The last stream's value for a stamped value ``y``."""
    return 2 * y - 3 if through == "pod_chain" else 2 * y


@pytest.mark.parametrize("executor", ["sequential", "concurrent"])
@pytest.mark.parametrize("through", ["pod_stream", "operator", "pod_chain"])
def test_a_reset_between_interleaved_reads_gives_one_state(
    through: str, executor: str
) -> None:
    """A reset of the input between two interleaved reads is a change of content.

    The stamped stream, one or two links up, is reset after the first read took one row.
    The second read then starts again from the new state, and once both reads are drained
    the stream is current and every read of it is of the new state only.
    """
    stamped_stream, last = _over_stamped(through, executor)
    output = "w" if through == "pod_chain" else "z"
    first = iter(last.iter_data())
    second = iter(last.iter_data())
    _ = next(first)
    EPOCH[0] = 1
    clear_cache = getattr(stamped_stream, "clear_cache")
    clear_cache()
    assert _stale(last)
    second_rows = [next(second) for _ in range(3)]
    _ = list(first)
    second_rows += list(second)
    assert not _stale(last)

    CALLS.clear()
    third_rows = list(last.iter_data())
    assert CALLS == Counter()
    assert _same_datagrams(second_rows, third_rows)
    values = _values(last, output)
    ys = _values(stamped_stream, "y")
    assert CALLS == Counter()
    assert sorted(values) == sorted(ys) == sorted(IDS)
    assert {y // 1000 for y in ys.values()} == {1}
    for key, y in ys.items():
        assert values[key] == _derived(through, y), key


@pytest.mark.parametrize("executor", ["sequential", "concurrent"])
@pytest.mark.parametrize("through", ["pod_stream", "operator", "pod_chain"])
def test_an_abandoned_read_does_not_hide_a_later_reset(
    through: str, executor: str
) -> None:
    """A read left after one row stays resumable, and a reset after it is still seen.

    The abandoned read keeps the stream's partial results and its open input; the next
    read resumes it unless the input changed. Here the stamped stream is reset after the
    read was abandoned, however far up it is, so the stream is stale and the next read
    starts again from the new state.
    """
    stamped_stream, last = _over_stamped(through, executor)
    output = "w" if through == "pod_chain" else "z"
    abandoned = iter(last.iter_data())
    _ = next(abandoned)
    del abandoned
    EPOCH[0] = 1
    clear_cache = getattr(stamped_stream, "clear_cache")
    clear_cache()
    assert _stale(last)

    values = _values(last, output)
    assert not _stale(last)
    ys = _values(stamped_stream, "y")
    assert sorted(values) == sorted(ys) == sorted(IDS)
    assert {y // 1000 for y in ys.values()} == {1}
    for key, y in ys.items():
        assert values[key] == _derived(through, y), key


@pytest.mark.parametrize("executor", ["sequential", "concurrent"])
@pytest.mark.parametrize("read", ["as_table", "head"])
@pytest.mark.parametrize("upstream", UPSTREAMS)
def test_a_table_or_head_during_an_open_read_computes_each_row_once(
    upstream: str, read: str, executor: str
) -> None:
    """``as_table()`` or ``head()`` while an iterator is open shares the stream's results."""
    stream = _stream(upstream, executor, cached=False)
    first = iter(stream.iter_data())
    first_rows = [next(first)]
    if read == "as_table":
        assert stream.as_table().num_rows == len(IDS)
    else:
        assert stream.head(100).table.num_rows >= 1
    first_rows += list(first)
    assert CALLS == _expected_calls(upstream)
    assert not _stale(stream)
    third_rows = list(stream.iter_data())
    assert CALLS == _expected_calls(upstream)
    assert _same_datagrams(first_rows, third_rows)
    assert stream.as_table().num_rows == len(IDS)
    assert CALLS == _expected_calls(upstream)


@pytest.mark.parametrize("executor", ["sequential", "concurrent"])
@pytest.mark.parametrize("upstream", UPSTREAMS)
def test_the_head_after_an_abandoned_read_shows_its_rows(
    upstream: str, executor: str
) -> None:
    """A read left after three rows is not stale: ``head()`` shows the rows it computed.

    On the sequential path three rows are computed and the head is partial; the concurrent
    path computes every row on the first one, so its head is complete. No head computes.
    """
    stream = _stream(upstream, executor, cached=False)
    abandoned = iter(stream.iter_data())
    for _ in range(3):
        _ = next(abandoned)
    del abandoned
    calls = Counter(CALLS)
    assert not _stale(stream)
    head = stream.head(2)
    assert head.status is (
        HeadStatus.PARTIAL if executor == "sequential" else HeadStatus.ROWS
    ), head.header()
    assert head.table.num_rows == 2
    assert CALLS == calls
