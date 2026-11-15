"""Streams outside a job keep each derived column with its own row (ITL-1034).

Paths: a source's ``ArrowTableStream`` (record-id tokens and positional tokens), a
function pod's ``FunctionPodStream`` computed row by row or concurrently (the gate forcing
every completion order on the concurrent path), a ``SideEffectPod``'s pass-through stream,
and a ``DerivedSource`` with ``tags_as_data`` (a new root with fresh provenance). Every
table must also hold exactly the expected rows (``expectations.py``). Each is read in four patterns (a
fresh read; a partial ``iter_data`` then a full read; a repeated read; two interleaved
iterations) and its ``as_table`` and ``head`` are checked under every column configuration,
for every tie scenario and input order. Every read must also give every row exactly once.
``head(n, cfg)`` takes every configuration ``as_table`` takes, sorting included, and
equals ``as_table(cfg).slice(0, n)``; before a full read it sorts the rows the stream
holds (ITL-1053).

No case is marked: the defects these paths showed (ITL-1039, ITL-1045, ITL-1046,
ITL-1065, ITL-1066) are fixed, and their exact cases run as ordinary tests.
"""

from __future__ import annotations

import itertools
from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.core.executors import LocalPythonFunctionExecutor
from orcapod.core.operators import Batch, GroupBy, Index
from orcapod.core.sources import DerivedSource
from orcapod.side_effects import SideEffectPod
from orcapod.system_constants import constants

from .checker import (
    Layout,
    assert_requested_columns,
    assert_rows_aligned,
    rows_table,
)
from .fixtures import (
    COLUMN_CONFIGS,
    INPUT_ORDERS,
    SCENARIOS,
    TAG_COLUMNS,
    Sources,
    content_hash_column,
    keys_a,
    make_sources,
    schema_hash_of,
)
from .expectations import member as member_of
from .expectations import rows
from .gate import GATE, ORDERS, new_gated_scale_pod, scale
from .oracle import Oracle, Origin
from .jobs import derive_y, pairs_function_tokens, stream_function_tokens


class ConcurrentLocalExecutor(LocalPythonFunctionExecutor):
    """The local executor, declared concurrent: the stream computes rows together."""

    @property
    def supports_concurrent_execution(self) -> bool:
        """Whether rows may be computed concurrently: yes."""
        return True


# A stream kind: builds the stream from the sources.
StreamBuilder = Callable[[Sources], Any]


def source_a(sources: Sources) -> Any:
    """Source A: record ids from ``record_id_column``."""
    return sources.a


def source_c(sources: Sources) -> Any:
    """Source C: positional record ids."""
    return sources.c


def pod_row_by_row(sources: Sources) -> Any:
    """``gated_scale`` over A, computed one row at a time (the gate is disarmed)."""
    derive_y(sources)
    return new_gated_scale_pod()(sources.a)


def pod_concurrent(sources: Sources) -> Any:
    """``gated_scale`` over A, computed concurrently; the gate forces the order."""
    derive_y(sources)
    stream = new_gated_scale_pod()(sources.a)
    stream.executor = ConcurrentLocalExecutor()
    return stream


def side_effect_stream(sources: Sources) -> Any:
    """A ``SideEffectPod`` over A: a pass-through stream that delivers each row."""

    def deliver(x: int, s: str, m: int | None, ctx: Any) -> None:
        """Deliver nothing: the stream's rows are what is checked."""

    return SideEffectPod(deliver)(sources.a)


def derived_tags_as_data(sources: Sources) -> Any:
    """``DerivedSource(A, tags_as_data={"g": "g_data"})``: a new root with fresh provenance.

    Its rows are registered in an oracle of their own (its ``x`` values repeat A's),
    from the test's own table of A with the copied column added: the copy of each row's
    ``g`` must be that row's ``g``.
    """
    derived = DerivedSource(
        sources.a, tags_as_data={"g": "g_data"}, record_id_column="x"
    )
    table = sources.oracle_tables["A"]
    copied = table.append_column(
        pa.field("g_data", table.schema.field("g").type), table.column("g")
    )
    oracle = Oracle()
    oracle.add_source(
        derived.source_id,
        copied,
        tag_columns=TAG_COLUMNS,
        schema_hash=schema_hash_of(derived),
        record_id_column="x",
    )
    sources.oracle = oracle
    sources.expected_source = derived.source_id
    return derived


KINDS: dict[str, StreamBuilder] = {
    "source_record_id": source_a,
    "source_positional": source_c,
    "pod_row_by_row": pod_row_by_row,
    "pod_concurrent": pod_concurrent,
    "side_effect": side_effect_stream,
    "derived_tags_as_data": derived_tags_as_data,
}
# The source whose rows each kind's stream holds (the derived kind sets its own).
EXPECTED_SOURCE = {"source_positional": "C"}
PATTERNS = ("fresh", "partial_then_full", "repeated", "interleaved")


def stream_params() -> list[Any]:
    """Every stream kind, completion order and read pattern."""
    params = []
    for kind in KINDS:
        orders = list(ORDERS) if kind == "pod_concurrent" else [None]
        for order in orders:
            for pattern in PATTERNS:
                name = "-".join(p for p in (kind, order, pattern) if p)
                params.append(pytest.param(kind, order, pattern, id=name))
    return params


def read(stream: Any, pattern: str) -> list[list[Any]]:
    """Read ``stream`` in ``pattern``; return the rows of every full read it made."""
    if pattern == "partial_then_full":
        partial = iter(stream.iter_data())
        next(partial)
        next(partial)
        del partial
        return [list(stream.iter_data())]
    if pattern == "repeated":
        first = list(stream.iter_data())
        # A full table between the reads.
        stream.as_table(all_info=True)
        return [first, list(stream.iter_data())]
    if pattern == "interleaved":
        first = iter(stream.iter_data())
        second = iter(stream.iter_data())
        seen_first = [next(first)]
        seen_second = list(itertools.islice(second, 3))
        seen_first += list(first)
        seen_second += list(second)
        return [seen_first, seen_second]
    return [list(stream.iter_data())]


@pytest.mark.parametrize(("kind", "order", "pattern"), stream_params())
@pytest.mark.parametrize("input_order", INPUT_ORDERS)
@pytest.mark.parametrize("scenario", list(SCENARIOS))
def test_every_row_of_a_stream_is_aligned(
    scenario: str, input_order: str, kind: str, order: str | None, pattern: str
) -> None:
    sources = make_sources(scenario, input_order)
    stream = KINDS[kind](sources)
    keys = keys_a(scenario, input_order)
    if order is None:
        GATE.disarm()
    else:
        GATE.arm(ORDERS[order](keys))
    try:
        reads = read(stream, pattern)
        realised = list(GATE.completed)
    finally:
        GATE.disarm()

    if order is not None:
        assert realised == ORDERS[order](keys), "the gate did not realise the order"
    tags, data = stream.keys()
    expected = rows(
        sources.oracle,
        sources.expected_source or EXPECTED_SOURCE.get(kind, "A"),
    )
    # A function output's tokens, matched by record id with the run's own rows.
    tokens = stream_function_tokens(stream) if "y" in data else None
    for read_rows in reads:
        assert len(read_rows) == len(keys), "a read lost or repeated rows"
        # The rows the iteration yielded, not only the tables built afterwards.
        assert_rows_aligned(
            rows_table(read_rows),
            Layout(tag_keys=tuple(tags), data_keys=tuple(data)),
            sources.oracle,
            stream.data_context,
            function_tokens=tokens,
            expected_rows=expected,
        )

    if kind == "side_effect":
        # Showing a side-effect stream never delivers: its head has no rows. Its tables
        # are checked config by config in test_side_effect_stream_tables.
        assert stream.head(100).as_table().num_rows == 0
        return
    failures = []
    for name, config in COLUMN_CONFIGS.items():
        layout = Layout(
            tag_keys=tuple(tags),
            data_keys=tuple(data),
            content_hash_column=content_hash_column(config),
        )
        full = stream.as_table(**config)
        tables = [("as_table", full), ("head", stream.head(100, **config).as_table())]
        for call, table in tables:
            try:
                assert table.num_rows == len(keys), f"{table.num_rows} rows"
                if call == "head":
                    assert table.equals(full.slice(0, len(keys))), (
                        "head(100) is not the first rows of as_table"
                    )
                assert_requested_columns(table, config, layout)
                assert_rows_aligned(
                    table,
                    layout,
                    sources.oracle,
                    stream.data_context,
                    function_tokens=tokens,
                    expected_rows=expected,
                )
            except AssertionError as exc:
                failures.append(f"{call} [{name}] {exc}")
    assert not failures, "\n".join(failures)


# ---------------------------------------------------------------------------
# Defects found by the suite, each with its exact case
# ---------------------------------------------------------------------------


def test_as_table_after_an_upstream_reset_keeps_each_hash_with_its_row() -> None:
    """An upstream reset between two ``as_table`` calls must not mix two computations."""
    counter = itertools.count()

    @function_pod(output_keys="y")
    def stamp(x: int) -> int:
        """Not deterministic: a call counter (a timestamp or a random draw alike)."""
        return 100 * next(counter) + x

    @function_pod(output_keys="z")
    def plus_one(y: int) -> int:
        """Return ``y + 1``."""
        return y + 1

    sources = make_sources("unique")
    upstream: Any = stamp.pod(sources.a.select_data_columns(["x"]))
    stream: Any = plus_one.pod(upstream)
    stream.as_table()
    upstream.clear_cache()
    table = stream.as_table(columns={"content_hash": True})
    hasher = stream.data_context.arrow_hasher
    own = [
        hasher.hash_table(table.slice(i, 1).select(["z"])).to_string()
        for i in range(table.num_rows)
    ]
    assert table.column("_content_hash").to_pylist() == own


@pytest.mark.parametrize("aggregate", ["batch", "group_by"])
def test_index_after_aggregation_gives_each_element_its_own_token(
    aggregate: str,
) -> None:
    """The element ``Index`` takes from a member list carries that member's token."""
    sources = make_sources("duplicate")
    members = (
        Batch(2)(sources.a) if aggregate == "batch" else GroupBy(by=["g"])(sources.a)
    )
    tokens = members.as_table(columns={"source": True}).column("_source_x").to_pylist()
    first = Index("x", 0, out="first")(members)
    table = first.as_table(columns={"source": True})
    expected = [member_tokens[0] if member_tokens else None for member_tokens in tokens]
    assert table.column(f"{constants.SOURCE_PREFIX}first").to_pylist() == expected


@pytest.mark.parametrize(
    ("input_order", "config_name"),
    [
        pytest.param(
            input_order,
            name,
            id=f"{input_order}-{name}",
        )
        for input_order in INPUT_ORDERS
        for name in COLUMN_CONFIGS
    ],
)
@pytest.mark.parametrize("scenario", list(SCENARIOS))
def test_side_effect_stream_tables(
    scenario: str, input_order: str, config_name: str
) -> None:
    """A side-effect stream's table under each configuration: aligned, every row once."""
    sources = make_sources(scenario, input_order)
    stream = side_effect_stream(sources)
    config = COLUMN_CONFIGS[config_name]
    tags, data = stream.keys()
    layout = Layout(
        tag_keys=tuple(tags),
        data_keys=tuple(data),
        content_hash_column=content_hash_column(config),
    )
    table = stream.as_table(**config)
    assert_requested_columns(table, config, layout)
    assert_rows_aligned(
        table,
        layout,
        sources.oracle,
        stream.data_context,
        expected_rows=rows(sources.oracle, "A"),
    )


# ---------------------------------------------------------------------------
# Previews before a full read
# ---------------------------------------------------------------------------

HEAD_SIZES = (0, 2, 4, 100)


def sorts(config: dict[str, Any]) -> bool:
    """Whether ``config`` asks for tag order (``sort_by_tags``, or ``all_info``)."""
    return bool(config.get("all_info") or config.get("columns", {}).get("sort_by_tags"))


def tag_order(origin: Origin) -> tuple[Any, ...]:
    """The tag-order key ``as_table`` sorts by: each tag ascending, nulls last."""
    return tuple(
        (1, 0) if origin.values[k] is None else (0, origin.values[k])
        for k in TAG_COLUMNS
    )


def expected_head(
    origins: list[Origin], available: int, n: int, *, sort: bool
) -> list[Origin]:
    """The rows a head of ``n`` shows (ITL-1053).

    ``origins`` is the stream's rows in input order and ``available`` how many of them
    the stream holds (a source all of them; a pod those computed so far). The head shows
    the first ``n`` of the rows it holds, in stream order or, when ``sort``, in tag order;
    rows with equal tags may come in either order.
    """
    held = origins[:available]
    return (sorted(held, key=tag_order) if sort else held)[:n]


def assert_head_rows(
    table: pa.Table, want: list[Origin], held: list[Origin], *, sort: bool
) -> None:
    """Assert ``table`` holds the rows of ``want``, up to the order of tag ties.

    Unsorted, the rows are exactly ``want``. Sorted, the tags must be those of ``want``
    in order, and each row one of the ``held`` rows, at most once: a tie at the cut may
    be filled by any of its rows. A unique value names each row: ``x`` in a source's
    table, ``y = scale(x)`` in a pod's.
    """
    column = "x" if "x" in table.column_names else "y"

    def name(origin: Origin) -> int:
        """The value of ``column`` the row of ``origin`` holds."""
        x = origin.values["x"]
        return x if column == "x" else scale(x)

    tags = list(zip(*(table.column(k).to_pylist() for k in TAG_COLUMNS), strict=True))
    names = table.column(column).to_pylist()
    if not sort:
        assert names == [name(o) for o in want], f"rows {column}={names}"
        return
    assert tags == [tuple(o.values[k] for k in TAG_COLUMNS) for o in want], (
        f"tags {tags} are not the first in tag order"
    )
    assert len(set(names)) == len(names), f"a row repeated: {column}={names}"
    by_name = {name(o): o for o in held}
    for value, row_tags in zip(names, tags, strict=True):
        assert value in by_name, f"row {column}={value} is not one the stream holds"
        assert tuple(by_name[value].values[k] for k in TAG_COLUMNS) == row_tags, (
            f"row {column}={value} has another row's tags"
        )


PREVIEW_STATES = ("fresh_source", "fresh_pod", "partial_pod", "complete_pod")


@pytest.mark.parametrize(
    "state",
    [
        pytest.param(
            state,
            id=state,
        )
        for state in PREVIEW_STATES
    ],
)
@pytest.mark.parametrize("input_order", INPUT_ORDERS)
@pytest.mark.parametrize("scenario", list(SCENARIOS))
def test_a_preview_before_a_full_read_shows_the_first_rows_and_computes_nothing(
    scenario: str, input_order: str, state: str
) -> None:
    """``head(n)`` of a fresh source, a fresh, partly or fully computed pod stream."""
    sources = make_sources(scenario, input_order)
    available = len(keys_a(scenario, input_order))
    if state == "fresh_source":
        stream: Any = sources.a
    else:
        derive_y(sources)
        stream = new_gated_scale_pod()(sources.a)
    GATE.disarm()
    # The (tag, data) pairs that prepared the state: the reference the preview's function
    # tokens must match, by record id (a partial stream is not read further for it).
    prepared: list[Any] = []
    if state == "fresh_pod":
        available = 0
    elif state == "partial_pod":
        partial = iter(stream.iter_data())
        prepared = [next(partial), next(partial), next(partial)]
        del partial
        available = 3
    elif state == "complete_pod":
        stream.as_table()
        prepared = list(stream.iter_data())
    tokens = pairs_function_tokens(prepared) if prepared else None
    computed_before = len(GATE.completed)

    origins = [o for o in sources.oracle.origins if o.source_id == "A"]
    tags, data = stream.keys()
    failures = []
    for name, config in COLUMN_CONFIGS.items():
        layout = Layout(
            tag_keys=tuple(tags),
            data_keys=tuple(data),
            content_hash_column=content_hash_column(config),
        )
        for n in HEAD_SIZES:
            table = stream.head(n, **config).as_table()
            want = expected_head(origins, available, n, sort=sorts(config))
            try:
                assert table.num_rows == len(want), f"{table.num_rows} rows"
                if not want:
                    # An empty preview still has the requested columns, and (where
                    # the stream holds every row) the types of the rows.
                    assert_requested_columns(
                        table, config, layout, require_sorted=False
                    )
                    if state in ("fresh_source", "complete_pod"):
                        full = stream.as_table(**config)
                        assert table.schema.equals(full.schema), (
                            f"empty preview schema {table.schema} differs from {full.schema}"
                        )
                    continue
                if state in ("fresh_source", "complete_pod"):
                    # Every row is held: as_table computes nothing more.
                    first = stream.as_table(**config).slice(0, n)
                    assert table.equals(first), "head is not the first rows of as_table"
                assert_requested_columns(table, config, layout)
                assert_head_rows(table, want, origins[:available], sort=sorts(config))
                assert_rows_aligned(
                    table,
                    layout,
                    sources.oracle,
                    stream.data_context,
                    function_tokens=tokens,
                    expected_rows=[(member_of(o),) for o in want]
                    if not sorts(config)
                    else None,
                )
            except AssertionError as exc:
                failures.append(f"head({n}) [{name}] {exc}")
    assert len(GATE.completed) == computed_before, "a preview computed rows"
    assert not failures, "\n".join(failures)
