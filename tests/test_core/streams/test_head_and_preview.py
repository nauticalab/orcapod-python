"""``head()`` and the notebook preview show what is readily available and never compute (ITL-991).

A stream's ``head(n)`` returns at most ``n`` rows it already holds, has cached from an earlier
computation, or can read from its table, and the preview (``_repr_html_``) renders exactly
that. Neither calls a function, runs an operator, delivers a side effect, polls a source or
writes a cache.
"""

from __future__ import annotations

import io
from typing import Any
from unittest import mock

import numpy as np
import numpy.typing as npt
import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.core.function_pod import FunctionPodStream
from orcapod.core.operators import GroupBy, Join, PolarsFilter, SelectDataColumns
from orcapod.core.sources import (
    ArrowTableSource,
    CachedSource,
    DerivedSource,
    DictSource,
    ListSource,
)
from orcapod.core.sources.polling_source import PollingSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.streams import ViewStatus, StreamView
from orcapod.side_effects import SideEffectPod
from orcapod.types import ColumnConfig, Cursor, Schema

ALL_COLUMNS = ColumnConfig(
    meta=True, context=True, source=True, system_tags=True, content_hash=True
)


def _table(rows: int, offset: int = 0) -> pa.Table:
    """``id`` and ``x``, both non-nullable ``int64``."""
    return pa.table(
        {
            "id": list(range(offset, offset + rows)),
            "x": [10 * i for i in range(offset, offset + rows)],
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )


def _source(rows: int = 25, label: str = "readings") -> ArrowTableSource:
    """An ``ArrowTableSource`` over ``_table(rows)``, labelled ``label``."""
    return ArrowTableSource(
        _table(rows), tag_columns=["id"], source_id="readings", label=label
    )


# ---------------------------------------------------------------------------
# Sources and in-memory streams
# ---------------------------------------------------------------------------


def test_source_head_is_the_first_rows_with_the_exact_total() -> None:
    """A source's head is the first rows of its table, with the exact row count."""
    source = _source()
    head = source.head()
    assert isinstance(head, StreamView)
    assert head.status is ViewStatus.ROWS
    assert len(head) == 10
    assert head.total_rows == 25
    assert head.table.equals(source.as_table().slice(0, 10))
    assert head.header() == "ArrowTableSource[readings] — first 10 of 25 rows"


def test_head_of_a_short_source_says_all_rows() -> None:
    """A source no longer than n says it shows all of its rows."""
    head = _source(rows=3).head()
    assert len(head) == 3
    assert head.header() == "ArrowTableSource[readings] — all 3 rows"


def test_head_with_every_column_group_is_the_prefix_of_as_table() -> None:
    """With every column group, head is the prefix of as_table (ITL-1053)."""
    source = _source()
    head = source.head(4, columns=ALL_COLUMNS)
    assert head.table.equals(source.as_table(columns=ALL_COLUMNS).slice(0, 4))
    # all_info sorts by tag, as it does for as_table.
    assert source.head(4, all_info=True).table.equals(
        source.as_table(all_info=True).slice(0, 4)
    )


def test_head_refuses_a_negative_n_and_accepts_sorting() -> None:
    """A negative n is refused; sort_by_tags is a column option, as for as_table."""
    source = _source()
    with pytest.raises(ValueError, match="n"):
        source.head(-1)
    head = source.head(3, columns={"sort_by_tags": True})
    assert head.table.equals(
        source.as_table(columns={"sort_by_tags": True}).slice(0, 3)
    )


def test_head_of_zero_rows_keeps_the_columns() -> None:
    """head(0) has no rows and keeps the columns."""
    head = _source().head(0)
    assert head.status is ViewStatus.ROWS
    assert head.table.column_names == ["id", "x"]
    assert head.table.num_rows == 0


@pytest.mark.parametrize(
    "make",
    [
        lambda: DictSource(
            [{"id": i, "x": i} for i in range(12)], tag_columns=["id"], label="d"
        ),
        lambda: ListSource(
            name="v", data=list(range(12)), tag_function=lambda e, i: {"i": i}
        ),
    ],
    ids=["dict", "list"],
)
def test_other_in_memory_sources_serve_their_first_rows(make: Any) -> None:
    """Dict and list sources serve the first rows they hold."""
    source = make()
    head = source.head(5)
    assert head.status is ViewStatus.ROWS
    assert head.total_rows == 12
    assert head.table.equals(source.as_table().slice(0, 5))


def test_preview_is_the_rendered_head() -> None:
    """The notebook preview is head() rendered, with the rows in tag order (ITL-1053)."""
    source = _source()
    html = source._repr_html_()
    assert "ArrowTableSource[readings] — first 10 of 25 rows, sorted by tags" in html
    assert (
        html.split("</strong>")[1:]
        == (
            source.head(columns={"sort_by_tags": True})
            ._repr_html_()
            .split("</strong>")[1:]
        )
    )


# ---------------------------------------------------------------------------
# Function pods
# ---------------------------------------------------------------------------


def _counting_pod(calls: list[int]) -> Any:
    """A function pod tripling ``x`` that appends each input to ``calls``."""

    @function_pod(output_keys="y")
    def triple(x: int) -> int:
        """Triple ``x``, recording the call."""
        calls.append(x)
        return 3 * x

    return triple


def test_function_pod_stream_is_not_computed_by_head_or_preview() -> None:
    """Neither head nor the preview calls the function of an uncomputed stream."""
    calls: list[int] = []
    stream = _counting_pod(calls).pod(_source(), label="triple")
    head = stream.head()
    html = stream._repr_html_()
    assert calls == []
    assert head.status is ViewStatus.NOT_COMPUTED
    assert len(head) == 0
    assert head.table.column_names == ["id", "y"]
    assert "not computed" in html
    assert "FunctionPodStream[triple]" in html


def test_function_pod_stream_shows_rows_computed_so_far() -> None:
    """After an incomplete iteration, head shows the rows computed so far."""
    calls: list[int] = []
    stream = _counting_pod(calls).pod(_source())
    rows = stream.iter_data()
    for _ in range(3):
        next(rows)
    head = stream.head()
    assert calls == [0, 10, 20]
    assert head.status is ViewStatus.PARTIAL
    assert head.table.column("y").to_pylist() == [0, 30, 60]
    assert "computed so far" in head.header()


def test_function_pod_stream_shows_its_computed_rows() -> None:
    """After as_table, head shows the computed rows without calling the function."""
    calls: list[int] = []
    stream = _counting_pod(calls).pod(_source())
    table = stream.as_table()
    assert len(calls) == 25
    head = stream.head(7)
    assert len(calls) == 25
    assert head.status is ViewStatus.ROWS
    assert head.total_rows == 25
    assert head.table.equals(table.slice(0, 7))
    assert head.header().endswith("first 7 of 25 computed rows")


def test_function_pod_stream_rows_from_iteration_only() -> None:
    """After a full iteration, head is the prefix of as_table with every column group."""
    calls: list[int] = []
    stream = _counting_pod(calls).pod(_source())
    _ = list(stream.iter_data())
    head = stream.head(4, columns=ALL_COLUMNS)
    assert len(calls) == 25
    assert head.status is ViewStatus.ROWS
    assert head.table.equals(stream.as_table(columns=ALL_COLUMNS).slice(0, 4))
    assert len(calls) == 25


# ---------------------------------------------------------------------------
# Operators
# ---------------------------------------------------------------------------


def _other_source() -> ArrowTableSource:
    """A source sharing ``id`` with ``_source``, with a ``z`` column for joins."""
    table = pa.table(
        {"id": list(range(25)), "z": [i * i for i in range(25)]},
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("z", pa.int64(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["id"], source_id="other")


def _grouped_source() -> ArrowTableSource:
    """A source with an ``id`` and a ``grp`` tag, for grouping."""
    table = pa.table(
        {
            "id": list(range(25)),
            "grp": [f"g{i % 3}" for i in range(25)],
            "x": list(range(25)),
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("grp", pa.large_string(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["id", "grp"], source_id="grouped")


def _calls_over_rows(spy: mock.MagicMock) -> int:
    """How many calls of the spied ``static_process`` were given rows.

    A call over inputs with no rows gives only the operator's zero-row table, which a
    head with no rows shows without computing a row (ITL-912).
    """
    return sum(
        any(stream.as_table().num_rows for stream in call.args[1:])
        for call in spy.call_args_list
    )


OPERATOR_CASES = {
    "join": (Join, lambda: Join()(_source(), _other_source())),
    "group_by": (GroupBy, lambda: GroupBy(by=["grp"])(_grouped_source())),
    "filter": (
        PolarsFilter,
        lambda: PolarsFilter(constraints={"x": 20})(_source()),
    ),
}


@pytest.mark.parametrize("case", list(OPERATOR_CASES), ids=list(OPERATOR_CASES))
def test_operator_stream_is_not_run_by_head_or_preview(case: str) -> None:
    """Neither head nor the preview runs the operator; its kept result is shown."""
    operator_class, build = OPERATOR_CASES[case]
    stream = build()
    original = operator_class.static_process
    # A spy on the non-public static_process: the test is about whether it runs.
    with mock.patch.object(
        operator_class, "static_process", autospec=True, side_effect=original
    ) as spy:
        head = stream.head()
        _ = stream._repr_html_()
        assert _calls_over_rows(spy) == 0
        assert head.status is ViewStatus.NOT_COMPUTED
        assert len(head) == 0
        table = stream.as_table()
        assert _calls_over_rows(spy) == 1
        after = stream.head(2)
        assert _calls_over_rows(spy) == 1
    assert after.status is ViewStatus.ROWS
    assert after.table.equals(table.slice(0, 2))


def test_an_empty_operator_result_is_not_not_computed() -> None:
    """A computed empty result is ROWS with no rows, not NOT_COMPUTED."""
    stream = PolarsFilter(constraints={"x": -1})(_source())
    _ = stream.as_table()
    head = stream.head()
    assert head.status is ViewStatus.ROWS
    assert len(head) == 0
    assert head.header().endswith("computed: no rows")


def test_operator_stream_with_changed_inputs_is_not_computed() -> None:
    """A kept result whose input changed is not shown."""
    calls: list[int] = []
    fn_stream = _counting_pod(calls).pod(_source())
    assert isinstance(fn_stream, FunctionPodStream)
    out = SelectDataColumns(["y"])(fn_stream)
    _ = out.as_table()
    fn_stream.clear_cache()
    head = out.head()
    assert head.status is ViewStatus.NOT_COMPUTED
    assert "inputs changed" in head.header()


# ---------------------------------------------------------------------------
# Side effects, polling and caching sources
# ---------------------------------------------------------------------------


def test_side_effect_stream_preview_never_delivers() -> None:
    """Showing a side-effect stream delivers nothing."""
    delivered: list[int] = []

    def deliver(x: int) -> None:
        """The side effect; recording or refusing a delivery."""
        delivered.append(x)

    stream = SideEffectPod(deliver)(_source())
    head = stream.head()
    html = stream._repr_html_()
    assert delivered == []
    assert head.status is ViewStatus.NOT_COMPUTED
    assert "displaying never delivers side effects" in html


class _FakeDynamicSource:
    """A ``DynamicSourceProtocol`` double that records every poll and fetch."""

    def __init__(self, declared: Schema | None = None) -> None:
        """Set up the double."""
        self.polls = 0
        self.fetches = 0
        self.declared = declared

    def identity(self) -> Any:
        """Return the double's identity."""
        return "fake"

    def to_config(self) -> dict[str, Any] | None:
        """Return no config: the double is not serializable."""
        return None

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> _FakeDynamicSource:
        """Refuse: the double is not serializable."""
        raise NotImplementedError

    def schema(self) -> Any:
        """Return the declared schema, if any."""
        return self.declared

    async def poll(self, cursor: Cursor[int] | None = None) -> bool:
        """Count the poll and say whether new data is there."""
        self.polls += 1
        return False

    async def fetch(self, cursor: Cursor[int] | None = None) -> tuple[Cursor[int], Any]:
        """Count the fetch and return the next batch."""
        self.fetches += 1
        return Cursor(value=1), {
            "id": pa.array([1], type=pa.int64()),
            "val": pa.array([7], type=pa.int64()),
        }

    async def close(self) -> None:
        """Release nothing."""
        pass


def test_polling_source_head_never_polls() -> None:
    """A polling source's head shows polled rows only and never polls."""
    fake = _FakeDynamicSource()
    source = PollingSource(fake, tag_columns="id")
    head = source.head()
    _ = source._repr_html_()
    assert (fake.polls, fake.fetches) == (0, 0)
    assert head.status is ViewStatus.NOT_COMPUTED
    assert "nothing polled yet" in head.header()
    _ = list(source.iter_data())
    seen = (fake.polls, fake.fetches)
    after = source.head()
    assert (fake.polls, fake.fetches) == seen
    assert after.status is ViewStatus.ROWS
    assert after.table.column("val").to_pylist() == [7]


def test_polling_source_head_shows_declared_system_tags_without_polling() -> None:
    """A declared schema answers head's columns, system tags included, without a poll."""
    fake = _FakeDynamicSource(declared=Schema({"id": int, "val": int}))
    source = PollingSource(fake, tag_columns="id")
    head = source.head(columns={"system_tags": True})
    assert (fake.polls, fake.fetches) == (0, 0)
    tag_schema, data_schema = source.output_schema(columns={"system_tags": True})
    assert head.table.column_names == [*tag_schema, *data_schema]
    assert any(name.startswith("_tag") for name in head.table.column_names)


def test_derived_source_over_a_polling_source_never_polls() -> None:
    """A derived source's head asks its origin for its head, so nothing is polled."""
    fake = _FakeDynamicSource()
    origin = PollingSource(fake, tag_columns="id")
    # Building the derived source reads the origin's columns, which polls it once.
    derived = DerivedSource(origin, tags_as_data={"id": "id_value"})
    seen = (fake.polls, fake.fetches)
    head = derived.head()
    _ = derived._repr_html_()
    assert (fake.polls, fake.fetches) == seen
    assert head.status is ViewStatus.ROWS
    assert head.table.column("id_value").to_pylist() == [1]


def test_downstream_of_an_unpolled_source_never_polls() -> None:
    """A stream over a source whose columns are known only by polling shows none."""
    fake = _FakeDynamicSource()
    source = PollingSource(fake, tag_columns="id")

    def deliver(val: int) -> None:
        """The side effect; recording or refusing a delivery."""
        raise AssertionError("delivered")

    stream = SideEffectPod(deliver)(source)
    head = stream.head()
    _ = stream._repr_html_()
    assert (fake.polls, fake.fetches) == (0, 0)
    assert head.status is ViewStatus.NOT_COMPUTED
    assert head.table.num_columns == 0
    assert "columns not known until its source is polled" in head.header()


@pytest.mark.parametrize(
    ("columns", "all_info"),
    [
        (ColumnConfig(source=True), False),
        (ColumnConfig(meta=True), False),
        (ColumnConfig(context=True), False),
        (ColumnConfig(content_hash=True), False),
        (None, True),
    ],
    ids=["source", "meta", "context", "content_hash", "all_info"],
)
@pytest.mark.parametrize("kind", ["function_pod", "side_effect"])
def test_downstream_head_never_polls_for_data_columns(
    kind: str, columns: ColumnConfig | None, all_info: bool
) -> None:
    """Asking for data-dependent columns downstream of an unpolled source never polls.

    A declared schema gives the user columns, but source info, meta, context and
    content hashes are known only from polled rows: the head then shows no columns.
    """
    fake = _FakeDynamicSource(declared=Schema({"id": int, "val": int}))
    source = PollingSource(fake, tag_columns="id")

    def deliver(val: int) -> None:
        """The side effect; refusing a delivery."""
        raise AssertionError(f"delivered {val}")

    @function_pod(output_keys="out")
    def copy(val: int) -> int:
        """Return ``val``; refusing a call."""
        raise AssertionError(f"called with {val}")

    stream = (
        copy.pod(source) if kind == "function_pod" else SideEffectPod(deliver)(source)
    )
    head = stream.head(columns=columns, all_info=all_info)
    assert (fake.polls, fake.fetches) == (0, 0)
    assert head.status is ViewStatus.NOT_COMPUTED
    assert head.table.num_columns == 0
    assert "columns not known until its source is polled" in head.header()


def test_cached_source_head_writes_nothing() -> None:
    """A cached source's head writes nothing to its cache store."""
    store = InMemoryArrowDatabase()
    source = CachedSource(_source(), cache_database=store)
    head = source.head(3)
    _ = source._repr_html_()
    assert store.list_table_paths() == []
    # Before first use its rows (earlier cached rows merged with the wrapped source's)
    # are not known: the columns are shown, with no rows.
    assert head.status is ViewStatus.NOT_COMPUTED
    assert head.table.column_names == ["id", "x"]
    table = source.as_table()
    after = source.head(3)
    assert after.status is ViewStatus.ROWS
    assert after.table.equals(table.slice(0, 3))


# ---------------------------------------------------------------------------
# Rendering of large cells
# ---------------------------------------------------------------------------


def test_preview_summarises_large_cells_and_head_keeps_them() -> None:
    """The preview summarises large cells; head's table keeps the full values."""
    source = DictSource(
        [
            {
                "id": 1,
                "arr": np.arange(6.0).reshape(2, 3),
                "blob": b"xyz" * 100,
                "text": "a" * 200,
                "items": list(range(12)),
            }
        ],
        tag_columns=["id"],
        label="cells",
    )
    head = source.head()
    html = head._repr_html_()
    assert "ndarray float64 (2, 3)" in html
    assert "bytes (300 B)" in html
    assert "a" * 200 not in html
    assert "…" in html
    assert "list (len 12)" in html
    assert head.table.equals(source.as_table())


def test_text_form_has_the_header() -> None:
    """The text form of a head starts with its header."""
    text = str(_source().head(2))
    assert text.startswith("ArrowTableSource[readings] — first 2 of 25 rows")


def test_cached_function_pod_stream_does_not_read_its_result_store() -> None:
    """A cached pod's stream shows only computed rows and never calls the function."""
    from orcapod.core.cached_function_pod import CachedFunctionPod

    calls: list[int] = []
    store = InMemoryArrowDatabase()
    pod = CachedFunctionPod(_counting_pod(calls).pod, result_database=store)
    _ = pod.process(_source()).as_table()  # fills the result store
    assert len(calls) == 25

    stream = pod.process(_source())
    head = stream.head()
    assert head.status is ViewStatus.NOT_COMPUTED
    table = stream.as_table()  # served from the result store
    assert len(calls) == 25
    after = stream.head(5)
    assert after.status is ViewStatus.ROWS
    assert after.table.equals(table.slice(0, 5))
    assert len(calls) == 25


class _NpyArrayType(pa.ExtensionType):
    """The array logical type stored as one ``.npy`` file per cell (the ITL-981 form)."""

    def __init__(self) -> None:
        """Set up the double."""
        from orcapod.logical_types.numpy_type import NDARRAY_EXTENSION_NAME

        super().__init__(pa.large_binary(), NDARRAY_EXTENSION_NAME)

    def __arrow_ext_serialize__(self) -> bytes:
        """Return no extension metadata."""
        return b""

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> _NpyArrayType:
        """Rebuild the type."""
        return cls()


def _npy(array: npt.NDArray[Any]) -> bytes:
    """Return ``array`` as the bytes of a ``.npy`` file (format 3.0)."""
    buffer = io.BytesIO()
    np.lib.format.write_array(buffer, array, version=(3, 0))
    return buffer.getvalue()


def test_preview_reads_an_npy_cell_header_only() -> None:
    """An array cell stored as ``.npy`` shows its dtype and shape from the header."""
    storage = pa.array(
        [_npy(np.zeros(40, dtype=np.float32)), _npy(np.ones((2, 3))), None],
        type=pa.large_binary(),
    )
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "frame": pa.ExtensionArray.from_storage(_NpyArrayType(), storage),
            "jpeg": pa.array([b"\xff\xd8\xff\xe0" * 10, b"", None], pa.large_binary()),
        }
    )
    head = StreamView(
        table=table,
        status=ViewStatus.ROWS,
        kind="ArrowTableStream",
        label="frames",
        tag_columns=("id",),
        total_rows=3,
    )
    html = head._repr_html_()
    assert "ndarray float32 (40,)" in html
    assert "ndarray float64 (2, 3)" in html
    assert "bytes (40 B)" in html
    assert "NUMPY" not in html


def test_preview_keeps_both_ends_of_a_long_key() -> None:
    """A long hash key keeps its start and its distinguishing end in the preview."""
    key = "derived:arrow_v1:" + "ab" * 61 + "XYZ"
    source = DictSource([{"key": key, "x": 1}], tag_columns=["key"], label="keys")
    html = source._repr_html_()
    assert "derived:arrow_v1:abab" in html
    assert "XYZ" in html
    assert key not in html
    assert source.head().table.column("key").to_pylist() == [key]


def test_an_unlabelled_source_is_shown_by_its_source_id() -> None:
    """A source given no label is named by its source id in the header; its label is kept."""
    source = DictSource(
        [{"id": 1, "x": 2}], tag_columns=["id"], source_id="spike_event"
    )
    assert source.head().header() == "DictSource[spike_event] — all 1 row"
    assert source.label == "DictSource"


class _BatchedDynamicSource(_FakeDynamicSource):
    """Serves three batches of two rows, one per poll that reports new data."""

    async def poll(self, cursor: Cursor[int] | None = None) -> bool:
        """Count the poll and say whether new data is there."""
        self.polls += 1
        return (cursor.value if cursor is not None else 0) < 3

    async def fetch(self, cursor: Cursor[int] | None = None) -> tuple[Cursor[int], Any]:
        """Count the fetch and return the next batch."""
        self.fetches += 1
        index = cursor.value if cursor is not None else 0
        return Cursor(value=index + 1), {
            "id": pa.array([2 * index, 2 * index + 1], type=pa.int64()),
            "val": pa.array([10 * index, 10 * index + 1], type=pa.int64()),
        }


def test_polling_source_head_over_several_batches_is_the_prefix() -> None:
    """With several polled batches, head is the prefix of all rows, with the full count."""
    fake = _BatchedDynamicSource()
    source = PollingSource(fake, tag_columns="id")
    for _ in range(3):
        _ = list(source.iter_data())
    seen = (fake.polls, fake.fetches)
    head = source.head(3)
    assert (fake.polls, fake.fetches) == seen
    assert head.total_rows == 6
    assert head.table.equals(source.as_table().slice(0, 3))


def test_derived_source_rowless_head_has_its_own_columns() -> None:
    """Before its origin polls, a derived source shows its columns, tag copies included."""
    fake = _FakeDynamicSource(declared=Schema({"id": int, "val": int}))
    origin = PollingSource(fake, tag_columns="id")
    derived = DerivedSource(origin, tags_as_data={"id": "id_value"})
    head = derived.head()
    assert (fake.polls, fake.fetches) == (0, 0)
    assert head.status is ViewStatus.NOT_COMPUTED
    assert "id_value" in head.table.column_names
    assert "nothing polled yet" in head.header()


def test_cached_source_keeps_the_wrapped_explanation_when_it_has_no_rows() -> None:
    """A cached source over an unpolled source says why there are no rows."""
    source = CachedSource(
        PollingSource(_FakeDynamicSource(), tag_columns="id"),
        cache_database=InMemoryArrowDatabase(),
    )
    head = source.head()
    assert "nothing polled yet" in head.header()


def test_head_of_adapts_any_head_the_protocol_allows() -> None:
    """A head that is not a ``StreamView`` but follows the protocol is adapted."""
    from orcapod.core.streams.view import head_of

    class _PlainHead:
        """A minimal ``StreamViewProtocol`` result."""

        table = _table(2)
        status = "rows"
        total_rows = 2

        def header(self) -> str:
            """Return a fixed header."""
            return "plain"

        def as_table(self) -> pa.Table:
            """Return the rows."""
            return self.table

        def as_polars_df(self) -> Any:
            """Return nothing: not used by the test."""
            return None

        def _repr_html_(self) -> str:
            """Return an empty preview."""
            return ""

    source = _source()
    head = head_of(_PlainHead(), source)
    assert head.status is ViewStatus.ROWS
    assert head.header() == "ArrowTableSource[readings] — all 2 rows"


def test_head_of_n_rows_renders_all_n_rows() -> None:
    """The preview of ``head(n)`` shows all ``n`` rows, not Polars' default of 10."""
    text = str(_source().head(25))
    assert "shape: (25, 2)" in text
    assert "…" not in text.split("\n", 2)[2]


def test_an_empty_computed_result_has_the_requested_columns() -> None:
    """A computed empty result has the columns ``as_table`` gives, hashes included."""
    calls: list[int] = []
    stream = _counting_pod(calls).pod(PolarsFilter(constraints={"x": -1})(_source()))
    _ = list(stream.iter_data())
    head = stream.head(columns={"content_hash": True})
    assert head.status is ViewStatus.ROWS
    assert len(head) == 0
    assert (
        head.table.column_names
        == stream.as_table(columns={"content_hash": True}).column_names
    )
