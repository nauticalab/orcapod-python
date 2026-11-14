"""A stream's ``head()`` sorts the way ``as_table`` does, among the rows it has (ITL-1053).

``head(n, cfg)`` takes the column configuration of ``as_table``, sorting included: with
``sort_by_tags`` (or ``all_info``) the rows the stream has are sorted by tag and the first
``n`` are returned, so ``head(n, cfg) == as_table(cfg).slice(0, n)``. The default
configuration does not sort, as ``as_table()`` does not. A stream that reads or holds only
part of its rows sorts those rows. Sorting never computes or reads anything more.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.core.function_pod import FunctionPodStream
from orcapod.core.operators import PolarsFilter, SelectDataColumns
from orcapod.core.sources import ArrowTableSource, DerivedSource, DictSource
from orcapod.core.sources.polling_source import PollingSource
from orcapod.streams import ViewStatus
from orcapod.types import ColumnConfig, Cursor, Schema

ROWS = 8
# Not in tag order, with the tag order's first rows last.
ORDER = [5, 2, 7, 0, 6, 1, 4, 3]

CONFIGS: dict[str, dict[str, Any]] = {
    "default": {},
    "sort_by_tags": {"columns": {"sort_by_tags": True}},
    "sort_by_tags_config": {"columns": ColumnConfig(sort_by_tags=True)},
    "all_info": {"all_info": True},
    "meta": {"columns": {"meta": True}},
    "context": {"columns": {"context": True}},
    "source": {"columns": {"source": True}},
    "system_tags": {"columns": {"system_tags": True}},
    "content_hash": {"columns": {"content_hash": True}},
    "content_hash_sorted": {"columns": {"content_hash": True, "sort_by_tags": True}},
}

CALLS: list[int] = []


@function_pod(output_keys="y")
def triple(x: int) -> int:
    """Triple ``x``, counting every call."""
    CALLS.append(x)
    return 3 * x


@pytest.fixture(autouse=True)
def _reset_calls() -> None:
    CALLS.clear()


def _table() -> pa.Table:
    return pa.table(
        {"id": ORDER, "x": ORDER},
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )


def _source() -> ArrowTableSource:
    return ArrowTableSource(
        _table(), tag_columns=["id"], source_id="nums", label="nums"
    )


def _computed_pod_stream() -> FunctionPodStream:
    stream = triple.pod(_source())
    assert isinstance(stream, FunctionPodStream)
    stream.as_table()
    CALLS.clear()
    return stream


def _computed_operator_stream() -> Any:
    stream = SelectDataColumns(["x"])(_source())
    stream.as_table()
    return stream


def _derived_source() -> DerivedSource:
    return DerivedSource(_source(), tags_as_data={"id": "id_copy"})


STREAMS = {
    "arrow_table_source": _source,
    "computed_function_pod_stream": _computed_pod_stream,
    "computed_operator_stream": _computed_operator_stream,
    "derived_source": _derived_source,
}


@pytest.mark.parametrize("config", list(CONFIGS))
@pytest.mark.parametrize("n", [0, 3, ROWS, 50])
@pytest.mark.parametrize("kind", list(STREAMS))
def test_head_is_the_prefix_of_as_table_for_every_configuration(
    kind: str, n: int, config: str
) -> None:
    """A stream whose rows are all held: head(n, cfg) is as_table(cfg) cut to n rows."""
    stream = STREAMS[kind]()
    head = stream.head(n, **CONFIGS[config])
    assert head.status is ViewStatus.ROWS
    assert head.table.equals(stream.as_table(**CONFIGS[config]).slice(0, n))
    assert CALLS == []


@pytest.mark.parametrize("kind", list(STREAMS))
def test_the_default_head_is_not_sorted_and_the_sorted_one_is(kind: str) -> None:
    """Only sort_by_tags and all_info sort; the first n of the sorted rows are shown."""
    stream = STREAMS[kind]()
    ids = stream.head(ROWS).table.column("id").to_pylist()
    assert ids == ORDER
    by_columns = stream.head(3, columns={"sort_by_tags": True})
    by_all_info = stream.head(3, all_info=True)
    assert by_columns.table.column("id").to_pylist() == [0, 1, 2]
    assert by_all_info.table.column("id").to_pylist() == [0, 1, 2]


def test_asking_for_sorted_rows_does_not_raise() -> None:
    """``sort_by_tags`` was refused by ``head`` before; now it is a column option."""
    head = _source().head(columns={"sort_by_tags": True})
    assert head.table.column("id").to_pylist() == sorted(ORDER)


def test_a_partly_computed_stream_sorts_the_rows_computed_so_far() -> None:
    """After a partial iteration, the head is the computed rows, sorted; no more is computed."""
    stream = triple.pod(_source())
    rows = stream.iter_data()
    for _ in range(4):  # computes the first four inputs: ids 5, 2, 7, 0
        next(rows)
    assert CALLS == [5, 2, 7, 0]

    head = stream.head(3, all_info=True)
    unsorted_head = stream.head(3)

    assert CALLS == [5, 2, 7, 0]
    assert head.status is ViewStatus.PARTIAL
    # The rows computed so far in tag order, not the first 3 of the whole result in tag order.
    assert head.table.column("id").to_pylist() == [0, 2, 5]
    assert unsorted_head.table.column("id").to_pylist() == [5, 2, 7]


class _OneBatch:
    """A ``DynamicSourceProtocol`` double that gives one batch, in ``ORDER``, and counts."""

    def __init__(self) -> None:
        self.polls = 0
        self.fetches = 0

    def identity(self) -> Any:
        return "one-batch"

    def to_config(self) -> dict[str, Any] | None:
        return None

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> _OneBatch:
        raise NotImplementedError

    def schema(self) -> Any:
        return None

    async def poll(self, cursor: Cursor[int] | None = None) -> bool:
        self.polls += 1
        return cursor is None

    async def fetch(self, cursor: Cursor[int] | None = None) -> tuple[Cursor[int], Any]:
        self.fetches += 1
        return Cursor(value=1), {
            "id": pa.array(ORDER, type=pa.int64()),
            "val": pa.array(ORDER, type=pa.int64()),
        }

    async def close(self) -> None:
        return None


def test_a_polling_source_sorts_the_rows_it_has_polled_and_polls_nothing() -> None:
    """Before a poll there are no rows to sort; after one, the polled rows sort."""
    fake = _OneBatch()
    source = PollingSource(fake, tag_columns="id")
    assert source.head(3, all_info=True).status is ViewStatus.NOT_COMPUTED
    assert (fake.polls, fake.fetches) == (0, 0)

    _ = list(source.iter_data())
    seen = (fake.polls, fake.fetches)
    head = source.head(3, all_info=True)

    assert (fake.polls, fake.fetches) == seen
    assert head.table.column("id").to_pylist() == [0, 1, 2]
    assert source.head(3).table.column("id").to_pylist() == ORDER[:3]


def test_a_filter_stream_after_a_run_sorts_like_as_table() -> None:
    """An operator stream with dropped rows: head(n, cfg) is as_table(cfg) cut to n."""
    stream = PolarsFilter(constraints={"x": 5})(_source())
    stream.as_table()
    head = stream.head(5, all_info=True)
    assert head.table.equals(stream.as_table(all_info=True).slice(0, 5))


def test_a_dict_source_head_sorts() -> None:
    """Every source that wraps an ArrowTableSource sorts the same way."""
    source = DictSource(
        [{"id": i, "x": i} for i in ORDER], tag_columns=["id"], source_id="d"
    )
    assert source.head(3, all_info=True).table.equals(
        source.as_table(all_info=True).slice(0, 3)
    )


def test_an_unbound_source_proxy_head_accepts_a_sorted_configuration() -> None:
    """A proxy with no live source shows no rows, whatever the column configuration."""
    from orcapod.core.sources.source_proxy import SourceProxy

    live = _source()
    proxy = SourceProxy(
        source_id="nums",
        expected_class_name="ArrowTableSource",
        tag_schema=Schema({"id": int}),
        data_schema=Schema({"x": int}),
        content_hash_str=live.content_hash().to_string(),
        pipeline_hash_str=live.pipeline_hash().to_string(),
    )
    head = proxy.head(3, all_info=True)
    assert head.status is ViewStatus.UNAVAILABLE


# ---------------------------------------------------------------------------
# The notebook preview sorts by tags (developer, November 12, 2026)
# ---------------------------------------------------------------------------


def _preview_ids(html: str) -> list[int]:
    """The ``id`` cells of a preview, in the order they are shown."""
    import re

    return [int(v) for v in re.findall(r"<td[^>]*>(\d+)</td>", html)][::2]


def test_the_preview_of_a_source_shows_rows_in_tag_order_and_says_so() -> None:
    """Rows not in tag order are previewed sorted; the default head is still unsorted."""
    source = _source()
    html = source._repr_html_()
    assert "ArrowTableSource[nums] — all 8 rows, sorted by tags" in html
    assert _preview_ids(html) == sorted(ORDER)
    assert source.head().table.column("id").to_pylist() == ORDER


def test_the_preview_of_a_partly_computed_stream_is_the_rows_computed_so_far() -> None:
    """A partial computation previews the computed rows, in tag order; nothing more runs."""
    stream = triple.pod(_source())
    assert isinstance(stream, FunctionPodStream)
    rows = stream.iter_data()
    for _ in range(4):
        next(rows)
    html = stream._repr_html_()
    assert CALLS == [5, 2, 7, 0]
    assert "first 4 of 4 rows computed so far" not in html
    assert "4 rows computed so far, sorted by tags (computation incomplete)" in html
    assert _preview_ids(html) == [0, 2, 5, 7]


def test_the_preview_of_a_stream_with_no_computed_rows_has_no_sorting_caption() -> None:
    """Only shown rows are described as sorted."""
    stream = triple.pod(_source())
    assert isinstance(stream, FunctionPodStream)
    html = stream._repr_html_()
    assert "sorted by tags" not in html
    assert CALLS == []


def test_the_preview_of_a_stream_without_tag_columns_keeps_stream_order() -> None:
    """With no tag column there is nothing to sort by; the rows keep their order."""
    source = ArrowTableSource(
        _table().select(["x"]), tag_columns=[], source_id="untagged", label="untagged"
    )
    html = source._repr_html_()
    assert "sorted by tags" not in html
    assert source.head(columns={"sort_by_tags": True}).table.column(
        "x"
    ).to_pylist() == (ORDER)


def test_a_custom_content_hash_column_is_kept_by_a_nodes_head() -> None:
    """A custom hash column name that looks like a meta column is still shown."""
    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase

    job = PipelineJob(name="digest", database=InMemoryArrowDatabase())
    with job:
        triple.pod(_source(), label="node")
    job.run()
    node = job.nodes["node"]
    config = {"content_hash": "__digest", "sort_by_tags": True}
    assert "__digest" in node.as_table(columns=config).column_names
    head = node.head(3, columns=config)
    assert head.table.equals(node.as_table(columns=config).slice(0, 3))


def test_a_tagless_source_head_follows_as_table_with_and_without_system_tags() -> None:
    """With no regular tag column the rows keep their order unless system tags are asked.

    Asked for, they are sorted by the system tags, as ``as_table`` sorts them (the
    stream's identity depends on that); the head is that table cut to ``n`` rows.
    """
    source = ArrowTableSource(
        _table().select(["x"]), tag_columns=[], source_id="untagged", label="untagged"
    )
    plain = source.head(8, columns={"sort_by_tags": True}).table
    assert plain.column("x").to_pylist() == ORDER
    asked = source.head(8, all_info=True).table
    assert asked.equals(source.as_table(all_info=True).slice(0, 8))


def test_a_tagless_stream_with_system_tags_keeps_its_canonical_identity_order() -> None:
    """A stream with no tag column is sorted by its system tags when they are asked for.

    ``identity_structure`` hashes ``as_table(system_tags, sort_by_tags)``: that order,
    system tags only, is part of the content hash and must not change (ITL-1053).
    """
    from orcapod.core.streams import ArrowTableStream

    table = pa.table({"record_id": ["b", "c", "a"], "x": [1, 2, 3]})
    stream = ArrowTableStream(table, tag_columns=[], system_tag_columns=["record_id"])
    ordered = stream.as_table(columns={"system_tags": True, "sort_by_tags": True})
    shown = next(c for c in ordered.column_names if c.endswith("record_id"))
    assert ordered.column(shown).to_pylist() == ["a", "b", "c"]
    plain = stream.as_table(columns={"sort_by_tags": True})
    assert plain.column("x").to_pylist() == [1, 2, 3]


def test_a_sorted_head_includes_rows_cached_after_a_missing_input() -> None:
    """Sorting uses every output the stream holds, also those after an input with none."""
    stream = triple.pod(_source())
    assert isinstance(stream, FunctionPodStream)
    rows = stream.iter_data()
    for _ in range(4):
        next(rows)
    # A concurrent failure: the first input has no cached output, the others do.
    # Private state on purpose: this test is about how head reads that gap.
    del stream._cached_output_datas[0]
    unsorted_head = stream.head(10)
    sorted_head = stream.head(10, columns={"sort_by_tags": True})
    assert len(unsorted_head) == 0
    assert sorted_head.table.column("id").to_pylist() == [0, 2, 7]


def test_a_custom_hash_column_named_like_a_system_tag_is_kept() -> None:
    """A function pod stream keeps a requested hash column whatever its name starts with."""
    from orcapod.system_constants import constants

    name = f"{constants.SYSTEM_TAG_PREFIX}digest"
    stream = triple.pod(_source())
    assert isinstance(stream, FunctionPodStream)
    config = {"content_hash": name, "sort_by_tags": True}
    table = stream.as_table(columns=config)
    assert name in table.column_names
    assert stream.head(3, columns=config).table.equals(table.slice(0, 3))


@pytest.mark.parametrize("name", ["_context_key", "_source_y", "_tag_digest"])
def test_a_hash_column_named_like_another_column_group_is_kept(name: str) -> None:
    """A requested hash name equal to a column the shaping drops or keeps still works."""
    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase

    job = PipelineJob(name="names", database=InMemoryArrowDatabase())
    with job:
        pod_stream = triple.pod(_source(), label="node")
    assert isinstance(pod_stream, FunctionPodStream)
    job.run()
    for target in (pod_stream, job.nodes["node"]):
        for sort in (False, True):
            config = {"content_hash": name, "sort_by_tags": sort}
            table = target.as_table(columns=config)
            assert table.column_names.count(name) == 1
            head = target.head(3, columns=config)
            assert head.table.equals(table.slice(0, 3))


def test_the_presentation_sort_orders_a_list_tag_while_identity_keeps_stream_order() -> (
    None
):
    """``as_table(sort_by_tags)`` orders list tags (Polars); the identity table does not.

    The identity of a stream hashes its own table, in the order main always gave it
    (``ArrowTableStream._identity_table``); only presentations use the new sort.
    """
    from orcapod.core.streams import ArrowTableStream

    table = pa.table(
        [pa.array([[2], [1]], pa.large_list(pa.int64())), pa.array([20, 10])],
        names=["k", "x"],
    )
    stream = ArrowTableStream(table, tag_columns=["k"])
    assert stream.as_table(columns={"sort_by_tags": True}).column("x").to_pylist() == [
        10,
        20,
    ]
    identity = stream.identity_structure()[1]
    assert identity.column("x").to_pylist() == [20, 10]


def test_a_hash_column_named_like_a_system_tag_does_not_reorder_tied_rows() -> None:
    """On an ``ArrowTableStream`` a requested ``_tag_digest`` is not a sort key.

    Rows with equal tags keep their input order whatever the hash column is called.
    """
    from orcapod.core.streams import ArrowTableStream

    table = pa.table(
        [pa.array([1, 1, 1], pa.int64()), pa.array([3, 1, 2], pa.int64())],
        names=["k", "x"],
    )
    stream = ArrowTableStream(table, tag_columns=["k"])
    for name in (True, "_tag_digest", "digest"):
        config = {"content_hash": name, "sort_by_tags": True}
        shown = stream.as_table(columns=config)
        assert shown.column("x").to_pylist() == [3, 1, 2]
        assert shown.column_names[-1] == ("_content_hash" if name is True else name)
        assert stream.head(2, columns=config).table.equals(shown.slice(0, 2))
