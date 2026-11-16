"""A zero-row table has the full Arrow schema of the populated one, for non-large inputs (ITL-912).

A source may hold ``string``, ``binary`` and ``list`` columns; every downstream table has
their large forms. The table with no rows must have them too: names, types, nullable
flags and order, with every column group.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.core.operators import (
    Batch,
    GroupBy,
    Join,
    MapTags,
    PolarsFilter,
    SelectDataColumns,
)
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline.job import PipelineJob

NESTED = pa.struct([pa.field("a", pa.string()), pa.field("l", pa.list_(pa.string()))])


@function_pod(output_keys="y")
def half(x: int) -> float:
    """Half of ``x``."""
    return x / 2


def _source(source_id: str = "s", data_prefix: str = "") -> ArrowTableSource:
    """Non-large tag and data columns, a non-null narrow int tag."""
    schema = pa.schema(
        [
            pa.field("k", pa.int8(), nullable=False),
            pa.field("g", pa.string()),
            pa.field("tb", pa.binary()),
            pa.field(data_prefix + "x", pa.int32(), nullable=False),
            pa.field(data_prefix + "s", pa.string()),
            pa.field(data_prefix + "b", pa.binary()),
            pa.field(data_prefix + "l", pa.list_(pa.string())),
            pa.field(data_prefix + "st", NESTED),
        ]
    )
    rows = {
        "k": [1, 2, 3],
        "g": ["a", "a", "b"],
        "tb": [b"p", b"p", b"q"],
        data_prefix + "x": [1, 2, 3],
        data_prefix + "s": ["u", "v", "w"],
        data_prefix + "b": [b"1", b"2", b"3"],
        data_prefix + "l": [["a"], ["b", "c"], []],
        data_prefix + "st": [{"a": "z", "l": ["q"]}] * 3,
    }
    return ArrowTableSource(
        pa.Table.from_pydict(rows, schema=schema),
        tag_columns=["k", "g", "tb"],
        tag_only_columns=True,
        source_id=source_id,
    )


def _build(kind: str, job: PipelineJob) -> None:
    """Add the node ``n`` of ``kind`` to the open job."""
    src = _source()
    if kind == "function":
        half.pod(SelectDataColumns(["x"])(src), label="n")
    elif kind == "group_by":
        GroupBy(["g"])(src, label="n")
    elif kind == "batch":
        Batch(batch_size=2)(src, label="n")
    elif kind == "map":
        MapTags({"g": "g2"})(src, label="n")
    elif kind == "filter":
        PolarsFilter(constraints={"x": 2})(src, label="n")
    elif kind == "join":
        Join()(src, _source("t", "o_"), label="n")
    elif kind == "group_by_function":
        grouped = GroupBy(["g"])(src)
        SelectDataColumns(["s"])(grouped, label="n")
    else:
        raise AssertionError(kind)


KINDS = ["function", "group_by", "batch", "map", "filter", "join", "group_by_function"]


def _schemas(kind: str) -> tuple[pa.Schema, pa.Schema, pa.Schema]:
    """(before run, after clear/zero-row, populated) schemas of node ``n``, all info."""
    job = PipelineJob(name="z", database=InMemoryArrowDatabase())
    with job:
        _build(kind, job)
    node = job.nodes["n"]
    before = node.head(all_info=True).table.schema
    assert node.head(all_info=True).table.num_rows == 0
    job.run()
    populated = node.as_table(all_info=True).schema
    assert node.as_table(all_info=True).num_rows > 0
    node.clear_cache()
    after = node.head(all_info=True).table.schema
    return before, after, populated


@pytest.mark.parametrize("kind", KINDS)
def test_zero_row_schema_equals_populated(kind: str) -> None:
    """Not run, and after clear_cache: the full schema equals the populated one."""
    before, after, populated = _schemas(kind)
    assert before.equals(populated, check_metadata=False), (before, populated)
    assert after.equals(populated, check_metadata=False), (after, populated)


@pytest.mark.parametrize("kind", KINDS)
def test_plain_stream_zero_row_head_equals_populated(kind: str) -> None:
    """A stream outside a job: head() of a fresh stream matches its populated table."""
    src = _source()
    stream = {
        "function": lambda: half.pod(SelectDataColumns(["x"])(src)),
        "group_by": lambda: GroupBy(["g"])(src),
        "batch": lambda: Batch(batch_size=2)(src),
        "map": lambda: MapTags({"g": "g2"})(src),
        "filter": lambda: PolarsFilter(constraints={"x": 2})(src),
        "join": lambda: Join()(src, _source("t", "o_")),
        "group_by_function": lambda: SelectDataColumns(["s"])(GroupBy(["g"])(src)),
    }[kind]()
    zero = stream.head(all_info=True).table.schema
    populated = stream.as_table(all_info=True).schema
    assert zero.equals(populated, check_metadata=False), (zero, populated)


def test_zero_row_run_equals_populated() -> None:
    """A run whose filter keeps nothing has the populated run's schema."""
    job = PipelineJob(name="z", database=InMemoryArrowDatabase())
    with job:
        PolarsFilter(constraints={"x": -1})(_source(), label="n")
    job.run()
    empty = job.nodes["n"].as_table(all_info=True).schema
    _, _, populated = _schemas("filter")
    assert empty.equals(populated, check_metadata=False), (empty, populated)


def test_no_op_filter_zero_row_equals_populated() -> None:
    """A filter with no predicate passes its input through: head matches the table."""
    stream = PolarsFilter()(_source())
    zero = stream.head(all_info=True).table.schema
    populated = stream.as_table(all_info=True).schema
    assert zero.equals(populated, check_metadata=False), (zero, populated)


def test_side_effect_stream_zero_row_equals_populated() -> None:
    """A side-effect pod's stream over non-large inputs: head matches its table."""
    from orcapod import SideEffectPod

    def deliver(ctx: object, x: int) -> None:
        return None

    stream = SideEffectPod(deliver)(SelectDataColumns(["x"])(_source()))
    zero = stream.head(all_info=True).table.schema
    populated = stream.as_table(all_info=True).schema
    assert zero.equals(populated, check_metadata=False), (zero, populated)


def test_cached_source_over_an_empty_source_equals_its_head() -> None:
    """A cache with nothing stored: its head before the read has the types of the read
    (the large form of the wrapped source's Arrow types, ITL-1122)."""
    from orcapod.core.sources import CachedSource

    empty = ArrowTableSource(
        _source().as_table().slice(0, 0),
        tag_columns=["k", "g", "tb"],
        tag_only_columns=True,
        source_id="empty",
    )
    cached = CachedSource(empty, cache_database=InMemoryArrowDatabase())
    zero = cached.head(all_info=True).table.schema
    populated = cached.as_table(all_info=True).schema
    assert zero.equals(populated, check_metadata=False), (zero, populated)
