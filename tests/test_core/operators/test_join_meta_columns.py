"""Join drops its inputs' Data meta columns on both paths (ITL-854, ITL-857).

Meta (``__``-prefixed) columns describe how one input row was produced (a cached
pod's ``__computed`` flag, the result cache's ``__pf_*`` and ``__pod_ts``
columns, a raw table's own ``__`` columns). Nothing downstream of a Join reads
or stores them, and inputs routinely share their names, so Join keeps none of
them: not on the sync path, not on the async path, for two inputs or more. Every
test drives Join through its public call and reads rows through the stream's
public methods.
"""

from __future__ import annotations

from collections.abc import Callable

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import Join
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.sources import DictSource
from orcapod.system_constants import constants
from tests.test_core.operators.test_async_sync_equivalence import (
    Row,
    cached_pod_stream,
    double_a,
    double_b,
    double_c,
    meta_stream,
    assert_equivalent,
    async_rows,
    sync_rows,
)

META = constants.META_PREFIX


def add_one_a(a: int) -> int:
    return a + 1


def add_one_b(b: int) -> int:
    return b + 1


def _dict_sources() -> tuple[DictSource, DictSource]:
    """Two sources on tag ``k`` with data columns ``a`` and ``b``."""
    return (
        DictSource(
            data=[{"k": 1, "a": 1}, {"k": 2, "a": 2}],
            tag_columns=["k"],
            source_id="sa",
            tag_only_columns=True,
        ),
        DictSource(
            data=[{"k": 1, "b": 1}, {"k": 2, "b": 2}],
            tag_columns=["k"],
            source_id="sb",
            tag_only_columns=True,
        ),
    )


def _cache_hit_streams() -> tuple[StreamProtocol, StreamProtocol]:
    """Two cached pod outputs whose rows all come from the result cache."""
    database = InMemoryArrowDatabase()
    source_a, source_b = _dict_sources()

    def cached(
        function: Callable[[int], int], key: str, source: DictSource
    ) -> StreamProtocol:
        pod = FunctionPod(
            PythonDataFunction(function, output_keys=key)  # pyright: ignore[reportArgumentType]  # PythonDataFunction is not yet typed as matching DataFunctionProtocol (baselined in other tests); it works at runtime
        )
        return CachedFunctionPod(pod, result_database=database)(source)

    def build() -> tuple[StreamProtocol, StreamProtocol]:
        return cached(add_one_a, "fa", source_a), cached(add_one_b, "fb", source_b)

    for stream in build():  # compute once, filling the cache
        _ = list(stream.iter_data())
    return build()


def _function_node_streams() -> tuple[StreamProtocol, StreamProtocol]:
    """Two function nodes of a run PipelineJob; their rows carry ``__computed``."""
    job = PipelineJob(name="join_meta", database=InMemoryArrowDatabase())
    with job:
        source_a, source_b = _dict_sources()
        _ = function_pod(output_keys="fa")(add_one_a).pod(source_a, label="fa")
        _ = function_pod(output_keys="fb")(add_one_b).pod(source_b, label="fb")
    _ = job.run()
    return job.nodes["fa"], job.nodes["fb"]


# (inputs, the meta column names the inputs' rows carry)
CASES = [
    pytest.param(
        lambda: (
            cached_pod_stream("a", double_a),
            cached_pod_stream("b", double_b),
        ),
        {f"{META}computed"},
        id="cached_pods_compute",
    ),
    # A stream served from the cache carries the same meta as a computed one (ITL-988).
    pytest.param(_cache_hit_streams, {f"{META}computed"}, id="cached_pods_cache_hit"),
    pytest.param(_function_node_streams, {f"{META}computed"}, id="function_nodes"),
    pytest.param(
        lambda: (
            meta_stream("x", "vx", [f"{META}p"]),
            meta_stream("y", "vy", [f"{META}q"]),
        ),
        {f"{META}p", f"{META}q"},
        id="raw_distinct",
    ),
    pytest.param(
        lambda: (
            meta_stream("x", "vx", [f"{META}m"]),
            meta_stream("y", "vy", [f"{META}m"]),
        ),
        {f"{META}m"},
        id="raw_colliding",
    ),
    pytest.param(
        # The input that used to crash: the raw stream already holds the name
        # the rename picked for its ``__computed`` (ITL-857).
        lambda: (
            cached_pod_stream("a", double_a),
            meta_stream("r", "v0", [f"{META}computed", f"{META}computed_1"]),
        ),
        {f"{META}computed", f"{META}computed_1"},
        id="raw_holds_rename_target",
    ),
    pytest.param(
        lambda: (
            cached_pod_stream("a", double_a),
            cached_pod_stream("b", double_b),
            cached_pod_stream("c", double_c),
        ),
        {f"{META}computed"},
        id="three_cached_pods",
    ),
    pytest.param(
        lambda: (
            cached_pod_stream("a", double_a),
            cached_pod_stream("b", double_b),
            meta_stream(
                "r", "v0", [f"{META}computed", f"{META}computed_1", f"{META}computed_2"]
            ),
        ),
        {f"{META}computed", f"{META}computed_1", f"{META}computed_2"},
        id="three_raw_holds_rename_targets",
    ),
]


def _plain_table_stream(value_column: str, computed: bool) -> ArrowTableStream:
    """A raw stream from a bare table holding ``__computed`` (no source columns).

    The stream adds a source-info column for every column, ``_source___computed``
    included, so inputs collide on that column as well.
    """
    table = pa.table(
        {
            "id": pa.array([1, 2], pa.int64()),
            value_column: pa.array([10, 20], pa.int64()),
            f"{META}computed": pa.array([computed, computed], pa.bool_()),
        }
    )
    return ArrowTableStream(table, tag_columns=["id"])


CASES.append(
    pytest.param(
        lambda: (
            _plain_table_stream("alpha", True),
            _plain_table_stream("beta", False),
            _plain_table_stream("gamma", True),
        ),
        {f"{META}computed"},
        id="three_plain_tables",
    )
)


def _input_meta(sources: tuple[StreamProtocol, ...]) -> set[str]:
    """The meta column names the inputs' rows carry."""
    return {
        name
        for source in sources
        for _, data in source.iter_data()
        for name in data.meta_columns
    }


def _is_meta(name: str) -> bool:
    """Whether ``name`` is a meta column or a meta column's source-info column."""
    return name.startswith((META, f"{constants.SOURCE_PREFIX}{META}"))


def _assert_no_meta(rows: list[Row]) -> None:
    """Assert no output row carries a meta column."""
    assert rows
    for _, data in rows:
        assert data.meta_columns == ()
        assert not [
            name for name in data.as_table(all_info=True).column_names if _is_meta(name)
        ]


@pytest.mark.parametrize(("make_sources", "input_meta"), CASES)
def test_join_drops_input_meta_on_both_paths(
    make_sources: Callable[[], tuple[StreamProtocol, ...]], input_meta: set[str]
) -> None:
    """Join's rows carry none of its inputs' meta columns, sync or async."""
    sources = make_sources()
    assert _input_meta(sources) == input_meta
    expected = sync_rows(Join(), *sources)
    actual = async_rows(Join(), *sources)
    _assert_no_meta(expected)
    _assert_no_meta(actual)
    assert_equivalent(expected, actual)


@pytest.mark.parametrize(("make_sources", "input_meta"), CASES)
def test_joined_table_has_no_meta_columns(
    make_sources: Callable[[], tuple[StreamProtocol, ...]], input_meta: set[str]
) -> None:
    """The joined stream's table and schema list no meta column."""
    del input_meta
    joined = Join()(*make_sources())
    table = joined.as_table(all_info=True)
    assert not [name for name in table.column_names if _is_meta(name)]
    tag_schema, data_schema = joined.output_schema(all_info=True)
    assert not [name for name in (*tag_schema, *data_schema) if _is_meta(name)]


@pytest.mark.parametrize(("make_sources", "input_meta"), CASES)
@pytest.mark.parametrize("columns", [{}, {"system_tags": True}])
def test_join_prediction_matches_result_without_meta(
    make_sources: Callable[[], tuple[StreamProtocol, ...]],
    input_meta: set[str],
    columns: dict[str, bool],
) -> None:
    """``output_schema`` predicts the materialized schema, meta excluded.

    A raw ``ArrowTableStream`` lists its ``__`` columns as data columns in its
    own ``output_schema``; Join's prediction must leave them out, as the
    result does.
    """
    del input_meta
    sources = make_sources()
    op = Join()
    predicted = op.output_schema(*sources, columns=columns)
    # Schema predictions are verified against static_process (CLAUDE.md).
    produced = op.static_process(*sources).output_schema(columns=columns)
    assert predicted == produced
    assert not [name for name in (*predicted[0], *predicted[1]) if _is_meta(name)]


def test_raw_inputs_keep_their_other_columns() -> None:
    """Dropping meta columns leaves every tag and data column and its type."""
    left = meta_stream("x", "vx", [f"{META}m"])
    right = meta_stream("y", "vy", [f"{META}m"])
    table = Join()(left, right).as_table()
    assert table.column_names == ["k", "vx", "vy"]
    assert table.schema.types == [pa.int16(), pa.int8(), pa.int8()]
