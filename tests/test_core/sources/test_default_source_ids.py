"""Default source ids built from a hash: ``<source kind>:<algorithm label>:<hex>`` (ITL-957).

Developer decision of November 11, 2026: a source id is the source's name and must make
the source easy to identify. A default id built from a hash carries the source kind (the
class's config type key) and the hash's own label. The one string is used unchanged as
the ``source_id`` system tag value and inside the ``_source_<col>`` values. A source id
given by the user is never changed.
"""

from __future__ import annotations

from collections.abc import Callable

import polars as pl
import pyarrow as pa
import pytest

from orcapod.config import HashingConfig, OrcapodConfig
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.operators import MapData
from orcapod.core.sources import (
    ArrowTableSource,
    DataFrameSource,
    DerivedSource,
    DictSource,
    ListSource,
    RootSource,
)
from orcapod.databases import InMemoryArrowDatabase
from orcapod.system_constants import constants
from orcapod.types import CacheMode

from ..conftest import double, make_int_stream

ROWS = [{"id": 1, "x": 10}, {"id": 2, "x": 20}, {"id": 3, "x": 30}]


def _arrow_table() -> pa.Table:
    return pa.table(
        {
            "id": pa.array([1, 2, 3], pa.int64()),
            "x": pa.array([10, 20, 30], pa.int64()),
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )


def _arrow_table_source(**kwargs) -> RootSource:
    return ArrowTableSource(_arrow_table(), tag_columns=["id"], **kwargs)


def _dict_source(**kwargs) -> RootSource:
    return DictSource(ROWS, tag_columns=["id"], **kwargs)


def _list_source(**kwargs) -> RootSource:
    return ListSource("x", [10, 20, 30], **kwargs)


def _data_frame_source(**kwargs) -> RootSource:
    return DataFrameSource(pl.DataFrame(ROWS), tag_columns=["id"], **kwargs)


# (source class, expected kind, factory, name of a data column)
InMemorySourceClass = (
    type[ArrowTableSource] | type[DictSource] | type[ListSource] | type[DataFrameSource]
)
IN_MEMORY_SOURCES: list[
    tuple[InMemorySourceClass, str, Callable[..., RootSource], str]
] = [
    (ArrowTableSource, "arrow_table", _arrow_table_source, "x"),
    (DictSource, "dict", _dict_source, "x"),
    (ListSource, "list", _list_source, "x"),
    (DataFrameSource, "data_frame", _data_frame_source, "x"),
]
IDS = [kind for _, kind, _, _ in IN_MEMORY_SOURCES]


def _arrow_label(source: RootSource) -> str:
    """The label the source's Arrow hasher puts on a table hash (never a literal)."""
    return source.data_context.arrow_hasher.hash_table(_arrow_table()).method


def _source_id_values(source: RootSource) -> set[str]:
    table = source.as_table(all_info=True)
    columns = [
        c
        for c in table.column_names
        if c.startswith(constants.SYSTEM_TAG_SOURCE_ID_PREFIX)
    ]
    assert columns, "the source has no source_id system tag column"
    values: set[str] = set()
    for column in columns:
        for value in table.column(column).to_pylist():
            assert isinstance(value, str)
            values.add(value)
    return values


@pytest.mark.parametrize(
    ("cls", "kind", "make", "data_column"), IN_MEMORY_SOURCES, ids=IDS
)
class TestInMemoryDefaultSourceId:
    def test_default_is_kind_label_hex(self, cls, kind, make, data_column):
        source = make()
        parts = source.source_id.split(":")
        assert len(parts) == 3, source.source_id
        assert parts[0] == kind
        assert parts[1] == _arrow_label(source)
        assert len(parts[2]) >= 64
        int(parts[2], 16)  # raises unless the last part is hex

    def test_kind_is_the_config_type_key_defined_once_on_the_class(
        self, cls, kind, make, data_column
    ):
        source = make()
        assert cls.SOURCE_KIND == kind
        assert source.to_config()["source_type"] == cls.SOURCE_KIND
        assert source.source_id.startswith(f"{cls.SOURCE_KIND}:")

    def test_same_string_in_system_tag_and_source_info(
        self, cls, kind, make, data_column
    ):
        source = make()
        assert _source_id_values(source) == {source.source_id}
        table = source.as_table(all_info=True)
        info = table.column(f"{constants.SOURCE_PREFIX}{data_column}").to_pylist()
        sep = constants.BLOCK_SEPARATOR
        assert info == [
            f"{source.source_id}{sep}row_{i}{sep}{data_column}" for i in range(3)
        ]

    def test_default_never_contains_the_block_separator(
        self, cls, kind, make, data_column
    ):
        assert constants.BLOCK_SEPARATOR not in make().source_id

    def test_default_is_deterministic(self, cls, kind, make, data_column):
        assert make().source_id == make().source_id

    @pytest.mark.parametrize("given", ["my_source", "a:b:c", "0123abcd", "arrow_table"])
    def test_given_source_id_is_never_changed(
        self, cls, kind, make, data_column, given
    ):
        source = make(source_id=given)
        assert source.source_id == given
        assert _source_id_values(source) == {given}
        table = source.as_table(all_info=True)
        info = table.column(f"{constants.SOURCE_PREFIX}{data_column}").to_pylist()
        sep = constants.BLOCK_SEPARATOR
        assert info == [f"{given}{sep}row_{i}{sep}{data_column}" for i in range(3)]

    def test_path_n_char_truncates_the_hex_only(self, cls, kind, make, data_column):
        full = make().source_id
        config = OrcapodConfig(hashing=HashingConfig(path_n_char=12))
        short = make(config=config).source_id
        kind_part, label, full_hex = full.split(":")
        assert short == f"{kind_part}:{label}:{full_hex[:12]}"


class TestArrowTableDefaultIsTheLabelledTableHash:
    def test_default_is_kind_plus_the_table_hash_string(self):
        source = _arrow_table_source()
        table_hash = source.data_context.arrow_hasher.hash_table(_arrow_table())
        assert source.source_id == f"arrow_table:{table_hash.to_string()}"


class TestKindsTellSourcesApart:
    def test_each_in_memory_class_has_its_own_kind(self):
        kinds = [cls.SOURCE_KIND for cls, _, _, _ in IN_MEMORY_SOURCES]
        assert len(set(kinds)) == len(kinds)

    def test_same_table_under_two_classes_gets_two_ids(self):
        # A DataFrameSource given an explicit schema builds the very table the
        # ArrowTableSource is given, so only the kind differs.
        arrow = _arrow_table_source()
        frame = DataFrameSource(
            pl.DataFrame(ROWS), tag_columns=["id"], schema=_arrow_table().schema
        )
        assert arrow.source_id.split(":")[1:] == frame.source_id.split(":")[1:]
        assert arrow.source_id != frame.source_id


# ---------------------------------------------------------------------------
# node.as_source() and a plain node-backed DerivedSource
# ---------------------------------------------------------------------------


def _function_node() -> FunctionJobNode:
    return FunctionJobNode(
        function_pod=FunctionPod.from_fn(double, output_keys="result"),
        input_stream=make_int_stream(n=3),
        pipeline_database=InMemoryArrowDatabase(),
    )


def _operator_node() -> OperatorJobNode:
    return OperatorJobNode(
        operator=MapData({"x": "renamed_x"}),
        input_streams=(make_int_stream(n=3),),
        pipeline_database=InMemoryArrowDatabase(),
        cache_mode=CacheMode.LOG,
    )


NODES = [_function_node, _operator_node]
NODE_IDS = ["function_node", "operator_node"]


@pytest.mark.parametrize("make_node", NODES, ids=NODE_IDS)
class TestAsSourceDefaultSourceId:
    def test_as_source_id_is_derived_plus_the_labelled_content_hash(self, make_node):
        node = make_node()
        assert (
            node.as_source().source_id == f"derived:{node.content_hash().to_string()}"
        )

    def test_kind_is_defined_once_on_the_class(self, make_node):
        node = make_node()
        assert DerivedSource.SOURCE_KIND == "derived"
        assert node.as_source().source_id.startswith(f"{DerivedSource.SOURCE_KIND}:")

    def test_as_source_id_has_three_parts_and_no_node_path(self, make_node):
        node = make_node()
        source_id = node.as_source().source_id
        assert len(source_id.split(":")) == 3
        assert "/" not in source_id
        for component in node.node_identity_path:
            assert component not in source_id

    def test_as_source_uses_the_derived_source_default(self, make_node):
        node = make_node()
        assert node.as_source().source_id == DerivedSource(node).source_id

    def test_as_source_id_is_the_same_before_and_after_the_run(self, make_node):
        node = make_node()
        before = node.as_source().source_id
        node.run()
        assert node.as_source().source_id == before

    def test_given_source_id_is_never_changed(self, make_node):
        assert DerivedSource(make_node(), source_id="my:name").source_id == "my:name"
