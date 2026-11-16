"""Batch and GroupBy build list columns through the shared helper (ITL-832).

``arrow_utils.aggregated_list_type`` is the one construction for the
``list[T]`` columns of every aggregating operator: the outer list is
``large_list`` (the variant orcapod's type converter uses for ``list[T]``),
the element is the input column's Arrow type normalized to its large variants
(width, nullability and logical types unchanged), and a logical element yields
the logical list type of that element. ``MergeJoin`` is covered in
``test_merge_join_arrow_types.py``.

The Arrow hasher treats ``list`` and ``large_list`` alike, so moving from
``list`` to ``large_list`` must leave every row's content hash unchanged.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_type_converter
from orcapod.core.operators import Batch, GroupBy
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.protocols.core_protocols import StreamProtocol

SCHEMA_CONFIGS: tuple[dict[str, bool], ...] = ({}, {"system_tags": True})
STRUCT = pa.struct([("a", pa.int8()), ("b", pa.string())])


def _source() -> ArrowTableSource:
    """Build a source with narrow, string, struct and logical (Path) data columns.

    Tag ``g`` groups rows 0-1 and 2-3; tag ``i`` is a per-row tag.
    """
    path_type = get_default_type_converter().python_type_to_arrow_type(Path)
    assert isinstance(path_type, pa.ExtensionType)
    paths = pa.array(["/a", "/b", "/c", "/d"], type=path_type.storage_type)
    columns: dict[str, tuple[pa.DataType, Any]] = {
        "g": (pa.int64(), pa.array([0, 0, 1, 1], type=pa.int64())),
        "i": (pa.int8(), pa.array([0, 1, 2, 3], type=pa.int8())),
        "n": (pa.int8(), pa.array([1, 2, 3, 4], type=pa.int8())),
        "f": (pa.float32(), pa.array([1.5, 2.5, 3.5, 4.5], type=pa.float32())),
        "s": (pa.string(), pa.array(["a", "b", "c", "d"], type=pa.string())),
        "ls": (pa.large_string(), pa.array(["a", "b", "c", "d"], pa.large_string())),
        "st": (
            STRUCT,
            pa.array([{"a": k, "b": str(k)} for k in range(4)], type=STRUCT),
        ),
        "p": (path_type, pa.ExtensionArray.from_storage(path_type, paths)),
    }
    table = pa.Table.from_arrays(
        [array for _, array in columns.values()],
        schema=pa.schema(
            [
                pa.field(name, arrow_type, nullable=False)
                for name, (arrow_type, _) in columns.items()
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["g", "i"], tag_only_columns=True)


OPERATORS = {
    "batch": (lambda: Batch(batch_size=2), ("g", "i")),
    "group_by": (lambda: GroupBy(by=["g"]), ("i",)),
}


@pytest.fixture(params=sorted(OPERATORS))
def aggregated(request: pytest.FixtureRequest) -> tuple[Any, StreamProtocol, Any]:
    """Return (operator, input source, output stream) for Batch and GroupBy."""
    factory, _ = OPERATORS[request.param]
    op = factory()
    source = _source()
    return op, source, op.unary_static_process(source)


class TestAggregatedColumnTypes:
    """List columns are ``large_list<input type>`` or a logical list."""

    @pytest.mark.parametrize(
        ("name", "element_type"),
        [
            ("n", pa.int8()),
            ("f", pa.float32()),
            ("s", pa.large_string()),
            ("ls", pa.large_string()),
            ("st", pa.struct([("a", pa.int8()), ("b", pa.large_string())])),
        ],
        ids=str,
    )
    def test_plain_column_is_large_list_of_normalized_input_type(
        self, aggregated: tuple[Any, StreamProtocol, Any], name: str, element_type: Any
    ) -> None:
        """The element is the input type in its large variants; width never changes.

        ``string`` becomes ``large_string`` (also inside a struct), while
        ``int8`` and ``float32`` keep their width.
        """
        _, _, result = aggregated
        assert result.as_table().schema.field(name).type == pa.large_list(element_type)

    def test_source_info_column_is_large_list_of_large_string(
        self, aggregated: tuple[Any, StreamProtocol, Any]
    ) -> None:
        """Source-info columns aggregate their ``large_string`` tokens."""
        _, _, result = aggregated
        schema = result.as_table(columns={"source": True}).schema
        for name in ("n", "s", "st", "p"):
            assert schema.field(f"_source_{name}").type == pa.large_list(
                pa.large_string()
            ), name

    def test_logical_column_is_logical_list(
        self, aggregated: tuple[Any, StreamProtocol, Any]
    ) -> None:
        """A ``Path`` column becomes the ``list[orcapod.path]`` logical list type.

        The inner logical type is kept in the extension name, and the values
        round-trip; the element is never stripped to a plain list of strings.
        """
        converter = get_default_type_converter()
        _, _, result = aggregated
        table = result.as_table()
        list_type = table.schema.field("p").type
        assert isinstance(list_type, pa.ExtensionType)
        assert list_type == converter.python_type_to_arrow_type(list[Path])
        assert list_type.extension_name == "list[orcapod.path]"
        assert list_type.storage_type == pa.large_list(pa.large_string())
        assert table.column("p").combine_chunks().storage.to_pylist() == [
            ["/a", "/b"],
            ["/c", "/d"],
        ]

    def test_prediction_matches_static_process(
        self, aggregated: tuple[Any, StreamProtocol, Any]
    ) -> None:
        """The predicted schema equals the schema of the computed stream."""
        op, source, result = aggregated
        for config in SCHEMA_CONFIGS:
            assert result.output_schema(columns=config) == op.output_schema(
                source, columns=config
            ), config


@pytest.mark.parametrize("name", sorted(OPERATORS))
def test_null_column_aggregates_to_large_list_of_null(name: str) -> None:
    """A ``null``-typed column aggregates to ``large_list<null>``, not ``large_string``."""
    factory, _ = OPERATORS[name]
    table = pa.table(
        {
            "g": pa.array([0, 0], pa.int64()),
            "i": pa.array([0, 1], pa.int8()),
            "z": pa.array([None, None], pa.null()),
        }
    )
    result = factory().unary_static_process(
        ArrowTableStream(table, tag_columns=["g", "i"])
    )
    assert result.as_table().schema.field("z").type == pa.large_list(pa.null())


def _row_hashes(stream: StreamProtocol) -> list[tuple[str, str]]:
    """Return each row's (tag content hash, data content hash)."""
    return [
        (tag.content_hash().to_hex(), data.content_hash().to_hex())
        for tag, data in stream.iter_data()
    ]


def _list_layout_table(stream: StreamProtocol) -> pa.Table:
    """Return ``stream``'s table with every top-level ``large_list`` column as ``list``.

    This is the layout ``Batch`` and ``GroupBy`` emitted before ITL-832
    (``pa.list_(input type)``); logical list columns were already extension
    types and are left unchanged.
    """
    table = stream.as_table(columns={"source": True, "system_tags": True})
    schema = pa.schema(
        [
            field.with_type(pa.list_(field.type.value_field))
            if pa.types.is_large_list(field.type)
            else field
            for field in table.schema
        ]
    )
    return table.cast(schema)


def _in_list_layout(stream: StreamProtocol) -> ArrowTableStream:
    """Rebuild ``stream`` from its ``list``-layout table (``_list_layout_table``).

    The stream presents ``large_list`` again (ITL-1122).
    """
    tag_columns, _ = stream.keys()
    return ArrowTableStream(_list_layout_table(stream), tag_columns=tag_columns)


class TestAggregatedContentHashes:
    """Moving from ``list`` to ``large_list`` changes no content hash."""

    def test_row_hashes_match_the_list_layout(
        self, aggregated: tuple[Any, StreamProtocol, Any]
    ) -> None:
        """Every row hashes the same in the ``large_list`` and ``list`` layouts."""
        _, _, result = aggregated
        # A stream presents large layouts only (ITL-1122): the old layout exists as a
        # table, which the Arrow hasher hashes as the new one, row by row.
        old_table = _list_layout_table(result)
        new_table = result.as_table(columns={"source": True, "system_tags": True})
        assert pa.types.is_list(old_table.schema.field("n").type)
        assert pa.types.is_large_list(new_table.schema.field("n").type)
        hasher = result.data_context.arrow_hasher
        for i in range(new_table.num_rows):
            assert hasher.hash_table(old_table.slice(i, 1)) == hasher.hash_table(
                new_table.slice(i, 1)
            )
        before = _in_list_layout(result)
        assert pa.types.is_large_list(before.as_table().schema.field("n").type)
        assert _row_hashes(result) == _row_hashes(before)

    def test_stream_content_hash_matches_the_list_layout(
        self, aggregated: tuple[Any, StreamProtocol, Any]
    ) -> None:
        """The whole output stream's content hash is layout-independent too."""
        _, _, result = aggregated
        assert (
            ArrowTableStream(
                result.as_table(columns={"source": True, "system_tags": True}),
                tag_columns=result.keys()[0],
            ).content_hash()
            == _in_list_layout(result).content_hash()
        )
