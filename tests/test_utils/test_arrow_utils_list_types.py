"""Tests for the list-type and type-restoration helpers in ``arrow_utils`` (ITL-832).

An aggregating operator must build a list column from the Arrow type its input
column already has, never from the type Arrow infers for the Python values.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_type_converter
from orcapod.utils.arrow_utils import (
    aggregated_list_type,
    list_array,
    restore_column_types,
)

PRIMITIVE_ELEMENTS: list[tuple[pa.DataType, list[object]]] = [
    (pa.int8(), [1, 2]),
    (pa.float32(), [1.5, 2.5]),
    (pa.large_string(), ["a", "b"]),
    (pa.large_binary(), [b"a", b"b"]),
    (pa.list_(pa.int16()), [[1], [2, 3]]),
]


class TestAggregatedListType:
    """``aggregated_list_type`` wraps the element type it is given in a ``large_list``."""

    @pytest.mark.parametrize(
        ("element_type", "expected_element"),
        [
            (pa.int8(), pa.int8()),
            (pa.float32(), pa.float32()),
            (pa.large_string(), pa.large_string()),
            (pa.string(), pa.large_string()),
            (pa.large_binary(), pa.large_binary()),
            (pa.list_(pa.int16()), pa.large_list(pa.int16())),
            (
                pa.struct([("a", pa.int8()), ("b", pa.string())]),
                pa.struct([("a", pa.int8()), ("b", pa.large_string())]),
            ),
        ],
        ids=str,
    )
    def test_plain_element_is_normalized_to_large(
        self, element_type: pa.DataType, expected_element: pa.DataType
    ) -> None:
        """A plain element becomes ``large_list<element in its large variants>``.

        ``large_list`` is the outer variant orcapod's type converter uses for
        ``list[T]``. The element is normalized to its large variants (which
        hash identically) but keeps its width: ``int8`` stays ``int8``.
        """
        converter = get_default_type_converter()
        list_type = aggregated_list_type(element_type, converter, column="c")
        assert list_type == pa.large_list(expected_element)

    def test_outer_variant_matches_the_type_converter(self) -> None:
        """The outer list variant is the one the converter gives ``list[T]``."""
        converter = get_default_type_converter()
        assert aggregated_list_type(
            pa.int64(), converter, column="c"
        ) == converter.python_type_to_arrow_type(list[int])

    def test_extension_element_uses_list_logical_type(self) -> None:
        """An extension element gets the converter's ``list[...]`` extension type.

        Arrow cannot embed an extension type in a list value field, so the outer
        list must itself be the ``ListLogicalType`` extension, which keeps the
        inner logical type in its name and metadata.
        """
        converter = get_default_type_converter()
        element_type = converter.python_type_to_arrow_type(Path)
        assert isinstance(element_type, pa.ExtensionType)
        list_type = aggregated_list_type(element_type, converter, column="c")
        assert list_type == converter.python_type_to_arrow_type(list[Path])
        assert isinstance(list_type, pa.ExtensionType)
        assert list_type.extension_name == "list[orcapod.path]"
        assert list_type.storage_type == pa.large_list(element_type.storage_type)

    def test_extension_element_without_logical_list_raises(self) -> None:
        """An extension the converter cannot list is rejected, never stripped.

        ``arrow.uuid`` is a PyArrow canonical extension orcapod does not
        register: the converter maps it to ``Any`` and returns a plain list for
        ``list[Any]``. Building a list of its storage type would silently drop
        the logical type, so this must raise, naming the column and the type.
        """
        converter = get_default_type_converter()
        with pytest.raises(ValueError, match=r"'ids'.*arrow\.uuid"):
            aggregated_list_type(pa.uuid(), converter, column="ids")


class TestListArray:
    """``list_array`` builds an array of exactly the requested list type."""

    @pytest.mark.parametrize(
        ("element_type", "values"),
        PRIMITIVE_ELEMENTS,
        ids=[str(t) for t, _ in PRIMITIVE_ELEMENTS],
    )
    @pytest.mark.parametrize("rows", [0, 1])
    def test_type_is_independent_of_values(
        self, element_type: pa.DataType, values: list[object], rows: int
    ) -> None:
        """Empty and non-empty arrays carry the same, non-inferred type."""
        list_type = pa.large_list(element_type)
        array = list_array([values] * rows, list_type)
        assert array.type == list_type
        assert len(array) == rows
        assert array.to_pylist() == [values] * rows

    @pytest.mark.parametrize("rows", [0, 1])
    def test_extension_list_type_wraps_storage(self, rows: int) -> None:
        """An extension list type is built over its storage and then wrapped."""
        converter = get_default_type_converter()
        list_type = converter.python_type_to_arrow_type(list[Path])
        assert isinstance(list_type, pa.ExtensionType)
        storage_values = [["/a", "/b"]] * rows
        array = list_array(storage_values, list_type)
        assert array.type == list_type
        assert len(array) == rows


class TestRestoreColumnTypes:
    """``restore_column_types`` undoes a layout change without touching nullability."""

    def test_restores_types_and_keeps_nullability(self) -> None:
        """Listed columns get their reference type back; flags, nulls and others stay."""
        table = pa.Table.from_arrays(
            [
                pa.array(["a", "b"], pa.large_string()),
                pa.array(["a", None], pa.large_string()),
                pa.array([[1], None], pa.large_list(pa.int8())),
                pa.array([1, 2], pa.int64()),
            ],
            schema=pa.schema(
                [
                    pa.field("s", pa.large_string(), nullable=False),
                    pa.field("n", pa.large_string(), nullable=True),
                    pa.field("l", pa.large_list(pa.int8()), nullable=True),
                    pa.field("other", pa.int64(), nullable=True),
                ]
            ),
        )
        restored = restore_column_types(
            table,
            {
                "s": pa.string(),
                "n": pa.string(),
                "l": pa.list_(pa.int8()),
                "absent": pa.int8(),
            },
        )
        assert restored.schema == pa.schema(
            [
                pa.field("s", pa.string(), nullable=False),
                pa.field("n", pa.string(), nullable=True),
                pa.field("l", pa.list_(pa.int8()), nullable=True),
                pa.field("other", pa.int64(), nullable=True),
            ]
        )
        assert restored.column("n").null_count == 1
        assert restored.column("l").null_count == 1
        assert restored.to_pylist() == table.to_pylist()
