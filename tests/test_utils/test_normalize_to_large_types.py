"""``normalize_to_large_types`` changes layouts only (ITL-832, ITL-842).

Normalizing to large variants must keep every nested field's nullability and
metadata, so that it never changes a type's Arrow schema hash or value hash.
Before this was fixed, a list's value field was rebuilt as
``large_list(element_type)``, which dropped a non-null item flag and changed
the schema hash of such columns.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.hashing.arrow_hashers import StarfixArrowHasher
from orcapod.utils.arrow_utils import (
    normalize_schema_to_large_types,
    normalize_to_large_types,
)

META = {b"note": b"kept"}


def _starfix_hasher() -> StarfixArrowHasher:
    """Return the default context's Arrow hasher, which hashes schemas too."""
    hasher = get_default_context().arrow_hasher
    assert isinstance(hasher, StarfixArrowHasher)
    return hasher


def _strict(name: str, arrow_type: pa.DataType) -> pa.Field[Any]:
    """Return a non-nullable field carrying ``META``."""
    return pa.field(name, arrow_type, nullable=False, metadata=META)


def _map(key: pa.Field[Any], item: pa.Field[Any]) -> pa.DataType:
    """Build a map type from key and item fields, keeping their flags."""
    # pyarrow accepts Fields here; its stubs only list DataTypes.
    return pa.map_(key, item)  # pyright: ignore[reportArgumentType]


# (input type, values, expected normalized type)
CASES: list[tuple[pa.DataType, list[Any], pa.DataType]] = [
    (pa.string(), ["a"], pa.large_string()),
    (pa.binary(), [b"a"], pa.large_binary()),
    (pa.list_(pa.int64()), [[1]], pa.large_list(pa.int64())),
    (
        pa.list_(_strict("item", pa.int64())),
        [[1]],
        pa.large_list(_strict("item", pa.int64())),
    ),
    (
        pa.large_list(_strict("item", pa.string())),
        [["a"]],
        pa.large_list(_strict("item", pa.large_string())),
    ),
    (
        pa.list_(_strict("item", pa.list_(_strict("item", pa.string())))),
        [[["a"]]],
        pa.large_list(
            _strict("item", pa.large_list(_strict("item", pa.large_string())))
        ),
    ),
    (
        pa.list_(_strict("item", pa.string()), 2),
        [["a", "b"]],
        pa.list_(_strict("item", pa.large_string()), 2),
    ),
    (
        pa.struct(
            [
                _strict("a", pa.string()),
                pa.field("b", pa.list_(_strict("item", pa.int8()))),
            ]
        ),
        [{"a": "x", "b": [1]}],
        pa.struct(
            [
                _strict("a", pa.large_string()),
                pa.field("b", pa.large_list(_strict("item", pa.int8()))),
            ]
        ),
    ),
    (
        _map(
            _strict("key", pa.string()),
            _strict("value", pa.list_(_strict("item", pa.int64()))),
        ),
        [[("a", [1])]],
        _map(
            _strict("key", pa.large_string()),
            _strict("value", pa.large_list(_strict("item", pa.int64()))),
        ),
    ),
]


def _nested_fields(arrow_type: pa.DataType) -> list[pa.Field[Any]]:
    """Return every nested field of ``arrow_type``, depth first."""
    if isinstance(arrow_type, pa.MapType):
        children = [arrow_type.key_field, arrow_type.item_field]
    elif isinstance(arrow_type, (pa.ListType, pa.LargeListType, pa.FixedSizeListType)):
        children = [arrow_type.value_field]
    elif isinstance(arrow_type, pa.StructType):
        children = list(arrow_type)
    else:
        children = []
    return [
        nested for child in children for nested in [child, *_nested_fields(child.type)]
    ]


@pytest.mark.parametrize(("arrow_type", "values", "expected"), CASES, ids=str)
class TestNormalizeKeepsFields:
    """Every nested field keeps its nullability and metadata."""

    def test_type(
        self, arrow_type: pa.DataType, values: list[Any], expected: pa.DataType
    ) -> None:
        """The normalized type is the large variant with the same fields."""
        del values
        assert normalize_to_large_types(arrow_type) == expected

    def test_nullability_and_metadata(
        self, arrow_type: pa.DataType, values: list[Any], expected: pa.DataType
    ) -> None:
        """Nullability and metadata survive at every depth.

        ``DataType`` equality ignores nested field metadata, so compare the
        nested fields directly.
        """
        del values, expected
        before = _nested_fields(arrow_type)
        after = _nested_fields(normalize_to_large_types(arrow_type))
        assert [(f.name, f.nullable, f.metadata) for f in after] == [
            (f.name, f.nullable, f.metadata) for f in before
        ]

    def test_hashes_unchanged(
        self, arrow_type: pa.DataType, values: list[Any], expected: pa.DataType
    ) -> None:
        """Ask the hasher: schema hash and value hash are the same after normalizing.

        The hasher has no value hashing for maps or fixed-size lists, so only
        their schema hash is compared.
        """
        del expected
        hasher = _starfix_hasher()
        large = normalize_to_large_types(arrow_type)
        assert hasher.hash_schema(
            pa.schema([pa.field("x", arrow_type)])
        ) == hasher.hash_schema(pa.schema([pa.field("x", large)]))
        if not (pa.types.is_map(arrow_type) or pa.types.is_fixed_size_list(arrow_type)):
            assert hasher.hash_table(
                pa.table({"x": pa.array(values, type=arrow_type)})
            ) == hasher.hash_table(pa.table({"x": pa.array(values, type=large)}))


def test_schema_normalization_keeps_top_level_fields() -> None:
    """``normalize_schema_to_large_types`` keeps top-level nullability and metadata."""
    schema = pa.schema([_strict("x", pa.list_(_strict("item", pa.string())))])
    normalized = normalize_schema_to_large_types(schema)
    field = normalized.field("x")
    assert field.type == pa.large_list(_strict("item", pa.large_string()))
    assert not field.nullable
    assert field.metadata == META
    assert not field.type.value_field.nullable
    assert field.type.value_field.metadata == META


def test_width_is_not_a_layout() -> None:
    """``int8`` and ``int64`` differ in both hashes; normalization keeps ``int8``."""
    hasher = _starfix_hasher()
    assert normalize_to_large_types(pa.int8()) == pa.int8()
    assert hasher.hash_schema(pa.schema([pa.field("x", pa.int8())])) != (
        hasher.hash_schema(pa.schema([pa.field("x", pa.int64())]))
    )
    assert hasher.hash_table(pa.table({"x": pa.array([1], pa.int8())})) != (
        hasher.hash_table(pa.table({"x": pa.array([1], pa.int64())}))
    )


def test_keep_null_leaves_null_fields_alone() -> None:
    """With ``keep_null=True`` only layouts change; ``null`` stays ``null``.

    The default (``null`` to ``large_string``) is what ``Data`` relies on and
    is unchanged.
    """
    nested = pa.struct([("a", pa.null()), ("b", pa.list_(pa.null()))])
    assert normalize_to_large_types(nested, keep_null=True) == pa.struct(
        [("a", pa.null()), ("b", pa.large_list(pa.null()))]
    )
    assert normalize_to_large_types(pa.null(), keep_null=True) == pa.null()
    assert normalize_to_large_types(pa.null()) == pa.large_string()
