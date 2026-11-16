"""Arrow hashing on starfix hash format 2 under the label ``arrow_v1`` (ITL-910).

starfix 0.5 replaced hash format 1 (prefix ``00 00 01``) with the lossless format 2 (prefix
``00 00 02``). These tests pin what that fixes in Orcapod (the collisions of ITL-848), what it
newly accepts (``fixed_size_list`` data, ITL-927; canonical extension types) and what it newly
rejects.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.core.operators import Join

# Non-public on purpose: the label constant itself is under test.
from orcapod.hashing.arrow_hashers import ARROW_HASH_LABEL
from orcapod.sources import ArrowTableSource
from orcapod.types import ContentHash

FORMAT_2_PREFIX = b"\x00\x00\x02"


def _hash_table(table: pa.Table) -> ContentHash:
    """Hash ``table`` with the default context's Arrow hasher."""
    return get_default_context().arrow_hasher.hash_table(table)


def _row_hashes(column: pa.Array[Any]) -> list[str]:
    """Content hashes of the data of a one-column source keyed by a row tag."""
    table = pa.table(
        {
            "k": pa.array([f"r{i}" for i in range(len(column))], pa.large_string()),
            "x": column,
        }
    )
    source = ArrowTableSource(table, tag_columns=["k"], tag_only_columns=True)
    return [data.content_hash().to_string() for _, data in source.iter_data()]


def _non_nullable(name: str, column: pa.Array[Any]) -> pa.Table:
    """A one-column table whose field is declared non-nullable."""
    return pa.Table.from_arrays(
        [column], schema=pa.schema([pa.field(name, column.type, nullable=False)])
    )


# ---------------------------------------------------------------------------
# Label and format
# ---------------------------------------------------------------------------


class TestLabel:
    """The Arrow hash label is ``arrow_v1`` and hashes carry the format 2 prefix."""

    def test_current_arrow_label_is_arrow_v0_2(self):
        """The label constant of the current Arrow hasher is ``arrow_v1``."""
        assert ARROW_HASH_LABEL == "arrow_v1"

    def test_default_context_uses_arrow_v0_2(self):
        """The default context's Arrow hasher reports the id ``arrow_v1``."""
        assert get_default_context().arrow_hasher.hasher_id == "arrow_v1"

    def test_table_hash_is_format_2(self):
        """A table hash has method ``arrow_v1`` and a digest starting ``00 00 02``."""
        result = _hash_table(pa.table({"x": pa.array([1, 2], pa.int64())}))
        assert result.method == "arrow_v1"
        assert result.digest[:3] == FORMAT_2_PREFIX

    def test_row_data_hash_carries_new_label(self):
        """A row's data hash, as text, starts with ``arrow_v1:000002``."""
        hashes = _row_hashes(pa.array([1], pa.int64()))
        assert hashes[0].startswith("arrow_v1:000002")


# ---------------------------------------------------------------------------
# ITL-848 collisions are gone
# ---------------------------------------------------------------------------


class TestFormat1CollisionsFixed:
    """Inputs that collided or depended on layout under format 1 (ITL-848)."""

    def test_nested_list_lengths_are_part_of_the_row_hash(self):
        """The same leaves grouped differently in nested lists hash differently."""
        list3 = pa.large_list(pa.large_list(pa.large_list(pa.int64())))
        first = _row_hashes(pa.array([[[[1]], [[2], [3]]]], list3))
        second = _row_hashes(pa.array([[[[1], [2]], [[3]]]], list3))
        assert first != second

    def test_null_struct_differs_from_struct_of_nulls(self):
        """A null struct and a struct whose only field is null hash differently."""
        struct = pa.struct([("a", pa.int64())])
        null_struct = _row_hashes(pa.array([None], struct))
        struct_of_nulls = _row_hashes(pa.array([{"a": None}], struct))
        assert null_struct != struct_of_nulls

    def test_boolean_hash_does_not_depend_on_chunking(self):
        """A boolean column split into one chunk per row hashes like the whole."""
        table = pa.table({"x": pa.array([True, False, True])})
        rechunked = pa.Table.from_batches(table.to_batches(max_chunksize=1))
        assert rechunked.column("x").num_chunks == 3
        assert _hash_table(table) == _hash_table(rechunked)

    def test_boolean_row_count_is_part_of_the_hash(self):
        """Non-nullable boolean columns of one and of three rows hash differently."""
        one = _non_nullable("x", pa.array([True]))
        three = _non_nullable("x", pa.array([True, False, False]))
        assert _hash_table(one) != _hash_table(three)

    def test_slice_offset_does_not_change_the_hash(self):
        """A slice with a non-zero offset hashes as a fresh table of the same rows."""
        sliced = pa.table({"x": pa.array([1, 2, 3, 4], pa.int64())}).slice(1, 2)
        fresh = pa.table({"x": pa.array([2, 3], pa.int64())})
        assert _hash_table(sliced) == _hash_table(fresh)

    def test_chunk_layout_does_not_change_the_hash(self):
        """A string column with a null hashes the same chunked and combined."""
        table = pa.table({"x": pa.array(["a", None, "ccc", "dd"], pa.large_string())})
        chunked = pa.Table.from_batches(table.to_batches(max_chunksize=3))
        assert chunked.column("x").num_chunks == 2
        assert _hash_table(chunked) == _hash_table(chunked.combine_chunks())
        assert _hash_table(chunked) == _hash_table(table)


# ---------------------------------------------------------------------------
# fixed_size_list (ITL-927)
# ---------------------------------------------------------------------------


def _fsl_table() -> pa.Table:
    """A two-row table with a tag ``k`` and a ``fixed_size_list<float32>[2]`` column."""
    return pa.table(
        {
            "k": pa.array(["a", "b"], pa.large_string()),
            "v": pa.array([[1.0, 2.0], [3.0, 4.0]], pa.list_(pa.float32(), 2)),
        }
    )


class TestFixedSizeList:
    """``fixed_size_list`` columns hash and keep their type (ITL-927)."""

    def test_fixed_size_list_column_hashes(self):
        """A table with a ``fixed_size_list`` column hashes to a format 2 digest."""
        assert _hash_table(_fsl_table()).digest[:3] == FORMAT_2_PREFIX

    def test_fixed_size_list_differs_from_large_list_of_same_values(self):
        """Equal values in ``fixed_size_list`` and ``large_list`` hash differently."""
        fsl = pa.table({"v": pa.array([[1.0, 2.0]], pa.list_(pa.float32(), 2))})
        large = pa.table({"v": pa.array([[1.0, 2.0]], pa.large_list(pa.float32()))})
        assert _hash_table(fsl) != _hash_table(large)

    def test_nested_fixed_size_list_hashes(self):
        """A ``fixed_size_list`` inside a struct and inside a ``large_list`` hashes."""
        nested = pa.table(
            {
                "s": pa.array(
                    [{"v": [1, 2]}], pa.struct([("v", pa.list_(pa.int64(), 2))])
                ),
                "l": pa.array([[[1, 2]]], pa.large_list(pa.list_(pa.int64(), 2))),
            }
        )
        assert _hash_table(nested).digest[:3] == FORMAT_2_PREFIX

    def test_source_with_fixed_size_list_column_keeps_its_type(self):
        """A source keeps the ``fixed_size_list`` type; its rows hash distinctly."""
        source = ArrowTableSource(
            _fsl_table(), tag_columns=["k"], tag_only_columns=True
        )
        assert source.as_table().schema.field("v").type == pa.list_(pa.float32(), 2)
        hashes = [data.content_hash() for _, data in source.iter_data()]
        assert len(set(hashes)) == 2

    def test_fixed_size_list_column_runs_through_a_join(self):
        """``Join`` keeps a ``fixed_size_list`` column's type, values and row hashes."""
        source = ArrowTableSource(
            _fsl_table(), tag_columns=["k"], infer_nullable=True, tag_only_columns=True
        )
        other = ArrowTableSource(
            pa.table(
                {
                    "k": pa.array(["a", "b"], pa.large_string()),
                    "w": pa.array([1, 2], pa.int64()),
                }
            ),
            tag_columns=["k"],
            infer_nullable=True,
            tag_only_columns=True,
        )
        joined = Join()(source, other)
        table = joined.as_table()
        assert table.schema.field("v").type == pa.list_(pa.float32(), 2)
        assert table.column("v").to_pylist() == [[1.0, 2.0], [3.0, 4.0]]
        assert len({data.content_hash() for _, data in joined.iter_data()}) == 2


# ---------------------------------------------------------------------------
# Canonical Arrow extension types keep their identity
# ---------------------------------------------------------------------------


class TestCanonicalExtensionTypes:
    """A canonical Arrow extension type hashes differently from its storage."""

    def test_uuid_column_differs_from_its_storage(self):
        """A ``uuid`` column and its ``binary(16)`` storage hash differently."""
        storage = pa.array([b"0" * 16, b"1" * 16], pa.binary(16))
        uuid_table = pa.table({"u": pa.ExtensionArray.from_storage(pa.uuid(), storage)})
        storage_table = pa.table({"u": storage})
        assert _hash_table(uuid_table) != _hash_table(storage_table)

    def test_fixed_shape_tensor_differs_from_its_storage(self):
        """A fixed-shape tensor column hashes differently from its list storage."""
        tensor_type = pa.fixed_shape_tensor(pa.float32(), [2])
        storage = pa.array([[1.0, 2.0]], pa.list_(pa.float32(), 2))
        tensor_table = pa.table(
            {"t": pa.ExtensionArray.from_storage(tensor_type, storage)}
        )
        assert _hash_table(tensor_table) != _hash_table(pa.table({"t": storage}))

    def test_uuid_schema_differs_from_its_storage(self):
        """A ``uuid`` field and a ``binary(16)`` field hash to different schemas."""
        hasher = get_default_context().arrow_hasher
        uuid_schema = pa.schema([pa.field("u", pa.uuid())])
        storage_schema = pa.schema([pa.field("u", pa.binary(16))])
        assert hasher.hash_schema(uuid_schema) != hasher.hash_schema(storage_schema)  # pyright: ignore[reportAttributeAccessIssue]  # concrete hasher method, not on the protocol


# ---------------------------------------------------------------------------
# Inputs starfix 0.5 rejects
# ---------------------------------------------------------------------------


class TestRejections:
    """Inputs the format 2 hasher refuses, and one it must still accept."""

    def test_zero_row_map_column_is_rejected(self):
        """A ``map`` column is refused by the hasher even when it has no rows."""
        table = pa.table({"m": pa.array([], pa.map_(pa.large_string(), pa.int64()))})
        with pytest.raises(ValueError, match="map"):
            _hash_table(table)

    def test_non_nullable_column_holding_a_null_is_rejected(self):
        """A source refuses a null in a column declared non-nullable."""
        table = pa.Table.from_arrays(
            [pa.array(["a", "b"], pa.large_string()), pa.array([1, None], pa.int64())],
            schema=pa.schema(
                [
                    pa.field("k", pa.large_string()),
                    pa.field("x", pa.int64(), nullable=False),
                ]
            ),
        )
        with pytest.raises(ValueError, match="Non-nullable field 'x'"):
            ArrowTableSource(table, tag_columns=["k"], tag_only_columns=True)

    def test_duplicate_column_names_are_rejected(self):
        """Hashing a table with two columns named ``x`` raises an ambiguity error."""
        table = pa.Table.from_arrays([pa.array([1]), pa.array([2])], names=["x", "x"])
        with pytest.raises(ValueError, match="Ambiguous node path 'x'"):
            get_default_context().semantic_hasher.hash_object(table)

    def test_slash_name_aliasing_a_struct_child_is_rejected(self):
        """A column ``a/b`` beside a struct ``a`` with child ``b`` is refused."""
        table = pa.table(
            {
                "k": pa.array(["a"], pa.large_string()),
                "a/b": pa.array([1], pa.int64()),
                "a": pa.array([{"b": 2}], pa.struct([("b", pa.int64())])),
            }
        )
        with pytest.raises(ValueError, match="Ambiguous node path 'a/b'"):
            ArrowTableSource(table, tag_columns=["k"], tag_only_columns=True)

    def test_slash_name_aliasing_nothing_is_accepted(self):
        """A column name with a slash that matches no struct child is accepted."""
        table = pa.table(
            {
                "k": pa.array(["a"], pa.large_string()),
                "km/h": pa.array([1], pa.int64()),
            }
        )
        ArrowTableSource(table, tag_columns=["k"], tag_only_columns=True)
