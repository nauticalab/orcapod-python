"""Tests for apply_logical_types in database_hooks."""

from __future__ import annotations

import uuid

import pyarrow as pa
import pytest

from orcapod.logical_types.registry import LogicalTypeRegistry, make_arrow_extension_type


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unique_name() -> str:
    return f"test.apply.{uuid.uuid4().hex[:8]}"


def _make_registry_with_type(
    arrow_name: str,
    storage: pa.DataType = pa.large_utf8(),
) -> tuple[LogicalTypeRegistry, pa.ExtensionType]:
    """Return a registry with one registered extension type and the type instance."""
    import polars as pl

    ExtCls = make_arrow_extension_type(arrow_name, storage)
    ext_type = ExtCls()
    pl_storage = pl.from_arrow(pa.array([], type=storage)).dtype

    class _PolarsExt(pl.BaseExtension):
        def __init__(self):
            super().__init__(arrow_name, pl_storage, None)
        @classmethod
        def ext_from_params(cls, ext_name, storage_dtype, metadata_str):
            return cls()

    class _LT:
        @property
        def logical_type_name(self):
            return arrow_name
        @property
        def python_type(self):
            return str
        def get_arrow_extension_type(self):
            return ext_type
        def get_polars_extension_type(self):
            return _PolarsExt()
        def python_to_storage(self, v):
            return str(v)
        def storage_to_python(self, v):
            return v

    registry = LogicalTypeRegistry()
    registry.register_logical_type(_LT())
    return registry, ext_type


def _degraded_table_with_metadata(
    arrow_name: str,
    storage: pa.DataType,
    values: list,
) -> pa.Table:
    """Build a table that carries extension field metadata but uses storage type.

    Simulates what you get when Arrow reads a Parquet/IPC file whose extension
    type was not registered at read time.
    """
    col = pa.array(values, type=storage)
    field = pa.field("col", storage).with_metadata({
        b"ARROW:extension:name": arrow_name.encode(),
        b"ARROW:extension:metadata": b"",
    })
    schema = pa.schema([field])
    return pa.table({"col": col}, schema=schema)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_noop_when_no_extension_metadata():
    """Table with plain Arrow types is returned unchanged."""
    from orcapod.logical_types.database_hooks import apply_logical_types

    registry = LogicalTypeRegistry()
    table = pa.table({"x": pa.array([1, 2, 3], type=pa.int32())})
    result = apply_logical_types(table, registry)
    assert result is table  # same object — nothing to do


def test_wraps_storage_column_into_extension_type():
    """A column with extension field metadata is re-wrapped into the registered type."""
    from orcapod.logical_types.database_hooks import apply_logical_types

    name = _unique_name()
    registry, ext_type = _make_registry_with_type(name, pa.large_utf8())
    table = _degraded_table_with_metadata(name, pa.large_utf8(), ["hello", "world"])

    result = apply_logical_types(table, registry)

    assert result.schema.field("col").type == ext_type
    assert result.column("col").to_pylist() == ["hello", "world"]


def test_zero_copy_single_chunk():
    """from_storage wrapping shares the underlying buffer — no data copy."""
    from orcapod.logical_types.database_hooks import apply_logical_types

    name = _unique_name()
    registry, _ = _make_registry_with_type(name, pa.large_utf8())
    table = _degraded_table_with_metadata(name, pa.large_utf8(), ["a", "b"])

    result = apply_logical_types(table, registry)

    orig_buf = table.column("col").chunk(0).buffers()[2]
    new_buf = result.column("col").chunk(0).buffers()[2]
    assert orig_buf == new_buf


def test_zero_copy_multiple_chunks():
    """Multi-chunk columns are wrapped per-chunk, all buffers shared."""
    from orcapod.logical_types.database_hooks import apply_logical_types

    name = _unique_name()
    registry, ext_type = _make_registry_with_type(name, pa.large_utf8())

    # Build a multi-chunk ChunkedArray with extension metadata on the field
    c1 = pa.array(["x"], type=pa.large_utf8())
    c2 = pa.array(["y", "z"], type=pa.large_utf8())
    chunked = pa.chunked_array([c1, c2], type=pa.large_utf8())
    field = pa.field("col", pa.large_utf8()).with_metadata({
        b"ARROW:extension:name": name.encode(),
        b"ARROW:extension:metadata": b"",
    })
    schema = pa.schema([field])
    table = pa.table({"col": chunked}, schema=schema)

    result = apply_logical_types(table, registry)
    result_col = result.column("col")

    assert result.schema.field("col").type == ext_type
    assert result_col.num_chunks == 2
    assert result_col.to_pylist() == ["x", "y", "z"]
    # Buffer identity per chunk
    for i, (orig, wrapped) in enumerate(zip(chunked.chunks, result_col.chunks)):
        assert orig.buffers()[2] == wrapped.buffers()[2], f"chunk {i} buffer differs"


def test_already_extension_type_passthrough():
    """Column already carrying an extension type is returned as-is."""
    from orcapod.logical_types.database_hooks import apply_logical_types

    name = _unique_name()
    registry, ext_type = _make_registry_with_type(name, pa.large_utf8())
    # Build a table with a properly typed extension column (already registered)
    arr = pa.ExtensionArray.from_storage(ext_type, pa.array(["a"], type=pa.large_utf8()))
    table = pa.table({"col": arr})

    result = apply_logical_types(table, registry)
    assert result is table


def test_unregistered_extension_metadata_left_as_storage():
    """A column whose extension type is not in the registry stays as storage type."""
    from orcapod.logical_types.database_hooks import apply_logical_types

    name = _unique_name()
    registry = LogicalTypeRegistry()  # no types registered
    table = _degraded_table_with_metadata(name, pa.large_utf8(), ["v"])

    result = apply_logical_types(table, registry)

    # Column stays as large_utf8 — registry has nothing to apply
    assert result.schema.field("col").type == pa.large_utf8()


def test_nested_struct_extension_type():
    """Extension type inside a struct child field is reconstructed recursively."""
    from orcapod.logical_types.database_hooks import apply_logical_types

    name = _unique_name()
    registry, ext_type = _make_registry_with_type(name, pa.large_utf8())

    # Build degraded struct: inner field has extension metadata but storage type
    inner_field = pa.field("inner", pa.large_utf8()).with_metadata({
        b"ARROW:extension:name": name.encode(),
        b"ARROW:extension:metadata": b"",
    })
    struct_type = pa.struct([inner_field])
    inner_data = pa.array(["p", "q"], type=pa.large_utf8())
    struct_col = pa.StructArray.from_arrays([inner_data], fields=[inner_field])
    schema = pa.schema([pa.field("s", struct_type)])
    table = pa.table({"s": struct_col}, schema=schema)

    result = apply_logical_types(table, registry)

    result_struct_type = result.schema.field("s").type
    assert pa.types.is_struct(result_struct_type)
    result_inner_field = result_struct_type.field("inner")
    assert result_inner_field.type == ext_type
    assert result.column("s").to_pylist() == [{"inner": "p"}, {"inner": "q"}]


def test_mixed_columns_only_ext_columns_changed():
    """Plain columns are left untouched when an extension column is processed."""
    from orcapod.logical_types.database_hooks import apply_logical_types

    name = _unique_name()
    registry, ext_type = _make_registry_with_type(name, pa.large_utf8())

    ext_field = pa.field("ext_col", pa.large_utf8()).with_metadata({
        b"ARROW:extension:name": name.encode(),
        b"ARROW:extension:metadata": b"",
    })
    plain_field = pa.field("plain_col", pa.int32())
    schema = pa.schema([ext_field, plain_field])
    table = pa.table(
        {"ext_col": pa.array(["a"], type=pa.large_utf8()), "plain_col": pa.array([1], type=pa.int32())},
        schema=schema,
    )

    result = apply_logical_types(table, registry)

    assert result.schema.field("ext_col").type == ext_type
    assert result.schema.field("plain_col").type == pa.int32()
    assert result.column("plain_col").to_pylist() == [1]


def test_schema_level_metadata_preserved():
    """Schema-level metadata (e.g. pandas metadata) is preserved when rebuilding schema."""
    from orcapod.logical_types.database_hooks import apply_logical_types

    name = _unique_name()
    registry, ext_type = _make_registry_with_type(name, pa.large_utf8())

    ext_field = pa.field("col", pa.large_utf8()).with_metadata({
        b"ARROW:extension:name": name.encode(),
        b"ARROW:extension:metadata": b"",
    })
    schema_meta = {b"pandas": b'{"some": "pandas_metadata"}', b"custom": b"value"}
    schema = pa.schema([ext_field], metadata=schema_meta)
    table = pa.table({"col": pa.array(["x"], type=pa.large_utf8())}, schema=schema)

    result = apply_logical_types(table, registry)

    assert result.schema.field("col").type == ext_type
    assert result.schema.metadata == schema_meta


def test_plain_struct_not_rebuilt():
    """A struct column with no extension children is returned as-is without rebuilding."""
    from orcapod.logical_types.database_hooks import apply_logical_types

    registry = LogicalTypeRegistry()  # empty — nothing registered
    inner_field = pa.field("x", pa.int32())
    struct_type = pa.struct([inner_field])
    struct_col = pa.StructArray.from_arrays(
        [pa.array([1, 2], type=pa.int32())], fields=[inner_field]
    )
    schema = pa.schema([pa.field("s", struct_type)])
    table = pa.table({"s": struct_col}, schema=schema)

    result = apply_logical_types(table, registry)

    # Nothing changed — same object returned
    assert result is table


def test_struct_null_bitmap_preserved():
    """Null struct rows retain their null status after extension type wrapping."""
    from orcapod.logical_types.database_hooks import apply_logical_types

    name = _unique_name()
    registry, ext_type = _make_registry_with_type(name, pa.large_utf8())

    inner_field = pa.field("inner", pa.large_utf8()).with_metadata({
        b"ARROW:extension:name": name.encode(),
        b"ARROW:extension:metadata": b"",
    })
    struct_type = pa.struct([inner_field])
    inner_data = pa.array(["a", "b", "c"], type=pa.large_utf8())
    # Build struct with a null at position 1
    struct_col = pa.StructArray.from_arrays(
        [inner_data],
        fields=[inner_field],
        mask=pa.array([False, True, False]),  # True = null
    )
    schema = pa.schema([pa.field("s", struct_type)])
    table = pa.table({"s": struct_col}, schema=schema)

    result = apply_logical_types(table, registry)

    result_col = result.column("s")
    assert result_col.null_count == 1
    rows = result_col.to_pylist()
    assert rows[0] is not None
    assert rows[1] is None
    assert rows[2] is not None
