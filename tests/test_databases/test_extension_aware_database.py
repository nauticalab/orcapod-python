"""Tests for ExtensionAwareDatabase."""

from __future__ import annotations

import uuid

import pyarrow as pa
import pytest

from orcapod.databases.extension_aware_database import ExtensionAwareDatabase
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
from orcapod.extension_types.registry import LogicalTypeRegistry, make_arrow_extension_type


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unique_name() -> str:
    return f"test.eadb.{uuid.uuid4().hex[:8]}"


def _make_registry_with_type(
    arrow_name: str,
    storage: pa.DataType = pa.large_utf8(),
):
    """Return a (registry, ext_type_instance) pair with one registered type."""
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


def _degraded_table(arrow_name: str, storage: pa.DataType, values: list) -> pa.Table:
    """Arrow table with extension field metadata but storage type (simulates unregistered read)."""
    col = pa.array(values, type=storage)
    field = pa.field("col", storage).with_metadata({
        b"ARROW:extension:name": arrow_name.encode(),
        b"ARROW:extension:metadata": b"",
    })
    return pa.table({"col": col}, schema=pa.schema([field]))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_get_all_records_applies_extension_types():
    """get_all_records returns table with extension types applied."""
    name = _unique_name()
    registry, ext_type = _make_registry_with_type(name)

    inner_db = InMemoryArrowDatabase()
    # Add two separate records (distinct record_ids) so both rows survive deduplication.
    r1 = _degraded_table(name, pa.large_utf8(), ["hello"])
    r2 = _degraded_table(name, pa.large_utf8(), ["world"])
    inner_db.add_record(("test",), record_id=b"r1", record=r1, flush=False)
    inner_db.add_record(("test",), record_id=b"r2", record=r2, flush=True)

    db = ExtensionAwareDatabase(inner_db, registry)
    result = db.get_all_records(("test",))

    assert result is not None
    assert result.schema.field("col").type == ext_type
    assert sorted(result.column("col").to_pylist()) == ["hello", "world"]


def test_get_record_by_id_applies_extension_types():
    """get_record_by_id returns table with extension types applied."""
    name = _unique_name()
    registry, ext_type = _make_registry_with_type(name)

    inner_db = InMemoryArrowDatabase()
    degraded = _degraded_table(name, pa.large_utf8(), ["x"])
    inner_db.add_record(("p",), record_id=b"r1", record=degraded, flush=True)

    db = ExtensionAwareDatabase(inner_db, registry)
    result = db.get_record_by_id(("p",), b"r1")

    assert result is not None
    assert result.schema.field("col").type == ext_type


def test_get_records_by_ids_applies_extension_types():
    """get_records_by_ids returns table with extension types applied."""
    name = _unique_name()
    registry, ext_type = _make_registry_with_type(name)

    inner_db = InMemoryArrowDatabase()
    degraded = _degraded_table(name, pa.large_utf8(), ["a"])
    inner_db.add_record(("p",), record_id=b"r1", record=degraded, flush=True)

    db = ExtensionAwareDatabase(inner_db, registry)
    result = db.get_records_by_ids(("p",), [b"r1"])

    assert result is not None
    assert result.schema.field("col").type == ext_type


def test_get_all_records_returns_none_when_no_records():
    """Returns None when the underlying database has no records for the path."""
    registry = LogicalTypeRegistry()
    inner_db = InMemoryArrowDatabase()
    db = ExtensionAwareDatabase(inner_db, registry)

    assert db.get_all_records(("nonexistent",)) is None


def test_write_methods_passthrough():
    """add_record and add_records write correctly through the wrapper."""
    registry = LogicalTypeRegistry()
    inner_db = InMemoryArrowDatabase()
    db = ExtensionAwareDatabase(inner_db, registry)

    t1 = pa.table({"x": pa.array([1], type=pa.int32())})
    t2 = pa.table({"x": pa.array([2], type=pa.int32())})
    db.add_record(("p",), record_id=b"r1", record=t1, flush=False)
    db.add_record(("p",), record_id=b"r2", record=t2, flush=True)

    result = inner_db.get_all_records(("p",))
    assert result is not None
    assert sorted(result.column("x").to_pylist()) == [1, 2]


def test_at_returns_extension_aware_database():
    """at() returns an ExtensionAwareDatabase with the same registry."""
    registry = LogicalTypeRegistry()
    inner_db = InMemoryArrowDatabase()
    db = ExtensionAwareDatabase(inner_db, registry)

    scoped = db.at("sub", "path")

    assert isinstance(scoped, ExtensionAwareDatabase)
    assert scoped._registry is registry
    assert scoped.base_path == ("sub", "path")


def test_base_path_delegates_to_inner():
    """base_path reflects the inner database's base_path."""
    registry = LogicalTypeRegistry()
    inner_db = InMemoryArrowDatabase()
    db = ExtensionAwareDatabase(inner_db, registry)

    assert db.base_path == ()
    assert db.at("a").base_path == ("a",)


def test_plain_table_passthrough_unchanged():
    """Tables with no extension type metadata are returned as-is (no wrapping overhead)."""
    registry = LogicalTypeRegistry()
    inner_db = InMemoryArrowDatabase()
    db = ExtensionAwareDatabase(inner_db, registry)

    table = pa.table({"n": pa.array([10, 20], type=pa.int64())})
    inner_db.add_record(("p",), record_id=b"r1", record=table, flush=True)

    result = db.get_all_records(("p",))
    assert result is not None
    assert result.schema.field("n").type == pa.int64()
