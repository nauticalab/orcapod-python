"""Tests for register_discovered_logical_types in database_hooks."""

from __future__ import annotations

import json
import uuid

import pyarrow as pa
import pytest

from orcapod.logical_types.registry import LogicalTypeRegistry, make_arrow_extension_type
from orcapod.semantic_types.universal_converter import UniversalTypeConverter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unique_name() -> str:
    """Unique Arrow extension name to avoid cross-test global-registry collisions."""
    return f"test.hooks.{uuid.uuid4().hex[:8]}"


def _make_ext_schema(
    arrow_name: str,
    metadata: bytes | None = None,
    storage: pa.DataType | None = None,
) -> pa.Schema:
    """Build a ``pa.Schema`` with one extension-typed field using ``make_arrow_extension_type``.

    Only call this when you have control over the metadata content — the resulting
    field's type is an in-memory ``pa.ExtensionType`` instance, not raw field metadata.
    """
    _storage = storage or pa.large_utf8()
    ext_cls = make_arrow_extension_type(arrow_name, _storage, metadata=metadata)
    return pa.schema([pa.field("col", ext_cls())])


def _make_field_metadata_schema(
    arrow_name: str,
    metadata: bytes,
    storage: pa.DataType | None = None,
) -> pa.Schema:
    """Build a schema where the extension is described by raw Arrow field metadata.

    This simulates a Parquet/IPC read where the extension type was not registered
    in the current process, so ``field.type`` is a plain Arrow storage type rather
    than a ``pa.ExtensionType`` instance.
    """
    _storage = storage or pa.large_utf8()
    field = pa.field("col", _storage).with_metadata({
        b"ARROW:extension:name": arrow_name.encode(),
        b"ARROW:extension:metadata": metadata,
    })
    return pa.schema([field])


def _make_stub_factory():
    """Return a minimal LogicalTypeFactory stub whose calls are recorded.

    The factory auto-creates a fresh ``LogicalType`` stub keyed by arrow name.
    Registering this factory in a registry causes it to also register a Polars
    extension type, which requires the Arrow ext type to be in PyArrow's global
    registry.  To avoid cross-test collisions, each test uses a unique arrow name.
    """
    class _Factory:
        def __init__(self):
            self.calls: list[tuple] = []

        def supports_class(self, python_type):
            return False

        def reconstruct_from_arrow(self, arrow_extension_name, storage_type, metadata, converter):
            import polars as pl
            from orcapod.logical_types.registry import make_arrow_extension_type

            self.calls.append((arrow_extension_name, storage_type, metadata))

            _name = arrow_extension_name
            _arrow_cls = make_arrow_extension_type(_name, storage_type)
            _pl_storage = pl.from_arrow(pa.array([], type=storage_type)).dtype

            class _PolarsExt(pl.BaseExtension):
                def __init__(self):
                    super().__init__(_name, _pl_storage, None)
                @classmethod
                def ext_from_params(cls, ext_name, storage_dtype, metadata_str):
                    return cls()

            class _StubLT:
                @property
                def logical_type_name(self):
                    return _name
                @property
                def python_type(self):
                    return str
                def get_arrow_extension_type(self):
                    return _arrow_cls()
                def get_polars_extension_type(self):
                    return _PolarsExt()
                def python_to_storage(self, value, converter=None):
                    return str(value)
                def storage_to_python(self, storage_value, converter=None):
                    return storage_value

            return _StubLT()

        def create_for_python_type(self, python_type, converter):
            pass

    return _Factory()


def _make_converter(factory=None, category=None) -> UniversalTypeConverter:
    """Make a UniversalTypeConverter with an optional factory registered."""
    registry = LogicalTypeRegistry()
    converter = UniversalTypeConverter(logical_type_registry=registry)
    if factory is not None and category is not None:
        converter.register_logical_type_factory(factory, category=category)
    return converter


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def fresh_converter():
    """A fresh, isolated converter (with empty registry) for each test."""
    return _make_converter()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_no_extension_types_is_noop(fresh_converter):
    """Schema with only primitives — register_discovered_logical_types returns without touching registry."""
    from orcapod.logical_types.database_hooks import register_discovered_logical_types

    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("name", pa.large_utf8()),
    ])
    register_discovered_logical_types(fresh_converter, schema)
    # fresh registry is empty — no error means no spurious lookup was triggered
    assert fresh_converter._logical_type_registry.get_by_arrow_extension_name("anything") is None


def test_known_type_is_registered():
    """Schema with one extension type whose factory is registered — type is registered after call."""
    from orcapod.logical_types.database_hooks import register_discovered_logical_types

    arrow_name = _unique_name()
    factory = _make_stub_factory()
    converter = _make_converter(factory=factory, category="TestCat")

    metadata_bytes = json.dumps({"category": "TestCat"}).encode()
    schema = _make_ext_schema(arrow_name, metadata=metadata_bytes)

    register_discovered_logical_types(converter, schema)

    assert converter._logical_type_registry.get_by_arrow_extension_name(arrow_name) is not None
    assert len(factory.calls) == 1


def test_already_registered_is_skipped():
    """Calling register_discovered_logical_types twice does not raise and factory is called once."""
    from orcapod.logical_types.database_hooks import register_discovered_logical_types

    arrow_name = _unique_name()
    factory = _make_stub_factory()
    converter = _make_converter(factory=factory, category="TestCat")

    metadata_bytes = json.dumps({"category": "TestCat"}).encode()
    schema = _make_ext_schema(arrow_name, metadata=metadata_bytes)

    register_discovered_logical_types(converter, schema)
    register_discovered_logical_types(converter, schema)  # second call

    assert len(factory.calls) == 1  # factory invoked exactly once


def test_none_metadata_already_registered_noop():
    """Extension type with None metadata that IS already in the registry — silent no-op."""
    from orcapod.logical_types.database_hooks import register_discovered_logical_types

    arrow_name = _unique_name()
    factory = _make_stub_factory()
    converter = _make_converter(factory=factory, category="TestCat")

    # First: register via metadata so it ends up in the registry.
    metadata_bytes = json.dumps({"category": "TestCat"}).encode()
    schema_with_meta = _make_ext_schema(arrow_name, metadata=metadata_bytes)
    register_discovered_logical_types(converter, schema_with_meta)

    # Now: same arrow name but with no metadata (simulates reading the schema without
    # metadata — e.g. after an IPC round-trip where the type is now registered in-process).
    schema_no_meta = _make_ext_schema(arrow_name, metadata=None)
    register_discovered_logical_types(converter, schema_no_meta)  # should NOT raise


def test_none_metadata_not_registered_raises():
    """Unregistered extension type with None metadata raises ValueError."""
    from orcapod.logical_types.database_hooks import register_discovered_logical_types

    arrow_name = _unique_name()
    converter = _make_converter()
    schema = _make_ext_schema(arrow_name, metadata=None)

    with pytest.raises(ValueError, match="Pre-register them explicitly"):
        register_discovered_logical_types(converter, schema)


def test_metadata_not_json_raises():
    """Unregistered extension type with non-JSON metadata bytes raises ValueError."""
    from orcapod.logical_types.database_hooks import register_discovered_logical_types

    arrow_name = _unique_name()
    converter = _make_converter()
    schema = _make_field_metadata_schema(arrow_name, metadata=b"not-json!")

    with pytest.raises(ValueError, match="not valid UTF-8 JSON"):
        register_discovered_logical_types(converter, schema)


def test_metadata_json_missing_category_raises():
    """Unregistered extension type with valid JSON but no 'category' key raises ValueError."""
    from orcapod.logical_types.database_hooks import register_discovered_logical_types

    arrow_name = _unique_name()
    converter = _make_converter()
    schema = _make_field_metadata_schema(
        arrow_name, metadata=json.dumps({"version": 1}).encode()
    )

    with pytest.raises(ValueError, match='"category"'):
        register_discovered_logical_types(converter, schema)


def test_unknown_metadata_raises():
    """Unregistered extension type with valid JSON and 'category' but no matching factory raises ValueError."""
    from orcapod.logical_types.database_hooks import register_discovered_logical_types

    arrow_name = _unique_name()
    converter = _make_converter()
    schema = _make_field_metadata_schema(
        arrow_name, metadata=json.dumps({"category": "NoSuchFactory"}).encode()
    )

    with pytest.raises(ValueError, match="NoSuchFactory"):
        register_discovered_logical_types(converter, schema)


def test_nested_extension_type():
    """Extension type inside a struct column is discovered and registered."""
    from orcapod.logical_types.database_hooks import register_discovered_logical_types

    arrow_name = _unique_name()
    factory = _make_stub_factory()
    converter = _make_converter(factory=factory, category="TestCat")

    metadata_bytes = json.dumps({"category": "TestCat"}).encode()
    inner_ext_cls = make_arrow_extension_type(arrow_name, pa.large_utf8(), metadata=metadata_bytes)

    struct_type = pa.struct([pa.field("inner", inner_ext_cls())])
    schema = pa.schema([pa.field("outer", struct_type)])

    register_discovered_logical_types(converter, schema)

    assert converter._logical_type_registry.get_by_arrow_extension_name(arrow_name) is not None
    assert len(factory.calls) == 1
