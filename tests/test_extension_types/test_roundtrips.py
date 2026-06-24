"""End-to-end integration tests for extension type round-trips.

Tests the complete pipeline:

    Python object → write → storage → peek-schema → register → read → Python object

Each round-trip test is parameterised over two storage backends:

- ``parquet``: direct ``pyarrow.parquet`` write/read.
- ``delta``: ``deltalake.write_deltalake`` / ``DeltaTable.to_pyarrow_table()``.

SQLite (``ConnectorArrowDatabase`` + ``SQLiteConnector``) is excluded because
``SQLiteConnector`` maps Arrow types to SQL column types and discards
``ARROW:extension:*`` field metadata.  Without that metadata, the
peek-register-read pattern cannot auto-register extension types on the read
path.  The ``ExtensionAwareDatabase`` wrapper behaviour over SQLite is already
tested in ``tests/test_databases/test_extension_aware_database.py``.
"""
from __future__ import annotations

import dataclasses
import pathlib
import uuid as uuid_module
from pathlib import Path
from typing import Callable

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from upath import UPath

from orcapod.contexts import create_registry
from orcapod.semantic_types.universal_converter import UniversalTypeConverter


# ── Module-level dataclasses ──────────────────────────────────────────────────
# DataclassLogicalTypeFactory rejects local (in-function) classes because they
# have no stable fully-qualified class name for reconstruction from Arrow schema.

@dataclasses.dataclass
class _PointA:
    x: int
    y: int


@dataclasses.dataclass
class _PointB:
    """Same struct shape as _PointA, different class name."""
    x: int
    y: int


@dataclasses.dataclass
class _Inner:
    value: int


@dataclasses.dataclass
class _Outer:
    inner: _Inner
    label: str


# ── Storage backend abstraction ───────────────────────────────────────────────


@dataclasses.dataclass
class _StorageBackend:
    """Encapsulates backend-specific write and read logic for parameterised tests.

    Args:
        name: Short identifier used in pytest test IDs (e.g. ``"parquet"``).
        write: Callable that writes an Arrow table to a directory.
        read: Callable that reads from that directory and returns an Arrow table
            with extension types registered and applied.  Must return only the
            original user data columns (no ``__record_id`` or similar).
    """
    name: str
    write: Callable[[pa.Table, Path], None]
    read: Callable[[Path, UniversalTypeConverter], pa.Table]


def _parquet_write(table: pa.Table, base_path: Path) -> None:
    pq.write_table(table, str(base_path / "data.parquet"))


def _parquet_read(base_path: Path, converter: UniversalTypeConverter) -> pa.Table:
    return converter.load_extension_types(pq.read_table(str(base_path / "data.parquet")))


def _delta_write(table: pa.Table, base_path: Path) -> None:
    import deltalake
    deltalake.write_deltalake(str(base_path / "delta"), table)


def _delta_read(base_path: Path, converter: UniversalTypeConverter) -> pa.Table:
    import deltalake
    import pyarrow.dataset as pa_ds
    dt = deltalake.DeltaTable(str(base_path / "delta"))
    # Read via PyArrow dataset directly rather than dt.to_pyarrow_table().
    # to_pyarrow_table() normalises large_string → string and large_binary →
    # binary via Delta Lake's schema layer, which causes the extension type
    # deserializer to reject the storage type mismatch.  Reading the underlying
    # Parquet files directly preserves the original Arrow types.
    raw = pa_ds.dataset(dt.file_uris(), format="parquet").to_table()
    return converter.load_extension_types(raw)


_BACKENDS = [
    _StorageBackend(name="parquet", write=_parquet_write, read=_parquet_read),
    _StorageBackend(name="delta", write=_delta_write, read=_delta_read),
]


@pytest.fixture(params=_BACKENDS, ids=lambda b: b.name)
def storage_backend(request: pytest.FixtureRequest) -> _StorageBackend:
    """Yield one storage backend per parametrised run."""
    return request.param


# ── Internal helpers ──────────────────────────────────────────────────────────


def _fresh_converter() -> UniversalTypeConverter:
    """Return a fresh converter from a new registry instance.

    Uses ``create_registry()`` instead of ``get_default_context()`` to avoid
    cross-test contamination through the global singleton cache.
    """
    return create_registry().get_context().type_converter


def _write_and_read(
    schema_dict: dict,
    rows: list[dict],
    backend: _StorageBackend,
    tmp_path: Path,
) -> tuple[pa.Table, UniversalTypeConverter]:
    """Write rows with a fresh write converter and read back with a fresh read converter.

    Returns the resulting Arrow table (with extension types applied) and the
    read-side converter (needed for ``arrow_table_to_python_dicts``).
    """
    write_converter = _fresh_converter()
    # Pre-register each type so the converter can map it to an Arrow extension
    # type before python_schema_to_arrow_schema inspects it.  Built-in types
    # (Path, UPath, UUID) are already registered in the context; dataclass types
    # are auto-discovered on the first register_python_class call.
    for python_type in schema_dict.values():
        write_converter.register_python_class(python_type)
    arrow_schema = write_converter.python_schema_to_arrow_schema(schema_dict)
    table = write_converter.python_dicts_to_arrow_table(rows, arrow_schema=arrow_schema)
    backend.write(table, tmp_path)

    read_converter = _fresh_converter()
    result = backend.read(tmp_path, read_converter)
    return result, read_converter


# ── Built-in type round-trip tests ───────────────────────────────────────────


def test_builtin_path_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """pathlib.Path round-trips through storage with extension name ``orcapod.path``.

    Built-in types (Path, UPath, UUID) are pre-registered in the default context
    so the read-side converter already knows about them.  The test verifies that:

    1. The Arrow field carries the ``orcapod.path`` extension type after read.
    2. The Python value is reconstructed as a ``pathlib.Path`` instance.
    """
    p = pathlib.Path("/tmp/orcapod/integration/test.txt")
    result, read_converter = _write_and_read(
        {"col": pathlib.Path},
        [{"col": p}],
        storage_backend,
        tmp_path,
    )

    field = result.schema.field("col")
    assert hasattr(field.type, "extension_name"), (
        f"Expected extension type on field 'col', got plain type {field.type!r}"
    )
    assert field.type.extension_name == "orcapod.path"

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    assert isinstance(rows[0]["col"], pathlib.Path)
    assert rows[0]["col"] == p


def test_builtin_upath_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """UPath round-trips through storage with extension name ``orcapod.upath``."""
    u = UPath("s3://my-bucket/data/file.parquet")
    result, read_converter = _write_and_read(
        {"col": UPath},
        [{"col": u}],
        storage_backend,
        tmp_path,
    )

    field = result.schema.field("col")
    assert hasattr(field.type, "extension_name"), (
        f"Expected extension type on field 'col', got plain type {field.type!r}"
    )
    assert field.type.extension_name == "orcapod.upath"

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    assert isinstance(rows[0]["col"], UPath)
    assert str(rows[0]["col"]) == str(u)


def test_builtin_uuid_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """uuid.UUID round-trips through storage with extension name ``orcapod.uuid``."""
    u = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    result, read_converter = _write_and_read(
        {"col": uuid_module.UUID},
        [{"col": u}],
        storage_backend,
        tmp_path,
    )

    field = result.schema.field("col")
    assert hasattr(field.type, "extension_name"), (
        f"Expected extension type on field 'col', got plain type {field.type!r}"
    )
    assert field.type.extension_name == "orcapod.uuid"

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    assert isinstance(rows[0]["col"], uuid_module.UUID)
    assert rows[0]["col"] == u


# ── Dataclass round-trip tests ────────────────────────────────────────────────


def test_simple_dataclass_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """Simple dataclass round-trips with correct FQCN as the Arrow extension name.

    The read-side converter starts with no knowledge of _PointA.  After read,
    register_discovered_extensions triggers DataclassLogicalTypeFactory which
    imports _PointA from its fully-qualified class name and registers it.
    """
    point = _PointA(x=3, y=7)
    result, read_converter = _write_and_read(
        {"point": _PointA},
        [{"point": point}],
        storage_backend,
        tmp_path,
    )

    fqcn = f"{_PointA.__module__}.{_PointA.__qualname__}"
    field = result.schema.field("point")
    assert hasattr(field.type, "extension_name"), (
        f"Expected extension type on field 'point', got {field.type!r}"
    )
    assert field.type.extension_name == fqcn

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    reconstructed = rows[0]["point"]
    assert isinstance(reconstructed, _PointA)
    assert reconstructed.x == 3
    assert reconstructed.y == 7


def test_two_dataclasses_same_shape_distinct_extension_names(
    storage_backend: _StorageBackend, tmp_path: Path
) -> None:
    """_PointA and _PointB have the same struct shape but different extension names.

    Writing _PointA and reading it back must NOT reconstruct a _PointB, even
    though their on-disk struct shapes (x: int, y: int) are identical.  The
    extension name (FQCN) is the sole identity signal.
    """
    point_a = _PointA(x=1, y=2)
    result, read_converter = _write_and_read(
        {"point": _PointA},
        [{"point": point_a}],
        storage_backend,
        tmp_path,
    )

    fqcn_a = f"{_PointA.__module__}.{_PointA.__qualname__}"
    fqcn_b = f"{_PointB.__module__}.{_PointB.__qualname__}"

    field = result.schema.field("point")
    assert hasattr(field.type, "extension_name")
    assert field.type.extension_name == fqcn_a
    assert field.type.extension_name != fqcn_b  # distinct from _PointB

    rows = read_converter.arrow_table_to_python_dicts(result)
    reconstructed = rows[0]["point"]
    assert isinstance(reconstructed, _PointA)
    assert not isinstance(reconstructed, _PointB)


def test_nested_dataclass_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """Nested dataclass: _Outer and _Inner both registered; full object reconstructed.

    register_discovered_extensions triggers DataclassLogicalTypeFactory for _Outer.
    That factory's reconstruct_from_arrow calls converter.register_python_class(_Inner)
    as a side-effect, so _Inner is also registered without an explicit peek step.
    """
    outer = _Outer(inner=_Inner(value=42), label="hello")
    result, read_converter = _write_and_read(
        {"item": _Outer},
        [{"item": outer}],
        storage_backend,
        tmp_path,
    )

    fqcn_outer = f"{_Outer.__module__}.{_Outer.__qualname__}"
    fqcn_inner = f"{_Inner.__module__}.{_Inner.__qualname__}"

    assert read_converter._logical_type_registry.get_by_arrow_extension_name(fqcn_outer) is not None, (
        "_Outer should be registered after read"
    )
    assert read_converter._logical_type_registry.get_by_arrow_extension_name(fqcn_inner) is not None, (
        "_Inner should be registered transitively after read"
    )

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    reconstructed = rows[0]["item"]
    assert isinstance(reconstructed, _Outer)
    assert isinstance(reconstructed.inner, _Inner)
    assert reconstructed.inner.value == 42
    assert reconstructed.label == "hello"


# ── Delta Lake: Polars native read ───────────────────────────────────────────


def test_delta_polars_read_delta(tmp_path: Path) -> None:
    """Write a dataclass column to Delta; read back via pl.read_delta; extension type survives.

    The write-side converter registers _PointA in both PyArrow's and Polars'
    global registries (``register_python_class`` calls ``make_polars_extension_type``
    which registers with Polars).  ``pl.read_delta`` can therefore decode the column
    as the correct Polars extension type, not a plain ``Struct``.

    Note: ``pl.DataFrame.to_arrow()`` exports Polars extension types as PyArrow
    extension arrays but with empty serialized bytes (Polars does not forward
    ``__arrow_ext_metadata__`` through its Arrow export).  Python-object
    reconstruction via the Polars-to-Arrow path is therefore not possible; that
    path is tested by the separate ``parquet`` / ``delta`` parametrised tests
    which read underlying Parquet files directly.
    """
    import deltalake
    import polars as pl

    delta_path = str(tmp_path / "polars_delta")
    fqcn = f"{_PointA.__module__}.{_PointA.__qualname__}"

    # Write — registers _PointA in PyArrow + Polars global registries.
    write_converter = _fresh_converter()
    write_converter.register_python_class(_PointA)
    arrow_schema = write_converter.python_schema_to_arrow_schema({"point": _PointA})
    rows = [{"point": _PointA(x=5, y=9)}]
    table = write_converter.python_dicts_to_arrow_table(rows, arrow_schema=arrow_schema)
    deltalake.write_deltalake(delta_path, table)

    # Read via Polars native Delta reader.
    # _PointA is already in the Polars global registry from the write step above.
    df = pl.read_delta(delta_path)

    # Assert the column carries the correct Polars extension type — not a plain Struct.
    col_dtype = df.dtypes[0]
    assert col_dtype.is_extension(), (
        f"Expected a Polars extension type on column 'point', got {col_dtype!r}"
    )
    assert col_dtype.ext_name() == fqcn, (
        f"Expected extension name {fqcn!r}, got {col_dtype.ext_name()!r}"
    )
