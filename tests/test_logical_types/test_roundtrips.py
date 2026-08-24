"""End-to-end integration tests for extension type round-trips.

Tests the complete pipeline:

    Python object → write → storage → peek-schema → register → read → Python object

Each round-trip test is parameterised over two storage backends:

- ``parquet``: direct ``pyarrow.parquet`` write/read.
- ``delta``: ``deltalake.write_deltalake`` / ``DeltaTable.to_pyarrow_dataset(as_large_types=True).to_table()``.

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


@dataclasses.dataclass
class _TaggedPoint:
    """Dataclass with a list[uuid.UUID] field — tests ET2 fix in DataclassLogicalTypeFactory."""
    name: str
    ids: list[uuid_module.UUID]


@dataclasses.dataclass
class _SimplePoint:
    """Dataclass with only scalar fields — used as element of list[_SimplePoint]."""
    label: str
    value: int


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
    return converter.load_logical_types(pq.read_table(str(base_path / "data.parquet")))


def _delta_write(table: pa.Table, base_path: Path) -> None:
    import deltalake
    deltalake.write_deltalake(str(base_path / "delta"), table)


def _delta_read(base_path: Path, converter: UniversalTypeConverter) -> pa.Table:
    import deltalake
    dt = deltalake.DeltaTable(str(base_path / "delta"))
    # as_large_types=True preserves large_string / large_binary rather than
    # normalising them to string / binary (Delta Lake's default behaviour).
    # Without this flag, extension types that use large_string or large_binary
    # as storage fail to deserialise because the _deserialize method strictly
    # checks that the storage type matches the registered one.
    raw = dt.to_pyarrow_dataset(as_large_types=True).to_table()
    return converter.load_logical_types(raw)


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


def test_builtin_ndarray_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """numpy.ndarray round-trips through storage with extension name ``numpy.ndarray``.

    Tests both a simple 1-D float64 array and a structured (record) array with
    named fields, since structured arrays are a primary motivation for this type.
    The read-side converter already knows about ``numpy.ndarray`` because it is
    registered in the default context (``v0.1.json``).
    """
    import numpy as np

    arr_simple = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    arr_struct = np.array([(1.0, 10), (2.0, 20)], dtype=np.dtype([("x", np.float64), ("y", np.int32)]))

    # Simple 1-D float64 array
    simple_path = tmp_path / "simple"
    simple_path.mkdir()
    result, read_converter = _write_and_read(
        {"col": np.ndarray},
        [{"col": arr_simple}],
        storage_backend,
        simple_path,
    )

    field = result.schema.field("col")
    assert hasattr(field.type, "extension_name"), (
        f"Expected extension type on field 'col', got plain type {field.type!r}"
    )
    assert field.type.extension_name == "numpy.ndarray"

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    assert isinstance(rows[0]["col"], np.ndarray)
    assert np.array_equal(rows[0]["col"], arr_simple)
    assert rows[0]["col"].dtype == arr_simple.dtype

    # Structured (record) array
    struct_path = tmp_path / "struct"
    struct_path.mkdir()
    result2, read_converter2 = _write_and_read(
        {"col": np.ndarray},
        [{"col": arr_struct}],
        storage_backend,
        struct_path,
    )

    rows2 = read_converter2.arrow_table_to_python_dicts(result2)
    assert len(rows2) == 1
    recovered = rows2[0]["col"]
    assert isinstance(recovered, np.ndarray)
    assert recovered.dtype == arr_struct.dtype
    assert np.array_equal(recovered["x"], arr_struct["x"])
    assert np.array_equal(recovered["y"], arr_struct["y"])


# ── Dataclass round-trip tests ────────────────────────────────────────────────


def test_simple_dataclass_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """Simple dataclass round-trips with correct FQCN as the Arrow extension name.

    The read-side converter starts with no knowledge of _PointA.  After read,
    register_discovered_logical_types triggers DataclassLogicalTypeFactory which
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

    register_discovered_logical_types triggers DataclassLogicalTypeFactory for _Outer.
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


def test_builtin_dataframe_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """pd.DataFrame round-trips through storage with extension name ``pandas.dataframe``."""
    import pandas as pd
    df = pd.DataFrame(
        {"x": [1.0, 2.0, 3.0], "label": ["a", "b", "c"]},
        index=pd.Index([10, 20, 30], name="row_id"),
    )
    result, read_converter = _write_and_read(
        {"frame": pd.DataFrame},
        [{"frame": df}],
        storage_backend,
        tmp_path,
    )

    field = result.schema.field("frame")
    assert hasattr(field.type, "extension_name"), (
        f"Expected extension type on field 'frame', got plain type {field.type!r}"
    )
    assert field.type.extension_name == "pandas.dataframe"

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    recovered = rows[0]["frame"]
    assert isinstance(recovered, pd.DataFrame)
    pd.testing.assert_frame_equal(recovered, df)


def test_builtin_series_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """pd.Series round-trips through storage with extension name ``pandas.series``."""
    import pandas as pd
    s = pd.Series([10.0, 20.0, 30.0], name="metric", index=pd.Index([1, 2, 3], name="id"))
    result, read_converter = _write_and_read(
        {"series": pd.Series},
        [{"series": s}],
        storage_backend,
        tmp_path,
    )

    field = result.schema.field("series")
    assert hasattr(field.type, "extension_name"), (
        f"Expected extension type on field 'series', got plain type {field.type!r}"
    )
    assert field.type.extension_name == "pandas.series"

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    recovered = rows[0]["series"]
    assert isinstance(recovered, pd.Series)
    pd.testing.assert_series_equal(recovered, s)


# ── list[T] / set[T] round-trip tests (ITL-173) ──────────────────────────────


def test_list_of_uuid_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """list[uuid.UUID] round-trips with extension name list[orcapod.uuid]."""
    u1 = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    u2 = uuid_module.UUID("87654321-4321-8765-4321-876543218765")

    result, read_converter = _write_and_read(
        {"ids": list[uuid_module.UUID]},
        [{"ids": [u1, u2]}],
        storage_backend,
        tmp_path,
    )

    field = result.schema.field("ids")
    assert hasattr(field.type, "extension_name"), (
        f"Expected extension type on 'ids', got {field.type!r}"
    )
    assert field.type.extension_name == "list[orcapod.uuid]"

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    assert rows[0]["ids"] == [u1, u2]
    assert all(isinstance(v, uuid_module.UUID) for v in rows[0]["ids"])


def test_set_of_uuid_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """set[uuid.UUID] round-trips with extension name set[orcapod.uuid]; read back as set."""
    u1 = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    u2 = uuid_module.UUID("87654321-4321-8765-4321-876543218765")

    result, read_converter = _write_and_read(
        {"ids": set[uuid_module.UUID]},
        [{"ids": {u1, u2}}],
        storage_backend,
        tmp_path,
    )

    field = result.schema.field("ids")
    assert hasattr(field.type, "extension_name")
    assert field.type.extension_name == "set[orcapod.uuid]"

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    assert isinstance(rows[0]["ids"], set)
    assert rows[0]["ids"] == {u1, u2}


def test_list_of_dataclass_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """list[_SimplePoint] round-trips — ListLogicalType wrapping DataclassLogicalType."""
    p1 = _SimplePoint(label="alpha", value=1)
    p2 = _SimplePoint(label="beta", value=2)

    result, read_converter = _write_and_read(
        {"points": list[_SimplePoint]},
        [{"points": [p1, p2]}],
        storage_backend,
        tmp_path,
    )

    field = result.schema.field("points")
    assert hasattr(field.type, "extension_name")
    assert field.type.extension_name.startswith("list[")

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    reconstructed = rows[0]["points"]
    assert len(reconstructed) == 2
    assert reconstructed[0] == p1
    assert reconstructed[1] == p2


def test_list_of_list_of_uuid_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """list[list[uuid.UUID]] round-trips — two-level nesting."""
    u1 = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    u2 = uuid_module.UUID("87654321-4321-8765-4321-876543218765")

    result, read_converter = _write_and_read(
        {"groups": list[list[uuid_module.UUID]]},
        [{"groups": [[u1, u2], [u2]]}],
        storage_backend,
        tmp_path,
    )

    field = result.schema.field("groups")
    assert hasattr(field.type, "extension_name")
    assert field.type.extension_name == "list[list[orcapod.uuid]]"

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    assert rows[0]["groups"] == [[u1, u2], [u2]]


def test_dataclass_with_list_uuid_field_round_trip(
    storage_backend: _StorageBackend, tmp_path: Path
) -> None:
    """Dataclass with list[uuid.UUID] field round-trips (previously broke DataclassLogicalTypeFactory)."""
    u1 = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    u2 = uuid_module.UUID("87654321-4321-8765-4321-876543218765")
    obj = _TaggedPoint(name="test", ids=[u1, u2])

    result, read_converter = _write_and_read(
        {"data": _TaggedPoint},
        [{"data": obj}],
        storage_backend,
        tmp_path,
    )

    field = result.schema.field("data")
    assert hasattr(field.type, "extension_name")

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    reconstructed = rows[0]["data"]
    assert isinstance(reconstructed, _TaggedPoint)
    assert reconstructed.name == "test"
    assert reconstructed.ids == [u1, u2]
    assert all(isinstance(v, uuid_module.UUID) for v in reconstructed.ids)


def test_list_of_int_produces_no_extension_type(tmp_path: Path) -> None:
    """list[int] must still produce plain large_list(int64) — no ListLogicalType wrapping."""
    import pyarrow as pa
    from orcapod.contexts import create_registry

    converter = create_registry().get_context().type_converter
    result = converter.register_python_class(list[int])

    assert not isinstance(result, pa.ExtensionType), (
        f"list[int] must not be wrapped as an extension type, got {result!r}"
    )
    assert pa.types.is_large_list(result)
    assert result.value_type == pa.int64()


def test_schema_round_trip_list_of_uuid(tmp_path: Path) -> None:
    """arrow_schema_to_python_schema then python_schema_to_arrow_schema is identity for list[UUID]."""
    import uuid
    import pyarrow as pa
    from orcapod.contexts import create_registry

    converter = create_registry().get_context().type_converter

    python_schema = {
        "ids": list[uuid.UUID],
        "groups": list[list[uuid.UUID]],
        "tag_set": set[uuid.UUID],
    }
    arrow_schema = converter.python_schema_to_arrow_schema(python_schema)

    recovered_python = converter.arrow_schema_to_python_schema(arrow_schema)
    assert recovered_python["ids"] == list[uuid.UUID]
    assert recovered_python["groups"] == list[list[uuid.UUID]]
    assert recovered_python["tag_set"] == set[uuid.UUID]

    arrow_schema2 = converter.python_schema_to_arrow_schema(recovered_python)
    assert arrow_schema2.field("ids").type.extension_name == "list[orcapod.uuid]"
    assert arrow_schema2.field("groups").type.extension_name == "list[list[orcapod.uuid]]"
    assert arrow_schema2.field("tag_set").type.extension_name == "set[orcapod.uuid]"


def test_python_type_property_list_and_set(tmp_path: Path) -> None:
    """ListLogicalType.python_type returns the exact generic alias."""
    import uuid
    from orcapod.logical_types.builtin_logical_types import LogicalUUID
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType

    lt_list = ListLogicalType(LogicalUUID(), is_set=False)
    assert lt_list.python_type == list[uuid.UUID]

    lt_set = ListLogicalType(LogicalUUID(), is_set=True)
    assert lt_set.python_type == set[uuid.UUID]


def test_fresh_converter_reads_list_of_uuid(
    storage_backend: _StorageBackend, tmp_path: Path
) -> None:
    """A fresh converter (no prior registration) can read list[UUID] via load_logical_types."""
    u1 = uuid_module.UUID("12345678-1234-5678-1234-567812345678")

    # Write with converter A
    write_converter = _fresh_converter()
    write_converter.register_python_class(list[uuid_module.UUID])
    arrow_schema = write_converter.python_schema_to_arrow_schema({"ids": list[uuid_module.UUID]})
    table = write_converter.python_dicts_to_arrow_table([{"ids": [u1]}], arrow_schema=arrow_schema)
    storage_backend.write(table, tmp_path)

    # Read with converter B — no prior registration; load_logical_types triggers factory
    read_converter = _fresh_converter()
    result = storage_backend.read(tmp_path, read_converter)

    field = result.schema.field("ids")
    assert hasattr(field.type, "extension_name")
    assert field.type.extension_name == "list[orcapod.uuid]"

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert rows[0]["ids"] == [u1]
    assert isinstance(rows[0]["ids"][0], uuid_module.UUID)


# ── set[T] native element write-path unit tests (ITL-611) ─────────────────────


def test_converter_set_of_int_produces_extension_type() -> None:
    """converter.python_type_to_arrow_type(set[int]) returns Arrow extension type 'set[int]'."""
    from orcapod.contexts import create_registry
    converter = create_registry().get_context().type_converter
    arrow_type = converter.python_type_to_arrow_type(set[int])
    assert isinstance(arrow_type, pa.ExtensionType), (
        f"Expected pa.ExtensionType for set[int], got {arrow_type!r}"
    )
    assert arrow_type.extension_name == "set[int]"


def test_converter_set_of_str_produces_extension_type() -> None:
    """converter.python_type_to_arrow_type(set[str]) returns Arrow extension type 'set[str]'."""
    from orcapod.contexts import create_registry
    converter = create_registry().get_context().type_converter
    arrow_type = converter.python_type_to_arrow_type(set[str])
    assert isinstance(arrow_type, pa.ExtensionType)
    assert arrow_type.extension_name == "set[str]"


def test_converter_list_of_int_unchanged_regression() -> None:
    """list[int] still produces plain large_list(int64) — no ListLogicalType wrapping (regression)."""
    from orcapod.contexts import create_registry
    converter = create_registry().get_context().type_converter
    result = converter.python_type_to_arrow_type(list[int])
    assert not isinstance(result, pa.ExtensionType), (
        f"list[int] must NOT be wrapped as extension type, got {result!r}"
    )
    assert pa.types.is_large_list(result)
    assert result.value_type == pa.int64()


def test_schema_round_trip_set_of_int() -> None:
    """arrow_schema_to_python_schema reconstructs set[int] (not list[int] or set[Any])."""
    from orcapod.contexts import create_registry
    converter = create_registry().get_context().type_converter
    python_schema = {"s": set[int]}
    arrow_schema = converter.python_schema_to_arrow_schema(python_schema)
    recovered = converter.arrow_schema_to_python_schema(arrow_schema)
    assert recovered["s"] == set[int], (
        f"Expected set[int], got {recovered['s']!r}"
    )


def test_schema_round_trip_set_of_str() -> None:
    """arrow_schema_to_python_schema reconstructs set[str]."""
    from orcapod.contexts import create_registry
    converter = create_registry().get_context().type_converter
    python_schema = {"tags": set[str]}
    arrow_schema = converter.python_schema_to_arrow_schema(python_schema)
    recovered = converter.arrow_schema_to_python_schema(arrow_schema)
    assert recovered["tags"] == set[str]


def test_explicit_native_list_construction() -> None:
    """ListLogicalType(int, is_set=False) builds a functional list[int] extension type."""
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(int, is_set=False)
    assert lt.logical_type_name == "list[int]"
    assert lt.python_type == list[int]
    storage = lt.python_to_storage([1, 2, 3], converter=None)
    assert storage == [1, 2, 3]
    result = lt.storage_to_python([1, 2, 3], converter=None)
    assert result == [1, 2, 3]


# ── set[T] native element full round-trip tests (ITL-611) ─────────────────────


def test_set_of_int_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """set[int] values round-trip as sets, not lists; extension name is 'set[int]'."""
    data = {1, 2, 3}
    result, read_converter = _write_and_read(
        {"s": set[int]},
        [{"s": data}],
        storage_backend,
        tmp_path,
    )
    field = result.schema.field("s")
    assert hasattr(field.type, "extension_name"), (
        f"Expected extension type on 's', got {field.type!r}"
    )
    assert field.type.extension_name == "set[int]"
    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    assert isinstance(rows[0]["s"], set), f"Expected set, got {type(rows[0]['s'])}"
    assert rows[0]["s"] == data


def test_set_of_str_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """set[str] values round-trip as sets; extension name is 'set[str]'."""
    data = {"alpha", "beta", "gamma"}
    result, read_converter = _write_and_read(
        {"tags": set[str]},
        [{"tags": data}],
        storage_backend,
        tmp_path,
    )
    field = result.schema.field("tags")
    assert hasattr(field.type, "extension_name")
    assert field.type.extension_name == "set[str]"
    rows = read_converter.arrow_table_to_python_dicts(result)
    assert isinstance(rows[0]["tags"], set)
    assert rows[0]["tags"] == data


def test_set_of_float_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """set[float] values round-trip as sets; extension name is 'set[float]'."""
    data = {1.0, 2.5, 3.14}
    result, read_converter = _write_and_read(
        {"values": set[float]},
        [{"values": data}],
        storage_backend,
        tmp_path,
    )
    field = result.schema.field("values")
    assert hasattr(field.type, "extension_name")
    assert field.type.extension_name == "set[float]"
    rows = read_converter.arrow_table_to_python_dicts(result)
    assert isinstance(rows[0]["values"], set)
    assert rows[0]["values"] == data


def test_set_of_bool_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """set[bool] values round-trip as sets; extension name is 'set[bool]'."""
    data = {True, False}
    result, read_converter = _write_and_read(
        {"flags": set[bool]},
        [{"flags": data}],
        storage_backend,
        tmp_path,
    )
    field = result.schema.field("flags")
    assert hasattr(field.type, "extension_name")
    assert field.type.extension_name == "set[bool]"
    rows = read_converter.arrow_table_to_python_dicts(result)
    assert isinstance(rows[0]["flags"], set)
    assert rows[0]["flags"] == data


def test_set_of_bytes_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """set[bytes] values round-trip as sets; extension name is 'set[bytes]'."""
    data = {b"foo", b"bar", b"baz"}
    result, read_converter = _write_and_read(
        {"blobs": set[bytes]},
        [{"blobs": data}],
        storage_backend,
        tmp_path,
    )
    field = result.schema.field("blobs")
    assert hasattr(field.type, "extension_name")
    assert field.type.extension_name == "set[bytes]"
    rows = read_converter.arrow_table_to_python_dicts(result)
    assert isinstance(rows[0]["blobs"], set)
    assert rows[0]["blobs"] == data


def test_set_of_datetime_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """set[datetime] values round-trip as sets of timezone-aware datetimes."""
    from datetime import datetime, timezone
    dt1 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    dt2 = datetime(2024, 6, 15, 12, 30, 0, tzinfo=timezone.utc)
    data = {dt1, dt2}
    result, read_converter = _write_and_read(
        {"timestamps": set[datetime]},
        [{"timestamps": data}],
        storage_backend,
        tmp_path,
    )
    field = result.schema.field("timestamps")
    assert hasattr(field.type, "extension_name")
    assert field.type.extension_name == "set[datetime]"
    rows = read_converter.arrow_table_to_python_dicts(result)
    assert isinstance(rows[0]["timestamps"], set)
    assert rows[0]["timestamps"] == data


def test_fresh_converter_reads_set_of_int(
    storage_backend: _StorageBackend, tmp_path: Path
) -> None:
    """A fresh converter (no prior registration) reconstructs set[int] via load_logical_types."""
    data = {1, 2, 3}

    # Write with converter A.
    write_converter = _fresh_converter()
    write_converter.register_python_class(set[int])
    arrow_schema = write_converter.python_schema_to_arrow_schema({"s": set[int]})
    table = write_converter.python_dicts_to_arrow_table([{"s": data}], arrow_schema=arrow_schema)
    storage_backend.write(table, tmp_path)

    # Read with converter B — no prior registration; load_logical_types triggers factory.
    read_converter = _fresh_converter()
    result = storage_backend.read(tmp_path, read_converter)

    field = result.schema.field("s")
    assert hasattr(field.type, "extension_name"), (
        f"Expected extension type after fresh-converter read, got {field.type!r}"
    )
    assert field.type.extension_name == "set[int]"

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert isinstance(rows[0]["s"], set)
    assert rows[0]["s"] == data
