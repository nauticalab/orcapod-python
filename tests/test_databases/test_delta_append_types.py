"""Appending rows with the stored schema succeeds for every type Delta can store.

ITL-884 added a check that refuses an append whose column types differ from the
stored table's. It twice refused appends that ``main`` accepted (a new nested
struct field, then non-microsecond timestamps), so this differential guard writes
one row of each Arrow type, appends a second row with the IDENTICAL schema, and
reads both back: once on the same database view and once after reopening the
store. A type the Delta writer cannot store at all is listed in
``NOT_STORABLE`` and must still fail on the first write, so the list stays true
as ``deltalake`` changes.
"""

from __future__ import annotations

import datetime
import decimal
import pathlib
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from upath import UPath

from orcapod.contexts import get_default_context
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import InputValidationError
from orcapod.logical_types.directory_type import Directory
from orcapod.logical_types.file_type import File

PATH = ("types", "t")


@dataclass
class Point:
    """A dataclass output type, stored as a logical (extension) type."""

    x: int
    y: float


def _converter() -> Any:
    """The default data context's type converter."""
    return get_default_context().type_converter


def _logical(python_type: Any) -> pa.DataType:
    """The Arrow type Orcapod stores ``python_type`` as."""
    converter = _converter()
    if python_type is Point:
        # Dataclasses are logical types once registered with the converter.
        converter.register_python_class(Point)
    return converter.python_type_to_arrow_type(python_type)


def sample(arrow_type: pa.DataType) -> Any:
    """Return one valid Python value of ``arrow_type`` (its storage, for extensions)."""
    if isinstance(arrow_type, pa.BaseExtensionType):
        return sample(arrow_type.storage_type)
    if pa.types.is_boolean(arrow_type):
        return True
    if pa.types.is_integer(arrow_type):
        return 1
    if pa.types.is_floating(arrow_type):
        return 1.5
    if pa.types.is_decimal(arrow_type):
        return decimal.Decimal("1.5")
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "a"
    if pa.types.is_fixed_size_binary(arrow_type):
        return b"a" * arrow_type.byte_width
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return b"a"
    if pa.types.is_date(arrow_type):
        return datetime.date(2020, 1, 2)
    if pa.types.is_time(arrow_type):
        return datetime.time(1, 2, 3)
    if pa.types.is_timestamp(arrow_type):
        return datetime.datetime(2020, 1, 2, 3, 4, 5)
    if pa.types.is_duration(arrow_type):
        return datetime.timedelta(seconds=3)
    if pa.types.is_dictionary(arrow_type):
        return sample(arrow_type.value_type)
    if pa.types.is_fixed_size_list(arrow_type):
        return [sample(arrow_type.value_type)] * arrow_type.list_size
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return [sample(arrow_type.value_type)]
    if pa.types.is_map(arrow_type):
        return [(sample(arrow_type.key_type), sample(arrow_type.item_type))]
    if pa.types.is_struct(arrow_type):
        return {f.name: sample(f.type) for f in arrow_type}
    raise NotImplementedError(arrow_type)


def column(arrow_type: pa.DataType) -> pa.Array[Any]:
    """A one-row array of ``arrow_type``."""
    if isinstance(arrow_type, pa.BaseExtensionType):
        return pa.ExtensionArray.from_storage(
            arrow_type, column(arrow_type.storage_type)
        )
    if pa.types.is_dictionary(arrow_type):
        return pa.array([sample(arrow_type)], arrow_type.value_type).dictionary_encode()
    return pa.array([sample(arrow_type)], arrow_type)


PLAIN_TYPES: dict[str, pa.DataType] = {
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "float16": pa.float16(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "bool": pa.bool_(),
    "string": pa.string(),
    "large_string": pa.large_string(),
    "binary": pa.binary(),
    "large_binary": pa.large_binary(),
    "fixed_size_binary": pa.binary(4),
    "date32": pa.date32(),
    "date64": pa.date64(),
    "time32[s]": pa.time32("s"),
    "time32[ms]": pa.time32("ms"),
    "time64[us]": pa.time64("us"),
    "time64[ns]": pa.time64("ns"),
    **{f"timestamp[{u}]": pa.timestamp(u) for u in ("s", "ms", "us", "ns")},
    **{f"timestamp[{u}, UTC]": pa.timestamp(u, "UTC") for u in ("s", "ms", "us", "ns")},
    "timestamp[us, Europe/Paris]": pa.timestamp("us", "Europe/Paris"),
    **{f"duration[{u}]": pa.duration(u) for u in ("s", "ms", "us", "ns")},
    "decimal128(10, 2)": pa.decimal128(10, 2),
    "decimal128(38, 10)": pa.decimal128(38, 10),
    "decimal256(40, 2)": pa.decimal256(40, 2),
    "list<int64>": pa.list_(pa.int64()),
    "large_list<int64>": pa.large_list(pa.int64()),
    "fixed_size_list<int64, 2>": pa.list_(pa.int64(), 2),
    "list<non-null int64>": pa.list_(pa.field("item", pa.int64(), nullable=False)),
    "struct<a: int64, b: string>": pa.struct([("a", pa.int64()), ("b", pa.string())]),
    "struct<a: non-null int64>": pa.struct([pa.field("a", pa.int64(), nullable=False)]),
    "map<string, int64>": pa.map_(pa.string(), pa.int64()),
    "dictionary<int32, string>": pa.dictionary(pa.int32(), pa.string()),
    # Nested combinations, with the units the Delta writer converts.
    "struct<t: timestamp[ns], u: uint32>": pa.struct(
        [("t", pa.timestamp("ns")), ("u", pa.uint32())]
    ),
    "large_list<struct<t: timestamp[ms, UTC]>>": pa.large_list(
        pa.struct([("t", pa.timestamp("ms", "UTC"))])
    ),
    "list<large_list<uint64>>": pa.list_(pa.large_list(pa.uint64())),
    "struct<l: list<struct<a: binary(3)>>>": pa.struct(
        [("l", pa.list_(pa.struct([("a", pa.binary(3))])))]
    ),
}

LOGICAL_TYPES: dict[str, Any] = {
    "logical Path": pathlib.Path,
    "logical UPath": UPath,
    "logical UUID": uuid.UUID,
    "logical File": File,
    "logical Directory": Directory,
    "logical numpy.ndarray": np.ndarray,
    "logical pandas.DataFrame": pd.DataFrame,
    "logical pandas.Series": pd.Series,
    "logical dataclass": Point,
    "logical list[Path]": list[pathlib.Path],
}

# Types the Delta writer cannot store at all (deltalake 1.0.2): the first write
# fails, before the append check is ever reached, on main as on this branch. Kept
# explicit so a deltalake upgrade that starts storing one of them makes the test
# fail and the type join the guarded list.
NOT_STORABLE: set[str] = {
    "float16",
    "decimal256(40, 2)",
}
# ``date64``, ``time`` and ``duration`` are stored as their integer value with the
# Arrow type recorded since ITL-928 (``orcapod.databases.delta_arrow_types``).

# Types Delta stores but whose append Delta's own merge rejects, on main as on
# this branch (not the append check): tracked in ITL-916.
MERGE_REJECTS: set[str] = {"fixed_size_list<int64, 2>"}


def table(arrow_type: pa.DataType, rid: bytes, *, nullable: bool) -> pa.Table:
    """A one-row table keyed by ``rid`` with an ``x`` column of ``arrow_type``."""
    schema = pa.schema(
        [
            pa.field("rid", pa.large_binary(), nullable=False),
            pa.field("x", arrow_type, nullable=nullable),
        ]
    )
    return pa.Table.from_arrays(
        [pa.array([rid], pa.large_binary()), column(arrow_type)], schema=schema
    )


def all_types() -> list[Any]:
    """Every (id, Arrow type) pair, nullable and non-nullable."""
    types = dict(PLAIN_TYPES)
    types.update({name: _logical(t) for name, t in LOGICAL_TYPES.items()})
    merge_rejects = pytest.mark.xfail(
        raises=RuntimeError,
        strict=True,
        reason="Delta's merge rejects this type, also on main (ITL-916)",
    )
    return [
        pytest.param(
            name,
            arrow_type,
            nullable,
            id=f"{name}-{'nullable' if nullable else 'non-null'}",
            marks=[merge_rejects] if name in MERGE_REJECTS else [],
        )
        for name, arrow_type in types.items()
        for nullable in (True, False)
    ]


def write(db: DeltaTableDatabase, rows: pa.Table) -> None:
    """Add ``rows`` at ``PATH`` and flush them."""
    db.add_records(PATH, rows, record_id_column="rid", flush=True)


@pytest.mark.parametrize(("name", "arrow_type", "nullable"), all_types())
@pytest.mark.parametrize("reopen", [False, True], ids=["same-view", "reopened"])
def test_identical_schema_appends(
    tmp_path: Path, name: str, arrow_type: pa.DataType, nullable: bool, reopen: bool
) -> None:
    """A second batch with the stored batch's exact schema appends and reads back."""
    db = DeltaTableDatabase(base_path=tmp_path)
    if name in NOT_STORABLE:
        # Delta cannot store the type: the first write already fails.
        with pytest.raises(RuntimeError):
            write(db, table(arrow_type, b"a", nullable=nullable))
        return
    write(db, table(arrow_type, b"a", nullable=nullable))

    if reopen:
        db = DeltaTableDatabase(base_path=tmp_path)
    write(db, table(arrow_type, b"b", nullable=nullable))

    back = DeltaTableDatabase(base_path=tmp_path).get_all_records(
        PATH, record_id_column="rid"
    )
    assert back is not None
    assert back.num_rows == 2
    assert set(back.column("rid").to_pylist()) == {b"a", b"b"}


@pytest.mark.parametrize(
    ("stored_type", "incoming_type", "value"),
    [
        (pa.int64(), pa.null(), None),
        (pa.struct([("a", pa.int64())]), pa.struct([("a", pa.null())]), {"a": None}),
        (pa.large_list(pa.int64()), pa.large_list(pa.null()), [None]),
        # A null field inside list<struct> is not here: Delta's own merge cannot
        # coerce it (also on main), whatever the append check decides.
    ],
    ids=[
        "int64<-null",
        "struct-field<-null",
        "list-item<-null",
    ],
)
def test_null_typed_values_append_to_a_typed_column(
    tmp_path: Path, stored_type: pa.DataType, incoming_type: pa.DataType, value: Any
) -> None:
    """Rows whose column (or nested field) is ``null``-typed append to a typed column."""
    db = DeltaTableDatabase(base_path=tmp_path)
    write(db, table(stored_type, b"a", nullable=True))
    nulls = pa.Table.from_arrays(
        [pa.array([b"b"], pa.large_binary()), pa.array([value], incoming_type)],
        names=["rid", "x"],
    )
    write(db, nulls)

    back = DeltaTableDatabase(base_path=tmp_path).get_all_records(
        PATH, record_id_column="rid"
    )
    assert back is not None
    assert back.num_rows == 2
    rows = dict(zip(back.column("rid").to_pylist(), back.column("x").to_pylist()))
    assert rows[b"b"] == value


@pytest.mark.parametrize(
    ("stored_type", "incoming_type", "value"),
    [
        (
            pa.struct([("a", pa.int64()), ("b", pa.int64())]),
            pa.struct([("a", pa.null()), ("b", pa.float64())]),
            {"a": None, "b": 2.5},
        ),
        (
            pa.large_list(pa.struct([("a", pa.int64()), ("b", pa.int64())])),
            pa.large_list(pa.struct([("a", pa.null()), ("b", pa.float64())])),
            [{"a": None, "b": 2.5}],
        ),
    ],
    ids=["struct", "list<struct>"],
)
def test_null_field_does_not_hide_a_changed_sibling(
    tmp_path: Path, stored_type: pa.DataType, incoming_type: pa.DataType, value: Any
) -> None:
    """A ``null`` struct field is skipped, but a changed sibling field is refused."""
    db = DeltaTableDatabase(base_path=tmp_path)
    write(db, table(stored_type, b"a", nullable=True))
    changed = pa.Table.from_arrays(
        [pa.array([b"b"], pa.large_binary()), pa.array([value], incoming_type)],
        names=["rid", "x"],
    )
    with pytest.raises(InputValidationError, match="'x'") as info:
        write(db, changed)
    assert "int64" in str(info.value)
    assert "double" in str(info.value)
