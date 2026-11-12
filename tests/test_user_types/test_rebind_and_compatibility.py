"""Rebound user classes in containers, in unions, and foreign storage (ITL-881).

Two classes declared under one name with the same fields have one identity; the second
definition rebinds it, as a re-run notebook cell does. These tests cover what follows a
rebind: the schema of a list of the class, a pod parameter typed as a union with the
class, and storage that differs from the registered class. Every test declares names under
``itl972.rebind.`` that no other test uses, because declarations are process-wide.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

import orcapod as op
from orcapod import function_pod
from orcapod.errors import TypeIdentityError
from orcapod.sources import ArrowTableSource, DictSource
from tests.test_user_types.child_harness import run_child


def _source(key: str, value: Any) -> DictSource:
    """Return a one-row ``DictSource`` holding ``value`` in data column ``key``."""
    return DictSource(data=[{"k": "a", key: value}], tag_columns=["k"])


def _read(source: Any, key: str) -> Any:
    """Return the value of data column ``key`` in the source's only row."""
    return [data.as_dict()[key] for _, data in source.iter_data()][0]


# ---------------------------------------------------------------------------
# A list of a rebound class
# ---------------------------------------------------------------------------


@op.dataclass(name="itl972.rebind.Point", frozen=True)
class PointV1:
    """First definition under ``itl972.rebind.Point``."""

    x: int


@op.dataclass(name="itl972.rebind.Point", frozen=True)
class PointV2:
    """Second definition under ``itl972.rebind.Point``, with the same fields."""

    x: int


def test_list_schema_names_the_rebound_class() -> None:
    """After a rebind, a list column's schema and values use the new class."""
    first = _source("ps", [PointV1(1)])
    stored = first.as_table()
    assert first.output_schema()[1]["ps"] == list[PointV1]

    second = _source("ps", [PointV2(1)])
    assert second.output_schema()[1]["ps"] == list[PointV2]
    assert _read(second, "ps") == [PointV2(1)]

    # A table written before the rebind reads back as the new class as well.
    reread = ArrowTableSource(stored.select(["k", "ps"]), tag_columns=["k"])
    assert reread.output_schema()[1]["ps"] == list[PointV2]
    assert type(_read(reread, "ps")[0]) is PointV2


@op.dataclass(name="itl972.rebind.Cell", frozen=True)
class CellV1:
    """First definition under ``itl972.rebind.Cell``."""

    x: int


@op.dataclass(name="itl972.rebind.Cell", frozen=True)
class CellV2:
    """Second definition under ``itl972.rebind.Cell``, with the same fields."""

    x: int


def test_nested_list_schema_names_the_rebound_class() -> None:
    """A list of lists follows the rebind of its innermost class."""
    first = _source("grid", [[CellV1(1)]])
    assert first.output_schema()[1]["grid"] == list[list[CellV1]]
    second = _source("grid", [[CellV2(1)]])
    assert second.output_schema()[1]["grid"] == list[list[CellV2]]
    assert _read(second, "grid") == [[CellV2(1)]]


# ---------------------------------------------------------------------------
# An identity-equivalent class offered to a union parameter
# ---------------------------------------------------------------------------


@op.dataclass(name="itl972.rebind.Reading")
class ReadingOld:
    """First definition under ``itl972.rebind.Reading``."""

    x: int


@op.dataclass(name="itl972.rebind.Reading")
class ReadingNew:
    """Second definition under ``itl972.rebind.Reading``, with the same fields."""

    x: int


@function_pod(output_keys="p")
def make_old(n: int) -> ReadingOld:
    """Return a ``ReadingOld`` holding ``n``."""
    return ReadingOld(n)


@function_pod(output_keys="y")
def read_optional(p: ReadingNew | None) -> int:
    """Return the reading's ``x``, or ``-1`` for no reading."""
    return -1 if p is None else p.x


@function_pod(output_keys="y")
def read_int_or_str(p: int | str) -> int:
    """Return ``0``; only the parameter type matters."""
    return 0


def test_old_class_output_feeds_a_union_parameter_of_the_new_class() -> None:
    """A pod output typed as the old class is accepted by ``New | None``."""
    source = DictSource(data=[{"k": "a", "n": 3}], tag_columns=["k"])
    out = read_optional.pod(make_old.pod(source))
    assert _read(out, "y") == 3


def test_union_parameter_still_rejects_another_type() -> None:
    """A union parameter with no member matching the column is still refused."""
    source = DictSource(data=[{"k": "a", "n": 3}], tag_columns=["k"])
    with pytest.raises(ValueError, match="column 'p' has type"):
        read_int_or_str.pod(make_old.pod(source))


# ---------------------------------------------------------------------------
# Storage that differs from the registered class
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class Named:
    """An ordinary dataclass with a string field."""

    label: str
    n: int


def _struct(label_type: pa.DataType) -> pa.DataType:
    """Return ``Named``'s storage struct with ``label`` of ``label_type``."""
    return pa.struct([pa.field("label", label_type), pa.field("n", pa.int64())])


def test_null_storage_is_not_the_registered_string_storage() -> None:
    """A ``null`` field stored where the class has a string raises ``TypeIdentityError``."""
    ext = _source("p", Named("x", 1)).as_table().schema.field("p").type
    metadata = ext.__arrow_ext_serialize__()
    # Non-public entry point: pyarrow's extension hook. No public path produces this
    # input (Delta refuses a null column; Parquet does not rebuild the extension type).
    deserialize = type(ext).__arrow_ext_deserialize__
    # A layout variant of the same type is still accepted (Delta compaction).
    assert deserialize(_struct(pa.string()), metadata).storage_type == ext.storage_type
    with pytest.raises(TypeIdentityError, match="expected storage_type"):
        deserialize(_struct(pa.null()), metadata)


# ---------------------------------------------------------------------------
# The child-process harness
# ---------------------------------------------------------------------------


def test_run_child_rejects_a_report_from_a_failed_child(tmp_path: Path) -> None:
    """A child that reports and then exits non-zero is a failure, not a result."""
    code = "print('JSON:1', flush=True); raise SystemExit(3)"
    with pytest.raises(AssertionError, match="exit 3"):
        run_child(["-c", code], cwd=tmp_path)
    ok = "print('JSON:1', flush=True)"
    assert run_child(["-c", ok], cwd=tmp_path) == 1
