"""Datagram mutations must keep known Arrow types (ITL-834).

An Arrow-backed datagram knows each column's exact Arrow type. Every mutator
must carry those types through unchanged, whether or not the Python dict cache
has been loaded, and infer types only for genuinely new columns. The rule:
whatever Arrow types the inputs have are what is respected; inferring an Arrow
type from Python values is reserved for when no Arrow type is known.

Meta columns have their own matrix in ``test_meta_arrow_types.py``.

Layout-only normalization (``string`` -> ``large_string`` and friends) happens
when a datagram is built from a table and does not change content hashes, so
expected types are the large-normalized input types.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_type_converter
from orcapod.core.datagrams import Data, Datagram, Tag
from orcapod.utils.arrow_utils import normalize_schema_to_large_types


def _typed_table() -> pa.Table:
    """One-row table covering narrow ints, floats, layouts, nesting and a logical type."""
    converter = get_default_type_converter()
    path_column = converter.python_dicts_to_arrow_table(
        [{"p": Path("/a")}], python_schema={"p": Path}
    ).column("p")
    struct_type = pa.struct([("x", pa.int8()), ("y", pa.string())])
    arrays: dict[str, pa.Array[Any] | pa.ChunkedArray[Any]] = {
        "i8": pa.array([1], pa.int8()),
        "i16": pa.array([2], pa.int16()),
        "i32": pa.array([3], pa.int32()),
        "u8": pa.array([4], pa.uint8()),
        "f32": pa.array([1.5], pa.float32()),
        "s": pa.array(["a"], pa.string()),
        "ls": pa.array(["b"], pa.large_string()),
        "bn": pa.array([b"x"], pa.binary()),
        "l8": pa.array([[1, 2]], pa.list_(pa.int8())),
        "st": pa.array([{"x": 1, "y": "a"}], struct_type),
        "p": path_column,
    }
    table = pa.table(list(arrays.values()), names=list(arrays))
    return table.cast(pa.schema([f.with_nullable(False) for f in table.schema]))


INPUT_TYPES = {
    f.name: f.type for f in normalize_schema_to_large_types(_typed_table().schema)
}
DATA_COLUMNS = list(INPUT_TYPES)


@dataclass(frozen=True)
class Mutation:
    """A datagram mutator and the input columns it legitimately changes or removes."""

    label: str
    apply: Callable[[Datagram], Datagram]
    changed: frozenset[str] = frozenset()


MUTATIONS = [
    Mutation("with_columns_inferred", lambda d: d.with_columns(new=3)),
    Mutation(
        "with_columns_python_typed",
        lambda d: d.with_columns(column_types={"new": float}, new=3),
    ),
    Mutation(
        "with_columns_arrow_typed",
        lambda d: d.with_columns(column_types={"new": pa.int8()}, new=3),
    ),
    Mutation("update", lambda d: d.update(i8=5, l8=[4], st={"x": 2, "y": "b"})),
    Mutation("drop", lambda d: d.drop("i16"), frozenset({"i16"})),
    Mutation("select", lambda d: d.select("i8", "f32", "l8", "st", "p")),
    Mutation("rename", lambda d: d.rename({"i8": "j8"}), frozenset({"i8"})),
    Mutation("with_meta_columns", lambda d: d.with_meta_columns(flag=True)),
    Mutation("drop_meta_columns", lambda d: d.drop_meta_columns("other")),
    Mutation("with_context_key", lambda d: d.with_context_key(d.data_context_key)),
    Mutation("copy_without_cache", lambda d: d.copy(include_cache=False)),
]
MUTATION = pytest.mark.parametrize(
    "mutation", MUTATIONS, ids=[m.label for m in MUTATIONS]
)
CLS = pytest.mark.parametrize(
    "cls", [Datagram, Tag, Data], ids=["datagram", "tag", "data"]
)
LOAD = pytest.mark.parametrize(
    "load_dict", [False, True], ids=["arrow_only", "dict_loaded"]
)


def _build(cls: type[Datagram], load_dict: bool) -> Datagram:
    """Build a datagram over the typed table, optionally loading its dict cache."""
    d = cls(_typed_table(), meta_info={"other": "x"})
    if load_dict:
        _ = d["i8"]
        _ = d.as_dict(all_info=True)
    return d


def _all_types(d: Datagram) -> dict[str, pa.DataType]:
    """Map every column of ``d`` (all info included) to its Arrow type."""
    return {f.name: f.type for f in d.as_table(all_info=True).schema}


@CLS
@MUTATION
@LOAD
def test_mutation_keeps_untouched_arrow_types(
    cls: type[Datagram], mutation: Mutation, load_dict: bool
) -> None:
    """Every column a mutator does not change keeps its exact input Arrow type."""
    out = mutation.apply(_build(cls, load_dict))
    types = _all_types(out)
    for name, expected in INPUT_TYPES.items():
        if name in mutation.changed or name not in types:
            continue
        assert types[name] == expected, name


@CLS
@MUTATION
def test_dict_cache_state_does_not_change_result(
    cls: type[Datagram], mutation: Mutation
) -> None:
    """A mutator gives the same table and hash whether or not the dict cache was loaded."""
    cold = mutation.apply(_build(cls, load_dict=False))
    warm = mutation.apply(_build(cls, load_dict=True))
    assert cold.as_table(all_info=True).schema == warm.as_table(all_info=True).schema
    assert cold.as_table(all_info=True).equals(warm.as_table(all_info=True))
    assert cold.content_hash() == warm.content_hash()


@CLS
@LOAD
def test_with_columns_keeps_existing_content_hash(
    cls: type[Datagram], load_dict: bool
) -> None:
    """Adding a column leaves the content hash of the existing columns unchanged."""
    d = _build(cls, load_dict)
    out = d.with_columns(new=3)
    assert (
        out.select(*DATA_COLUMNS).content_hash()
        == d.select(*DATA_COLUMNS).content_hash()
    )


@CLS
@LOAD
class TestNewColumnTypes:
    """Only genuinely new columns are typed: declared Arrow verbatim, else Python, else inferred."""

    def test_arrow_typed_new_column_used_verbatim(
        self, cls: type[Datagram], load_dict: bool
    ) -> None:
        """A declared ``pa.DataType`` is used verbatim and non-nullable for a non-null value."""
        out = _build(cls, load_dict).with_columns(column_types={"a": pa.int8()}, a=5)
        field = out.as_table().schema.field("a")
        assert field.type == pa.int8()
        assert not field.nullable
        assert out["a"] == 5
        assert out.schema()["a"] is int

    def test_arrow_field_used_verbatim_with_nullability(
        self, cls: type[Datagram], load_dict: bool
    ) -> None:
        """A declared ``pa.Field`` keeps its type and nullability; the key names it."""
        declared = pa.field("ignored", pa.int8(), nullable=True)
        out = _build(cls, load_dict).with_columns(column_types={"a": declared}, a=5)
        field = out.as_table().schema.field("a")
        assert field.type == pa.int8()
        assert field.nullable
        assert out["a"] == 5

    def test_nullable_field_schema_stable_across_values(
        self, cls: type[Datagram], load_dict: bool
    ) -> None:
        """A nullable ``pa.Field`` gives ``None`` and non-null rows one schema."""
        declared = pa.field("a", pa.int8(), nullable=True)
        tables = [
            _build(cls, load_dict)
            .with_columns(column_types={"a": declared}, a=value)
            .as_table()
            for value in (None, 5)
        ]
        assert tables[0].schema == tables[1].schema
        assert pa.concat_tables(tables).column("a").to_pylist() == [None, 5]

    def test_non_nullable_field_rejects_none(
        self, cls: type[Datagram], load_dict: bool
    ) -> None:
        """A ``None`` value for a non-nullable ``pa.Field`` raises ``ValueError``."""
        declared = pa.field("a", pa.int8(), nullable=False)
        with pytest.raises(ValueError, match="declared non-nullable"):
            _ = _build(cls, load_dict).with_columns(
                column_types={"a": declared}, a=None
            )

    def test_new_column_order_follows_updates_not_declarations(
        self, cls: type[Datagram], load_dict: bool
    ) -> None:
        """Columns follow ``updates`` order, however ``column_types`` is ordered."""
        out = _build(cls, load_dict).with_columns(
            column_types={"b": pa.int8(), "a": float}, a=1, b=2, c="x"
        )
        assert list(out.keys()) == [*DATA_COLUMNS, "a", "b", "c"]
        types = _all_types(out)
        assert types["a"] == pa.float64()
        assert types["b"] == pa.int8()
        assert types["c"] == pa.large_string()

    def test_python_typed_new_column_unchanged(
        self, cls: type[Datagram], load_dict: bool
    ) -> None:
        """A declared Python type is converted exactly as before."""
        out = _build(cls, load_dict).with_columns(column_types={"n": int}, n=3)
        field = out.as_table().schema.field("n")
        assert field.type == pa.int64()
        assert not field.nullable
        assert out.schema()["n"] is int

    def test_partial_column_types_keeps_all_new_columns(
        self, cls: type[Datagram], load_dict: bool
    ) -> None:
        """New columns missing from ``column_types`` are inferred, not dropped."""
        out = _build(cls, load_dict).with_columns(column_types={"a": float}, a=1, b="x")
        types = _all_types(out)
        assert types["a"] == pa.float64()
        assert types["b"] == pa.large_string()
        assert out["a"] == 1.0
        assert out["b"] == "x"

    def test_new_columns_appended_and_readable(
        self, cls: type[Datagram], load_dict: bool
    ) -> None:
        """New columns are appended after the existing ones and read back."""
        out = _build(cls, load_dict).with_columns(new=3)
        assert list(out.keys()) == [*DATA_COLUMNS, "new"]
        assert out["new"] == 3
        assert out["i8"] == 1


@LOAD
def test_update_values_and_types(load_dict: bool) -> None:
    """update() writes the new values into the existing Arrow fields."""
    out = _build(Datagram, load_dict).update(i8=7, l8=[3])
    assert out["i8"] == 7
    assert out["l8"] == [3]
    assert _all_types(out)["i8"] == pa.int8()
    assert _all_types(out)["l8"] == pa.large_list(pa.int8())


def test_dict_backed_with_arrow_column_type() -> None:
    """A dict-born datagram also honours an explicit Arrow type for a new column."""
    d = Datagram({"a": 1})
    out = d.with_columns(column_types={"b": pa.int16()}, b=2)
    assert {f.name: f.type for f in out.as_table().schema} == {
        "a": pa.int64(),
        "b": pa.int16(),
    }
    assert out.as_dict() == {"a": 1, "b": 2}


def test_dict_backed_with_columns_unchanged() -> None:
    """Dict-born datagrams keep their existing Python-schema behaviour."""
    d = Datagram({"a": 1, "s": "x"}, python_schema={"a": float, "s": str})
    out = d.with_columns(column_types={"c": int}, c=2, n=None)
    assert dict(out.schema()) == {"a": float, "s": str, "c": int, "n": str | None}
    assert out.as_table().schema.field("a").type == pa.float64()
