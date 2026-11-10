"""Meta columns keep their Arrow types through every datagram mutator (ITL-834).

The meta Arrow table of an Arrow-born datagram carries each meta column's exact
Arrow type. No mutator may rebuild it from Python types: untouched meta columns
keep their Arrow fields verbatim, and only a meta column being set is (re)typed
from its new value.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pyarrow as pa
import pytest

from orcapod.core.datagrams import Data, Datagram, Tag

META_TYPES = {
    "__i8": pa.int8(),
    "__f32": pa.float32(),
    "__ts": pa.timestamp("ms", tz="UTC"),
}


@dataclass(frozen=True)
class Mutation:
    """A datagram mutator under test."""

    label: str
    apply: Callable[[Datagram], Datagram]


MUTATIONS = [
    Mutation("with_columns", lambda d: d.with_columns(new=3)),
    Mutation(
        "with_columns_python_typed",
        lambda d: d.with_columns(column_types={"n": int}, n=3),
    ),
    Mutation("update", lambda d: d.update(a=5)),
    Mutation("drop", lambda d: d.drop("b")),
    Mutation("select", lambda d: d.select("a")),
    Mutation("rename", lambda d: d.rename({"a": "z"})),
    Mutation("with_meta_columns", lambda d: d.with_meta_columns(flag=True)),
    Mutation("drop_meta_columns", lambda d: d.drop_meta_columns("other")),
    Mutation("with_context_key", lambda d: d.with_context_key(d.data_context_key)),
    Mutation("copy_without_cache", lambda d: d.copy(include_cache=False)),
]
CLS = pytest.mark.parametrize(
    "cls", [Datagram, Tag, Data], ids=["datagram", "tag", "data"]
)


def _build(cls: type[Datagram], load_dict: bool) -> Datagram:
    """Build a datagram with narrow meta columns, optionally loading its dict cache."""
    table = pa.table(
        [
            pa.array([1], pa.int8()),
            pa.array(["x"], pa.large_string()),
            pa.array([7], pa.int8()),
            pa.array([1.5], pa.float32()),
            pa.array([1_700_000_000_000], pa.timestamp("ms", tz="UTC")),
        ],
        names=["a", "b", *META_TYPES],
    )
    d = cls(table, meta_info={"other": "x"})
    if load_dict:
        _ = d["a"]
        _ = d.as_dict(all_info=True)
    return d


def _meta_types(d: Datagram) -> dict[str, pa.DataType]:
    """Map each meta column of ``d`` to its Arrow type."""
    return {
        f.name: f.type
        for f in d.as_table(all_info=True).schema
        if f.name.startswith("__")
    }


@CLS
@pytest.mark.parametrize("mutation", MUTATIONS, ids=[m.label for m in MUTATIONS])
@pytest.mark.parametrize("load_dict", [False, True], ids=["arrow_only", "dict_loaded"])
def test_meta_columns_keep_arrow_types(
    cls: type[Datagram], mutation: Mutation, load_dict: bool
) -> None:
    """Untouched meta columns keep their exact Arrow types through every mutator."""
    out = mutation.apply(_build(cls, load_dict))
    types = _meta_types(out)
    for name, expected in META_TYPES.items():
        assert types[name] == expected, name
    assert out.get_meta_value("i8") == 7


@CLS
def test_meta_overwrite_retypes_only_that_column(cls: type[Datagram]) -> None:
    """Overwriting a meta column re-types it from its new value; others keep their type."""
    d = _build(cls, load_dict=False).with_meta_columns(other=1)
    types = _meta_types(d)
    assert types["__other"] == pa.int64()
    assert types["__i8"] == pa.int8()
    assert d.get_meta_value("other") == 1


@CLS
def test_dropped_meta_column_is_gone(cls: type[Datagram]) -> None:
    """Dropping a meta column removes it from both the dict and the Arrow table."""
    d = _build(cls, load_dict=False).drop_meta_columns("i8")
    assert "__i8" not in _meta_types(d)
    assert "__i8" not in d.get_meta_info()
    assert _meta_types(d)["__f32"] == pa.float32()
