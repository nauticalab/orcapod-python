"""``arrow_schema()`` always describes the table ``as_table()`` returns (ITL-834).

A dict-born datagram caches its Arrow schema. Every mutator must invalidate or
carry that cache correctly, so the reported schema never lists stale columns or
misses new ones, for dict-born and Arrow-born datagrams alike.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pyarrow as pa
import pytest

from orcapod.core.datagrams import Data, Datagram, Tag


@dataclass(frozen=True)
class Mutation:
    """A datagram mutator under test."""

    label: str
    apply: Callable[[Datagram], Datagram]


MUTATIONS = [
    Mutation("with_columns", lambda d: d.with_columns(n=1)),
    Mutation(
        "with_columns_python_typed",
        lambda d: d.with_columns(column_types={"n": float}, n=1),
    ),
    Mutation("update", lambda d: d.update(a=5)),
    Mutation("select", lambda d: d.select("a")),
    Mutation("drop", lambda d: d.drop("b")),
    Mutation("rename", lambda d: d.rename({"a": "z"})),
    Mutation("with_meta_columns", lambda d: d.with_meta_columns(flag=True)),
    Mutation("drop_meta_columns", lambda d: d.drop_meta_columns("m")),
    Mutation("with_context_key", lambda d: d.with_context_key(d.data_context_key)),
    Mutation("copy_without_cache", lambda d: d.copy(include_cache=False)),
]


@pytest.mark.parametrize("cls", [Datagram, Tag, Data], ids=["datagram", "tag", "data"])
@pytest.mark.parametrize("mutation", MUTATIONS, ids=[m.label for m in MUTATIONS])
@pytest.mark.parametrize("arrow_born", [False, True], ids=["dict_born", "arrow_born"])
def test_arrow_schema_matches_table_after_mutation(
    cls: type[Datagram], mutation: Mutation, arrow_born: bool
) -> None:
    """After any mutator, ``arrow_schema()`` equals ``as_table().schema``."""
    if arrow_born:
        d = cls(pa.Table.from_pylist([{"a": 1, "b": "x"}]), meta_info={"__m": "v"})
    else:
        d = cls({"a": 1, "b": "x"}, meta_info={"__m": "v"})
    _ = d.arrow_schema(all_info=True)  # populate the cached schema
    out = mutation.apply(d)
    assert out.arrow_schema(all_info=True) == out.as_table(all_info=True).schema
    assert out.arrow_schema() == out.as_table().schema
