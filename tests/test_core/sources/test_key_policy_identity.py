"""A source's key policy is part of its identity (developer, November 16, 2026, ITL-1154).

Two sources that differ only in ``record_id_columns`` name their rows differently, so
they are two sources: different content hashes (and so different cache tables), the
same pipeline hash, and the same source id, given or default.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.sources import (
    ArrowTableSource,
    CachedSource,
    DataFrameSource,
    DerivedSource,
    DictSource,
    ListSource,
    SQLiteTableSource,
)
from orcapod.databases import InMemoryArrowDatabase

_TABLE = pa.table({"k": ["a", "b"], "v": [1, 2]})

Make = Callable[[Path, Any], Any]


def _arrow(_tmp: Path, key: Any) -> Any:
    return ArrowTableSource(
        _TABLE, tag_columns=["k"], record_id_columns=key, tag_only_columns=True
    )


def _dict(_tmp: Path, key: Any) -> Any:
    return DictSource(
        _TABLE.to_pylist(),
        tag_columns=["k"],
        record_id_columns=key,
        tag_only_columns=True,
    )


def _data_frame(_tmp: Path, key: Any) -> Any:
    return DataFrameSource(
        _TABLE.to_pandas(),
        tag_columns=["k"],
        record_id_columns=key,
        tag_only_columns=True,
    )


def _list(_tmp: Path, key: Any) -> Any:
    return ListSource(
        "v", [1, 2], record_id_columns=None if key is None else "element_index"
    )


def _derived(_tmp: Path, key: Any) -> Any:
    origin = ArrowTableSource(
        _TABLE, tag_columns=["k"], source_id="origin", tag_only_columns=True
    )
    return DerivedSource(
        origin, tags_as_data={"k": "k_value"}, source_id="d", record_id_columns=key
    )


def _sqlite(tmp: Path, key: Any) -> Any:
    path = tmp / "keys.sqlite"
    if not path.exists():
        with sqlite3.connect(path) as conn:
            _ = conn.execute("CREATE TABLE t (k TEXT PRIMARY KEY, v INTEGER)")
            _ = conn.executemany("INSERT INTO t VALUES (?, ?)", [("a", 1), ("b", 2)])
    # None: the primary key; () when the key is asked for: the position.
    return SQLiteTableSource(
        path, "t", record_id_columns=() if key is None else None, tag_only_columns=True
    )


SOURCES = {
    "arrow_table": _arrow,
    "dict": _dict,
    "data_frame": _data_frame,
    "list": _list,
    "derived_tags_as_data": _derived,
    "sqlite_table": _sqlite,
}


@pytest.mark.parametrize("make", SOURCES.values(), ids=SOURCES.keys())
def test_two_key_policies_are_two_sources_with_one_pipeline(
    make: Make, tmp_path: Path
) -> None:
    by_position = make(tmp_path, None)
    by_key = make(tmp_path, "k")
    assert by_position.content_hash() != by_key.content_hash()
    assert by_position.pipeline_hash() == by_key.pipeline_hash()
    # The key policy names rows; it is not part of the source's name.
    assert by_position.source_id == by_key.source_id


def test_a_cached_source_takes_its_sources_identity() -> None:
    source = ArrowTableSource(
        _TABLE,
        tag_columns=["k"],
        source_id="s",
        record_id_columns="k",
        tag_only_columns=True,
    )
    cached = CachedSource(source, cache_database=InMemoryArrowDatabase())
    assert cached.content_hash() == source.content_hash()
