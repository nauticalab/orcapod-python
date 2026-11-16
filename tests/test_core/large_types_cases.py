"""Shared cases for the large-type tests (ITL-1122): every source kind, built from non-large data.

The rule (developer, November 14, 2026): every column of every table Orcapod presents or
stores has its string, binary and list types in their large variants, recursively, with no
exception for a user's Arrow table or an explicit Arrow schema. These helpers build each
source kind from data that holds ``string``, ``binary``, ``list<string>`` and a nested
``struct<list<string>, string>`` wherever the kind can carry them, and find any non-large
type in a table by walking its Arrow types.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any

import deltalake
import polars as pl
import pyarrow as pa
import pyarrow.csv as pa_csv

from orcapod.core.sources import (
    ArrowTableSource,
    CachedSource,
    CSVSource,
    DataFrameSource,
    DeltaTableSource,
    DictSource,
    SQLiteTableSource,
)
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.protocols.core_protocols import StreamProtocol

ALL_COLUMNS = {"source": True, "system_tags": True, "context": True}

NESTED = pa.struct([pa.field("x", pa.list_(pa.string())), pa.field("y", pa.string())])
FULL_SCHEMA = pa.schema(
    [
        pa.field("t", pa.string(), nullable=False),
        pa.field("k", pa.int64(), nullable=False),
        pa.field("s", pa.string(), nullable=False),
        pa.field("u", pa.string(), nullable=False),
        pa.field("b", pa.binary(), nullable=False),
        pa.field("l", pa.list_(pa.string()), nullable=False),
        pa.field("n", NESTED, nullable=False),
    ]
)
ROWS: list[dict[str, Any]] = [
    {
        "t": "a",
        "k": 0,
        "s": "x",
        "u": "p",
        "b": b"1",
        "l": ["p"],
        "n": {"x": ["q"], "y": "r"},
    },
    {
        "t": "b",
        "k": 1,
        "s": "y",
        "u": "q",
        "b": b"2",
        "l": ["p", "q"],
        "n": {"x": [], "y": "s"},
    },
]
TAGS = ["t", "k"]


def non_large_paths(arrow_type: pa.DataType, path: str) -> list[str]:
    """Name every place in ``arrow_type`` that holds a non-large string, binary or list.

    Walks list items, struct children, map keys and items, dictionary values and
    extension storage, so a ``string`` nested at any depth is found. Written without
    ``normalize_to_large_types``, so it checks that function's work rather than repeat it.

    Args:
        arrow_type: The type to walk.
        path: The name of the place ``arrow_type`` is at, for the result.

    Returns:
        ``"<path>: <type>"`` for each non-large place, outermost first.
    """
    found: list[str] = []
    if pa.types.is_string(arrow_type) or pa.types.is_binary(arrow_type):
        found.append(f"{path}: {arrow_type}")
    elif pa.types.is_list(arrow_type):
        found.append(f"{path}: {arrow_type}")
        found += non_large_paths(arrow_type.value_type, f"{path}[]")
    elif pa.types.is_large_list(arrow_type) or pa.types.is_fixed_size_list(arrow_type):
        found += non_large_paths(arrow_type.value_type, f"{path}[]")
    elif pa.types.is_struct(arrow_type):
        for field in arrow_type:
            found += non_large_paths(field.type, f"{path}.{field.name}")
    elif pa.types.is_map(arrow_type):
        found += non_large_paths(arrow_type.key_type, f"{path}{{key}}")
        found += non_large_paths(arrow_type.item_type, f"{path}{{item}}")
    elif pa.types.is_dictionary(arrow_type):
        found += non_large_paths(arrow_type.value_type, f"{path}{{value}}")
    elif isinstance(arrow_type, pa.ExtensionType):
        found += non_large_paths(arrow_type.storage_type, f"{path}<storage>")
    return found


def non_large_columns(table: pa.Table) -> list[str]:
    """Every non-large place in every column of ``table`` (``non_large_paths``)."""
    found: list[str] = []
    for field in table.schema:
        found += non_large_paths(field.type, field.name)
    return found


def _rows(n_rows: int, columns: list[str]) -> list[dict[str, Any]]:
    return [{name: row[name] for name in columns} for row in ROWS[:n_rows]]


def _table(n_rows: int, columns: list[str] | None = None) -> pa.Table:
    """The rows as a user's Arrow table in the non-large types of ``FULL_SCHEMA``."""
    names = columns or FULL_SCHEMA.names
    schema = pa.schema([FULL_SCHEMA.field(name) for name in names])
    return pa.Table.from_pylist(_rows(n_rows, names), schema=schema)


SourceFactory = Callable[[int, Path], StreamProtocol]


def _arrow_table_source(n_rows: int, tmp: Path) -> StreamProtocol:
    return ArrowTableSource(
        _table(n_rows), tag_columns=TAGS, tag_only_columns=True, source_id="arrow"
    )


def _arrow_table_stream(n_rows: int, tmp: Path) -> StreamProtocol:
    return ArrowTableStream(_table(n_rows), tag_columns=TAGS)


def _dict_source_python(n_rows: int, tmp: Path) -> StreamProtocol:
    columns = ["t", "k", "s", "u", "b", "l"]
    if n_rows == 0:
        return DictSource(
            [],
            tag_columns=TAGS,
            tag_only_columns=True,
            data_schema={
                "t": str,
                "k": int,
                "s": str,
                "u": str,
                "b": bytes,
                "l": list[str],
            },
            source_id="dict",
        )
    return DictSource(
        _rows(n_rows, columns),
        tag_columns=TAGS,
        tag_only_columns=True,
        source_id="dict",
    )


def _dict_source_arrow_schema(n_rows: int, tmp: Path) -> StreamProtocol:
    return DictSource(
        _rows(n_rows, FULL_SCHEMA.names),
        tag_columns=TAGS,
        tag_only_columns=True,
        data_schema=FULL_SCHEMA,
        source_id="dict_schema",
    )


def _csv_path(n_rows: int, tmp: Path) -> Path:
    path = tmp / f"rows{n_rows}.csv"
    pa_csv.write_csv(_table(n_rows, ["t", "k", "s", "u"]), path)
    return path


def _csv_source(n_rows: int, tmp: Path) -> StreamProtocol:
    return CSVSource(
        str(_csv_path(n_rows, tmp)),
        tag_columns=TAGS,
        tag_only_columns=True,
        source_id="csv",
    )


def _csv_source_arrow_schema(n_rows: int, tmp: Path) -> StreamProtocol:
    return CSVSource(
        str(_csv_path(n_rows, tmp)),
        tag_columns=TAGS,
        tag_only_columns=True,
        source_id="csv_schema",
        schema=pa.schema([FULL_SCHEMA.field(name) for name in ["t", "k", "s", "u"]]),
    )


def _data_frame_source_polars(n_rows: int, tmp: Path) -> StreamProtocol:
    return DataFrameSource(
        pl.from_arrow(_table(n_rows)),
        tag_columns=TAGS,
        tag_only_columns=True,
        source_id="polars",
    )


def _data_frame_source_pandas(n_rows: int, tmp: Path) -> StreamProtocol:
    # No bytes column: an empty pandas object column has no values to tell bytes
    # from str, so a zero-row frame could not give the populated frame's type.
    return DataFrameSource(
        _table(n_rows, ["t", "k", "s", "u"]).to_pandas(),
        tag_columns=TAGS,
        tag_only_columns=True,
        source_id="pandas",
    )


def _data_frame_source_arrow_table(n_rows: int, tmp: Path) -> StreamProtocol:
    return DataFrameSource(
        _table(n_rows), tag_columns=TAGS, tag_only_columns=True, source_id="df_arrow"
    )


def _data_frame_source_arrow_schema(n_rows: int, tmp: Path) -> StreamProtocol:
    return DataFrameSource(
        pl.from_arrow(_table(n_rows)),
        tag_columns=TAGS,
        tag_only_columns=True,
        source_id="df_schema",
        schema=FULL_SCHEMA,
    )


def _delta_table_source(n_rows: int, tmp: Path) -> StreamProtocol:
    path = tmp / f"delta{n_rows}"
    if not path.exists():
        deltalake.write_deltalake(str(path), _table(n_rows))
    return DeltaTableSource(
        path, tag_columns=TAGS, tag_only_columns=True, source_id="delta"
    )


def _sqlite_table_source(n_rows: int, tmp: Path) -> StreamProtocol:
    path = tmp / f"rows{n_rows}.sqlite"
    if not path.exists():
        connection = sqlite3.connect(path)
        _ = connection.execute(
            "CREATE TABLE r (t TEXT NOT NULL, k INTEGER NOT NULL, s TEXT NOT NULL, "
            "u TEXT NOT NULL, b BLOB NOT NULL, PRIMARY KEY (t, k))"
        )
        for row in ROWS[:n_rows]:
            _ = connection.execute(
                "INSERT INTO r VALUES (?, ?, ?, ?, ?)",
                (row["t"], row["k"], row["s"], row["u"], row["b"]),
            )
        connection.commit()
        connection.close()
    return SQLiteTableSource(
        path, "r", tag_columns=TAGS, tag_only_columns=True, source_id="sqlite"
    )


def _cached_source_in_memory(n_rows: int, tmp: Path) -> StreamProtocol:
    return CachedSource(
        ArrowTableSource(
            _table(n_rows),
            tag_columns=TAGS,
            tag_only_columns=True,
            source_id="cached_mem",
        ),
        cache_database=InMemoryArrowDatabase(),
    )


def _cached_source_delta(n_rows: int, tmp: Path) -> StreamProtocol:
    return CachedSource(
        ArrowTableSource(
            _table(n_rows),
            tag_columns=TAGS,
            tag_only_columns=True,
            source_id="cached_delta",
        ),
        cache_database=DeltaTableDatabase(base_path=tmp / f"cache{n_rows}"),
    )


# Every source kind that carries tags ``t``, ``k`` and data ``s``, ``u`` (so every
# operator applies), with the row counts it can be built with. A header-only CSV and
# an empty SQLite table cannot be read (separate issues), so those kinds have rows.
SOURCES: dict[str, tuple[SourceFactory, tuple[int, ...]]] = {
    "ArrowTableSource": (_arrow_table_source, (2, 0)),
    "ArrowTableStream": (_arrow_table_stream, (2, 0)),
    "DictSource(python)": (_dict_source_python, (2, 0)),
    "DictSource(arrow schema)": (_dict_source_arrow_schema, (2, 0)),
    "CSVSource": (_csv_source, (2,)),
    "CSVSource(arrow schema)": (_csv_source_arrow_schema, (2, 0)),
    "DataFrameSource(polars)": (_data_frame_source_polars, (2, 0)),
    "DataFrameSource(pandas)": (_data_frame_source_pandas, (2, 0)),
    "DataFrameSource(arrow table)": (_data_frame_source_arrow_table, (2, 0)),
    "DataFrameSource(arrow schema)": (_data_frame_source_arrow_schema, (2, 0)),
    "DeltaTableSource": (_delta_table_source, (2, 0)),
    "SQLiteTableSource": (_sqlite_table_source, (2,)),
    "CachedSource(in memory)": (_cached_source_in_memory, (2, 0)),
    "CachedSource(delta)": (_cached_source_delta, (2, 0)),
}

SOURCE_CASES = [
    (name, n_rows) for name, (_, counts) in SOURCES.items() for n_rows in counts
]

# A raw ``ArrowTableStream`` has no system-tag columns, so it cannot be a pipeline's
# input (ITL-1126; binding one is ITL-1127): the job sweeps take every other kind, and
# ``test_large_normalization_sources`` asserts the refusal.
RAW_STREAM = "ArrowTableStream"
JOB_SOURCE_CASES = [
    (name, n_rows) for name, n_rows in SOURCE_CASES if name != RAW_STREAM
]


def build_source(name: str, n_rows: int, tmp: Path) -> StreamProtocol:
    """Build the source kind ``name`` with ``n_rows`` rows, its files under ``tmp``."""
    factory, _ = SOURCES[name]
    return factory(n_rows, tmp)
