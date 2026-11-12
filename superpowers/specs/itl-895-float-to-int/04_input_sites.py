"""Claim 4: the same truncation is reachable for INPUT data, not only function outputs.

Every place orcapod turns Python values into Arrow against a declared type goes through
``UniversalTypeConverter.python_dicts_to_arrow_table`` (``pa.Table.from_pylist`` with a
schema, ``universal_converter.py:1106``), whose primitive converter is a passthrough
(``universal_converter.py:1586-1588``). Paths that start from Arrow (Arrow/Delta/CSV/
DataFrame sources, operators) use ``Table.cast``, which is a SAFE cast and raises.

Each probe below prints the stored value and type, or the error, and the tracer's site.

Run: uv run python superpowers/specs/itl-895-float-to-int/04_input_sites.py
"""

from __future__ import annotations

import importlib

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

_tracer = importlib.import_module("_tracer")  # the sibling helper, via sys.path

import polars as pl  # noqa: E402
import pyarrow as pa  # noqa: E402

from orcapod.core.datagrams import Data, Datagram, Tag  # noqa: E402
from orcapod.operators import Batch, PolarsFilter  # noqa: E402
from orcapod.sources import (  # noqa: E402
    ArrowTableSource,
    DataFrameSource,
    DictSource,
    ListSource,
)

_tracer.install()


def probe(label: str, fn) -> None:
    _tracer.HITS.clear()
    try:
        result = fn()
        verdict = "TRUNCATED" if _tracer.HITS else "no truncation"
        print(f"- {label}\n    {verdict}: {result}")
    except Exception as e:  # noqa: BLE001
        print(f"- {label}\n    raises {type(e).__name__}: {str(e)[:110]}")
    for site in _tracer.summary():
        print(f"    site: {site}")


def col(stream, name):
    t = stream.as_table()
    return f"{name}={t[name].to_pylist()} ({t.schema.field(name).type})"


print("== sources built from Python values ==")
probe(
    "DictSource, data_schema={'k': str, 'x': int}, x=1.5",
    lambda: col(
        DictSource(
            data=[{"k": "a", "x": 1.5}],
            tag_columns=["k"],
            data_schema={"k": str, "x": int},
        ),
        "x",
    ),
)
probe(
    "DictSource, data_schema=pa.schema(x: int64), x=1.5",
    lambda: col(
        DictSource(
            data=[{"k": "a", "x": 1.5}],
            tag_columns=["k"],
            data_schema=pa.schema([("k", pa.large_string()), ("x", pa.int64())]),
        ),
        "x",
    ),
)
probe(
    "DictSource, TAG column declared int, k=1.5",
    lambda: col(
        DictSource(
            data=[{"k": 1.5, "x": 1}],
            tag_columns=["k"],
            data_schema={"k": int, "x": int},
        ),
        "k",
    ),
)
probe(
    "DictSource, no schema, x = 1 then 1.5 (inferred)",
    lambda: col(
        DictSource(data=[{"k": "a", "x": 1}, {"k": "b", "x": 1.5}], tag_columns=["k"]),
        "x",
    ),
)
probe(
    "ListSource, data_schema={'v': int}, element 1.5",
    lambda: col(
        ListSource(name="v", data=[1.5], data_schema={"v": int, "element_index": int}),
        "v",
    ),
)

print("\n== sources built from Arrow / DataFrames (Table.cast, safe) ==")
probe(
    "DataFrameSource, float column, schema=int64",
    lambda: col(
        DataFrameSource(
            pl.DataFrame({"k": ["a"], "x": [1.5]}),
            tag_columns=["k"],
            schema=pa.schema([("k", pa.large_string()), ("x", pa.int64())]),
        ),
        "x",
    ),
)
probe(
    "ArrowTableSource, float64 column (type kept, no declared type to disagree with)",
    lambda: col(
        ArrowTableSource(pa.table({"k": ["a"], "x": [1.5]}), tag_columns=["k"]), "x"
    ),
)

print("\n== datagrams built from Python values ==")
probe(
    "Datagram({'x': 1.5}, python_schema={'x': int}).as_table()",
    lambda: Datagram({"x": 1.5}, python_schema={"x": int}).as_table().to_pylist(),
)
probe(
    "Data({'x': 1.5}, python_schema={'x': int}).as_table()",
    lambda: Data({"x": 1.5}, python_schema={"x": int}).as_table().to_pylist(),
)
probe(
    "Tag({'k': 1.5}, python_schema={'k': int}).as_table()",
    lambda: Tag({"k": 1.5}, python_schema={"k": int}).as_table().to_pylist(),
)


def _update_arrow_backed():
    d = Datagram({"x": 1}, python_schema={"x": int})
    d.as_table()  # Arrow table loaded: update() takes its Arrow path
    return d.update(x=1.5).as_table().to_pylist()


def _update_dict_backed():
    d = Datagram({"x": 1}, python_schema={"x": int})
    return d.update(x=1.5).as_table().to_pylist()


probe("Datagram.update(x=1.5) on an int column, Arrow-backed", _update_arrow_backed)
probe("Datagram.update(x=1.5) on an int column, dict-backed", _update_dict_backed)
probe(
    "Datagram.with_columns(column_types={'z': int}, z=1.5)",
    lambda: (
        Datagram({"x": 1}, python_schema={"x": int})
        .with_columns(column_types={"z": int}, z=1.5)
        .as_table()
        .to_pylist()
    ),
)
probe(
    "Datagram.with_columns(column_types={'z': pa.int64()}, z=1.5)",
    lambda: (
        Datagram({"x": 1}, python_schema={"x": int})
        .with_columns(column_types={"z": pa.int64()}, z=1.5)
        .as_table()
        .to_pylist()
    ),
)
probe(
    "Data source_info value 1.5 (type derived from the value, no declared type)",
    lambda: (
        Data({"y": 1}, source_info={"y": 1.5})  # pyright: ignore[reportArgumentType]  # deliberate wrong value: probes the conversion
        .as_table(columns={"source": True})
        .to_pylist()
    ),
)

print("\n== operators (Arrow in, Arrow out) ==")
src = DictSource(data=[{"k": "a", "x": 1}, {"k": "b", "x": 2}], tag_columns=["k"])
probe("Batch(batch_size=2) over int column", lambda: col(Batch(batch_size=2)(src), "x"))
probe(
    "PolarsFilter(x > 1) over int column",
    lambda: col(PolarsFilter([pl.col("x") > 1])(src), "x"),
)
