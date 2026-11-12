"""Stored and registered shapes of a user class must agree on read (ITL-881, S2).

A stored column whose extension name matches a registered class but whose Arrow storage
differs (possible only if two data contexts map a field differently, or a table was
written by other means) must raise ``TypeIdentityError`` from ``DeltaTableDatabase``
instead of being logged and read as "no records".
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any

import deltalake
import pyarrow as pa
import pytest

from orcapod.databases import DeltaTableDatabase
from orcapod.errors import TypeIdentityError
from orcapod.sources import DictSource


@dataclasses.dataclass
class StoredPoint:
    """An ordinary dataclass whose stored shape the tests change."""

    x: int
    y: int


def _extension_field_metadata() -> dict[bytes | str, bytes | str]:
    """Return Arrow field metadata marking a column as a ``StoredPoint`` column."""
    value: Any = StoredPoint(1, 2)
    source = DictSource(data=[{"k": "a", "p": value}], tag_columns=["k"])
    ext = source.as_table().schema.field("p").type
    return {
        b"ARROW:extension:name": ext.extension_name.encode(),
        b"ARROW:extension:metadata": ext.__arrow_ext_serialize__(),
    }


def test_storage_mismatch_raises_instead_of_reading_nothing(tmp_path: Path) -> None:
    """A stored column whose storage differs from its class raises on read."""
    wrong = pa.struct([pa.field("x", pa.float64()), pa.field("y", pa.int64())])
    field = pa.field("q", wrong).with_metadata(_extension_field_metadata())
    table = pa.table(
        {"q": pa.array([{"x": 1.5, "y": 2}], type=wrong)},
        schema=pa.schema([field]),
    )
    deltalake.write_deltalake(str(tmp_path / "points"), table)

    with pytest.raises(TypeIdentityError, match="expected storage_type"):
        DeltaTableDatabase(tmp_path).get_all_records(("points",))


@dataclasses.dataclass
class Labelled:
    """An ordinary dataclass with a string field."""

    label: str
    n: int


def _run_labelled(db_root: Path) -> tuple[int, list[Labelled]]:
    """Run an echo pod over two ``Labelled`` rows; return the call count and values."""
    from orcapod import PipelineJob, function_pod

    calls: list[Labelled] = []

    @function_pod(output_keys="q")
    def echo(p: Labelled) -> Labelled:
        """Record the call and return ``p`` unchanged."""
        calls.append(p)
        return p

    rows: list[Any] = [{"k": f"r{i}", "p": Labelled(f"x{i}", i)} for i in (1, 2)]
    job = PipelineJob(name="compact", store=DeltaTableDatabase(db_root))
    with job:
        echo.pod(DictSource(data=rows, tag_columns=["k"], source_id="s"), label="echo")
    # One commit per row, so the result table has one file per row to compact.
    job.run(commit_every_rows=1)
    values = [data.as_dict()["q"] for _, data in job.nodes["echo"].iter_data()]
    return len(calls), sorted(values, key=lambda v: v.n)


def test_stored_values_read_after_delta_compaction(tmp_path: Path) -> None:
    """Delta compaction rewrites large_string struct fields as string; still readable."""
    expected = [Labelled("x1", 1), Labelled("x2", 2)]
    assert _run_labelled(tmp_path) == (2, expected)
    rewritten: list[str] = []
    for log in tmp_path.rglob("_delta_log"):
        table = deltalake.DeltaTable(str(log.parent))
        before = set(table.file_uris())
        metrics = table.optimize.compact()
        if "q" in [field.name for field in table.schema().fields]:
            # The result table must really be rewritten, or the cached read below
            # would pass on the files the first run wrote.
            assert metrics["numFilesRemoved"] == len(before) >= 2
            assert metrics["numFilesAdded"] == 1
            after = set(deltalake.DeltaTable(str(log.parent)).file_uris())
            assert len(after) == 1 and not after & before
            rewritten.append(str(log.parent))
    assert len(rewritten) == 1
    assert _run_labelled(tmp_path) == (0, expected)


@pytest.mark.xfail(
    strict=True,
    raises=pa.ArrowInvalid,
    reason=(
        "ITL-894: _deserialize accepts a non-large storage variant but returns the "
        "large storage type, so IPC reinterprets 32-bit offsets as 64-bit. Parquet "
        "and Delta reads convert the data and are correct."
    ),
)
def test_ipc_read_of_non_large_nested_storage_is_valid() -> None:
    """A user-class column with non-large nested storage reads correctly over IPC."""
    import io

    narrow = pa.struct([pa.field("label", pa.string()), pa.field("n", pa.int64())])
    value: Any = Labelled("x1", 1)
    ext = (
        DictSource(data=[{"k": "a", "p": value}], tag_columns=["k"])
        .as_table()
        .schema.field("p")
        .type
    )
    metadata: dict[bytes | str, bytes | str] = {
        b"ARROW:extension:name": ext.extension_name.encode(),
        b"ARROW:extension:metadata": ext.__arrow_ext_serialize__(),
    }
    table = pa.table(
        {"p": pa.array([{"label": "hello", "n": 1}], type=narrow)},
        schema=pa.schema([pa.field("p", narrow).with_metadata(metadata)]),
    )
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    read = pa.ipc.open_stream(sink.getvalue()).read_all()
    read.validate(full=True)
    read_type = read.schema.field("p").type
    assert read_type.extension_name == ext.extension_name
    assert read_type.__arrow_ext_serialize__() == ext.__arrow_ext_serialize__()
    assert read.column("p").to_pylist() == [{"label": "hello", "n": 1}]
