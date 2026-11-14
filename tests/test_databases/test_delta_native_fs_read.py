"""Reading a local Delta store returns exactly what deltalake's default reader returns (ITL-892).

Orcapod scans local Delta tables through a native pyarrow filesystem instead of deltalake's
default ``PyFileSystem(DeltaStorageHandler)``, to avoid a hang at interpreter exit. Orcapod is
type strict, so the change must be invisible: the same Arrow schema (types, nullability at
every level, field and schema metadata, logical types) and the same values. Each test reads
the same table through Orcapod's public API and through deltalake's default reader
(``to_pyarrow_dataset(as_large_types=True)`` with no filesystem), and compares the two.
"""

from __future__ import annotations

import datetime as dt
import decimal
from dataclasses import dataclass
from pathlib import Path

import deltalake
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import orcapod as op
from orcapod.databases import DeltaTableDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator


@dataclass
class Measurement:
    """A dataclass output, stored through Orcapod's dataclass logical type."""

    label: str
    value: float
    tags: list[int]


def _default_reader(table_dir: Path) -> pa.Table:
    """deltalake's own reader: its Python filesystem, no ``filesystem`` argument."""
    return (
        deltalake.DeltaTable(str(table_dir))
        .to_pyarrow_dataset(as_large_types=True)
        .to_table()
    )


def _delta_table_dirs(root: Path) -> list[Path]:
    """Return every Delta table directory under ``root``, sorted."""
    return sorted(p.parent for p in root.rglob("_delta_log"))


def _assert_identical(actual: pa.Table, expected: pa.Table, sort_by: str) -> None:
    """Assert equal schemas, metadata included, and equal values after sorting.

    Args:
        actual: The table Orcapod read.
        expected: The table deltalake's default reader read.
        sort_by: Column that orders both tables before the value comparison.
    """
    assert actual.schema.equals(expected.schema, check_metadata=True), (
        f"schema differs:\n{actual.schema}\nvs default reader:\n{expected.schema}"
    )
    assert actual.sort_by(sort_by).equals(expected.sort_by(sort_by))


@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_pipeline_store_reads_match_default_reader_for_logical_types(
    tmp_path: Path,
    orchestrator: type[SyncPipelineOrchestrator | AsyncPipelineOrchestrator],
) -> None:
    """Every table a pipeline writes reads back exactly as deltalake's default reader reads it.

    The pods return logical types (Path, a dataclass, numpy, pandas), a nullable value and
    a list, under both orchestrators.
    """
    files = []
    for i in range(3):
        f = tmp_path / "inputs" / f"f{i}.txt"
        f.parent.mkdir(exist_ok=True)
        f.write_text(f"content {i}")
        files.append(f)
    paths = op.sources.DictSource(
        [{"id": i, "f": files[i]} for i in range(3)], tag_columns=["id"]
    )
    numbers = op.sources.DictSource(
        [{"id": i, "x": i} for i in range(3)], tag_columns=["id"]
    )

    @op.function_pod(output_keys=["path"])
    def same_path(f: Path) -> Path:
        """Return the input path, so the store holds a ``Path`` column."""
        return f

    @op.function_pod(output_keys=["m"])
    def measure(x: int) -> Measurement:
        """Return a dataclass built from ``x``."""
        return Measurement(label=f"m{x}", value=x / 3, tags=list(range(x)))

    @op.function_pod(output_keys=["arr"])
    def array(x: int) -> np.ndarray:  # pyright: ignore[reportMissingTypeArgument]  # Orcapod's numpy logical type is declared on bare ndarray
        """Return an ``int16`` numpy array of shape (1, x + 1)."""
        return np.arange(x + 1, dtype=np.int16).reshape(1, -1)

    @op.function_pod(output_keys=["df"])
    def frame(x: int) -> pd.DataFrame:
        """Return a two-row DataFrame with a null string."""
        return pd.DataFrame({"a": [x, x + 1], "b": [str(x), None]})

    @op.function_pod(output_keys=["maybe", "values"])
    def mixed(x: int) -> tuple[int | None, list[int]]:
        """Return a nullable int and a list of ints."""
        return (None if x == 1 else x, [x] * x)

    store_dir = tmp_path / "store"
    db = DeltaTableDatabase(store_dir)
    job = PipelineJob(database=db)
    with job:
        same_path.pod(paths, label="same_path")
        measure.pod(numbers, label="measure")
        array.pod(numbers, label="array")
        frame.pod(numbers, label="frame")
        mixed.pod(numbers, label="mixed")
    job.run(orchestrator=orchestrator())

    fresh = DeltaTableDatabase(store_dir)
    record_paths = fresh.list_sources()
    assert record_paths, "the pipeline wrote no Delta tables"
    table_dirs = {
        d.relative_to(store_dir).parts: d for d in _delta_table_dirs(store_dir)
    }
    compared = 0
    for record_path in record_paths:
        # Keep the stored record-id column under its own name so every column is compared.
        actual = fresh.get_all_records(
            record_path, record_id_column=DeltaTableDatabase.RECORD_ID_COLUMN
        )
        expected = _default_reader(table_dirs[record_path])
        assert actual is not None
        _assert_identical(actual, expected, sort_by=DeltaTableDatabase.RECORD_ID_COLUMN)
        compared += 1
    assert compared == len(table_dirs)


def _typed_table(offset: int) -> pa.Table:
    """Build rows with narrow, decimal, timestamp, nested and non-nullable columns.

    Args:
        offset: Makes the ids of each call distinct, so appends add new rows.

    Returns:
        The table, with a ``part`` column whose values need escaping in a partition path.
    """
    schema = pa.schema(
        [
            pa.field("id", pa.large_string(), nullable=False),
            pa.field("part", pa.large_string()),
            pa.field("i8", pa.int8()),
            pa.field("u16", pa.uint16()),
            pa.field("f32", pa.float32()),
            pa.field("dec", pa.decimal128(12, 3)),
            pa.field("ts", pa.timestamp("us", tz="UTC")),
            pa.field("day", pa.date32()),
            pa.field("bin", pa.large_binary()),
            pa.field(
                "ints", pa.large_list(pa.field("item", pa.int16(), nullable=False))
            ),
            pa.field(
                "st",
                pa.struct(
                    [
                        pa.field("a", pa.int32(), nullable=False),
                        pa.field("b", pa.large_string()),
                    ]
                ),
            ),
            pa.field("empty", pa.large_string()),
        ],
        metadata={"origin": "itl-892"},
    )
    parts = ["a b", "x/y", "é%", "plain", None]
    n = 10
    return pa.table(
        {
            "id": [f"r{offset}-{k}" for k in range(n)],
            "part": [parts[k % len(parts)] for k in range(n)],
            "i8": [k - 5 for k in range(n)],
            "u16": [k * 1000 for k in range(n)],
            "f32": [k / 7 for k in range(n)],
            "dec": [decimal.Decimal(k * 1001) / 1000 for k in range(n)],
            "ts": [
                dt.datetime(2026, 11, 10, k, tzinfo=dt.timezone.utc) for k in range(n)
            ],
            "day": [dt.date(2026, 11, 1 + k) for k in range(n)],
            "bin": [bytes([k]) * 3 for k in range(n)],
            "ints": [[k, k + 1] if k % 3 else None for k in range(n)],
            "st": [{"a": k, "b": None if k % 2 else str(k)} for k in range(n)],
            "empty": [None] * n,
        },
        schema=schema,
    )


@pytest.mark.parametrize("partitioned", [False, True], ids=["flat", "partitioned"])
def test_delta_table_source_matches_default_reader(
    tmp_path: Path, partitioned: bool
) -> None:
    """``DeltaTableSource`` reads flat and partitioned tables exactly as the default reader does."""
    table_dir = tmp_path / "table"
    partition_by = ["part"] if partitioned else None
    deltalake.write_deltalake(
        str(table_dir), _typed_table(0), partition_by=partition_by
    )
    deltalake.write_deltalake(
        str(table_dir), _typed_table(1), mode="append", partition_by=partition_by
    )

    source = op.sources.DeltaTableSource(table_dir, tag_columns=["id"])
    reference = _default_reader(table_dir)

    actual = source.as_table()
    # The source adds its own provenance columns; compare the stored columns only.
    stored = actual.select(reference.column_names)
    assert stored.schema.equals(reference.schema, check_metadata=True), (
        f"schema differs:\n{stored.schema}\nvs default reader:\n{reference.schema}"
    )
    assert stored.sort_by("id").equals(reference.sort_by("id"))


def test_local_tables_open_on_a_native_filesystem(tmp_path: Path) -> None:
    """Local tables open on a native filesystem, and a caller's filesystem is kept as given."""
    import pyarrow.dataset as pa_ds
    import pyarrow.fs as pa_fs

    from orcapod.databases.storage_utils import delta_to_pyarrow_dataset

    deltalake.write_deltalake(str(tmp_path), _typed_table(0))
    table = deltalake.DeltaTable(str(tmp_path))

    native = delta_to_pyarrow_dataset(table, as_large_types=True)
    assert isinstance(native, pa_ds.FileSystemDataset)
    assert isinstance(native.filesystem, pa_fs.SubTreeFileSystem)
    assert isinstance(native.filesystem.base_fs, pa_fs.LocalFileSystem)

    # A filesystem chosen by the caller is used as given (here one the helper never builds).
    opened = delta_to_pyarrow_dataset(table, filesystem=pa_fs.LocalFileSystem())
    assert isinstance(opened, pa_ds.FileSystemDataset)
    assert isinstance(opened.filesystem, pa_fs.LocalFileSystem)


# deltalake percent-encodes ``DeltaTable.table_uri`` (``/x/a%20b/t/`` on 1.0.2 and 1.2.1,
# ``file:///x/a%20b/`` on 1.6.6), so the native filesystem must decode the table root.
_SPECIAL_ROOTS = ["a b", "pct%", "p%20q", "é ü", "x#y", "q?r", "semi;c", "plus+"]


def _write_or_skip(table_dir: Path, table: pa.Table) -> None:
    """Write ``table`` with deltalake, skipping a root name deltalake itself cannot write.

    Args:
        table_dir: Directory of the new Delta table.
        table: Rows to write.
    """
    try:
        deltalake.write_deltalake(str(table_dir), table)
        deltalake.DeltaTable(str(table_dir))
    except Exception as exc:  # noqa: BLE001 - deltalake's own limit, not Orcapod's
        pytest.skip(
            f"deltalake cannot write a table at {table_dir.parent.name!r}: {exc}"
        )


@pytest.mark.parametrize("root_name", _SPECIAL_ROOTS)
def test_database_reads_a_store_under_a_special_character_root(
    tmp_path: Path, root_name: str
) -> None:
    """A store whose directory needs percent-encoding is read in full, not as empty."""
    store_dir = tmp_path / root_name / "store"
    _write_or_skip(tmp_path / root_name / "probe", _typed_table(0))
    table = pa.table(
        {
            "__record_id": pa.array(["a", "b", "c"], pa.large_string()),
            "v": pa.array([1, 2, 3], pa.int64()),
        }
    )
    db = DeltaTableDatabase(store_dir)
    db.add_records(("t",), table, record_id_column="__record_id")
    db.flush()

    actual = DeltaTableDatabase(store_dir).get_all_records(
        ("t",), record_id_column=DeltaTableDatabase.RECORD_ID_COLUMN
    )

    (table_dir,) = _delta_table_dirs(store_dir)
    expected = _default_reader(table_dir)
    assert expected.num_rows == 3
    assert actual is not None, f"the store under {root_name!r} read as empty"
    _assert_identical(actual, expected, sort_by=DeltaTableDatabase.RECORD_ID_COLUMN)


@pytest.mark.parametrize("root_name", _SPECIAL_ROOTS)
def test_delta_table_source_reads_a_table_under_a_special_character_root(
    tmp_path: Path, root_name: str
) -> None:
    """A table whose directory needs percent-encoding reads like the default reader."""
    table_dir = tmp_path / root_name / "table"
    _write_or_skip(table_dir, _typed_table(0))

    source = op.sources.DeltaTableSource(table_dir, tag_columns=["id"])
    reference = _default_reader(table_dir)

    stored = source.as_table().select(reference.column_names)
    assert stored.schema.equals(reference.schema, check_metadata=True)
    assert stored.sort_by("id").equals(reference.sort_by("id"))
