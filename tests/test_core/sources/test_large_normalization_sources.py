"""Every source presents large Arrow types, with no exception (ITL-1122).

The rule (developer, November 14, 2026): every column of every table Orcapod presents or
stores has its string, binary and list types in their large variants, recursively. A
user's own Arrow table and an explicit Arrow schema are normalized too. The one place is
where a table becomes a stream (``ArrowTableStream``), so a source, its job node, its
head and a cache over it agree. Large and non-large variants hash identically, so no
row hash, content hash, pipeline hash or default source id moves.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.core.sources import (
    ArrowTableSource,
    CachedSource,
    DictSource,
    ListSource,
    PollingSource,
)
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.operators import SelectTagColumns
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.types import Cursor, PollingConfig
from orcapod.utils import arrow_utils
from tests.test_core.large_types_cases import (
    ALL_COLUMNS,
    FULL_SCHEMA,
    ROWS,
    SOURCE_CASES,
    TAGS,
    build_source,
    non_large_columns,
)

ORCHESTRATORS = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}
LARGE_SCHEMA = arrow_utils.normalize_schema_to_large_types(FULL_SCHEMA, keep_null=True)


def _job_node_tables(source: StreamProtocol, orchestrator: str) -> list[pa.Table]:
    """Every node's table after a job over ``source`` runs on ``orchestrator``."""
    job = PipelineJob(name="itl1122", database=InMemoryArrowDatabase())
    with job:
        _ = SelectTagColumns(["t"])(source, label="selected")
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    return [node.as_table(columns=ALL_COLUMNS) for node in job.nodes.values()]


@pytest.mark.parametrize(("name", "n_rows"), SOURCE_CASES)
class TestEverySourceKind:
    def test_table_and_head_have_no_non_large_type(
        self, name: str, n_rows: int, tmp_path: Path
    ) -> None:
        source = build_source(name, n_rows, tmp_path)
        table = source.as_table(columns=ALL_COLUMNS)
        assert table.num_rows == n_rows
        assert non_large_columns(table) == []
        assert non_large_columns(source.head(columns=ALL_COLUMNS).table) == []

    @pytest.mark.parametrize("orchestrator", ["sync", "async"])
    def test_job_nodes_have_no_non_large_type(
        self, name: str, n_rows: int, orchestrator: str, tmp_path: Path
    ) -> None:
        tables = _job_node_tables(build_source(name, n_rows, tmp_path), orchestrator)
        for table in tables:
            assert non_large_columns(table) == []


def test_explicit_arrow_schema_is_normalized(tmp_path: Path) -> None:
    """An Arrow schema the user gives is taken in its large variants."""
    table = build_source("CSVSource(arrow schema)", 2, tmp_path).as_table()
    assert table.schema.field("t").type == pa.large_string()
    assert table.schema.field("s").type == pa.large_string()
    dict_table = build_source("DictSource(arrow schema)", 2, tmp_path).as_table()
    arrow_table = build_source("ArrowTableSource", 2, tmp_path).as_table()
    assert dict_table.schema == arrow_table.schema


def test_user_arrow_table_keeps_width_and_nullability() -> None:
    """Only layouts change: widths, nullability (top level and nested), field names."""
    items = pa.list_(pa.field("item", pa.int8(), nullable=False))
    schema = pa.schema(
        [
            pa.field("t", pa.string(), nullable=False),
            pa.field("l", items, nullable=False),
            pa.field("s", pa.string(), nullable=True),
        ]
    )
    table = pa.Table.from_pylist([{"t": "a", "l": [1], "s": None}], schema=schema)
    out = ArrowTableSource(table, tag_columns=["t"], source_id="u").as_table()
    assert out.schema.field("t") == pa.field("t", pa.large_string(), nullable=False)
    assert out.schema.field("l") == pa.field(
        "l", pa.large_list(pa.field("item", pa.int8(), nullable=False)), nullable=False
    )
    assert out.schema.field("s") == pa.field("s", pa.large_string(), nullable=True)


def test_list_view_is_not_supported_today() -> None:
    """Pins today's behaviour: ``list_view`` is not a supported type (ITL-1129).

    A source refuses a ``list_view`` column (the Arrow hasher cannot hash it); a raw
    ``ArrowTableStream`` holds it as given, untyped and unhashable. Supporting it
    (normalized to ``large_list``) is a deliberate later change.
    """
    table = pa.table(
        {
            "t": pa.array(["a"], pa.large_string()),
            "lv": pa.array([["x"]], pa.list_view(pa.string())),
        }
    )
    with pytest.raises(ValueError, match="list_view"):
        _ = ArrowTableSource(table, tag_columns=["t"], source_id="lv")
    stream = ArrowTableStream(table, tag_columns=["t"])
    assert stream.as_table().schema.field("lv").type == pa.list_view(pa.string())
    with pytest.raises(ValueError, match="list_view"):
        _ = stream.content_hash()


def test_list_source_is_large() -> None:
    source = ListSource("v", ["a", "b"])
    assert non_large_columns(source.as_table(columns=ALL_COLUMNS)) == []


class _Impl:
    """A polling implementation that fetches one batch, as a dict or an Arrow table."""

    def __init__(self, as_arrow: bool) -> None:
        self.as_arrow = as_arrow

    def identity(self) -> str:
        return f"impl-{self.as_arrow}"

    def to_config(self) -> None:
        return None

    @classmethod
    def from_config(cls, config: Any) -> _Impl:
        return cls(False)

    def schema(self) -> None:
        return None

    async def poll(self, cursor: Cursor[int] | None = None) -> bool:
        return cursor is None

    async def fetch(self, cursor: Cursor[int] | None = None) -> tuple[Cursor[int], Any]:
        if cursor is not None:
            return Cursor(value=1), {}
        if self.as_arrow:
            return Cursor(value=1), pa.table(
                {"t": pa.array(["a"], pa.string()), "s": pa.array(["x"], pa.string())}
            )
        return Cursor(value=1), {"t": ["a"], "s": ["x"]}

    async def close(self) -> None:
        return None


@pytest.mark.parametrize("as_arrow", [False, True])
def test_polling_source_is_large(as_arrow: bool) -> None:
    source = PollingSource(
        _Impl(as_arrow), tag_columns="t", polling_config=PollingConfig(duration=0.01)
    )
    assert non_large_columns(source.as_table(columns=ALL_COLUMNS)) == []


@function_pod(output_keys="v")
def _upper(s: str) -> str:
    return s.upper()


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
@pytest.mark.parametrize("n_rows", [2, 0])
def test_function_nodes_and_derived_sources_are_large(
    orchestrator: str, n_rows: int, tmp_path: Path
) -> None:
    """A function node passing a ``string`` tag through, and derived sources."""
    job = PipelineJob(name="itl1122", database=InMemoryArrowDatabase())
    with job:
        source = build_source("ArrowTableSource", n_rows, tmp_path)
        _ = _upper.pod(source, label="upper")
        _ = SelectTagColumns(["t"])(source, label="selected")
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    for label in ("upper", "selected"):
        node = job.nodes[label]
        assert non_large_columns(node.as_table(columns=ALL_COLUMNS)) == []
        derived = node.as_source()
        assert non_large_columns(derived.as_table(columns=ALL_COLUMNS)) == []
        with_tags = node.as_source(tags_as_data={"t": "t_data"})
        assert non_large_columns(with_tags.as_table(columns=ALL_COLUMNS)) == []


@pytest.mark.parametrize("n_rows", [2, 0])
def test_function_pod_stream_is_large(n_rows: int, tmp_path: Path) -> None:
    stream = _upper.pod(build_source("ArrowTableSource", n_rows, tmp_path))
    assert non_large_columns(stream.as_table(columns=ALL_COLUMNS)) == []


def test_spiraldb_table_source_is_large(tmp_path: Path) -> None:
    from orcapod.sources import SpiralDBTableSource
    from tests.test_databases import fake_pyspiral

    with fake_pyspiral.install(tmp_path / "spiral"):
        project = fake_pyspiral.FakeSpiral().project("p")
        table = project.create_table(
            "default.r", key_schema=pa.schema([pa.field("t", pa.string())])
        )
        table.write(
            pa.table(
                {
                    "t": pa.array(["a", "b"], pa.string()),
                    "s": pa.array(["x", "y"], pa.string()),
                    "b": pa.array([b"1", b"2"], pa.binary()),
                    "l": pa.array([["x"], []], pa.list_(pa.string())),
                }
            )
        )
        source = SpiralDBTableSource("p", "r", tag_columns=["t"])
        assert non_large_columns(source.as_table(columns=ALL_COLUMNS)) == []


# ---------------------------------------------------------------------------
# What does not move
# ---------------------------------------------------------------------------


def _row_hashes(stream: StreamProtocol) -> list[tuple[str, str]]:
    return [
        (tag.content_hash().to_hex(), data.content_hash().to_hex())
        for tag, data in stream.iter_data()
    ]


@pytest.mark.parametrize("source_id", [None, "named"])
def test_string_and_large_string_sources_hash_alike(source_id: str | None) -> None:
    """The same values as ``string`` and as ``large_string`` give equal row, content and
    pipeline hashes and the same (default) source id."""
    small = ArrowTableSource(
        pa.Table.from_pylist(ROWS, schema=FULL_SCHEMA),
        tag_columns=TAGS,
        source_id=source_id,
    )
    large = ArrowTableSource(
        pa.Table.from_pylist(ROWS, schema=LARGE_SCHEMA),
        tag_columns=TAGS,
        source_id=source_id,
    )
    assert small.source_id == large.source_id
    assert small.content_hash() == large.content_hash()
    assert small.pipeline_hash() == large.pipeline_hash()
    assert _row_hashes(small) == _row_hashes(large)
    assert small.as_table(columns=ALL_COLUMNS).equals(
        large.as_table(columns=ALL_COLUMNS)
    )


def test_dict_source_arrow_schema_hashes_alike() -> None:
    """A ``DictSource`` given ``string`` or ``large_string`` in its Arrow schema."""
    small = DictSource(ROWS, tag_columns=TAGS, data_schema=FULL_SCHEMA)
    large = DictSource(ROWS, tag_columns=TAGS, data_schema=LARGE_SCHEMA)
    assert small.source_id == large.source_id
    assert small.content_hash() == large.content_hash()
    assert small.pipeline_hash() == large.pipeline_hash()
    assert _row_hashes(small) == _row_hashes(large)


# ---------------------------------------------------------------------------
# Stored tables
# ---------------------------------------------------------------------------


def _user_source(n_rows: int) -> ArrowTableSource:
    """A user's Arrow table in the non-large types, as a source."""
    return ArrowTableSource(
        pa.Table.from_pylist(ROWS[:n_rows], schema=FULL_SCHEMA),
        tag_columns=TAGS,
        source_id="user",
    )


def _store(kind: str, tmp_path: Path) -> ArrowDatabaseProtocol:
    if kind == "in_memory":
        return InMemoryArrowDatabase()
    return DeltaTableDatabase(base_path=tmp_path / "store")


@pytest.mark.parametrize("store_kind", ["in_memory", "delta"])
class TestStoredTables:
    def test_cached_records_are_large(self, store_kind: str, tmp_path: Path) -> None:
        cached = CachedSource(
            _user_source(2),
            cache_database=_store(store_kind, tmp_path),
        )
        _ = cached.as_table()
        stored = cached.get_all_records()
        assert stored is not None
        assert non_large_columns(stored) == []

    @pytest.mark.parametrize("n_rows", [2, 0])
    def test_cache_head_before_the_read_has_the_types_of_the_read(
        self, store_kind: str, n_rows: int, tmp_path: Path
    ) -> None:
        cached = CachedSource(
            _user_source(n_rows),
            cache_database=_store(store_kind, tmp_path),
        )
        unread = cached.head(columns=ALL_COLUMNS).table
        read = cached.as_table(columns=ALL_COLUMNS)
        assert non_large_columns(unread) == []
        assert unread.schema.equals(read.schema, check_metadata=False)


def test_a_delta_table_written_with_string_accepts_large_rows(tmp_path: Path) -> None:
    """A Delta table a build before this change wrote with ``string`` columns takes the
    ``large_string`` rows written after it (Delta keeps one string type). An in-memory
    store keeps no data across processes, so no such table of it can exist."""
    store = _store("delta", tmp_path)
    before = pa.Table.from_pylist(ROWS[:1], schema=FULL_SCHEMA)
    after = pa.Table.from_pylist(ROWS[1:], schema=LARGE_SCHEMA)
    _ = store.add_records(("t",), before, record_id_column="b")
    store.flush()
    _ = store.add_records(("t",), after, record_id_column="b")
    store.flush()
    stored = store.get_all_records(("t",), record_id_column="b")
    assert stored is not None
    assert stored.num_rows == 2
