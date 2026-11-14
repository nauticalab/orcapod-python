"""No operator output has a non-large Arrow type, from any source, on any path (ITL-1122).

The rule (developer, November 14, 2026): every column of every table Orcapod presents or
stores has its string, binary and list types in their large variants, recursively, with no
exception for a user's Arrow table. So every operator over every source kind gives large
types: on a plain stream and inside a job, sync and async, with rows and without, and a
zero-row result has the schema of a populated one. Large and non-large variants hash
identically, so no hash moves.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest

from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.operators import (
    Batch,
    DropColumns,
    DropDataColumns,
    DropNulls,
    DropTagColumns,
    GroupBy,
    Index,
    Join,
    MapData,
    MapTags,
    MergeJoin,
    PolarsFilter,
    SelectColumns,
    SelectDataColumns,
    SelectTagColumns,
    SemiJoin,
)
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.protocols.core_protocols import StreamProtocol
from tests.test_core.large_types_cases import (
    ALL_COLUMNS,
    FULL_SCHEMA,
    ROWS,
    SOURCE_CASES,
    SOURCES,
    TAGS,
    build_source,
    non_large_columns,
)

ORCHESTRATORS = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}


def _other(n_rows: int, data_column: str) -> ArrowTableSource:
    """A second user's table: tag ``t`` and one data column, both ``string``."""
    table = pa.table(
        {
            "t": pa.array(["a", "b"][:n_rows], pa.string()),
            data_column: pa.array(["u", "v"][:n_rows], pa.string()),
        }
    )
    table = table.cast(
        pa.schema([pa.field(field.name, field.type, False) for field in table.schema])
    )
    return ArrowTableSource(table, tag_columns=["t"], source_id=f"other_{data_column}")


Apply = Callable[[StreamProtocol, int, str | None], StreamProtocol]

OPERATORS: dict[str, Apply] = {
    "SelectTagColumns": lambda s, n, label: SelectTagColumns(["t"])(s, label=label),
    "DropTagColumns": lambda s, n, label: DropTagColumns(["k"])(s, label=label),
    "SelectDataColumns": lambda s, n, label: SelectDataColumns(["s"])(s, label=label),
    "DropDataColumns": lambda s, n, label: DropDataColumns(["u"])(s, label=label),
    "SelectColumns": lambda s, n, label: SelectColumns(["t", "k", "s"])(s, label=label),
    "DropColumns": lambda s, n, label: DropColumns(["u"])(s, label=label),
    "MapTags": lambda s, n, label: MapTags({"t": "t2"})(s, label=label),
    "MapData": lambda s, n, label: MapData({"s": "s2"})(s, label=label),
    "PolarsFilter(predicate)": lambda s, n, label: PolarsFilter(
        predicates=[pl.col("k") >= 0]
    )(s, label=label),
    "PolarsFilter()": lambda s, n, label: PolarsFilter()(s, label=label),
    "DropNulls()": lambda s, n, label: DropNulls()(s, label=label),
    "DropNulls([s])": lambda s, n, label: DropNulls(["s"])(s, label=label),
    "Join": lambda s, n, label: Join()(s, _other(n, "z"), label=label),
    "MergeJoin": lambda s, n, label: MergeJoin()(s, _other(n, "s"), label=label),
    "SemiJoin": lambda s, n, label: SemiJoin()(s, _other(n, "z"), label=label),
    "Batch": lambda s, n, label: Batch(batch_size=2)(s, label=label),
    "GroupBy(t)": lambda s, n, label: GroupBy(by=["t"])(s, label=label),
    "GroupBy(k)": lambda s, n, label: GroupBy(by=["k"])(s, label=label),
}


def _cases(row_counts: tuple[int, ...]) -> list[tuple[str, str, int]]:
    """Every (source kind, operator, rows) with rows among ``row_counts``."""
    return [
        (source, operator, n_rows)
        for source, n_rows in SOURCE_CASES
        if n_rows in row_counts
        for operator in OPERATORS
    ]


def _job_tables(
    source: str, operator: str, n_rows: int, orchestrator: str, tmp_path: Path
) -> tuple[pa.Table, list[pa.Table]]:
    """The operator's node table, and every other node's, after a job run over
    inputs of ``n_rows`` rows."""
    job = PipelineJob(name="itl1122", database=InMemoryArrowDatabase())
    with job:
        OPERATORS[operator](build_source(source, n_rows, tmp_path), n_rows, "op")
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    tables = {
        label: node.as_table(columns=ALL_COLUMNS) for label, node in job.nodes.items()
    }
    return tables.pop("op"), list(tables.values())


def _user_schema(table: pa.Table) -> pa.Schema:
    """The schema of ``table``'s user tag and data columns."""
    return table.select([name for name in table.column_names if name[0] != "_"]).schema


@pytest.mark.parametrize(("source", "operator", "n_rows"), _cases((2, 0)))
def test_plain_stream_has_no_non_large_type(
    source: str, operator: str, n_rows: int, tmp_path: Path
) -> None:
    out = OPERATORS[operator](build_source(source, n_rows, tmp_path), n_rows, None)
    assert non_large_columns(out.as_table(columns=ALL_COLUMNS)) == []
    assert non_large_columns(out.head(columns=ALL_COLUMNS).table) == []


@pytest.mark.parametrize(
    ("source", "operator"),
    [(source, operator) for source, operator, _ in _cases((0,))],
)
def test_zero_row_schema_equals_populated(
    source: str, operator: str, tmp_path: Path
) -> None:
    populated = OPERATORS[operator](build_source(source, 2, tmp_path), 2, None)
    empty = OPERATORS[operator](build_source(source, 0, tmp_path), 0, None)
    populated_table = populated.as_table(columns=ALL_COLUMNS)
    empty_table = empty.as_table(columns=ALL_COLUMNS)
    # The user tag and data columns: which system columns a zero-row result
    # carries is ITL-912's subject, not this rule's.
    assert _user_schema(empty_table) == _user_schema(populated_table)


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
@pytest.mark.parametrize(("source", "operator", "n_rows"), _cases((2, 0)))
def test_job_has_no_non_large_type_and_equals_plain(
    source: str, operator: str, n_rows: int, orchestrator: str, tmp_path: Path
) -> None:
    """With rows and without, a job node equals the same operator on a plain stream."""
    op_table, other_tables = _job_tables(
        source, operator, n_rows, orchestrator, tmp_path
    )
    assert (op_table.num_rows > 0) == (n_rows > 0)
    assert non_large_columns(op_table) == []
    for table in other_tables:
        assert non_large_columns(table) == []
    plain = OPERATORS[operator](build_source(source, n_rows, tmp_path), n_rows, None)
    assert _user_schema(op_table) == _user_schema(plain.as_table(columns=ALL_COLUMNS))


@pytest.mark.parametrize("operator", list(OPERATORS))
def test_row_hashes_equal_for_large_and_non_large_input(
    operator: str, tmp_path: Path
) -> None:
    """A ``string`` source and a ``large_string`` source of the same values give equal
    row, content and pipeline hashes after every operator."""
    small = ArrowTableSource(
        pa.Table.from_pylist(ROWS, schema=FULL_SCHEMA), tag_columns=TAGS
    )
    large_schema = pa.schema(
        [
            field.with_type(
                pa.large_string()
                if field.type == pa.string()
                else pa.large_binary()
                if field.type == pa.binary()
                else pa.large_list(pa.large_string())
                if field.type == pa.list_(pa.string())
                else pa.struct(
                    [
                        pa.field("x", pa.large_list(pa.large_string())),
                        pa.field("y", pa.large_string()),
                    ]
                )
                if field.name == "n"
                else field.type
            )
            for field in FULL_SCHEMA
        ]
    )
    large = ArrowTableSource(
        pa.Table.from_pylist(ROWS, schema=large_schema), tag_columns=TAGS
    )
    small_out = OPERATORS[operator](small, 2, None)
    large_out = OPERATORS[operator](large, 2, None)
    assert small_out.content_hash() == large_out.content_hash()
    assert small_out.pipeline_hash() == large_out.pipeline_hash()

    def row_hashes(stream: StreamProtocol) -> list[tuple[str, str]]:
        return sorted(
            (tag.content_hash().to_hex(), data.content_hash().to_hex())
            for tag, data in stream.iter_data()
        )

    assert row_hashes(small_out) == row_hashes(large_out)


def test_index_over_a_list_column_is_large(tmp_path: Path) -> None:
    """``Index`` reads a list column (the kinds above that carry one: ArrowTableSource)."""
    for n_rows in (2, 0):
        out = Index("l", 0)(build_source("ArrowTableSource", n_rows, tmp_path))
        assert non_large_columns(out.as_table(columns=ALL_COLUMNS)) == []


class TestJoinNeverPicksASide:
    """A shared tag that is ``string`` in one user table and ``large_string`` in the
    other is ``large_string``, whichever comes first."""

    @pytest.mark.parametrize("n_rows", [2, 0])
    def test_either_argument_order_gives_large(self, n_rows: int) -> None:
        large = ArrowTableSource(
            pa.table(
                {"t": pa.array(["a", "b"][:n_rows], pa.large_string())}
            ).append_column("w", pa.array([1, 2][:n_rows], pa.int64())),
            tag_columns=["t"],
            source_id="large",
        )
        small = _other(n_rows, "z")
        forward = Join()(large, small).as_table(columns=ALL_COLUMNS)
        backward = Join()(small, large).as_table(columns=ALL_COLUMNS)
        assert forward.schema.field("t").type == pa.large_string()
        assert backward.schema.field("t").type == pa.large_string()
        assert non_large_columns(forward) == []
        assert non_large_columns(backward) == []


def test_every_source_kind_is_swept() -> None:
    """The sweep covers every source kind of ``large_types_cases``."""
    assert {source for source, _ in SOURCE_CASES} == set(SOURCES)
