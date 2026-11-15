"""A SpiralDB source's rows keep their own derived columns across scan batches (ITL-1034).

``SpiralDBTableSource`` builds provenance batch by batch (each row's token is the labelled
Arrow hash of its primary key) and concatenates the batches before a sort. Against the
disk-backed pyspiral fake, read two rows per scan batch, a six-row table whose tag
(``grp``, not a key column) interleaves across the batches is checked through
``iter_data``, ``async_iter_data`` and every ``as_table`` configuration (the sorted ones
reorder rows across batch boundaries). The oracle recomputes each token from the test's
own key columns by the documented recipe. No live Spiral call is made.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.sources import SpiralDBTableSource
from tests.test_databases import fake_pyspiral

from .checker import (
    Layout,
    assert_requested_columns,
    assert_rows_aligned,
    rows_table,
)
from .expectations import rows
from .fixtures import COLUMN_CONFIGS, content_hash_column, schema_hash_of
from .oracle import Oracle

PROJECT = "alignment"
SOURCE_ID = "spiral_events"
KEYS = pa.schema(
    [pa.field("subject_id", pa.string()), pa.field("timestamp", pa.uint64())]
)
# Key order (the scan order) is timestamp order; grp interleaves across the batches.
TIMESTAMPS = [1, 2, 3, 4, 5, 6]
GROUPS = ["c", "a", "b", "a", "c", "b"]


def written_table() -> pa.Table:
    """The rows the test writes, in key order: keys, the tag ``grp`` and ``amplitude``."""
    table = pa.table(
        {
            "subject_id": ["m1"] * len(TIMESTAMPS),
            "timestamp": pa.array(TIMESTAMPS, pa.uint64()),
            "grp": GROUPS,
            "amplitude": pa.array([t + 0.5 for t in TIMESTAMPS], pa.float32()),
        }
    )
    return table.cast(pa.schema([pa.field(f.name, f.type, True) for f in table.schema]))


def key_tokens(table: pa.Table) -> list[str]:
    """Each row's token by the documented recipe: the labelled Arrow hash of its key.

    The key fields are hashed as non-nullable (a key is never null).
    """
    keys = table.select(KEYS.names)
    keys = keys.cast(pa.schema([f.with_nullable(False) for f in keys.schema]))
    hasher = get_default_context().arrow_hasher
    return [hasher.hash_table(keys.slice(i, 1)).to_string() for i in range(len(keys))]


@pytest.fixture()
def source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """A source over the fake table, scanned two rows per batch, and its oracle."""
    monkeypatch.setattr(fake_pyspiral, "scan_batch_rows", 2)
    with fake_pyspiral.install(tmp_path / "spiral"):
        project = fake_pyspiral.FakeSpiral().project(PROJECT)
        project.create_table("default.event", key_schema=KEYS).write(written_table())
        yield SpiralDBTableSource(
            PROJECT, "event", tag_columns=["grp"], source_id=SOURCE_ID, batch_rows=2
        )


def oracle_of(table: pa.Table, source: Any) -> Oracle:
    """The oracle of the written rows of ``source``, with their key-hash tokens."""
    oracle = Oracle()
    oracle.add_source(
        SOURCE_ID,
        table,
        tag_columns=["grp"],
        schema_hash=schema_hash_of(source),
        tokens=key_tokens(table),
    )
    return oracle


def test_iterated_rows_are_aligned_across_scan_batches(source: Any) -> None:
    """``iter_data`` and ``async_iter_data`` yield every row once, each its own."""
    oracle = oracle_of(written_table(), source)
    tags, data = source.keys()
    layout = Layout(tag_keys=tuple(tags), data_keys=tuple(data))

    async def collect() -> list[Any]:
        """The rows ``async_iter_data`` yields."""
        return [pair async for pair in source.async_iter_data()]

    for read in (list(source.iter_data()), asyncio.run(collect())):
        assert_rows_aligned(
            rows_table(read),
            layout,
            oracle,
            source.data_context,
            expected_rows=rows(oracle, SOURCE_ID),
        )


@pytest.mark.parametrize("config_name", list(COLUMN_CONFIGS))
def test_tables_are_aligned_across_scan_batches(source: Any, config_name: str) -> None:
    """Every configuration, the sorted ones reordering rows across batch boundaries."""
    oracle = oracle_of(written_table(), source)
    config = COLUMN_CONFIGS[config_name]
    tags, data = source.keys()
    layout = Layout(
        tag_keys=tuple(tags),
        data_keys=tuple(data),
        content_hash_column=content_hash_column(config),
    )
    table = source.as_table(**config)
    assert table.num_rows == len(TIMESTAMPS)
    assert_requested_columns(table, config, layout)
    assert_rows_aligned(
        table,
        layout,
        oracle,
        source.data_context,
        expected_rows=rows(oracle, SOURCE_ID),
    )


@pytest.mark.parametrize("config_name", list(COLUMN_CONFIGS))
@pytest.mark.parametrize("n", [1, 3])
def test_previews_are_aligned_across_scan_batches(
    source: Any, n: int, config_name: str
) -> None:
    """``head(n)`` is a bounded read of the first ``n`` rows in key order (scan order).

    1 row stays inside the first two-row batch, 3 cross into the second. Each preview
    holds exactly those rows, each aligned, in key order, or in tag order when the
    configuration sorts (the rows the bounded read holds, sorted).
    """
    written = written_table()
    oracle = oracle_of(written, source)
    config = COLUMN_CONFIGS[config_name]
    tags, data = source.keys()
    layout = Layout(
        tag_keys=tuple(tags),
        data_keys=tuple(data),
        content_hash_column=content_hash_column(config),
    )
    table = source.head(n, **config).as_table()
    assert table.num_rows == n
    assert_requested_columns(table, config, layout)
    assert_rows_aligned(
        table,
        layout,
        oracle,
        source.data_context,
        expected_rows=rows(oracle, SOURCE_ID)[:n],
    )
    if not (config.get("all_info") or config.get("columns", {}).get("sort_by_tags")):
        assert table.equals(source.as_table(**config).slice(0, n)), (
            "an unsorted preview is not the first rows of as_table"
        )
