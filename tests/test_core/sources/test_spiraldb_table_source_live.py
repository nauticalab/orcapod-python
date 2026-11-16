"""SpiralDBTableSource against a live SpiralDB project (ITL-186). Opt-in.

Skipped unless ``ORCAPOD_SPIRAL_LIVE_PROJECT`` names a project the saved ``spiral login``
can read, with a ``default.spike_event`` table of more than 1,000 rows (the workshop
project is ``edi-tutorial-v1-841226``, 63,349 rows); the tests that need that many skip
when the table records fewer. Every test is read-only except
``test_rows_appended_between_runs_are_the_only_ones_computed`` and
``test_a_source_reads_the_version_it_was_built_at``, which also need
``ORCAPOD_SPIRAL_LIVE_WRITE=1`` and write only to a table each creates and drops in the
project's ``scratch`` dataset.

    ORCAPOD_SPIRAL_LIVE_PROJECT=edi-tutorial-v1-841226 \\
        uv run --extra spiraldb pytest tests/test_core/sources/test_spiraldb_table_source_live.py
"""

from __future__ import annotations

import os
import time
import uuid
from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import SourceTooLargeError
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.sources import SpiralDBTableSource

PROJECT = os.environ.get("ORCAPOD_SPIRAL_LIVE_PROJECT", "")
WRITE = os.environ.get("ORCAPOD_SPIRAL_LIVE_WRITE") == "1"

pytestmark = pytest.mark.skipif(
    not PROJECT, reason="Set ORCAPOD_SPIRAL_LIVE_PROJECT to run the live Spiral tests"
)

# The rows ``default.spike_event`` must record for the tests that read or refuse many.
_MIN_ROWS = 1_000


def _require_a_large_table() -> None:
    """Skip the calling test unless ``spike_event`` records more than ``_MIN_ROWS`` rows."""
    recorded = SpiralDBTableSource(
        PROJECT, "spike_event", tag_only_columns=True
    ).row_count_estimate
    if recorded <= _MIN_ROWS:
        pytest.skip(f"spike_event records {recorded} rows; these tests need more")


def test_building_a_source_is_quick_and_names_the_table():
    """Building a source takes under 10 seconds and names the table and its id."""
    start = time.monotonic()
    source = SpiralDBTableSource(PROJECT, "spike_event", tag_only_columns=True)
    elapsed = time.monotonic() - start

    assert elapsed < 10
    assert source.source_id == f"{PROJECT}/default.spike_event@{source.table_id}"
    assert source.table_id.startswith("table_")
    assert "unit_id" in source.keys()[0]
    assert source.row_count_estimate > 0


def test_limit_reads_the_first_rows():
    """``limit=1000`` reads 1,000 rows with distinct record ids."""
    _require_a_large_table()
    source = SpiralDBTableSource(
        PROJECT, "spike_event", limit=1000, tag_only_columns=True
    )

    table = source.as_table(all_info=True)

    assert table.num_rows == 1000
    (record_ids,) = [
        table.column(name)
        for name in table.column_names
        if name.startswith("_tag_record_id::")
    ]
    assert len(set(record_ids.to_pylist())) == 1000


def test_where_reads_only_matching_rows_with_the_same_record_ids():
    """``where`` reads only matching rows, each with its unfiltered record id."""
    first = SpiralDBTableSource(
        PROJECT, "spike_event", limit=1, tag_only_columns=True
    ).as_table(all_info=True)
    unit_id = first.column("unit_id")[0].as_py()
    source = SpiralDBTableSource(
        PROJECT,
        "spike_event",
        where={"unit_id": unit_id},
        max_rows=None,
        tag_only_columns=True,
    )

    table = source.as_table(all_info=True)

    assert table.num_rows > 0
    assert set(table.column("unit_id").to_pylist()) == {unit_id}
    record_id = next(n for n in table.column_names if n.startswith("_tag_record_id::"))
    assert first.column(record_id)[0].as_py() in table.column(record_id).to_pylist()


def test_a_whole_table_over_the_cap_is_refused_without_reading():
    """A table over ``max_rows`` is refused within 10 seconds, without a read."""
    _require_a_large_table()
    source = SpiralDBTableSource(
        PROJECT, "spike_event", max_rows=10, tag_only_columns=True
    )

    start = time.monotonic()
    with pytest.raises(SourceTooLargeError, match="limit="):
        source.as_table()
    assert time.monotonic() - start < 10


def test_an_async_job_over_a_limited_read(tmp_path: Path):
    """An async job computes a 500-row read once; a rerun computes nothing."""
    calls: list[float | None] = []

    @function_pod(output_keys="doubled")
    def double(spike_amplitude_uv: float | None) -> float | None:
        """Return twice ``spike_amplitude_uv``, recording the call."""
        calls.append(spike_amplitude_uv)
        return None if spike_amplitude_uv is None else 2 * spike_amplitude_uv

    def run() -> PipelineJob:
        """Build the job over a 500-row read and run it asynchronously."""
        job = PipelineJob(name="live", database=DeltaTableDatabase(tmp_path / "store"))
        with job:
            double.pod(
                SpiralDBTableSource(
                    PROJECT, "spike_event", limit=500, tag_only_columns=True
                ),
                label="double",
            )
        job.run(orchestrator=AsyncPipelineOrchestrator())
        return job

    assert run().nodes["double"].as_table().num_rows == 500
    assert 0 < len(calls) <= 500
    calls.clear()
    run()
    assert calls == []


def test_tags_as_data_gives_each_unit_its_spike_times_in_time_order():
    """Grouped on the unit key, each unit's copied spike times are in time order (ITL-996).

    ``timestamp`` is the only key column outside the unit key, so it alone orders the
    members of a group.
    """
    _require_a_large_table()
    spikes = SpiralDBTableSource(
        PROJECT,
        "spike_event",
        tags_as_data={"timestamp": "spike_time_us"},
        limit=20_000,
        tag_only_columns=True,
    )
    unit_key = [c for c in spikes.keys()[0] if c != "timestamp"]

    per_unit = spikes.group_by(unit_key).drop_tag_columns("timestamp")

    assert "spike_time_us" in per_unit.keys()[1]
    rows = per_unit.as_table().to_pylist()
    assert len(rows) > 1
    assert sum(len(r["spike_time_us"]) for r in rows) == spikes.as_table().num_rows
    for row in rows:
        assert row["spike_time_us"] == sorted(row["spike_time_us"])


@pytest.mark.skipif(
    not WRITE, reason="Set ORCAPOD_SPIRAL_LIVE_WRITE=1 to write a scratch table"
)
def test_rows_appended_between_runs_are_the_only_ones_computed(tmp_path: Path):
    """After rows are appended, a rerun computes only the new rows."""
    import spiral  # pyright: ignore[reportMissingImports]  # spiraldb extra, absent in the type-check job

    name = f"itl186_live_{uuid.uuid4().hex[:8]}"
    project = spiral.Spiral().project(PROJECT)
    table = project.create_table(f"scratch.{name}", key_schema=[("k", pa.int64())])
    calls: list[int | None] = []

    @function_pod(output_keys="w")
    def double(v: int | None) -> int | None:
        """Return twice ``v``, recording the call."""
        calls.append(v)
        return None if v is None else 2 * v

    def run() -> None:
        """Build the job over the scratch table and run it."""
        job = PipelineJob(
            name="append", database=DeltaTableDatabase(tmp_path / "store")
        )
        with job:
            double.pod(
                SpiralDBTableSource(
                    PROJECT, name, dataset="scratch", tag_only_columns=True
                ),
                label="double",
            )
        job.run()

    try:
        table.write(pa.table({"k": [2, 3], "v": [20, 30]}))
        run()
        assert set(calls) == {20, 30} and len(calls) == 2
        calls.clear()
        table.write(pa.table({"k": [1], "v": [10]}))
        run()
        assert calls == [10]
    finally:
        project.drop_table(f"scratch.{name}")


@pytest.mark.skipif(
    not WRITE, reason="Set ORCAPOD_SPIRAL_LIVE_WRITE=1 to write a scratch table"
)
def test_a_source_reads_the_version_it_was_built_at():
    """A source, and its row cap, use the table version current when it was built."""
    import spiral  # pyright: ignore[reportMissingImports]  # spiraldb extra, absent in the type-check job

    name = f"itl186_asof_{uuid.uuid4().hex[:8]}"
    project = spiral.Spiral().project(PROJECT)
    table = project.create_table(f"scratch.{name}", key_schema=[("k", pa.int64())])

    def keys(source: SpiralDBTableSource) -> list[int]:
        """Return the ``k`` key of every row ``source`` reads."""
        return cast("list[int]", source.as_table().column("k").to_pylist())

    try:
        table.write(pa.table({"k": [1, 2], "v": [10, 20]}))
        before = SpiralDBTableSource(
            PROJECT, name, dataset="scratch", tag_only_columns=True
        )
        table.write(pa.table({"k": [3], "v": [30]}))

        assert keys(before) == [1, 2]
        assert [tag.as_dict()["k"] for tag, _ in before.iter_data()] == [1, 2]
        assert keys(
            SpiralDBTableSource(PROJECT, name, dataset="scratch", tag_only_columns=True)
        ) == [1, 2, 3]

        # The cap counts the rows of the version the source reads, not the current one.
        capped = SpiralDBTableSource(
            PROJECT, name, dataset="scratch", max_rows=3, tag_only_columns=True
        )
        table.write(pa.table({"k": [4], "v": [40]}))
        assert capped.row_count_estimate == 3
        assert keys(capped) == [1, 2, 3]
    finally:
        project.drop_table(f"scratch.{name}")
