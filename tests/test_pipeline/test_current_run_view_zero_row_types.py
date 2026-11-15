"""A job node's current-run view has one Arrow schema in every state (ITL-1095, ITL-912).

ITL-1095: a job node shows its current run, and reads zero rows before a run, after
``clear_cache()``, after a run that raised and while stale. ITL-912: a table with no rows
has exactly the Arrow schema of the node's rows (types, nullability, system tags, source
info, column order), derived from the input schemas. Together: ``as_table(all_info=True)``
of a function node and of operator nodes (``group_by``, ``join``) has the same schema in
each state, over inputs whose types are not the defaults (an ``int8`` non-null tag, an
``int32`` data column).
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import ArrowTableSource
from orcapod.system_constants import constants

ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}
NODES = ("inc", "grouped", "joined")
FAIL = {"on": False}


@function_pod(output_keys="y")
def inc(x: int) -> int:
    """Return ``x + 1``; raise while ``FAIL["on"]`` is set."""
    if FAIL["on"]:
        raise RuntimeError("inc failed on purpose")
    return x + 1


@pytest.fixture(autouse=True)
def _reset_failure() -> None:
    """Let ``inc`` succeed at the start of each test."""
    FAIL["on"] = False


def _source(data: str, source_id: str) -> ArrowTableSource:
    """Tags ``g``, ``id`` (``int8`` non-null) and one ``int32`` non-null data column."""
    schema = pa.schema(
        [
            pa.field("g", pa.int8(), nullable=False),
            pa.field("id", pa.int8(), nullable=False),
            pa.field(data, pa.int32(), nullable=False),
        ]
    )
    table = pa.table(
        {"g": [0, 1, 0, 1], "id": [0, 1, 2, 3], data: [10, 20, 30, 40]}, schema=schema
    )
    return ArrowTableSource(table, tag_columns=["g", "id"], source_id=source_id)


def _job(db: InMemoryArrowDatabase | None = None) -> PipelineJob:
    """``inc`` (function) -> ``grouped`` (GroupBy on ``g``); ``inc`` joined with ``b``."""
    job = PipelineJob(name="j", database=db or InMemoryArrowDatabase())
    with job:
        rowwise: Any = inc.pod(_source("x", "a"), label="inc")
        _ = rowwise.group_by("g", label="grouped")
        _ = rowwise.join(_source("z", "b"), label="joined")
    return job


def _assert_reads_with(
    job: PipelineJob, expected: dict[str, pa.Schema], *, rows: bool
) -> None:
    """Each node reads rows (or none) and has exactly the ``expected`` schema."""
    for label in NODES:
        table = job.nodes[label].as_table(all_info=True)
        assert (table.num_rows > 0) is rows, label
        assert table.schema.equals(expected[label]), (
            label,
            table.schema,
            expected[label],
        )


def _assert_empty_with(job: PipelineJob, expected: dict[str, pa.Schema]) -> None:
    """Each node reads zero rows and has exactly the ``expected`` schema."""
    _assert_reads_with(job, expected, rows=False)


def _run_schemas(orchestrator: str) -> dict[str, pa.Schema]:
    """The schema of each node's populated table after a run of a fresh job."""
    job = _job()
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    schemas: dict[str, pa.Schema] = {}
    for label in NODES:
        table = job.nodes[label].as_table(all_info=True)
        assert table.num_rows > 0, label
        schemas[label] = table.schema
    return schemas


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
class TestOneSchemaInEveryState:
    """``as_table(all_info=True)`` of every node keeps its run's schema in each state."""

    def test_the_populated_tables_keep_the_narrow_input_types(
        self, orchestrator: str
    ) -> None:
        """The reference schemas carry the input's types and the system tags."""
        schemas = _run_schemas(orchestrator)
        for label in ("inc", "joined"):
            assert schemas[label].field("id").type == pa.int8(), label
            assert not schemas[label].field("id").nullable, label
        assert schemas["grouped"].field("g").type == pa.int8()
        assert schemas["joined"].field("z").type == pa.int32()
        for label in NODES:
            assert any(
                n.startswith(constants.SYSTEM_TAG_PREFIX) for n in schemas[label].names
            ), label

    def test_before_a_run(self, orchestrator: str) -> None:
        """A fresh job: every node reads zero rows with its run's schema."""
        _assert_empty_with(_job(), _run_schemas(orchestrator))

    def test_after_a_run_and_after_clear_cache(self, orchestrator: str) -> None:
        """A run fills each node; ``clear_cache()`` empties it with the same schema."""
        expected = _run_schemas(orchestrator)
        job = _job()
        _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
        _assert_reads_with(job, expected, rows=True)
        for label in NODES:
            job.nodes[label].clear_cache()
        _assert_empty_with(job, expected)

    def test_after_a_failed_run(self, orchestrator: str) -> None:
        """A run that raised leaves every node empty, with its run's schema."""
        expected = _run_schemas(orchestrator)
        job = _job()
        FAIL["on"] = True
        with pytest.raises(RuntimeError, match="inc failed on purpose"):
            _ = job.run(
                orchestrator=ORCHESTRATORS[orchestrator](error_policy="fail_fast")
            )
        _assert_empty_with(job, expected)

    def test_while_stale(self, orchestrator: str) -> None:
        """A rebound source makes every node stale and empty, with the same schema."""
        expected = _run_schemas(orchestrator)
        job = _job()
        _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
        # A new source object of the same schema makes every node below it stale.
        slot = next(name for name, src in job.sources.items() if "x" in src.keys()[1])
        job.bind(sources={slot: _source("x", "a")})
        for label in NODES:
            assert job.nodes[label].is_stale, label
        _assert_empty_with(job, expected)

    def test_after_a_load_only_run(self, orchestrator: str) -> None:
        """A load-only run over a database filled by a run shows that run's schema."""
        expected = _run_schemas(orchestrator)
        db = InMemoryArrowDatabase()
        _ = _job(db).run(orchestrator=ORCHESTRATORS[orchestrator]())
        job = _job(db)
        FAIL["on"] = True  # a load-only run never calls the function
        _ = job.run(orchestrator=ORCHESTRATORS[orchestrator](), load_only=True)
        _assert_reads_with(job, expected, rows=True)

    def test_after_a_load_only_run_with_nothing_stored(self, orchestrator: str) -> None:
        """A load-only run over an empty database fills every node with zero rows.

        Each has its populated table's schema, column order included (ITL-1131).
        """
        expected = _run_schemas(orchestrator)
        job = _load_only_over_an_empty_store(orchestrator)
        for label in NODES:
            table = job.nodes[label].as_table(all_info=True)
            assert table.num_rows == 0, label
            assert table.schema.equals(expected[label]), label

    def test_a_join_with_no_input_rows_keeps_its_column_order(
        self, orchestrator: str
    ) -> None:
        """The join that received no rows has its populated schema, in order (ITL-1131)."""
        expected = _run_schemas(orchestrator)["joined"]
        job = _load_only_over_an_empty_store(orchestrator)
        assert job.nodes["joined"].as_table(all_info=True).schema.equals(expected)


def _load_only_over_an_empty_store(orchestrator: str) -> PipelineJob:
    """A job after a load-only run over an empty database: every node holds zero rows."""
    job = _job()
    FAIL["on"] = True  # a load-only run never calls the function
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator](), load_only=True)
    return job
