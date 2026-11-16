"""Function-node tag tables store each input row's own source info (ITL-845).

A ``FunctionJobNode`` writes one tag-table (pipeline DB, ``pdb_v1``) row per input,
carrying that input's source-info tokens. A source-info token is the provenance
value a row holds for one data column: the value of its ``_source_<column>``
column, as ``Data.source_info()`` returns it (for example ``S::row_0::x``).
"Token" below always means a source-info token.

The sync orchestrator turns each upstream node's buffered rows into a stream before
handing it to the next node, and that step used to stamp row 0's source-info tokens
on every row, so every tag table written through a sync ``PipelineJob`` attributed
all inputs to the first one.

The later tests pin what happens to a database written before the fix: nothing is
rewritten or recomputed (the tag rows are cache hits and keep their old source-info
tokens), and dropping only the tag tables while keeping the result tables refills
them with correct source-info tokens without running any function again. Correcting
stored values is tracked in ITL-853.
"""

from __future__ import annotations

import shutil
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.operators import SelectDataColumns
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.system_constants import PIPELINE_DB_SCHEMA_VERSION

CALLS: list[str] = []

_JOB = "tags"
_FUNCTION_NODES = ("direct", "second", "via_op")
# Columns that identify a tag-table row and its cached result (plus the system tags).
_ID_COLUMNS = ("__data_id", "__input_data_hash", "__output_data_hash")


def add_xy(x: int, y: int) -> int:
    """Record the call and return ``x + y``."""
    CALLS.append("add_xy")
    return x + y


def times_two(z: int) -> int:
    """Record the call and return ``z * 2``."""
    CALLS.append("times_two")
    return z * 2


def add_one(x: int) -> int:
    """Record the call and return ``x + 1``."""
    CALLS.append("add_one")
    return x + 1


@pytest.fixture(autouse=True)
def _reset_calls() -> Iterator[None]:
    """Start and end every test with an empty call log."""
    CALLS.clear()
    yield
    CALLS.clear()


def _source() -> ArrowTableSource:
    """Build the three-row source (source-info tokens ``S::row_<i>::<column>``)."""
    table = pa.table(
        {
            "id": pa.array(["a", "b", "c"], pa.large_string()),
            "x": pa.array([1, 2, 3], pa.int64()),
            "y": pa.array([4, 5, 6], pa.int64()),
        }
    )
    table = table.cast(pa.schema([f.with_nullable(False) for f in table.schema]))
    return ArrowTableSource(
        table, tag_columns=["id"], source_id="S", tag_only_columns=True
    )


def _pod(function: Callable[..., int], output_key: str) -> FunctionPod:
    """Wrap ``function`` in a ``FunctionPod`` with one output key."""
    data_function = PythonDataFunction(function, output_keys=output_key)
    # PythonDataFunction does not structurally match DataFunctionProtocol yet
    # (same pre-existing mismatch as the baselined FunctionPod(pf) calls in tests).
    return FunctionPod(data_function)  # pyright: ignore[reportArgumentType]


def _job(store: ArrowDatabaseProtocol) -> PipelineJob:
    """source -> add_xy ("direct") -> times_two ("second"); source -> Select -> add_one."""
    src = _source()
    job = PipelineJob(name=_JOB, database=store)
    with job:
        z = _pod(add_xy, "z")(src, label="direct")
        _ = _pod(times_two, "w")(z, label="second")
        selected = SelectDataColumns(["x"])(src, label="sel")
        _ = _pod(add_one, "z")(selected, label="via_op")
    return job


def _run(job: PipelineJob, orchestrator: str = "sync") -> None:
    """Run ``job`` with the sync or the async orchestrator."""
    if orchestrator == "async":
        _ = job.run(orchestrator=AsyncPipelineOrchestrator())
    else:
        _ = job.run()


def _function_node(job: PipelineJob, label: str) -> FunctionJobNode:
    """Return the job's function node named ``label``."""
    node = job.nodes[label]
    assert isinstance(node, FunctionJobNode)
    return node


def _tag_rows(job: PipelineJob, label: str) -> list[dict[str, Any]]:
    """The node's stored records (tag-table rows joined to results), sorted by tag."""
    table = _function_node(job, label).get_all_records(
        columns={"source": True, "meta": True, "system_tags": True}
    )
    assert table is not None
    return sorted(table.to_pylist(), key=lambda row: str(row["id"]))


def _ids(row: dict[str, Any]) -> dict[str, Any]:
    """Return a stored row's identifying columns: IDs, data hashes, system tags."""
    return {c: v for c, v in row.items() if c in _ID_COLUMNS or c.startswith("_tag_")}


def _column(rows: Sequence[dict[str, Any]], column: str) -> list[Any]:
    """Return the values of ``column`` across ``rows``."""
    return [row[column] for row in rows]


def _input_source_info_tokens(column: str) -> list[str | None]:
    """Return the source's own source-info token for ``column`` on each row."""
    tokens = [data.source_info()[column] for _, data in _source().iter_data()]
    assert all(isinstance(t, str) for t in tokens)
    return [t if isinstance(t, str) else None for t in tokens]


def _output_source_info_tokens(job: PipelineJob, label: str, column: str) -> list[Any]:
    """Return the node's output source-info token for ``column`` per row, by tag."""
    rows = sorted(
        _function_node(job, label).iter_data(),
        key=lambda row: str(row[0].as_dict()["id"]),
    )
    return [data.source_info()[column] for _, data in rows]


def _broadcast_row_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make every materialization stamp row 0's source info, as before ITL-845."""
    original = StaticOutputOperatorPod._rows_to_table  # pyright: ignore[reportPrivateUsage]  # re-creating the pre-fix behaviour

    def broadcasting(
        rows: Sequence[tuple[TagProtocol, DataProtocol]],
    ) -> tuple[pa.Table, tuple[str, ...]]:
        """Stamp row 0's source info on every row, then build the table."""
        row_zero = rows[0][1].source_info()
        return original(
            [(tag, data.with_source_info(**row_zero)) for tag, data in rows]
        )

    monkeypatch.setattr(
        StaticOutputOperatorPod, "_rows_to_table", staticmethod(broadcasting)
    )


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
class TestFreshRunStoresPerRowSourceInfo:
    def test_source_into_function(self, orchestrator: str):
        """A function fed by a source stores each input row's own source-info tokens."""
        job = _job(InMemoryArrowDatabase())
        _run(job, orchestrator)
        rows = _tag_rows(job, "direct")
        assert _column(rows, "_source_x") == _input_source_info_tokens("x")
        assert _column(rows, "_source_y") == _input_source_info_tokens("y")

    def test_operator_into_function(self, orchestrator: str):
        """A function fed through an operator stores each row's source-info tokens."""
        job = _job(InMemoryArrowDatabase())
        _run(job, orchestrator)
        assert _column(
            _tag_rows(job, "via_op"), "_source_x"
        ) == _input_source_info_tokens("x")

    def test_function_into_function(self, orchestrator: str):
        """A function fed by a function stores each upstream source-info token."""
        job = _job(InMemoryArrowDatabase())
        _run(job, orchestrator)
        stored = _column(_tag_rows(job, "second"), "_source_z")
        expected = _output_source_info_tokens(job, "direct", "z")
        assert len(set(expected)) == 3
        assert stored == expected


class TestStoreWrittenBeforeFix:
    """A re-run never rewrites or recomputes tag rows stored before the fix."""

    def test_rerun_keeps_old_rows_and_runs_nothing(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """A re-run serves old rows from the cache without rewriting them."""
        store = InMemoryArrowDatabase()
        with monkeypatch.context() as patch:
            _broadcast_row_zero(patch)
            old_job = _job(store)
            _run(old_job)
        before = {label: _tag_rows(old_job, label) for label in _FUNCTION_NODES}
        row_zero = _input_source_info_tokens("x")[0]
        assert _column(before["direct"], "_source_x") == [row_zero] * 3
        assert sorted(CALLS) == sorted(["add_xy", "times_two", "add_one"] * 3)

        CALLS.clear()
        new_job = _job(store)
        _run(new_job)

        assert CALLS == []  # every input is a cache hit
        for label in _FUNCTION_NODES:
            assert _tag_rows(new_job, label) == before[label]  # not rewritten
        # The run's own output carries correct source-info tokens downstream regardless.
        assert len(set(_output_source_info_tokens(new_job, "direct", "z"))) == 3


class TestDropOnlyTagTables:
    """Dropping the tag tables but keeping the result tables refills them correctly.

    The result tables (``_result/.../rdb_v1``) serve every output by input data
    hash, so no function runs again; only ``__computed`` flips to ``False``.
    """

    def _pre_fix_store(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> tuple[DeltaTableDatabase, dict[str, list[dict[str, Any]]]]:
        """Write a Delta database the pre-fix way and read its tag tables back."""
        store = DeltaTableDatabase(tmp_path)
        with monkeypatch.context() as patch:
            _broadcast_row_zero(patch)
            _run(_job(store))
            store.flush()
        # Read back through a fresh handle: the writer's handle can serve a stale
        # view of a Delta table it has just written.
        reader = _job(DeltaTableDatabase(tmp_path))
        before = {label: _tag_rows(reader, label) for label in _FUNCTION_NODES}
        assert all(len(rows) == 3 for rows in before.values())
        CALLS.clear()
        return store, before

    def _drop_tag_table(self, tmp_path: Path, label: str) -> None:
        """Delete the node's ``pdb_v1`` tag table on disk."""
        node = _function_node(_job(InMemoryArrowDatabase()), label)
        path = tmp_path.joinpath(
            _JOB, *node.node_identity_path, PIPELINE_DB_SCHEMA_VERSION
        )
        assert path.is_dir()
        shutil.rmtree(path)

    def test_all_tag_tables_rebuilt_without_recomputation(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Dropped tag tables refill correctly with no function re-run."""
        _, before = self._pre_fix_store(tmp_path, monkeypatch)
        for label in _FUNCTION_NODES:
            self._drop_tag_table(tmp_path, label)

        store = DeltaTableDatabase(tmp_path)
        _run(_job(store))
        store.flush()
        job = _job(DeltaTableDatabase(tmp_path))

        assert CALLS == []
        after = {label: _tag_rows(job, label) for label in _FUNCTION_NODES}
        assert _column(after["direct"], "_source_x") == _input_source_info_tokens("x")
        assert _column(after["via_op"], "_source_x") == _input_source_info_tokens("x")
        assert len(set(_column(after["second"], "_source_z"))) == 3
        for label in _FUNCTION_NODES:
            for old, new in zip(before[label], after[label], strict=True):
                assert _ids(new) == _ids(old)
                assert new["__computed"] is False

    def test_dropping_one_tag_table_leaves_the_others(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Dropping one tag table leaves the others as they were."""
        _, before = self._pre_fix_store(tmp_path, monkeypatch)
        self._drop_tag_table(tmp_path, "direct")

        store = DeltaTableDatabase(tmp_path)
        _run(_job(store))
        store.flush()
        job = _job(DeltaTableDatabase(tmp_path))

        assert CALLS == []
        assert _column(
            _tag_rows(job, "direct"), "_source_x"
        ) == _input_source_info_tokens("x")
        assert _tag_rows(job, "second") == before["second"]
        assert _tag_rows(job, "via_op") == before["via_op"]
