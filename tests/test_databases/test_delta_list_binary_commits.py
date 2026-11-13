"""A later commit to a Delta table holding a list of a binary-stored type (ITL-984).

Delta stores ``large_list<large_binary>`` as ``list<binary>``. A second commit used to
fail in the MERGE (``LargeList(LargeBinary)`` vs ``List(Binary)``), whichever runner
made it. The stored table must be what a single commit stores.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

import deltalake
import pyarrow as pa
import pytest

import orcapod as op
from orcapod.databases import DeltaTableDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

N_ROWS = 4


@op.function_pod(output_keys=["ids"])
def make_uuids(x: int) -> list[uuid.UUID]:
    return [uuid.uuid5(uuid.NAMESPACE_DNS, f"{x}-{i}") for i in range(2)]


@op.function_pod(output_keys=["ids"])
def make_strs(x: int) -> list[str]:
    return [f"{x}-{i}" for i in range(2)]


@op.function_pod(output_keys=["ids"])
def make_uuid(x: int) -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_DNS, str(x))


POD_CASES = [make_uuids, make_strs, make_uuid]


def _build(store: Path, pod: Any, n: int) -> PipelineJob:
    job = PipelineJob(name="j", store=DeltaTableDatabase(base_path=store))
    with job:
        src = op.sources.DictSource(
            [{"id": i, "x": i} for i in range(n)], tag_columns=["id"], source_id="s"
        )
        pod.pod(src, label="node")
    return job


def _result_table(store: Path) -> pa.Table:
    """The stored function-result table, read with Delta directly, sorted by record id."""
    (path,) = [p for p in store.rglob("rdb_v2") if (p / "_delta_log").is_dir()]
    table = deltalake.DeltaTable(str(path)).to_pyarrow_table()
    return table.sort_by("__record_id")


def _stored_form(table: pa.Table) -> list[tuple[str, str, bool]]:
    return [(f.name, str(f.type), f.nullable) for f in table.schema]


def _values(job: PipelineJob) -> list[Any]:
    return sorted(
        (row.as_dict()["ids"] for _, row in job.nodes["node"].iter_data()), key=repr
    )


@pytest.mark.parametrize("pod", POD_CASES, ids=lambda p: p.__name__)
@pytest.mark.parametrize(
    "runner",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)
def test_one_commit_per_row_stores_what_a_single_commit_stores(
    tmp_path: Path, pod: Any, runner: Any
) -> None:
    single = tmp_path / "single"
    reference = _build(single, pod, N_ROWS)
    reference.run()

    many = tmp_path / "many"
    job = _build(many, pod, N_ROWS)
    runner().run(job.dag)  # the result cache flushes once per row

    expected, actual = _result_table(single), _result_table(many)
    assert _stored_form(actual) == _stored_form(expected)
    assert actual.num_rows == N_ROWS
    assert _values(job) == _values(reference)


@pytest.mark.parametrize("pod", POD_CASES, ids=lambda p: p.__name__)
def test_second_run_that_adds_rows(tmp_path: Path, pod: Any) -> None:
    _build(tmp_path / "grown", pod, 2).run()
    job = _build(tmp_path / "grown", pod, N_ROWS)
    job.run()

    single = tmp_path / "single"
    reference = _build(single, pod, N_ROWS)
    reference.run()

    assert _stored_form(_result_table(tmp_path / "grown")) == _stored_form(
        _result_table(single)
    )
    assert _values(job) == _values(reference)


def test_read_back_has_the_logical_type(tmp_path: Path) -> None:
    job = _build(tmp_path, make_uuids, N_ROWS)
    AsyncPipelineOrchestrator().run(job.dag)
    reopened = _build(tmp_path, make_uuids, N_ROWS)
    reopened.run()  # every row is stored: it reads them back
    rows = [row.as_dict()["ids"] for _, row in reopened.nodes["node"].iter_data()]
    assert len(rows) == N_ROWS
    assert all(isinstance(u, uuid.UUID) for ids in rows for u in ids)


def _bytes_rows(ids: list[bytes], values: list[list[bytes | None] | None]) -> pa.Table:
    return pa.table(
        {
            "__record_id": pa.array(ids, pa.large_binary()),
            "v": pa.array(values, pa.large_list(pa.large_binary())),
        }
    )


def test_list_bytes_batch_holding_a_stored_id(tmp_path: Path) -> None:
    db = DeltaTableDatabase(base_path=tmp_path)
    path = ("t",)
    db.add_records(path, _bytes_rows([b"r0", b"r1"], [[b"a"], [b"b"]]), flush=True)
    db.add_records(
        path,
        _bytes_rows([b"r1", b"r2", b"r3", b"r4"], [[b"X"], [b"c"], None, [None, b"d"]]),
        flush=True,
    )

    table = DeltaTableDatabase(base_path=tmp_path).get_all_records(
        path, record_id_column="__record_id"
    )  # reopened
    assert table is not None
    got = dict(zip(table["__record_id"].to_pylist(), table["v"].to_pylist()))
    # the stored id keeps its stored row (skipped, as when_not_matched_insert_all does)
    assert got == {
        b"r0": [b"a"],
        b"r1": [b"b"],
        b"r2": [b"c"],
        b"r3": None,
        b"r4": [None, b"d"],
    }
