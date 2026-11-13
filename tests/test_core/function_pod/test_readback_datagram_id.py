"""A result row read back from the store carries its stored ``__data_id`` (ITL-1060)."""

from __future__ import annotations

import uuid

import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import ArrowTableSource


@function_pod(output_keys="y")
def times_ten(x: int) -> int:
    """Return ``10 * x``."""
    return 10 * x


def source() -> ArrowTableSource:
    """Three rows, not in tag order."""
    table = pa.table(
        {"k": ["c", "a", "b"], "x": [3, 1, 2]},
        schema=pa.schema(
            [
                pa.field("k", pa.large_string(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["k"], source_id="src")


def build(store: InMemoryArrowDatabase) -> PipelineJob:
    job = PipelineJob(name="itl1060", store=store)
    with job:
        times_ten.pod(source(), label="node")
    return job


def stored_ids(job: PipelineJob) -> dict[str, uuid.UUID]:
    """Stored ``__data_id`` by tag ``k``."""
    records = job.nodes["node"].get_all_records(all_info=True)
    assert records is not None
    return {
        k: uuid.UUID(bytes=bytes(i))
        for k, i in zip(
            records.column("k").to_pylist(),
            records.column("__data_id").to_pylist(),
            strict=True,
        )
    }


def read_back_ids(job: PipelineJob) -> dict[str, uuid.UUID]:
    """Datagram id of each row the node yields, by tag ``k``."""
    return {
        str(tag.as_dict()["k"]): data.datagram_uuid
        for tag, data in job.nodes["node"].iter_data()
    }


def test_rows_read_back_by_a_second_run_carry_their_stored_id() -> None:
    store = InMemoryArrowDatabase()
    _ = build(store).run(orchestrator=SyncPipelineOrchestrator())

    second = build(store)
    _ = second.run(orchestrator=SyncPipelineOrchestrator())
    stored = stored_ids(second)
    assert len(stored) == 3
    assert read_back_ids(second) == stored
    # reading the same stored row twice gives the same id
    assert read_back_ids(second) == read_back_ids(second)


def test_async_read_back_carries_the_stored_id() -> None:
    store = InMemoryArrowDatabase()
    _ = build(store).run(orchestrator=SyncPipelineOrchestrator())

    second = build(store)
    _ = second.run(orchestrator=AsyncPipelineOrchestrator())
    assert read_back_ids(second) == stored_ids(second)


def test_a_fresh_job_on_the_store_reads_the_stored_ids() -> None:
    store = InMemoryArrowDatabase()
    first = build(store)
    _ = first.run(orchestrator=SyncPipelineOrchestrator())
    stored = stored_ids(first)

    fresh = build(store)  # never run: every row comes from the store
    assert read_back_ids(fresh) == stored


def test_a_pod_cached_row_read_back_carries_the_id_it_was_stored_under() -> None:
    from orcapod.core.cached_function_pod import CachedFunctionPod
    from orcapod.core.streams.arrow_table_stream import ArrowTableStream

    table = pa.table(
        {"k": ["c", "a", "b"], "x": [3, 1, 2]},
        schema=pa.schema(
            [
                pa.field("k", pa.large_string(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )

    store = InMemoryArrowDatabase()

    def ids() -> dict[str, uuid.UUID]:
        pod = CachedFunctionPod(
            function_pod=times_ten.pod,  # pyright: ignore[reportArgumentType]  # FunctionPod lacks from_config, as in CachedFunctionPod.process
            result_database=store,
        )
        stream = pod.process(ArrowTableStream(table, tag_columns=["k"]))
        return {str(t.as_dict()["k"]): d.datagram_uuid for t, d in stream.iter_data()}

    first = ids()
    assert ids() == first  # the second read takes every row from the cache


def test_a_read_back_row_is_the_same_row_apart_from_its_id() -> None:
    store = InMemoryArrowDatabase()
    first = build(store)
    _ = first.run(orchestrator=SyncPipelineOrchestrator())
    computed = {t.as_dict()["k"]: d for t, d in first.nodes["node"].iter_data()}
    second = build(store)
    for tag, data in second.nodes["node"].iter_data():
        before = computed[tag.as_dict()["k"]]
        assert data.content_hash() == before.content_hash()
        assert data.source_info() == before.source_info()
        # the computed row also carries the ``__computed`` meta column; the rest is equal
        columns = {"source": True, "context": True}
        assert data.as_table(columns=columns).equals(before.as_table(columns=columns))
