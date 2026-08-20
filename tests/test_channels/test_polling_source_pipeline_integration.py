"""Integration tests: PollingSource bound into PipelineJob + AsyncPipelineOrchestrator.

Verifies that the async polling loop runs (not just a static snapshot) when
a PollingSource is bound as the data source for a PipelineJob and executed
via the async orchestrator.
"""
from __future__ import annotations

import pytest
import pyarrow as pa

from orcapod import function_pod
from orcapod.core.nodes.source_node import SourceJobNode
from orcapod.core.sources.polling_source import PollingSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import UnboundSourceError
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.types import Cursor, PollingConfig, Schema


# ---------------------------------------------------------------------------
# Shared fake impl — two-batch DynamicSourceProtocol
# ---------------------------------------------------------------------------


class _TwoBatchImpl:
    """Serves exactly 2 batches then reports no new data.

    batch 0: id=1, val=10
    batch 1: id=2, val=20
    """

    _BATCHES = [
        {"id": pa.array([1], type=pa.int64()), "val": pa.array([10], type=pa.int64())},
        {"id": pa.array([2], type=pa.int64()), "val": pa.array([20], type=pa.int64())},
    ]

    def identity(self):
        return "_TwoBatchImpl"

    def to_config(self):
        return None

    @classmethod
    def from_config(cls, config):
        return cls()

    def schema(self):
        return Schema({"id": int, "val": int})

    async def poll(self, cursor=None):
        idx = cursor.value if cursor is not None else 0
        return idx < len(self._BATCHES)

    async def fetch(self, cursor=None):
        idx = cursor.value if cursor is not None else 0
        if idx >= len(self._BATCHES):
            return Cursor(value=idx), {}
        return Cursor(value=idx + 1), self._BATCHES[idx]

    async def close(self):
        pass


# ---------------------------------------------------------------------------
# Downstream function pod used by integration tests
# ---------------------------------------------------------------------------


@function_pod(output_keys="doubled")
def _double_val(val: int) -> int:
    """Doubles the incoming value — used to produce a queryable DB record."""
    return val * 2


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPollingSourcePipelineJobIntegration:
    @pytest.mark.asyncio
    async def test_async_orchestrator_runs_polling_loop(self):
        """PollingSource bound to PipelineJob runs the full async polling loop.

        A static snapshot would process only the first batch (id=1, val=10)
        and produce 1 DB record. The polling loop must process both batches and
        produce 2 DB records.
        """
        src = PollingSource(
            _TwoBatchImpl(),
            tag_columns="id",
            polling_config=PollingConfig(
                interval=0.05,
                duration=0.5,
                max_missed_intervals=50,
            ),
            source_id="two_batch_src",
        )

        store = InMemoryArrowDatabase()
        job = PipelineJob(name="polling_integration_test", store=store)
        with job:
            _double_val.pod(src, label="doubled")

        await AsyncPipelineOrchestrator().run_async(job.dag)
        job.store.at(*job._name).flush()
        job.store.at(*job._name).at("_result").flush()

        records = job.nodes["doubled"].get_all_records()
        assert records is not None
        # Both batches must be processed — static snapshot yields only 1 row
        assert records.num_rows == 2, (
            f"Expected 2 rows (one per batch), got {records.num_rows}. "
            "This means only the static snapshot ran, not the polling loop."
        )
        # get_all_records() returns tag + output columns only (not input data).
        # Verify both batches by checking the tag (id) and output (doubled) columns.
        ids = sorted(records.column("id").to_pylist())
        assert ids == [1, 2]
        doubled = sorted(records.column("doubled").to_pylist())
        assert doubled == [20, 40]

    @pytest.mark.asyncio
    async def test_unbound_source_job_node_async_iter_raises(self):
        """Unbound SourceJobNode.async_iter_data() raises UnboundSourceError."""
        node = SourceJobNode(
            name="unbound",
            tag_schema=Schema({"id": int}),
            data_schema=Schema({"val": int}),
            bound_source=None,
        )

        with pytest.raises(UnboundSourceError):
            async for _ in node.async_iter_data():
                pass
