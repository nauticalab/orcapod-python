# tests/test_core/side_effect_function/test_side_effect_function_pod.py
from __future__ import annotations

import pytest
import pyarrow as pa

from orcapod.core.streams import ArrowTableStream
from orcapod.side_effects import InvocationContext


def _make_stream(n: int = 3) -> ArrowTableStream:
    """Simple stream: tag=id (int), data=value (int)."""
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("value", pa.int64(), nullable=False),
    ])
    table = pa.table(
        {"id": list(range(n)), "value": list(range(n))},
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=["id"])


def _make_in_memory_db():
    """Return a fresh in-memory ArrowDatabase."""
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
    return InMemoryArrowDatabase()


class TestSideEffectFunctionPodSchema:
    """SF-01, SF-02, SF-03, SF-10: schema inference and ctx stripping."""

    def test_sf01_ctx_stripped_from_input_schema(self):
        """SF-01: 'ctx' param stripped; data params form the input schema."""
        from orcapod.core.side_effect_function import SideEffectFunctionPod

        def my_fn(value: int, ctx: InvocationContext) -> str:
            return f"result_{value}"

        pod = SideEffectFunctionPod(my_fn, output_keys=["result"])

        # Input schema excludes 'ctx'
        assert "ctx" not in pod.input_data_schema
        assert "value" in pod.input_data_schema
        assert pod.input_data_schema["value"] == int

        # Output schema has the declared key
        assert "result" in pod.output_data_schema
        assert pod.output_data_schema["result"] == str

    def test_sf02_custom_ctx_arg_name(self):
        """SF-02: ctx_arg_name='context' — stripped and injected by correct name."""
        from orcapod.core.side_effect_function import SideEffectFunctionPod

        def my_fn(value: int, context: InvocationContext) -> str:
            return f"r_{value}"

        pod = SideEffectFunctionPod(my_fn, output_keys=["result"], ctx_arg_name="context")
        assert "context" not in pod.input_data_schema
        assert "value" in pod.input_data_schema

    def test_sf03_missing_ctx_arg_raises_at_construction(self):
        """SF-03: Missing ctx_arg_name raises ValueError at construction time."""
        from orcapod.core.side_effect_function import SideEffectFunctionPod

        def my_fn(value: int) -> str:
            return str(value)

        with pytest.raises(ValueError, match="ctx_arg_name"):
            SideEffectFunctionPod(my_fn, output_keys=["result"])
            # Default ctx_arg_name="ctx" is missing from my_fn's signature

    def test_sf10_node_uri_shape(self):
        """SF-10: node_uri starts with 'side_effect_function' and has 5 elements."""
        from orcapod.core.side_effect_function import SideEffectFunctionPod

        def my_fn(value: int, ctx: InvocationContext) -> str:
            return str(value)

        pod = SideEffectFunctionPod(my_fn, output_keys=["result"])
        assert pod.uri[0] == "side_effect_function"
        assert pod.uri[-1] == "python_side_effect_function"
        assert len(pod.uri) == 5
        assert pod.uri[3] == "v1"


class TestSideEffectFunctionPodStreamStandalone:
    """SF-04, SF-05: standalone execution via SideEffectFunctionPodStream."""

    def test_sf04_iter_data_returns_correct_output(self):
        """SF-04: iter_data() returns correct (tag, output_data) per row."""
        from orcapod.core.side_effect_function import SideEffectFunctionPod

        def my_fn(value: int, ctx: InvocationContext) -> str:
            return f"v{value}"

        pod = SideEffectFunctionPod(my_fn, output_keys=["result"])
        stream = _make_stream(3)
        rows = list(pod.process(stream).iter_data())

        assert len(rows) == 3
        for i, (tag, data) in enumerate(rows):
            assert data.as_dict()["result"] == f"v{i}"
        # Tags pass through unchanged
        assert rows[0][0].as_dict()["id"] == 0
        assert rows[1][0].as_dict()["id"] == 1

    def test_sf05_invocation_context_fields_standalone(self):
        """SF-05: InvocationContext has pod_name, non-empty hash, pipeline_run_id=None."""
        from orcapod.core.side_effect_function import SideEffectFunctionPod

        received_ctx: list[InvocationContext] = []

        def my_fn(value: int, ctx: InvocationContext) -> str:
            received_ctx.append(ctx)
            return str(value)

        pod = SideEffectFunctionPod(my_fn, output_keys=["result"])
        stream = _make_stream(1)
        list(pod.process(stream).iter_data())

        assert len(received_ctx) == 1
        ctx = received_ctx[0]
        assert ctx.pod_name == pod.label
        assert isinstance(ctx.invocation_hash, str)
        assert len(ctx.invocation_hash) > 0
        assert "::" in ctx.invocation_hash
        assert ctx.pipeline_run_id is None  # standalone: no run_id


class TestSideEffectFunctionJobNode:
    """SF-06, SF-07, SF-08, SF-09: DB-backed sync execution."""

    def test_sf06_output_cached_after_first_run(self):
        """SF-06: Output cached; second run returns cached result without re-calling fn."""
        from orcapod.core.side_effect_function import SideEffectFunctionPod, SideEffectFunctionJobNode

        call_count = 0

        def my_fn(value: int, ctx: InvocationContext) -> str:
            nonlocal call_count
            call_count += 1
            return f"r{value}"

        pod = SideEffectFunctionPod(my_fn, output_keys=["result"])
        stream = _make_stream(2)
        pipeline_db = _make_in_memory_db()
        result_db = _make_in_memory_db()

        node1 = SideEffectFunctionJobNode(pod=pod, input_stream=stream)
        node1.attach_databases(pipeline_database=pipeline_db, result_database=result_db)
        results1 = node1.execute(stream)
        assert len(results1) == 2
        assert call_count == 2

        # Second run — same pod, same data, same DBs — fn must NOT be called again
        node2 = SideEffectFunctionJobNode(pod=pod, input_stream=stream)
        node2.attach_databases(pipeline_database=pipeline_db, result_database=result_db)
        results2 = node2.execute(stream)
        assert len(results2) == 2
        assert call_count == 2  # NOT incremented — cache hit

        # Both runs produce equal result values
        for (_, d1), (_, d2) in zip(results1, results2):
            assert d1.as_dict()["result"] == d2.as_dict()["result"]

    def test_sf07_invocation_log_written_on_first_run(self):
        """SF-07: Invocation log row written to pipeline_database on first run."""
        import polars as pl
        from orcapod.core.side_effect_function import SideEffectFunctionPod, SideEffectFunctionJobNode

        def my_fn(value: int, ctx: InvocationContext) -> str:
            return f"r{value}"

        pod = SideEffectFunctionPod(my_fn, output_keys=["result"])
        stream = _make_stream(3)
        pipeline_db = _make_in_memory_db()
        result_db = _make_in_memory_db()
        node = SideEffectFunctionJobNode(pod=pod, input_stream=stream)
        node.attach_databases(pipeline_database=pipeline_db, result_database=result_db)
        results = node.execute(stream)

        assert len(results) == 3

        # Invocation log has 3 rows
        table_path = node._table_path
        records = pipeline_db.get_all_records(table_path)
        assert records is not None
        df = pl.from_arrow(records)
        assert len(df) == 3
        assert "record_id_hash" in df.columns
        assert "executed_at" in df.columns

    def test_sf08_track_completion_false_always_reruns(self):
        """SF-08: track_completion=False — fn called every run; invocation logged each time."""
        from orcapod.side_effects import SideEffectPodConfig
        from orcapod.core.side_effect_function import SideEffectFunctionPod, SideEffectFunctionJobNode

        call_count = 0

        def my_fn(value: int, ctx: InvocationContext) -> str:
            nonlocal call_count
            call_count += 1
            return f"r{value}"

        cfg = SideEffectPodConfig(track_completion=False)
        pod = SideEffectFunctionPod(my_fn, output_keys=["result"], config=cfg)
        stream = _make_stream(2)
        pipeline_db = _make_in_memory_db()
        result_db = _make_in_memory_db()

        node1 = SideEffectFunctionJobNode(pod=pod, input_stream=stream)
        node1.attach_databases(pipeline_database=pipeline_db, result_database=result_db)
        node1.execute(stream)
        assert call_count == 2

        node2 = SideEffectFunctionJobNode(pod=pod, input_stream=stream)
        node2.attach_databases(pipeline_database=pipeline_db, result_database=result_db)
        node2.execute(stream)
        assert call_count == 4  # called again — track_completion=False

        # Invocation log must grow with each run (track_completion=False means always re-log)
        import polars as pl
        records = pipeline_db.get_all_records(node2._table_path)
        assert records is not None
        df = pl.from_arrow(records)
        assert len(df) == 4  # 2 rows from run 1 + 2 rows from run 2

    def test_sf09_on_error_log_reraises(self):
        """SF-09: on_error='log' — exception logged then always re-raised."""
        from orcapod.side_effects import SideEffectPodConfig
        from orcapod.core.side_effect_function import SideEffectFunctionPod, SideEffectFunctionJobNode

        def my_fn(value: int, ctx: InvocationContext) -> str:
            raise RuntimeError("test error")

        cfg = SideEffectPodConfig(on_error="log")
        pod = SideEffectFunctionPod(my_fn, output_keys=["result"], config=cfg)
        stream = _make_stream(1)
        pipeline_db = _make_in_memory_db()
        result_db = _make_in_memory_db()
        node = SideEffectFunctionJobNode(pod=pod, input_stream=stream)
        node.attach_databases(pipeline_database=pipeline_db, result_database=result_db)

        # Must propagate — no silent row suppression
        with pytest.raises(RuntimeError, match="test error"):
            node.execute(stream)

        # Invocation log must NOT be written when fn raises
        records = pipeline_db.get_all_records(node._table_path)
        assert records is None or len(records) == 0


class TestSideEffectFunctionPodDecorator:
    """SF-11: @side_effect_function_pod decorator."""

    def test_sf11_decorator_creates_correct_pod(self):
        """SF-11: Decorator creates a SideEffectFunctionPod with correct URI."""
        from orcapod.core.side_effect_function import (
            SideEffectFunctionPod,
            side_effect_function_pod,
        )

        @side_effect_function_pod(output_keys=["result"])
        def my_fn(value: int, ctx: InvocationContext) -> str:
            return str(value)

        assert isinstance(my_fn, SideEffectFunctionPod)
        assert my_fn.uri[0] == "side_effect_function"
        assert my_fn.canonical_function_name == "my_fn"

    def test_sf11_decorator_accessible_from_public_api(self):
        """SF-11: Decorator and pod class accessible from orcapod top-level."""
        import orcapod
        assert hasattr(orcapod, "side_effect_function_pod")
        assert hasattr(orcapod, "SideEffectFunctionPod")


class TestSideEffectFunctionPodPipelineIntegration:
    """SF-12: Full pipeline compilation and execution."""

    def test_sf12_pipeline_compilation_and_execution(self):
        """SF-12: SideEffectFunctionJobNode compiled, fn called, invocation logged."""
        import polars as pl
        from orcapod.pipeline.job import PipelineJob
        from orcapod.core.side_effect_function import SideEffectFunctionPod, SideEffectFunctionJobNode
        from orcapod.core.sources.dict_source import DictSource

        received_ctx: list[InvocationContext] = []

        def transform(value: int, ctx: InvocationContext) -> str:
            received_ctx.append(ctx)
            return f"result_{value}"

        pod = SideEffectFunctionPod(transform, output_keys=["result"])
        db = _make_in_memory_db()

        with PipelineJob(name="test_sef", store=db) as job:
            source = DictSource(
                [{"id": 0, "value": 10}, {"id": 1, "value": 20}],
                tag_columns=["id"],
            )
            pod.process(source)

        job.run()

        assert len(received_ctx) == 2  # fn called once per row

        # Find the side_effect_function node in the compiled graph
        sef_nodes = [
            n for n in job.dag.nodes()
            if getattr(n, "node_type", None) == "side_effect_function"
        ]
        assert len(sef_nodes) == 1

        # Verify the invocation log was written to the pipeline_db
        node = sef_nodes[0]
        pipeline_db = db.at("test_sef")
        table_path = node._table_path
        records = pipeline_db.get_all_records(table_path)
        assert records is not None
        df = pl.from_arrow(records)
        assert len(df) == 2
        assert "record_id_hash" in df.columns

    def test_sf12_second_pipeline_run_uses_cache(self):
        """SF-12: Second pipeline run uses cached output; fn not called again."""
        from orcapod.pipeline.job import PipelineJob
        from orcapod.core.side_effect_function import SideEffectFunctionPod
        from orcapod.core.sources.dict_source import DictSource

        call_count = 0

        def transform(value: int, ctx: InvocationContext) -> str:
            nonlocal call_count
            call_count += 1
            return f"r{value}"

        pod = SideEffectFunctionPod(transform, output_keys=["result"])
        db = _make_in_memory_db()
        source_data = [{"id": 0, "value": 10}, {"id": 1, "value": 20}]

        with PipelineJob(name="test_sef_cache", store=db) as job1:
            source1 = DictSource(source_data, tag_columns=["id"])
            pod.process(source1)
        job1.run()
        assert call_count == 2

        with PipelineJob(name="test_sef_cache", store=db) as job2:
            source2 = DictSource(source_data, tag_columns=["id"])
            pod.process(source2)
        job2.run()
        assert call_count == 2  # NOT called again — cache hit


class TestSideEffectFunctionJobNodeAsync:
    """SF-13: async_execute produces same output as sync path."""

    def test_sf13_async_execute_basic(self):
        """SF-13: async_execute processes all rows, writes cache + log, returns correct output."""
        import asyncio
        import polars as pl
        from orcapod.core.side_effect_function import SideEffectFunctionPod, SideEffectFunctionJobNode
        from orcapod.channels import Channel

        call_count = 0

        def my_fn(value: int, ctx: InvocationContext) -> str:
            nonlocal call_count
            call_count += 1
            return f"async_{value}"

        pod = SideEffectFunctionPod(my_fn, output_keys=["result"])
        stream = _make_stream(3)
        pipeline_db = _make_in_memory_db()
        result_db = _make_in_memory_db()
        node = SideEffectFunctionJobNode(pod=pod, input_stream=stream)
        node.attach_databases(pipeline_database=pipeline_db, result_database=result_db)

        async def _run():
            ch_in = Channel(buffer_size=10)
            ch_out = Channel(buffer_size=10)

            async def feed():
                for tag, data in stream.iter_data():
                    await ch_in.writer.send((tag, data))
                await ch_in.writer.close()

            await asyncio.gather(
                feed(),
                node.async_execute(
                    [ch_in.reader], ch_out.writer, run_id="test-run-sf13"
                ),
            )
            return await ch_out.reader.collect()

        results = asyncio.run(_run())
        assert len(results) == 3
        assert call_count == 3

        # Verify invocation log written
        table_path = node._table_path
        records = pipeline_db.get_all_records(table_path)
        assert records is not None
        df = pl.from_arrow(records)
        assert len(df) == 3
        assert "record_id_hash" in df.columns

        # Verify output values
        output_values = [data.as_dict()["result"] for _, data in results]
        for i in range(3):
            assert f"async_{i}" in output_values
