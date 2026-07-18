# tests/test_core/function_pod/test_function_pod_ctx_arg.py
"""Tests for the ctx_arg parameter on the @function_pod decorator.

Full parity with tests/test_core/side_effect_function/test_side_effect_function_pod.py,
exercising @function_pod(ctx_arg=...) as the preferred side-effect pod entry point.
"""
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


def _make_pipeline_db():
    """Return a fresh in-memory ArrowDatabase."""
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
    return InMemoryArrowDatabase()


class TestCtxArgSchema:
    """Scenarios 1–2: schema inference and ctx stripping via @function_pod(ctx_arg=...)."""

    def test_ctx01_ctx_stripped_from_input_schema(self):
        """Scenario 1: ctx param absent from pod.input_data_schema; present in data function."""
        from orcapod.core.function_pod import function_pod

        @function_pod(output_keys="result", ctx_arg="ctx")
        def my_fn(value: int, ctx: InvocationContext) -> str:
            return f"result_{value}"

        pod = my_fn.pod

        # Pod's exposed input schema excludes 'ctx'
        assert "ctx" not in pod.input_data_schema
        assert "value" in pod.input_data_schema
        assert pod.input_data_schema["value"] == int

        # Underlying data function retains full schema (ctx included for hashing)
        assert "ctx" in pod._data_function.input_data_schema

        # Output schema has the declared key
        assert "result" in pod._data_function.output_data_schema
        assert pod._data_function.output_data_schema["result"] == str

    def test_ctx02_ctx_arg_name_property(self):
        """Scenario 2: pod.ctx_arg_name reflects the configured arg name."""
        from orcapod.core.function_pod import function_pod

        @function_pod(output_keys="result", ctx_arg="invocation_ctx")
        def my_fn(value: int, invocation_ctx: InvocationContext) -> str:
            return f"r_{value}"

        pod = my_fn.pod
        assert pod.ctx_arg_name == "invocation_ctx"
        assert "invocation_ctx" not in pod.input_data_schema
        assert "value" in pod.input_data_schema


class TestCtxArgStandaloneExecution:
    """Scenarios 3–4: standalone execution via pod.process() / FunctionPodStream."""

    def test_ctx03_sync_execution_injects_ctx_per_row(self):
        """Scenario 3: ctx injected per row; output values correct; tags pass through."""
        from orcapod.core.function_pod import function_pod

        received_ctx: list[InvocationContext] = []

        @function_pod(output_keys="result", ctx_arg="ctx")
        def my_fn(value: int, ctx: InvocationContext) -> str:
            received_ctx.append(ctx)
            return f"v{value}"

        pod = my_fn.pod
        stream = _make_stream(3)
        rows = list(pod.process(stream).iter_data())

        assert len(rows) == 3
        assert len(received_ctx) == 3

        # Each ctx is a proper InvocationContext
        for ctx in received_ctx:
            assert isinstance(ctx, InvocationContext)
            assert isinstance(ctx.invocation_hash, str)
            assert len(ctx.invocation_hash) > 0
            assert "::" in ctx.invocation_hash

        # Standalone execution: no pipeline run id; pod_name matches pod label
        assert received_ctx[0].pipeline_run_id is None
        assert received_ctx[0].pod_name == pod.label

        # Output values match expected
        for i, (tag, data) in enumerate(rows):
            assert data.as_dict()["result"] == f"v{i}"

        # Tags pass through unchanged
        assert rows[0][0].as_dict()["id"] == 0
        assert rows[2][0].as_dict()["id"] == 2

    def test_ctx04_async_fn_routed_through_sync_path(self):
        """Scenario 4: async user function runs correctly via synchronous wrapper."""
        import asyncio
        from orcapod.core.function_pod import function_pod

        @function_pod(output_keys="result", ctx_arg="ctx")
        async def my_async_fn(value: int, ctx: InvocationContext) -> str:
            await asyncio.sleep(0)
            return f"async_{value}"

        pod = my_async_fn.pod
        stream = _make_stream(2)
        rows = list(pod.process(stream).iter_data())

        assert len(rows) == 2
        assert rows[0][1].as_dict()["result"] == "async_0"
        assert rows[1][1].as_dict()["result"] == "async_1"


class TestCtxArgDBBacked:
    """Scenarios 5–6: DB-backed execution via FunctionJobNode and PipelineJob."""

    def test_ctx05_function_job_node_caches_output(self):
        """Scenario 5: second run uses cached result; fn not called again."""
        from orcapod.core.function_pod import function_pod
        from orcapod.core.nodes.function_node import FunctionJobNode

        call_count = 0

        @function_pod(output_keys="result", ctx_arg="ctx")
        def my_fn(value: int, ctx: InvocationContext) -> str:
            nonlocal call_count
            call_count += 1
            return f"r{value}"

        pod = my_fn.pod
        stream = _make_stream(2)
        pipeline_db = _make_pipeline_db()

        node1 = FunctionJobNode(function_pod=pod, input_stream=stream)
        node1.attach_databases(pipeline_database=pipeline_db)
        results1 = node1.execute(stream)
        assert len(results1) == 2
        assert call_count == 2

        # Second run — same pod, same data, same DB — must NOT call fn again
        node2 = FunctionJobNode(function_pod=pod, input_stream=stream)
        node2.attach_databases(pipeline_database=pipeline_db)
        results2 = node2.execute(stream)
        assert len(results2) == 2
        assert call_count == 2  # NOT incremented

        # Both runs produce identical result values
        for (_, d1), (_, d2) in zip(results1, results2):
            assert d1.as_dict()["result"] == d2.as_dict()["result"]

    def test_ctx06_pipeline_job_integration(self):
        """Scenario 6: end-to-end PipelineJob compilation + execution with ctx_arg pod."""
        from orcapod.core.function_pod import function_pod
        from orcapod.pipeline.job import PipelineJob
        from orcapod.core.sources.dict_source import DictSource

        received_ctx: list[InvocationContext] = []

        @function_pod(output_keys="result", ctx_arg="ctx")
        def transform(value: int, ctx: InvocationContext) -> str:
            received_ctx.append(ctx)
            return f"result_{value}"

        pod = transform.pod
        db = _make_pipeline_db()

        with PipelineJob(name="test_ctx_pod", store=db) as job:
            source = DictSource(
                [{"id": 0, "value": 10}, {"id": 1, "value": 20}],
                tag_columns=["id"],
            )
            pod.process(source)

        job.run()

        # fn called once per row, ctx injected each time
        assert len(received_ctx) == 2
        for ctx in received_ctx:
            assert isinstance(ctx, InvocationContext)


class TestCtxArgDecoratorForms:
    """Scenarios 7–8: decorator usage with ctx_arg."""

    def test_ctx07_decorator_minimal_form(self):
        """Scenario 7: @function_pod(output_keys='result', ctx_arg='ctx') — pod attached correctly."""
        from orcapod.core.function_pod import function_pod, FunctionPod

        @function_pod(output_keys="result", ctx_arg="ctx")
        def my_fn(value: int, ctx: InvocationContext) -> str:
            return f"v{value}"

        # my_fn is still callable (wraps original fn)
        assert callable(my_fn)
        # .pod is attached
        pod = my_fn.pod
        assert isinstance(pod, FunctionPod)
        assert pod.ctx_arg_name == "ctx"
        # Output key is set correctly
        assert "result" in pod._data_function.output_data_schema

    def test_ctx08_decorator_with_explicit_output_keys(self):
        """Scenario 8: @function_pod(output_keys=['out'], ctx_arg='ctx') — explicit output_keys."""
        from orcapod.core.function_pod import function_pod, FunctionPod

        @function_pod(output_keys=["out"], ctx_arg="ctx")
        def my_fn(value: int, ctx: InvocationContext) -> str:
            return f"v{value}"

        pod = my_fn.pod
        assert isinstance(pod, FunctionPod)
        assert pod.ctx_arg_name == "ctx"
        assert "out" in pod._data_function.output_data_schema
        assert "ctx" not in pod.input_data_schema


class TestCtxArgInvalidParam:
    """Scenario 9: ValueError at construction time when ctx_arg names a parameter not present in the function."""

    def test_ctx09_invalid_ctx_arg_raises_at_decoration(self):
        """Scenario 9: ValueError at decoration time if ctx_arg is not a parameter of the function.

        ``_FunctionPodBase.__init__`` raises immediately when ``ctx_arg_name`` is not
        in the underlying data function's input schema.
        """
        from orcapod.core.function_pod import function_pod

        with pytest.raises(ValueError, match="ctx_arg_name"):
            @function_pod(output_keys="result", ctx_arg="nonexistent_param")
            def my_fn(value: int) -> str:
                return str(value)


class TestCtxArgCachedPodWrapping:
    """Scenario 10: @function_pod(ctx_arg=..., pod_cache_database=...) — CachedFunctionPod wrapping."""

    def test_ctx10_cached_pod_injects_ctx_on_miss_skips_on_hit(self):
        """Scenario 10: CachedFunctionPod wrapping a ctx_arg pod:
        - ctx stripped from exposed schema
        - InvocationContext injected on cache miss
        - fn NOT called on cache hit; output same
        """
        from orcapod.core.function_pod import function_pod
        from orcapod.core.cached_function_pod import CachedFunctionPod

        received_ctx: list[InvocationContext] = []
        call_count = 0

        db = _make_pipeline_db()

        @function_pod(output_keys="result", ctx_arg="ctx", pod_cache_database=db)
        def my_fn(value: int, ctx: InvocationContext) -> str:
            nonlocal call_count
            call_count += 1
            received_ctx.append(ctx)
            return f"v{value}"

        pod = my_fn.pod
        assert isinstance(pod, CachedFunctionPod)

        # Schema: ctx stripped from wrapped pod's exposed input schema
        assert "ctx" not in pod.input_data_schema
        assert "value" in pod.input_data_schema
        assert pod.ctx_arg_name == "ctx"

        stream = _make_stream(2)

        # First run: cache miss → fn called, InvocationContext injected
        rows1 = list(pod.process(stream).iter_data())
        assert call_count == 2
        assert len(received_ctx) == 2
        assert len(rows1) == 2
        for ctx in received_ctx:
            assert isinstance(ctx, InvocationContext)
            assert len(ctx.invocation_hash) > 0

        # Second run: cache hit → fn NOT called
        rows2 = list(pod.process(stream).iter_data())
        assert call_count == 2  # unchanged — cache hit

        # Output values must match
        for (_, d1), (_, d2) in zip(rows1, rows2):
            assert d1.as_dict()["result"] == d2.as_dict()["result"]
