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
