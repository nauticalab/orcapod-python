# tests/test_core/function_pod/test_post_run_hooks.py
"""Tests for FunctionPod post-run hooks (ITL-523).

Covers: registration, firing order, failure semantics, payload correctness,
filtered output, error status, cache hit status, parallel execution,
decorator convenience, and empty-hooks no-overhead path.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.executors.base import PythonFunctionExecutorBase
from orcapod.core.function_pod import FunctionPod, function_pod
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.hooks import HookConfig, InvocationStatus, PostRunPayload


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_stream(n: int = 2) -> ArrowTableStream:
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("x", pa.int64(), nullable=False),
    ])
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
        },
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=["id"])


def double(x: int) -> int:
    return x * 2


def _make_double_pod() -> FunctionPod:
    pf = PythonDataFunction(double, output_keys="result")
    return FunctionPod(pf)


# ---------------------------------------------------------------------------
# 1. Single hook fires with correct payload
# ---------------------------------------------------------------------------


class TestSingleHookPayload:
    def test_hook_fires_with_correct_payload(self):
        pod = _make_double_pod()
        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(payloads.append)

        stream = pod.process(_make_stream(n=1))
        list(stream.iter_data())

        assert len(payloads) == 1
        p = payloads[0]
        assert p.stats.status == InvocationStatus.COMPUTED
        assert p.stats.duration_ms >= 0
        assert p.stats.error is None
        assert p.output is not None
        assert p.record_id_hash == str(p.output.datagram_uuid)
        assert p.pod.label == pod.label
        assert p.pod.pod_hash == pod.content_hash().to_string()
        assert p.input is not None
        assert p.tag is not None

    def test_hook_fires_for_each_row(self):
        pod = _make_double_pod()
        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(payloads.append)

        stream = pod.process(_make_stream(n=3))
        list(stream.iter_data())

        assert len(payloads) == 3


# ---------------------------------------------------------------------------
# 2. Multiple hooks fire in registration order
# ---------------------------------------------------------------------------


class TestHookOrdering:
    def test_multiple_hooks_fire_in_order(self):
        pod = _make_double_pod()
        fired: list[str] = []
        pod.add_post_run_hook(lambda p: fired.append("first"))
        pod.add_post_run_hook(lambda p: fired.append("second"))

        stream = pod.process(_make_stream(n=1))
        list(stream.iter_data())

        assert fired == ["first", "second"]

    def test_hooks_fire_in_order_for_every_row(self):
        pod = _make_double_pod()
        fired: list[str] = []
        pod.add_post_run_hook(lambda p: fired.append("first"))
        pod.add_post_run_hook(lambda p: fired.append("second"))

        stream = pod.process(_make_stream(n=2))
        list(stream.iter_data())

        assert fired == ["first", "second", "first", "second"]


# ---------------------------------------------------------------------------
# 3. Fail-loud hook error
# ---------------------------------------------------------------------------


class TestHookFailureFast:
    def test_failing_hook_propagates_exception(self):
        pod = _make_double_pod()

        def bad_hook(p: PostRunPayload) -> None:
            raise ValueError("hook exploded")

        pod.add_post_run_hook(bad_hook)

        stream = pod.process(_make_stream(n=1))
        with pytest.raises(ValueError, match="hook exploded"):
            list(stream.iter_data())

    def test_failing_hook_stops_remaining_hooks(self):
        pod = _make_double_pod()
        second_fired: list[bool] = []

        def bad_hook(p: PostRunPayload) -> None:
            raise ValueError("stops here")

        pod.add_post_run_hook(bad_hook)
        pod.add_post_run_hook(lambda p: second_fired.append(True))

        stream = pod.process(_make_stream(n=1))
        with pytest.raises(ValueError):
            list(stream.iter_data())

        assert second_fired == []


# ---------------------------------------------------------------------------
# 4. Resilient hook error
# ---------------------------------------------------------------------------


class TestHookFailureResilient:
    def test_resilient_hook_suppresses_exception(self):
        pod = _make_double_pod()
        second_fired: list[bool] = []

        def bad_hook(p: PostRunPayload) -> None:
            raise ValueError("suppressed")

        def second_hook(p: PostRunPayload) -> None:
            second_fired.append(True)

        pod.add_post_run_hook(HookConfig(fn=bad_hook, on_error="log"))
        pod.add_post_run_hook(second_hook)

        stream = pod.process(_make_stream(n=1))
        results = list(stream.iter_data())

        assert results  # computation result returned
        assert second_fired == [True]  # next hook still fired

    def test_hookconfig_raise_is_same_as_plain_callable(self):
        pod = _make_double_pod()

        def bad_hook(p: PostRunPayload) -> None:
            raise ValueError("still loud")

        pod.add_post_run_hook(HookConfig(fn=bad_hook, on_error="raise"))

        stream = pod.process(_make_stream(n=1))
        with pytest.raises(ValueError, match="still loud"):
            list(stream.iter_data())


# ---------------------------------------------------------------------------
# 6. Error status (pod function raises)
# ---------------------------------------------------------------------------


class TestErrorStatus:
    def test_pod_error_fires_hook_with_error_status(self):
        def explodes(x: int) -> int:
            raise RuntimeError("boom")

        pf = PythonDataFunction(explodes, output_keys="result")
        pod = FunctionPod(pf)

        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(HookConfig(fn=payloads.append, on_error="log"))

        stream = pod.process(_make_stream(n=1))
        with pytest.raises(RuntimeError, match="boom"):
            list(stream.iter_data())

        assert len(payloads) == 1
        p = payloads[0]
        assert p.stats.status == InvocationStatus.ERROR
        assert isinstance(p.stats.error, RuntimeError)
        assert str(p.stats.error) == "boom"
        assert p.output is None
        assert p.record_id_hash is None

    def test_original_exception_reraises_after_hooks(self):
        def explodes(x: int) -> int:
            raise RuntimeError("original")

        pf = PythonDataFunction(explodes, output_keys="result")
        pod = FunctionPod(pf)
        pod.add_post_run_hook(HookConfig(fn=lambda p: None, on_error="log"))

        stream = pod.process(_make_stream(n=1))
        with pytest.raises(RuntimeError, match="original"):
            list(stream.iter_data())


# ---------------------------------------------------------------------------
# 7. Filtered output
# ---------------------------------------------------------------------------


class TestFilteredOutput:
    def test_filtered_row_fires_hook_with_none_output(self):
        pf = PythonDataFunction(double, output_keys="result")
        pf.set_active(False)  # set_active(False) causes process_data to return None output
        pod = FunctionPod(pf)

        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(payloads.append)

        stream = pod.process(_make_stream(n=1))
        results = list(stream.iter_data())

        assert results == []
        assert len(payloads) == 1
        p = payloads[0]
        assert p.output is None
        assert p.record_id_hash is None
        assert p.stats.status == InvocationStatus.COMPUTED
        assert p.stats.error is None


# ---------------------------------------------------------------------------
# 10. Empty hooks — no overhead path
# ---------------------------------------------------------------------------


class TestEmptyHooks:
    def test_pod_with_no_hooks_has_empty_list(self):
        pod = _make_double_pod()
        assert pod._post_run_hooks == []

    def test_pod_with_no_hooks_processes_normally(self):
        pod = _make_double_pod()
        stream = pod.process(_make_stream(n=2))
        results = list(stream.iter_data())
        assert len(results) == 2


# ---------------------------------------------------------------------------
# 8. Parallel execution (concurrent path via _iter_data_concurrent)
# ---------------------------------------------------------------------------


class _ConcurrentExecutor(PythonFunctionExecutorBase):
    """Minimal executor that marks supports_concurrent_execution=True."""

    @property
    def executor_type_id(self) -> str:
        return "test-concurrent"

    def supported_function_type_ids(self) -> frozenset[str]:
        return frozenset()

    @property
    def supports_concurrent_execution(self) -> bool:
        return True

    def execute_callable(self, fn, kwargs, executor_options=None, **kw):
        return fn(**kwargs)

    async def async_execute_callable(self, fn, kwargs, executor_options=None, **kw):
        return fn(**kwargs)


class TestParallelExecution:
    def test_hooks_fire_for_all_inputs_under_concurrent_executor(self):
        executor = _ConcurrentExecutor()
        pf = PythonDataFunction(double, output_keys="result", executor=executor)
        pod = FunctionPod(pf)

        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(payloads.append)

        stream = pod.process(_make_stream(n=4))
        results = list(stream.iter_data())

        assert len(results) == 4
        assert len(payloads) == 4
        assert all(
            p.stats.status == InvocationStatus.COMPUTED for p in payloads
        )


# ---------------------------------------------------------------------------
# async_execute path
# ---------------------------------------------------------------------------


class TestAsyncExecuteHooks:
    @pytest.mark.asyncio
    async def test_hooks_fire_through_async_execute(self):
        from orcapod.channels import Channel

        pod = _make_double_pod()
        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(payloads.append)

        stream = _make_stream(n=3)
        input_ch: Channel = Channel(buffer_size=16)
        output_ch: Channel = Channel(buffer_size=16)

        async def feed() -> None:
            for tag, data in stream.iter_data():
                await input_ch.writer.send((tag, data))
            await input_ch.writer.close()

        import asyncio
        await asyncio.gather(
            feed(),
            pod.async_execute([input_ch.reader], output_ch.writer),
        )

        results = await output_ch.reader.collect()

        assert len(results) == 3
        assert len(payloads) == 3
        assert all(p.stats.status == InvocationStatus.COMPUTED for p in payloads)


# ---------------------------------------------------------------------------
# 5. Cache hit status
# ---------------------------------------------------------------------------


class TestCacheHitStatus:
    def test_second_call_fires_hook_with_hit_status(self):
        inner = _make_double_pod()
        db = InMemoryArrowDatabase()
        pod = CachedFunctionPod(function_pod=inner, result_database=db)

        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(payloads.append)

        stream1 = pod.process(_make_stream(n=1))
        list(stream1.iter_data())

        stream2 = pod.process(_make_stream(n=1))
        list(stream2.iter_data())

        assert len(payloads) == 2
        assert payloads[0].stats.status == InvocationStatus.COMPUTED
        assert payloads[1].stats.status == InvocationStatus.HIT

    def test_cache_hit_payload_has_record_id(self):
        inner = _make_double_pod()
        db = InMemoryArrowDatabase()
        pod = CachedFunctionPod(function_pod=inner, result_database=db)

        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(payloads.append)

        stream1 = pod.process(_make_stream(n=1))
        list(stream1.iter_data())
        stream2 = pod.process(_make_stream(n=1))
        list(stream2.iter_data())

        assert payloads[1].record_id_hash is not None
        assert payloads[1].output is not None
