"""
Tests for the packet function executor system.

Covers:
- PacketFunctionExecutorBase (supports, get_execution_data)
- LocalExecutor (in-process execution)
- Executor as property on PacketFunctionBase (get/set/validation)
- Executor routing in call() / direct_call()
- Executor delegation through PacketFunctionWrapper / CachedPacketFunction
- Custom executor with restricted type support
"""

from __future__ import annotations

from typing import Any

import pytest

from orcapod.core.datagrams import Packet
from orcapod.core.executors import LocalExecutor, PacketFunctionExecutorBase
from orcapod.core.packet_function import (
    CachedPacketFunction,
    PacketFunctionWrapper,
    PythonPacketFunction,
)
from orcapod.protocols.core_protocols import (
    PacketFunctionExecutorProtocol,
    PacketFunctionProtocol,
    PacketProtocol,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def add(x: int, y: int) -> int:
    return x + y


def noop(x: int) -> int:
    return x


class SpyExecutor(PacketFunctionExecutorBase):
    """Executor that records calls for testing."""

    def __init__(self, supported_types: frozenset[str] | None = None) -> None:
        self._supported = supported_types or frozenset()
        self.calls: list[tuple[Any, Any]] = []

    @property
    def executor_type_id(self) -> str:
        return "spy"

    def supported_function_type_ids(self) -> frozenset[str]:
        return self._supported

    def execute(
        self,
        packet_function: PacketFunctionProtocol,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        self.calls.append((packet_function, packet))
        return packet_function.direct_call(packet)


class PythonOnlyExecutor(PacketFunctionExecutorBase):
    """Executor that only supports python.function.v0."""

    @property
    def executor_type_id(self) -> str:
        return "python-only"

    def supported_function_type_ids(self) -> frozenset[str]:
        return frozenset({"python.function.v0"})

    def execute(
        self,
        packet_function: PacketFunctionProtocol,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        return packet_function.direct_call(packet)


class NonPythonExecutor(PacketFunctionExecutorBase):
    """Executor that explicitly does NOT support python.function.v0."""

    @property
    def executor_type_id(self) -> str:
        return "non-python"

    def supported_function_type_ids(self) -> frozenset[str]:
        return frozenset({"wasm.function.v0"})

    def execute(
        self,
        packet_function: PacketFunctionProtocol,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        return packet_function.direct_call(packet)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def add_pf() -> PythonPacketFunction:
    return PythonPacketFunction(add, output_keys="result")


@pytest.fixture
def add_packet() -> Packet:
    return Packet({"x": 1, "y": 2})


@pytest.fixture
def spy_executor() -> SpyExecutor:
    return SpyExecutor()


@pytest.fixture
def local_executor() -> LocalExecutor:
    return LocalExecutor()


# ---------------------------------------------------------------------------
# 1. PacketFunctionExecutorBase
# ---------------------------------------------------------------------------


class TestPacketFunctionExecutorBase:
    def test_supports_all_when_empty_frozenset(self):
        executor = SpyExecutor(supported_types=frozenset())
        assert executor.supports("python.function.v0")
        assert executor.supports("wasm.function.v0")
        assert executor.supports("anything")

    def test_supports_restricted_types(self):
        executor = PythonOnlyExecutor()
        assert executor.supports("python.function.v0")
        assert not executor.supports("wasm.function.v0")

    def test_get_execution_data_returns_type(self):
        executor = SpyExecutor()
        data = executor.get_execution_data()
        assert data["executor_type"] == "spy"


# ---------------------------------------------------------------------------
# 2. LocalExecutor
# ---------------------------------------------------------------------------


class TestLocalExecutor:
    def test_executor_type_id(self, local_executor: LocalExecutor):
        assert local_executor.executor_type_id == "local"

    def test_supports_all_types(self, local_executor: LocalExecutor):
        assert local_executor.supports("python.function.v0")
        assert local_executor.supports("anything.v99")

    def test_execute_delegates_to_direct_call(
        self, local_executor: LocalExecutor, add_pf: PythonPacketFunction, add_packet: Packet
    ):
        result = local_executor.execute(add_pf, add_packet)
        assert result is not None
        assert result.as_dict()["result"] == 3

    def test_get_execution_data(self, local_executor: LocalExecutor):
        data = local_executor.get_execution_data()
        assert data["executor_type"] == "local"


# ---------------------------------------------------------------------------
# 3. Executor as property on PacketFunctionBase
# ---------------------------------------------------------------------------


class TestExecutorProperty:
    def test_default_executor_is_none(self, add_pf: PythonPacketFunction):
        assert add_pf.executor is None

    def test_set_executor(self, add_pf: PythonPacketFunction, spy_executor: SpyExecutor):
        add_pf.executor = spy_executor
        assert add_pf.executor is spy_executor

    def test_unset_executor(self, add_pf: PythonPacketFunction, spy_executor: SpyExecutor):
        add_pf.executor = spy_executor
        add_pf.executor = None
        assert add_pf.executor is None

    def test_set_compatible_executor(self, add_pf: PythonPacketFunction):
        executor = PythonOnlyExecutor()
        add_pf.executor = executor
        assert add_pf.executor is executor

    def test_set_incompatible_executor_raises(self, add_pf: PythonPacketFunction):
        executor = NonPythonExecutor()
        with pytest.raises(TypeError, match="does not support"):
            add_pf.executor = executor

    def test_executor_via_constructor(self):
        executor = PythonOnlyExecutor()
        pf = PythonPacketFunction(add, output_keys="result", executor=executor)
        assert pf.executor is executor


# ---------------------------------------------------------------------------
# 4. Executor routing in call() / direct_call()
# ---------------------------------------------------------------------------


class TestExecutorRouting:
    def test_call_without_executor_uses_direct_call(
        self, add_pf: PythonPacketFunction, add_packet: Packet
    ):
        result = add_pf.call(add_packet)
        assert result is not None
        assert result.as_dict()["result"] == 3

    def test_call_with_executor_routes_through_executor(
        self, add_pf: PythonPacketFunction, add_packet: Packet, spy_executor: SpyExecutor
    ):
        add_pf.executor = spy_executor
        result = add_pf.call(add_packet)
        assert result is not None
        assert result.as_dict()["result"] == 3
        assert len(spy_executor.calls) == 1
        assert spy_executor.calls[0][0] is add_pf

    def test_direct_call_bypasses_executor(
        self, add_pf: PythonPacketFunction, add_packet: Packet, spy_executor: SpyExecutor
    ):
        add_pf.executor = spy_executor
        result = add_pf.direct_call(add_packet)
        assert result is not None
        assert result.as_dict()["result"] == 3
        # Executor was NOT called
        assert len(spy_executor.calls) == 0

    def test_swapping_executor_changes_routing(
        self, add_pf: PythonPacketFunction, add_packet: Packet
    ):
        spy1 = SpyExecutor()
        spy2 = SpyExecutor()

        add_pf.executor = spy1
        add_pf.call(add_packet)
        assert len(spy1.calls) == 1
        assert len(spy2.calls) == 0

        add_pf.executor = spy2
        add_pf.call(add_packet)
        assert len(spy1.calls) == 1
        assert len(spy2.calls) == 1

    def test_unsetting_executor_reverts_to_direct(
        self, add_pf: PythonPacketFunction, add_packet: Packet, spy_executor: SpyExecutor
    ):
        add_pf.executor = spy_executor
        add_pf.call(add_packet)
        assert len(spy_executor.calls) == 1

        add_pf.executor = None
        add_pf.call(add_packet)
        # No additional executor calls
        assert len(spy_executor.calls) == 1


# ---------------------------------------------------------------------------
# 5. Executor delegation through wrappers
# ---------------------------------------------------------------------------


class TestWrapperExecutorDelegation:
    def test_wrapper_executor_reads_from_wrapped(self, add_pf: PythonPacketFunction):
        spy = SpyExecutor()
        add_pf.executor = spy

        class SimpleWrapper(PacketFunctionWrapper):
            pass

        wrapper = SimpleWrapper(add_pf, version="v0.0")
        assert wrapper.executor is spy

    def test_wrapper_executor_set_targets_wrapped(self, add_pf: PythonPacketFunction):
        spy = SpyExecutor()

        class SimpleWrapper(PacketFunctionWrapper):
            pass

        wrapper = SimpleWrapper(add_pf, version="v0.0")
        wrapper.executor = spy
        assert add_pf.executor is spy

    def test_wrapper_call_routes_through_inner_executor(
        self, add_pf: PythonPacketFunction, add_packet: Packet
    ):
        spy = SpyExecutor()
        add_pf.executor = spy

        class SimpleWrapper(PacketFunctionWrapper):
            pass

        wrapper = SimpleWrapper(add_pf, version="v0.0")
        result = wrapper.call(add_packet)
        assert result is not None
        assert result.as_dict()["result"] == 3
        assert len(spy.calls) == 1


# ---------------------------------------------------------------------------
# 6. Protocol conformance
# ---------------------------------------------------------------------------


class TestProtocolConformance:
    def test_local_executor_satisfies_protocol(self):
        executor = LocalExecutor()
        assert isinstance(executor, PacketFunctionExecutorProtocol)

    def test_spy_executor_satisfies_protocol(self):
        executor = SpyExecutor()
        assert isinstance(executor, PacketFunctionExecutorProtocol)

    def test_packet_function_with_executor_satisfies_protocol(self):
        pf = PythonPacketFunction(add, output_keys="result")
        pf.executor = LocalExecutor()
        assert isinstance(pf, PacketFunctionProtocol)


# ---------------------------------------------------------------------------
# 7. Executor access through FunctionPod / FunctionNode
# ---------------------------------------------------------------------------


def _make_add_stream(rows: list[dict] | None = None):
    """Helper to create an ArrowTableStream suitable for the ``add`` function."""
    import pyarrow as pa

    from orcapod.core.streams.arrow_table_stream import ArrowTableStream

    if rows is None:
        rows = [{"id": 0, "x": 1, "y": 2}, {"id": 1, "x": 3, "y": 4}]
    table = pa.table(
        {k: pa.array([r[k] for r in rows], type=pa.int64()) for k in rows[0]}
    )
    return ArrowTableStream(table, tag_columns=["id"])


class TestFunctionPodExecutorAccess:
    def test_pod_executor_reads_from_packet_function(self):
        from orcapod.core.function_pod import FunctionPod

        spy = SpyExecutor()
        pf = PythonPacketFunction(add, output_keys="result")
        pf.executor = spy
        pod = FunctionPod(pf)
        assert pod.executor is spy

    def test_pod_executor_set_targets_packet_function(self):
        from orcapod.core.function_pod import FunctionPod

        spy = SpyExecutor()
        pf = PythonPacketFunction(add, output_keys="result")
        pod = FunctionPod(pf)
        pod.executor = spy
        assert pf.executor is spy

    def test_pod_executor_unset(self):
        from orcapod.core.function_pod import FunctionPod

        spy = SpyExecutor()
        pf = PythonPacketFunction(add, output_keys="result")
        pf.executor = spy
        pod = FunctionPod(pf)
        pod.executor = None
        assert pf.executor is None

    def test_pod_process_uses_executor(self):
        from orcapod.core.function_pod import FunctionPod

        spy = SpyExecutor()
        pf = PythonPacketFunction(add, output_keys="result")
        pf.executor = spy
        pod = FunctionPod(pf)

        stream = _make_add_stream()
        output_stream = pod.process(stream)

        results = list(output_stream.iter_packets())
        assert len(results) == 2
        assert results[0][1].as_dict()["result"] == 3
        assert results[1][1].as_dict()["result"] == 7
        assert len(spy.calls) == 2


class TestFunctionPodStreamExecutorAccess:
    def test_stream_executor_reads_from_packet_function(self):
        from orcapod.core.function_pod import FunctionPod

        spy = SpyExecutor()
        pf = PythonPacketFunction(add, output_keys="result")
        pf.executor = spy
        pod = FunctionPod(pf)

        stream = pod.process(_make_add_stream())
        assert stream.executor is spy

    def test_stream_executor_set_targets_packet_function(self):
        from orcapod.core.function_pod import FunctionPod

        spy = SpyExecutor()
        pf = PythonPacketFunction(add, output_keys="result")
        pod = FunctionPod(pf)

        stream = pod.process(_make_add_stream())
        stream.executor = spy
        assert pf.executor is spy


class TestFunctionNodeExecutorAccess:
    def test_node_executor_reads_from_packet_function(self):
        from orcapod.core.function_pod import FunctionNode, FunctionPod

        spy = SpyExecutor()
        pf = PythonPacketFunction(add, output_keys="result")
        pf.executor = spy
        pod = FunctionPod(pf)

        node = FunctionNode(pod, _make_add_stream())
        assert node.executor is spy

    def test_node_executor_set_targets_packet_function(self):
        from orcapod.core.function_pod import FunctionNode, FunctionPod

        spy = SpyExecutor()
        pf = PythonPacketFunction(add, output_keys="result")
        pod = FunctionPod(pf)

        node = FunctionNode(pod, _make_add_stream())
        node.executor = spy
        assert pf.executor is spy

    def test_node_iter_uses_executor(self):
        from orcapod.core.function_pod import FunctionNode, FunctionPod

        spy = SpyExecutor()
        pf = PythonPacketFunction(add, output_keys="result")
        pf.executor = spy
        pod = FunctionPod(pf)

        node = FunctionNode(pod, _make_add_stream())

        results = list(node.iter_packets())
        assert len(results) == 2
        assert results[0][1].as_dict()["result"] == 3
        assert len(spy.calls) == 2
