"""
Tests for chaining multiple function pods in sequence.

Covers:
- Two-pod linear chain: output stream of pod1 feeds into pod2
- Three-pod linear chain with value verification at each stage
- Chaining via the decorator (@function_pod) interface
- TagProtocol preservation across chained pods
- Row count preservation across chained pods
- as_table() results after chaining
- Chain where an intermediate pod is inactive (packets filtered out)
"""

from __future__ import annotations

import pytest

from orcapod.core.function_pod import FunctionPodStream, FunctionPod, function_pod
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.protocols.core_protocols import StreamProtocol

from ..conftest import double, make_int_stream


# ---------------------------------------------------------------------------
# Helper functions used across chaining tests
# ---------------------------------------------------------------------------


def triple(x: int) -> int:
    return x * 3


def add_one(result: int) -> int:
    return result + 1


def square(result: int) -> int:
    return result * result


# ---------------------------------------------------------------------------
# 1. Two-pod linear chain
# ---------------------------------------------------------------------------


class TestTwoPodChain:
    @pytest.fixture
    def double_pod(self):
        return FunctionPod(
            packet_function=PythonPacketFunction(double, output_keys="result")
        )

    @pytest.fixture
    def add_one_pod(self):
        return FunctionPod(
            packet_function=PythonPacketFunction(add_one, output_keys="result")
        )

    def test_chain_returns_function_pod_stream(self, double_pod, add_one_pod):
        stream1 = double_pod.process(make_int_stream(n=3))
        stream2 = add_one_pod.process(stream1)
        assert isinstance(stream2, FunctionPodStream)

    def test_chain_satisfies_stream_protocol(self, double_pod, add_one_pod):
        stream1 = double_pod.process(make_int_stream(n=3))
        stream2 = add_one_pod.process(stream1)
        assert isinstance(stream2, StreamProtocol)

    def test_chain_row_count_preserved(self, double_pod, add_one_pod):
        n = 5
        stream1 = double_pod.process(make_int_stream(n=n))
        stream2 = add_one_pod.process(stream1)
        assert len(list(stream2.iter_packets())) == n

    def test_chain_values_correct(self, double_pod, add_one_pod):
        # double(x) → result = x*2, then add_one(result) → result = x*2 + 1
        n = 4
        for i, (_, packet) in enumerate(
            add_one_pod.process(double_pod.process(make_int_stream(n=n))).iter_packets()
        ):
            assert packet["result"] == i * 2 + 1

    def test_chain_tag_preserved(self, double_pod, add_one_pod):
        n = 3
        for i, (tag, _) in enumerate(
            add_one_pod.process(double_pod.process(make_int_stream(n=n))).iter_packets()
        ):
            assert tag["id"] == i

    def test_chain_as_table_has_correct_columns(self, double_pod, add_one_pod):
        table = add_one_pod.process(double_pod.process(make_int_stream(n=3))).as_table()
        assert "id" in table.column_names
        assert "result" in table.column_names

    def test_chain_as_table_values_correct(self, double_pod, add_one_pod):
        n = 3
        table = add_one_pod.process(double_pod.process(make_int_stream(n=n))).as_table()
        results = table.column("result").to_pylist()
        assert results == [i * 2 + 1 for i in range(n)]

    def test_intermediate_stream_upstream_is_first_pod_stream(
        self, double_pod, add_one_pod
    ):
        stream1 = double_pod.process(make_int_stream(n=3))
        stream2 = add_one_pod.process(stream1)
        assert stream1 in stream2.upstreams


# ---------------------------------------------------------------------------
# 2. Three-pod linear chain
# ---------------------------------------------------------------------------


class TestThreePodChain:
    @pytest.fixture
    def double_pod(self):
        return FunctionPod(
            packet_function=PythonPacketFunction(double, output_keys="result")
        )

    @pytest.fixture
    def add_one_pod(self):
        return FunctionPod(
            packet_function=PythonPacketFunction(add_one, output_keys="result")
        )

    @pytest.fixture
    def square_pod(self):
        return FunctionPod(
            packet_function=PythonPacketFunction(square, output_keys="result")
        )

    def test_three_pod_chain_row_count(self, double_pod, add_one_pod, square_pod):
        n = 4
        stream = make_int_stream(n=n)
        stream = double_pod.process(stream)
        stream = add_one_pod.process(stream)
        stream = square_pod.process(stream)
        assert len(list(stream.iter_packets())) == n

    def test_three_pod_chain_values(self, double_pod, add_one_pod, square_pod):
        # double(x) → x*2, add_one(x*2) → x*2+1, square(x*2+1) → (x*2+1)^2
        n = 4
        stream = square_pod.process(
            add_one_pod.process(double_pod.process(make_int_stream(n=n)))
        )
        for i, (_, packet) in enumerate(stream.iter_packets()):
            expected = (i * 2 + 1) ** 2
            assert packet["result"] == expected

    def test_three_pod_chain_tags_preserved(self, double_pod, add_one_pod, square_pod):
        n = 4
        stream = square_pod.process(
            add_one_pod.process(double_pod.process(make_int_stream(n=n)))
        )
        for i, (tag, _) in enumerate(stream.iter_packets()):
            assert tag["id"] == i

    def test_three_pod_chain_as_table_correct(
        self, double_pod, add_one_pod, square_pod
    ):
        n = 3
        table = square_pod.process(
            add_one_pod.process(double_pod.process(make_int_stream(n=n)))
        ).as_table()
        results = table.column("result").to_pylist()
        assert results == [(i * 2 + 1) ** 2 for i in range(n)]

    def test_three_pod_chain_table_has_tag_column(
        self, double_pod, add_one_pod, square_pod
    ):
        table = square_pod.process(
            add_one_pod.process(double_pod.process(make_int_stream(n=3)))
        ).as_table()
        assert "id" in table.column_names

    def test_each_intermediate_stream_has_correct_producer(
        self, double_pod, add_one_pod, square_pod
    ):
        src = make_int_stream(n=3)
        s1 = double_pod.process(src)
        s2 = add_one_pod.process(s1)
        s3 = square_pod.process(s2)
        assert s1.producer is double_pod
        assert s2.producer is add_one_pod
        assert s3.producer is square_pod


# ---------------------------------------------------------------------------
# 3. Chaining via the @function_pod decorator
# ---------------------------------------------------------------------------


@function_pod(output_keys="result")
def decor_double(x: int) -> int:
    return x * 2


@function_pod(output_keys="result")
def decor_triple(result: int) -> int:
    return result * 3


@function_pod(output_keys="result")
def decor_add_five(result: int) -> int:
    return result + 5


class TestDecoratorChaining:
    def test_two_decorator_pods_chain(self):
        n = 3
        stream = decor_triple.pod.process(
            decor_double.pod.process(make_int_stream(n=n))
        )
        assert isinstance(stream, FunctionPodStream)

    def test_two_decorator_pods_values(self):
        # double(x) → x*2, triple(x*2) → x*6
        n = 4
        for i, (_, packet) in enumerate(
            decor_triple.pod.process(
                decor_double.pod.process(make_int_stream(n=n))
            ).iter_packets()
        ):
            assert packet["result"] == i * 6

    def test_three_decorator_pods_values(self):
        # double(x) → x*2, triple(x*2) → x*6, add_five(x*6) → x*6 + 5
        n = 4
        stream = decor_add_five.pod.process(
            decor_triple.pod.process(decor_double.pod.process(make_int_stream(n=n)))
        )
        for i, (_, packet) in enumerate(stream.iter_packets()):
            assert packet["result"] == i * 6 + 5

    def test_decorator_chain_as_table_correct(self):
        n = 3
        table = decor_add_five.pod.process(
            decor_triple.pod.process(decor_double.pod.process(make_int_stream(n=n)))
        ).as_table()
        results = table.column("result").to_pylist()
        assert results == [i * 6 + 5 for i in range(n)]

    def test_decorator_chain_row_count_preserved(self):
        n = 5
        stream = decor_triple.pod.process(
            decor_double.pod.process(make_int_stream(n=n))
        )
        assert len(list(stream.iter_packets())) == n


# ---------------------------------------------------------------------------
# 4. Chain where an intermediate pod is inactive (packets filtered out)
# ---------------------------------------------------------------------------


class TestChainWithInactivePod:
    """
    When a pod's packet function is set inactive, its call() returns None and
    those packets are silently dropped by iter_packets().  Downstream pods in
    the chain therefore receive zero packets.
    """

    @pytest.fixture
    def double_pf(self):
        return PythonPacketFunction(double, output_keys="result")

    @pytest.fixture
    def add_one_pf(self):
        return PythonPacketFunction(add_one, output_keys="result")

    def test_inactive_first_pod_yields_no_packets(self, double_pf, add_one_pf):
        double_pf.set_active(False)
        pod1 = FunctionPod(packet_function=double_pf)
        pod2 = FunctionPod(packet_function=add_one_pf)
        stream = pod2.process(pod1.process(make_int_stream(n=3)))
        assert list(stream.iter_packets()) == []

    def test_inactive_second_pod_yields_no_packets(self, double_pf, add_one_pf):
        add_one_pf.set_active(False)
        pod1 = FunctionPod(packet_function=double_pf)
        pod2 = FunctionPod(packet_function=add_one_pf)
        stream = pod2.process(pod1.process(make_int_stream(n=3)))
        assert list(stream.iter_packets()) == []

    def test_reactivating_pod_restores_output(self, double_pf, add_one_pf):
        double_pf.set_active(False)
        pod1 = FunctionPod(packet_function=double_pf)
        pod2 = FunctionPod(packet_function=add_one_pf)

        stream_inactive = pod2.process(pod1.process(make_int_stream(n=3)))
        assert list(stream_inactive.iter_packets()) == []

        double_pf.set_active(True)
        stream_active = pod2.process(pod1.process(make_int_stream(n=3)))
        assert len(list(stream_active.iter_packets())) == 3
