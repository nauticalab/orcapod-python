"""Tests for PacketFunctionProxy invocation, bind, and unbind behavior."""

import pytest

from orcapod.core.datagrams.tag_packet import Packet
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.packet_function_proxy import PacketFunctionProxy
from orcapod.errors import PacketFunctionUnavailableError


# ==================== Helpers ====================


def _make_sample_function() -> PythonPacketFunction:
    def double_age(age: int) -> int:
        return age * 2

    return PythonPacketFunction(double_age, output_keys="doubled_age", version="v1.0")


def _make_proxy_from_function(pf: PythonPacketFunction) -> PacketFunctionProxy:
    config = pf.to_config()
    return PacketFunctionProxy(
        config=config,
        uri=tuple(pf.uri),
        content_hash_str=pf.content_hash().to_string(),
        pipeline_hash_str=pf.pipeline_hash().to_string(),
    )


# ==================== Task 3: Invocation tests ====================


class TestPacketFunctionProxyInvocation:
    """Tests for proxy behavior when no function is bound."""

    def test_call_raises_when_unbound(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        packet = Packet({"age": 25})
        with pytest.raises(
            PacketFunctionUnavailableError, match="double_age"
        ):
            proxy.call(packet)

    @pytest.mark.asyncio
    async def test_async_call_raises_when_unbound(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        packet = Packet({"age": 25})
        with pytest.raises(
            PacketFunctionUnavailableError, match="double_age"
        ):
            await proxy.async_call(packet)

    def test_direct_call_raises_when_unbound(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        packet = Packet({"age": 25})
        with pytest.raises(
            PacketFunctionUnavailableError, match="double_age"
        ):
            proxy.direct_call(packet)

    def test_variation_data_empty_when_unbound(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        assert proxy.get_function_variation_data() == {}

    def test_execution_data_empty_when_unbound(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        assert proxy.get_execution_data() == {}


# ==================== Task 4: Bind/unbind tests ====================


class TestPacketFunctionProxyBinding:
    """Tests for bind/unbind and identity mismatch detection."""

    def test_bind_succeeds_with_matching_function(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        proxy.bind(pf)
        assert proxy.is_bound

    def test_call_delegates_after_bind(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        proxy.bind(pf)
        packet = Packet({"age": 25})
        result = proxy.call(packet)
        assert result is not None
        assert result.as_dict()["doubled_age"] == 50

    def test_variation_data_delegates_after_bind(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        proxy.bind(pf)
        data = proxy.get_function_variation_data()
        assert "function_name" in data
        assert data["function_name"] == "double_age"

    def test_execution_data_delegates_after_bind(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        proxy.bind(pf)
        data = proxy.get_execution_data()
        assert "python_version" in data

    def test_unbind_reverts_to_proxy_mode(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        proxy.bind(pf)
        assert proxy.is_bound
        proxy.unbind()
        assert not proxy.is_bound
        packet = Packet({"age": 25})
        with pytest.raises(PacketFunctionUnavailableError):
            proxy.call(packet)

    def test_bind_rejects_mismatched_function_name(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)

        def other_func(age: int) -> int:
            return age + 1

        other_pf = PythonPacketFunction(
            other_func, output_keys="doubled_age", version="v1.0"
        )
        with pytest.raises(ValueError, match="canonical_function_name"):
            proxy.bind(other_pf)

    def test_bind_rejects_mismatched_version(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)

        def double_age(age: int) -> int:
            return age * 2

        other_pf = PythonPacketFunction(
            double_age, output_keys="doubled_age", version="v2.0"
        )
        with pytest.raises(ValueError, match="major_version"):
            proxy.bind(other_pf)

    def test_bind_rejects_mismatched_output_schema(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)

        def double_age(age: int) -> str:
            return str(age * 2)

        other_pf = PythonPacketFunction(
            double_age, output_keys="doubled_age", version="v1.0"
        )
        with pytest.raises(ValueError, match="output_packet_schema"):
            proxy.bind(other_pf)
