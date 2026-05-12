"""Tests for DataFunctionProxy invocation, bind, and unbind behavior."""

import pytest

from orcapod.core.datagrams.tag_data import Data, Tag
from orcapod.core.function_pod import FunctionPod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.data_function_proxy import DataFunctionProxy
from orcapod.core.sources.dict_source import DictSource
from orcapod.errors import DataFunctionUnavailableError


# ==================== Task 2: Construction tests ====================


class TestDataFunctionProxyConstruction:
    """Tests for proxy construction and executor property."""

    def test_executor_returns_none(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        assert proxy.executor is None

    def test_executor_setter_is_noop(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        proxy.executor = None  # should not raise


# ==================== Helpers ====================


def _make_sample_function() -> PythonDataFunction:
    def double_age(age: int) -> int:
        return age * 2

    return PythonDataFunction(double_age, output_keys="doubled_age", version="v1.0")


def _make_proxy_from_function(pf: PythonDataFunction) -> DataFunctionProxy:
    config = pf.to_config()
    return DataFunctionProxy(
        config=config,
        content_hash_str=pf.content_hash().to_string(),
        pipeline_hash_str=pf.pipeline_hash().to_string(),
    )


# ==================== Task 3: Invocation tests ====================


class TestDataFunctionProxyInvocation:
    """Tests for proxy behavior when no function is bound."""

    def test_call_raises_when_unbound(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        data = Data({"age": 25})
        with pytest.raises(
            DataFunctionUnavailableError, match="double_age"
        ):
            proxy.call(data)

    @pytest.mark.asyncio
    async def test_async_call_raises_when_unbound(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        data = Data({"age": 25})
        with pytest.raises(
            DataFunctionUnavailableError, match="double_age"
        ):
            await proxy.async_call(data)

    def test_direct_call_raises_when_unbound(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        data = Data({"age": 25})
        with pytest.raises(
            DataFunctionUnavailableError, match="double_age"
        ):
            proxy.direct_call(data)

    @pytest.mark.asyncio
    async def test_direct_async_call_raises_when_unbound(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        data = Data({"age": 25})
        with pytest.raises(
            DataFunctionUnavailableError, match="double_age"
        ):
            await proxy.direct_async_call(data)

    def test_variation_data_empty_when_unbound(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        assert proxy.get_function_variation_data() == {}

    def test_execution_data_empty_when_unbound(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        assert proxy.get_execution_data() == {}


# ==================== Task 4: Bind/unbind tests ====================


class TestDataFunctionProxyBinding:
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
        data = Data({"age": 25})
        result = proxy.call(data)
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
        data = Data({"age": 25})
        with pytest.raises(DataFunctionUnavailableError):
            proxy.call(data)

    def test_bind_rejects_mismatched_function_name(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)

        def other_func(age: int) -> int:
            return age + 1

        other_pf = PythonDataFunction(
            other_func, output_keys="doubled_age", version="v1.0"
        )
        with pytest.raises(ValueError, match="canonical_function_name"):
            proxy.bind(other_pf)

    def test_bind_rejects_mismatched_version(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)

        def double_age(age: int) -> int:
            return age * 2

        other_pf = PythonDataFunction(
            double_age, output_keys="doubled_age", version="v2.0"
        )
        with pytest.raises(ValueError, match="major_version"):
            proxy.bind(other_pf)

    def test_bind_rejects_mismatched_output_schema(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)

        def double_age(age: int) -> str:
            return str(age * 2)

        other_pf = PythonDataFunction(
            double_age, output_keys="doubled_age", version="v1.0"
        )
        with pytest.raises(ValueError, match="output_data_schema"):
            proxy.bind(other_pf)

    def test_bind_rejects_mismatched_input_schema(self):
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)

        def double_age(name: str) -> int:
            return len(name) * 2

        other_pf = PythonDataFunction(
            double_age, output_keys="doubled_age", version="v1.0"
        )
        with pytest.raises(ValueError, match="input_data_schema"):
            proxy.bind(other_pf)


# ==================== Task 5: FunctionPod with proxy ====================


class TestFunctionPodWithProxy:
    """Tests for FunctionPod constructed with a DataFunctionProxy."""

    def test_function_pod_constructs_with_proxy(self):
        """FunctionPod accepts a proxy and exposes it as data_function."""
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        pod = FunctionPod(data_function=proxy)
        assert pod.data_function is proxy

    def test_function_pod_output_schema(self):
        """FunctionPod with proxy correctly reports output schema."""
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        pod = FunctionPod(data_function=proxy)
        source = DictSource(
            data=[{"age": 10}, {"age": 20}, {"age": 30}],
        )
        _tag_schema, data_schema = pod.output_schema(source)
        assert "doubled_age" in data_schema

    def test_function_pod_process_data_raises(self):
        """FunctionPod with unbound proxy raises on process_data."""
        pf = _make_sample_function()
        proxy = _make_proxy_from_function(pf)
        pod = FunctionPod(data_function=proxy)
        tag = Tag({})
        data = Data({"age": 25})
        with pytest.raises(DataFunctionUnavailableError):
            pod.process_data(tag, data)
