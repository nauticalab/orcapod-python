"""Tests for write-side LogicalType auto-registration at function pod declaration.

These tests verify that _FunctionPodBase.__init__ triggers factory synthesis for
any non-native Python types in the pod's input/output schemas, and raises TypeError
at declaration time when no factory is registered.
"""

from __future__ import annotations

import uuid as _uuid_module

import pyarrow as pa
import polars as pl
import pytest

from orcapod.contexts import get_default_context
from orcapod.contexts.core import DataContext
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.extension_types.protocols import LogicalTypeProtocol
from orcapod.extension_types.registry import (
    LogicalTypeRegistry,
    make_arrow_extension_type,
)
from orcapod.semantic_types.universal_converter import UniversalTypeConverter


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_test_context(registry: LogicalTypeRegistry) -> DataContext:
    """Create a DataContext with a fresh converter so the global default is not mutated.

    Re-using the default context's ``type_converter`` singleton would cause
    ``DataContext.__post_init__`` to overwrite its ``_logical_type_registry``
    to point to the test registry, corrupting global converter state for
    subsequently-run tests.
    """
    base_ctx = get_default_context()
    # Fresh converter so we don't mutate the module-level singleton.
    fresh_converter = UniversalTypeConverter(
        semantic_registry=base_ctx.type_converter.semantic_registry,
    )
    return DataContext(
        context_key="test",
        version="test",
        description="test",
        type_converter=fresh_converter,
        arrow_hasher=base_ctx.arrow_hasher,
        semantic_hasher=base_ctx.semantic_hasher,
        type_handler_registry=base_ctx.type_handler_registry,
        logical_type_registry=registry,
    )


def _make_logical_type(py_type: type) -> LogicalTypeProtocol:
    """Synthesize a minimal LogicalType for py_type."""
    arrow_name = f"{py_type.__module__}.{py_type.__qualname__}.{_uuid_module.uuid4().hex[:6]}"
    ArrowExt = make_arrow_extension_type(arrow_name, pa.large_string())

    class _PolarsExt(pl.BaseExtension):
        def __init__(self):
            super().__init__(arrow_name, pl.String, None)
        @classmethod
        def ext_from_params(cls, ext_name, storage_dtype, metadata_str):
            return cls()

    class _LT:
        logical_type_name = arrow_name
        python_type = py_type
        def get_arrow_extension_type(self): return ArrowExt()
        def get_polars_extension_type(self): return _PolarsExt()
        def python_to_storage(self, v): return str(v)
        def storage_to_python(self, v): return v

    return _LT()


def _make_registry_with_factory(target_base: type) -> tuple[LogicalTypeRegistry, list[type]]:
    """Return a registry with a factory for target_base and a call log."""
    call_log: list[type] = []

    class _Factory:
        def reconstruct_from_arrow(self, name, storage, meta):
            return _make_logical_type(object)

        def create_for_python_type(self, python_type):
            call_log.append(python_type)
            return _make_logical_type(python_type)

    registry = LogicalTypeRegistry()
    registry.register_logical_type_factory(_Factory(), python_bases=[target_base])
    return registry, call_log


# ── Custom classes used in tests ─────────────────────────────────────────────

class _MyBase:
    pass


class _MyChild(_MyBase):
    pass


# ── Tests ────────────────────────────────────────────────────────────────────

def test_pod_declaration_triggers_factory_for_unregistered_class():
    """Declaring a FunctionPod with an unregistered type causes factory synthesis."""
    registry, call_log = _make_registry_with_factory(_MyBase)
    ctx = _make_test_context(registry)

    def my_func(x: _MyChild) -> str:
        return str(x)

    FunctionPod(
        data_function=PythonDataFunction(my_func, output_keys=["result"]),
        data_context=ctx,
    )
    assert _MyChild in call_log
    assert registry.get_by_python_type(_MyChild) is not None


def test_pod_declaration_with_nested_list_type():
    """list[_MyChild] in the schema causes factory synthesis for _MyChild."""
    registry, call_log = _make_registry_with_factory(_MyBase)
    ctx = _make_test_context(registry)

    def my_func(items: list[_MyChild]) -> str:
        return ""

    FunctionPod(
        data_function=PythonDataFunction(my_func, output_keys=["result"]),
        data_context=ctx,
    )
    assert _MyChild in call_log


def test_pod_declaration_native_types_no_factory_call():
    """Pods using only native types (int, str, etc.) never trigger factory lookup."""

    class _NeverCalledFactory:
        def reconstruct_from_arrow(self, *a): ...
        def create_for_python_type(self, pt):
            raise AssertionError(f"factory called for {pt!r}")

    registry = LogicalTypeRegistry()
    registry.register_logical_type_factory(_NeverCalledFactory(), python_bases=[object])
    ctx = _make_test_context(registry)

    def my_func(x: int, y: str) -> float:
        return 0.0

    # Should not raise — int, str, float are native
    FunctionPod(
        data_function=PythonDataFunction(my_func, output_keys=["result"]),
        data_context=ctx,
    )


def test_pod_declaration_raises_type_error_for_unhandled_class():
    """Pod with a type that has no registered factory raises TypeError at declaration."""
    registry = LogicalTypeRegistry()  # empty — no factories
    ctx = _make_test_context(registry)

    def my_func(x: _MyChild) -> str:
        return ""

    with pytest.raises(TypeError, match="No LogicalType or LogicalTypeFactory"):
        FunctionPod(
            data_function=PythonDataFunction(my_func, output_keys=["result"]),
            data_context=ctx,
        )


def test_pod_declaration_already_registered_type_no_factory_call():
    """Pre-registered types are not passed to the factory."""
    registry, call_log = _make_registry_with_factory(_MyBase)
    # Pre-register _MyChild directly
    registry.register_logical_type(_make_logical_type(_MyChild))
    ctx = _make_test_context(registry)

    def my_func(x: _MyChild) -> str:
        return ""

    FunctionPod(
        data_function=PythonDataFunction(my_func, output_keys=["result"]),
        data_context=ctx,
    )
    # Factory was NOT called — _MyChild was already registered
    assert _MyChild not in call_log
