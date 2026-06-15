"""Tests for write-side LogicalType auto-registration at function pod declaration.

These tests verify that _FunctionPodBase.__init__ triggers factory synthesis for
any non-native Python types in the pod's input/output schemas, and raises TypeError
at declaration time when no factory is registered.
"""

from __future__ import annotations

import uuid as _uuid_module
from typing import Optional

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.contexts.core import DataContext
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.extension_types.protocols import LogicalTypeProtocol
from orcapod.extension_types.registry import (
    LogicalTypeRegistry,
    make_arrow_extension_type,
    make_polars_extension_type,
)
from orcapod.semantic_types.universal_converter import UniversalTypeConverter


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_test_context(registry: LogicalTypeRegistry) -> DataContext:
    """Create a DataContext with a fresh converter bound to the given registry.

    A fresh ``UniversalTypeConverter`` is constructed with ``logical_type_registry``
    set at construction time, which is the canonical way to bind the two objects.
    """
    base_ctx = get_default_context()
    fresh_converter = UniversalTypeConverter(
        semantic_registry=base_ctx.type_converter.semantic_registry,
        logical_type_registry=registry,
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
    PolarsExt = make_polars_extension_type(arrow_name, pa.large_string())

    class _LT:
        logical_type_name = arrow_name
        python_type = py_type
        def get_arrow_extension_type(self): return ArrowExt()
        def get_polars_extension_type(self): return PolarsExt()
        def python_to_storage(self, v): return str(v)
        def storage_to_python(self, v): return v

    return _LT()


def _make_registry_with_factory(*target_bases: type) -> tuple[LogicalTypeRegistry, list[type]]:
    """Return a registry with a factory covering all target_bases and a call log."""
    call_log: list[type] = []

    class _Factory:
        def reconstruct_from_arrow(self, name, storage, meta):
            return _make_logical_type(object)

        def create_for_python_type(self, python_type):
            call_log.append(python_type)
            return _make_logical_type(python_type)

    registry = LogicalTypeRegistry()
    registry.register_logical_type_factory(_Factory(), python_bases=list(target_bases))
    return registry, call_log


# ── Custom classes used in tests ─────────────────────────────────────────────

class _MyBase:
    pass


class _MyChild(_MyBase):
    pass


class _MyOtherBase:
    pass


class _MyOtherChild(_MyOtherBase):
    pass


class _ThirdBase:
    pass


class _ThirdChild(_ThirdBase):
    pass


# ── Basic triggering tests ────────────────────────────────────────────────────

def test_pod_declaration_triggers_factory_for_input_type():
    """Declaring a FunctionPod with a custom input type causes factory synthesis."""
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


def test_pod_declaration_triggers_factory_for_output_type():
    """Declaring a FunctionPod with a custom output type causes factory synthesis."""
    registry, call_log = _make_registry_with_factory(_MyBase)
    ctx = _make_test_context(registry)

    def my_func(x: int) -> _MyChild:
        return _MyChild()

    FunctionPod(
        data_function=PythonDataFunction(my_func, output_keys=["result"]),
        data_context=ctx,
    )
    assert _MyChild in call_log
    assert registry.get_by_python_type(_MyChild) is not None


# ── Complex / nested type tests ───────────────────────────────────────────────

def test_pod_declaration_with_nested_list_input():
    """list[_MyChild] in a function input causes factory synthesis for _MyChild."""
    registry, call_log = _make_registry_with_factory(_MyBase)
    ctx = _make_test_context(registry)

    def my_func(items: list[_MyChild]) -> str:
        return ""

    FunctionPod(
        data_function=PythonDataFunction(my_func, output_keys=["result"]),
        data_context=ctx,
    )
    assert _MyChild in call_log


def test_pod_declaration_with_doubly_nested_input():
    """dict[str, list[_MyChild]] causes factory synthesis for _MyChild."""
    registry, call_log = _make_registry_with_factory(_MyBase)
    ctx = _make_test_context(registry)

    def my_func(mapping: dict[str, list[_MyChild]]) -> str:
        return ""

    FunctionPod(
        data_function=PythonDataFunction(my_func, output_keys=["result"]),
        data_context=ctx,
    )
    assert _MyChild in call_log


def test_pod_declaration_with_optional_input():
    """Optional[_MyChild] causes factory synthesis for _MyChild."""
    registry, call_log = _make_registry_with_factory(_MyBase)
    ctx = _make_test_context(registry)

    def my_func(x: Optional[_MyChild]) -> str:
        return ""

    FunctionPod(
        data_function=PythonDataFunction(my_func, output_keys=["result"]),
        data_context=ctx,
    )
    assert _MyChild in call_log


def test_pod_declaration_with_complex_output():
    """list[_MyChild] in the output schema causes factory synthesis."""
    registry, call_log = _make_registry_with_factory(_MyBase)
    ctx = _make_test_context(registry)

    def my_func(x: str) -> list[_MyChild]:
        return []

    FunctionPod(
        data_function=PythonDataFunction(my_func, output_keys=["result"]),
        data_context=ctx,
    )
    assert _MyChild in call_log


def test_pod_declaration_with_doubly_nested_output():
    """dict[str, list[_MyChild]] in the output causes factory synthesis for _MyChild."""
    registry, call_log = _make_registry_with_factory(_MyBase)
    ctx = _make_test_context(registry)

    def my_func(x: int) -> dict[str, list[_MyChild]]:
        return {}

    FunctionPod(
        data_function=PythonDataFunction(my_func, output_keys=["result"]),
        data_context=ctx,
    )
    assert _MyChild in call_log


# ── Multi-class tests ─────────────────────────────────────────────────────────

def test_pod_declaration_two_classes_one_in_input_one_in_output():
    """Two different custom classes — one in input, one in output — each gets synthesized."""
    registry, call_log = _make_registry_with_factory(_MyBase, _MyOtherBase)
    ctx = _make_test_context(registry)

    def my_func(x: _MyChild) -> _MyOtherChild:
        return _MyOtherChild()

    FunctionPod(
        data_function=PythonDataFunction(my_func, output_keys=["result"]),
        data_context=ctx,
    )
    assert _MyChild in call_log
    assert _MyOtherChild in call_log


def test_pod_declaration_two_classes_both_in_input():
    """Two different custom classes both as inputs each get synthesized."""
    registry, call_log = _make_registry_with_factory(_MyBase, _MyOtherBase)
    ctx = _make_test_context(registry)

    def my_func(x: _MyChild, y: _MyOtherChild) -> str:
        return ""

    FunctionPod(
        data_function=PythonDataFunction(my_func, output_keys=["result"]),
        data_context=ctx,
    )
    assert _MyChild in call_log
    assert _MyOtherChild in call_log


def test_pod_declaration_two_classes_both_in_output():
    """Two different custom classes both as outputs each get synthesized."""
    registry, call_log = _make_registry_with_factory(_MyBase, _MyOtherBase)
    ctx = _make_test_context(registry)

    def my_func(x: int) -> tuple[_MyChild, _MyOtherChild]:
        return _MyChild(), _MyOtherChild()

    FunctionPod(
        data_function=PythonDataFunction(
            my_func,
            output_keys=["first", "second"],
        ),
        data_context=ctx,
    )
    assert _MyChild in call_log
    assert _MyOtherChild in call_log


def test_pod_declaration_three_classes_mixed():
    """Three custom classes spread across input and output each get synthesized."""
    registry, call_log = _make_registry_with_factory(_MyBase, _MyOtherBase, _ThirdBase)
    ctx = _make_test_context(registry)

    def my_func(a: _MyChild, b: list[_MyOtherChild]) -> _ThirdChild:
        return _ThirdChild()

    FunctionPod(
        data_function=PythonDataFunction(my_func, output_keys=["result"]),
        data_context=ctx,
    )
    assert _MyChild in call_log
    assert _MyOtherChild in call_log
    assert _ThirdChild in call_log


def test_pod_declaration_three_classes_all_in_input():
    """Three custom classes all in input parameters each get synthesized."""
    registry, call_log = _make_registry_with_factory(_MyBase, _MyOtherBase, _ThirdBase)
    ctx = _make_test_context(registry)

    def my_func(a: _MyChild, b: _MyOtherChild, c: _ThirdChild) -> str:
        return ""

    FunctionPod(
        data_function=PythonDataFunction(my_func, output_keys=["result"]),
        data_context=ctx,
    )
    assert _MyChild in call_log
    assert _MyOtherChild in call_log
    assert _ThirdChild in call_log


# ── Skip / guard tests ────────────────────────────────────────────────────────

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


def test_pod_declaration_raises_for_nested_unhandled_class():
    """TypeError is raised even when the custom type is nested inside list[T]."""
    registry = LogicalTypeRegistry()  # empty — no factories
    ctx = _make_test_context(registry)

    def my_func(items: list[_MyChild]) -> str:
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


def test_pod_declaration_with_union_none_syntax():
    """``_MyChild | None`` (new-style union) causes factory synthesis for _MyChild.

    Python 3.10+ ``X | Y`` produces a ``types.UnionType``, which is a different
    runtime object from ``typing.Union[X, Y]``. This test confirms that
    ``extract_leaf_classes`` correctly unwraps both union forms and that
    ``NoneType`` is skipped in both cases.
    """
    registry, call_log = _make_registry_with_factory(_MyBase)
    ctx = _make_test_context(registry)

    def my_func(x: _MyChild | None) -> str:
        return ""

    FunctionPod(
        data_function=PythonDataFunction(my_func, output_keys=["result"]),
        data_context=ctx,
    )
    assert _MyChild in call_log
    assert registry.get_by_python_type(_MyChild) is not None
