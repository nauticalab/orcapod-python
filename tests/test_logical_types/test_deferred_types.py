"""Deferred registration of optional value types (ITL-871).

A registry can name a value type by its qualified class name and extension name
without importing it; the type is loaded on the first lookup that needs it.
"""

from __future__ import annotations

import pytest

from orcapod.errors import MissingOptionalDependencyError
from orcapod.hashing.semantic_hashing.type_handler_registry import (
    PythonTypeHandlerRegistry,
)
from orcapod.logical_types.registry import LogicalTypeRegistry

from . import deferred_fixture as fx

_MOD = fx.__name__


def _registry(requires: str = "polars") -> LogicalTypeRegistry:
    """A logical type registry with ``Thing`` deferred; ``requires`` names the needed module."""
    return LogicalTypeRegistry(
        deferred_logical_types=[
            {
                "python_type": f"{_MOD}.Thing",
                "extension_name": fx.THING_EXTENSION_NAME,
                "logical_type": f"{_MOD}.LogicalThing",
                "requires": requires,
            }
        ]
    )


def _handlers() -> PythonTypeHandlerRegistry:
    """A handler registry with ``ThingHandler`` deferred."""
    registry = PythonTypeHandlerRegistry()
    registry.register_deferred(f"{_MOD}.Thing", f"{_MOD}.ThingHandler")
    return registry


def test_python_type_lookup_loads_the_deferred_type_once() -> None:
    """Looking up ``Thing`` loads and registers its logical type once."""
    registry = _registry()
    before = fx.LogicalThing.instances
    first = registry.get_by_python_type(fx.Thing)
    second = registry.get_by_python_type(fx.Thing)
    assert isinstance(first, fx.LogicalThing)
    assert second is first
    assert fx.LogicalThing.instances == before + 1
    assert registry.get_by_arrow_extension_name(fx.THING_EXTENSION_NAME) is first


def test_subclass_resolves_through_the_mro() -> None:
    """A subclass of a deferred class resolves to the deferred logical type."""
    assert isinstance(_registry().get_by_python_type(fx.SubThing), fx.LogicalThing)


def test_extension_name_lookup_loads_the_deferred_type() -> None:
    """Looking up the extension name loads the type and binds the Python type too."""
    registry = _registry()
    lt = registry.get_by_arrow_extension_name(fx.THING_EXTENSION_NAME)
    assert isinstance(lt, fx.LogicalThing)
    assert registry.get_by_python_type(fx.Thing) is lt


def test_unrelated_lookups_load_nothing() -> None:
    """Lookups of unrelated types and names return ``None`` without loading anything."""
    registry = _registry()
    before = fx.LogicalThing.instances
    assert registry.get_by_python_type(complex) is None
    assert registry.get_by_arrow_extension_name("orcapod_test.unknown") is None
    assert fx.LogicalThing.instances == before


def test_missing_dependency_names_extension_and_module() -> None:
    """A deferred type whose module is missing raises an error naming both."""
    registry = _registry(requires="orcapod_test_no_such_module")
    with pytest.raises(MissingOptionalDependencyError) as exc_info:
        registry.get_by_arrow_extension_name(fx.THING_EXTENSION_NAME)
    message = str(exc_info.value)
    assert fx.THING_EXTENSION_NAME in message
    assert "orcapod_test_no_such_module" in message
    assert isinstance(exc_info.value, ImportError)


def test_handler_lookup_loads_the_deferred_handler_once() -> None:
    """Looking up a ``Thing`` instance or type loads its handler once."""
    handlers = _handlers()
    before = fx.ThingHandler.instances
    first = handlers.get_handler(fx.Thing("a"))
    assert isinstance(first, fx.ThingHandler)
    assert handlers.get_handler(fx.SubThing("b")) is first
    assert handlers.get_handler_for_type(fx.Thing) is first
    assert fx.ThingHandler.instances == before + 1


def test_handler_lookup_for_unrelated_type_loads_nothing() -> None:
    """Unrelated handler lookups return ``None`` without loading anything."""
    handlers = _handlers()
    before = fx.ThingHandler.instances
    assert handlers.get_handler(1.5) is None
    assert handlers.get_handler_for_type(complex) is None
    assert fx.ThingHandler.instances == before


def test_concurrent_first_lookups_load_the_type_once() -> None:
    """Threads resolving a deferred type at the same time all get one registered instance."""
    from concurrent.futures import ThreadPoolExecutor

    registry = LogicalTypeRegistry(
        deferred_logical_types=[
            {
                "python_type": f"{_MOD}.SlowThing",
                "extension_name": fx.SLOW_EXTENSION_NAME,
                "logical_type": f"{_MOD}.LogicalSlowThing",
                "requires": "polars",
            }
        ]
    )
    with ThreadPoolExecutor(8) as pool:
        found = list(
            pool.map(lambda _: registry.get_by_python_type(fx.SlowThing), range(8))
        )
    assert all(isinstance(lt, fx.LogicalSlowThing) for lt in found)
    assert len({id(lt) for lt in found}) == 1


class _BaseHandler:
    """A handler registered for the base class ``Thing``."""

    def handle(self, obj: object, hasher: object) -> str:
        """Return a marker."""
        return "base"


def test_deferred_handler_of_a_subclass_wins_over_a_registered_base_handler() -> None:
    """Lookup walks the MRO once: the most-derived handler wins, registered or deferred."""
    registry = PythonTypeHandlerRegistry()
    base_handler = _BaseHandler()
    registry.register(fx.Thing, base_handler)  # pyright: ignore[reportArgumentType]  # toy handler implements the protocol structurally
    registry.register_deferred(f"{_MOD}.SubThing", f"{_MOD}.ThingHandler")

    assert registry.get_handler(fx.Thing("a")) is base_handler
    assert registry.get_handler_for_type(fx.Thing) is base_handler
    assert isinstance(registry.get_handler_for_type(fx.SubThing), fx.ThingHandler)
    assert isinstance(registry.get_handler(fx.SubThing("a")), fx.ThingHandler)


def test_deferred_type_of_a_subclass_wins_over_a_registered_base_type() -> None:
    """The logical type of the most-derived class wins, registered or deferred."""
    registry = LogicalTypeRegistry(
        logical_types=[fx.LogicalThing()],
        deferred_logical_types=[
            {
                "python_type": f"{_MOD}.SubThing",
                "extension_name": fx.SUB_EXTENSION_NAME,
                "logical_type": f"{_MOD}.LogicalSubThing",
                "requires": "polars",
            }
        ],
    )
    assert type(registry.get_by_python_type(fx.Thing)) is fx.LogicalThing
    assert type(registry.get_by_python_type(fx.SubThing)) is fx.LogicalSubThing
    # The answer is the same once the deferred type is loaded.
    assert type(registry.get_by_python_type(fx.SubThing)) is fx.LogicalSubThing


def test_deferred_exact_type_is_never_returned_for_a_subclass() -> None:
    """A deferred type that matches its class exactly is not used for a subclass.

    The answer is the same whether the subclass or the class is looked up first.
    """

    def registry() -> LogicalTypeRegistry:
        return LogicalTypeRegistry(
            deferred_logical_types=[
                {
                    "python_type": f"{_MOD}.ExactThing",
                    "extension_name": fx.EXACT_EXTENSION_NAME,
                    "logical_type": f"{_MOD}.LogicalExactThing",
                    "requires": "polars",
                }
            ]
        )

    subclass_first = registry()
    assert subclass_first.get_by_python_type(fx.SubExactThing) is None
    assert isinstance(
        subclass_first.get_by_python_type(fx.ExactThing), fx.LogicalExactThing
    )
    assert subclass_first.get_by_python_type(fx.SubExactThing) is None

    class_first = registry()
    assert isinstance(
        class_first.get_by_python_type(fx.ExactThing), fx.LogicalExactThing
    )
    assert class_first.get_by_python_type(fx.SubExactThing) is None
